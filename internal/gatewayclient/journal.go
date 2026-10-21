package gatewayclient

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"
)

const _journalVersion = 1

type operation struct {
	Version       int             `json:"version"`
	ID            string          `json:"operation_id"`
	Key           string          `json:"idempotency_key"`
	Origin        string          `json:"origin"`
	Namespace     string          `json:"namespace"`
	Subject       string          `json:"caller_subject"`
	Command       string          `json:"command"`
	Path          string          `json:"path"`
	Body          json.RawMessage `json:"body"`
	Attempt       int             `json:"selected_attempt"`
	Baseline      []rootTurn      `json:"root_baseline"`
	BaselineKnown bool            `json:"baseline_observed"`
	CreatedAt     time.Time       `json:"created_at"`
}

type receipt struct {
	ConversationID  string    `json:"conversation_id"`
	ConversationURL string    `json:"conversation_url"`
	RouteID         string    `json:"route_id"`
	Attempt         int       `json:"attempt"`
	EventID         string    `json:"event_id"`
	ReceivedAt      time.Time `json:"received_at"`
	Acceptance      string    `json:"acceptance"`
}

type journal struct{ root *os.Root }

func openJournal(path string) (*journal, error) {
	if path == "" {
		return nil, errors.New("state-dir is required for durable operations")
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return nil, errors.New("resolve private state directory")
	}
	if err := rejectPathSymlinks(absolute); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(absolute, 0700); err != nil {
		return nil, errors.New("create private state directory")
	}
	if err := syncDirectoryParents(absolute); err != nil {
		return nil, err
	}
	info, err := os.Lstat(absolute)
	if err != nil || !info.IsDir() || !privateOwned(info) {
		return nil, errors.New("state directory must be owned by the caller with mode 0700")
	}
	root, err := os.OpenRoot(absolute)
	if err != nil {
		return nil, errors.New("open private state directory")
	}
	return &journal{root: root}, nil
}

func (j *journal) close() { _ = j.root.Close() }

func (j *journal) load(id string) (operation, error) {
	var op operation
	if !validOperationID(id) {
		return op, errors.New("invalid operation ID")
	}
	if err := j.read(id+".json", &op); err != nil {
		return op, err
	}
	if op.Version != _journalVersion || op.ID != id || op.Key != id || !json.Valid(op.Body) {
		return operation{}, errors.New("invalid saved operation")
	}
	return op, nil
}

func (j *journal) read(name string, target any) error {
	info, err := j.root.Lstat(name)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || !privateOwned(info) {
		return errors.New("journal entry must be a restricted regular file")
	}
	file, err := j.root.OpenFile(name, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return errors.New("open private journal entry")
	}
	defer func() { _ = file.Close() }()
	openedInfo, err := file.Stat()
	if err != nil || !openedInfo.Mode().IsRegular() || !privateOwned(openedInfo) || !os.SameFile(info, openedInfo) {
		return errors.New("journal entry changed while opening")
	}
	raw, err := io.ReadAll(io.LimitReader(file, _maxResponseBytes+1))
	if err != nil || len(raw) > _maxResponseBytes {
		return errors.New("read bounded journal entry")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return errors.New("invalid journal entry")
	}
	var extra any
	if !errors.Is(decoder.Decode(&extra), io.EOF) {
		return errors.New("invalid trailing journal content")
	}
	return nil
}

func (j *journal) save(op operation) (operation, error) {
	if err := j.persist(op.ID+".json", op); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return operation{}, err
		}
		existing, loadErr := j.load(op.ID)
		if loadErr != nil {
			return operation{}, loadErr
		}
		if !sameRequest(existing, op) {
			return operation{}, errors.New("saved operation conflicts with body, target, namespace or service replay subject")
		}
		return existing, nil
	}
	return op, nil
}

func (j *journal) loadReceipt(id string) (receipt, error) {
	var result receipt
	if !validOperationID(id) {
		return result, errors.New("invalid operation ID")
	}
	err := j.read(id+".receipt.json", &result)
	return result, err
}

func (j *journal) saveReceipt(id string, value receipt) error {
	if err := j.persist(id+".receipt.json", value); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return err
		}
		existing, readErr := j.loadReceipt(id)
		if readErr != nil || existing != value {
			return errors.New("saved receipt conflicts; retain the operation for inspection")
		}
	}
	return j.sync()
}

// Publish a complete fsynced file using an exclusive hard link, then fsync the
// directory. Neither a torn temporary file nor an existing symlink is adopted.
// Immutable operation and receipt files also make concurrent resumes safe.
func (j *journal) persist(name string, value any) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return errors.New("encode private journal entry")
	}
	id, err := newID()
	if err != nil {
		return err
	}
	temporary := ".pending-" + id
	file, err := j.root.OpenFile(temporary, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return errors.New("create exclusive private journal entry")
	}
	defer func() { _ = j.root.Remove(temporary) }()
	_, writeErr := file.Write(raw)
	syncErr := file.Sync()
	closeErr := file.Close()
	if errors.Join(writeErr, syncErr, closeErr) != nil {
		return errors.New("write durable private journal entry")
	}
	if err := j.root.Link(temporary, name); err != nil {
		return fmt.Errorf("publish private journal entry: %w", err)
	}
	return j.sync()
}

func (j *journal) sync() error {
	directory, err := j.root.Open(".")
	if err != nil {
		return errors.New("open journal directory for sync")
	}
	dirSyncErr := directory.Sync()
	dirCloseErr := directory.Close()
	if errors.Join(dirSyncErr, dirCloseErr) != nil {
		return errors.New("sync private journal directory")
	}
	return nil
}

func syncDirectoryParents(path string) error {
	for current := path; ; current = filepath.Dir(current) {
		directory, err := os.Open(current)
		if err != nil {
			return errors.New("open state directory ancestry for sync")
		}
		syncErr := directory.Sync()
		closeErr := directory.Close()
		if errors.Join(syncErr, closeErr) != nil {
			return errors.New("sync state directory ancestry")
		}
		if filepath.Dir(current) == current {
			return nil
		}
	}
}

func (j *journal) list() ([]string, error) {
	directory, err := j.root.Open(".")
	if err != nil {
		return nil, errors.New("open journal directory")
	}
	defer func() { _ = directory.Close() }()
	entries, err := directory.ReadDir(-1)
	if err != nil {
		return nil, errors.New("read journal directory")
	}
	ids := make([]string, 0, len(entries))
	for _, entry := range entries {
		id := strings.TrimSuffix(entry.Name(), ".json")
		if strings.HasSuffix(entry.Name(), ".json") && validOperationID(id) {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	return ids, nil
}

func sameRequest(a, b operation) bool {
	identity := a.ID == b.ID && a.Key == b.Key && a.Origin == b.Origin && a.Namespace == b.Namespace
	request := a.Subject == b.Subject && a.Command == b.Command && a.Path == b.Path && bytes.Equal(a.Body, b.Body)
	return identity && request
}

func validOperationID(id string) bool {
	if len(id) < 1 || len(id) > 128 || strings.HasPrefix(id, "action-") {
		return false
	}
	for _, char := range id {
		letter := char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z'
		digit := char >= '0' && char <= '9'
		if !letter && !digit && char != '_' && char != '-' {
			return false
		}
	}
	return true
}

func newID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", errors.New("generate operation identity")
	}
	return "client-" + hex.EncodeToString(raw[:]), nil
}

func privateOwned(info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Uid == uint32(os.Getuid()) && info.Mode().Perm()&0077 == 0
}

func rejectPathSymlinks(path string) error {
	for current := path; ; current = filepath.Dir(current) {
		info, err := os.Lstat(current)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return errors.New("inspect private path")
		}
		if err == nil && info.Mode()&os.ModeSymlink != 0 {
			return errors.New("private paths must not contain symlinks")
		}
		if filepath.Dir(current) == current {
			return nil
		}
	}
}
