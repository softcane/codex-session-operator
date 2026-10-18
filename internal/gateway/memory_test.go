package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"slices"
	"sync"
	"testing"
)

// memoryStore isolates HTTP/worker unit tests. Durability and restart tests use
// real PostgreSQL; this test double is never compiled into the gateway.
type memoryStore struct {
	mu      sync.Mutex
	records map[string][]byte
	closed  bool
}

var _ Storage = (*memoryStore)(nil)

func testStore(t *testing.T) *memoryStore {
	t.Helper()
	store := &memoryStore{records: make(map[string][]byte)}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func (s *memoryStore) Get(ctx context.Context, uid string) (Record, error) {
	if err := ctx.Err(); err != nil {
		return Record{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return Record{}, errUnavailable
	}
	raw, ok := s.records[uid]
	if !ok {
		return Record{}, os.ErrNotExist
	}
	var record Record
	err := json.Unmarshal(raw, &record)
	return record, err
}

func (s *memoryStore) GetInNamespace(ctx context.Context, namespace, uid string) (Record, error) {
	record, err := s.Get(ctx, uid)
	if err == nil && record.Namespace != namespace {
		return Record{}, os.ErrNotExist
	}
	return record, err
}

func (s *memoryStore) Records(ctx context.Context) ([]Record, error) {
	return s.RecordsInNamespace(ctx, "")
}

func (s *memoryStore) RecordsInNamespace(ctx context.Context, namespace string) ([]Record, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, errUnavailable
	}
	keys := make([]string, 0, len(s.records))
	for key := range s.records {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	var records []Record
	for _, key := range keys {
		var record Record
		if err := json.Unmarshal(s.records[key], &record); err != nil {
			return nil, err
		}
		if namespace == "" || record.Namespace == namespace {
			records = append(records, record)
		}
	}
	return records, nil
}

func (s *memoryStore) Update(ctx context.Context, uid string, change func(*Record) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errUnavailable
	}
	record := Record{Version: _recordVersion, UID: uid, Actions: make(map[string]Action), Notes: make(map[string]string)}
	if raw, ok := s.records[uid]; ok {
		if err := json.Unmarshal(raw, &record); err != nil {
			return err
		}
	}
	if err := change(&record); err != nil {
		return err
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return err
	}
	if len(raw) > _maxRecordBytes {
		return errConflict
	}
	s.records[uid] = raw
	return nil
}

func (s *memoryStore) Purge(ctx context.Context, uid string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errUnavailable
	}
	raw, ok := s.records[uid]
	if !ok {
		return os.ErrNotExist
	}
	var record Record
	if err := json.Unmarshal(raw, &record); err != nil {
		return err
	}
	if !closedForPurge(record) || record.Work != nil {
		return errors.New("record cannot be purged")
	}
	delete(s.records, uid)
	return nil
}

func (s *memoryStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}
