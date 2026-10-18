// Package repository prepares a pinned checkout without replacing existing work.
package repository

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

const (
	ProjectDirectory = "project"
	PreparationFile  = "preparation.json"
	_stagingFile     = "staging.json"
	_credentialsEnv  = "WORKSPACE_BOOTSTRAP_CREDENTIAL_DIRECTORY"
	_urlEnv          = "WORKSPACE_BOOTSTRAP_REPOSITORY_URL"
)

var (
	_commit     = regexp.MustCompile(`^[a-f0-9]{40}$`)
	_sessionUID = regexp.MustCompile(`^[a-zA-Z0-9-]{1,128}$`)
)

// Config contains public configuration and protected file paths, never tokens.
type Config struct {
	StorageRoot, SessionUID, URL, Commit, CredentialDirectory, AskPassExecutable string
}

// Provenance remains outside the project subtree mounted into the executor.
type Provenance struct {
	Version          int    `json:"version"`
	SessionUID       string `json:"session_uid"`
	RepositoryDigest string `json:"repository_digest"`
	InitialCommit    string `json:"initial_commit"`
}

// Validate restricts the first repository contract to credential-free HTTPS and SHA-1 commits.
func Validate(rawURL, commit string) error {
	u, err := url.Parse(rawURL)
	if err != nil || len(rawURL) > 2048 || u.Scheme != "https" || u.Hostname() == "" ||
		u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" ||
		u.Port() != "" || u.Host != strings.ToLower(u.Host) || u.Path == "" || u.Path == "/" ||
		u.RawPath != "" || path.Clean(u.Path) != u.Path || strings.ContainsAny(rawURL, "\\\r\n\t ") ||
		!_commit.MatchString(commit) {
		return errors.New("repository requires a credential-free HTTPS URL and a full lowercase commit")
	}
	return nil
}

// Digest excludes credential source versions so rotation never resets completed work.
func Digest(rawURL, commit string) string {
	raw, _ := json.Marshal([2]string{rawURL, commit})
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

type gitRunner func(context.Context, string, []string, []string) ([]byte, error)

// Prepare atomically installs the initial checkout or verifies existing provenance.
func Prepare(ctx context.Context, config Config) error {
	return prepare(ctx, config, runGit)
}

func prepare(ctx context.Context, config Config, git gitRunner) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := Validate(config.URL, config.Commit); err != nil {
		return err
	}
	if !_sessionUID.MatchString(config.SessionUID) || !filepath.IsAbs(config.StorageRoot) || filepath.Clean(config.StorageRoot) != config.StorageRoot {
		return errors.New("repository preparation identity or storage root is invalid")
	}
	info, err := os.Lstat(config.StorageRoot)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("repository storage root must be an ordinary directory")
	}
	root, err := os.OpenRoot(config.StorageRoot)
	if err != nil {
		return errors.New("repository storage root is unavailable")
	}
	defer func() { _ = root.Close() }()
	identity := Provenance{Version: 1, SessionUID: config.SessionUID, RepositoryDigest: Digest(config.URL, config.Commit), InitialCommit: config.Commit}
	staging := ".prepare-" + config.SessionUID
	if err := validateEntries(root, staging); err != nil {
		return err
	}
	completed, exists, err := readProvenance(root, PreparationFile)
	if err != nil {
		return err
	}
	if exists && completed != identity {
		return errors.New("repository preparation provenance does not match the Session")
	}
	claimed, hasClaim, err := readProvenance(root, _stagingFile)
	if err != nil || hasClaim && claimed != identity {
		return errors.New("repository staging identity is incompatible; audit required")
	}
	temporary, err := os.MkdirTemp("", "workspace-bootstrap-")
	if err != nil {
		return errors.New("repository temporary directory is unavailable")
	}
	defer func() { _ = os.RemoveAll(temporary) }()
	env := gitEnvironment(config, temporary)
	if exists {
		return reuseProject(ctx, root, config, git, env, hasClaim)
	}
	return initializeProject(ctx, root, config, git, env, hasClaim)
}

func reuseProject(ctx context.Context, root *os.Root, config Config, git gitRunner, env []string, hasClaim bool) error {
	if hasClaim {
		// Completion can be durable before removing its staging acknowledgment.
		if _, err := root.Lstat(".prepare-" + config.SessionUID); !errors.Is(err, os.ErrNotExist) {
			return errors.New("completed repository has ambiguous staging; audit required")
		}
		if err := verifyProject(ctx, root, config, git, env, true); err != nil {
			return err
		}
		if err := root.Remove(_stagingFile); err != nil {
			return errors.New("completed repository staging acknowledgment could not be removed")
		}
		return syncRoot(root)
	}
	return verifyProject(ctx, root, config, git, env, false)
}

func initializeProject(ctx context.Context, root *os.Root, config Config, git gitRunner, env []string, hasClaim bool) error {
	identity := Provenance{Version: 1, SessionUID: config.SessionUID, RepositoryDigest: Digest(config.URL, config.Commit), InitialCommit: config.Commit}
	staging := ".prepare-" + config.SessionUID
	if projectInfo, statErr := root.Lstat(ProjectDirectory); statErr == nil {
		if !hasClaim || !projectInfo.IsDir() || projectInfo.Mode()&os.ModeSymlink != 0 {
			return errors.New("existing repository is not a claimed checkout; audit required")
		}
		// A crash after the atomic directory rename but before acknowledgment is recoverable.
		if err := verifyProject(ctx, root, config, git, env, true); err != nil {
			return err
		}
		return finish(root, identity)
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return errors.New("existing repository cannot be inspected")
	}
	if stagingInfo, statErr := root.Lstat(staging); statErr == nil {
		if !hasClaim || !stagingInfo.IsDir() || stagingInfo.Mode()&os.ModeSymlink != 0 {
			return errors.New("repository staging is not owned by this Session")
		}
		if err := root.RemoveAll(staging); err != nil {
			return errors.New("interrupted repository staging could not be removed")
		}
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return errors.New("repository staging could not be inspected")
	}
	if !hasClaim {
		if err := writeProvenance(root, _stagingFile, identity); err != nil {
			return err
		}
	}
	if err := root.Mkdir(staging, 0o750); err != nil {
		return errors.New("repository staging could not be created")
	}
	stagePath := filepath.Join(config.StorageRoot, staging)
	for _, args := range [][]string{
		{"init", "--initial-branch=bootstrap", "."},
		{"remote", "add", "origin", config.URL},
		{"fetch", "--depth=1", "--no-tags", "origin", config.Commit},
		{"checkout", "--detach", config.Commit},
	} {
		if _, err := git(ctx, stagePath, args, env); err != nil {
			return gitError(ctx, "repository checkout failed; retry preparation or audit its source")
		}
	}
	actual, err := git(ctx, stagePath, []string{"rev-parse", "HEAD"}, env)
	if err != nil || strings.TrimSpace(string(actual)) != config.Commit {
		return errors.New("repository checkout did not resolve to the pinned commit")
	}
	if _, err := root.Lstat(ProjectDirectory); !errors.Is(err, os.ErrNotExist) {
		return errors.New("repository destination appeared during preparation; audit required")
	}
	if err := root.Rename(staging, ProjectDirectory); err != nil {
		return errors.New("verified repository checkout could not be installed")
	}
	if err := syncRoot(root); err != nil {
		return err
	}
	return finish(root, identity)
}

func validateEntries(root *os.Root, staging string) error {
	directory, err := root.Open(".")
	if err != nil {
		return errors.New("repository storage contents are unavailable")
	}
	defer func() { _ = directory.Close() }()
	entries, err := directory.ReadDir(-1)
	if err != nil {
		return errors.New("repository storage contents are unavailable")
	}
	for _, entry := range entries {
		switch entry.Name() {
		case ProjectDirectory, PreparationFile, _stagingFile, staging, "lost+found":
			if entry.Type()&os.ModeSymlink != 0 {
				return errors.New("repository storage contains a symbolic provenance path")
			}
		default:
			return errors.New("repository storage contains foreign contents; audit required")
		}
	}
	return nil
}

func readProvenance(root *os.Root, name string) (Provenance, bool, error) {
	info, err := root.Lstat(name)
	if errors.Is(err, os.ErrNotExist) {
		return Provenance{}, false, nil
	}
	if err != nil || !info.Mode().IsRegular() || info.Size() > 4096 {
		return Provenance{}, false, errors.New("repository provenance is unsafe or unreadable")
	}
	file, err := root.Open(name)
	if err != nil {
		return Provenance{}, false, errors.New("repository provenance is unavailable")
	}
	defer func() { _ = file.Close() }()
	decoder := json.NewDecoder(io.LimitReader(file, 4097))
	decoder.DisallowUnknownFields()
	var result Provenance
	if decoder.Decode(&result) != nil || decoder.Decode(new(any)) != io.EOF {
		return Provenance{}, false, errors.New("repository provenance is invalid")
	}
	return result, true, nil
}

func writeProvenance(root *os.Root, name string, identity Provenance) error {
	file, err := root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return errors.New("repository provenance could not be claimed")
	}
	encodeErr := json.NewEncoder(file).Encode(identity)
	syncErr := file.Sync()
	closeErr := file.Close()
	if errors.Join(encodeErr, syncErr, closeErr) != nil {
		return errors.New("repository provenance could not be persisted; audit required")
	}
	return syncRoot(root)
}

func finish(root *os.Root, identity Provenance) error {
	if err := writeProvenance(root, PreparationFile, identity); err != nil {
		return err
	}
	if err := root.Remove(_stagingFile); err != nil {
		return errors.New("repository completion has unacknowledged staging; audit required")
	}
	return syncRoot(root)
}

func syncRoot(root *os.Root) error {
	directory, err := root.Open(".")
	if err != nil {
		return errors.New("repository storage could not be synchronized")
	}
	defer func() { _ = directory.Close() }()
	if directory.Sync() != nil {
		return errors.New("repository storage could not be synchronized")
	}
	return nil
}

func verifyProject(ctx context.Context, root *os.Root, config Config, git gitRunner, env []string, initial bool) error {
	for _, name := range []string{ProjectDirectory, ProjectDirectory + "/.git"} {
		info, err := root.Lstat(name)
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return errors.New("acknowledged repository is missing or unsafe; audit required")
		}
	}
	project := filepath.Join(config.StorageRoot, ProjectDirectory)
	object, err := git(ctx, project, []string{"cat-file", "-t", config.Commit}, env)
	if err != nil || strings.TrimSpace(string(object)) != "commit" {
		return errors.New("acknowledged repository lost its pinned object; audit required")
	}
	if initial {
		actual, err := git(ctx, project, []string{"rev-parse", "HEAD"}, env)
		if err != nil || strings.TrimSpace(string(actual)) != config.Commit {
			return errors.New("unacknowledged repository differs from its initial commit")
		}
	}
	return nil
}

func gitEnvironment(config Config, temporary string) []string {
	env := []string{"PATH=/usr/bin:/bin", "HOME=" + temporary, "XDG_CONFIG_HOME=" + temporary,
		"LANG=C", "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_TERMINAL_PROMPT=0", "GIT_NO_LAZY_FETCH=1"}
	if config.CredentialDirectory != "" {
		env = append(env, "GIT_ASKPASS="+config.AskPassExecutable, _credentialsEnv+"="+config.CredentialDirectory, _urlEnv+"="+config.URL)
	}
	return env
}

func runGit(ctx context.Context, directory string, args, env []string) ([]byte, error) {
	options := []string{"-c", "protocol.allow=never", "-c", "protocol.https.allow=always", "-c", "http.followRedirects=false",
		"-c", "credential.helper=", "-c", "core.hooksPath=/dev/null", "-c", "init.templateDir=",
		"-c", "submodule.recurse=false", "-c", "fetch.recurseSubmodules=false", "-c", "filter.lfs.smudge=", "-c", "filter.lfs.process="}
	arguments := make([]string, 0, len(options)+len(args))
	arguments = append(arguments, options...)
	arguments = append(arguments, args...)
	command := exec.CommandContext(ctx, "/usr/bin/git", arguments...)
	command.Dir, command.Env = directory, env
	command.WaitDelay = 2 * time.Second
	configureProcess(command)
	// Never expose Git errors, which may contain protected authentication material.
	command.Stderr = io.Discard
	output := &limitedOutput{}
	command.Stdout = output
	if err := command.Run(); err != nil {
		return nil, gitError(ctx, "repository Git operation failed")
	}
	return output.data, nil
}

type limitedOutput struct{ data []byte }

func (w *limitedOutput) Write(data []byte) (int, error) {
	if len(w.data) < 4096 {
		w.data = append(w.data, data[:min(len(data), 4096-len(w.data))]...)
	}
	return len(data), nil
}

func gitError(ctx context.Context, message string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return errors.New(message)
}
