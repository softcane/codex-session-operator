package repository

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

const _testURL = "https://github.com/fixture/project.git"

func localGit(t *testing.T, directory string, args ...string) string {
	t.Helper()
	command := exec.CommandContext(t.Context(), "/usr/bin/git", args...)
	command.Dir = directory
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("local Git fixture failed: %v: %s", err, output)
	}
	return strings.TrimSpace(string(output))
}

func fixture(t *testing.T) (Config, gitRunner) {
	t.Helper()
	source := t.TempDir()
	localGit(t, source, "init", "--initial-branch=fixture")
	for _, name := range []string{"tracked.txt", "deleted.txt", "committed.txt"} {
		if err := os.WriteFile(filepath.Join(source, name), []byte("initial\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	localGit(t, source, "add", ".")
	localGit(t, source, "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "-m", "fixture")
	config := Config{StorageRoot: t.TempDir(), SessionUID: "session-test", URL: _testURL, Commit: localGit(t, source, "rev-parse", "HEAD")}
	runner := func(ctx context.Context, directory string, args, env []string) ([]byte, error) {
		if args[0] != "fetch" {
			return runGit(ctx, directory, args, env)
		}
		// Only the test runner redirects fetch to a local repository. Production
		// still validates HTTPS and disables the file protocol without an exception.
		command := exec.CommandContext(ctx, "/usr/bin/git", "-c", "protocol.file.allow=always", "fetch", "--depth=1", source, config.Commit)
		command.Dir, command.Env = directory, env
		return command.Output()
	}
	return config, runner
}

func TestPreparePreservesDirtyDeletedUntrackedFilesAndLaterCommits(t *testing.T) {
	config, runner := fixture(t)
	if err := prepare(t.Context(), config, runner); err != nil {
		t.Fatal(err)
	}
	project := filepath.Join(config.StorageRoot, ProjectDirectory)
	if got := localGit(t, project, "rev-parse", "HEAD"); got != config.Commit {
		t.Fatalf("initial commit = %s", got)
	}
	for _, file := range []string{"tracked.txt", "untracked.txt", "committed.txt"} {
		if err := os.WriteFile(filepath.Join(project, file), []byte("agent work\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	localGit(t, project, "add", "committed.txt")
	localGit(t, project, "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "-m", "agent work")
	newHead := localGit(t, project, "rev-parse", "HEAD")
	if err := os.Remove(filepath.Join(project, "deleted.txt")); err != nil {
		t.Fatal(err)
	}
	beforeStatus := localGit(t, project, "status", "--porcelain")
	readOnlyRunner := func(ctx context.Context, dir string, args, env []string) ([]byte, error) {
		if !reflect.DeepEqual(args, []string{"cat-file", "-t", config.Commit}) {
			t.Fatalf("replacement performed a non-verification Git operation: %v", args)
		}
		return runner(ctx, dir, args, env)
	}
	config.CredentialDirectory = filepath.Join(t.TempDir(), "retired-credential")
	if err := prepare(t.Context(), config, readOnlyRunner); err != nil {
		t.Fatal(err)
	}
	if got := localGit(t, project, "rev-parse", "HEAD"); got != newHead || got == config.Commit {
		t.Fatal("replacement reset an agent commit")
	}
	if got := localGit(t, project, "status", "--porcelain"); got != beforeStatus {
		t.Fatal("replacement changed dirty, deleted or untracked files")
	}
	data, err := os.ReadFile(filepath.Join(project, "tracked.txt"))
	if err != nil || string(data) != "agent work\n" {
		t.Fatal("replacement changed agent content", err)
	}
	if got := localGit(t, project, "remote", "get-url", "origin"); got != _testURL {
		t.Fatal("origin did not retain its credential-free URL")
	}
}

func TestInterruptedOwnedStagingRetriesWithoutExposingErrors(t *testing.T) {
	config, runner := fixture(t)
	private := "private-credential-from-git-stderr"
	failing := func(ctx context.Context, dir string, args, env []string) ([]byte, error) {
		if strings.Contains(strings.Join(append(args, env...), " "), private) {
			t.Fatal("credential entered command configuration")
		}
		if args[0] == "fetch" {
			return nil, errors.New(private)
		}
		return runner(ctx, dir, args, env)
	}
	if err := prepare(t.Context(), config, failing); err == nil || strings.Contains(err.Error(), private) {
		t.Fatal("Git failure was accepted or leaked protected details")
	}
	if _, err := os.Stat(filepath.Join(config.StorageRoot, PreparationFile)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("failed checkout was acknowledged")
	}
	if err := prepare(t.Context(), config, runner); err != nil {
		t.Fatal("owned interrupted staging did not recover", err)
	}
}

func TestPreparationRecoversAtomicRenameAndAcknowledgmentGaps(t *testing.T) {
	for _, completionPresent := range []bool{false, true} {
		t.Run(map[bool]string{false: "directory renamed", true: "completion persisted"}[completionPresent], func(t *testing.T) {
			config, runner := fixture(t)
			if err := prepare(t.Context(), config, runner); err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(filepath.Join(config.StorageRoot, PreparationFile))
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(config.StorageRoot, _stagingFile), data, 0o600); err != nil {
				t.Fatal(err)
			}
			if !completionPresent {
				if err := os.Remove(filepath.Join(config.StorageRoot, PreparationFile)); err != nil {
					t.Fatal(err)
				}
			}
			if err := prepare(t.Context(), config, runner); err != nil {
				t.Fatal("coherent installed checkout could not recover", err)
			}
			if _, err := os.Stat(filepath.Join(config.StorageRoot, _stagingFile)); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("recovery retained its incomplete acknowledgment")
			}
		})
	}
}

func TestPrepareRejectsForeignAndSymbolicContentsWithoutMutation(t *testing.T) {
	for _, name := range []string{"foreign file", "foreign project", "symbolic project", "symbolic provenance", "symbolic staging", "different Session"} {
		t.Run(name, func(t *testing.T) {
			config, runner := fixture(t)
			outside := t.TempDir()
			sentinel := filepath.Join(outside, "sentinel")
			if err := os.WriteFile(sentinel, []byte("preserve"), 0o600); err != nil {
				t.Fatal(err)
			}
			var err error
			switch name {
			case "foreign file":
				err = os.WriteFile(filepath.Join(config.StorageRoot, "foreign"), []byte("preserve"), 0o600)
			case "foreign project":
				err = os.Mkdir(filepath.Join(config.StorageRoot, ProjectDirectory), 0o750)
			case "symbolic project":
				err = os.Symlink(outside, filepath.Join(config.StorageRoot, ProjectDirectory))
			case "symbolic provenance":
				err = os.Symlink(sentinel, filepath.Join(config.StorageRoot, PreparationFile))
			case "symbolic staging":
				err = os.Symlink(outside, filepath.Join(config.StorageRoot, ".prepare-"+config.SessionUID))
			case "different Session":
				err = prepare(t.Context(), config, runner)
				config.SessionUID = "foreign-session"
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := prepare(t.Context(), config, runner); err == nil {
				t.Fatal("foreign or symbolic data was accepted")
			}
			data, err := os.ReadFile(sentinel)
			if err != nil || string(data) != "preserve" {
				t.Fatal("preparation changed unrelated data", err)
			}
		})
	}
}

func TestRepositoryURLAndCommitValidation(t *testing.T) {
	sha := strings.Repeat("a", 40)
	for _, tt := range []struct {
		name, url, commit string
		valid             bool
	}{
		{name: "HTTPS pinned", url: _testURL, commit: sha, valid: true},
		{name: "HTTP", url: "http://github.com/project", commit: sha},
		{name: "URL credentials", url: "https://token@github.com/project", commit: sha},
		{name: "query", url: _testURL + "?token=secret", commit: sha},
		{name: "fragment", url: _testURL + "#secret", commit: sha},
		{name: "moving ref", url: _testURL, commit: "main"},
		{name: "short SHA", url: _testURL, commit: "abcd"},
		{name: "traversal", url: "https://github.com/fixture/../other", commit: sha},
		{name: "encoded traversal", url: "https://github.com/%2e%2e/other", commit: sha},
		{name: "option injection", url: "--upload-pack=bad", commit: sha},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if (Validate(tt.url, tt.commit) == nil) != tt.valid {
				t.Fatal("repository validation mismatch")
			}
		})
	}
}

func TestPrepareCancellationAndGitEnvironmentIsolation(t *testing.T) {
	config, runner := fixture(t)
	t.Setenv("GIT_CONFIG_COUNT", "1")
	t.Setenv("OPENAI_API_KEY", "application-secret-must-not-be-inherited")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := prepare(ctx, config, runner); !errors.Is(err, context.Canceled) {
		t.Fatal("preparation did not preserve cancellation", err)
	}
	for _, entry := range gitEnvironment(config, t.TempDir()) {
		if strings.Contains(entry, "application-secret") || strings.HasPrefix(entry, "GIT_CONFIG_COUNT=") {
			t.Fatal("Git inherited ambient credentials or configuration")
		}
	}
}
