package gatewayclient

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestTaskStartAndResumeDurableAcceptance(t *testing.T) {
	posts := 0
	cli := newTestCLI(t, func(w http.ResponseWriter, req *http.Request) {
		if strings.HasSuffix(req.URL.Path, "/identity") {
			writeTestJSON(t, w, http.StatusOK, testIdentity(req, _testSubject))
			return
		}
		if req.Method != http.MethodPost || req.URL.Path != "/v1/namespaces/team/tasks" {
			t.Errorf("unexpected request %s %s", req.Method, req.URL.Path)
		}
		posts++
		writeTestJSON(t, w, http.StatusAccepted, map[string]any{_testTaskIDField: _testTaskID, _testTaskURLField: _testTaskURL, _testTaskEventField: "event-first", _acceptanceField: _acceptanceCommitted, _testTaskReceivedField: _testTaskReceivedAt})
	})
	code, out, diagnostic := cli.run(t, "diagnose\r\n", _commandTask, "start", "--workflow", "go-bugfix", _textFileFlag, "-", _operationFlag, _commandTaskStart)
	if code != 0 || !strings.Contains(out, `"task_id":"task-first"`) {
		t.Fatalf("start: %d %s %s", code, out, diagnostic)
	}
	code, out, diagnostic = cli.run(t, "", "resume", _commandTaskStart)
	if code != 0 || posts != 1 || !strings.Contains(out, `"saved_receipt":true`) {
		t.Fatalf("resume: %d posts=%d %s %s", code, posts, out, diagnostic)
	}
}

func TestTaskRoutes(t *testing.T) {
	tests := []struct {
		name               string
		args               []string
		method, path, body string
	}{
		{name: _commandWorkflows, args: []string{_commandWorkflows, _commandList}, method: http.MethodGet, path: "/workflows"},
		{name: "workflow", args: []string{_commandWorkflows, "show", "go-bugfix"}, method: http.MethodGet, path: "/workflows/go-bugfix"},
		{name: _commandTask, args: []string{_commandTask, "read", _testTaskID}, method: http.MethodGet, path: "/tasks/task-first"},
		{name: "result", args: []string{_commandTask, "result", _testTaskID, "result-first"}, method: http.MethodGet, path: "/tasks/task-first/results/result-first"},
		{name: _commandContinue, args: []string{_commandTask, _commandContinue, _testTaskID, _textFileFlag, "-", _operationFlag, "next"}, method: http.MethodPost, path: "/tasks/task-first/messages", body: `{"text":"next\n"}`},
		{name: _commandStop, args: []string{_commandTask, _commandStop, _testTaskID, _operationFlag, _commandStop}, method: http.MethodPost, path: "/tasks/task-first/stop", body: `{}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cli := newTestCLI(t, func(w http.ResponseWriter, req *http.Request) {
				if strings.HasSuffix(req.URL.Path, "/identity") {
					writeTestJSON(t, w, http.StatusOK, testIdentity(req, _testSubject))
					return
				}
				if req.Method != tt.method || req.URL.Path != "/v1/namespaces/team"+tt.path {
					t.Errorf("unexpected route %s %s", req.Method, req.URL.Path)
				}
				if tt.method == http.MethodPost {
					raw, err := io.ReadAll(req.Body)
					if err != nil {
						t.Error(err)
					}
					if string(raw) != tt.body || req.Header.Get("Idempotency-Key") == "" {
						t.Errorf("unexpected mutation %s", raw)
					}
					writeTestJSON(t, w, http.StatusAccepted, map[string]any{_testTaskIDField: _testTaskID, _testTaskURLField: _testTaskURL, _testTaskEventField: "event", _acceptanceField: _acceptanceCommitted, _testTaskReceivedField: _testTaskReceivedAt})
					return
				}
				writeTestJSON(t, w, http.StatusOK, map[string]any{"content": "exact bytes\n", "digest": "sha256:abc"})
			})
			code, out, diagnostic := cli.run(t, "next\r\n", tt.args...)
			if code != 0 {
				t.Fatalf("code %d: %s %s", code, out, diagnostic)
			}
			if tt.method == http.MethodGet && !strings.Contains(out, `"digest":"sha256:abc"`) {
				t.Fatalf("result fields lost: %s", out)
			}
		})
	}
}

func TestTaskRejectsMismatchedReceipt(t *testing.T) {
	cli := newTestCLI(t, func(w http.ResponseWriter, req *http.Request) {
		if strings.HasSuffix(req.URL.Path, "/identity") {
			writeTestJSON(t, w, http.StatusOK, testIdentity(req, _testSubject))
			return
		}
		writeTestJSON(t, w, http.StatusAccepted, map[string]any{_testTaskIDField: "task-other", _testTaskURLField: "/v1/namespaces/team/tasks/task-other", _testTaskEventField: "event", _acceptanceField: _acceptanceCommitted, _testTaskReceivedField: _testTaskReceivedAt})
	})
	code, _, _ := cli.run(t, "", _commandTask, _commandStop, _testTaskID, _operationFlag, _commandStop)
	if code != _exitAmbiguous {
		t.Fatalf("expected uncertain acceptance, got %d", code)
	}
}

const _testTaskReceivedAt = "2026-10-31T00:00:00Z"

const _testTaskEventField = "event_id"

const _testTaskReceivedField = "received_at"

const _testTaskID = "task-first"

const (
	_testTaskIDField  = "task_id"
	_testTaskURLField = "task_url"
	_testTaskURL      = "/v1/namespaces/team/tasks/task-first"
)

func TestTaskResultExportExactBytes(t *testing.T) {
	raw := "{\n  \"patch\": \"diff\\r\\n\", \"note\": \"å\"\n}\n"
	sum := sha256.Sum256([]byte(raw))
	cli := newTestCLI(t, func(w http.ResponseWriter, req *http.Request) {
		writeTestJSON(t, w, http.StatusOK, map[string]any{"raw": raw, "sha256": hex.EncodeToString(sum[:])})
	})
	target := filepath.Join(t.TempDir(), "result.json")
	code, out, diagnostic := cli.run(t, "", _commandTask, "result", _testTaskID, "result-first", "--output", target)
	if code != 0 {
		t.Fatalf("export: %d %s %s", code, out, diagnostic)
	}
	got, err := os.ReadFile(target)
	if err != nil || string(got) != raw {
		t.Fatalf("bytes differ: %q %v", got, err)
	}
	info, err := os.Stat(target)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("file mode: %v %v", info, err)
	}
	code, _, _ = cli.run(t, "", _commandTask, "result", _testTaskID, "result-first", "--output", target)
	if code == 0 {
		t.Fatal("existing destination overwritten")
	}
	got, err = os.ReadFile(target)
	if err != nil || string(got) != raw {
		t.Fatalf("existing bytes changed: %q %v", got, err)
	}
}

func TestTaskResultExportRejectsTamperedDigest(t *testing.T) {
	cli := newTestCLI(t, func(w http.ResponseWriter, req *http.Request) {
		writeTestJSON(t, w, http.StatusOK, map[string]any{"raw": "{}", "sha256": strings.Repeat("0", 64)})
	})
	target := filepath.Join(t.TempDir(), "result.json")
	code, _, _ := cli.run(t, "", _commandTask, "result", _testTaskID, "result-first", "--output", target)
	if code == 0 {
		t.Fatal("tampered digest accepted")
	}
	if _, err := os.Stat(target); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("destination created: %v", err)
	}
}

func TestWorkflowPagination(t *testing.T) {
	cli := newTestCLI(t, func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != "/v1/namespaces/team/workflows" || req.URL.Query().Get("limit") != "7" || req.URL.Query().Get("after") != "opaque+/=" {
			t.Errorf("unexpected page request: %s", req.URL)
		}
		writeTestJSON(t, w, http.StatusOK, map[string]any{"workflows": []any{}, "more": true, "next_cursor": "next+/="})
	})
	code, out, diagnostic := cli.run(t, "", _commandWorkflows, _commandList, "--limit", "7", "--after", "opaque+/=")
	if code != 0 || !strings.Contains(out, `"next_cursor":"next+/="`) {
		t.Fatalf("page: %d %s %s", code, out, diagnostic)
	}
}

func TestWorkflowPaginationRejectsInvalidLimit(t *testing.T) {
	for _, limit := range []string{"0", "101", "bad"} {
		t.Run(limit, func(t *testing.T) {
			cli := newTestCLI(t, func(w http.ResponseWriter, req *http.Request) { t.Error("invalid arguments made a request") })
			code, _, _ := cli.run(t, "", _commandWorkflows, _commandList, "--limit", limit)
			if code != _exitUsage {
				t.Fatalf("expected usage error, got %d", code)
			}
		})
	}
}
