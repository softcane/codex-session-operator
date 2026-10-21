package gatewayclient

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	_testNamespace      = "team"
	_testSubject        = "caller-a"
	_testBodyChange     = "body"
	_testOperation      = "same"
	_testInvalidProfile = "github:payments"
)

const (
	_testConversationID = "conv-first"
	_testTargetChange   = "target"
	_testSubjectChange  = "subject"
)

const _testBeforeReceipt = "before-receipt"

const (
	_testSourceField   = "source"
	_testProfilesField = "profiles"
)

type testCLI struct {
	args  []string
	state string
}

func newTestCLI(t *testing.T, handler http.HandlerFunc) testCLI {
	t.Helper()
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)
	directory, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ca := filepath.Join(directory, "ca.pem")
	cert := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	if err := os.WriteFile(ca, cert, 0600); err != nil {
		t.Fatal(err)
	}
	state := filepath.Join(directory, "operations")
	return testCLI{args: []string{"--origin", server.URL, "--namespace", _testNamespace,
		"--ca-file", ca, "--state-dir", state}, state: state}
}

func (c testCLI) run(t *testing.T, input string, args ...string) (int, string, string) {
	t.Helper()
	var out, diagnostic bytes.Buffer
	code := Run(t.Context(), append(append([]string{}, c.args...), args...), strings.NewReader(input), &out, &diagnostic)
	return code, out.String(), diagnostic.String()
}

func writeTestJSON(t *testing.T, w http.ResponseWriter, status int, value any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		t.Error(err)
	}
}

func testIdentity(req *http.Request, subject string) map[string]string {
	return map[string]string{_callerSubjectField: subject, _namespaceField: strings.Split(req.URL.Path, "/")[3]}
}

func testReceipt(id string, attempt int) receipt {
	return receipt{ConversationID: id, ConversationURL: "/v1/namespaces/team/conversations/" + id,
		RouteID: "investigation", Attempt: attempt, EventID: "event-1", Acceptance: "committed"}
}

func testDetail(attempt int, turns string) map[string]any {
	id := _testConversationID
	return map[string]any{"summary": map[string]any{"conversation_id": id,
		"conversation_url": "/v1/namespaces/team/conversations/" + id, "attempt": attempt,
		_stateField: "open", "active": true, "phase": "Ready"},
		"history": map[string]any{"id": "remote-1", "turns": json.RawMessage(turns)}}
}

func TestRunLostAcceptanceResumesExactRequest(t *testing.T) {
	var cli testCLI
	var mu sync.Mutex
	keys := make([]string, 0)
	bodies := make([]string, 0)
	cli = newTestCLI(t, func(w http.ResponseWriter, req *http.Request) {
		if req.Header.Get("Authorization") != "" {
			t.Error("CLI unexpectedly supplied gateway authentication")
		}
		if req.Method == http.MethodGet {
			writeTestJSON(t, w, 200, testIdentity(req, _testSubject))
			return
		}
		raw, err := io.ReadAll(req.Body)
		if err != nil {
			t.Error(err)
			return
		}
		stored, err := os.ReadFile(filepath.Join(cli.state, "stable.json"))
		if err != nil || !bytes.Contains(stored, []byte(_testSubject)) {
			t.Error("request sent before operation persistence")
		}
		if bytes.Contains(stored, []byte("private-test-token")) {
			t.Error("credential retained in operation journal")
		}
		mu.Lock()
		keys, bodies = append(keys, req.Header.Get("Idempotency-Key")), append(bodies, string(raw))
		count := len(keys)
		mu.Unlock()
		if count == 1 {
			hijacker, ok := w.(http.Hijacker)
			if !ok {
				t.Error("no hijacker")
				return
			}
			connection, _, err := hijacker.Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			if err := connection.Close(); err != nil {
				t.Error(err)
			}
			return
		}
		writeTestJSON(t, w, 202, testReceipt(_testConversationID, 1))
	})
	code, output, diagnostic := cli.run(t, "first\r\nmessage", _commandStart, "--profile", "investigation",
		"--text-file", "-", "--operation", "stable")
	if code != _exitAmbiguous || output != "" || !strings.Contains(diagnostic, "stable") {
		t.Fatalf("lost acceptance: code=%d output=%s diagnostic=%s", code, output, diagnostic)
	}
	code, output, diagnostic = cli.run(t, "", "resume", "stable")
	if code != 0 || !strings.Contains(output, `"state":"accepted"`) || diagnostic != "" {
		t.Fatalf("resume: code=%d output=%s diagnostic=%s", code, output, diagnostic)
	}
	mu.Lock()
	if len(keys) != 2 || keys[0] != "stable" || keys[1] != keys[0] || bodies[0] != bodies[1] ||
		!strings.Contains(bodies[0], `first\nmessage`) {
		t.Errorf("request identity changed: keys=%v bodies=%v", keys, bodies)
	}
	mu.Unlock()
	if _, err := os.Stat(filepath.Join(cli.state, "stable.receipt.json")); err != nil {
		t.Fatal(err)
	}
	code, output, _ = cli.run(t, "", "resume", "stable")
	if code != 0 || !strings.Contains(output, `"saved_receipt":true`) {
		t.Fatalf("persisted receipt: %d %s", code, output)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(keys) != 2 {
		t.Fatal("resume resubmitted after durable receipt")
	}
}

func TestRunReceiptMustPersistBeforeSuccess(t *testing.T) {
	var cli testCLI
	var mu sync.Mutex
	posts := 0
	cli = newTestCLI(t, func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodGet {
			writeTestJSON(t, w, 200, testIdentity(req, _testSubject))
			return
		}
		mu.Lock()
		posts++
		count := posts
		mu.Unlock()
		if count == 1 {
			if err := os.Mkdir(filepath.Join(cli.state, "persist.receipt.json"), 0700); err != nil {
				t.Error(err)
			}
		}
		writeTestJSON(t, w, 202, testReceipt(_testConversationID, 1))
	})
	code, output, diagnostic := cli.run(t, "task", _commandStart, "--profile", "investigation", "--text-file", "-", "--operation", "persist")
	if code != _exitAmbiguous || output != "" || !strings.Contains(diagnostic, "persistence failed") {
		t.Fatalf("unpersisted receipt: %d %s %s", code, output, diagnostic)
	}
	if err := os.Remove(filepath.Join(cli.state, "persist.receipt.json")); err != nil {
		t.Fatal(err)
	}
	code, output, _ = cli.run(t, "", "resume", "persist")
	if code != 0 || !strings.Contains(output, _stateAccepted) {
		t.Fatalf("resume: %d %s", code, output)
	}
	mu.Lock()
	defer mu.Unlock()
	if posts != 2 {
		t.Fatalf("posts=%d", posts)
	}
}

func TestRunRefusesChangedOperationIdentity(t *testing.T) {
	tests := []struct{ name, change string }{
		{name: _testBodyChange, change: _testBodyChange}, {name: _testTargetChange, change: _testTargetChange},
		{name: _testSubjectChange, change: _testSubjectChange}, {name: _namespaceField, change: _namespaceField},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var mu sync.Mutex
			subject := _testSubject
			posts := 0
			cli := newTestCLI(t, func(w http.ResponseWriter, req *http.Request) {
				if strings.HasSuffix(req.URL.Path, "/identity") {
					mu.Lock()
					identity := subject
					mu.Unlock()
					writeTestJSON(t, w, 200, testIdentity(req, identity))
					return
				}
				if req.Method == http.MethodGet {
					writeTestJSON(t, w, 200, testDetail(1, `[]`))
					return
				}
				mu.Lock()
				posts++
				mu.Unlock()
				writeTestJSON(t, w, 202, testReceipt(_testConversationID, 1))
			})
			code, _, diagnostic := cli.run(t, "original", "continue", _testConversationID, "--text-file", "-", "--operation", _testOperation)
			if code != 0 {
				t.Fatalf("initial: %d %s", code, diagnostic)
			}
			input, target := "original", _testConversationID
			switch tt.change {
			case _testBodyChange:
				input = "changed"
			case _testTargetChange:
				target = "conv-second"
			case _testSubjectChange:
				mu.Lock()
				subject = "caller-b"
				mu.Unlock()
			case _namespaceField:
				for i, value := range cli.args {
					if value == _testNamespace {
						cli.args[i] = "other"
					}
				}
			}
			code, output, diagnostic := cli.run(t, input, "continue", target, "--text-file", "-", "--operation", _testOperation)
			if code != _exitFailure || output != "" || !strings.Contains(diagnostic, "conflicts") {
				t.Fatalf("changed identity: %d %s %s", code, output, diagnostic)
			}
			mu.Lock()
			defer mu.Unlock()
			if posts != 1 {
				t.Fatalf("conflict sent another POST: %d", posts)
			}
		})
	}
}

func TestRunExpiredTokenKeepsOperation(t *testing.T) {
	var mu sync.Mutex
	expired := false
	posts := 0
	cli := newTestCLI(t, func(w http.ResponseWriter, req *http.Request) {
		mu.Lock()
		unauthorized := expired
		mu.Unlock()
		if unauthorized {
			http.Error(w, "private-test-token", http.StatusUnauthorized)
			return
		}
		if req.Method == http.MethodGet {
			writeTestJSON(t, w, 200, testIdentity(req, _testSubject))
			return
		}
		mu.Lock()
		posts++
		mu.Unlock()
		writeTestJSON(t, w, 202, testReceipt(_testConversationID, 1))
	})
	code, _, _ := cli.run(t, "task", _commandStart, "--profile", "investigation", "--text-file", "-", "--operation", "expiry")
	if code != 0 {
		t.Fatal(code)
	}
	mu.Lock()
	expired = true
	mu.Unlock()
	code, output, diagnostic := cli.run(t, "", "resume", "expiry")
	if code != _exitFailure || strings.Contains(output+diagnostic, "private-test-token") {
		t.Fatalf("expiry: %d %s %s", code, output, diagnostic)
	}
	if _, err := os.Stat(filepath.Join(cli.state, "expiry.json")); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if posts != 1 {
		t.Fatal("expired credentials resubmitted")
	}
}

func TestRunProfilePaginationAndNamespace(t *testing.T) {
	var mu sync.Mutex
	pages := make([]string, 0)
	cli := newTestCLI(t, func(w http.ResponseWriter, req *http.Request) {
		mu.Lock()
		pages = append(pages, req.URL.Query().Get("after"))
		mu.Unlock()
		more := req.URL.Query().Get("after") == ""
		id := "first"
		if !more {
			id = "beyond-first-page"
		}
		writeTestJSON(t, w, 200, map[string]any{_callerSubjectField: _testSubject, _testSourceField: "crd", "more": more,
			"next_cursor": "opaque+cursor", _testProfilesField: []map[string]any{{"id": id, _namespaceField: _testNamespace, "ready": true}}})
	})
	code, output, diagnostic := cli.run(t, "", _testProfilesField, "list", "--limit", "1")
	if code != 0 || !strings.Contains(output, "beyond-first-page") {
		t.Fatalf("pages: %d %s %s", code, output, diagnostic)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(pages) != 2 || pages[1] != "opaque+cursor" {
		t.Fatalf("cursor not retained: %v", pages)
	}
}

func TestValidProfileIdentifierMatchesKubernetesNames(t *testing.T) {
	for _, tt := range []struct {
		name  string
		valid bool
	}{
		{name: "payments", valid: true},
		{name: "github.payments", valid: true},
		{name: "github-payments-v2", valid: true},
		{name: ""},
		{name: _testInvalidProfile},
		{name: "GitHub-payments"},
		{name: "github_payments"},
		{name: "-payments"},
		{name: "payments-"},
		{name: strings.Repeat("a", 254)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := validProfileIdentifier(tt.name); got != tt.valid {
				t.Fatalf("validProfileIdentifier(%q) = %v, want %v", tt.name, got, tt.valid)
			}
		})
	}
}

func TestRunRejectsNonKubernetesProfileName(t *testing.T) {
	requests := 0
	cli := newTestCLI(t, func(http.ResponseWriter, *http.Request) { requests++ })
	for _, args := range [][]string{
		{_testProfilesField, "show", _testInvalidProfile},
		{_commandStart, "--profile", _testInvalidProfile, "--text-file", "-", "--operation", "invalid-profile"},
	} {
		code, _, _ := cli.run(t, "task", args...)
		if code != _exitUsage {
			t.Fatalf("invalid Profile name returned %d", code)
		}
	}
	if requests != 0 {
		t.Fatalf("invalid Profile name made %d requests", requests)
	}
}

func TestRunWrongNamespaceProfileAndURLs(t *testing.T) {
	cli := newTestCLI(t, func(w http.ResponseWriter, req *http.Request) {
		writeTestJSON(t, w, 200, map[string]any{"profile": map[string]any{"id": "test", _namespaceField: "other"}})
	})
	code, output, diagnostic := cli.run(t, "", _testProfilesField, "show", "test")
	if code != _exitFailure || output != "" || !strings.Contains(diagnostic, _namespaceField) {
		t.Fatalf("namespace: %d %s %s", code, output, diagnostic)
	}
	for _, target := range []string{"https://example.invalid/v1/namespaces/team/conversations/conv-first", "/v1/namespaces/other/conversations/conv-first", "conv-first/../close"} {
		code, _, _ := cli.run(t, "", "read", target)
		if code != _exitUsage {
			t.Fatalf("unsafe target %q accepted: %d", target, code)
		}
	}
}

func TestRunStartFreshRequiresCleanupAndFreezesAttempt(t *testing.T) {
	var mu sync.Mutex
	cleaned, posts := false, 0
	cli := newTestCLI(t, func(w http.ResponseWriter, req *http.Request) {
		if strings.HasSuffix(req.URL.Path, "/identity") {
			writeTestJSON(t, w, 200, testIdentity(req, _testSubject))
			return
		}
		if req.Method == http.MethodGet {
			detail := testDetail(1, `[{"id":"previous","status":"completed"}]`)
			mu.Lock()
			ready := cleaned
			mu.Unlock()
			detail["summary"].(map[string]any)[_stateField] = _stateClosed
			detail["summary"].(map[string]any)["cleaned"] = ready
			writeTestJSON(t, w, 200, detail)
			return
		}
		if req.URL.Path != "/v1/namespaces/team/conversations/conv-first/restart" {
			t.Error(req.URL.Path)
		}
		mu.Lock()
		posts++
		mu.Unlock()
		writeTestJSON(t, w, 202, testReceipt("conv-second", 2))
	})
	args := []string{_commandFresh, _testConversationID, "--text-file", "-", "--operation", "fresh"}
	code, _, _ := cli.run(t, "task", args...)
	if code != _exitFailure {
		t.Fatal("restart accepted without cleanup")
	}
	mu.Lock()
	cleaned = true
	mu.Unlock()
	code, output, diagnostic := cli.run(t, "task", args...)
	if code != 0 || !strings.Contains(output, "conv-second") || !strings.Contains(output, "new workspace") {
		t.Fatalf("fresh: %d %s %s", code, output, diagnostic)
	}
	j, err := openJournal(cli.state)
	if err != nil {
		t.Fatal(err)
	}
	defer j.close()
	op, err := j.load("fresh")
	if err != nil || op.Attempt != 1 || len(op.Baseline) != 1 {
		t.Fatalf("baseline not frozen: %+v %v", op, err)
	}
	mu.Lock()
	defer mu.Unlock()
	if posts != 1 {
		t.Fatalf("posts=%d", posts)
	}
}

func TestRunPreservesConversationOutput(t *testing.T) {
	cli := newTestCLI(t, func(w http.ResponseWriter, req *http.Request) {
		writeTestJSON(t, w, 200, map[string]string{"history": "ordinary task output"})
	})
	code, output, diagnostic := cli.run(t, "", "read", _testConversationID)
	if code != 0 || !strings.Contains(output, "ordinary task output") {
		t.Fatalf("conversation output changed: %d %s %s", code, output, diagnostic)
	}
}

func TestRunWatchDoesNotCountPriorRootsOrSubagents(t *testing.T) {
	var mu sync.Mutex
	watching := false
	cli := newTestCLI(t, func(w http.ResponseWriter, req *http.Request) {
		if strings.HasSuffix(req.URL.Path, "/identity") {
			writeTestJSON(t, w, 200, testIdentity(req, _testSubject))
			return
		}
		if req.Method == http.MethodPost {
			writeTestJSON(t, w, 202, testReceipt(_testConversationID, 1))
			return
		}
		mu.Lock()
		after := watching
		mu.Unlock()
		turns := `[{"id":"old-failed","status":"failed"},{"id":"old-success","status":"completed"},{"id":"steered","status":"in_progress"}]`
		if after {
			turns = `[{"id":"steered","status":"completed"},{"id":"old-success","status":"completed"},{"id":"old-failed","status":"failed"},{"id":"child","status":"completed","subagent_id":"sub-1"},{"id":"old-success","status":"completed"}]`
		}
		writeTestJSON(t, w, 200, testDetail(1, turns))
	})
	code, _, diagnostic := cli.run(t, "steering", "continue", _testConversationID, "--text-file", "-", "--operation", "steer")
	if code != 0 {
		t.Fatalf("continue: %d %s", code, diagnostic)
	}
	mu.Lock()
	watching = true
	mu.Unlock()
	code, output, diagnostic := cli.run(t, "", "watch", _testConversationID, "--operation", "steer", "--timeout", "40ms", "--poll", "5ms")
	if code != _exitPending || strings.Contains(output, `"state":"observed_root_outcome"`) || strings.Contains(output, `"id":"child"`) {
		t.Fatalf("old roots qualified: %d %s %s", code, output, diagnostic)
	}
	if _, err := os.Stat(filepath.Join(cli.state, "steer.json")); err != nil {
		t.Fatal(err)
	}
}

func TestRunWatchNewRootsRemainUncorrelated(t *testing.T) {
	var mu sync.Mutex
	watching := false
	posts := 0
	cli := newTestCLI(t, func(w http.ResponseWriter, req *http.Request) {
		if strings.HasSuffix(req.URL.Path, "/identity") {
			writeTestJSON(t, w, 200, testIdentity(req, _testSubject))
			return
		}
		if req.Method == http.MethodPost {
			mu.Lock()
			posts++
			mu.Unlock()
			writeTestJSON(t, w, 202, testReceipt(_testConversationID, 1))
			return
		}
		mu.Lock()
		after := watching
		mu.Unlock()
		turns := `[{"id":"old","status":"completed"}]`
		if after {
			turns = `[{"id":"concurrent-a","status":"completed"},{"id":"concurrent-b","status":"failed"},{"id":"old","status":"completed"}]`
		}
		writeTestJSON(t, w, 200, testDetail(1, turns))
	})
	code, _, _ := cli.run(t, "follow-up", "continue", _testConversationID, "--text-file", "-", "--operation", "follow")
	if code != 0 {
		t.Fatal(code)
	}
	mu.Lock()
	watching = true
	mu.Unlock()
	code, output, diagnostic := cli.run(t, "", "watch", _testConversationID, "--operation", "follow", "--timeout", "1s", "--poll", "5ms")
	if code != _exitAmbiguous || !strings.Contains(output, "concurrent-a") || !strings.Contains(output, "concurrent-b") || !strings.Contains(output, "cannot be attributed") {
		t.Fatalf("concurrent roots: %d %s %s", code, output, diagnostic)
	}
	mu.Lock()
	defer mu.Unlock()
	if posts != 1 {
		t.Fatal("watch resent input")
	}
}

func TestRunWatchSpecificRootAndCancellation(t *testing.T) {
	cli := newTestCLI(t, func(w http.ResponseWriter, req *http.Request) {
		writeTestJSON(t, w, 200, testDetail(1, `[{"id":"target","status":"failed"}]`))
	})
	code, output, _ := cli.run(t, "", "watch", _testConversationID, "--root-id", _testTargetChange)
	if code != _exitRootFailed || !strings.Contains(output, _testTargetChange) {
		t.Fatalf("specific root: %d %s", code, output)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	var out, diagnostic bytes.Buffer
	code = Run(ctx, append(cli.args, "watch", _testConversationID), strings.NewReader(""), &out, &diagnostic)
	if code != 130 {
		t.Fatalf("cancellation code=%d", code)
	}
}

func TestRunWatchRejectsAnotherAttempt(t *testing.T) {
	var mu sync.Mutex
	attempt := 1
	cli := newTestCLI(t, func(w http.ResponseWriter, req *http.Request) {
		if strings.HasSuffix(req.URL.Path, "/identity") {
			writeTestJSON(t, w, 200, testIdentity(req, _testSubject))
			return
		}
		if req.Method == http.MethodPost {
			writeTestJSON(t, w, 202, testReceipt(_testConversationID, 1))
			return
		}
		mu.Lock()
		selected := attempt
		mu.Unlock()
		writeTestJSON(t, w, 200, testDetail(selected, `[{"id":"root","status":"completed"}]`))
	})
	code, _, _ := cli.run(t, "task", "continue", _testConversationID, "--text-file", "-", "--operation", "attempt")
	if code != 0 {
		t.Fatal(code)
	}
	mu.Lock()
	attempt = 2
	mu.Unlock()
	code, output, diagnostic := cli.run(t, "", "watch", _testConversationID, "--operation", "attempt")
	if code != _exitAmbiguous || output != "" || !strings.Contains(diagnostic, "attempt differs") {
		t.Fatalf("another attempt observed: %d %s %s", code, output, diagnostic)
	}
}

func TestRunHTTPConversationURL(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Header.Get("Authorization") != "" {
			t.Error("HTTP request unexpectedly supplied caller token")
		}
		writeTestJSON(t, w, http.StatusOK, map[string]string{"id": _testConversationID})
	}))
	defer server.Close()
	cli := newTestCLI(t, func(http.ResponseWriter, *http.Request) { t.Error("unexpected TLS request") })
	cli.args[1] = server.URL
	// HTTP needs no CA file, including when reading an absolute conversation URL.
	code, output, diagnostic := cli.run(t, "", "read", server.URL+"/v1/namespaces/"+_testNamespace+"/conversations/"+_testConversationID)
	if code != 0 || !strings.Contains(output, _testConversationID) {
		t.Fatalf("HTTP read failed: %d %s %s", code, output, diagnostic)
	}
}

func TestRunUnsupportedScheme(t *testing.T) {
	cli := newTestCLI(t, func(w http.ResponseWriter, req *http.Request) { t.Error("unexpected network request") })
	args := append([]string{}, cli.args...)
	args[1] = "ftp://localhost:8080"
	var output bytes.Buffer
	if code := Run(t.Context(), append(args, "read", _testConversationID), strings.NewReader(""), &output, &output); code != _exitFailure {
		t.Fatal(code)
	}

}

func TestRunBoundedRequestRetainsOperation(t *testing.T) {
	cli := newTestCLI(t, func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodGet {
			writeTestJSON(t, w, 200, testIdentity(req, _testSubject))
			return
		}
		select {
		case <-req.Context().Done():
		case <-time.After(300 * time.Millisecond):
		}
	})
	cli.args = append(cli.args, "--request-timeout", "100ms")
	code, output, _ := cli.run(t, "task", _commandStart, "--profile", "investigation", "--text-file", "-", "--operation", "timeout")
	if code != _exitAmbiguous || output != "" {
		t.Fatalf("request timeout: %d %s", code, output)
	}
	if _, err := os.Stat(filepath.Join(cli.state, "timeout.json")); err != nil {
		t.Fatal(err)
	}
}

func TestRunResumeUsesIdentityWhileCatalogAndObservationAreUnavailable(t *testing.T) {
	var mu sync.Mutex
	posts := 0
	cli := newTestCLI(t, func(w http.ResponseWriter, req *http.Request) {
		if strings.HasSuffix(req.URL.Path, "/identity") {
			writeTestJSON(t, w, http.StatusOK, testIdentity(req, _testSubject))
			return
		}
		if req.Method == http.MethodGet {
			t.Error("resume depended on live catalog, profiles or history")
			http.Error(w, "Kubernetes observation unavailable", http.StatusServiceUnavailable)
			return
		}
		mu.Lock()
		posts++
		count := posts
		mu.Unlock()
		if req.Header.Get("Idempotency-Key") != "outage" {
			t.Error("recovery replaced the saved operation key")
		}
		if count == 1 {
			// Acceptance committed before a response failure. Profile retirement
			// or Kubernetes unavailability cannot require a different create.
			http.Error(w, "commit response unavailable", http.StatusServiceUnavailable)
			return
		}
		writeTestJSON(t, w, http.StatusAccepted, testReceipt(_testConversationID, 1))
	})
	code, output, diagnostic := cli.run(t, "task", _commandStart, "--profile", "retired-profile", "--text-file", "-", "--operation", "outage")
	if code != _exitAmbiguous || output != "" {
		t.Fatalf("uncertain acceptance: %d %s %s", code, output, diagnostic)
	}
	code, output, diagnostic = cli.run(t, "", "resume", "outage")
	if code != 0 || !strings.Contains(output, _stateAccepted) {
		t.Fatalf("uncertain replay during outage: %d %s %s", code, output, diagnostic)
	}
	code, output, diagnostic = cli.run(t, "", "resume", "outage")
	if code != 0 || !strings.Contains(output, `"saved_receipt":true`) {
		t.Fatalf("persisted receipt during outage: %d %s %s", code, output, diagnostic)
	}
	mu.Lock()
	defer mu.Unlock()
	if posts != 2 {
		t.Fatalf("persisted receipt was resubmitted: %d", posts)
	}
}

func TestRunProcessCrashBeforeAndAfterReceiptPersistence(t *testing.T) {
	for _, stage := range []string{_testBeforeReceipt, "after-receipt"} {
		t.Run(stage, func(t *testing.T) {
			var mu sync.Mutex
			posts := 0
			bodies := make([]string, 0)
			accepted := make(chan struct{}, 1)
			release := make(chan struct{})
			cli := newTestCLI(t, func(w http.ResponseWriter, req *http.Request) {
				if req.Method == http.MethodGet {
					writeTestJSON(t, w, 200, testIdentity(req, _testSubject))
					return
				}
				raw, err := io.ReadAll(req.Body)
				if err != nil {
					t.Error(err)
					return
				}
				if req.Header.Get("Idempotency-Key") != "crash" {
					t.Error("crashed process changed the replay identity")
				}
				mu.Lock()
				posts++
				count := posts
				bodies = append(bodies, string(raw))
				mu.Unlock()
				if stage == _testBeforeReceipt && count == 1 {
					// Simulate a committed server acceptance whose response has not
					// reached the caller when its process dies.
					accepted <- struct{}{}
					<-release
					return
				}
				writeTestJSON(t, w, 202, testReceipt(_testConversationID, 1))
			})
			executable, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			childArgs := append([]string{"-test.run=^TestClientCrashHelper$", "--"}, cli.args...)
			childArgs = append(childArgs, _commandStart, "--profile", "investigation", "--text-file", "-", "--operation", "crash")
			child := exec.CommandContext(t.Context(), executable, childArgs...)
			child.Env = append(os.Environ(), "GATEWAY_CLIENT_TEST_CHILD=1", "GATEWAY_CLIENT_TEST_CRASH_STAGE="+stage)
			child.Stdin = strings.NewReader("durable task")
			if err := child.Start(); err != nil {
				t.Fatal(err)
			}
			if stage == _testBeforeReceipt {
				select {
				case <-accepted:
				case <-time.After(5 * time.Second):
					_ = child.Process.Kill()
					_ = child.Wait()
					close(release)
					t.Fatal("child did not reach accepted POST")
				}
				if err := child.Process.Kill(); err != nil {
					t.Fatal(err)
				}
			}
			if err := child.Wait(); err == nil {
				t.Fatal("child did not crash")
			}
			if stage == _testBeforeReceipt {
				close(release)
			}
			if _, err := os.Stat(filepath.Join(cli.state, "crash.json")); err != nil {
				t.Fatal("crash lost operation", err)
			}
			code, output, diagnostic := cli.run(t, "", "resume", "crash")
			if code != 0 || !strings.Contains(output, _stateAccepted) {
				t.Fatalf("crash recovery: %d %s %s", code, output, diagnostic)
			}
			mu.Lock()
			defer mu.Unlock()
			wantPosts := 1
			if stage == _testBeforeReceipt {
				wantPosts = 2
			}
			if posts != wantPosts || len(bodies) == 2 && bodies[0] != bodies[1] {
				t.Fatalf("recovery changed request: posts=%d bodies=%v", posts, bodies)
			}
		})
	}
}

func TestClientCrashHelper(t *testing.T) {
	if os.Getenv("GATEWAY_CLIENT_TEST_CHILD") != "1" {
		return
	}
	for i, arg := range os.Args {
		if arg != "--" {
			continue
		}
		code := Run(context.Background(), os.Args[i+1:], os.Stdin, io.Discard, io.Discard)
		if code != 0 {
			os.Exit(70)
		}
		if os.Getenv("GATEWAY_CLIENT_TEST_CRASH_STAGE") == "after-receipt" {
			os.Exit(71)
		}
		os.Exit(0)
	}
	t.Fatal("child command arguments missing")
}

func ExampleRun() {
	var out, diagnostic bytes.Buffer
	code := Run(context.Background(), []string{"--help"}, strings.NewReader(""), &out, &diagnostic)
	fmt.Println(code, strings.Contains(out.String(), "HTTP 202 confirms durable acceptance"))
	// Output: 0 true
}
