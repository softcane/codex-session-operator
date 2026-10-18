package openaiapi

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	openai "github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/option"
	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
)

func TestGatewayWireAndSavedPagination(t *testing.T) {
	var mu sync.Mutex
	var posts int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("OpenAI-Beta") != "agents=v1" || r.Header.Get("Authorization") != "Bearer test-key" {
			t.Error("missing pinned API/auth headers")
		}
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/events"):
			if r.Header.Get("Idempotency-Key") != "durable-message-key" {
				t.Error("missing stable key")
			}
			var body map[string]json.RawMessage
			if json.NewDecoder(r.Body).Decode(&body) != nil || !strings.Contains(string(body["events"]), "agent.session.input.message") {
				t.Error("event body missing")
			}
			mu.Lock()
			posts++
			mu.Unlock()
			w.WriteHeader(http.StatusAccepted)
		case strings.HasSuffix(r.URL.Path, "/items"):
			if r.URL.Query().Get("order") != "asc" {
				t.Error("history order missing")
			}
			if r.URL.Query().Get("after") == "" {
				_, _ = fmt.Fprint(w, `{"data":[{"id":"item_1","type":"message","role":"user","content":[]}],"has_more":true,"last_id":"item_1"}`)
			} else {
				_, _ = fmt.Fprint(w, `{"data":[{"id":"item_2","type":"message","role":"assistant","content":[{"type":"output_text","text":"saved"}]}],"has_more":false}`)
			}
		case strings.HasSuffix(r.URL.Path, "/turns"):
			_, _ = fmt.Fprint(w, `{"data":[{"id":"turn_1","status":"completed"}],"has_more":false}`)
		default:
			_, _ = fmt.Fprint(w, `{"id":"sess_wire","status":"requires_action","required_actions":[{"type":"function_call","turn_id":"turn_1","call_id":"call_1","name":"gateway_echo","arguments":{"value":1}}]}`)
		}
	}))
	defer server.Close()
	c := &Client{SDK: openai.NewClient(option.WithBaseURL(server.URL+"/"), option.WithAPIKey("test-key"), option.WithMaxRetries(0))}
	event := json.RawMessage(`{"type":"agent.session.input.message","input":[{"role":"user","content":[{"type":"input_text","text":"wire"}]}]}`)
	for range 2 {
		if err := c.SubmitInput(context.Background(), "sess_wire", "durable-message-key", event); err != nil {
			t.Fatal(err)
		}
	}
	history, err := c.SavedHistory(context.Background(), "sess_wire")
	if err != nil {
		t.Fatal(err)
	}
	if posts != 2 || len(history.Items) != 2 || len(history.Turns) != 1 || len(history.Actions) != 1 || history.Actions[0].CallID != "call_1" {
		t.Fatalf("incomplete history: %+v", history)
	}
}

func TestInitialNoneCreationCarriesExactlyOneInput(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path != "/agents/sessions" || r.Header.Get("Idempotency-Key") != "" {
			t.Error("invalid create path or unsafe transport idempotency header")
		}
		var body map[string]json.RawMessage
		if json.NewDecoder(r.Body).Decode(&body) != nil || string(body["input"]) != `"durable first"` || string(body["stream"]) != "false" || string(body["environment"]) != `{"type":"none"}` || string(body[spendControlField]) != `{"limit":125}` {
			t.Errorf("unexpected first input fields: %s", body["input"])
		}
		_, _ = fmt.Fprint(w, `{"id":"sess_none","environment":{"type":"none"},"agent":{"model":"gpt-6-luna"},"status":"in_progress","metadata":{"kubernetes_uid":"uid","operator":"codex-session-operator","owner_scope":"scope"},"spend_control":{"limit":125,"consumed":0}}`)
	}))
	defer server.Close()
	c := &Client{SDK: openai.NewClient(option.WithBaseURL(server.URL+"/"), option.WithAPIKey("test-key"))}
	limit := int64(125)
	spend := &agentsv1alpha1.SessionSpendControl{Mode: sessionSpendModeLimited, LimitCents: &limit}
	remote, err := c.CreateInitialSession(context.Background(), "uid", "scope", "", map[string]any{typeField: _noneValue}, nil, nil, nil, spend, "durable first", "first-key")
	if err != nil || remote.ID != "sess_none" || remote.EnvironmentType != _noneValue || remote.SpendLimitCents == nil || *remote.SpendLimitCents != limit {
		t.Fatal(remote, err)
	}
}

func TestGatewayStreamCancellationAndRedaction(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Error("Response writer does not support streaming")
			return
		}
		flusher.Flush()
		<-r.Context().Done()
	}))
	defer server.Close()
	c := &Client{SDK: openai.NewClient(option.WithBaseURL(server.URL+"/"), option.WithAPIKey("test-key"))}
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := c.OpenMessageStream(ctx, "sess_wire")
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { defer close(done); stream.Next() }()
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("stream survived cancellation")
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestGatewayIncompleteHistoryAndSanitizedErrors(t *testing.T) {
	tests := []struct {
		name   string
		status int
		body   string
	}{
		{name: "missing pagination evidence", status: 200, body: `{"data":[],"object":"list"}`},
		{name: "secret error response", status: 503, body: `{"error":{"message":"raw-credential-must-not-escape"}}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/agents/sessions/sess_wire" {
					_, _ = fmt.Fprint(w, `{"id":"sess_wire","status":"idle"}`)
					return
				}
				w.WriteHeader(tt.status)
				_, _ = fmt.Fprint(w, tt.body)
			}))
			defer server.Close()
			c := &Client{SDK: openai.NewClient(option.WithBaseURL(server.URL+"/"), option.WithAPIKey("test-key"))}
			if _, err := c.SavedHistory(context.Background(), "sess_wire"); err == nil || strings.Contains(err.Error(), "raw-credential") {
				t.Fatal("incomplete history accepted or error leaked")
			}
		})
	}
}

func TestGatewayNullSessionStateIsAnError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, "null")
	}))
	defer server.Close()
	c := &Client{SDK: openai.NewClient(option.WithBaseURL(server.URL+"/"), option.WithAPIKey("test-key"))}
	if _, err := c.SavedHistory(context.Background(), "sess_wire"); err == nil {
		t.Fatal("null Session state accepted")
	}
}

func TestInitialCreationDoesNotReplayOnReusedConnectionLoss(t *testing.T) {
	var posts atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"id":"preflight","environment":{"type":"none"},"agent":{"model":"gpt-6-luna"}}`)
			return
		}
		_, _ = io.Copy(io.Discard, r.Body)
		posts.Add(1) // Commit happened; the response is deliberately lost.
		hijacker, ok := w.(http.Hijacker)
		if !ok {
			t.Error("fixture cannot lose connection")
			return
		}
		connection, _, err := hijacker.Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		_ = connection.Close()
	}))
	defer server.Close()
	c := &Client{SDK: openai.NewClient(option.WithBaseURL(server.URL+"/"), option.WithAPIKey("test-key"), option.WithMaxRetries(0), option.WithHTTPClient(server.Client()))}
	if _, err := c.GetSession(context.Background(), "preflight"); err != nil {
		t.Fatal(err)
	}
	var reused atomic.Bool
	ctx := httptrace.WithClientTrace(context.Background(), &httptrace.ClientTrace{GotConn: func(info httptrace.GotConnInfo) {
		if info.Reused {
			reused.Store(true)
		}
	}})
	spend := &agentsv1alpha1.SessionSpendControl{Mode: sessionSpendModeUnlimited}
	_, err := c.CreateInitialSession(ctx, "uid", "scope", "", map[string]any{typeField: _noneValue}, nil, nil, nil, spend, "commit once", "logical-first-key")
	if err == nil || !reused.Load() {
		t.Fatal("fixture did not exercise response loss on a reused connection")
	}
	if posts.Load() != 1 {
		t.Fatalf("transport replayed non-idempotent creation: %d commits", posts.Load())
	}
}
