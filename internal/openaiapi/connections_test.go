package openaiapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

const (
	testConnectionSessionIDField = "session_id"
	testConnectionStatusField    = "status"
)

func waitConnection(t *testing.T, c *Connections, predicate func(Connection) bool) Connection {
	t.Helper()
	return waitConnectionWithin(t, c, predicate, 3*time.Second)
}

func waitConnectionWithin(t *testing.T, c *Connections, predicate func(Connection) bool, timeout time.Duration) Connection {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		state, err := c.Observe(testSessionID, testEnvironmentID)
		if err == nil && predicate(state) {
			return state
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("Connection did not converge")
	return Connection{}
}

// The real SDK SSE decoder receives every event. Closing the first connection
// exercises the production reconnect delay; subsequent streams stay available.
func controlledConnectionStream(t *testing.T) (*Connections, chan map[string]any, chan struct{}) {
	t.Helper()
	events := make(chan map[string]any, 1)
	disconnect := make(chan struct{})
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Error("response writer does not support streaming")
			return
		}
		flusher.Flush()
		var detached <-chan struct{}
		if calls.Add(1) == 1 {
			detached = disconnect
		}
		for {
			select {
			case <-r.Context().Done():
				return
			case <-detached:
				return
			case event := <-events:
				data, err := json.Marshal(event)
				if err != nil {
					t.Error(err)
					return
				}
				if _, err := fmt.Fprintf(w, "data: %s\n\n", data); err != nil {
					return
				}
				flusher.Flush()
			}
		}
	}))
	t.Cleanup(server.Close)
	c := &Connections{SDK: clientForServer(server, nil).SDK}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- c.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(3 * time.Second):
			t.Error("observer cancellation did not join its streams")
		}
	})
	waitConnection(t, c, func(s Connection) bool { return s.Subscribed })
	return c, events, disconnect
}

func environmentWireEvent(eventType, status string) map[string]any {
	return map[string]any{typeField: eventType, testConnectionSessionIDField: testSessionID,
		testEnvironmentField: map[string]any{"id": testEnvironmentID, typeField: selfHostedType, testConnectionStatusField: status}}
}

func TestConnectionStreamIgnoresUnknownEnvironmentNotifications(t *testing.T) {
	c, events, _ := controlledConnectionStream(t)
	events <- environmentWireEvent("agent.session.environment.future", "connected")
	events <- environmentWireEvent("agent.session.environment.ready", "ready")
	events <- environmentWireEvent("agent.session.environment.ready", "connected")
	events <- environmentWireEvent("agent.session.environment.connected", "connected")
	first := waitConnection(t, c, func(s Connection) bool { return s.Connected })
	if first.Sequence != 1 {
		t.Fatalf("setup notifications established a connection baseline: %+v", first)
	}
	for _, eventType := range []string{"agent.session.environment.updated", "agent.session.environment.usage", "agent.session.environment.future"} {
		events <- environmentWireEvent(eventType, "connected")
	}
	events <- environmentWireEvent("agent.session.environment.ready", "ready")
	events <- environmentWireEvent("agent.session.environment.ready", "connected")
	// A supported following transition is a stream-processing barrier. Only the
	// two actual connected transitions may advance the Pod-readiness sequence.
	events <- environmentWireEvent("agent.session.environment.connected", "connected")
	last := waitConnection(t, c, func(s Connection) bool { return s.Connected && s.Sequence > first.Sequence })
	if last.Sequence != first.Sequence+1 || last.Epoch != first.Epoch {
		t.Fatalf("benign notifications changed connection generation: first=%+v last=%+v", first, last)
	}
}

func TestConnectionStreamFiltersForeignEventsAndRevokesOnTransitions(t *testing.T) {
	c, events, _ := controlledConnectionStream(t)
	events <- environmentWireEvent("agent.session.environment.connected", "connected")
	first := waitConnection(t, c, func(s Connection) bool { return s.Connected })
	foreignSession := environmentWireEvent("agent.session.environment.disconnected", "disconnected")
	foreignSession[testConnectionSessionIDField] = "foreign-session"
	events <- foreignSession
	foreignEnvironment := environmentWireEvent("agent.session.environment.disconnected", "disconnected")
	foreignEnvironment[testEnvironmentField] = map[string]any{"id": "foreign-environment", typeField: selfHostedType, testConnectionStatusField: "disconnected"}
	events <- foreignEnvironment
	hosted := environmentWireEvent("agent.session.environment.failed", "failed")
	hosted[testEnvironmentField] = map[string]any{"id": testEnvironmentID, typeField: "openai_hosted", testConnectionStatusField: "failed"}
	events <- hosted
	events <- map[string]any{typeField: "agent.session.environment.reset", testConnectionSessionIDField: testSessionID, "environment_id": "foreign-environment"}
	events <- environmentWireEvent("agent.session.environment.connected", "connected")
	state := waitConnection(t, c, func(s Connection) bool { return s.Connected && s.Sequence > first.Sequence })
	if state.Sequence != first.Sequence+1 {
		t.Fatalf("foreign notifications changed this environment: %+v", state)
	}
	for _, tt := range []struct {
		eventType string
		status    string
	}{
		{eventType: "agent.session.environment.disconnected", status: "disconnected"},
		{eventType: "agent.session.environment.pending", status: "pending"},
		{eventType: "agent.session.environment.failed", status: "failed"},
	} {
		t.Run(tt.eventType, func(t *testing.T) {
			events <- environmentWireEvent(tt.eventType, tt.status)
			state = waitConnection(t, c, func(s Connection) bool { return s.Sequence > state.Sequence })
			if state.Connected || !state.Subscribed {
				t.Fatalf("negative transition did not revoke connection: %+v", state)
			}
			events <- environmentWireEvent("agent.session.environment.connected", "connected")
			state = waitConnection(t, c, func(s Connection) bool { return s.Connected && s.Sequence > state.Sequence })
		})
	}
	events <- map[string]any{typeField: "agent.session.environment.reset", testConnectionSessionIDField: testSessionID, "environment_id": testEnvironmentID}
	reset := waitConnection(t, c, func(s Connection) bool { return s.Sequence > state.Sequence })
	if reset.Connected || !reset.Subscribed || reset.Sequence != state.Sequence+1 {
		t.Fatalf("reset did not revoke exactly this connection: %+v", reset)
	}
}

func TestConnectionStreamLossRequiresNewEpochAndConnectedEvent(t *testing.T) {
	c, events, disconnect := controlledConnectionStream(t)
	events <- environmentWireEvent("agent.session.environment.connected", "connected")
	first := waitConnection(t, c, func(s Connection) bool { return s.Connected })
	close(disconnect)
	lost := waitConnection(t, c, func(s Connection) bool { return !s.Subscribed })
	if lost.Connected || lost.Epoch != "" || lost.Sequence != 0 {
		t.Fatalf("stream loss retained stale connection evidence: %+v", lost)
	}
	resubscribed := waitConnectionWithin(t, c, func(s Connection) bool { return s.Subscribed }, 7*time.Second)
	if resubscribed.Epoch == first.Epoch || resubscribed.Epoch == "" || resubscribed.Connected || resubscribed.Sequence != 0 {
		t.Fatalf("resubscription reused prior readiness evidence: first=%+v next=%+v", first, resubscribed)
	}
	events <- environmentWireEvent("agent.session.environment.connected", "connected")
	connected := waitConnection(t, c, func(s Connection) bool { return s.Connected })
	if connected.Epoch != resubscribed.Epoch || connected.Sequence != 1 {
		t.Fatalf("fresh connected event did not establish the new epoch: %+v", connected)
	}
}

func TestConnectionsEventsAndShutdown(t *testing.T) {
	events := make(chan map[string]any, 8)
	closeStream := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/agents/sessions/"+testSessionID+"/events" || r.Header.Get("OpenAI-Beta") != agentsBetaHeader {
			t.Error("Unexpected stream contract")
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Error("Response writer does not support streaming")
			return
		}
		flusher.Flush()
		for {
			select {
			case <-r.Context().Done():
				return
			case <-closeStream:
				return
			case event := <-events:
				data, err := json.Marshal(event)
				if err != nil {
					t.Error(err)
					return
				}
				if _, err := fmt.Fprintf(w, "data: %s\n\n", data); err != nil {
					return
				}
				flusher.Flush()
			}
		}
	}))
	defer server.Close()
	c := &Connections{SDK: clientForServer(server, nil).SDK}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Start(ctx) }()
	defer cancel()
	first := waitConnection(t, c, func(s Connection) bool { return s.Subscribed })
	if first.Connected || first.Epoch == "" {
		t.Fatal("Subscription alone must not imply connection")
	}
	send := func(session, environment, eventType, status string) {
		events <- map[string]any{typeField: eventType, testConnectionSessionIDField: session, testEnvironmentField: map[string]any{"id": environment, typeField: selfHostedType, testConnectionStatusField: status}}
	}
	send(testSessionID, testEnvironmentID, "agent.session.environment.connected", "connected")
	waitConnection(t, c, func(s Connection) bool { return s.Connected })
	send(testSessionID, testEnvironmentID, "agent.session.environment.disconnected", "disconnected")
	waitConnection(t, c, func(s Connection) bool { return s.Subscribed && !s.Connected })
	send("other-session", testEnvironmentID, "agent.session.environment.connected", "connected")
	send(testSessionID, "other-environment", "agent.session.environment.connected", "connected")
	// A following matching pending event provides an ordering barrier; unrelated
	// events must never be mistaken for this environment's readiness.
	send(testSessionID, testEnvironmentID, "agent.session.environment.pending", "pending")
	close(closeStream)
	waitConnection(t, c, func(s Connection) bool { return !s.Subscribed && !s.Connected })
	c.Forget(testSessionID)
	if _, err := c.Observe(testSessionID, "new-env"); err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Manager cancellation did not join stream workers")
	}
	if _, err := c.Observe(testSessionID, testEnvironmentID); err == nil {
		t.Fatal("Stopped observer accepted new work")
	}
}

func TestConnectionsCapacityAndIdentity(t *testing.T) {
	ctx := t.Context()
	c := &Connections{ctx: ctx, watchers: make(map[string]*connectionWatch)}
	for i := range maxConnections {
		c.watchers[fmt.Sprint(i)] = &connectionWatch{environmentID: testEnvironmentID, cancel: func() {}}
	}
	if _, err := c.Observe("overflow", testEnvironmentID); err == nil {
		t.Fatal("Unbounded stream allocation")
	}
	if _, err := c.Observe("0", "another-env"); err == nil {
		t.Fatal("Environment identity change accepted")
	}
}

func TestConnectionsRejectUnsupportedDefaultTransport(t *testing.T) {
	original := http.DefaultTransport
	http.DefaultTransport = roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("unexpected request")
	})
	t.Cleanup(func() { http.DefaultTransport = original })

	c := &Connections{ctx: t.Context(), watchers: make(map[string]*connectionWatch)}
	if _, err := c.Observe(testSessionID, testEnvironmentID); err == nil {
		t.Fatal("Unsupported default transport was accepted")
	}
}
