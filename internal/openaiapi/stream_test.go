package openaiapi

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestEventStreamSetupDeadline(t *testing.T) {
	for _, tt := range []struct {
		name    string
		headers bool
	}{
		{name: "stalled headers"},
		{name: "stalled error body", headers: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tt.headers {
					w.WriteHeader(http.StatusBadGateway)
					w.(http.Flusher).Flush()
				}
				<-r.Context().Done()
			}))
			t.Cleanup(server.Close)
			transport := http.DefaultTransport.(*http.Transport).Clone()
			t.Cleanup(transport.CloseIdleConnections)
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			stream, stop, err := openEventStream(ctx, clientForServer(server, nil).SDK, testSessionID, eventStreamSettings{
				transport: transport, setupTimeout: 50 * time.Millisecond,
			})
			if stop != nil {
				stop()
			}
			if stream != nil || !errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
				t.Fatalf("stream setup did not enforce its own deadline: %v", err)
			}
		})
	}
}

func TestEventStreamSurvivesSetupDeadline(t *testing.T) {
	emit := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		select {
		case <-emit:
			_, _ = fmt.Fprintf(w, "data: {\"type\":\"agent.session.created\",\"session_id\":%q}\n\n", testSessionID)
			w.(http.Flusher).Flush()
		case <-r.Context().Done():
			return
		}
		<-r.Context().Done()
	}))
	t.Cleanup(server.Close)
	transport := http.DefaultTransport.(*http.Transport).Clone()
	t.Cleanup(transport.CloseIdleConnections)
	stream, cancel, err := openEventStream(t.Context(), clientForServer(server, nil).SDK, testSessionID, eventStreamSettings{
		transport: transport, setupTimeout: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cancel(); _ = stream.Close() })
	// A successful subscription must survive longer than its establishment bound.
	timer := time.NewTimer(150 * time.Millisecond)
	defer timer.Stop()
	<-timer.C
	close(emit)
	next := make(chan bool, 1)
	go func() { next <- stream.Next() }()
	select {
	case ok := <-next:
		if !ok || stream.Current().SessionID != testSessionID {
			t.Fatalf("healthy stream expired with setup deadline: %v", stream.Err())
		}
	case <-time.After(time.Second):
		t.Fatal("healthy stream did not deliver an event")
	}
}
