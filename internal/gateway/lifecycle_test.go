package gateway

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/softcane/codex-session-operator/internal/openaiapi"
)

// This stream obeys MessageStream's Close contract without relying on context
// cancellation to unblock Next.
type closeOnlyStream struct {
	started    chan struct{}
	closed     chan struct{}
	closeOnce  sync.Once
	closeCalls atomic.Int32
}

var _ openaiapi.MessageStream = (*closeOnlyStream)(nil)

func (s *closeOnlyStream) Next() bool {
	close(s.started)
	<-s.closed
	return false
}

func (s *closeOnlyStream) Event() json.RawMessage { return nil }
func (s *closeOnlyStream) Err() error             { return nil }
func (s *closeOnlyStream) Close() error {
	s.closeCalls.Add(1)
	s.release()
	return nil
}

func (s *closeOnlyStream) release() {
	s.closeOnce.Do(func() { close(s.closed) })
}

type closeOnlyProvider struct {
	Provider

	stream *closeOnlyStream
}

var _ Provider = (*closeOnlyProvider)(nil)

func (p *closeOnlyProvider) OpenMessageStream(context.Context, string) (openaiapi.MessageStream, error) {
	return p.stream, nil
}

func TestGatewayJoinsCloseOnlyStreams(t *testing.T) {
	tests := []struct {
		name string
		stop func(*Gateway) error
	}{
		{
			name: "stop watch",
			stop: func(g *Gateway) error {
				g.stopWatch(_testUID)
				return nil
			},
		},
		{
			name: "worker shutdown",
			stop: func(g *Gateway) error {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return g.Start(ctx)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g, _ := testGateway(t, testStore(t), simulatorForTest())
			stream := &closeOnlyStream{started: make(chan struct{}), closed: make(chan struct{})}
			t.Cleanup(stream.release)
			g.provider = &closeOnlyProvider{Provider: g.provider, stream: stream}
			if err := g.ensureWatch(_testUID, _testRemoteID); err != nil {
				t.Fatal(err)
			}
			select {
			case <-stream.started:
			case <-time.After(2 * time.Second):
				t.Fatal("stream reader did not start")
			}
			done := make(chan error, 1)
			go func() { done <- tt.stop(g) }()
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(2 * time.Second):
				stream.release()
				<-done
				t.Fatal("shutdown did not close the blocked stream")
			}
			if got := stream.closeCalls.Load(); got != 1 {
				t.Fatalf("stream closed %d times, want once", got)
			}
		})
	}
}
