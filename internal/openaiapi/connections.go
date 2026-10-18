package openaiapi

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"net/http"
	"sync"
	"time"

	openai "github.com/openai/openai-go/v3"
)

// Connection is a live observation, never inferred from Session or Pod status.
// Epoch changes after stream loss; compute is replaced after resubscription so a
// fresh connected event establishes readiness even if the API replays no snapshot.
type Connection struct {
	Subscribed bool
	Connected  bool
	Epoch      string
	Sequence   int64
}

type connectionWatch struct {
	environmentID string
	state         Connection
	cancel        context.CancelFunc
}

// Connections owns a bounded set of live streams. Start and all worker lifetimes
// are owned by the manager. Reconciliation only requests or reads observations.
type Connections struct {
	SDK      openai.Client
	mu       sync.Mutex
	ctx      context.Context
	watchers map[string]*connectionWatch
	wg       sync.WaitGroup
}

const maxConnections = 32

func (c *Connections) NeedLeaderElection() bool { return true }

func (c *Connections) Start(ctx context.Context) error {
	c.mu.Lock()
	c.ctx = ctx
	c.watchers = make(map[string]*connectionWatch)
	c.mu.Unlock()
	<-ctx.Done()
	c.mu.Lock()
	// Exclude new workers before waiting for all existing workers to exit.
	c.ctx = nil
	for _, watch := range c.watchers {
		watch.cancel()
	}
	c.mu.Unlock()
	c.wg.Wait()
	return nil
}

func (c *Connections) Observe(sessionID, environmentID string) (Connection, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ctx == nil || c.ctx.Err() != nil {
		return Connection{}, errors.New("environment observer is not running")
	}
	if watch := c.watchers[sessionID]; watch != nil {
		if watch.environmentID != environmentID {
			return Connection{}, errors.New("environment identity changed")
		}
		return watch.state, nil
	}
	if len(c.watchers) >= maxConnections {
		return Connection{}, errors.New("environment observer capacity reached")
	}
	base, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return Connection{}, errors.New("default HTTP transport is unsupported")
	}
	transport := base.Clone()
	transport.ResponseHeaderTimeout = callTimeout
	ctx, cancel := context.WithCancel(c.ctx)
	watch := &connectionWatch{environmentID: environmentID, cancel: cancel}
	c.watchers[sessionID] = watch
	c.wg.Go(func() {
		c.run(ctx, sessionID, watch, transport)
	})
	return Connection{}, nil
}

func (c *Connections) Forget(sessionID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if watch := c.watchers[sessionID]; watch != nil {
		watch.cancel()
		delete(c.watchers, sessionID)
	}
}

func (c *Connections) set(watch *connectionWatch, state Connection) {
	c.mu.Lock()
	defer c.mu.Unlock()
	watch.state = state
}

func (c *Connections) run(ctx context.Context, sessionID string, watch *connectionWatch, transport *http.Transport) {
	// Bound connection establishment but allow an idle stream to stay open. The
	// normal lifecycle client's 20-second request timeout must not end this stream.
	defer transport.CloseIdleConnections()
	for ctx.Err() == nil {
		stream, cancel, err := openEventStream(
			ctx,
			c.SDK,
			sessionID,
			eventStreamSettings{transport: transport, setupTimeout: callTimeout},
		)
		var state Connection
		if err == nil {
			var nonce [16]byte
			if _, err := rand.Read(nonce[:]); err == nil {
				state = Connection{Subscribed: true, Epoch: hex.EncodeToString(nonce[:])}
				c.set(watch, state)
				for stream.Next() {
					event := stream.Current()
					if event.SessionID != sessionID {
						continue
					}
					if event.Type == "error" || event.Type == "agent.session.failed" {
						break
					}
					if event.Type == "agent.session.environment.reset" && event.EnvironmentID == watch.environmentID {
						state.Connected = false
						state.Sequence++
						c.set(watch, state)
						continue
					}
					if event.Environment.ID != watch.environmentID || event.Environment.Type != selfHostedType {
						continue
					}
					// Only supported connection transitions can establish or revoke
					// readiness. Other environment notifications must not change
					// connection evidence or the executor's sequence baseline.
					switch event.Type {
					case "agent.session.environment.connected":
						state.Connected = event.Environment.Status == "connected"
					case "agent.session.environment.disconnected", "agent.session.environment.pending",
						"agent.session.environment.failed":
						state.Connected = false
					case "agent.session.environment.ready":
						// Setup readiness is separate from executor connection and
						// cannot establish or invalidate a connected Pod generation.
						continue
					default:
						continue
					}
					state.Sequence++
					c.set(watch, state)
				}
			}
			cancel()
			_ = stream.Close()
		}
		c.set(watch, Connection{})
		timer := time.NewTimer(5 * time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}
