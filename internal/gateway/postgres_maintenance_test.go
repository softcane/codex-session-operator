package gateway

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgreSQLCommitMarkerRetention(t *testing.T) {
	store, _ := postgresForTest(t)
	_, err := store.pool.Exec(t.Context(), `INSERT INTO gateway_commits(mutation,committed_at)
 SELECT 'expired-' || n, clock_timestamp() - interval '2 hours' FROM generate_series(1,$1::int) n`, _commitPruneBatch+1)
	if err != nil {
		t.Fatal(safeSQLError(err))
	}
	if _, err := store.pool.Exec(t.Context(), "INSERT INTO gateway_commits(mutation) VALUES('recent')"); err != nil {
		t.Fatal(safeSQLError(err))
	}
	if err := store.pruneCommitMarkers(t.Context()); err != nil {
		t.Fatal(err)
	}
	var expired int
	var recent bool
	query := `SELECT (SELECT count(*) FROM gateway_commits WHERE mutation LIKE 'expired-%'),
 EXISTS(SELECT 1 FROM gateway_commits WHERE mutation='recent')`
	if err := store.pool.QueryRow(t.Context(), query).Scan(&expired, &recent); err != nil {
		t.Fatal(safeSQLError(err))
	}
	if expired != 1 || !recent {
		t.Fatalf("first sweep exceeded batch or deleted recent evidence: expired=%d recent=%t", expired, recent)
	}
	if err := store.pruneCommitMarkers(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := store.pool.QueryRow(t.Context(), query).Scan(&expired, &recent); err != nil {
		t.Fatal(safeSQLError(err))
	}
	if expired != 0 || !recent {
		t.Fatalf("backlog failed to drain safely: expired=%d recent=%t", expired, recent)
	}
}

func TestPostgreSQLWorkerPrunesDuringDeliveryAndRestoreHold(t *testing.T) {
	for _, tt := range []struct {
		name string
		held bool
	}{
		{name: "delivery enabled"},
		{name: "restore hold", held: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			store, _ := postgresForTest(t)
			if _, err := store.pool.Exec(t.Context(), `INSERT INTO gateway_commits(mutation,committed_at)
 VALUES('expired',clock_timestamp()-interval '2 hours')`); err != nil {
				t.Fatal(safeSQLError(err))
			}
			if _, err := store.pool.Exec(t.Context(), "UPDATE gateway_meta SET delivery_disabled=$1 WHERE id=1", tt.held); err != nil {
				t.Fatal(safeSQLError(err))
			}
			g, _ := testGateway(t, store, simulatorForTest())
			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan error, 1)
			go func() { done <- g.Start(ctx) }()
			t.Cleanup(func() {
				cancel()
				select {
				case err := <-done:
					if err != nil {
						t.Error(err)
					}
				case <-time.After(2 * time.Second):
					t.Error("maintenance worker did not exit")
				}
			})
			waitCtx, stop := context.WithTimeout(t.Context(), 2*time.Second)
			defer stop()
			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()
			for {
				var expired bool
				if err := store.pool.QueryRow(waitCtx, "SELECT EXISTS(SELECT 1 FROM gateway_commits WHERE mutation='expired')").Scan(&expired); err != nil {
					t.Fatal(safeSQLError(err))
				}
				if !expired {
					break
				}
				select {
				case <-ticker.C:
				case <-waitCtx.Done():
					t.Fatal("running gateway did not prune expired commit evidence")
				}
			}
			if held := errors.Is(store.DeliveryAllowed(t.Context()), errRestoreHeld); held != tt.held {
				t.Fatal("maintenance changed the delivery gate")
			}
		})
	}
}

type pausedCommitDisconnect struct {
	*commitDisconnect
	answered chan struct{}
	resume   <-chan struct{}
}

func (c *pausedCommitDisconnect) Read(raw []byte) (int, error) {
	n, err := c.Conn.Read(raw)
	if c.drop && n > 0 {
		close(c.answered)
		<-c.resume
		_ = c.Close()
		return 0, io.ErrUnexpectedEOF
	}
	return n, err
}

func TestPostgreSQLPruningPreservesUnknownCommit(t *testing.T) {
	for _, tt := range []struct {
		name   string
		cancel bool
	}{
		{name: "lost response"},
		{name: "lost response after cancellation", cancel: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			store, _ := postgresForTest(t)
			if _, err := store.pool.Exec(t.Context(), `INSERT INTO gateway_commits(mutation,committed_at)
 VALUES('expired',clock_timestamp()-interval '2 hours')`); err != nil {
				t.Fatal(safeSQLError(err))
			}
			answered := make(chan struct{})
			resume := make(chan struct{})
			var resumeOnce sync.Once
			release := func() { resumeOnce.Do(func() { close(resume) }) }
			armed := new(atomic.Bool)
			config := store.pool.Config()
			config.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
			config.ConnConfig.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
				conn, err := (&net.Dialer{}).DialContext(ctx, network, address)
				if err != nil {
					return nil, err
				}
				return &pausedCommitDisconnect{
					commitDisconnect: &commitDisconnect{Conn: conn, armed: armed}, answered: answered, resume: resume,
				}, nil
			}
			pool, err := pgxpool.NewWithConfig(t.Context(), config)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(pool.Close)
			proxy := &PostgreSQLStore{pool: pool}
			ctx, cancel := context.WithCancel(t.Context())
			result := make(chan error, 1)
			done := make(chan struct{})
			var callbacks atomic.Int32
			t.Cleanup(func() {
				cancel()
				release()
				select {
				case <-done:
				case <-time.After(2 * time.Second):
					t.Error("commit observer did not exit")
				}
			})
			armed.Store(true)
			go func() {
				defer close(done)
				result <- proxy.Update(ctx, "pending-outcome", func(r *Record) error {
					callbacks.Add(1)
					r.Namespace = _testNamespace
					return nil
				})
			}()
			select {
			case <-answered:
			case <-time.After(2 * time.Second):
				t.Fatal("server did not commit before response fault")
			}
			if err := store.pruneCommitMarkers(t.Context()); err != nil {
				t.Fatal(err)
			}
			var markers int
			if err := store.pool.QueryRow(t.Context(), "SELECT count(*) FROM gateway_commits").Scan(&markers); err != nil {
				t.Fatal(safeSQLError(err))
			}
			if markers != 1 {
				t.Fatalf("pruning did not preserve only the unresolved recent commit: %d", markers)
			}
			if tt.cancel {
				cancel()
			}
			release()
			select {
			case err := <-result:
				if err != nil || callbacks.Load() != 1 {
					t.Fatalf("pruning changed durable acceptance: callbacks=%d err=%v", callbacks.Load(), err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("durable outcome was not resolved")
			}
		})
	}
}
