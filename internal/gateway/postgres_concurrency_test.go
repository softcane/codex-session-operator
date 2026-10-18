package gateway

import (
	"context"
	"errors"
	"net"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	_testReadSnapshotUID   = "reader"
	_testReadRevision      = "revision"
	_testSnapshotBefore    = "before"
	_testSnapshotAfter     = "after"
	_testReadableUID       = "readable"
	_testCanceledCommitUID = "canceled-commit"
)

func TestPostgreSQLCanceledMutationLockWait(t *testing.T) {
	store, _ := postgresForTest(t)
	held, err := store.begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = held.Rollback(t.Context()) }()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	result := make(chan error, 1)
	var invoked atomic.Bool
	go func() {
		result <- store.Update(ctx, "canceled", func(r *Record) error {
			invoked.Store(true)
			r.Namespace = _testNamespace
			return nil
		})
	}()
	// Inspect the real database wait instead of relying on a startup sleep.
	waitCtx, done := context.WithTimeout(t.Context(), 3*time.Second)
	defer done()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var waiting bool
		if err := store.pool.QueryRow(waitCtx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND wait_event_type='Lock')`).Scan(&waiting); err != nil {
			t.Fatal(safeSQLError(err))
		}
		if waiting {
			break
		}
		select {
		case <-ticker.C:
		case <-waitCtx.Done():
			t.Fatal("mutation did not enter database lock wait")
		}
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled lock wait: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled mutation stayed blocked on the metadata lock")
	}
	if invoked.Load() {
		t.Fatal("callback ran after canceled lock wait")
	}
	if err := held.Rollback(t.Context()); err != nil {
		t.Fatal(safeSQLError(err))
	}
	if _, err := store.Get(t.Context(), "canceled"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("canceled input persisted: %v", err)
	}
}

func TestPostgreSQLReadSnapshotDoesNotBlockWriter(t *testing.T) {
	store, _ := postgresForTest(t)
	if err := store.Update(t.Context(), _testReadSnapshotUID, func(r *Record) error {
		r.Namespace = _testNamespace
		r.Name = _testSnapshotBefore
		r.Notes[_testReadRevision] = _testSnapshotBefore
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	snapshot, err := store.beginRead(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = snapshot.Rollback(t.Context()) }()
	before, err := readSQLRecord(t.Context(), snapshot, _testReadSnapshotUID)
	if err != nil {
		t.Fatal(err)
	}

	writeCtx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := store.Update(writeCtx, "unrelated", func(r *Record) error { r.Namespace = _testNamespace; return nil }); err != nil {
		t.Fatalf("read transaction blocked unrelated writer: %v", err)
	}
	if err := store.Update(writeCtx, _testReadSnapshotUID, func(r *Record) error {
		r.Name = _testSnapshotAfter
		r.Notes[_testReadRevision] = _testSnapshotAfter
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	during, err := readSQLRecord(t.Context(), snapshot, _testReadSnapshotUID)
	if err != nil || during.Name != before.Name || during.Notes[_testReadRevision] != before.Notes[_testReadRevision] {
		t.Fatalf("snapshot mixed record/history revisions: %+v %v", during, err)
	}
	current, err := store.Get(t.Context(), _testReadSnapshotUID)
	if err != nil || current.Name != _testSnapshotAfter || current.Notes[_testReadRevision] != _testSnapshotAfter {
		t.Fatalf("new reader missed committed revision: %+v %v", current, err)
	}
}

func TestPostgreSQLReadWhileWriterOwnsMetadataLock(t *testing.T) {
	store, _ := postgresForTest(t)
	if err := store.Update(t.Context(), _testReadableUID, func(r *Record) error { r.Namespace = _testNamespace; return nil }); err != nil {
		t.Fatal(err)
	}
	held, err := store.begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = held.Rollback(t.Context()) }()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	record, err := store.Get(ctx, _testReadableUID)
	if err != nil || record.UID != _testReadableUID {
		t.Fatalf("reader waited for mutation lock: %+v %v", record, err)
	}
	records, err := store.Records(ctx)
	if err != nil || len(records) != 1 {
		t.Fatalf("list waited for mutation lock: %d %v", len(records), err)
	}
}

func TestPostgreSQLStaleWorkerReadsRemainFenced(t *testing.T) {
	store, _ := postgresForTest(t)
	first, release, err := store.ClaimWorker(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if _, err := store.pool.Exec(t.Context(), "UPDATE gateway_meta SET expires='-infinity' WHERE id=1"); err != nil {
		t.Fatal(safeSQLError(err))
	}
	_, releaseNext, err := store.ClaimWorker(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer releaseNext()
	if _, err := first.Get(t.Context(), "absent"); !errors.Is(err, errFenced) {
		t.Fatalf("stale worker read was not fenced: %v", err)
	}
	if _, err := first.Records(t.Context()); !errors.Is(err, errFenced) {
		t.Fatalf("stale worker scan was not fenced: %v", err)
	}
}

// canceledCommitDisconnect cancels the caller only after PostgreSQL answered
// COMMIT, then withholds that answer. The durable outcome check must remain live.
type canceledCommitDisconnect struct {
	*commitDisconnect
	cancel context.CancelFunc
}

func (c *canceledCommitDisconnect) Read(raw []byte) (int, error) {
	n, err := c.commitDisconnect.Read(raw)
	if c.drop {
		c.cancel()
	}
	return n, err
}

func TestPostgreSQLCanceledUnknownCommitResolvesDurableOutcome(t *testing.T) {
	store, _ := postgresForTest(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	armed := new(atomic.Bool)
	config := store.pool.Config()
	config.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	config.ConnConfig.DialFunc = func(dialCtx context.Context, network, address string) (net.Conn, error) {
		conn, err := (&net.Dialer{}).DialContext(dialCtx, network, address)
		if err != nil {
			return nil, err
		}
		return &canceledCommitDisconnect{commitDisconnect: &commitDisconnect{Conn: conn, armed: armed}, cancel: cancel}, nil
	}
	pool, err := pgxpool.NewWithConfig(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	proxy := &PostgreSQLStore{pool: pool}
	armed.Store(true)
	var callbacks atomic.Int32
	if err := proxy.Update(ctx, _testCanceledCommitUID, func(r *Record) error {
		callbacks.Add(1)
		r.Namespace = _testNamespace
		return nil
	}); err != nil {
		t.Fatalf("durable canceled commit was not resolved: %v", err)
	}
	if armed.Load() || !errors.Is(ctx.Err(), context.Canceled) || callbacks.Load() != 1 {
		t.Fatal("commit fault/cancellation did not fire once")
	}
	record, err := store.Get(t.Context(), _testCanceledCommitUID)
	if err != nil || record.UID != _testCanceledCommitUID {
		t.Fatalf("acknowledged outcome not durable: %+v %v", record, err)
	}
}
