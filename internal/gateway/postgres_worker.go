package gateway

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func (s *PostgreSQLStore) ClaimWorker(ctx context.Context) (Storage, func(), error) {
	ctx, cancel := context.WithTimeout(ctx, _sqlTimeout)
	defer cancel()
	tx, err := s.begin(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	owner := uuid.NewString()
	var epoch int64
	err = tx.QueryRow(ctx, `UPDATE gateway_meta SET owner=$1,epoch=epoch+1,expires=clock_timestamp()+interval '30 seconds' WHERE id=1 AND NOT delivery_disabled AND expires<=clock_timestamp() RETURNING epoch`, owner).Scan(&epoch)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil, errWorkerHeld
	}
	if err != nil {
		return nil, nil, safeSQLError(err)
	}
	if err := s.commit(ctx, tx, owner); err != nil {
		return nil, nil, err
	}
	view := &PostgreSQLStore{pool: s.pool, owner: owner, epoch: epoch}
	release := func() {
		releaseCtx, done := context.WithTimeout(context.WithoutCancel(ctx), _sqlTimeout)
		defer done()
		_, _ = s.pool.Exec(releaseCtx, `UPDATE gateway_meta SET owner='',expires='-infinity' WHERE id=1 AND owner=$1 AND epoch=$2`, owner, epoch)
	}
	return view, release, nil
}

func (s *PostgreSQLStore) renew(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, _sqlTimeout)
	defer cancel()
	result, err := s.pool.Exec(ctx, `UPDATE gateway_meta SET expires=clock_timestamp()+interval '30 seconds' WHERE id=1 AND owner=$1 AND epoch=$2 AND expires>clock_timestamp() AND NOT delivery_disabled`, s.owner, s.epoch)
	if err != nil {
		return safeSQLError(err)
	}
	if result.RowsAffected() != 1 {
		return errFenced
	}
	return nil
}

// All replicas accept HTTP and serve retained history. One owned worker runs
// remote I/O; losing renewal cancels and joins its streams before retrying.
func (g *Gateway) startShared(ctx context.Context, store DeliveryStorage) error {
	g.mu.Lock()
	g.ctx = ctx
	g.mu.Unlock()
	defer func() { g.mu.Lock(); g.ctx = nil; g.mu.Unlock() }()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for ctx.Err() == nil {
		// Garbage collection remains safe while restore holds prevent delivery.
		g.maintainStore(ctx)
		view, release, err := store.ClaimWorker(ctx)
		if err == nil {
			worker := &Gateway{store: view, reader: g.reader, provider: g.provider, principals: g.principals,
				functions: g.functions, sessionWriter: g.sessionWriter,
				profiles: g.profiles, gates: g.gates,
				watches: make(map[string]*watch), metrics: g.metricState()}
			workerCtx, cancel := context.WithCancel(ctx)
			joined := make(chan struct{})
			go func() { defer close(joined); _ = worker.runWorker(workerCtx) }()
			g.mu.Lock()
			g.worker = worker
			g.mu.Unlock()
			heartbeat := time.NewTicker(5 * time.Second)
			running := true
			for running {
				select {
				case <-ctx.Done():
					running = false
				case <-joined:
					running = false
				case <-heartbeat.C:
					if owned, ok := view.(*PostgreSQLStore); ok && owned.renew(workerCtx) != nil {
						running = false
					}
				}
			}
			heartbeat.Stop()
			g.metricState().workerLeader.Store(0)
			cancel()
			<-joined
			release()
			g.mu.Lock()
			g.worker = nil
			g.mu.Unlock()
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
	return nil
}
