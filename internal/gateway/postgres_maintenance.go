package gateway

import (
	"context"
	"time"

	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

func (g *Gateway) maintainStore(ctx context.Context) {
	if store, ok := g.store.(*PostgreSQLStore); ok {
		if err := store.pruneCommitMarkers(ctx); err != nil && ctx.Err() == nil {
			logf.FromContext(ctx).Error(err, "Could not prune expired gateway commit markers")
		}
	}
}

const (
	_commitRetention     = time.Hour // Exceeds a 30-minute admin transaction and its outcome lookup.
	_commitPruneInterval = time.Minute
	_commitPruneTimeout  = time.Second
	_commitPruneBatch    = 1000
)

// pruneCommitMarkers is independent of acceptance transactions. Its failure
// cannot change a committed receipt, and recent outcome evidence is never pruned.
func (s *PostgreSQLStore) pruneCommitMarkers(ctx context.Context) error {
	now := time.Now()
	due := s.pruneAfter.Load()
	next := now.Add(_commitPruneInterval).UnixNano()
	if now.UnixNano() < due || !s.pruneAfter.CompareAndSwap(due, next) {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, _commitPruneTimeout)
	defer cancel()
	result, err := s.pool.Exec(ctx, `DELETE FROM gateway_commits WHERE mutation IN (
 SELECT mutation FROM gateway_commits
 WHERE committed_at < clock_timestamp() - $1::double precision * interval '1 second'
 LIMIT $2 FOR UPDATE SKIP LOCKED)`, _commitRetention.Seconds(), _commitPruneBatch)
	if err != nil {
		return safeSQLError(err)
	}
	if result.RowsAffected() == _commitPruneBatch {
		// Drain a backlog one bounded batch per worker pass; otherwise wait a minute.
		s.pruneAfter.CompareAndSwap(next, 0)
	}
	return nil
}
