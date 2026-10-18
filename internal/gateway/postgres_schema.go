package gateway

import (
	"context"
	_ "embed"
	"errors"
)

const _postgresSchemaVersion = 1

//go:embed schema.sql
var _postgresInitialDDL string

func (s *PostgreSQLStore) initialize(ctx context.Context, options PostgreSQLConfig) error {
	ctx, cancel := context.WithTimeout(ctx, _sqlTimeout)
	defer cancel()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return safeSQLError(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	// Serialize startup schema initialization across gateway replicas.
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(731904221)"); err != nil {
		return safeSQLError(err)
	}
	if err := checkDurability(ctx, tx); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, _postgresInitialDDL); err != nil {
		return safeSQLError(err)
	}
	if _, err := tx.Exec(
		ctx,
		`INSERT INTO gateway_meta(id,version,max_active,max_records)
		 VALUES(1,$1,$2,$3) ON CONFLICT(id) DO NOTHING`,
		_postgresSchemaVersion,
		options.MaxActive,
		options.MaxRecords,
	); err != nil {
		return safeSQLError(err)
	}
	var active, retained, version int
	if err := tx.QueryRow(ctx, "SELECT version,max_active,max_records FROM gateway_meta WHERE id=1").Scan(
		&version,
		&active,
		&retained,
	); err != nil {
		return safeSQLError(err)
	}
	if version != _postgresSchemaVersion {
		return errors.New("unsupported gateway database schema")
	}
	if active != options.MaxActive || retained != options.MaxRecords {
		return errors.New("gateway database quotas differ from configured quotas")
	}
	if err := tx.Commit(ctx); err != nil {
		return safeSQLError(err)
	}
	return nil
}
