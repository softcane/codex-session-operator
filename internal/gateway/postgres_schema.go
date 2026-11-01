package gateway

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"
)

const _postgresSchemaVersion = 2

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
		 SELECT 1,$1,$2,$3 WHERE NOT EXISTS (SELECT 1 FROM gateway_meta WHERE id=1)`,
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
	if version == 1 {
		// Hold the metadata row while upgrading so older workers cannot write.
		if _, err := tx.Exec(ctx, `SELECT id FROM gateway_meta WHERE id=1 FOR UPDATE`); err != nil {
			return safeSQLError(err)
		}
		records, err := migrationRecords(ctx, tx)
		if err != nil {
			return err
		}
		for _, record := range records {
			if record.Version != 3 {
				return errors.New("unsupported gateway record version")
			}
			record.Version = _recordVersion
			if err := writeSQLRecord(ctx, tx, record, "schema-v2"); err != nil {
				return err
			}
		}
		if _, err := tx.Exec(ctx, `ALTER TABLE gateway_meta DROP CONSTRAINT gateway_meta_version_check; UPDATE gateway_meta SET version=2,owner='',epoch=epoch+1,expires='-infinity' WHERE id=1; ALTER TABLE gateway_meta ADD CONSTRAINT gateway_meta_version_check CHECK(version=2); ALTER TABLE gateway_records ADD CONSTRAINT gateway_record_version_check CHECK ((convert_from(data, 'UTF8')::jsonb->>'version')::integer=4)`); err != nil {
			return safeSQLError(err)
		}
		version = _postgresSchemaVersion
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

// migrationRecords reads legacy payloads without weakening normal version checks.
func migrationRecords(ctx context.Context, tx pgx.Tx) ([]Record, error) {
	rows, err := tx.Query(ctx, `SELECT r.uid,r.namespace,r.data,h.data FROM gateway_records r LEFT JOIN gateway_history h USING(uid)`)
	if err != nil {
		return nil, safeSQLError(err)
	}
	defer rows.Close()
	var records []Record
	for rows.Next() {
		var uid, namespace string
		var raw, history []byte
		if err := rows.Scan(&uid, &namespace, &raw, &history); err != nil {
			return nil, safeSQLError(err)
		}
		var fields map[string]json.RawMessage
		if json.Unmarshal(raw, &fields) != nil || string(fields["version"]) != "3" {
			return nil, errUnavailable
		}
		fields["version"] = json.RawMessage("4")
		raw, err = json.Marshal(fields)
		if err != nil {
			return nil, errUnavailable
		}
		record, err := decodeSQLRecord(uid, namespace, raw, history)
		if err != nil {
			return nil, err
		}
		record.Version = 3
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, safeSQLError(err)
	}
	return records, nil
}
