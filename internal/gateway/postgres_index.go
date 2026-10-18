package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strconv"

	"github.com/jackc/pgx/v5"
)

var _ IndexedStorage = (*PostgreSQLStore)(nil)

var _ ConversationStorage = (*PostgreSQLStore)(nil)

func (s *PostgreSQLStore) ListConversationAttempts(ctx context.Context, namespace, after string, limit int) ([]ConversationEntry, error) {
	if limit < 1 || limit > _maxConversationPage {
		return nil, errConflict
	}
	ctx, cancel := context.WithTimeout(ctx, _sqlTimeout)
	defer cancel()
	tx, err := s.beginRead(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rows, err := tx.Query(ctx, `SELECT r.uid,r.namespace,r.data,k.key,k.digest FROM gateway_keys k JOIN gateway_records r ON r.uid=k.uid AND r.namespace=k.namespace WHERE k.kind='conversation' AND k.namespace=$1 AND k.scope='' AND k.key>$2 ORDER BY k.key LIMIT $3`, namespace, after, limit+1)
	if err != nil {
		return nil, safeSQLError(err)
	}
	entries, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (ConversationEntry, error) {
		var uid, storedNamespace, id, number string
		var raw []byte
		if err := row.Scan(&uid, &storedNamespace, &raw, &id, &number); err != nil {
			return ConversationEntry{}, safeSQLError(err)
		}
		record, err := decodeSQLRecord(uid, storedNamespace, raw, nil)
		if err != nil {
			return ConversationEntry{}, err
		}
		attempt, ok := conversationAttempt(record, id)
		if !ok || strconv.Itoa(attempt.Number) != number {
			return ConversationEntry{}, errUnavailable
		}
		record.Submissions = nil
		record.Actions = nil
		record.Notes = nil
		record.Work.Events = nil
		return ConversationEntry{Record: record, Attempt: attempt}, nil
	})
	if err != nil {
		return nil, safeSQLError(err)
	}
	return entries, nil
}

func (s *PostgreSQLStore) LookupConversation(ctx context.Context, namespace, id string) (Record, WorkAttempt, error) {
	ctx, cancel := context.WithTimeout(ctx, _sqlTimeout)
	defer cancel()
	tx, err := s.beginRead(ctx)
	if err != nil {
		return Record{}, WorkAttempt{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var uid, storedNamespace, number string
	var raw, history []byte
	err = tx.QueryRow(ctx, `SELECT r.uid,r.namespace,r.data,h.data,k.digest FROM gateway_keys k JOIN gateway_records r ON r.uid=k.uid AND r.namespace=k.namespace LEFT JOIN gateway_history h ON h.uid=r.uid WHERE k.kind='conversation' AND k.namespace=$1 AND k.scope='' AND k.key=$2`, namespace, id).Scan(&uid, &storedNamespace, &raw, &history, &number)
	if errors.Is(err, pgx.ErrNoRows) {
		return Record{}, WorkAttempt{}, os.ErrNotExist
	}
	if err != nil {
		return Record{}, WorkAttempt{}, safeSQLError(err)
	}
	r, err := decodeSQLRecord(uid, storedNamespace, raw, history)
	if err != nil {
		return Record{}, WorkAttempt{}, err
	}
	if r.Work != nil && r.Work.Conversation != nil {
		for _, attempt := range r.Work.Attempts {
			if attempt.ConversationID == id && strconv.Itoa(attempt.Number) == number {
				return r, attempt, nil
			}
		}
	}
	return Record{}, WorkAttempt{}, errUnavailable
}

func (s *PostgreSQLStore) ActiveRecords(ctx context.Context) ([]Record, error) {
	ctx, cancel := context.WithTimeout(ctx, _sqlTimeout)
	defer cancel()
	tx, err := s.beginRead(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rows, err := tx.Query(ctx, `SELECT r.uid,r.namespace,r.data,h.data FROM gateway_records r LEFT JOIN gateway_history h USING(uid) WHERE EXISTS(SELECT 1 FROM gateway_reservations p WHERE p.uid=r.uid) ORDER BY r.uid`)
	if err != nil {
		return nil, safeSQLError(err)
	}
	records, err := collectSQLRecords(rows)
	if err != nil {
		return nil, safeSQLError(err)
	}
	return records, nil
}

func (s *PostgreSQLStore) RecordSummaries(ctx context.Context, namespace, kind string) ([]Record, error) {
	ctx, cancel := context.WithTimeout(ctx, _sqlTimeout)
	defer cancel()
	tx, err := s.beginRead(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rows, err := tx.Query(ctx, `SELECT uid,namespace,data FROM gateway_records r WHERE namespace=$1 AND ($2='' OR EXISTS(SELECT 1 FROM gateway_keys k WHERE k.uid=r.uid AND k.kind=$2)) ORDER BY uid`, namespace, kind)
	if err != nil {
		return nil, safeSQLError(err)
	}
	defer rows.Close()
	var records []Record
	for rows.Next() {
		var uid, storedNamespace string
		var raw []byte
		var r Record
		if err := rows.Scan(&uid, &storedNamespace, &raw); err != nil {
			return nil, safeSQLError(err)
		}
		if json.Unmarshal(raw, &r) != nil || r.UID != uid || r.Namespace != storedNamespace {
			return nil, errUnavailable
		}
		if err := validateRecordVersion(r); err != nil {
			return nil, errUnavailable
		}
		// Never load the history table for a list/ownership lookup, and discard
		// queued text and results before retaining another summary in memory.
		r.Submissions = nil
		r.Actions = nil
		r.Notes = nil
		if r.Work != nil {
			r.Work.Events = nil
		}
		records = append(records, r)
	}
	if err := rows.Err(); err != nil {
		return nil, safeSQLError(err)
	}
	return records, nil
}
