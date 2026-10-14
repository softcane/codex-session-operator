package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/softcane/codex-session-operator/internal/openaiapi"
)

const _sqlTimeout = 10 * time.Second

var (
	errWorkerHeld  = errors.New("gateway delivery worker already claimed")
	errFenced      = errors.New("gateway delivery worker ownership lost")
	errRestoreHeld = errors.New("gateway delivery disabled pending restore reconciliation")
)

// PostgreSQLConfig separates active execution reservations from retained records.
// AllowInsecureLocal is only for a disposable local database; production requires
// sslmode=verify-full, a trust root and no plaintext fallback connections.
type PostgreSQLConfig struct {
	DSN                string
	MaxActive          int
	MaxRecords         int
	AllowInsecureLocal bool
}

type PostgreSQLStore struct {
	pool  *pgxpool.Pool
	owner string
	epoch int64
}

var _ DeliveryStorage = (*PostgreSQLStore)(nil)

const _schema = `
CREATE TABLE IF NOT EXISTS gateway_meta (
 id integer PRIMARY KEY CHECK (id=1), version integer NOT NULL CHECK(version=1),
 max_active integer NOT NULL, max_records integer NOT NULL,
 delivery_disabled boolean NOT NULL DEFAULT false,
 owner text NOT NULL DEFAULT '', epoch bigint NOT NULL DEFAULT 0,
 expires timestamptz NOT NULL DEFAULT '-infinity');
CREATE TABLE IF NOT EXISTS gateway_records (
 uid text PRIMARY KEY, namespace text NOT NULL, data bytea NOT NULL,
 mutation text NOT NULL, archived boolean NOT NULL DEFAULT false);
CREATE INDEX IF NOT EXISTS gateway_namespace ON gateway_records(namespace);
CREATE TABLE IF NOT EXISTS gateway_history (
 uid text PRIMARY KEY REFERENCES gateway_records(uid) ON DELETE CASCADE, data bytea NOT NULL);
CREATE TABLE IF NOT EXISTS gateway_keys (
 kind text NOT NULL, namespace text NOT NULL, scope text NOT NULL, key text NOT NULL,
 uid text NOT NULL REFERENCES gateway_records(uid) ON DELETE CASCADE, digest text NOT NULL,
 PRIMARY KEY(kind, namespace, scope, key));
CREATE TABLE IF NOT EXISTS gateway_reservations (
 uid text NOT NULL REFERENCES gateway_records(uid) ON DELETE CASCADE, slot text NOT NULL,
 PRIMARY KEY(uid, slot));
CREATE TABLE IF NOT EXISTS gateway_commits (
 mutation text PRIMARY KEY, committed_at timestamptz NOT NULL DEFAULT clock_timestamp());
`

func OpenPostgreSQL(ctx context.Context, options PostgreSQLConfig) (*PostgreSQLStore, error) {
	config, err := pgxpool.ParseConfig(options.DSN)
	if err != nil {
		return nil, errors.New("invalid gateway database configuration")
	}
	if !options.AllowInsecureLocal {
		// Checking the parsed TLS config alone is insufficient: pgx supports
		// plaintext and unverified fallbacks for prefer/require modes.
		if !strings.Contains(options.DSN, "sslmode=verify-full") || config.ConnConfig.TLSConfig == nil || config.ConnConfig.TLSConfig.InsecureSkipVerify || len(config.ConnConfig.Fallbacks) != 0 {
			return nil, errors.New("gateway database requires sslmode=verify-full")
		}
	}
	if options.MaxActive <= 0 {
		options.MaxActive = 32
	}
	if options.MaxRecords <= 0 {
		options.MaxRecords = 100000
	}
	config.ConnConfig.ConnectTimeout = _sqlTimeout
	config.ConnConfig.RuntimeParams["synchronous_commit"] = "on"
	config.ConnConfig.RuntimeParams["application_name"] = "codex-gateway"
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, safeSQLError(err)
	}
	store := &PostgreSQLStore{pool: pool}
	if err := store.initialize(ctx, options); err != nil {
		pool.Close()
		return nil, err
	}
	return store, nil
}

func (s *PostgreSQLStore) initialize(ctx context.Context, options PostgreSQLConfig) error {
	ctx, cancel := context.WithTimeout(ctx, _sqlTimeout)
	defer cancel()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return safeSQLError(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	// Serialize initial DDL even when two replicas open a fresh database.
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(731904221)"); err != nil {
		return safeSQLError(err)
	}
	if err := checkDurability(ctx, tx); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, _schema); err != nil {
		return safeSQLError(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO gateway_meta(id,version,max_active,max_records) VALUES(1,1,$1,$2) ON CONFLICT(id) DO NOTHING`, options.MaxActive, options.MaxRecords); err != nil {
		return safeSQLError(err)
	}
	var active, retained, version int
	if err := tx.QueryRow(ctx, "SELECT version,max_active,max_records FROM gateway_meta WHERE id=1").Scan(&version, &active, &retained); err != nil {
		return safeSQLError(err)
	}
	if version != 1 {
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

func checkDurability(ctx context.Context, tx pgx.Tx) error {
	var fsync, full, synchronous string
	if err := tx.QueryRow(ctx, "SELECT current_setting('fsync'),current_setting('full_page_writes'),current_setting('synchronous_commit')").Scan(&fsync, &full, &synchronous); err != nil {
		return safeSQLError(err)
	}
	if fsync != "on" || full != "on" || synchronous != "on" {
		return errors.New("gateway database durability settings unsafe")
	}
	return nil
}

func (s *PostgreSQLStore) Close() error {
	// Worker views borrow the pool from the HTTP store.
	if s.owner == "" {
		s.pool.Close()
	}
	return nil
}

func (s *PostgreSQLStore) DeliveryAllowed(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, _sqlTimeout)
	defer cancel()
	var disabled bool
	if err := s.pool.QueryRow(ctx, "SELECT delivery_disabled FROM gateway_meta WHERE id=1").Scan(&disabled); err != nil {
		return safeSQLError(err)
	}
	if disabled {
		return errRestoreHeld
	}
	return nil
}

func (s *PostgreSQLStore) begin(ctx context.Context) (pgx.Tx, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, safeSQLError(err)
	}
	var owner string
	var epoch int64
	var live, disabled bool
	err = tx.QueryRow(ctx, `SELECT owner,epoch,expires>clock_timestamp(),delivery_disabled FROM gateway_meta WHERE id=1 FOR UPDATE`).Scan(&owner, &epoch, &live, &disabled)
	if err == nil {
		err = checkDurability(ctx, tx)
	}
	if err == nil && s.owner != "" {
		if disabled {
			err = errRestoreHeld
		} else if owner != s.owner || epoch != s.epoch || !live {
			err = errFenced
		}
	}
	if err != nil {
		_ = tx.Rollback(ctx)
		return nil, safeSQLError(err)
	}
	return tx, nil
}

// beginRead gives HTTP and maintenance readers a consistent snapshot without
// serializing unrelated writes. Worker views retain the existing ownership lock:
// a repeatable-read snapshot alone cannot observe a later lease takeover.
func (s *PostgreSQLStore) beginRead(ctx context.Context) (pgx.Tx, error) {
	if s.owner != "" {
		return s.begin(ctx)
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, safeSQLError(err)
	}
	var version int
	if err := tx.QueryRow(ctx, "SELECT version FROM gateway_meta WHERE id=1").Scan(&version); err != nil {
		_ = tx.Rollback(ctx)
		return nil, safeSQLError(err)
	}
	if version != 1 {
		_ = tx.Rollback(ctx)
		return nil, errUnavailable
	}
	if err := checkDurability(ctx, tx); err != nil {
		_ = tx.Rollback(ctx)
		return nil, err
	}
	return tx, nil
}

func safeSQLError(err error) error {
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	if errors.Is(err, errConflict) || errors.Is(err, errFenced) || errors.Is(err, errRestoreHeld) {
		return err
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return errConflict
	}
	return errUnavailable // Driver errors can contain credentials and SQL values.
}

type storedHistory struct {
	History openaiapi.History `json:"history"`
	Events  []TimelineEvent   `json:"events"`
	Notes   map[string]string `json:"notes"`
}

func readSQLRecord(ctx context.Context, tx pgx.Tx, uid string) (Record, error) {
	return readSQLRecordNamespace(ctx, tx, "", uid)
}

func readSQLRecordNamespace(ctx context.Context, tx pgx.Tx, namespace, uid string) (Record, error) {
	var raw, history []byte
	var storedNamespace string
	err := tx.QueryRow(ctx, `SELECT r.namespace,r.data,h.data FROM gateway_records r LEFT JOIN gateway_history h USING(uid) WHERE r.uid=$1 AND ($2='' OR r.namespace=$2)`, uid, namespace).Scan(&storedNamespace, &raw, &history)
	if errors.Is(err, pgx.ErrNoRows) {
		return Record{}, os.ErrNotExist
	}
	if err != nil {
		return Record{}, safeSQLError(err)
	}
	return decodeSQLRecord(uid, storedNamespace, raw, history)
}

func decodeSQLRecord(uid, namespace string, raw, history []byte) (Record, error) {
	var record Record
	if json.Unmarshal(raw, &record) != nil || record.UID != uid || record.Namespace != namespace || record.Actions == nil {
		return Record{}, errUnavailable
	}
	if err := validateRecordVersion(record); err != nil {
		return Record{}, errUnavailable
	}
	if len(history) > 0 {
		var saved storedHistory
		if json.Unmarshal(history, &saved) != nil {
			return Record{}, errUnavailable
		}
		record.History, record.Events, record.Notes = saved.History, saved.Events, saved.Notes
	}
	if record.Notes == nil {
		record.Notes = make(map[string]string)
	}
	return record, nil
}

// collectSQLRecords materializes a joined record/history result in one query,
// rather than keeping a transaction open for one round trip per retained UID.
func collectSQLRecords(rows pgx.Rows) ([]Record, error) {
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (Record, error) {
		var uid, namespace string
		var raw, history []byte
		if err := row.Scan(&uid, &namespace, &raw, &history); err != nil {
			return Record{}, safeSQLError(err)
		}
		return decodeSQLRecord(uid, namespace, raw, history)
	})
}

func (s *PostgreSQLStore) Get(ctx context.Context, uid string) (Record, error) {
	return s.GetInNamespace(ctx, "", uid)
}

func (s *PostgreSQLStore) GetInNamespace(ctx context.Context, namespace, uid string) (Record, error) {
	ctx, cancel := context.WithTimeout(ctx, _sqlTimeout)
	defer cancel()
	tx, err := s.beginRead(ctx)
	if err != nil {
		return Record{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	return readSQLRecordNamespace(ctx, tx, namespace, uid)
}

func listSQLRecords(ctx context.Context, tx pgx.Tx) ([]Record, error) {
	return listSQLRecordsNamespace(ctx, tx, "")
}

func listSQLRecordsNamespace(ctx context.Context, tx pgx.Tx, namespace string) ([]Record, error) {
	rows, err := tx.Query(ctx, `SELECT r.uid,r.namespace,r.data,h.data FROM gateway_records r LEFT JOIN gateway_history h USING(uid) WHERE ($1='' OR r.namespace=$1) ORDER BY r.uid`, namespace)
	if err != nil {
		return nil, safeSQLError(err)
	}
	records, err := collectSQLRecords(rows)
	if err != nil {
		return nil, safeSQLError(err)
	}
	return records, nil
}

func (s *PostgreSQLStore) Records(ctx context.Context) ([]Record, error) {
	return s.RecordsInNamespace(ctx, "")
}

func (s *PostgreSQLStore) RecordsInNamespace(ctx context.Context, namespace string) ([]Record, error) {
	ctx, cancel := context.WithTimeout(ctx, _sqlTimeout)
	defer cancel()
	tx, err := s.beginRead(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	return listSQLRecordsNamespace(ctx, tx, namespace)
}

func validateRecord(record Record) error {
	if record.UID == "" || record.Actions == nil || record.Notes == nil || record.Work != nil && record.Issue != nil {
		return errConflict
	}
	if record.Work != nil && (record.Work.ID != record.UID || record.Work.RouteID == "" || record.Work.Source == "" || record.Work.ExternalID == "") {
		return errConflict
	}
	if record.Issue != nil && (record.Issue.ID != record.UID || record.Issue.RouteID == "" || len(record.Issue.Deliveries) == 0 || record.Issue.Policy.AgentRef == nil || record.Issue.Policy.EnvironmentRef == nil) {
		return errConflict
	}
	if err := validateRecordVersion(record); err != nil {
		return err
	}
	raw, err := json.Marshal(record)
	if err != nil || len(raw) > _maxRecordBytes {
		return errors.New("gateway record size limit reached")
	}
	return nil
}

func validateRecordVersion(record Record) error {
	if record.Work != nil && record.Issue != nil {
		return errConflict
	}
	switch record.Version {
	case 1:
		if record.Work != nil && record.Work.Conversation != nil {
			return errConflict
		}
	case 2:
		if record.Work == nil || record.Work.Conversation == nil {
			return errConflict
		}
	default:
		return errConflict
	}
	return validateConversationRecord(record)
}

func validateConversationRecord(record Record) error {
	if record.Work == nil {
		return nil
	}
	w := record.Work
	if w.Conversation == nil {
		for _, attempt := range w.Attempts {
			if attempt.ConversationID != "" || attempt.InputsArchived {
				return errConflict
			}
		}
		return nil
	}
	origin := w.Conversation
	if origin.Subject == "" || origin.Key == "" || len(origin.Digest) != sha256.Size*2 ||
		w.ID != record.UID || w.ExternalID != record.UID || w.RouteID == "" || w.Source == "" ||
		record.UID != conversationRecordID(record.Namespace, origin.Subject, origin.Key) ||
		len(w.Attempts) == 0 || len(w.Events) == 0 ||
		w.Events[0].Kind != _workTask || w.Events[0].Attempt != 1 || w.Events[0].Digest != origin.Digest {
		return errConflict
	}
	if _, err := hex.DecodeString(origin.Digest); err != nil {
		return errConflict
	}
	for i, attempt := range w.Attempts {
		if attempt.Number != i+1 || attempt.ConversationID != conversationID(record.UID, attempt.Number) {
			return errConflict
		}
	}
	return nil
}

func (s *PostgreSQLStore) Update(ctx context.Context, uid string, change func(*Record) error) error {
	ctx, cancel := context.WithTimeout(ctx, _sqlTimeout)
	defer cancel()
	tx, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	record, err := readSQLRecord(ctx, tx, uid)
	if errors.Is(err, os.ErrNotExist) {
		record = Record{Version: 1, UID: uid, Actions: make(map[string]Action), Notes: make(map[string]string)}
	} else if err != nil {
		return err
	}
	namespace := record.Namespace
	previous := record
	var previousEvents, previousAttempts int
	var previousSessionUID string
	if record.Work != nil {
		previousEvents, previousAttempts = len(record.Work.Events), len(record.Work.Attempts)
		if previousAttempts > 0 {
			previousSessionUID = record.Work.Attempts[previousAttempts-1].SessionUID
		}
	}
	if err := change(&record); err != nil {
		return err
	}
	if record.UID != uid || namespace != "" && namespace != record.Namespace {
		return errConflict
	}
	if previous.RemoteID != "" && previous.RemoteID != record.RemoteID {
		return errConflict
	}
	if err := validateRecord(record); err != nil {
		return err
	}
	if record.Work != nil && record.Work.Conversation != nil &&
		(len(record.Work.Events) > previousEvents || len(record.Work.Attempts) > previousAttempts) {
		// The metadata lock serializes admission with restore/resume, including
		// changes to the bound Session record's hold after the HTTP preflight.
		var disabled bool
		if err := tx.QueryRow(ctx, "SELECT delivery_disabled FROM gateway_meta WHERE id=1").Scan(&disabled); err != nil {
			return safeSQLError(err)
		}
		if disabled || previous.DeliveryHeld {
			return errRestoreHeld
		}
		if previousSessionUID != "" {
			kind := record.Work.Events[len(record.Work.Events)-1].Kind
			if err := conversationSessionInputAllowed(ctx, tx, record.Namespace, previousSessionUID, kind); err != nil {
				return err
			}
		}
	}
	mutation := uuid.NewString()
	if err := writeSQLRecord(ctx, tx, record, mutation); err != nil {
		return err
	}
	if err := checkQuotas(ctx, tx); err != nil {
		return err
	}
	return s.commit(ctx, tx, mutation)
}

func conversationSessionInputAllowed(ctx context.Context, tx pgx.Tx, namespace, uid, kind string) error {
	var raw []byte
	err := tx.QueryRow(ctx, "SELECT data FROM gateway_records WHERE namespace=$1 AND uid=$2", namespace, uid).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return safeSQLError(err)
	}
	var record Record
	if json.Unmarshal(raw, &record) != nil || record.UID != uid || record.Namespace != namespace {
		return errUnavailable
	}
	if err := validateRecordVersion(record); err != nil {
		return errUnavailable
	}
	if record.DeliveryHeld {
		return errRestoreHeld
	}
	if record.Closed && kind == _workEvidence {
		return errClosed
	}
	return nil
}

func (s *PostgreSQLStore) commit(ctx context.Context, tx pgx.Tx, mutation string) error {
	if s.owner != "" {
		var live bool
		err := tx.QueryRow(ctx, "SELECT owner=$1 AND epoch=$2 AND expires>clock_timestamp() AND NOT delivery_disabled FROM gateway_meta WHERE id=1", s.owner, s.epoch).Scan(&live)
		if err != nil {
			return safeSQLError(err)
		}
		if !live {
			return errFenced
		}
	}

	if _, err := tx.Exec(ctx, "INSERT INTO gateway_commits(mutation) VALUES($1)", mutation); err != nil {
		return safeSQLError(err)
	}
	if err := tx.Commit(ctx); err == nil {
		return nil
	}
	// A lost COMMIT response is resolved by this transaction's durable identity.
	// Never rerun a callback or proceed to remote I/O on an unobserved outcome.
	checkCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), _sqlTimeout)
	defer cancel()
	var found bool
	if err := s.pool.QueryRow(checkCtx, "SELECT EXISTS(SELECT 1 FROM gateway_commits WHERE mutation=$1)", mutation).Scan(&found); err == nil && found {
		return nil
	}
	return errUnavailable
}

func writeSQLRecord(ctx context.Context, tx pgx.Tx, record Record, mutation string) error {
	content, err := json.Marshal(storedHistory{History: record.History, Events: record.Events, Notes: record.Notes})
	if err != nil {
		return safeSQLError(err)
	}
	record.History = openaiapi.History{}
	record.Events = nil
	record.Notes = nil
	raw, err := json.Marshal(record)
	if err != nil {
		return safeSQLError(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO gateway_records(uid,namespace,data,mutation,archived) VALUES($1,$2,$3,$4,$5) ON CONFLICT(uid) DO UPDATE SET namespace=excluded.namespace,data=excluded.data,mutation=excluded.mutation,archived=excluded.archived`, record.UID, record.Namespace, raw, mutation, record.Archived); err != nil {
		return safeSQLError(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO gateway_history(uid,data) VALUES($1,$2) ON CONFLICT(uid) DO UPDATE SET data=excluded.data`, record.UID, content); err != nil {
		return safeSQLError(err)
	}
	if err := writeKeys(ctx, tx, record); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, "DELETE FROM gateway_reservations WHERE uid=$1", record.UID); err != nil {
		return safeSQLError(err)
	}
	for _, slot := range activeSlots(record) {
		if _, err := tx.Exec(ctx, "INSERT INTO gateway_reservations(uid,slot) VALUES($1,$2) ON CONFLICT DO NOTHING", record.UID, slot); err != nil {
			return safeSQLError(err)
		}
	}
	return nil
}

func digestValue(value any) string {
	raw, _ := json.Marshal(value)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func writeKeys(ctx context.Context, tx pgx.Tx, r Record) error {
	put := func(kind, scope, key, digest string) error {
		var uid, previous string
		err := tx.QueryRow(ctx, `INSERT INTO gateway_keys(kind,namespace,scope,key,uid,digest) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(kind,namespace,scope,key) DO UPDATE SET key=excluded.key RETURNING uid,digest`, kind, r.Namespace, scope, key, r.UID, digest).Scan(&uid, &previous)
		if err != nil {
			return safeSQLError(err)
		}
		if uid != r.UID || previous != digest {
			return errConflict
		}
		return nil
	}
	for _, submission := range r.Submissions {
		// Initial changes to a regular submission during hosted-mode discovery;
		// the immutable caller key and text, rather than delivery state, identify it.
		event := ""
		if submission.Text == "" {
			event = string(submission.Event)
		}
		digest := digestValue([]string{submission.Text, submission.TargetTurn, event})
		if err := put("submission", r.UID, submission.Key, digest); err != nil {
			return err
		}
	}
	if r.Work != nil {
		w := r.Work
		if w.Conversation != nil {
			origin := w.Conversation
			if err := put("conversation-create", origin.Subject, origin.Key, origin.Digest); err != nil {
				return err
			}
			for _, attempt := range w.Attempts {
				if err := put("conversation", "", attempt.ConversationID, strconv.Itoa(attempt.Number)); err != nil {
					return err
				}
			}
		}
		if err := put("work", digestValue([]string{w.RouteID, w.Source}), w.ExternalID, r.UID); err != nil {
			return err
		}
		for _, e := range w.Events {
			if err := put("work-event", w.RouteID, e.ID, e.Digest); err != nil {
				return err
			}
		}
	}
	if r.Issue != nil {
		if err := put("issue", "github", r.Issue.StableID, r.UID); err != nil {
			return err
		}
		for _, e := range r.Issue.Deliveries {
			if err := put("issue-event", r.Issue.RouteID, e.ID, e.Digest); err != nil {
				return err
			}
		}
	}
	return nil
}

func activeSlots(r Record) []string {
	if closedForPurge(r) {
		return nil
	}
	if r.Work != nil {
		var slots []string
		for _, a := range r.Work.Attempts {
			if !a.Cleaned {
				slot := a.SessionUID
				if slot == "" {
					slot = r.UID + ":" + a.SessionName
				}
				slots = append(slots, slot)
			}
		}
		return slots
	}
	if r.Issue != nil {
		slot := r.Issue.SessionUID
		if slot == "" {
			slot = r.UID + ":session"
		}
		return []string{slot}
	}
	return []string{r.UID}
}

func checkQuotas(ctx context.Context, tx pgx.Tx) error {
	var active, retained, maxActive, maxRecords int
	err := tx.QueryRow(ctx, `SELECT (SELECT count(DISTINCT slot) FROM gateway_reservations),(SELECT count(*) FROM gateway_records)+(SELECT count(DISTINCT slot) FROM gateway_reservations p WHERE NOT EXISTS(SELECT 1 FROM gateway_records r WHERE r.uid=p.slot)),max_active,max_records FROM gateway_meta WHERE id=1`).Scan(&active, &retained, &maxActive, &maxRecords)
	if err != nil {
		return safeSQLError(err)
	}
	if active > maxActive || retained > maxRecords {
		return errors.New("gateway storage quota reached")
	}
	return nil
}

func (s *PostgreSQLStore) Purge(ctx context.Context, uid string) error {
	ctx, cancel := context.WithTimeout(ctx, _sqlTimeout)
	defer cancel()
	tx, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	r, err := readSQLRecord(ctx, tx, uid)
	if err != nil {
		return err
	}
	if !closedForPurge(r) || r.Work != nil || r.Issue != nil {
		return errors.New("gateway active or source replay record cannot be purged")
	}
	if _, err := tx.Exec(ctx, "DELETE FROM gateway_records WHERE uid=$1", uid); err != nil {
		return safeSQLError(err)
	}
	return s.commit(ctx, tx, uuid.NewString())
}

// Archive drops closed transcript content but keeps receipts, digests, policies,
// operation IDs and cleanup claims. APIs return archived:true and empty history.
func (s *PostgreSQLStore) Archive(ctx context.Context, uid string) error {
	return s.Update(ctx, uid, func(r *Record) error {
		if !closedForPurge(*r) {
			return errClosed
		}
		r.Archived = true
		r.History = openaiapi.History{}
		r.Events = nil
		r.Notes = make(map[string]string)
		if r.Work != nil {
			if r.Work.Conversation != nil {
				r.Work.Conversation.Title = ""
				for i := range r.Work.Attempts {
					r.Work.Attempts[i].InputsArchived = true
				}
			}
			for i := range r.Work.Events {
				r.Work.Events[i].Text = ""
			}
		}
		return nil
	})
}
