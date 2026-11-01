package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// StorageReport contains no transcripts, credentials or driver error details.
type StorageReport struct {
	Records          int               `json:"records"`
	DeliveryDisabled bool              `json:"delivery_disabled"`
	Digests          map[string]string `json:"digests"`
}

type StorageBackup struct {
	Version   int       `json:"version"`
	CreatedAt time.Time `json:"created_at"`
	Records   []Record  `json:"records"`
	Digest    string    `json:"digest"`
}

// Backup is a consistent snapshot including replay/approval/operation identities.
// Treat the serialized backup as confidential conversation data (0600/encrypted
// storage). Native pg_dump is also supported but requires an offline restore gate.
func (s *PostgreSQLStore) Backup(ctx context.Context) (StorageBackup, error) {
	createdAt := time.Now().UTC()
	ctx, cancel := context.WithTimeout(ctx, 30*time.Minute)
	defer cancel()
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return StorageBackup{}, safeSQLError(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	records, err := listSQLRecords(ctx, tx)
	if err != nil {
		return StorageBackup{}, err
	}
	return StorageBackup{Version: 1, CreatedAt: createdAt, Records: records, Digest: digestValue(records)}, nil
}

func (s *PostgreSQLStore) Restore(ctx context.Context, backup StorageBackup) (StorageReport, error) {
	if backup.Version != 1 || backup.CreatedAt.IsZero() || backup.Digest != digestValue(backup.Records) {
		return StorageReport{}, errors.New("invalid gateway backup")
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Minute)
	defer cancel()
	tx, err := s.begin(ctx)
	if err != nil {
		return StorageReport{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var count int
	if err := tx.QueryRow(ctx, "SELECT count(*) FROM gateway_records").Scan(&count); err != nil {
		return StorageReport{}, safeSQLError(err)
	}
	if count != 0 {
		return StorageReport{}, errors.New("restore requires an empty gateway database")
	}
	report := StorageReport{Records: len(backup.Records), DeliveryDisabled: true, Digests: make(map[string]string)}
	mutation := uuid.NewString()
	for _, r := range backup.Records {
		if r.Version == 3 && r.Task == nil && r.TaskMember == nil {
			r.Version = _recordVersion
		}
		if err := validateRecord(r); err != nil {
			return StorageReport{}, err
		}
		if _, exists := report.Digests[r.UID]; exists {
			return StorageReport{}, errConflict
		}
		if err := writeSQLRecord(ctx, tx, r, mutation); err != nil {
			return StorageReport{}, err
		}
		report.Digests[r.UID] = digestValue(r)
	}
	if err := validateRestoredTaskLinks(ctx, tx, backup.Records); err != nil {
		return StorageReport{}, err
	}
	if err := checkQuotas(ctx, tx); err != nil {
		return StorageReport{}, err
	}
	if _, err := tx.Exec(ctx, "UPDATE gateway_meta SET delivery_disabled=true,owner='',epoch=epoch+1,expires='-infinity' WHERE id=1"); err != nil {
		return StorageReport{}, safeSQLError(err)
	}
	if err := s.commit(ctx, tx, mutation); err != nil {
		return StorageReport{}, err
	}
	return report, nil
}

type Reconciliation struct {
	UID    string `json:"uid"`
	Digest string `json:"digest"`
	// Reference identifies the administrator's Kubernetes/provider/log audit.
	Reference string `json:"reference"`
	// Blocked preserves unresolved gaps without authorizing any remote work.
	Blocked bool `json:"blocked"`
	// Verified is an explicit attestation; an omitted decision never resumes work.
	Verified bool `json:"verified"`
}

// ResumeDelivery requires an explicit outcome audit for every exact restored
// record. It never edits existing one-time creation, delivery or action claims.
func (s *PostgreSQLStore) ResumeDelivery(ctx context.Context, audit []Reconciliation) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Minute)
	defer cancel()
	tx, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	records, err := listSQLRecords(ctx, tx)
	if err != nil {
		return err
	}
	if len(audit) != len(records) {
		return errors.New("reconciliation must cover every restored identity")
	}
	byID := make(map[string]Reconciliation, len(audit))
	for _, item := range audit {
		if _, exists := byID[item.UID]; exists || item.Reference == "" || item.Blocked == item.Verified {
			return errConflict
		}
		byID[item.UID] = item
	}
	mutation := uuid.NewString()
	for _, r := range records {
		item, ok := byID[r.UID]
		if !ok || item.Digest != digestValue(r) {
			return errConflict
		}
		r.DeliveryHeld = item.Blocked
		if err := writeSQLRecord(ctx, tx, r, mutation); err != nil {
			return err
		}
	}
	if _, err := tx.Exec(ctx, "UPDATE gateway_meta SET delivery_disabled=false,owner='',epoch=epoch+1,expires='-infinity' WHERE id=1"); err != nil {
		return safeSQLError(err)
	}
	return s.commit(ctx, tx, mutation)
}

func (s *PostgreSQLStore) Report(ctx context.Context) (StorageReport, error) {
	records, err := s.Records(ctx)
	if err != nil {
		return StorageReport{}, err
	}
	report := StorageReport{Records: len(records), Digests: make(map[string]string, len(records))}
	for _, r := range records {
		report.Digests[r.UID] = digestValue(r)
	}
	err = s.DeliveryAllowed(ctx)
	if err != nil && !errors.Is(err, errRestoreHeld) {
		return StorageReport{}, err
	}
	report.DeliveryDisabled = errors.Is(err, errRestoreHeld)
	return report, nil
}

// DecodeBackup rejects malformed JSON before restoring; Restore verifies its digest.
func DecodeBackup(raw []byte) (StorageBackup, error) {
	var backup StorageBackup
	if json.Unmarshal(raw, &backup) != nil {
		return backup, errors.New("invalid gateway backup")
	}
	return backup, nil
}

func validateRestoredTaskLinks(ctx context.Context, tx pgx.Tx, records []Record) error {
	for _, record := range records {
		if record.TaskMember != nil {
			parent, err := readSQLRecord(ctx, tx, record.TaskMember.TaskID)
			if err != nil {
				return errConflict
			}
			parent.DeliveryHeld = false
			parent.Task = copyCleanupTask(parent.Task)
			if err := taskAuthorityAllowed(parent, record); err != nil {
				return err
			}
		}
		if record.Task == nil {
			continue
		}
		if err := validateRestoredTaskResults(ctx, tx, record); err != nil {
			return err
		}
		for _, member := range record.Task.Members {
			child, err := readSQLRecord(ctx, tx, member.WorkID)
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			if err != nil {
				return err
			}
			if child.Work == nil || child.TaskMember == nil || child.TaskMember.TaskID != record.UID || child.TaskMember.MemberID != member.ID {
				return errConflict
			}
		}
	}
	return nil
}

// A result can be captured before its action acknowledgement, but its producer
// Session and the member's frozen Work binding already exist at acceptance.
func validateRestoredTaskResults(ctx context.Context, tx pgx.Tx, record Record) error {
	for _, result := range record.Task.Results {
		member, ok := taskMember(*record.Task, result.MemberID)
		if !ok {
			return errConflict
		}
		work, err := readSQLRecord(ctx, tx, member.WorkID)
		if err != nil || work.Work == nil {
			return errConflict
		}
		matched := false
		for _, attempt := range work.Work.Attempts {
			if attempt.SessionUID == result.Action.SessionUID {
				matched = true
			}
		}
		if !matched {
			return errConflict
		}
		session, err := readSQLRecord(ctx, tx, result.Action.SessionUID)
		expected := TaskMemberBinding{TaskID: record.UID, MemberID: member.ID, Role: member.Role}
		if err != nil || session.Namespace != record.Namespace || session.Work != nil || session.Task != nil || session.TaskMember == nil || *session.TaskMember != expected {
			return errConflict
		}
	}
	return nil
}
