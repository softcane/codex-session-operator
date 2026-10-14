package gateway

import (
	"context"
	"encoding/json"
	"errors"
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
