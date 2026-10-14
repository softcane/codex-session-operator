// gateway-store performs offline backup, restore and audited delivery
// resumption. The database URL is read from a Secret file, never a command flag.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/softcane/codex-session-operator/internal/gateway"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	var action, databaseFile, path string
	var insecure bool
	var maxActive, maxRecords int
	flag.StringVar(&action, "action", "report", "report, backup, restore, resume or archive")
	flag.StringVar(&databaseFile, "database-file", os.Getenv("GATEWAY_DATABASE_FILE"), "Secret-mounted database URL")
	flag.StringVar(&path, "path", "", "private backup/audit path or archive UID")
	flag.BoolVar(&insecure, "insecure-local", false, "disposable local plaintext database")
	flag.IntVar(&maxActive, "max-active", 32, "database active quota")
	flag.IntVar(&maxRecords, "max-records", 100000, "database retention quota")
	flag.Parse()
	raw, err := os.ReadFile(databaseFile)
	if err != nil {
		return errors.New("read database Secret")
	}
	store, err := gateway.OpenPostgreSQL(ctx, gateway.PostgreSQLConfig{
		DSN:       strings.TrimSpace(string(raw)),
		MaxActive: maxActive, MaxRecords: maxRecords, AllowInsecureLocal: insecure})
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()
	var result any
	switch action {
	case "report":
		result, err = store.Report(ctx)
	case "backup":
		var backup gateway.StorageBackup
		backup, err = store.Backup(ctx)
		if err != nil {
			return err
		}
		data, encodeErr := json.Marshal(backup)
		if encodeErr != nil {
			return encodeErr
		}
		// O_EXCL avoids overwriting a good backup or following an existing symlink.
		file, openErr := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if openErr != nil {
			return errors.New("create private backup")
		}
		_, writeErr := file.Write(data)
		syncErr := file.Sync()
		closeErr := file.Close()
		if errors.Join(writeErr, syncErr, closeErr) != nil {
			return errors.New("write durable backup")
		}
		directory, dirErr := os.Open(filepath.Dir(path))
		if dirErr != nil {
			return errors.New("open backup directory")
		}
		dirSyncErr := directory.Sync()
		_ = directory.Close()
		if dirSyncErr != nil {
			return errors.New("sync backup directory")
		}
		result = map[string]any{"records": len(backup.Records), "digest": backup.Digest, "created_at": backup.CreatedAt}
	case "restore":
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			return errors.New("read private backup")
		}
		backup, decodeErr := gateway.DecodeBackup(data)
		if decodeErr != nil {
			return decodeErr
		}
		result, err = store.Restore(ctx, backup)
	case "resume":
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			return errors.New("read reconciliation audit")
		}
		var audit []gateway.Reconciliation
		if json.Unmarshal(data, &audit) != nil {
			return errors.New("invalid reconciliation audit")
		}
		err = store.ResumeDelivery(ctx, audit)
		result = map[string]bool{"delivery_enabled": err == nil}
	case "archive":
		err = store.Archive(ctx, path)
		result = map[string]bool{"archived": err == nil}
	default:
		return errors.New("unknown storage action")
	}
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(result)
}
