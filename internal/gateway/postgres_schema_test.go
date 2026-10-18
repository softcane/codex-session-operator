package gateway

import (
	"sync"
	"testing"
)

func TestPostgreSQLStartupInitializesAndPreservesData(t *testing.T) {
	dsn := postgresDatabaseForTest(t)
	options := PostgreSQLConfig{
		DSN:                dsn,
		MaxActive:          7,
		MaxRecords:         71,
		AllowInsecureLocal: true,
	}
	store, err := OpenPostgreSQL(t.Context(), options)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Update(t.Context(), "startup-preserved", func(record *Record) error {
		record.Namespace = _testNamespace
		record.Notes["state"] = "preserved"
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenPostgreSQL(t.Context(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.Close() }()
	record, err := reopened.Get(t.Context(), "startup-preserved")
	if err != nil {
		t.Fatal(err)
	}
	if record.Notes["state"] != "preserved" {
		t.Fatal("startup schema initialization changed existing data")
	}
}

func TestPostgreSQLStartupSerializesConcurrentReplicas(t *testing.T) {
	dsn := postgresDatabaseForTest(t)
	options := PostgreSQLConfig{DSN: dsn, AllowInsecureLocal: true}
	type result struct {
		store *PostgreSQLStore
		err   error
	}
	results := make(chan result, 8)
	var group sync.WaitGroup
	for range 8 {
		group.Go(func() {
			store, err := OpenPostgreSQL(t.Context(), options)
			results <- result{store: store, err: err}
		})
	}
	group.Wait()
	close(results)
	for result := range results {
		if result.err != nil {
			t.Fatal(result.err)
		}
		if err := result.store.Close(); err != nil {
			t.Fatal(err)
		}
	}

	reopened, err := OpenPostgreSQL(t.Context(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.Close() }()
	var metadataRows int
	if err := reopened.pool.QueryRow(t.Context(), "SELECT count(*) FROM gateway_meta").Scan(&metadataRows); err != nil {
		t.Fatal(safeSQLError(err))
	}
	if metadataRows != 1 {
		t.Fatalf("gateway metadata rows = %d, want 1", metadataRows)
	}
}
