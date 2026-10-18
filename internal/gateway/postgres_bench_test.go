package gateway

import (
	"fmt"
	"testing"
)

// BenchmarkPostgreSQLRecords measures materialization of retained records against
// the opt-in PostgreSQL fixture. Setup is outside the timed loop.
func BenchmarkPostgreSQLRecords(b *testing.B) {
	for _, size := range []int{1, 64} {
		b.Run(fmt.Sprintf("retained=%d", size), func(b *testing.B) {
			store, _ := postgresForTest(b)
			for i := range size {
				if err := store.Update(b.Context(), fmt.Sprintf("record-%04d", i), func(r *Record) error {
					r.Namespace = _testNamespace
					r.Closed = true
					r.Notes["fixture"] = "retained history"
					return nil
				}); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			for b.Loop() {
				records, err := store.Records(b.Context())
				if err != nil || len(records) != size {
					b.Fatalf("records=%d err=%v", len(records), err)
				}
			}
		})
	}
}
