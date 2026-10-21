package gateway

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
)

const _recordsPath = "/v1/namespaces/team/records"
const _auditAgentName = "agent"

func TestAdminClosedRecordPurgeRequiresAbsentSession(t *testing.T) {
	store := testStore(t)
	provider := simulatorForTest()
	g, k8s := testGateway(t, store, provider, testSession())

	if err := store.Update(t.Context(), "uid-1", func(r *Record) error {
		r.Namespace = _testNamespace
		r.Name = _testSessionName
		r.Closed = true
		r.RemoteID = _testRemoteID
		r.Notes["evidence"] = "retained transcript"
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if got := request(g, http.MethodGet, _recordsPath+"/uid-1", "", ""); got.Code != http.StatusOK ||
		!strings.Contains(got.Body.String(), "retained transcript") {
		t.Fatalf("admin could not inspect retained record: %d", got.Code)
	}
	if got := request(g, http.MethodGet, _recordsPath, "", ""); got.Code != http.StatusOK ||
		!containsRecord(got.Body.String(), "uid-1") {
		t.Fatalf("admin could not list closed record: %d", got.Code)
	}
	if got := request(g, http.MethodDelete, _recordsPath+"/uid-1", "", "").Code; got != http.StatusConflict {
		t.Fatalf("purged a record while its Session UID exists: %d", got)
	}
	if err := k8s.Delete(context.Background(), testSession()); err != nil {
		t.Fatal(err)
	}
	if got := request(g, http.MethodDelete, _recordsPath+"/uid-1", "", "").Code; got != http.StatusConflict {
		t.Fatalf("remote Session still exists but purge returned %d", got)
	}
	provider.mu.Lock()
	provider.purgeErr = errors.New("provider unavailable")
	provider.mu.Unlock()
	if got := request(g, http.MethodDelete, _recordsPath+"/uid-1", "", "").Code; got != http.StatusServiceUnavailable {
		t.Fatalf("provider outage did not block purge: %d", got)
	}
	provider.mu.Lock()
	provider.purgeErr = nil
	provider.remotePresent = false
	provider.mu.Unlock()
	if got := request(g, http.MethodDelete, _recordsPath+"/uid-1", "", "").Code; got != http.StatusNoContent {
		t.Fatalf("closed record purge returned %d", got)
	}
	if _, err := store.Get(t.Context(), "uid-1"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("purged record remains: %v", err)
	}
	if got := request(g, http.MethodDelete, _recordsPath+"/uid-1", "", "").Code; got != http.StatusNotFound {
		t.Fatalf("repeat purge returned %d", got)
	}
}

func TestAdminPurgeWithoutSavedRemoteIDRequiresCompleteUIDDiscovery(t *testing.T) {
	store := testStore(t)
	provider := simulatorForTest()
	g, _ := testGateway(t, store, provider)
	if err := store.Update(t.Context(), "uid-1", func(r *Record) error {
		r.Namespace = _testNamespace
		r.Name = _testSessionName
		r.Closed = true
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if got := request(g, http.MethodDelete, _recordsPath+"/uid-1", "", "").Code; got != http.StatusConflict {
		t.Fatalf("UID discovery found remote Session but purge returned %d", got)
	}
	provider.mu.Lock()
	provider.remotePresent = false
	provider.mu.Unlock()
	if got := request(g, http.MethodDelete, _recordsPath+"/uid-1", "", "").Code; got != http.StatusNoContent {
		t.Fatalf("complete no-match discovery did not permit purge: %d", got)
	}
}

func TestAdminPurgeRejectsActiveAndOtherNamespace(t *testing.T) {
	store := testStore(t)
	g, _ := testGateway(t, store, simulatorForTest())
	for uid, namespace := range map[string]string{"active": _testNamespace, "other": "another-team"} {
		if err := store.Update(t.Context(), uid, func(r *Record) error {
			r.Namespace = namespace
			r.Name = uid
			r.Closed = uid == "other"
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}

	if got := request(g, http.MethodDelete, _recordsPath+"/active", "", "").Code; got != http.StatusConflict {
		t.Fatalf("active record purge returned %d", got)
	}
	if got := request(g, http.MethodDelete, _recordsPath+"/other", "", "").Code; got != http.StatusNotFound {
		t.Fatalf("cross-namespace record purge returned %d", got)
	}
	if got := request(g, http.MethodGet, _recordsPath, "", "").Body.String(); containsRecord(got, "other") {
		t.Fatal("cross-namespace record leaked in admin list")
	}
	if err := store.Purge(t.Context(), "active"); err == nil {
		t.Fatal("store accepted direct purge of active record")
	}
}

func TestGatewayGateStateIsBoundedUnderUIDChurn(t *testing.T) {
	store := testStore(t)
	g, _ := testGateway(t, store, simulatorForTest())
	seen := make(map[*sync.Mutex]bool)
	first := g.gate(_testUID)
	for i := range 100_000 {
		seen[g.gate("uid-"+strconv.Itoa(i))] = true
	}
	if len(seen) > 64 || g.gate(_testUID) != first {
		t.Fatalf("gate state grew or changed under churn: %d mutexes", len(seen))
	}
}

func containsRecord(body, uid string) bool {
	return strings.Contains(body, `"uid":"`+uid+`"`)
}

func TestStorePurgeFreesDurableCapacity(t *testing.T) {
	store, dsn := postgresForTestLimits(t, 32, 32)
	for i := range 32 {
		uid := fmt.Sprintf("record-%d", i)
		if err := store.Update(t.Context(), uid, func(r *Record) error {
			r.Namespace = _testNamespace
			r.Name = uid
			r.Closed = true
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Update(t.Context(), "overflow", func(*Record) error { return nil }); err == nil {
		t.Fatal("store exceeded capacity")
	}
	if err := store.Purge(t.Context(), "record-0"); err != nil {
		t.Fatal(err)
	}
	if err := store.Update(t.Context(), "replacement", func(r *Record) error { r.Namespace = _testNamespace; return nil }); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenPostgreSQL(context.Background(), PostgreSQLConfig{DSN: dsn, MaxRecords: 32, AllowInsecureLocal: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.Close() }()
	if _, err := reopened.Get(t.Context(), "record-0"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("purged record reappeared: %v", err)
	}
	if _, err := reopened.Get(t.Context(), "replacement"); err != nil {
		t.Fatal(err)
	}
}
