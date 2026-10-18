package gateway

import (
	"errors"
	"net/http"
	"os"
	"reflect"
	"testing"
)

func TestPostgreSQLProfileProvenanceReplayRestoreArchiveAndVersionGates(t *testing.T) {
	store, dsn := postgresForTest(t)
	g, _, _ := profileGateway(t, store)
	receipt := readConversationReceipt(t, conversationRequest(g, http.MethodPost, _conversationPath, "profile-durable", _profileTestBody))
	id := conversationRecordID(_testNamespace, _testConversationSubject, "profile-durable")
	original, originalAttempt, err := store.LookupConversation(t.Context(), _testNamespace, receipt.ConversationID)
	if err != nil || original.Version != _recordVersion || originalAttempt.Profile == nil {
		t.Fatal("Profile provenance was not indexed durably", err)
	}
	for _, change := range []func(*Record){
		func(r *Record) { r.Work.Attempts[0].Profile.UID = "replacement-profile" },
		func(r *Record) { r.Work.Attempts[0].Policy.SpendControl.Mode = _scenarioLimited },
		func(r *Record) { r.Version = 2; r.Work.Attempts[0].Profile = nil },
	} {
		if err := store.Update(t.Context(), id, func(r *Record) error { change(r); return nil }); !errors.Is(err, errConflict) {
			t.Fatal("writer replaced frozen Profile policy/provenance", err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := reopenConversationStore(t, dsn)
	next, writer, _ := profileConversationGateway(t, reopened)
	if err := next.EnableProfiles(writer); err != nil {
		t.Fatal(err)
	}
	// The current Kubernetes client has no Profile. Replay must precede lookup.
	if replay := readConversationReceipt(t, conversationRequest(next, http.MethodPost, _conversationPath, "profile-durable", _profileTestBody)); !reflect.DeepEqual(receipt, replay) {
		t.Fatal("reopened Profile replay changed original receipt")
	}
	backup, err := reopened.Backup(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	fresh, _ := postgresForTest(t)
	report, err := fresh.Restore(t.Context(), backup)
	if err != nil {
		t.Fatal(err)
	}
	_, restoredAttempt, err := fresh.LookupConversation(t.Context(), _testNamespace, receipt.ConversationID)
	if err != nil || !reflect.DeepEqual(originalAttempt, restoredAttempt) {
		t.Fatal("restore changed v3 attempt or dependency UID expectations", err)
	}
	restoredGateway, _, _ := profileGateway(t, fresh)
	if replay := readConversationReceipt(t, conversationRequest(restoredGateway, http.MethodPost, _conversationPath, "profile-durable", _profileTestBody)); !reflect.DeepEqual(receipt, replay) {
		t.Fatal("held restore changed Profile replay")
	}
	assertConversationInputHeld(t, restoredGateway, receipt)
	if response := conversationRequest(restoredGateway, http.MethodPost, _conversationPath, "profile-held-new", _profileTestBody); response.Code != http.StatusServiceUnavailable {
		t.Fatal("restore admitted new Profile conversation", response.Code)
	}
	if err := fresh.ResumeDelivery(t.Context(), []Reconciliation{{UID: id, Digest: report.Digests[id], Reference: "test unresolved Profile create", Blocked: true}}); err != nil {
		t.Fatal(err)
	}
	assertConversationInputHeld(t, restoredGateway, receipt)
	if err := reopened.Update(t.Context(), id, func(r *Record) error {
		r.Work.State = _workClosed
		r.Work.Attempts[0].Cleaned = true
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := reopened.Archive(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	archived, attempt, err := reopened.LookupConversation(t.Context(), _testNamespace, receipt.ConversationID)
	if err != nil || !attempt.InputsArchived || !reflect.DeepEqual(attempt.Profile, originalAttempt.Profile) || archived.Work.Events[0].Text != "" {
		t.Fatal("archive lost safe provenance or retained input", err)
	}
	if replay := readConversationReceipt(t, conversationRequest(next, http.MethodPost, _conversationPath, "profile-durable", _profileTestBody)); !reflect.DeepEqual(receipt, replay) {
		t.Fatal("archived Profile replay changed acceptance")
	}
	for _, version := range []int{1, 2, 4} {
		candidate := archived
		candidate.Version = version
		if _, err := reopened.pool.Exec(t.Context(), "UPDATE gateway_records SET data=$1 WHERE uid=$2", mustJSON(t, candidate), id); err != nil {
			t.Fatal(safeSQLError(err))
		}
		if _, _, err := reopened.LookupConversation(t.Context(), _testNamespace, receipt.ConversationID); !errors.Is(err, errUnavailable) {
			t.Fatal("indexed reader trusted unsupported provenance version", version, err)
		}
		if _, err := reopened.ListConversationAttempts(t.Context(), _testNamespace, "", 100); !errors.Is(err, errUnavailable) {
			t.Fatal("paged reader trusted unsupported provenance version", version, err)
		}
		if _, err := reopened.RecordSummaries(t.Context(), _testNamespace, "conversation"); !errors.Is(err, errUnavailable) {
			t.Fatal("compact reader trusted unsupported provenance version", version, err)
		}
		called := false
		if err := reopened.Update(t.Context(), id, func(*Record) error { called = true; return nil }); !errors.Is(err, errUnavailable) || called {
			t.Fatal("writer callback received unsupported provenance version", version, err)
		}
	}
}

func TestPostgreSQLProfileQuotaAndLeaseFence(t *testing.T) {
	limited, _ := postgresForTestLimits(t, 32, 1)
	g, writer, _ := profileGateway(t, limited)
	if response := conversationRequest(g, http.MethodPost, _conversationPath, "profile-quota", _profileTestBody); response.Code == http.StatusAccepted {
		t.Fatal("Profile accepted without future Session capacity")
	}
	if records, err := limited.Records(t.Context()); err != nil || len(records) != 0 || writer.creates != 0 {
		t.Fatal("quota rejection persisted partial Profile work", err)
	}
	store, _ := postgresForTest(t)
	first, release, err := store.ClaimWorker(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	owned, _, _ := profileGateway(t, first)
	receipt := readConversationReceipt(t, conversationRequest(owned, http.MethodPost, _conversationPath, "profile-lease", _profileTestBody))
	if _, err := store.pool.Exec(t.Context(), "UPDATE gateway_meta SET expires='-infinity' WHERE id=1"); err != nil {
		t.Fatal(safeSQLError(err))
	}
	_, releaseNext, err := store.ClaimWorker(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer releaseNext()
	id := conversationRecordID(_testNamespace, _testConversationSubject, "profile-lease")
	if err := first.Update(t.Context(), id, func(r *Record) error { r.Work.Attempts[0].CreationClaimed = true; return nil }); !errors.Is(err, errFenced) {
		t.Fatal("expired worker mutated Profile creation claim", err)
	}
	if response := conversationRequest(owned, http.MethodPost, _conversationPath, "profile-expired", _profileTestBody); response.Code != http.StatusServiceUnavailable {
		t.Fatal("expired worker admitted Profile work", response.Code)
	}
	if _, err := store.Get(t.Context(), conversationRecordID(_testNamespace, _testConversationSubject, "profile-expired")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("expired admission persisted work", err)
	}
	_, attempt, err := store.LookupConversation(t.Context(), _testNamespace, receipt.ConversationID)
	if err != nil || attempt.CreationClaimed || attempt.Profile == nil {
		t.Fatal("worker takeover changed preserved Profile identity", err)
	}
}
