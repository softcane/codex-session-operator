package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"slices"
	"sync"
	"testing"
)

const _postgresConversationInput = `{"route_id":"payments","text":"Investigate checkout","title":"Checkout"}`

func reopenConversationStore(t *testing.T, dsn string) *PostgreSQLStore {
	t.Helper()
	store, err := OpenPostgreSQL(t.Context(), PostgreSQLConfig{DSN: dsn, AllowInsecureLocal: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func TestPostgreSQLConversationConcurrentAcceptanceAndSubjectScope(t *testing.T) {
	store, dsn := postgresForTest(t, 32)
	other := reopenConversationStore(t, dsn)
	first, firstWriter, firstProvider := workHTTPGateway(t, store)
	second, secondWriter, secondProvider := workHTTPGateway(t, other)
	responses := make(chan *httptest.ResponseRecorder, 16)
	var group sync.WaitGroup
	for i := range 16 {
		group.Go(func() {
			g := first
			if i%2 == 1 {
				g = second
			}
			responses <- conversationRequest(g, http.MethodPost, _conversationPath, "create-once", _postgresConversationInput)
		})
	}
	group.Wait()
	close(responses)
	var original ConversationReceipt
	for response := range responses {
		receipt := readConversationReceipt(t, response)
		if original.ConversationID == "" {
			original = receipt
		}
		if !reflect.DeepEqual(original, receipt) {
			t.Fatal("replicas returned different receipts")
		}
	}
	records, err := store.Records(t.Context())
	if err != nil || len(records) != 1 || len(records[0].Work.Attempts) != 1 || len(records[0].Work.Events) != 1 {
		t.Fatal("replicas duplicated accepted work", err)
	}
	if original.Acceptance != "committed" || firstWriter.creates != 0 || secondWriter.creates != 0 ||
		firstProvider.posts != 0 || secondProvider.posts != 0 {
		t.Fatal("acceptance performed provisioning or provider I/O")
	}
	r, attempt, err := other.LookupConversation(t.Context(), _testNamespace, original.ConversationID)
	if err != nil || r.UID != records[0].UID || attempt.Number != 1 || attempt.ConversationID != original.ConversationID {
		t.Fatal("accepted conversation missing its indexed attempt", err)
	}
	for _, changed := range []string{
		`{"route_id":"other-profile","text":"Investigate checkout","title":"Checkout"}`,
		`{"route_id":"payments","text":"Changed","title":"Checkout"}`,
		`{"route_id":"payments","text":"Investigate checkout","title":"Changed"}`,
	} {
		if response := conversationRequest(second, http.MethodPost, _conversationPath, "create-once", changed); response.Code != http.StatusConflict {
			t.Fatalf("changed creation identity got %d: %s", response.Code, response.Body.String())
		}
	}
	response := request(second, http.MethodPost, _conversationPath, _token+"-approver", "create-once", _postgresConversationInput)
	scoped := readConversationReceipt(t, response)
	if scoped.ConversationID == original.ConversationID {
		t.Fatal("creation key was shared across subjects")
	}
	response = request(second, http.MethodGet, original.ConversationURL, _token+"-approver", "", "")
	if response.Code != http.StatusOK {
		t.Fatal("namespace-authorized subject could not read a conversation", response.Code)
	}
	summaries, err := store.RecordSummaries(t.Context(), _testNamespace, "conversation")
	if err != nil || len(summaries) != 2 || len(summaries[0].Work.Events) != 0 || len(summaries[1].Work.Events) != 0 {
		t.Fatal("conversation catalog did not use compact indexed records", err)
	}
}

func TestPostgreSQLConversationReopenRetainsReceiptAndFrozenProfile(t *testing.T) {
	store, dsn := postgresForTest(t, 32)
	g, _, _ := workHTTPGateway(t, store)
	original := readConversationReceipt(t, conversationRequest(g, http.MethodPost, _conversationPath, "reopen", _postgresConversationInput))
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := reopenConversationStore(t, dsn)
	next, _, _ := workHTTPGateway(t, reopened)
	route := next.workRoutes[_testWorkRouteID]
	route.Agent, route.Environment = _testWorkAgentV2, "node-v2"
	next.workRoutes[route.ID] = route
	replay := readConversationReceipt(t, conversationRequest(next, http.MethodPost, _conversationPath, "reopen", _postgresConversationInput))
	if !reflect.DeepEqual(original, replay) {
		t.Fatal("database reopening changed receipt")
	}
	_, attempt, err := reopened.LookupConversation(t.Context(), _testNamespace, original.ConversationID)
	if err != nil || attempt.Policy.AgentRef.Name != _testWorkAgentV1 || attempt.Policy.EnvironmentRef.Name != _testWorkEnvironmentV1 {
		t.Fatal("database reopening changed selected profile", err)
	}
}

func TestPostgreSQLConversationMessageKeysAreScopedToLineage(t *testing.T) {
	store, _ := postgresForTest(t, 32)
	g, _, _ := workHTTPGateway(t, store)
	first := readConversationReceipt(t, conversationRequest(g, http.MethodPost, _conversationPath, "first-lineage", _postgresConversationInput))
	second := readConversationReceipt(t, conversationRequest(g, http.MethodPost, _conversationPath, "second-lineage", _postgresConversationInput))
	for _, receipt := range []ConversationReceipt{first, second} {
		message := readConversationReceipt(t, conversationRequest(g, http.MethodPost, receipt.ConversationURL+"/messages", "shared-followup", `{"text":"Additional evidence"}`))
		if message.ConversationID != receipt.ConversationID {
			t.Fatal("message key selected a different conversation")
		}
	}
	for _, changed := range []struct{ path, body string }{
		{first.ConversationURL + "/messages", `{"text":"Changed evidence"}`},
		{first.ConversationURL + "/close", `{}`},
	} {
		if response := conversationRequest(g, http.MethodPost, changed.path, "shared-followup", changed.body); response.Code != http.StatusConflict {
			t.Fatalf("changed lineage mutation got %d: %s", response.Code, response.Body.String())
		}
	}
}

type conversationRestoreRaceStore struct {
	*PostgreSQLStore
	sessionUID string
	afterRead  func()
}

func (s *conversationRestoreRaceStore) GetInNamespace(ctx context.Context, namespace, uid string) (Record, error) {
	record, err := s.PostgreSQLStore.GetInNamespace(ctx, namespace, uid)
	if err == nil && uid == s.sessionUID && s.afterRead != nil {
		hook := s.afterRead
		s.afterRead = nil
		hook()
	}
	return record, err
}

func TestPostgreSQLConversationAdmissionRechecksChildHoldDuringRestoreRelease(t *testing.T) {
	store, _ := postgresForTest(t, 32)
	g, _, _ := workHTTPGateway(t, store)
	original := readConversationReceipt(t, conversationRequest(g, http.MethodPost, _conversationPath, "restore-race", _postgresConversationInput))
	id := conversationRecordID(_testNamespace, _testConversationSubject, "restore-race")
	const sessionUID = "restore-race-session"
	if err := store.Update(t.Context(), id, func(r *Record) error {
		r.Work.Attempts[0].SessionUID = sessionUID
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Update(t.Context(), sessionUID, func(r *Record) error {
		r.Namespace = _testNamespace
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	backup, err := store.Backup(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	fresh, _ := postgresForTest(t, 32)
	report, err := fresh.Restore(t.Context(), backup)
	if err != nil {
		t.Fatal(err)
	}
	interleaved := &conversationRestoreRaceStore{PostgreSQLStore: fresh, sessionUID: sessionUID}
	interleaved.afterRead = func() {
		audit := []Reconciliation{
			{UID: id, Digest: report.Digests[id], Reference: "test verified work mapping", Verified: true},
			{UID: sessionUID, Digest: report.Digests[sessionUID], Reference: "test uncertain child outcome", Blocked: true},
		}
		if err := fresh.ResumeDelivery(t.Context(), audit); err != nil {
			t.Fatal(err)
		}
	}
	next, _, _ := workHTTPGateway(t, interleaved)
	response := conversationRequest(next, http.MethodPost, original.ConversationURL+"/messages", "race-message", `{"text":"More evidence"}`)
	if response.Code != http.StatusServiceUnavailable || interleaved.afterRead != nil {
		t.Fatalf("held child accepted input across restore release: %d %s", response.Code, response.Body.String())
	}
	record := readWorkRecord(t, fresh, id)
	if record.DeliveryHeld || len(record.Work.Events) != 1 {
		t.Fatal("restore race test held the wrong identity or saved rejected evidence")
	}
}

func TestPostgreSQLConversationRestoreRebuildsIndexesAndHoldsNewInput(t *testing.T) {
	store, _ := postgresForTest(t, 32)
	g, _, _ := workHTTPGateway(t, store)
	original := readConversationReceipt(t, conversationRequest(g, http.MethodPost, _conversationPath, "restore", _postgresConversationInput))
	backup, err := store.Backup(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	fresh, _ := postgresForTest(t, 32)
	report, err := fresh.Restore(t.Context(), backup)
	if err != nil {
		t.Fatal(err)
	}
	next, writer, provider := workHTTPGateway(t, fresh)
	replay := readConversationReceipt(t, conversationRequest(next, http.MethodPost, _conversationPath, "restore", _postgresConversationInput))
	if !reflect.DeepEqual(original, replay) {
		t.Fatal("held restore changed the original receipt")
	}
	restored, attempt, err := fresh.LookupConversation(t.Context(), _testNamespace, original.ConversationID)
	if err != nil || attempt.ConversationID != original.ConversationID || restored.Work.Conversation.Subject != _testConversationSubject {
		t.Fatal("restore did not rebuild conversation mapping", err)
	}
	assertConversationInputHeld(t, next, original)
	if response := conversationRequest(next, http.MethodPost, _conversationPath, "fresh-held", _postgresConversationInput); response.Code != http.StatusServiceUnavailable {
		t.Fatalf("restore accepted a new conversation: %d", response.Code)
	}
	if err := fresh.Update(t.Context(), restored.UID, func(r *Record) error {
		r.Work.Events = append(r.Work.Events, WorkEvent{ID: "direct-held", Attempt: 1})
		return nil
	}); !errors.Is(err, errRestoreHeld) {
		t.Fatal("storage mutation bypassed atomic restore gate", err)
	}
	audit := []Reconciliation{{UID: restored.UID, Digest: report.Digests[restored.UID], Reference: "test unresolved provider outcome", Blocked: true}}
	if err := fresh.ResumeDelivery(t.Context(), audit); err != nil {
		t.Fatal(err)
	}
	assertConversationInputHeld(t, next, original)
	replay = readConversationReceipt(t, conversationRequest(next, http.MethodPost, _conversationPath, "restore", _postgresConversationInput))
	if !reflect.DeepEqual(original, replay) || writer.creates != 0 || provider.posts != 0 {
		t.Fatal("held replay changed identity or performed remote I/O")
	}
}

func assertConversationInputHeld(t *testing.T, g *Gateway, receipt ConversationReceipt) {
	t.Helper()
	for _, requestInput := range []struct{ path, key, body string }{
		{receipt.ConversationURL + "/messages", "held-message", `{"text":"More evidence"}`},
		{receipt.ConversationURL + "/close", "held-close", `{}`},
	} {
		response := conversationRequest(g, http.MethodPost, requestInput.path, requestInput.key, requestInput.body)
		if response.Code != http.StatusServiceUnavailable {
			t.Fatalf("held input got %d: %s", response.Code, response.Body.String())
		}
	}
}

func TestPostgreSQLConversationArchivePreservesReplayAndAttemptLookup(t *testing.T) {
	store, _ := postgresForTest(t, 32)
	g, _, _ := workHTTPGateway(t, store)
	original := readConversationReceipt(t, conversationRequest(g, http.MethodPost, _conversationPath, "archive", _postgresConversationInput))
	id := conversationRecordID(_testNamespace, _testConversationSubject, "archive")
	if err := store.Update(t.Context(), id, func(r *Record) error {
		r.Work.State = _workClosed
		r.Work.Attempts[0].Cleaned = true
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Archive(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	archived, attempt, err := store.LookupConversation(t.Context(), _testNamespace, original.ConversationID)
	if err != nil || !archived.Archived || archived.Work.Conversation.Title != "" || archived.Work.Events[0].Text != "" || attempt.Number != 1 || !attempt.InputsArchived {
		t.Fatal("archive lost mapping or retained conversation content", err)
	}
	replay := readConversationReceipt(t, conversationRequest(g, http.MethodPost, _conversationPath, "archive", _postgresConversationInput))
	if !reflect.DeepEqual(original, replay) {
		t.Fatal("archive changed original receipt")
	}
	restarted := readConversationReceipt(t, conversationRequest(g, http.MethodPost, original.ConversationURL+"/restart", "archived-restart", `{"text":"New investigation"}`))
	_, newAttempt, err := store.LookupConversation(t.Context(), _testNamespace, restarted.ConversationID)
	if err != nil || newAttempt.InputsArchived {
		t.Fatal("restart inherited prior attempt's archive state", err)
	}
	backup, err := store.Backup(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	fresh, _ := postgresForTest(t, 32)
	if _, err := fresh.Restore(t.Context(), backup); err != nil {
		t.Fatal(err)
	}
	_, restoredAttempt, err := fresh.LookupConversation(t.Context(), _testNamespace, original.ConversationID)
	if err != nil || !reflect.DeepEqual(attempt, restoredAttempt) {
		t.Fatal("restore changed archived attempt mapping", err)
	}
	next, _, _ := workHTTPGateway(t, fresh)
	if restoredReceipt := readConversationReceipt(t, conversationRequest(next, http.MethodPost, _conversationPath, "archive", _postgresConversationInput)); !reflect.DeepEqual(original, restoredReceipt) {
		t.Fatal("restored archive changed receipt")
	}
}

func TestPostgreSQLConversationAttemptIndexesFailClosed(t *testing.T) {
	store, _ := postgresForTest(t, 32)
	g, _, _ := workHTTPGateway(t, store)
	original := readConversationReceipt(t, conversationRequest(g, http.MethodPost, _conversationPath, "attempts", _postgresConversationInput))
	id := conversationRecordID(_testNamespace, _testConversationSubject, "attempts")
	if err := store.Update(t.Context(), id, func(r *Record) error {
		r.Work.State = _workClosed
		r.Work.Attempts[0].Cleaned = true
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	restarted := readConversationReceipt(t, conversationRequest(g, http.MethodPost, original.ConversationURL+"/restart", "restart", `{"text":"Investigate again"}`))
	for _, receipt := range []ConversationReceipt{original, restarted} {
		_, attempt, err := store.LookupConversation(t.Context(), _testNamespace, receipt.ConversationID)
		if err != nil || attempt.Number != receipt.Attempt {
			t.Fatal("attempt index pointed to another conversation", err)
		}
	}
	for _, lookup := range []struct{ namespace, id string }{
		{_testWorkOther, original.ConversationID},
		{_testNamespace, "conv-unknown"},
	} {
		if _, _, err := store.LookupConversation(t.Context(), lookup.namespace, lookup.id); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("unknown conversation did not return absence", err)
		}
	}
	if _, err := store.pool.Exec(t.Context(), "UPDATE gateway_keys SET digest=$1 WHERE kind='conversation' AND namespace=$2 AND scope='' AND key=$3", "2", _testNamespace, original.ConversationID); err != nil {
		t.Fatal("inject stale conversation mapping", safeSQLError(err))
	}
	if _, _, err := store.LookupConversation(t.Context(), _testNamespace, original.ConversationID); !errors.Is(err, errUnavailable) {
		t.Fatal("stale conversation mapping was trusted", err)
	}
	if response := conversationRequest(g, http.MethodGet, original.ConversationURL, "", ""); response.Code != http.StatusServiceUnavailable {
		t.Fatal("API read a stale conversation mapping", response.Code)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, _, err := store.LookupConversation(ctx, _testNamespace, restarted.ConversationID); !errors.Is(err, context.Canceled) {
		t.Fatal("conversation lookup ignored caller cancellation", err)
	}
}

func TestPostgreSQLConversationQuotasReserveFutureSession(t *testing.T) {
	for _, limits := range []struct {
		name            string
		active, records int
		firstAccepted   bool
	}{
		{name: "future record capacity", active: 32, records: 1},
		{name: "active execution capacity", active: 1, records: 32, firstAccepted: true},
	} {
		t.Run(limits.name, func(t *testing.T) {
			store, _ := postgresForTestLimits(t, limits.active, limits.records)
			g, _, _ := workHTTPGateway(t, store)
			first := conversationRequest(g, http.MethodPost, _conversationPath, "first", _postgresConversationInput)
			if limits.firstAccepted {
				readConversationReceipt(t, first)
				if response := conversationRequest(g, http.MethodPost, _conversationPath, "second", _postgresConversationInput); response.Code == http.StatusAccepted {
					t.Fatal("accepted conversation above active capacity")
				}
			} else if first.Code == http.StatusAccepted {
				t.Fatal("accepted conversation without future Session capacity")
			}
			records, err := store.Records(t.Context())
			want := 0
			if limits.firstAccepted {
				want = 1
			}
			if err != nil || len(records) != want {
				t.Fatal("quota rejection persisted partial conversation", err)
			}
		})
	}
}

func TestConversationRecordVersionsPreserveLegacyAndRejectUnknownData(t *testing.T) {
	store := testStore(t)
	g, _, _ := workHTTPGateway(t, store)
	readConversationReceipt(t, conversationRequest(g, http.MethodPost, _conversationPath, "version", _postgresConversationInput))
	record := readWorkRecord(t, store, conversationRecordID(_testNamespace, _testConversationSubject, "version"))
	if record.Version != 2 {
		t.Fatal("new conversation did not reserve version 2")
	}
	raw := mustJSON(t, record)
	var oldReader struct {
		Version int `json:"version"`
	}
	if err := json.Unmarshal(raw, &oldReader); err != nil || oldReader.Version == 1 {
		t.Fatal("legacy version-1 reader would accept conversation data", err)
	}
	for _, tt := range []struct {
		name   string
		mutate func(*Record)
		valid  bool
	}{
		{name: "new conversation", valid: true, mutate: func(*Record) {}},
		{name: "origin under legacy version", mutate: func(r *Record) { r.Version = 1 }},
		{name: "unknown record version", mutate: func(r *Record) { r.Version = 3 }},
		{name: "version 2 requires origin", mutate: func(r *Record) { r.Work = nil }},
		{name: "legacy child Session", valid: true, mutate: func(r *Record) { r.Version = 1; r.Work = nil }},
		{name: "legacy Work", valid: true, mutate: func(r *Record) {
			r.Version = 1
			r.Work.Conversation = nil
			r.Work.Attempts[0].ConversationID = ""
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var candidate Record
			if err := json.Unmarshal(raw, &candidate); err != nil {
				t.Fatal(err)
			}
			tt.mutate(&candidate)
			if err := validateRecord(candidate); (err == nil) != tt.valid {
				t.Fatalf("record validity mismatch: %v", err)
			}
			_, err := decodeSQLRecord(candidate.UID, candidate.Namespace, mustJSON(t, candidate), nil)
			if (err == nil) != tt.valid || !tt.valid && !errors.Is(err, errUnavailable) {
				t.Fatalf("database read did not enforce version gate: %v", err)
			}
		})
	}
}

func TestPostgreSQLConversationListPaginatesAttemptsWithinNamespace(t *testing.T) {
	store, _ := postgresForTest(t, 32)
	g, _, _ := workHTTPGateway(t, store)
	first := readConversationReceipt(t, conversationRequest(g, http.MethodPost, _conversationPath, "page-first", _postgresConversationInput))
	second := readConversationReceipt(t, conversationRequest(g, http.MethodPost, _conversationPath, "page-second", _postgresConversationInput))
	id := conversationRecordID(_testNamespace, _testConversationSubject, "page-first")
	if err := store.Update(t.Context(), id, func(r *Record) error {
		r.Work.State = _workClosed
		r.Work.Attempts[0].Cleaned = true
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	restarted := readConversationReceipt(t, conversationRequest(g, http.MethodPost, first.ConversationURL+"/restart", "page-restart", `{"text":"Restart investigation"}`))
	route := workHTTPRoute()
	route.ID, route.Namespace = "other-profile", _testWorkOther
	g.workRoutes[route.ID] = route
	principals, err := copyPrincipals([]Principal{{Token: _token + "-other", Subject: _testConversationSubject, Namespace: _testWorkOther}})
	if err != nil {
		t.Fatal(err)
	}
	g.principals = append(g.principals, principals...)
	other := readConversationReceipt(t, request(g, http.MethodPost, "/v1/namespaces/other/conversations", _token+"-other", "other-page", `{"route_id":"other-profile","text":"Other namespace"}`))
	page, err := store.ListConversationAttempts(t.Context(), _testNamespace, "", 2)
	if err != nil || len(page) != 3 {
		t.Fatal("first page did not return two entries and lookahead", err)
	}
	for _, entry := range page {
		if entry.Record.Namespace != _testNamespace || entry.Attempt.ConversationID == other.ConversationID ||
			len(entry.Record.Work.Events) != 0 || len(entry.Record.Submissions) != 0 ||
			len(entry.Record.Actions) != 0 || len(entry.Record.Notes) != 0 {
			t.Fatal("page retained transcripts or crossed namespace")
		}
	}
	last, err := store.ListConversationAttempts(t.Context(), _testNamespace, page[1].Attempt.ConversationID, 2)
	if err != nil || len(last) != 1 || last[0].Attempt.ConversationID != page[2].Attempt.ConversationID {
		t.Fatal("cursor repeated or skipped an attempt", err)
	}
	want := []string{first.ConversationID, second.ConversationID, restarted.ConversationID}
	slices.Sort(want)
	for i, entry := range page {
		if entry.Attempt.ConversationID != want[i] {
			t.Fatal("indexed list order changed")
		}
	}
	foreign, err := store.ListConversationAttempts(t.Context(), _testWorkOther, "", 2)
	if err != nil || len(foreign) != 1 || foreign[0].Attempt.ConversationID != other.ConversationID {
		t.Fatal("namespace page did not select its own conversation", err)
	}
	apiFirst := readPostgreSQLConversationPage(t, g, _conversationPath+"?limit=2")
	if len(apiFirst.Conversations) != 2 || !apiFirst.More || apiFirst.NextCursor != page[1].Attempt.ConversationID {
		t.Fatal("API did not bound or mark its first conversation page")
	}
	apiLast := readPostgreSQLConversationPage(t, g, _conversationPath+"?limit=2&after="+apiFirst.NextCursor)
	if len(apiLast.Conversations) != 1 || apiLast.More || apiLast.NextCursor != "" ||
		apiLast.Conversations[0].ConversationID != last[0].Attempt.ConversationID {
		t.Fatal("API cursor repeated or skipped an attempt")
	}
}

type postgresConversationPage struct {
	Conversations []ConversationSummary `json:"conversations"`
	More          bool                  `json:"more"`
	NextCursor    string                `json:"next_cursor"`
}

func readPostgreSQLConversationPage(t *testing.T, g *Gateway, path string) postgresConversationPage {
	t.Helper()
	response := conversationRequest(g, http.MethodGet, path, "", "")
	var page postgresConversationPage
	if response.Code != http.StatusOK {
		t.Fatalf("conversation list got %d: %s", response.Code, response.Body.String())
	}
	if err := json.Unmarshal(response.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	return page
}

func TestPostgreSQLConversationReadersFailClosedForRecordVersions(t *testing.T) {
	store, _ := postgresForTest(t, 32)
	g, _, _ := workHTTPGateway(t, store)
	original := readConversationReceipt(t, conversationRequest(g, http.MethodPost, _conversationPath, "version-data", _postgresConversationInput))
	id := conversationRecordID(_testNamespace, _testConversationSubject, "version-data")
	record := readWorkRecord(t, store, id)
	for _, version := range []int{1, 3} {
		record.Version = version
		if _, err := store.pool.Exec(t.Context(), "UPDATE gateway_records SET data=$1 WHERE uid=$2", mustJSON(t, record), id); err != nil {
			t.Fatal("inject inconsistent record version", safeSQLError(err))
		}
		if _, _, err := store.LookupConversation(t.Context(), _testNamespace, original.ConversationID); !errors.Is(err, errUnavailable) {
			t.Fatal("indexed read trusted incompatible version", version, err)
		}
		if _, err := store.RecordSummaries(t.Context(), _testNamespace, "conversation"); !errors.Is(err, errUnavailable) {
			t.Fatal("summary read trusted incompatible version", version, err)
		}
		if _, err := store.ListConversationAttempts(t.Context(), _testNamespace, "", 2); !errors.Is(err, errUnavailable) {
			t.Fatal("paged read trusted incompatible version", version, err)
		}
		called := false
		if err := store.Update(t.Context(), id, func(*Record) error { called = true; return nil }); !errors.Is(err, errUnavailable) || called {
			t.Fatal("writer changed a record with incompatible version", version, err)
		}
	}
}
