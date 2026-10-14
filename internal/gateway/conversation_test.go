package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/softcane/codex-session-operator/internal/openaiapi"
)

const (
	_conversationPath               = "/v1/namespaces/team/conversations"
	_testConversationSubject        = "writer"
	_testOtherConversationNamespace = "other"
	_testConversationHistory        = "retained-history"
)

func conversationRequest(g *Gateway, method, path, key, body string) *httptest.ResponseRecorder {
	return request(g, method, path, _token, key, body)
}

func readConversationReceipt(t *testing.T, response *httptest.ResponseRecorder) ConversationReceipt {
	t.Helper()
	if response.Code != http.StatusAccepted {
		t.Fatalf("status %d: %s", response.Code, response.Body.String())
	}
	var receipt ConversationReceipt
	if err := json.Unmarshal(response.Body.Bytes(), &receipt); err != nil {
		t.Fatal(err)
	}
	return receipt
}

func TestConversationFirstMessageFreezesProfileAndReturnsImmutableReceipt(t *testing.T) {
	store := testStore(t)
	g, writer, provider := workHTTPGateway(t, store)
	body := `{"route_id":"payments","text":"Investigate checkout\r\nUse logs","title":"Checkout"}`
	first := readConversationReceipt(t, conversationRequest(g, http.MethodPost, _conversationPath, "first", body))
	id := conversationRecordID(_testNamespace, _testConversationSubject, "first")
	record := readWorkRecord(t, store, id)
	if record.Version != 2 || writer.creates != 0 || provider.posts != 0 || len(record.Work.Attempts) != 1 || len(record.Work.Events) != 1 {
		t.Fatalf("HTTP acceptance performed delivery or duplicated work: %+v", record.Work)
	}
	attempt := record.Work.Attempts[0]
	if first.Acceptance != "committed" || first.ConversationID != attempt.ConversationID ||
		first.ConversationURL != _conversationPath+"/"+attempt.ConversationID || first.Attempt != 1 ||
		first.ReceivedAt.IsZero() || attempt.Policy.AgentRef.Name != _testWorkAgentV1 ||
		attempt.Policy.EnvironmentRef.Name != _testWorkEnvironmentV1 || record.Work.Conversation.Title != "Checkout" ||
		strings.Contains(record.Work.Events[0].Text, "\r") {
		t.Fatalf("missing frozen profile or durable receipt: %+v %+v", first, record.Work)
	}
	detailResponse := conversationRequest(g, http.MethodGet, first.ConversationURL, "", "")
	var detail ConversationDetail
	if err := json.Unmarshal(detailResponse.Body.Bytes(), &detail); err != nil {
		t.Fatal(err)
	}
	if detailResponse.Code != http.StatusOK || len(detail.Inputs) != 1 || detail.Inputs[0].Text != record.Work.Events[0].Text {
		t.Fatal("accepted first input disappeared before provisioning")
	}
	if err := store.Update(t.Context(), id, func(r *Record) error {
		r.Work.State = _workClosed
		r.DeliveryHeld = true
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	delete(g.workRoutes, _testWorkRouteID)
	replay := readConversationReceipt(t, conversationRequest(g, http.MethodPost, _conversationPath, "first", body))
	if !reflect.DeepEqual(first, replay) {
		t.Fatalf("closed/held/retired retry changed receipt: %+v %+v", first, replay)
	}
	for _, changed := range []string{
		`{"route_id":"retired","text":"Investigate checkout\nUse logs","title":"Checkout"}`,
		`{"route_id":"payments","text":"Changed","title":"Checkout"}`,
		`{"route_id":"payments","text":"Investigate checkout\nUse logs","title":"Changed"}`,
	} {
		response := conversationRequest(g, http.MethodPost, _conversationPath, "first", changed)
		if response.Code != http.StatusConflict {
			t.Fatalf("changed key body got %d: %s", response.Code, response.Body.String())
		}
	}
}

func TestConversationCollectionPagesAttemptsAndRejectsInvalidCursors(t *testing.T) {
	store := testStore(t)
	g, _, _ := workHTTPGateway(t, store)
	var wantIDs []string
	first := readConversationReceipt(t, conversationRequest(g, http.MethodPost, _conversationPath, "first",
		`{"route_id":"payments","text":"Investigate"}`))
	wantIDs = append(wantIDs, first.ConversationID)
	for _, key := range []string{"second", "third"} {
		receipt := readConversationReceipt(t, conversationRequest(g, http.MethodPost, _conversationPath, key,
			`{"route_id":"payments","text":"Investigate"}`))
		wantIDs = append(wantIDs, receipt.ConversationID)
	}
	if err := store.Update(t.Context(), conversationRecordID(_testNamespace, _testConversationSubject, "first"), func(r *Record) error {
		r.Work.State = _workClosed
		r.Work.Attempts[0].Cleaned = true
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	restarted := readConversationReceipt(t, conversationRequest(g, http.MethodPost, first.ConversationURL+"/restart", "restart", `{"text":"Again"}`))
	wantIDs = append(wantIDs, restarted.ConversationID)
	otherRoute := workHTTPRoute()
	otherRoute.ID, otherRoute.Namespace = "other-profile", _testOtherConversationNamespace
	g.workRoutes[otherRoute.ID] = otherRoute
	if _, _, err := g.createConversation(t.Context(), Principal{Namespace: _testOtherConversationNamespace, Subject: _testConversationSubject}, "first",
		ConversationInput{RouteID: otherRoute.ID, Text: "Other namespace"}); err != nil {
		t.Fatal(err)
	}
	slices.Sort(wantIDs)
	var page struct {
		Conversations []ConversationSummary `json:"conversations"`
		More          bool                  `json:"more"`
		NextCursor    string                `json:"next_cursor"`
	}
	response := conversationRequest(g, http.MethodGet, _conversationPath+"?limit=2", "", "")
	if err := json.Unmarshal(response.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	if response.Code != http.StatusOK || len(page.Conversations) != 2 || !page.More || page.NextCursor != wantIDs[1] {
		t.Fatalf("invalid first page: %s", response.Body.String())
	}
	gotIDs := make([]string, 0, len(wantIDs))
	gotIDs = append(gotIDs, page.Conversations[0].ConversationID, page.Conversations[1].ConversationID)
	response = conversationRequest(g, http.MethodGet, _conversationPath+"?limit=2&after="+page.NextCursor, "", "")
	if err := json.Unmarshal(response.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	if response.Code != http.StatusOK || len(page.Conversations) != 2 || page.More || page.NextCursor != "" {
		t.Fatalf("invalid final page: %s", response.Body.String())
	}
	gotIDs = append(gotIDs, page.Conversations[0].ConversationID, page.Conversations[1].ConversationID)
	if !slices.Equal(gotIDs, wantIDs) {
		t.Fatalf("page skipped, repeated or disclosed an attempt: %v want %v", gotIDs, wantIDs)
	}
	for _, query := range []string{"limit=0", "limit=101", "limit=no", "limit=1&limit=2", "after=bad", "after=", "after=" + first.ConversationID + "&after=" + restarted.ConversationID} {
		response := conversationRequest(g, http.MethodGet, _conversationPath+"?"+query, "", "")
		if response.Code != http.StatusBadRequest {
			t.Fatalf("invalid page query %q got %d", query, response.Code)
		}
	}
}

func TestConversationRecordVersionPreservesLegacyWorkContract(t *testing.T) {
	store := testStore(t)
	g, _, _ := workHTTPGateway(t, store)
	readConversationReceipt(t, conversationRequest(g, http.MethodPost, _conversationPath, "first",
		`{"route_id":"payments","text":"Investigate"}`))
	conversation := readWorkRecord(t, store, conversationRecordID(_testNamespace, _testConversationSubject, "first"))
	legacy := readWorkReceipt(t, postWork(g, workInput("legacy-task", _workTask, "Legacy input")))
	legacyRecord := readWorkRecord(t, store, legacy.ID)
	if conversation.Version != 2 || legacyRecord.Version != 1 || legacyRecord.Work.Conversation != nil ||
		legacyRecord.Work.Attempts[0].ConversationID != "" {
		t.Fatal("new record version changed legacy Work storage contract")
	}
}

func TestConversationMutationSerializesTwoGateOrderingWithSourceClosures(t *testing.T) {
	store := testStore(t)
	g, _, _ := workHTTPGateway(t, store)
	first := readConversationReceipt(t, conversationRequest(g, http.MethodPost, _conversationPath, "first",
		`{"route_id":"payments","text":"Investigate"}`))
	id := conversationRecordID(_testNamespace, _testConversationSubject, "first")
	// Pick a child UID whose shard differs from its source. Source closures can
	// hold those shards in the reverse order, so both operations need one fence.
	childUID := "child-session"
	for g.gate(childUID) == g.gate(id) {
		childUID += "x"
	}
	if err := store.Update(t.Context(), id, func(r *Record) error {
		r.Work.Attempts[0].SessionUID = childUID
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	g.issueCloseMu.Lock()
	closeFenceHeld := true
	defer func() {
		if closeFenceHeld {
			g.issueCloseMu.Unlock()
		}
	}()
	g.gate(childUID).Lock()
	childGateHeld := true
	defer func() {
		if childGateHeld {
			g.gate(childUID).Unlock()
		}
	}()
	started := make(chan struct{})
	finished := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		close(started)
		finished <- conversationRequest(g, http.MethodPost, first.ConversationURL+"/messages", _workEvidence, `{"text":"Evidence"}`)
	}()
	<-started
	// The competing closure already owns the global fence and child shard. Its
	// source shard must remain obtainable, allowing it to finish and release both.
	deadline := time.Now().Add(50 * time.Millisecond)
	for time.Now().Before(deadline) {
		if !g.workMu.TryLock() {
			t.Fatal("conversation mutation acquired work lock before the source closure fence")
		}
		g.workMu.Unlock()
		select {
		case <-finished:
			t.Fatal("mutation escaped the source closure fence")
		default:
		}
		if !g.gate(id).TryLock() {
			t.Fatal("conversation mutation blocked the source closure's work shard")
		}
		g.gate(id).Unlock()
		time.Sleep(time.Millisecond)
	}
	g.gate(childUID).Unlock()
	childGateHeld = false
	g.issueCloseMu.Unlock()
	closeFenceHeld = false
	select {
	case response := <-finished:
		readConversationReceipt(t, response)
	case <-time.After(time.Second):
		t.Fatal("mutation failed to finish after the closure released its fences")
	}
}

func TestConversationMutationsBindAttemptAndReplayAfterRestart(t *testing.T) {
	store := testStore(t)
	g, _, _ := workHTTPGateway(t, store)
	first := readConversationReceipt(t, conversationRequest(g, http.MethodPost, _conversationPath, "first",
		`{"route_id":"payments","text":"Investigate"}`))
	message := readConversationReceipt(t, conversationRequest(g, http.MethodPost, first.ConversationURL+"/messages", _workEvidence, `{"text":"More evidence"}`))
	closeReceipt := readConversationReceipt(t, conversationRequest(g, http.MethodPost, first.ConversationURL+"/close", _workClose, `{}`))
	if response := conversationRequest(g, http.MethodPost, first.ConversationURL+"/messages", "fresh", `{"text":"Too late"}`); response.Code != http.StatusGone {
		t.Fatalf("closing conversation accepted fresh input: %d", response.Code)
	}
	if response := conversationRequest(g, http.MethodPost, first.ConversationURL+"/restart", "restart", `{"text":"Again"}`); response.Code != http.StatusConflict {
		t.Fatalf("restart bypassed cleanup: %d", response.Code)
	}
	id := conversationRecordID(_testNamespace, _testConversationSubject, "first")
	if err := store.Update(t.Context(), id, func(r *Record) error {
		r.Work.State = _workClosed
		r.Work.Attempts[0].Cleaned = true
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	restarted := readConversationReceipt(t, conversationRequest(g, http.MethodPost, first.ConversationURL+"/restart", "restart", `{"text":"Again"}`))
	if restarted.Attempt != 2 || restarted.ConversationID == first.ConversationID {
		t.Fatalf("restart reused conversation identity: %+v", restarted)
	}
	for _, tt := range []struct {
		path, key, body string
		want            ConversationReceipt
	}{
		{first.ConversationURL + "/messages", _workEvidence, `{"text":"More evidence"}`, message},
		{first.ConversationURL + "/close", _workClose, `{}`, closeReceipt},
		{first.ConversationURL + "/restart", "restart", `{"text":"Again"}`, restarted},
	} {
		replay := readConversationReceipt(t, conversationRequest(g, http.MethodPost, tt.path, tt.key, tt.body))
		if !reflect.DeepEqual(tt.want, replay) {
			t.Fatalf("attempt retry changed receipt: %+v %+v", tt.want, replay)
		}
	}
	if response := conversationRequest(g, http.MethodPost, restarted.ConversationURL+"/messages", _workEvidence, `{"text":"More evidence"}`); response.Code != http.StatusConflict {
		t.Fatalf("key reused for different target did not conflict: %d", response.Code)
	}
	if response := conversationRequest(g, http.MethodPost, first.ConversationURL+"/close", "new-close", `{}`); response.Code != http.StatusGone {
		t.Fatalf("old attempt closed new attempt: %d", response.Code)
	}
}

func TestConversationConcurrentCreateAcrossGatewaysHasOneAttempt(t *testing.T) {
	store := testStore(t)
	first, _, _ := workHTTPGateway(t, store)
	second, _, _ := workHTTPGateway(t, store)
	responses := make(chan *httptest.ResponseRecorder, 16)
	var group sync.WaitGroup
	for i := range 16 {
		group.Go(func() {
			g := first
			if i%2 == 1 {
				g = second
			}
			responses <- conversationRequest(g, http.MethodPost, _conversationPath, "same-key", `{"route_id":"payments","text":"Investigate"}`)
		})
	}
	group.Wait()
	close(responses)
	var firstReceipt ConversationReceipt
	for response := range responses {
		receipt := readConversationReceipt(t, response)
		if firstReceipt.ConversationID == "" {
			firstReceipt = receipt
		}
		if !reflect.DeepEqual(firstReceipt, receipt) {
			t.Fatal("concurrent creation returned different receipts")
		}
	}
	record := readWorkRecord(t, store, conversationRecordID(_testNamespace, _testConversationSubject, "same-key"))
	if len(record.Work.Attempts) != 1 || len(record.Work.Events) != 1 {
		t.Fatal("concurrent creation duplicated an attempt or initial input")
	}
}

func TestConversationReadsRetainedAttemptAndNamespaceIsolation(t *testing.T) {
	store := testStore(t)
	g, _, provider := workHTTPGateway(t, store)
	first := readConversationReceipt(t, conversationRequest(g, http.MethodPost, _conversationPath, "first", `{"route_id":"payments","text":"Investigate"}`))
	id := conversationRecordID(_testNamespace, _testConversationSubject, "first")
	if err := store.Update(t.Context(), id, func(r *Record) error {
		r.Work.Attempts[0].SessionUID = "retained-session"
		r.Work.Attempts[0].Cleaned = true
		r.Work.State = _workClosed
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Update(t.Context(), "retained-session", func(r *Record) error {
		r.Namespace = _testNamespace
		r.History = openaiapi.History{ID: _testConversationHistory, Status: "completed"}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	readConversationReceipt(t, conversationRequest(g, http.MethodPost, first.ConversationURL+"/restart", "restart", `{"text":"Again"}`))
	response := conversationRequest(g, http.MethodGet, first.ConversationURL, "", "")
	var detail ConversationDetail
	if err := json.Unmarshal(response.Body.Bytes(), &detail); err != nil {
		t.Fatal(err)
	}
	if response.Code != http.StatusOK || detail.Summary.Active || detail.Summary.State != _workClosed ||
		detail.History.ID != _testConversationHistory || detail.Summary.Attempt != 1 || provider.posts != 0 {
		t.Fatalf("old history unavailable or read performed delivery: %s", response.Body.String())
	}
	list := conversationRequest(g, http.MethodGet, _conversationPath, "", "")
	var collection struct {
		Conversations []ConversationSummary `json:"conversations"`
	}
	if err := json.Unmarshal(list.Body.Bytes(), &collection); err != nil {
		t.Fatal(err)
	}
	if list.Code != http.StatusOK || len(collection.Conversations) != 2 {
		t.Fatalf("collection did not retain both attempts: %s", list.Body.String())
	}
	unauthorized := conversationRequest(g, http.MethodGet, strings.Replace(first.ConversationURL, "/team/", "/other/", 1), "", "")
	if unauthorized.Code != http.StatusNotFound || strings.Contains(unauthorized.Body.String(), first.ConversationID) {
		t.Fatal("cross namespace lookup disclosed a conversation")
	}
}

func TestConversationRejectsInvalidInputAndHeldFreshMutation(t *testing.T) {
	store := testStore(t)
	g, _, _ := workHTTPGateway(t, store)
	for _, body := range []string{
		`null`, `{}`, `{"route_id":"payments","text":" "}`, `{"route_id":"payments","text":"x","agent":"untrusted"}`,
		`{"route_id":"payments","text":"x"} {}`, `{"route_id":"payments","text":"x","title":"bad\nline"}`,
		`{"route_id":"payments","text":"` + strings.Repeat("x", _maxInputBytes+1) + `"}`,
	} {
		response := conversationRequest(g, http.MethodPost, _conversationPath, "invalid", body)
		if response.Code != http.StatusBadRequest {
			t.Fatalf("invalid input accepted with %d", response.Code)
		}
	}
	if response := conversationRequest(g, http.MethodPost, _conversationPath, "", `{"route_id":"payments","text":"x"}`); response.Code != http.StatusBadRequest {
		t.Fatal("missing key accepted")
	}
	first := readConversationReceipt(t, conversationRequest(g, http.MethodPost, _conversationPath, "first", `{"route_id":"payments","text":"Investigate"}`))
	if err := store.Update(t.Context(), conversationRecordID(_testNamespace, _testConversationSubject, "first"), func(r *Record) error {
		r.DeliveryHeld = true
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{_messagesSegment, _workClose, "restart"} {
		body := `{"text":"x"}`
		if path == _workClose {
			body = `{}`
		}
		if response := conversationRequest(g, http.MethodPost, first.ConversationURL+"/"+path, "new", body); response.Code != http.StatusServiceUnavailable {
			t.Fatalf("held %s accepted with %d", path, response.Code)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, _, err := g.createConversation(ctx, Principal{Namespace: _testNamespace, Subject: _testConversationSubject}, "canceled",
		ConversationInput{RouteID: _testWorkRouteID, Text: "x"}); err == nil {
		t.Fatal("canceled creation succeeded")
	}
}

func TestConversationArchiveRemainsVisibleAndRestartHasFreshInputs(t *testing.T) {
	store := testStore(t)
	g, _, _ := workHTTPGateway(t, store)
	first := readConversationReceipt(t, conversationRequest(g, http.MethodPost, _conversationPath, "first",
		`{"route_id":"payments","text":"Investigate","title":"Checkout"}`))
	id := conversationRecordID(_testNamespace, _testConversationSubject, "first")
	if err := store.Update(t.Context(), id, func(r *Record) error {
		r.Archived = true
		r.Work.State = _workClosed
		r.Work.Attempts[0].Cleaned = true
		r.Work.Attempts[0].InputsArchived = true
		r.Work.Conversation.Title = ""
		r.Work.Events[0].Text = ""
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	response := conversationRequest(g, http.MethodGet, first.ConversationURL, "", "")
	var detail ConversationDetail
	if err := json.Unmarshal(response.Body.Bytes(), &detail); err != nil {
		t.Fatal(err)
	}
	if response.Code != http.StatusOK || !detail.Archived || !detail.InputsArchived || !detail.Summary.Archived {
		t.Fatal("archived accepted inputs lost their archival marker")
	}
	replay := readConversationReceipt(t, conversationRequest(g, http.MethodPost, _conversationPath, "first",
		`{"route_id":"payments","text":"Investigate","title":"Checkout"}`))
	if !reflect.DeepEqual(first, replay) {
		t.Fatal("archive changed immutable receipt")
	}
	restarted := readConversationReceipt(t, conversationRequest(g, http.MethodPost, first.ConversationURL+"/restart", "restart", `{"text":"New investigation"}`))
	response = conversationRequest(g, http.MethodGet, restarted.ConversationURL, "", "")
	if err := json.Unmarshal(response.Body.Bytes(), &detail); err != nil {
		t.Fatal(err)
	}
	if detail.Archived || detail.Summary.Archived || len(detail.Inputs) != 1 || detail.Inputs[0].Text != "New investigation" {
		t.Fatal("new attempt inherited old input archival state")
	}
}

func TestConversationCannotBeClosedThroughLegacyWorkIdentity(t *testing.T) {
	store := testStore(t)
	g, _, _ := workHTTPGateway(t, store)
	readConversationReceipt(t, conversationRequest(g, http.MethodPost, _conversationPath, "first",
		`{"route_id":"payments","text":"Investigate"}`))
	id := conversationRecordID(_testNamespace, _testConversationSubject, "first")
	response := postWork(g, WorkInput{RouteID: _testWorkRouteID, WorkID: id, EventID: "legacy-close", Kind: _workClose})
	if response.Code != http.StatusConflict {
		t.Fatalf("legacy close accepted a conversational source identity: %d", response.Code)
	}
	records, err := store.Records(t.Context())
	if err != nil || len(records) != 1 || records[0].Work.State != _workOpen {
		t.Fatal("legacy close created a phantom source or changed the conversation", err)
	}
	response = request(g, http.MethodGet, "/v1/namespaces/team/work", _token, "", "")
	var collection struct {
		Work []WorkSummary `json:"work"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &collection); err != nil {
		t.Fatal(err)
	}
	if response.Code != http.StatusOK || len(collection.Work) != 0 {
		t.Fatal("advanced Work list exposed interactive conversation records")
	}
}
