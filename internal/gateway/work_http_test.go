package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	_workEventPath         = "/v1/namespaces/team/work-events"
	_testWorkRouteID       = "payments"
	_testWorkAgentV1       = "payments-v1"
	_testWorkAgentV2       = "payments-v2"
	_testWorkEnvironmentV1 = "node-v1"
	_testWorkUnlimited     = "unlimited"
	_testWorkOther         = "other"
	_testWorkPhase         = "Ready"
)

func workHTTPGateway(t *testing.T, store Storage) (*Gateway, *issueWriter, *simulator) {
	t.Helper()
	provider := simulatorForTest()
	g, k8s := testGateway(t, store, provider)
	writer := &issueWriter{Client: k8s}
	if err := g.EnableWork(writer, []WorkRoute{workHTTPRoute()}); err != nil {
		t.Fatal(err)
	}
	return g, writer, provider
}

func workHTTPRoute() WorkRoute {
	return WorkRoute{ID: _testWorkRouteID, Namespace: _testNamespace, Source: "ci",
		Agent: _testWorkAgentV1, Environment: _testWorkEnvironmentV1,
		SpendControl: &agentsv1alpha1.SessionSpendControl{Mode: _testWorkUnlimited}}
}

func postWork(g *Gateway, input WorkInput) *httptest.ResponseRecorder {
	raw, _ := json.Marshal(input)
	return request(g, http.MethodPost, _workEventPath, _token, "", string(raw))
}

func workInput(eventID, kind, text string) WorkInput {
	return WorkInput{RouteID: _testWorkRouteID, WorkID: "build-123", EventID: eventID,
		Kind: kind, Text: text}
}

func readWorkReceipt(t *testing.T, response *httptest.ResponseRecorder) WorkReceipt {
	t.Helper()
	if response.Code != http.StatusAccepted {
		t.Fatalf("status %d: %s", response.Code, response.Body.String())
	}
	var receipt WorkReceipt
	if err := json.Unmarshal(response.Body.Bytes(), &receipt); err != nil {
		t.Fatal(err)
	}
	return receipt
}

func readWorkRecord(t *testing.T, store Storage, id string) Record {
	t.Helper()
	record, err := store.Get(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return record
}

func TestWorkHTTPPersistsReceiptBeforeProvisioningAndReplaysAfterRestart(t *testing.T) {
	store, dsn := postgresForTest(t, 32)
	t.Cleanup(func() { _ = store.Close() })
	g, writer, provider := workHTTPGateway(t, store)
	input := workInput("delivery-1", _workTask, "Investigate checkout failure\r\nUse the supplied evidence")
	first := readWorkReceipt(t, postWork(g, input))
	record := readWorkRecord(t, store, first.ID)
	if writer.creates != 0 || provider.posts != 0 || len(record.Work.Attempts) != 1 ||
		len(record.Work.Events) != 1 || record.Work.Events[0].Queued || first.Attempt != 1 ||
		first.WorkID != input.WorkID || first.ReceivedAt.IsZero() || first.WorkURL == "" {
		t.Fatalf("acceptance must precede provisioning: %+v", record)
	}
	if record.Work.Attempts[0].Policy.AgentRef.Name != _testWorkAgentV1 ||
		record.Work.Attempts[0].Policy.EnvironmentRef.Name != _testWorkEnvironmentV1 ||
		record.Work.Events[0].Digest == "" || strings.Contains(record.Work.Events[0].Text, "\r") {
		t.Fatal("accepted event or release selection was not frozen")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenPostgreSQL(context.Background(), PostgreSQLConfig{DSN: dsn, AllowInsecureLocal: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	next, nextWriter, _ := workHTTPGateway(t, reopened)
	input.Text = strings.ReplaceAll(input.Text, "\r\n", "\n")
	second := readWorkReceipt(t, postWork(next, input))
	if !reflect.DeepEqual(first, second) || nextWriter.creates != 0 {
		t.Fatal("receipt changed or retry provisioned synchronously", first, second)
	}
}

func TestWorkHTTPAuthenticationRoutingAndValidation(t *testing.T) {
	g, _, _ := workHTTPGateway(t, testStore(t))
	valid := string(mustJSON(t, workInput("event-1", _workTask, "Investigate")))
	tests := []struct {
		name  string
		path  string
		token string
		body  string
		want  int
	}{
		{name: "unauthenticated", path: _workEventPath, token: "invalid", body: valid, want: http.StatusUnauthorized},
		{name: "namespace", path: "/v1/namespaces/other/work-events", token: _token, body: valid, want: http.StatusNotFound},
		{name: "unknown route", path: _workEventPath, token: _token, body: strings.ReplaceAll(valid, _testWorkRouteID, "missing"), want: http.StatusNotFound},
		{name: "unknown fields", path: _workEventPath, token: _token, body: `{"route_id":"payments","work_id":"build","event_id":"x","kind":"task","text":"test","agent":"override"}`, want: http.StatusBadRequest},
		{name: "blank task", path: _workEventPath, token: _token, body: string(mustJSON(t, workInput("event-2", _workTask, "  "))), want: http.StatusBadRequest},
		{name: "close text", path: _workEventPath, token: _token, body: string(mustJSON(t, workInput("event-3", _workClose, " "))), want: http.StatusBadRequest},
		{name: "unsupported kind", path: _workEventPath, token: _token, body: string(mustJSON(t, workInput("event-4", "done", "test"))), want: http.StatusBadRequest},
		{name: "invalid key", path: _workEventPath, token: _token, body: string(mustJSON(t, workInput("invalid/key", _workTask, "test"))), want: http.StatusBadRequest},
		{name: "event ID limit", path: _workEventPath, token: _token, body: string(mustJSON(t, workInput(strings.Repeat("x", 129), _workTask, "test"))), want: http.StatusBadRequest},
		{name: "task limit", path: _workEventPath, token: _token, body: string(mustJSON(t, workInput("large", _workTask, strings.Repeat("x", _maxInputBytes+1)))), want: http.StatusBadRequest},
		{name: "invalid UTF8", path: _workEventPath, token: _token, body: valid + string([]byte{0xff}), want: http.StatusBadRequest},
		{name: "trailing JSON", path: _workEventPath, token: _token, body: valid + `{}`, want: http.StatusBadRequest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			response := request(g, http.MethodPost, tt.path, tt.token, "", tt.body)
			if response.Code != tt.want {
				t.Fatalf("status %d, want %d: %s", response.Code, tt.want, response.Body.String())
			}
		})
	}
	route := workHTTPRoute()
	route.ID, route.Namespace = "other-route", _testWorkOther
	g.workRoutes[route.ID] = route
	input := workInput("event-5", _workTask, "test")
	input.RouteID = route.ID
	if response := postWork(g, input); response.Code != http.StatusNotFound {
		t.Fatal("route escaped principal namespace", response.Code)
	}
	if records, err := g.store.Records(t.Context()); err != nil || len(records) != 0 {
		t.Fatal("invalid request persisted work", err, len(records))
	}
}

func TestWorkHTTPDuplicateScopeConcurrentReuseAndFrozenPolicy(t *testing.T) {
	g, _, _ := workHTTPGateway(t, testStore(t))
	input := workInput("event-1", _workTask, "Investigate")
	first := readWorkReceipt(t, postWork(g, input))
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			if response := postWork(g, input); response.Code != http.StatusAccepted {
				t.Error(response.Code)
			}
		})
	}
	wg.Wait()
	changed := input
	changed.Text = "Different request"
	if response := postWork(g, changed); response.Code != http.StatusConflict {
		t.Fatal("changed payload reused delivery", response.Code)
	}
	changed = input
	changed.WorkID = "other-build"
	if response := postWork(g, changed); response.Code != http.StatusConflict {
		t.Fatal("delivery reused across work", response.Code)
	}
	route := g.workRoutes[input.RouteID]
	route.Agent, route.Environment = _testWorkAgentV2, "node-v2"
	g.workRoutes[route.ID] = route
	readWorkReceipt(t, postWork(g, workInput("event-2", _workEvidence, "Additional logs")))
	readWorkReceipt(t, postWork(g, workInput("event-3", _workTask, "Additional request")))
	record := readWorkRecord(t, g.store, first.ID)
	if len(record.Work.Attempts) != 1 || len(record.Work.Events) != 3 ||
		record.Work.Attempts[0].Policy.AgentRef.Name != _testWorkAgentV1 ||
		record.Work.Attempts[0].Policy.EnvironmentRef.Name != _testWorkEnvironmentV1 {
		t.Fatal("follow-up created another attempt or changed frozen selection", record.Work)
	}
	for _, eventID := range []string{"concurrent-1", "concurrent-2", "concurrent-3", "action-source-event"} {
		wg.Go(func() {
			if response := postWork(g, workInput(eventID, _workTask, "More evidence")); response.Code != http.StatusAccepted {
				t.Error(response.Code)
			}
		})
	}
	wg.Wait()
	record = readWorkRecord(t, g.store, first.ID)
	if len(record.Work.Attempts) != 1 || len(record.Work.Events) != 7 {
		t.Fatal("concurrent events created multiple attempts or lost receipt", record.Work)
	}
}

func TestWorkHTTPCloseTombstoneAndExplicitRestart(t *testing.T) {
	g, writer, _ := workHTTPGateway(t, testStore(t))
	if response := postWork(g, workInput("unknown", _workEvidence, "logs")); response.Code != http.StatusConflict {
		t.Fatal("evidence started unknown work", response.Code)
	}
	closeInput := workInput("close-unknown", _workClose, "")
	tombstone := readWorkReceipt(t, postWork(g, closeInput))
	record := readWorkRecord(t, g.store, tombstone.ID)
	if record.Work.State != _workClosed || len(record.Work.Attempts) != 0 || tombstone.Attempt != 0 || writer.creates != 0 {
		t.Fatal("unknown close provisioned work", record.Work)
	}
	if duplicate := readWorkReceipt(t, postWork(g, closeInput)); !reflect.DeepEqual(tombstone, duplicate) {
		t.Fatal("tombstone receipt changed")
	}
	for _, input := range []WorkInput{workInput("later-task", _workTask, "Investigate"), workInput("later-evidence", _workEvidence, "Logs")} {
		if response := postWork(g, input); response.Code != http.StatusConflict {
			t.Fatal("tombstone reopened", response.Code)
		}
	}
	fromTombstone := readWorkReceipt(t, postWork(g, workInput("later-restart", _workRestart, "Investigate")))
	if fromTombstone.ID != tombstone.ID || fromTombstone.Attempt != 1 {
		t.Fatal("explicit restart did not start first tombstone attempt", fromTombstone)
	}
	input := workInput("new-task", _workTask, "Investigate")
	input.WorkID = "build-456"
	first := readWorkReceipt(t, postWork(g, input))
	input.EventID, input.Kind, input.Text = "close-live", _workClose, ""
	readWorkReceipt(t, postWork(g, input))
	input.EventID, input.Kind, input.Text = "restart-early", _workRestart, "Investigate again"
	if response := postWork(g, input); response.Code != http.StatusConflict {
		t.Fatal("restart accepted before cleanup", response.Code)
	}
	if err := g.store.Update(t.Context(), first.ID, func(r *Record) error {
		r.Work.State = _workClosed
		r.Work.Attempts[0].Cleaned = true
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	route := g.workRoutes[input.RouteID]
	route.Agent = _testWorkAgentV2
	g.workRoutes[route.ID] = route
	second := readWorkReceipt(t, postWork(g, input))
	record = readWorkRecord(t, g.store, first.ID)
	if second.ID != first.ID || second.Attempt != 2 || record.Work.State != _workOpen ||
		len(record.Work.Attempts) != 2 || record.Work.Attempts[0].Policy.AgentRef.Name != _testWorkAgentV1 ||
		record.Work.Attempts[1].Policy.AgentRef.Name != _testWorkAgentV2 ||
		record.Work.Attempts[0].SessionName == record.Work.Attempts[1].SessionName {
		t.Fatal("restart did not create a new frozen attempt", record.Work)
	}
	if err := g.store.Update(t.Context(), first.ID, func(r *Record) error {
		r.Work.State = _workClosed
		r.Work.Attempts[0].Cleaned = false
		r.Work.Attempts[1].Cleaned = true
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	input.EventID = "restart-prior-unclean"
	if response := postWork(g, input); response.Code != http.StatusConflict {
		t.Fatal("restart ignored an unclean prior attempt", response.Code)
	}
	if err := g.store.Update(t.Context(), first.ID, func(r *Record) error {
		r.Work.Attempts[0].Cleaned = true
		for len(r.Work.Attempts) < _maxWorkAttempts {
			number := len(r.Work.Attempts) + 1
			r.Work.Attempts = append(r.Work.Attempts, WorkAttempt{Number: number,
				SessionName: workSessionName(r.Work.ID, number), Policy: workPolicy(route, r.Work.ID, number), Cleaned: true})
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	input.EventID = "restart-limit"
	if response := postWork(g, input); response.Code != http.StatusConflict {
		t.Fatal("restart exceeded attempt limit", response.Code)
	}
}

func TestWorkHTTPCloseFencesDirectSessionMessagesAndNamespaceViews(t *testing.T) {
	g, writer, _ := workHTTPGateway(t, testStore(t))
	first := readWorkReceipt(t, postWork(g, workInput("event-1", _workTask, "Investigate")))
	record := readWorkRecord(t, g.store, first.ID)
	attempt := record.Work.Attempts[0]
	session := &agentsv1alpha1.Session{ObjectMeta: metav1.ObjectMeta{Namespace: _testNamespace,
		Name: attempt.SessionName, UID: _testUID, Annotations: map[string]string{_workAnnotation: first.ID}},
		Spec: attempt.Policy, Status: agentsv1alpha1.SessionStatus{OpenAISessionID: _testRemoteID, Phase: _testWorkPhase}}
	if err := writer.Client.Create(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	if err := g.store.Update(t.Context(), first.ID, func(r *Record) error {
		r.Work.Attempts[0].SessionUID = _testUID
		r.Work.Attempts[0].CreationClaimed = true
		r.Work.Attempts[0].InitialQueued = true
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	messagePath := "/v1/namespaces/team/sessions/" + attempt.SessionName + "/messages"
	if response := request(g, http.MethodPost, messagePath, _token, "message-1", `{"text":"More logs"}`); response.Code != http.StatusAccepted {
		t.Fatal(response.Code, response.Body.String())
	}
	inputGate := g.gate(_testUID)
	inputGate.Lock()
	closed := make(chan *httptest.ResponseRecorder, 1)
	go func() { closed <- postWork(g, workInput("close", _workClose, "")) }()
	select {
	case response := <-closed:
		inputGate.Unlock()
		t.Fatal("close passed in-flight Session fence", response.Code)
	case <-time.After(20 * time.Millisecond):
	}
	inputGate.Unlock()
	select {
	case response := <-closed:
		readWorkReceipt(t, response)
	case <-time.After(2 * time.Second):
		t.Fatal("close did not complete after Session fence released")
	}
	if response := request(g, http.MethodPost, messagePath, _token, "message-2", `{"text":"More logs"}`); response.Code != http.StatusGone {
		t.Fatal("closed work accepted direct message", response.Code, response.Body.String())
	}
	detail := request(g, http.MethodGet, first.WorkURL, _token, "", "")
	var detailBody struct {
		Summary      WorkSummary `json:"summary"`
		Conversation *Record     `json:"conversation"`
	}
	if detail.Code != http.StatusOK || json.Unmarshal(detail.Body.Bytes(), &detailBody) != nil ||
		detailBody.Summary.Phase != _testWorkPhase || detailBody.Conversation == nil {
		t.Fatal("work detail missing bound Session", detail.Code, detail.Body.String())
	}
	if response := request(g, http.MethodGet, strings.Replace(first.WorkURL, "/team/", "/other/", 1), _token, "", ""); response.Code != http.StatusNotFound {
		t.Fatal("cross-namespace work detail leaked", response.Code)
	}
	other := workHTTPRoute()
	other.ID, other.Namespace = _testWorkOther, _testWorkOther
	id := workIdentity(_testWorkOther, _testWorkOther, other.Source, "hidden")
	if err := g.store.Update(t.Context(), id, func(r *Record) error {
		r.Namespace = _testWorkOther
		r.Work = &Work{ID: id, RouteID: other.ID, ExternalID: "hidden", Source: other.Source, State: _workClosed,
			Events: []WorkEvent{{ID: "close", Digest: "digest", Kind: _workClose, ReceivedAt: time.Now().UTC()}}}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	collection := request(g, http.MethodGet, "/v1/namespaces/team/work", _token, "", "")
	var body struct {
		Work []WorkSummary `json:"work"`
	}
	if collection.Code != http.StatusOK || json.Unmarshal(collection.Body.Bytes(), &body) != nil ||
		len(body.Work) != 1 || body.Work[0].ID != first.ID || body.Work[0].SessionUID != _testUID {
		t.Fatal("collection failed namespace binding", collection.Code, collection.Body.String())
	}
	var sessions agentsv1alpha1.SessionList
	if err := writer.List(context.Background(), &sessions, client.InNamespace(_testNamespace)); err != nil || len(sessions.Items) != 1 {
		t.Fatal(err, len(sessions.Items))
	}
}
