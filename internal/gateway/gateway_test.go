package gateway

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	"github.com/softcane/codex-session-operator/internal/openaiapi"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

const (
	_testInProgress       = "in_progress"
	_testCallID           = "call-1"
	_testTurnID           = "turn-1"
	_testNamespace        = "team"
	_testRemoteID         = "sess-1"
	_testNone             = "none"
	_testSessionName      = "work"
	_testUID              = "uid-1"
	_testMessagePath      = "/v1/namespaces/team/sessions/work/messages"
	_testFirstMessage     = "first message"
	_testFirstMessageBody = `{"text":"first message"}`
)
const _token = "0123456789abcdefghijklmnopqrstuvwxyz"

type simulator struct {
	mu            sync.Mutex
	remotePresent bool
	purgeErr      error
	history       openaiapi.History
	keys          map[string]string
	commits       int
	posts         int
	lose          bool
	subscribed    bool
	stream        *testStream
	beforeSend    func(string, json.RawMessage)
}

func (p *simulator) GetSession(context.Context, string) (openaiapi.Session, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.purgeErr != nil {
		return openaiapi.Session{}, p.purgeErr
	}
	if !p.remotePresent {
		return openaiapi.Session{}, openaiapi.ErrNotFound
	}
	return openaiapi.Session{ID: _testRemoteID, OwnerUID: _testUID}, nil
}

func (p *simulator) FindSession(context.Context, string) (openaiapi.Session, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.purgeErr != nil {
		return openaiapi.Session{}, p.purgeErr
	}
	if !p.remotePresent {
		return openaiapi.Session{}, nil
	}
	return openaiapi.Session{ID: _testRemoteID, OwnerUID: _testUID}, nil
}

type testStream struct {
	ctx    context.Context
	events chan json.RawMessage
	event  json.RawMessage
}

func (s *testStream) Next() bool {
	select {
	case <-s.ctx.Done():
		return false
	case event, ok := <-s.events:
		s.event = event
		return ok
	}
}
func (s *testStream) Event() json.RawMessage { return s.event }
func (s *testStream) Err() error             { return s.ctx.Err() }
func (s *testStream) Close() error           { return nil }

func (p *simulator) SubmitInput(_ context.Context, _ string, key string, event json.RawMessage) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.beforeSend != nil {
		p.beforeSend(key, event)
	}
	p.posts++
	if previous, ok := p.keys[key]; ok {
		if previous != string(event) {
			return errConflict
		}
		return nil
	}
	p.keys[key] = string(event)
	p.commits++
	if p.lose {
		p.lose = false
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (p *simulator) SavedHistory(context.Context, string) (openaiapi.History, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	raw, _ := json.Marshal(p.history)
	var result openaiapi.History
	_ = json.Unmarshal(raw, &result)
	return result, nil
}
func (p *simulator) OpenMessageStream(ctx context.Context, _ string) (openaiapi.MessageStream, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.subscribed = true
	p.stream = &testStream{ctx: ctx, events: make(chan json.RawMessage, 1)}
	return p.stream, nil
}

func testGateway(t *testing.T, store Storage, p *simulator, objects ...client.Object) (*Gateway, client.Client) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := agentsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	k8s := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
	g, err := New(store, k8s, p, []Principal{{Token: _token, Subject: "writer", Namespace: _testNamespace}, {Token: _token + "-approver", Subject: _testApprover, Namespace: _testNamespace, Approver: true}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	g.ctx = ctx
	t.Cleanup(func() {
		cancel()
		g.mu.Lock()
		for _, w := range g.watches {
			w.stop()
		}
		g.mu.Unlock()
		g.wg.Wait()
	})
	return g, k8s
}
func testSession() *agentsv1alpha1.Session {
	return &agentsv1alpha1.Session{ObjectMeta: metav1.ObjectMeta{Name: _testSessionName, Namespace: _testNamespace, UID: "uid-1"}, Status: agentsv1alpha1.SessionStatus{OpenAISessionID: _testRemoteID}}
}
func request(g *Gateway, method, path, token, key, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Idempotency-Key", key)
	w := httptest.NewRecorder()
	g.ServeHTTP(w, req)
	return w
}
func mustRecord(t *testing.T, store Storage) Record {
	t.Helper()
	r, err := store.Get(context.Background(), "uid-1")
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func simulatorForTest() *simulator {
	return &simulator{keys: make(map[string]string), remotePresent: true,
		history: openaiapi.History{ID: _testRemoteID, Status: "idle", Metadata: map[string]string{"kubernetes_uid": "uid-1", "operator": "codex-session-operator"}}}
}

func TestDurableRetryAfterLostAcceptanceAndRestart(t *testing.T) {
	store, dsn := postgresForTest(t, 32)
	p := simulatorForTest()
	p.lose = true
	g, _ := testGateway(t, store, p, testSession())
	p.beforeSend = func(key string, event json.RawMessage) {
		r := mustRecord(t, store)
		if !p.subscribed || len(r.Submissions) != 1 || r.Submissions[0].RemoteKey != key || string(r.Submissions[0].Event) != string(event) || r.Submissions[0].Attempts == 0 {
			t.Error("send preceded subscription or durable intent")
		}
	}
	path := _testMessagePath
	if got := request(g, http.MethodPost, path, _token, "message-1", `{"text":"same text"}`).Code; got != http.StatusAccepted {
		t.Fatal(got)
	}
	if err := g.sync(context.Background(), "uid-1"); err == nil {
		t.Fatal("lost response not surfaced")
	}
	g.stopWatch("uid-1")
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := OpenPostgreSQL(context.Background(), PostgreSQLConfig{DSN: dsn, AllowInsecureLocal: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = restarted.Close() }()
	p.beforeSend = nil
	next, _ := testGateway(t, restarted, p, testSession())
	if err := next.sync(context.Background(), "uid-1"); err != nil {
		t.Fatal(err)
	}
	record := mustRecord(t, restarted)
	if p.commits != 1 || p.posts != 2 || !record.Submissions[0].Accepted || record.Submissions[0].Attempts != 2 {
		t.Fatalf("duplicate or missing acceptance: commits=%d posts=%d record=%+v", p.commits, p.posts, record.Submissions)
	}
	if got := request(next, http.MethodPost, path, _token, "message-1", `{"text":"different"}`).Code; got != http.StatusConflict {
		t.Fatal(got)
	}
	if got := request(next, http.MethodPost, path, _token, "message-1", `{"text":"same text"}`).Code; got != http.StatusAccepted {
		t.Fatal(got)
	}
	if got := request(next, http.MethodPost, path, _token, "message-2", `{"text":"same text"}`).Code; got != http.StatusAccepted {
		t.Fatal(got)
	}
	if err := next.sync(context.Background(), "uid-1"); err != nil {
		t.Fatal(err)
	}
	if p.commits != 2 {
		t.Fatalf("distinct logical messages did not get distinct keys: %d", p.commits)
	}
}

func TestAuthenticationEveryEndpointAndTenant(t *testing.T) {
	store := testStore(t)
	g, _ := testGateway(t, store, simulatorForTest(), testSession())
	for _, endpoint := range []string{"", "/messages", "/history", "/events", "/cancel", "/actions", "/actions/turn:call"} {
		t.Run(endpoint, func(t *testing.T) {
			if got := request(g, http.MethodGet, "/v1/namespaces/team/sessions/work"+endpoint, "invalid", "", "").Code; got != http.StatusUnauthorized {
				t.Fatal(got)
			}
			if got := request(g, http.MethodGet, "/v1/namespaces/other/sessions/work"+endpoint, _token, "", "").Code; got != http.StatusNotFound {
				t.Fatal(got)
			}
		})
	}
	if records, err := store.Records(t.Context()); err != nil || len(records) != 0 {
		t.Fatal("unauthenticated request accessed store")
	}
}

func TestNoneInitialMessageNeverSentAgain(t *testing.T) {
	store := testStore(t)
	p := simulatorForTest()
	session := testSession()
	session.Status.OpenAISessionID = ""
	session.Spec.EnvironmentRef = &agentsv1alpha1.AgentReference{Name: _testNone}
	environment := &agentsv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: _testNone, Namespace: _testNamespace}, Spec: agentsv1alpha1.EnvironmentSpec{Mode: _testNone}}
	g, k8s := testGateway(t, store, p, session, environment)
	path := _testMessagePath
	if got := request(g, http.MethodPost, path, _token, "first", _testFirstMessageBody).Code; got != http.StatusAccepted {
		t.Fatal(got)
	}
	if got := request(g, http.MethodPost, path, _token, "second", `{"text":"follow up"}`).Code; got != http.StatusAccepted {
		t.Fatal(got)
	}
	text, key, err := g.InitialInput(context.Background(), "uid-1")
	if err != nil || text != _testFirstMessage || key != wireKey("uid-1", "first") {
		t.Fatal(text, key, err)
	}
	session.Status.OpenAISessionID = _testRemoteID
	session.Status.EnvironmentSnapshot = &agentsv1alpha1.SessionEnvironmentSnapshot{Profile: agentsv1alpha1.EnvironmentSpec{Mode: _testNone}}
	if err := k8s.Update(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	if err := g.sync(context.Background(), "uid-1"); err != nil {
		t.Fatal(err)
	}
	if p.commits != 1 || p.keys[wireKey("uid-1", "first")] != "" || p.keys[wireKey("uid-1", "second")] == "" {
		t.Fatal("initial message was replayed")
	}
	if got := request(g, http.MethodPost, path, _token, "first", _testFirstMessageBody).Code; got != http.StatusAccepted {
		t.Fatal(got)
	}
}

func TestApprovalDurableCallAndResultRecovery(t *testing.T) {
	store := testStore(t)
	p := simulatorForTest()
	p.history.Status = "requires_action"
	p.history.Actions = []openaiapi.PendingAction{{Type: _functionCall, TurnID: _testTurnID, CallID: _testCallID, Name: "gateway_record_note", Arguments: json.RawMessage(`{"note":"reviewed write"}`)}}
	g, _ := testGateway(t, store, p, testSession())
	if got := request(g, http.MethodGet, "/v1/namespaces/team/sessions/work", _token, "", "").Code; got != 200 {
		t.Fatal(got)
	}
	if err := g.sync(context.Background(), "uid-1"); err != nil {
		t.Fatal(err)
	}
	if len(mustRecord(t, store).Notes) != 0 || p.commits != 0 {
		t.Fatal("write ran before approval")
	}
	path := "/v1/namespaces/team/sessions/work/actions/turn-1:call-1"
	if got := request(g, http.MethodPost, path, _token, "", `{"decision":"approve"}`).Code; got != 403 {
		t.Fatal(got)
	}
	if got := request(g, http.MethodPost, path, _token+"-approver", "", `{"decision":"approve"}`).Code; got != 202 {
		t.Fatal(got)
	}
	p.lose = true
	if err := g.sync(context.Background(), "uid-1"); err == nil {
		t.Fatal("lost result acceptance not surfaced")
	}
	g.stopWatch("uid-1")
	next, _ := testGateway(t, store, p, testSession())
	if err := next.sync(context.Background(), "uid-1"); err != nil {
		t.Fatal(err)
	}
	r := mustRecord(t, store)
	if len(r.Notes) != 1 || len(r.Submissions) != 1 || p.commits != 1 || !r.Submissions[0].Accepted || r.Actions["turn-1:call-1"].DecidedBy != _testApprover {
		t.Fatal("approval/result lost or write duplicated")
	}
	// Reusing a call ID for changed arguments must not execute it again.
	p.history.Actions[0].Arguments = json.RawMessage(`{"note":"different write"}`)
	if err := next.sync(context.Background(), "uid-1"); err == nil {
		t.Fatal("reused call identity accepted")
	}
}

func TestExternalFunctionRecoveryUsesSameOperationID(t *testing.T) {
	store := testStore(t)
	p := simulatorForTest()
	p.history.Actions = []openaiapi.PendingAction{{Type: _functionCall, TurnID: _testTurnID, CallID: _testCallID, Name: _testExternal, Arguments: json.RawMessage(`{}`)}}
	g, _ := testGateway(t, store, p, testSession())
	ids := make(map[string]bool)
	outcomes := make(map[string]string)
	commits := 0
	attempts := 0
	g.functions[_testExternal] = Function{Consequential: true, DownstreamDeduplicates: true, Execute: func(_ context.Context, op Operation) (string, error) {
		attempts++
		ids[op.ID] = true
		if _, found := outcomes[op.ID]; !found {
			commits++
			outcomes[op.ID] = "committed once by downstream ID"
		}
		if attempts == 1 {
			return "", io.ErrUnexpectedEOF
		}
		return outcomes[op.ID], nil
	}}
	_ = request(g, http.MethodGet, "/v1/namespaces/team/sessions/work", _token, "", "")
	if err := g.sync(context.Background(), "uid-1"); err != nil {
		t.Fatal(err)
	}
	if err := g.decide(t.Context(), "uid-1", "turn-1:call-1", "approve", _testApprover); err != nil {
		t.Fatal(err)
	}
	if err := g.sync(context.Background(), "uid-1"); err == nil {
		t.Fatal("ambiguous handler result hidden")
	}
	if err := g.sync(context.Background(), "uid-1"); err != nil {
		t.Fatal(err)
	}
	if attempts != 2 || len(ids) != 1 || commits != 1 {
		t.Fatal("downstream operation identity changed")
	}
}

func TestStreamLossRecoversSavedHistoryWithoutMessageReplay(t *testing.T) {
	store := testStore(t)
	p := simulatorForTest()
	g, _ := testGateway(t, store, p, testSession())
	_ = request(g, http.MethodPost, _testMessagePath, _token, "m1", `{"text":"work"}`)
	if err := g.sync(context.Background(), "uid-1"); err != nil {
		t.Fatal(err)
	}
	p.mu.Lock()
	close(p.stream.events)
	p.history.Items = []json.RawMessage{json.RawMessage(`{"id":"item-final","type":"message","content":[{"text":"saved final"}]}`)}
	p.history.Turns = []json.RawMessage{json.RawMessage(`{"id":"turn-1","status":"completed"}`)}
	p.mu.Unlock()
	g.mu.Lock()
	done := g.watches["uid-1"].done
	g.mu.Unlock()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("stream did not terminate")
	}
	if err := g.sync(context.Background(), "uid-1"); err != nil {
		t.Fatal(err)
	}
	record := mustRecord(t, store)
	if p.commits != 1 || len(record.History.Items) != 1 || len(record.History.Turns) != 1 || record.RecoveryRequired {
		t.Fatal("saved state not recovered")
	}
}

func TestActiveSteeringAndTargetedCancellation(t *testing.T) {
	store := testStore(t)
	p := simulatorForTest()
	p.history.Status = _testInProgress
	p.history.Turns = []json.RawMessage{json.RawMessage(`{"id":"turn-1","status":"in_progress","subagent_id":null}`)}
	g, _ := testGateway(t, store, p, testSession())
	_ = request(g, http.MethodPost, _testMessagePath, _token, "steer", `{"text":"change direction"}`)
	if err := g.sync(context.Background(), "uid-1"); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(p.keys[wireKey("uid-1", "steer")], "agent.session.input.message") {
		t.Fatal("steering not sent to existing Session")
	}
	_ = request(g, http.MethodPost, "/v1/namespaces/team/sessions/work/cancel", _token, "cancel", `{"turn_id":"turn-1"}`)
	p.lose = true
	if err := g.sync(context.Background(), "uid-1"); err == nil {
		t.Fatal("lost cancel hidden")
	}
	p.history.Turns = []json.RawMessage{json.RawMessage(`{"id":"turn-1","status":"cancelled"}`), json.RawMessage(`{"id":"turn-2","status":"in_progress"}`)}
	if err := g.sync(context.Background(), "uid-1"); err != nil {
		t.Fatal(err)
	}
	if p.posts != 2 {
		t.Fatal("retry cancelled a later turn")
	}
}

func TestCleanupStopsInputStreamAndUIDReuse(t *testing.T) {
	store := testStore(t)
	p := simulatorForTest()
	session := testSession()
	g, k8s := testGateway(t, store, p, session)
	_ = request(g, http.MethodPost, _testMessagePath, _token, "queued", `{"text":"must not run after closure"}`)
	p.history.Status = "requires_action"
	if err := g.Quiesce(context.Background(), "uid-1", _testRemoteID); err != nil {
		t.Fatal(err)
	}
	if got := request(g, http.MethodPost, _testMessagePath, _token, "later", `{"text":"blocked"}`).Code; got != http.StatusGone {
		t.Fatal(got)
	}
	if err := g.sync(context.Background(), "uid-1"); err != nil {
		t.Fatal(err)
	}
	if p.commits != 1 || p.keys[wireKey("uid-1", "queued")] != "" {
		t.Fatal("queued input ran after cleanup")
	}
	if err := k8s.Delete(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	replacement := testSession()
	replacement.UID = "uid-2"
	replacement.ResourceVersion = ""
	if err := k8s.Create(context.Background(), replacement); err != nil {
		t.Fatal(err)
	}
	if got := request(g, http.MethodGet, "/v1/namespaces/team/sessions/work", _token, "", "").Code; got != http.StatusOK {
		t.Fatal(got)
	}
	old := mustRecord(t, store)
	if !old.Closed {
		t.Fatal("old UID revived")
	}
}

func TestSSELiveEventsAndExpiredCursorSnapshot(t *testing.T) {
	store := testStore(t)
	g, _ := testGateway(t, store, simulatorForTest(), testSession())
	_ = request(g, http.MethodGet, "/v1/namespaces/team/sessions/work", _token, "", "")
	if err := store.Update(t.Context(), "uid-1", func(r *Record) error {
		for range _maxEvents + 2 {
			appendEvent(r, json.RawMessage(`{"session_id":"sess-1","type":"agent.session.turn.output_text.delta","delta":"live"}`))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(g)
	defer server.Close()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, server.URL+"/v1/namespaces/team/sessions/work/events", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+_token)
	req.Header.Set("Last-Event-ID", "1")
	httpClient := &http.Client{Timeout: 3 * time.Second}
	response, err := httpClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	reader := bufio.NewReader(response.Body)
	readData := func() string {
		t.Helper()
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				t.Fatal(err)
			}
			if strings.HasPrefix(line, "data: ") {
				return line
			}
		}
	}
	if data := readData(); !strings.Contains(data, "gateway.recovery.snapshot") {
		t.Fatal("expired cursor did not recover saved snapshot")
	}
	if err := store.Update(t.Context(), "uid-1", func(r *Record) error {
		appendEvent(r, json.RawMessage(`{"session_id":"sess-1","type":"agent.session.turn.output_text.delta","delta":"fresh"}`))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if data := readData(); !strings.Contains(data, `"delta":"fresh"`) {
		t.Fatal("live event missing")
	}
}

func TestServerShutdownAndTLSBoundary(t *testing.T) {
	store := testStore(t)
	g, _ := testGateway(t, store, simulatorForTest(), testSession())
	if err := (&Server{Gateway: g, Address: "0.0.0.0:0"}).Start(context.Background()); err == nil {
		t.Fatal("exposed HTTP listener accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan error, 1)
	go func() { done <- (&Server{Gateway: g, Address: "127.0.0.1:0"}).Start(ctx) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server did not join worker shutdown")
	}
}

func TestComputerApprovalVariantsAndDeniedFunction(t *testing.T) {
	tests := []struct {
		name      string
		request   string
		decision  string
		wantError bool
		want      string
	}{
		{name: "browser approve", request: `{"type":"browser_origin_access","origin":"https://example.com"}`, decision: _approve, want: `"decision":"approve"`},
		{name: "browser deny", request: `{"type":"browser_origin_access"}`, decision: _deny, want: `"decision":"deny"`},
		{name: "authentication cancel", request: `{"type":"browser_authentication"}`, decision: _deny, want: `"action":"cancel"`},
		{name: "authentication unsupported", request: `{"type":"browser_authentication"}`, decision: _approve, wantError: true},
		{name: "unknown action", request: `{"type":"unknown"}`, decision: _approve, wantError: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event, err := approvalEvent(openaiapi.PendingAction{RequestID: "request-1", Request: json.RawMessage(tt.request)}, tt.decision)
			if tt.wantError {
				if err == nil {
					t.Fatal("unsupported approval allowed")
				}
				return
			}
			if err != nil || !strings.Contains(string(event), tt.want) || !strings.Contains(string(event), `"request_id":"request-1"`) {
				t.Fatal(string(event), err)
			}
		})
	}
	store := testStore(t)
	p := simulatorForTest()
	p.history.Actions = []openaiapi.PendingAction{{Type: _functionCall, TurnID: _testTurnID, CallID: _testCallID, Name: "gateway_record_note", Arguments: json.RawMessage(`{"note":"denied"}`)}}
	g, _ := testGateway(t, store, p, testSession())
	_ = request(g, http.MethodGet, "/v1/namespaces/team/sessions/work", _token, "", "")
	if err := g.sync(context.Background(), "uid-1"); err != nil {
		t.Fatal(err)
	}
	if err := g.decide(t.Context(), "uid-1", "turn-1:call-1", "deny", _testApprover); err != nil {
		t.Fatal(err)
	}
	if err := g.sync(context.Background(), "uid-1"); err != nil {
		t.Fatal(err)
	}
	if len(mustRecord(t, store).Notes) != 0 || !strings.Contains(p.keys[wireKey("uid-1", resultKey("uid-1", "turn-1:call-1"))], `"success":false`) {
		t.Fatal("denied action executed")
	}
}

func TestPendingCallIgnoresJSONFormattingAndKeyOrder(t *testing.T) {
	store := testStore(t)
	p := simulatorForTest()
	p.history.Actions = []openaiapi.PendingAction{{Type: _functionCall, TurnID: _testTurnID, CallID: _testCallID, Name: _echoFunction, Arguments: json.RawMessage(`{ "a": 1, "b": 2 }`)}}
	g, _ := testGateway(t, store, p, testSession())
	_ = request(g, http.MethodGet, "/v1/namespaces/team/sessions/work", _token, "", "")
	if err := g.sync(context.Background(), "uid-1"); err != nil {
		t.Fatal(err)
	}
	p.history.Actions[0].Arguments = json.RawMessage(`{"b":2,"a":1}`)
	if err := g.sync(context.Background(), "uid-1"); err != nil {
		t.Fatal(err)
	}
	if p.commits != 1 {
		t.Fatal("format change repeated function result")
	}
	p.history.Actions[0].Arguments = json.RawMessage(`{"b":3,"a":1}`)
	if err := g.sync(context.Background(), "uid-1"); err == nil {
		t.Fatal("actual argument change was allowed")
	}
}

func TestLegacyPendingSubmissionKeepsItsPreviouslySentKey(t *testing.T) {
	store := testStore(t)
	p := simulatorForTest()
	g, _ := testGateway(t, store, p, testSession())
	_ = request(g, http.MethodPost, _testMessagePath, _token, _testLegacy, `{"text":"old pending payload"}`)
	if err := store.Update(t.Context(), "uid-1", func(r *Record) error { r.Submissions[0].RemoteKey = ""; r.Submissions[0].Attempts = 1; return nil }); err != nil {
		t.Fatal(err)
	}
	// Simulate old gateway delivery committing before losing its response.
	p.keys[_testLegacy] = string(mustRecord(t, store).Submissions[0].Event)
	p.commits = 1
	if err := g.sync(context.Background(), "uid-1"); err != nil {
		t.Fatal(err)
	}
	if p.commits != 1 || mustRecord(t, store).Submissions[0].RemoteKey != _testLegacy {
		t.Fatal("migration changed an ambiguous submission key")
	}
}

func TestGatewaySubmissionKeysAreUniqueAcrossSessionUIDs(t *testing.T) {
	if wireKey("uid-1", "same-caller-key") == wireKey("uid-2", "same-caller-key") {
		t.Fatal("remote keys collide across Sessions")
	}
	if wireKey("uid-1", "a") == wireKey("uid-1", "b") {
		t.Fatal("remote keys collide across messages")
	}
}

func TestCancellationRecognizesPinnedTurnStates(t *testing.T) {
	for _, status := range []string{"queued", "in_progress", "waiting", "completed", "failed", "cancelled"} {
		t.Run(status, func(t *testing.T) {
			raw, _ := json.Marshal(map[string]any{"id": "turn-1", "status": status, "subagent_id": nil})
			want := status == "queued" || status == _testInProgress || status == "waiting"
			if got := activeTurn(openaiapi.History{Turns: []json.RawMessage{raw}}, "turn-1"); got != want {
				t.Fatalf("state %q active=%v want=%v", status, got, want)
			}
		})
	}
}

func TestForeignRemoteOwnershipCannotStreamOrAcknowledgeInput(t *testing.T) {
	store := testStore(t)
	p := simulatorForTest()
	p.history.Metadata["kubernetes_uid"] = "foreign-uid"
	session := testSession()
	session.Status.EnvironmentSnapshot = &agentsv1alpha1.SessionEnvironmentSnapshot{Profile: agentsv1alpha1.EnvironmentSpec{Mode: _noneMode}}
	g, _ := testGateway(t, store, p, session)
	_ = request(g, http.MethodPost, _testMessagePath, _token, "message", `{"text":"protected input"}`)
	if err := g.sync(context.Background(), "uid-1"); err == nil {
		t.Fatal("foreign Session accepted")
	}
	if p.subscribed || p.commits != 0 || mustRecord(t, store).Submissions[0].Accepted {
		t.Fatal("foreign Session streamed, received or acknowledged input")
	}
}

func TestGatewayReadinessTracksWorkerLifetime(t *testing.T) {
	store := testStore(t)
	p := simulatorForTest()
	g, _ := testGateway(t, store, p, testSession())
	g.mu.Lock()
	g.ctx = nil
	g.mu.Unlock()
	if err := g.Ready(nil); err == nil {
		t.Fatal("unstarted worker was ready")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := g.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := g.Ready(nil); err == nil {
		t.Fatal("stopped worker was ready")
	}
}
