package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	"github.com/softcane/codex-session-operator/internal/openaiapi"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	_scenarioSessionPath           = "/v1/namespaces/team/sessions/work"
	_scenarioMessage               = "message"
	_scenarioCancel                = "cancel"
	_scenarioApproval              = "approval"
	_scenarioOwnershipMismatch     = "OwnershipMismatch"
	_scenarioConfigurationMismatch = "ConfigurationMismatch"
	_scenarioForeignRemote         = "foreign-remote"
	_scenarioOperator              = "operator"
	_scenarioOwnerScope            = "scope-a"
	_scenarioLimited               = "limited"
	_scenarioRemoteMissing         = "RemoteSessionMissing"
)

// SavedHistory models the console deletion boundary. The ordinary simulator
// intentionally permits history reads regardless of its purge-presence switch.
type scenarioHistoryProvider struct {
	Provider

	mu          sync.Mutex
	err         error
	reads       int
	alterRead   int
	alterResult func(*openaiapi.History)
}

var _ Provider = (*scenarioHistoryProvider)(nil)

func (p *scenarioHistoryProvider) SavedHistory(ctx context.Context, id string) (openaiapi.History, error) {
	p.mu.Lock()
	p.reads++
	err := p.err
	alter := p.reads == p.alterRead
	p.mu.Unlock()
	if err != nil {
		return openaiapi.History{}, err
	}
	history, err := p.Provider.SavedHistory(ctx, id)
	if err == nil && alter {
		p.alterResult(&history)
	}
	return history, err
}

func (p *scenarioHistoryProvider) fail(err error) {
	p.mu.Lock()
	p.err = err
	p.mu.Unlock()
}

func scenarioPendingAction() openaiapi.PendingAction {
	return openaiapi.PendingAction{Type: _functionCall, TurnID: _testTurnID,
		CallID: _testCallID, Name: _noteFunction,
		Arguments: json.RawMessage(`{"note":"approved evidence"}`)}
}

func scenarioRecover(t *testing.T, g *Gateway, store Storage, p *simulator) {
	t.Helper()
	if response := request(g, http.MethodGet, _scenarioSessionPath, _token, "", ""); response.Code != http.StatusOK {
		t.Fatalf("bind Session: %d %s", response.Code, response.Body.String())
	}
	if err := g.recover(t.Context(), _testUID, p.history, false); err != nil {
		t.Fatal(err)
	}
	if err := store.Update(t.Context(), _testUID, func(r *Record) error {
		r.RemoteID = _testRemoteID
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func scenarioMutation(g *Gateway, kind string) *httptest.ResponseRecorder {
	switch kind {
	case _scenarioMessage:
		return request(g, http.MethodPost, _testMessagePath, _token, "scenario-message", `{"text":"new input"}`)
	case _scenarioCancel:
		return request(g, http.MethodPost, _scenarioSessionPath+"/cancel", _token, "scenario-cancel", `{"turn_id":"turn-1"}`)
	default:
		return request(g, http.MethodPost, _scenarioSessionPath+"/actions/"+actionID(scenarioPendingAction()),
			_token+"-approver", "", `{"decision":"approve"}`)
	}
}

type scenarioChangingReader struct {
	client.Reader

	sessionReads int
	changeAt     int
	change       func(*agentsv1alpha1.Session)
}

func (r *scenarioChangingReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if err := r.Reader.Get(ctx, key, obj, opts...); err != nil {
		return err
	}
	session, ok := obj.(*agentsv1alpha1.Session)
	if !ok {
		return nil
	}
	r.sessionReads++
	if r.sessionReads == r.changeAt {
		r.change(session)
	}
	return nil
}

func TestScenarioGatewayRejectsWritesToUnreadyRemoteSession(t *testing.T) {
	tests := []struct {
		name      string
		condition metav1.Condition
	}{
		{name: "console deletion", condition: metav1.Condition{Type: _readyCondition,
			Status: metav1.ConditionFalse, Reason: _scenarioRemoteMissing, ObservedGeneration: 2}},
		{name: "ownership mismatch", condition: metav1.Condition{Type: _readyCondition,
			Status: metav1.ConditionFalse, Reason: _scenarioOwnershipMismatch, ObservedGeneration: 2}},
		{name: "immutable configuration mismatch", condition: metav1.Condition{Type: _readyCondition,
			Status: metav1.ConditionFalse, Reason: _scenarioConfigurationMismatch, ObservedGeneration: 2}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, kind := range []string{_scenarioMessage, _scenarioCancel, _scenarioApproval} {
				t.Run(kind, func(t *testing.T) {
					store := testStore(t)
					p := simulatorForTest()
					p.history.Actions = []openaiapi.PendingAction{scenarioPendingAction()}
					session := testSession()
					session.Generation = 2
					session.Spec.SpendControl = &agentsv1alpha1.SessionSpendControl{Mode: _testWorkUnlimited}
					session.Status.Conditions = []metav1.Condition{tt.condition}
					g, _ := testGateway(t, store, p, session)
					scenarioRecover(t, g, store, p)
					before := mustRecord(t, store)
					response := scenarioMutation(g, kind)
					if response.Code != http.StatusServiceUnavailable {
						t.Fatalf("%s accepted while Session is %s: %d %s", kind, tt.name, response.Code, response.Body.String())
					}
					after := mustRecord(t, store)
					if !reflect.DeepEqual(before.Submissions, after.Submissions) || !reflect.DeepEqual(before.Actions, after.Actions) || p.posts != 0 {
						t.Fatal("rejected mutation changed durable input or actions")
					}
					if response := request(g, http.MethodGet, _scenarioSessionPath+"/history", _token, "", ""); response.Code != http.StatusOK {
						t.Fatal("unready Session lost saved history", response.Code)
					}
				})
			}
		})
	}
}

func TestScenarioGatewayClosedWorkRejectsEveryMutation(t *testing.T) {
	for _, kind := range []string{_scenarioMessage, _scenarioCancel, _scenarioApproval} {
		t.Run(kind, func(t *testing.T) {
			store := testStore(t)
			p := simulatorForTest()
			p.history.Actions = []openaiapi.PendingAction{scenarioPendingAction()}
			session := testSession()
			session.Annotations = map[string]string{_workAnnotation: "scenario-closed-work"}
			g, _ := testGateway(t, store, p, session)
			scenarioRecover(t, g, store, p)
			if err := store.Update(t.Context(), "scenario-closed-work", func(r *Record) error {
				r.Namespace = _testNamespace
				r.Work = &Work{ID: r.UID, State: _workClosing, Attempts: []WorkAttempt{
					{Number: 1, SessionName: session.Name, SessionUID: string(session.UID),
						CreationClaimed: true, InitialQueued: true, Policy: session.Spec},
				}}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			before := mustRecord(t, store)
			response := scenarioMutation(g, kind)
			if response.Code != http.StatusGone {
				t.Fatalf("%s accepted after work close: %d %s", kind, response.Code, response.Body.String())
			}
			after := mustRecord(t, store)
			if !reflect.DeepEqual(before.Actions, after.Actions) || !reflect.DeepEqual(before.Submissions, after.Submissions) {
				t.Fatal("closed attempt persisted mutation")
			}
		})
	}
}

func TestScenarioGatewayConsoleDeletionRetainsHistoryAndJoinsStream(t *testing.T) {
	store := testStore(t)
	p := simulatorForTest()
	p.history.Actions = []openaiapi.PendingAction{scenarioPendingAction()}
	g, _ := testGateway(t, store, p, testSession())
	scenarioRecover(t, g, store, p)
	if response := scenarioMutation(g, _scenarioMessage); response.Code != http.StatusAccepted {
		t.Fatal(response.Code)
	}
	if err := g.sync(t.Context(), _testUID); err != nil {
		t.Fatal(err)
	}
	g.mu.Lock()
	watch := g.watches[_testUID]
	g.mu.Unlock()
	if watch == nil {
		t.Fatal("ready remote Session did not start its stream")
	}
	before := mustRecord(t, store)
	provider := &scenarioHistoryProvider{Provider: p, err: openaiapi.ErrNotFound}
	g.provider = provider
	if err := g.sync(t.Context(), _testUID); !errors.Is(err, openaiapi.ErrNotFound) {
		t.Fatal("console deletion did not report remote absence", err)
	}
	g.mu.Lock()
	watchCount := len(g.watches)
	g.mu.Unlock()
	if watchCount != 0 {
		t.Fatal("console deletion kept an obsolete message stream", watchCount)
	}
	select {
	case <-watch.done:
	default:
		t.Fatal("console deletion returned before its stream reader exited")
	}
	after := mustRecord(t, store)
	if !after.Closed || !after.RecoveryRequired || after.LastError == "" || !reflect.DeepEqual(after.History, before.History) {
		t.Fatalf("console deletion lost history or write fence: %+v", after)
	}
	for _, kind := range []string{_scenarioMessage, _scenarioCancel, _scenarioApproval} {
		if response := scenarioMutation(g, kind); response.Code != http.StatusGone {
			t.Fatal("known missing remote accepts", kind, response.Code)
		}
	}
	if response := request(g, http.MethodGet, _scenarioSessionPath+"/history", _token, "", ""); response.Code != http.StatusOK || !strings.Contains(response.Body.String(), _testRemoteID) {
		t.Fatal("console deletion made retained history unreadable", response.Code, response.Body.String())
	}
	if err := g.sync(t.Context(), _testUID); err != nil || provider.reads != 1 || p.commits != 1 {
		t.Fatal("missing remote was retried or replaced", err, provider.reads, p.commits)
	}
}

func TestScenarioGatewayControllerFirstRemoteDeletionSealsConversation(t *testing.T) {
	store := testStore(t)
	p := simulatorForTest()
	p.history.Actions = []openaiapi.PendingAction{scenarioPendingAction()}
	session := testSession()
	session.Generation = 2
	g, writer := testGateway(t, store, p, session)
	scenarioRecover(t, g, store, p)
	if err := g.sync(t.Context(), _testUID); err != nil {
		t.Fatal(err)
	}
	if response := scenarioMutation(g, _scenarioMessage); response.Code != http.StatusAccepted {
		t.Fatal(response.Code)
	}
	if response := scenarioMutation(g, _scenarioApproval); response.Code != http.StatusAccepted {
		t.Fatal(response.Code)
	}
	before := mustRecord(t, store)
	g.mu.Lock()
	watch := g.watches[_testUID]
	g.mu.Unlock()
	if watch == nil {
		t.Fatal("ready remote Session did not start its stream")
	}
	provider := &scenarioHistoryProvider{Provider: p, err: openaiapi.ErrNotFound}
	g.provider = provider
	session.Status.Conditions = []metav1.Condition{{Type: _readyCondition, Status: metav1.ConditionFalse,
		Reason: _scenarioRemoteMissing, ObservedGeneration: session.Generation}}
	if err := writer.Update(t.Context(), session); err != nil {
		t.Fatal(err)
	}
	if err := g.sync(t.Context(), _testUID); !errors.Is(err, openaiapi.ErrNotFound) {
		t.Fatal("controller-confirmed deletion did not seal the remote identity", err)
	}
	after := mustRecord(t, store)
	if !after.Closed || !after.RecoveryRequired || after.LastError == "" ||
		!reflect.DeepEqual(after.History, before.History) || !reflect.DeepEqual(after.Submissions, before.Submissions) {
		t.Fatalf("controller-first deletion lost terminal fence/history: %+v", after)
	}
	action := after.Actions[actionID(scenarioPendingAction())]
	if action.Active || action.Decision != _approve || len(after.Notes) != 0 || p.posts != 0 {
		t.Fatal("confirmed deletion kept an executable action or lost its prior decision")
	}
	select {
	case <-watch.done:
	default:
		t.Fatal("controller-first deletion did not join its obsolete stream")
	}
	for _, kind := range []string{_scenarioMessage, _scenarioCancel, _scenarioApproval} {
		if response := scenarioMutation(g, kind); response.Code != http.StatusGone {
			t.Fatal("sealed identity accepted mutation", kind, response.Code)
		}
	}
	if response := request(g, http.MethodGet, _scenarioSessionPath+"/history", _token, "", ""); response.Code != http.StatusOK || !strings.Contains(response.Body.String(), _testRemoteID) {
		t.Fatal("controller-first deletion lost historical reads", response.Code)
	}
	if err := g.sync(t.Context(), _testUID); err != nil || provider.reads != 0 {
		t.Fatal("confirmed controller absence required repeated remote reads", err, provider.reads)
	}
}

func TestScenarioGatewayStaleMissingConditionCannotSealDifferentObservation(t *testing.T) {
	tests := []struct {
		name               string
		remoteID           string
		observedGeneration int64
	}{
		{name: "stale generation", remoteID: _testRemoteID, observedGeneration: 1},
		{name: "different remote identity", remoteID: _scenarioForeignRemote, observedGeneration: 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := testStore(t)
			p := simulatorForTest()
			session := testSession()
			session.Generation = 2
			session.Status.OpenAISessionID = tt.remoteID
			session.Status.Conditions = []metav1.Condition{{Type: _readyCondition, Status: metav1.ConditionFalse,
				Reason: _scenarioRemoteMissing, ObservedGeneration: tt.observedGeneration}}
			g, _ := testGateway(t, store, p, session)
			scenarioRecover(t, g, store, p)
			before := mustRecord(t, store)
			if err := g.sync(t.Context(), _testUID); err == nil {
				t.Fatal("unsafe or stale observation allowed delivery")
			}
			after := mustRecord(t, store)
			if after.Closed || !reflect.DeepEqual(before.History, after.History) || p.posts != 0 {
				t.Fatal("stale or unrelated absence destroyed original conversation")
			}
		})
	}
}

func TestScenarioGatewayDeletingCRRetainsSavedHistory(t *testing.T) {
	store := testStore(t)
	p := simulatorForTest()
	session := testSession()
	session.Finalizers = []string{"scenario.example/retain"}
	g, writer := testGateway(t, store, p, session)
	scenarioRecover(t, g, store, p)
	if err := writer.Delete(t.Context(), session); err != nil {
		t.Fatal(err)
	}
	if err := g.sync(t.Context(), _testUID); err != nil {
		t.Fatal(err)
	}
	if response := request(g, http.MethodGet, _scenarioSessionPath+"/history", _token, "", ""); response.Code != http.StatusOK || !strings.Contains(response.Body.String(), _testRemoteID) {
		t.Fatal("deleting Session cannot read saved history", response.Code, response.Body.String())
	}
	if response := scenarioMutation(g, _scenarioMessage); response.Code != http.StatusGone {
		t.Fatal("deleting Session accepted input", response.Code)
	}
}

func TestScenarioGatewayTransientHistoryFailureDoesNotCloseSession(t *testing.T) {
	store := testStore(t)
	p := simulatorForTest()
	g, _ := testGateway(t, store, p, testSession())
	scenarioRecover(t, g, store, p)
	if response := scenarioMutation(g, _scenarioMessage); response.Code != http.StatusAccepted {
		t.Fatal(response.Code)
	}
	provider := &scenarioHistoryProvider{Provider: p, err: io.ErrUnexpectedEOF}
	g.provider = provider
	if err := g.sync(t.Context(), _testUID); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatal(err)
	}
	if mustRecord(t, store).Closed || p.posts != 0 {
		t.Fatal("transient history failure was treated as deletion or delivered unknown ownership")
	}
	provider.fail(nil)
	if err := g.sync(t.Context(), _testUID); err != nil || p.commits != 1 {
		t.Fatal("history retry did not resume input", err, p.commits)
	}
}

func TestScenarioGatewayConnectingAndTemporaryObservationAllowDurableQueue(t *testing.T) {
	for _, reason := range []string{"RemoteSessionCreated", "ObservationFailed", "EnvironmentObservationFailed"} {
		t.Run(reason, func(t *testing.T) {
			store := testStore(t)
			p := simulatorForTest()
			p.history.Actions = []openaiapi.PendingAction{scenarioPendingAction()}
			session := testSession()
			session.Status.Conditions = []metav1.Condition{{Type: _readyCondition,
				Status: metav1.ConditionFalse, Reason: reason}}
			g, _ := testGateway(t, store, p, session)
			scenarioRecover(t, g, store, p)
			for _, kind := range []string{_scenarioMessage, _scenarioCancel, _scenarioApproval} {
				response := scenarioMutation(g, kind)
				if response.Code != http.StatusAccepted {
					t.Fatal("recoverable state rejected durable intent", kind, response.Code)
				}
			}
			if len(mustRecord(t, store).Submissions) != 2 || p.posts != 0 {
				t.Fatal("HTTP acceptance performed remote delivery")
			}
		})
	}
}

func TestScenarioGatewayDefiniteUnsafeObservationPausesQueuedInputAndApprovedAction(t *testing.T) {
	for _, reason := range []string{_scenarioRemoteMissing, _scenarioOwnershipMismatch, _scenarioConfigurationMismatch} {
		t.Run(reason, func(t *testing.T) {
			store := testStore(t)
			p := simulatorForTest()
			p.history.Actions = []openaiapi.PendingAction{scenarioPendingAction()}
			session := testSession()
			g, writer := testGateway(t, store, p, session)
			scenarioRecover(t, g, store, p)
			if response := scenarioMutation(g, _scenarioApproval); response.Code != http.StatusAccepted {
				t.Fatal(response.Code)
			}
			if response := scenarioMutation(g, _scenarioMessage); response.Code != http.StatusAccepted {
				t.Fatal(response.Code)
			}
			session.Status.Conditions = []metav1.Condition{{Type: _readyCondition,
				Status: metav1.ConditionFalse, Reason: reason}}
			if err := writer.Update(t.Context(), session); err != nil {
				t.Fatal(err)
			}
			wantErr := errUnavailable
			if reason == _scenarioRemoteMissing {
				wantErr = openaiapi.ErrNotFound
			}
			if err := g.sync(t.Context(), _testUID); !errors.Is(err, wantErr) {
				t.Fatal("worker ignored unsafe observation", err)
			}
			r := mustRecord(t, store)
			if p.posts != 0 || len(r.Notes) != 0 || r.Submissions[0].Attempts != 0 ||
				r.Actions[actionID(scenarioPendingAction())].Decision != _approve {
				t.Fatal("unsafe observation delivered input, ran approved write, or lost approval")
			}
		})
	}
}

func TestScenarioGatewaySpendVerificationPausesTurnsAndAllowsTargetCancellation(t *testing.T) {
	tests := []struct {
		name   string
		mode   string
		reason string
		digest string
	}{
		{name: "stale limited readiness", mode: _scenarioLimited, reason: "Observed"},
		{name: "unconfirmed unlimited update", mode: _testWorkUnlimited,
			reason: "SpendControlOutcomeUnknown", digest: "claimed-update"},
		{name: "unverified unlimited readback", mode: _testWorkUnlimited, reason: "SpendControlUnverified"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := testStore(t)
			p := simulatorForTest()
			p.history.Actions = []openaiapi.PendingAction{scenarioPendingAction()}
			p.history.Turns = []json.RawMessage{json.RawMessage(`{"id":"turn-1","status":"in_progress","subagent_id":null}`)}
			session := testSession()
			session.Generation = 2
			limit := int64(100)
			session.Spec.SpendControl = &agentsv1alpha1.SessionSpendControl{Mode: tt.mode}
			if tt.mode == _scenarioLimited {
				session.Spec.SpendControl.LimitCents = &limit
			}
			session.Status.SpendControlAttemptedDigest = tt.digest
			session.Status.Conditions = []metav1.Condition{{Type: _readyCondition,
				Status: metav1.ConditionFalse, Reason: tt.reason, ObservedGeneration: 1}}
			g, _ := testGateway(t, store, p, session)
			scenarioRecover(t, g, store, p)
			if response := scenarioMutation(g, _scenarioMessage); response.Code != http.StatusServiceUnavailable {
				t.Fatal("new turn accepted before spend readback", response.Code)
			}
			if response := scenarioMutation(g, _scenarioCancel); response.Code != http.StatusAccepted {
				t.Fatal("target cancellation blocked by pending spending choice", response.Code)
			}
			if response := scenarioMutation(g, _scenarioApproval); response.Code != http.StatusAccepted {
				t.Fatal("durable approval decision unavailable", response.Code)
			}
			if err := g.enqueue(t.Context(), session, "previously-queued", "Wait for confirmed spend policy", "", nil); err != nil {
				t.Fatal(err)
			}
			if err := g.sync(t.Context(), _testUID); err != nil {
				t.Fatal(err)
			}
			r := mustRecord(t, store)
			if p.posts != 1 || !r.Submissions[0].Accepted || r.Submissions[1].Attempts != 0 || len(r.Notes) != 0 {
				t.Fatal("spend hold ran an approved action/new turn or blocked targeted cancel")
			}
		})
	}
}

func TestScenarioGatewayInFlightObservationChangeFencesActionsAndDelivery(t *testing.T) {
	tests := []struct {
		name      string
		changeAt  int
		hasAction bool
		change    func(*agentsv1alpha1.Session)
	}{
		{name: "unsafe configuration before approved action", changeAt: 2, hasAction: true, change: func(s *agentsv1alpha1.Session) {
			s.Status.Conditions = []metav1.Condition{{Type: _readyCondition,
				Status: metav1.ConditionFalse, Reason: _scenarioConfigurationMismatch}}
		}},
		{name: "remote identity before approved action", changeAt: 2, hasAction: true, change: func(s *agentsv1alpha1.Session) {
			s.Status.OpenAISessionID = _scenarioForeignRemote
		}},
		{name: "unsafe configuration at function execution", changeAt: 3, hasAction: true, change: func(s *agentsv1alpha1.Session) {
			s.Status.Conditions = []metav1.Condition{{Type: _readyCondition,
				Status: metav1.ConditionFalse, Reason: _scenarioOwnershipMismatch}}
		}},
		{name: "remote identity at function execution", changeAt: 3, hasAction: true, change: func(s *agentsv1alpha1.Session) {
			s.Status.OpenAISessionID = _scenarioForeignRemote
		}},
		{name: "remote identity immediately before send", changeAt: 3, change: func(s *agentsv1alpha1.Session) {
			s.Status.OpenAISessionID = _scenarioForeignRemote
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := testStore(t)
			p := simulatorForTest()
			if tt.hasAction {
				p.history.Actions = []openaiapi.PendingAction{scenarioPendingAction()}
			}
			g, reader := testGateway(t, store, p, testSession())
			scenarioRecover(t, g, store, p)
			if tt.hasAction {
				if response := scenarioMutation(g, _scenarioApproval); response.Code != http.StatusAccepted {
					t.Fatal(response.Code)
				}
			}
			if response := scenarioMutation(g, _scenarioMessage); response.Code != http.StatusAccepted {
				t.Fatal(response.Code)
			}
			g.reader = &scenarioChangingReader{Reader: reader, changeAt: tt.changeAt, change: tt.change}
			if err := g.sync(t.Context(), _testUID); err == nil {
				t.Fatal("in-flight observation change was ignored")
			}
			if p.posts != 0 || len(mustRecord(t, store).Notes) != 0 || mustRecord(t, store).Submissions[0].Attempts != 0 {
				t.Fatal("stale observation delivered input or ran approved action")
			}
		})
	}
}

func TestScenarioGatewayRejectsRemoteHistoryIdentityMismatch(t *testing.T) {
	for _, field := range []string{"id", "kubernetes_uid", _scenarioOperator, "owner_scope"} {
		t.Run(field, func(t *testing.T) {
			store := testStore(t)
			p := simulatorForTest()
			p.history.Metadata["owner_scope"] = _scenarioOwnerScope
			session := testSession()
			session.Status.OwnerScope = _scenarioOwnerScope
			g, _ := testGateway(t, store, p, session)
			scenarioRecover(t, g, store, p)
			if response := scenarioMutation(g, _scenarioMessage); response.Code != http.StatusAccepted {
				t.Fatal(response.Code)
			}
			if field == "id" {
				p.history.ID = "foreign-session"
			} else {
				p.history.Metadata[field] = "different-owner"
			}
			before := mustRecord(t, store)
			if err := g.sync(t.Context(), _testUID); err == nil || p.posts != 0 {
				t.Fatal("history identity mismatch permitted delivery", err, p.posts)
			}
			after := mustRecord(t, store)
			if !reflect.DeepEqual(before.History, after.History) || !reflect.DeepEqual(before.Submissions, after.Submissions) {
				t.Fatal("foreign history replaced trusted saved state")
			}
		})
	}
}

func TestScenarioGatewayTargetCancellationRechecksCompleteRemoteOwnership(t *testing.T) {
	for _, field := range []string{_scenarioOperator, "owner_scope"} {
		t.Run(field, func(t *testing.T) {
			store := testStore(t)
			p := simulatorForTest()
			p.history.Metadata["owner_scope"] = _scenarioOwnerScope
			p.history.Turns = []json.RawMessage{json.RawMessage(`{"id":"turn-1","status":"in_progress","subagent_id":null}`)}
			session := testSession()
			session.Status.OwnerScope = _scenarioOwnerScope
			g, _ := testGateway(t, store, p, session)
			scenarioRecover(t, g, store, p)
			if response := scenarioMutation(g, _scenarioCancel); response.Code != http.StatusAccepted {
				t.Fatal(response.Code)
			}
			g.provider = &scenarioHistoryProvider{Provider: p, alterRead: 2,
				alterResult: func(history *openaiapi.History) { history.Metadata[field] = "different-owner" }}
			if err := g.sync(t.Context(), _testUID); err == nil || p.posts != 0 {
				t.Fatal("targeted cancellation ignored fresh ownership drift", field, err, p.posts)
			}
		})
	}
}

func TestScenarioGatewayWorkHistorySurvivesMissingOrRecreatedCR(t *testing.T) {
	for _, recreate := range []bool{false, true} {
		name := "missing CR"
		if recreate {
			name = "different UID at same name"
		}
		t.Run(name, func(t *testing.T) {
			store := testStore(t)
			g, writer, id := workerWorkGateway(t, store, "selfHosted")
			if err := g.syncWork(t.Context(), id); err != nil {
				t.Fatal(err)
			}
			record := workerWorkRecord(t, store, id)
			session := workerWorkSession(t, writer, record)
			uid := string(session.UID)
			if err := store.Update(t.Context(), uid, func(r *Record) error {
				r.History.ID = "retained-history"
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if err := writer.Client.Delete(t.Context(), session); err != nil {
				t.Fatal(err)
			}
			if recreate {
				session.UID = "foreign-uid"
				session.ResourceVersion = ""
				if err := writer.Client.Create(t.Context(), session); err != nil {
					t.Fatal(err)
				}
			}
			before := workerWorkRecord(t, store, id)
			response := request(g, http.MethodGet, "/v1/namespaces/team/work/"+id, _token, "", "")
			if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), "retained-history") ||
				strings.Contains(response.Body.String(), `"session_url"`) {
				t.Fatalf("old history lost or foreign CR advertised: %d %s", response.Code, response.Body.String())
			}
			if after := workerWorkRecord(t, store, id); !reflect.DeepEqual(before, after) || writer.creates != 1 {
				t.Fatal("reading history changed work or recreated its Session")
			}
			response = request(g, http.MethodGet, "/v1/namespaces/foreign/work/"+id, _token, "", "")
			if response.Code != http.StatusNotFound || strings.Contains(response.Body.String(), "retained-history") {
				t.Fatal("work history leaked across namespaces", response.Code)
			}
		})
	}
}

func TestScenarioGatewayWorkCloseReplayAndRestartSurvivePostgreSQLReopen(t *testing.T) {
	store, dsn := postgresForTest(t, 32)
	g, writer, _ := workerWorkGateway(t, store, "selfHosted")
	// Use HTTP acceptance to exercise the route-scoped receipt index as well as
	// the worker. The fixture's independent work record remains untouched.
	input := WorkInput{RouteID: workerWorkRoute().ID, WorkID: "scenario-durable-work",
		EventID: "scenario-task", Kind: _workTask, Text: "Investigate durable work"}
	body := string(mustJSON(t, input))
	first := request(g, http.MethodPost, _workEventPath, _token, "", body)
	if first.Code != http.StatusAccepted {
		t.Fatal(first.Code, first.Body.String())
	}
	id := workIdentity(_testNamespace, input.RouteID, workerWorkRoute().Source, input.WorkID)
	writer.workID = id
	if err := g.syncWork(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	old := workerWorkSession(t, writer, workerWorkRecord(t, store, id))
	input.EventID, input.Kind, input.Text = "scenario-close", _workClose, ""
	closeBody := string(mustJSON(t, input))
	closed := request(g, http.MethodPost, _workEventPath, _token, "", closeBody)
	if closed.Code != http.StatusAccepted {
		t.Fatal(closed.Code, closed.Body.String())
	}
	for range 2 {
		if err := g.syncWork(t.Context(), id); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenPostgreSQL(t.Context(), PostgreSQLConfig{DSN: dsn, AllowInsecureLocal: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	g.store = reopened
	writer.store = reopened
	for _, expected := range []struct {
		body    string
		receipt string
	}{
		{body: body, receipt: first.Body.String()},
		{body: closeBody, receipt: closed.Body.String()},
	} {
		response := request(g, http.MethodPost, _workEventPath, _token, "", expected.body)
		if response.Code != http.StatusAccepted || response.Body.String() != expected.receipt {
			t.Fatal("reopen lost original accepted receipt", response.Code, response.Body.String())
		}
	}
	input.EventID, input.Kind, input.Text = "late-task", _workTask, "Late duplicate work"
	if response := request(g, http.MethodPost, _workEventPath, _token, "", string(mustJSON(t, input))); response.Code != http.StatusConflict {
		t.Fatal("tombstone did not reject new task", response.Code)
	}
	input.EventID, input.Kind, input.Text = "explicit-restart", _workRestart, "Investigate again"
	if response := request(g, http.MethodPost, _workEventPath, _token, "", string(mustJSON(t, input))); response.Code != http.StatusAccepted {
		t.Fatal("explicit restart rejected", response.Code, response.Body.String())
	}
	if err := g.syncWork(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	current := workerWorkRecord(t, reopened, id)
	if writer.creates != 2 || len(current.Work.Attempts) != 2 || current.Work.Attempts[1].SessionUID == string(old.UID) {
		t.Fatal("restart duplicated or adopted prior attempt")
	}
	if err := g.workSessionOpen(t.Context(), old); err == nil {
		t.Fatal("prior attempt accepted direct input after reopen and restart")
	}
	var remaining agentsv1alpha1.SessionList
	if err := writer.List(t.Context(), &remaining, client.InNamespace(_testNamespace)); err != nil || len(remaining.Items) != 1 {
		t.Fatal("close/restart left an unexpected Session", err, len(remaining.Items))
	}
}
