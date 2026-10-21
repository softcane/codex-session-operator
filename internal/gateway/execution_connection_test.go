package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"

	"github.com/softcane/codex-session-operator/internal/openaiapi"
)

const _testDisconnectedStatus = "disconnected"

type executionConnectionFixture struct {
	*simulator
	state       openaiapi.HistoryEnvironment
	unavailable bool
	onRead      func()
}

func (p *executionConnectionFixture) ConnectionState(context.Context, string) (openaiapi.HistoryEnvironment, error) {
	state := p.state
	if p.onRead != nil {
		hook := p.onRead
		p.onRead = nil
		hook()
	}
	return state, nil
}

func (p *executionConnectionFixture) OpenMessageStream(ctx context.Context, id string) (openaiapi.MessageStream, error) {
	if p.unavailable {
		return nil, errors.New("stream unavailable")
	}
	return p.simulator.OpenMessageStream(ctx, id)
}

func TestSelfHostedLiveConnectionReadAndStreamLossRecovery(t *testing.T) {
	session, pod := executionSessionAndPod(t)
	store := testStore(t)
	provider := &executionConnectionFixture{simulator: simulatorForTest(),
		state: openaiapi.HistoryEnvironment{ID: session.Status.EnvironmentID, Type: _selfHostedProviderType, Status: _connectedStatus}}
	g, _ := testGateway(t, store, provider.simulator, executionObjects(t, session, pod)...)
	g.provider = provider
	if response := request(g, http.MethodPost, _testMessagePath, "live-first", `{"text":"First"}`); response.Code != http.StatusAccepted {
		t.Fatal(response.Code)
	}
	// No connected event is replayed. A current runtime GET plus the active new
	// subscription qualifies an already-connected executor after gateway restart.
	if err := g.sync(t.Context(), _testUID); err != nil || provider.posts != 1 {
		t.Fatal("current runtime read did not qualify initial subscription", err)
	}
	g.stopWatch(_testUID)
	provider.unavailable = true
	if response := request(g, http.MethodPost, _testMessagePath, "live-second", `{"text":"Second"}`); response.Code != http.StatusAccepted {
		t.Fatal(response.Code)
	}
	if err := g.sync(t.Context(), _testUID); err != nil || provider.posts != 1 {
		t.Fatal("stale stored connection proof survived outgoing stream loss", err)
	}
	provider.unavailable = false
	if err := g.sync(t.Context(), _testUID); err != nil || provider.posts != 2 {
		t.Fatal("resubscription and current runtime read did not recover queued input", err)
	}
}

func TestSelfHostedLiveConnectionRejectsMissingForeignAndNegativeState(t *testing.T) {
	for _, tt := range []struct {
		name  string
		state openaiapi.HistoryEnvironment
	}{
		{name: "missing state"},
		{name: "foreign identity", state: openaiapi.HistoryEnvironment{ID: "foreign-environment", Type: _selfHostedProviderType, Status: _connectedStatus}},
		{name: "hosted type", state: openaiapi.HistoryEnvironment{ID: _executionTestEnvironmentID, Type: "openai_hosted", Status: _connectedStatus}},
		{name: _testDisconnectedStatus, state: openaiapi.HistoryEnvironment{ID: _executionTestEnvironmentID, Type: _selfHostedProviderType, Status: _testDisconnectedStatus}},
		{name: "pending", state: openaiapi.HistoryEnvironment{ID: _executionTestEnvironmentID, Type: _selfHostedProviderType, Status: "pending"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			session, pod := executionSessionAndPod(t)
			store := testStore(t)
			provider := &executionConnectionFixture{simulator: simulatorForTest(), state: tt.state}
			g, _ := testGateway(t, store, provider.simulator, executionObjects(t, session, pod)...)
			g.provider = provider
			if response := request(g, http.MethodPost, _testMessagePath, "live-held", `{"text":"Held"}`); response.Code != http.StatusAccepted {
				t.Fatal(response.Code)
			}
			if err := g.sync(t.Context(), _testUID); err != nil || provider.posts != 0 || mustRecord(t, store).Submissions[0].Attempts != 0 {
				t.Fatal("unverified live environment authorized ordinary input", err)
			}
		})
	}
}

func TestSelfHostedLiveNegativeEventWinsOverInFlightRead(t *testing.T) {
	session, pod := executionSessionAndPod(t)
	store := testStore(t)
	provider := &executionConnectionFixture{simulator: simulatorForTest(),
		state: openaiapi.HistoryEnvironment{ID: session.Status.EnvironmentID, Type: _selfHostedProviderType, Status: _connectedStatus}}
	g, _ := testGateway(t, store, provider.simulator, executionObjects(t, session, pod)...)
	g.provider = provider
	if response := request(g, http.MethodPost, _testMessagePath, "inflight-live", `{"text":"Held during disconnect"}`); response.Code != http.StatusAccepted {
		t.Fatal(response.Code)
	}
	provider.onRead = func() {
		g.mu.Lock()
		w := g.watches[_testUID]
		g.mu.Unlock()
		g.observeExecutionEvent(w, json.RawMessage(`{"type":"agent.session.environment.disconnected","environment":{"id":"env-execution","type":"self_hosted","status":"disconnected"}}`))
		provider.state.Status = _testDisconnectedStatus
	}
	if err := g.sync(t.Context(), _testUID); err != nil || provider.posts != 0 {
		t.Fatal("older positive GET overrode an observed disconnect", err)
	}
	if err := g.sync(t.Context(), _testUID); err != nil || provider.posts != 0 {
		t.Fatal("negative live observation did not remain a veto", err)
	}
	g.mu.Lock()
	w := g.watches[_testUID]
	g.mu.Unlock()
	g.observeExecutionEvent(w, json.RawMessage(`{"type":"agent.session.environment.connected","environment":{"id":"env-execution","type":"self_hosted","status":"connected"}}`))
	provider.state.Status = _connectedStatus
	if err := g.sync(t.Context(), _testUID); err != nil || provider.posts != 1 {
		t.Fatal("fresh connected transition and GET did not release input", err)
	}
}

func TestSelfHostedFreshRuntimeReadRecoversReorderedPendingSnapshot(t *testing.T) {
	session, pod := executionSessionAndPod(t)
	store := testStore(t)
	provider := &executionConnectionFixture{simulator: simulatorForTest(),
		state: openaiapi.HistoryEnvironment{ID: session.Status.EnvironmentID, Type: _selfHostedProviderType, Status: _connectedStatus}}
	g, _ := testGateway(t, store, provider.simulator, executionObjects(t, session, pod)...)
	g.provider = provider
	if response := request(g, http.MethodPost, _testMessagePath, "ordered-first", `{"text":"First"}`); response.Code != http.StatusAccepted {
		t.Fatal(response.Code)
	}
	if err := g.sync(t.Context(), _testUID); err != nil || provider.posts != 1 {
		t.Fatal(err)
	}
	g.mu.Lock()
	w := g.watches[_testUID]
	g.mu.Unlock()
	g.observeExecutionEvent(w, json.RawMessage(`{"type":"agent.session.environment.connected","environment":{"id":"env-execution","type":"self_hosted","status":"connected"}}`))
	g.observeExecutionEvent(w, json.RawMessage(`{"type":"agent.session.environment.pending","environment":{"id":"env-execution","type":"self_hosted","status":"pending"}}`))
	if g.executionWatchAvailable(session) {
		t.Fatal("late negative snapshot did not immediately revoke eligibility")
	}
	if response := request(g, http.MethodPost, _testMessagePath, "ordered-second", `{"text":"Second"}`); response.Code != http.StatusAccepted {
		t.Fatal(response.Code)
	}
	// The runtime has already connected. No additional connected SSE event is
	// replayed; a later authoritative read with an unchanged revision recovers.
	if err := g.sync(t.Context(), _testUID); err != nil || provider.posts != 2 || !g.executionWatchAvailable(session) {
		t.Fatal("older pending snapshot permanently latched the active stream", err)
	}
}

func TestSelfHostedConnectionLossBeforeApprovedWritePreventsExecution(t *testing.T) {
	session, pod := executionSessionAndPod(t)
	store := testStore(t)
	provider := &executionConnectionFixture{simulator: simulatorForTest(),
		state: openaiapi.HistoryEnvironment{ID: session.Status.EnvironmentID, Type: _selfHostedProviderType, Status: _connectedStatus}}
	provider.history.Actions = []openaiapi.PendingAction{scenarioPendingAction()}
	g, _ := testGateway(t, store, provider.simulator, executionObjects(t, session, pod)...)
	g.provider = provider
	if _, _, err := g.lookup(t.Context(), _testNamespace, _testSessionName); err != nil {
		t.Fatal(err)
	}
	if err := g.sync(t.Context(), _testUID); err != nil {
		t.Fatal(err)
	}
	if err := g.decide(t.Context(), _testUID, actionID(scenarioPendingAction()), _approve, _testApprover); err != nil {
		t.Fatal(err)
	}
	provider.onRead = func() { g.stopWatch(_testUID) }
	if err := g.sync(t.Context(), _testUID); err != nil || len(mustRecord(t, store).Notes) != 0 || provider.posts != 0 {
		t.Fatal("stream loss authorized an approved write", err)
	}
}

type executionClaimFaultStore struct {
	*memoryStore
	afterClaim func()
	action     bool
}

func (s *executionClaimFaultStore) Update(ctx context.Context, uid string, change func(*Record) error) error {
	if err := s.memoryStore.Update(ctx, uid, change); err != nil {
		return err
	}
	if s.afterClaim == nil {
		return nil
	}
	record, err := s.Get(ctx, uid)
	if err != nil {
		return err
	}
	claimed := false
	if s.action {
		for _, action := range record.Actions {
			claimed = claimed || action.OperationID != ""
		}
	} else {
		for _, submission := range record.Submissions {
			claimed = claimed || submission.Attempts > 0
		}
	}
	if claimed {
		// Inject only after the transaction/callback ends. Storage callbacks never
		// perform stream, provider or Kubernetes work.
		hook := s.afterClaim
		s.afterClaim = nil
		hook()
	}
	return nil
}

func TestSelfHostedStreamLossAfterDeliveryClaimPreventsProviderPost(t *testing.T) {
	session, pod := executionSessionAndPod(t)
	store := &executionClaimFaultStore{memoryStore: testStore(t)}
	provider := simulatorForTest()
	g, _ := testGateway(t, store, provider, executionObjects(t, session, pod)...)
	if response := request(g, http.MethodPost, _testMessagePath, "claim-loss", `{"text":"Preserve the keyed input"}`); response.Code != http.StatusAccepted {
		t.Fatal(response.Code)
	}
	store.afterClaim = func() { g.stopWatch(_testUID) }
	if err := g.sync(t.Context(), _testUID); err != nil || provider.posts != 0 {
		t.Fatal("stream loss during claim still performed provider I/O", err)
	}
	record := mustRecord(t, store)
	if record.Submissions[0].Attempts != 1 || record.Submissions[0].Accepted {
		t.Fatal("claim loss changed durable acceptance or lost the send claim")
	}
	if err := g.sync(t.Context(), _testUID); err != nil || provider.commits != 1 ||
		mustRecord(t, store).Submissions[0].RemoteKey != wireKey(_testUID, "claim-loss") {
		t.Fatal("recovery failed to reuse the original pending submission identity", err)
	}
}

func TestSelfHostedStreamLossAfterFunctionClaimPreventsExternalSideEffect(t *testing.T) {
	session, pod := executionSessionAndPod(t)
	store := &executionClaimFaultStore{memoryStore: testStore(t), action: true}
	provider := simulatorForTest()
	pending := scenarioPendingAction()
	pending.Name = "external-write"
	provider.history.Actions = []openaiapi.PendingAction{pending}
	g, _ := testGateway(t, store, provider, executionObjects(t, session, pod)...)
	calls := 0
	g.functions[pending.Name] = Function{Consequential: true, DownstreamDeduplicates: true,
		Execute: func(context.Context, Operation) (string, error) { calls++; return "Written", nil }}
	if _, _, err := g.lookup(t.Context(), _testNamespace, _testSessionName); err != nil {
		t.Fatal(err)
	}
	if err := g.sync(t.Context(), _testUID); err != nil {
		t.Fatal(err)
	}
	if err := g.decide(t.Context(), _testUID, actionID(pending), _approve, _testApprover); err != nil {
		t.Fatal(err)
	}
	store.afterClaim = func() { g.stopWatch(_testUID) }
	if err := g.sync(t.Context(), _testUID); err != nil || calls != 0 || provider.posts != 0 {
		t.Fatal("stream loss during operation claim authorized an external write", err)
	}
	claimed := mustRecord(t, store).Actions[actionID(pending)]
	if claimed.OperationID == "" || claimed.AttemptedAt == nil || len(claimed.Result) != 0 {
		t.Fatal("held side effect lost its original operation claim")
	}
	if err := g.sync(t.Context(), _testUID); err != nil || calls != 1 || provider.posts != 1 ||
		mustRecord(t, store).Actions[actionID(pending)].OperationID != claimed.OperationID {
		t.Fatal("live recovery did not preserve the original external operation identity", err)
	}
}
