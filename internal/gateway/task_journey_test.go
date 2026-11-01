package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const _taskTestDiagnose = "diagnose"
const _taskTestWorkflow = "journey"
const _taskTestWorker = "worker"

func workflowJourneyGateway(t *testing.T, store Storage) (*Gateway, *sessionWriter) {
	t.Helper()
	g, writer, _ := profileConversationGateway(t, store)
	template := &agentsv1alpha1.WorkflowTemplate{ObjectMeta: metav1.ObjectMeta{Name: _taskTestWorkflow, Namespace: _testNamespace, UID: "workflow-journey", Generation: 1}, Spec: agentsv1alpha1.WorkflowTemplateSpec{Coordinator: agentsv1alpha1.WorkflowCoordinator{ProfileRef: agentsv1alpha1.AgentReference{Name: _profileTestName}}, Roles: []agentsv1alpha1.WorkflowRole{{Name: _taskTestDiagnose, ProfileRef: agentsv1alpha1.AgentReference{Name: _profileTestName}, Outputs: []string{"diagnosis"}}, {Name: _taskCoordinator, ProfileRef: agentsv1alpha1.AgentReference{Name: _profileTestName}, Outputs: []string{"finding"}}}}}
	if err := writer.Client.Create(t.Context(), template); err != nil {
		t.Fatal(err)
	}
	writer.createRace = func(object client.Object) error {
		object.SetUID(types.UID("session-" + object.GetName()))
		return writer.Client.Create(t.Context(), object)
	}
	return g, writer
}

func TestTaskJourneyAcceptanceDelegationResultAndStop(t *testing.T) {
	checkTaskJourney(t, testStore(t))
}
func TestPostgreSQLTaskJourneyAcceptanceDelegationResultAndStop(t *testing.T) {
	store, _ := postgresForTest(t)
	checkTaskJourney(t, store)
}
func checkTaskJourney(t *testing.T, store Storage) {
	t.Helper()
	ctx := t.Context()
	g, writer := workflowJourneyGateway(t, store)
	scope := requestScope{Namespace: _testNamespace, Subject: _testConversationSubject}
	record, event, err := g.createTask(ctx, scope, "start", TaskInput{Workflow: _taskTestWorkflow, Text: "Find the bug"})
	if err != nil {
		t.Fatal(err)
	}
	replay, replayEvent, err := g.createTask(ctx, scope, "start", TaskInput{Workflow: _taskTestWorkflow, Text: "Find the bug"})
	if err != nil || replay.UID != record.UID || event.ID != replayEvent.ID {
		t.Fatalf("replay: %v", err)
	}
	if _, _, err := g.createTask(ctx, scope, "start", TaskInput{Workflow: _taskTestWorkflow, Text: "changed task input"}); !errors.Is(err, errConflict) {
		t.Fatalf("changed replay: %v", err)
	}
	if writer.creates != 0 {
		t.Fatalf("acceptance created execution: %d", writer.creates)
	}
	if err := g.syncTask(ctx, record.UID); err != nil {
		t.Fatal(err)
	}
	coordinator := record.Task.Members[0]
	child := readWorkRecord(t, store, coordinator.WorkID)
	if child.TaskMember == nil || len(child.Work.Attempts) != 1 || child.Work.Attempts[0].SessionUID == "" {
		t.Fatalf("coordinator not provisioned: %+v", child)
	}
	coordinatorUID := child.Work.Attempts[0].SessionUID
	action := WorkflowAction{SessionUID: coordinatorUID, TurnID: "turn-root", CallID: "delegate-one"}
	raw := json.RawMessage(`{"role":"diagnose","text":"Inspect the failing path"}`)
	if _, err := g.delegateTask(ctx, record, action, raw); err != nil {
		t.Fatal(err)
	}
	current := readWorkRecord(t, store, record.UID)
	if _, err := g.delegateTask(ctx, current, action, raw); err != nil {
		t.Fatal(err)
	}
	current = readWorkRecord(t, store, record.UID)
	if len(current.Task.Members) != 2 {
		t.Fatal("replay duplicated assignment")
	}
	member := current.Task.Members[1]
	if err := g.syncTask(ctx, record.UID); err != nil {
		t.Fatal(err)
	}
	worker := readWorkRecord(t, store, member.WorkID)
	if worker.Work.Attempts[0].SessionUID == coordinatorUID || worker.Name == child.Name {
		t.Fatal("members share Session")
	}
	invalid := json.RawMessage(`{"outcome":"success","outputs":[]}`)
	resultAction := WorkflowAction{SessionUID: worker.Work.Attempts[0].SessionUID, TurnID: "worker-turn", CallID: "result-invalid"}
	if _, err := g.saveTaskResult(ctx, current, member, Record{}, resultAction, invalid); !errors.Is(err, errWorkflowInput) {
		t.Fatalf("missing output accepted: %v", err)
	}
	current = readWorkRecord(t, store, record.UID)
	if len(current.Task.Results) != 0 || len(current.Task.Rejections) != 1 {
		t.Fatal("invalid result finalized or rejection missing")
	}
	resultAction.CallID = "result-valid"
	valid := json.RawMessage("{\n \"outcome\":\"success\",\"outputs\":[{\"name\":\"diagnosis\",\"content\":\"The guard is missing\"}]\n}")
	ack, err := g.saveTaskResult(ctx, current, member, Record{}, resultAction, valid)
	if err != nil {
		t.Fatal(err)
	}
	if len(ack) > 1024 {
		t.Fatal("result acknowledgement contains report")
	}
	current = readWorkRecord(t, store, record.UID)
	if current.Task.Results[0].Raw != string(valid) {
		t.Fatal("saved bytes changed")
	}
	if err := g.queueTaskEvents(ctx, current); err != nil {
		t.Fatal(err)
	}
	current = readWorkRecord(t, store, record.UID)
	if err := g.queueTaskEvents(ctx, current); err != nil {
		t.Fatal(err)
	}
	child = readWorkRecord(t, store, coordinator.WorkID)
	if len(child.Work.Events) != 2 {
		t.Fatal("handoff duplicated")
	}
	if _, err := g.saveTaskResult(ctx, current, member, Record{}, resultAction, valid); err != nil {
		t.Fatal(err)
	}
	checkTaskWorkerAuthority(t, g, store, current, action, raw)
	checkTaskStop(t, g, writer, store, scope, record.UID)
}

func checkTaskWorkerAuthority(t *testing.T, g *Gateway, store Storage, current Record, action WorkflowAction, raw json.RawMessage) {
	t.Helper()
	ctx := t.Context()
	record := current
	// A configured worker role named coordinator never acquires coordinator authority.
	action.CallID = "named-coordinator"
	if _, err := g.delegateTask(ctx, current, action, json.RawMessage(`{"role":"coordinator","text":"Review"}`)); err != nil {
		t.Fatal(err)
	}
	current = readWorkRecord(t, store, record.UID)
	fakeCoordinator := current.Task.Members[2]
	if fakeCoordinator.IsCoordinator {
		t.Fatal("role label became authority")
	}
	if err := g.syncTask(ctx, record.UID); err != nil {
		t.Fatal(err)
	}
	impostor := readWorkRecord(t, store, fakeCoordinator.WorkID)
	uid := impostor.Work.Attempts[0].SessionUID
	if err := store.Update(ctx, uid, func(r *Record) error {
		r.Namespace = _testNamespace
		r.TaskMember = &TaskMemberBinding{TaskID: record.UID, MemberID: fakeCoordinator.ID, Role: fakeCoordinator.Role}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := g.workflowTool(ctx, WorkflowAction{SessionUID: uid, TurnID: "impostor-turn", CallID: "impostor-call"}, "workflow_delegate", raw); !errors.Is(err, errWorkflowInput) {
		t.Fatalf("worker coordinator delegated: %v", err)
	}

}

func checkTaskStop(t *testing.T, g *Gateway, writer *sessionWriter, store Storage, scope requestScope, id string) {
	t.Helper()
	ctx := t.Context()
	record := Record{UID: id}
	if _, _, err := g.acceptTaskMutation(ctx, scope, record.UID, _taskStop, "stop", ""); err != nil {
		t.Fatal(err)
	}
	if err := g.syncTask(ctx, record.UID); err != nil {
		t.Fatal(err)
	}
	for _, intent := range readWorkRecord(t, store, record.UID).Task.Members {
		saved := readWorkRecord(t, store, intent.WorkID)
		if saved.Work.State == _workOpen {
			t.Fatal("stop left member open")
		}
		var session agentsv1alpha1.Session
		err := writer.Get(ctx, client.ObjectKey{Namespace: _testNamespace, Name: saved.Name}, &session)
		if err == nil && session.DeletionTimestamp.IsZero() {
			t.Fatal("stop did not request member deletion")
		}
	}
	if _, _, err := g.acceptTaskMutation(ctx, scope, record.UID, _taskMessage, "late", "more"); !errors.Is(err, errClosed) {
		t.Fatalf("stopped task accepted input: %v", err)
	}
}

func TestTaskHTTPStartAndStopAtEventCapacity(t *testing.T) {
	store := testStore(t)
	g, _ := workflowJourneyGateway(t, store)
	response := request(g, http.MethodPost, "/v1/namespaces/team/tasks", "http-start", `{"workflow":"journey","text":"Investigate"}`)
	if response.Code != http.StatusAccepted {
		t.Fatalf("start %d: %s", response.Code, response.Body)
	}
	var receipt TaskReceipt
	if err := json.Unmarshal(response.Body.Bytes(), &receipt); err != nil {
		t.Fatal(err)
	}
	if err := store.Update(context.Background(), receipt.TaskID, func(r *Record) error {
		for len(r.Task.Events) < _maxSubmissions {
			r.Task.Events = append(r.Task.Events, TaskEvent{ID: taskDigest(len(r.Task.Events)), Kind: _taskMessage, Digest: taskDigest([]string{_taskMessage, ""}), ReceivedAt: r.Task.Events[0].ReceivedAt})
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	response = request(g, http.MethodPost, receipt.TaskURL+"/stop", "http-stop", `{}`)
	if response.Code != http.StatusAccepted {
		t.Fatalf("stop full task %d: %s", response.Code, response.Body)
	}
}

func TestTaskMemberPolicyFrozenBeforeProfileEdit(t *testing.T) {
	store := testStore(t)
	g, writer := workflowJourneyGateway(t, store)
	ctx := t.Context()
	scope := requestScope{Namespace: _testNamespace, Subject: _testConversationSubject}
	record, _, err := g.createTask(ctx, scope, "freeze", TaskInput{Workflow: _taskTestWorkflow, Text: "Investigate"})
	if err != nil {
		t.Fatal(err)
	}
	var profile agentsv1alpha1.Profile
	if err := writer.Get(ctx, client.ObjectKey{Namespace: _testNamespace, Name: _profileTestName}, &profile); err != nil {
		t.Fatal(err)
	}
	limit := int64(300)
	profile.Spec.SpendControl = agentsv1alpha1.SessionSpendControl{Mode: agentsv1alpha1.SessionSpendModeLimited, LimitCents: &limit}
	profile.Status.PolicyDigest = agentsv1alpha1.ProfilePolicyDigest(profile.Spec)
	if err := writer.Update(ctx, &profile); err != nil {
		t.Fatal(err)
	}
	action := WorkflowAction{SessionUID: "root-uid", TurnID: "root-turn", CallID: "new-profile"}
	if _, err := g.delegateTask(ctx, record, action, json.RawMessage(`{"role":"diagnose","text":"Inspect"}`)); err != nil {
		t.Fatal(err)
	}
	saved := readWorkRecord(t, store, record.UID)
	if saved.Task.Members[0].Policy.SpendControl.Mode != _testWorkUnlimited || saved.Task.Members[1].Policy.SpendControl.Mode != agentsv1alpha1.SessionSpendModeLimited {
		t.Fatal("accepted policy changed or later member did not resolve current Profile")
	}
}

type taskSelectiveDeleteClient struct {
	client.Client
	failName string
}

func (c *taskSelectiveDeleteClient) Delete(ctx context.Context, object client.Object, opts ...client.DeleteOption) error {
	if object.GetName() == c.failName {
		return errors.New("injected cleanup hold")
	}
	return c.Client.Delete(ctx, object, opts...)
}
func TestTaskStopClosesOtherMembersWhenOneCleanupFails(t *testing.T) {
	store := testStore(t)
	g, writer := workflowJourneyGateway(t, store)
	ctx := t.Context()
	scope := requestScope{Namespace: _testNamespace, Subject: _testConversationSubject}
	record, _, err := g.createTask(ctx, scope, "stop-race", TaskInput{Workflow: _taskTestWorkflow, Text: "Investigate"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.delegateTask(ctx, record, WorkflowAction{SessionUID: "parent", TurnID: "turn", CallID: "call"}, json.RawMessage(`{"role":"diagnose","text":"Inspect"}`)); err != nil {
		t.Fatal(err)
	}
	if err := g.syncTask(ctx, record.UID); err != nil {
		t.Fatal(err)
	}
	record = readWorkRecord(t, store, record.UID)
	coordinator := readWorkRecord(t, store, record.Task.Members[0].WorkID)
	worker := readWorkRecord(t, store, record.Task.Members[1].WorkID)
	g.sessionWriter = &taskSelectiveDeleteClient{Client: writer.Client, failName: coordinator.Name}
	if _, _, err := g.acceptTaskMutation(ctx, scope, record.UID, _taskStop, "stop", ""); err != nil {
		t.Fatal(err)
	}
	if err := g.syncTask(ctx, record.UID); err == nil {
		t.Fatal("cleanup failure not reported")
	}
	var survivor agentsv1alpha1.Session
	if err := writer.Get(ctx, client.ObjectKey{Namespace: _testNamespace, Name: worker.Name}, &survivor); err == nil {
		t.Fatal("one member failure kept another member running")
	}
	saved := readWorkRecord(t, store, record.UID)
	if saved.Task.State != _taskStopping {
		t.Fatal("unknown cleanup marked closed")
	}
}

func TestTaskInitialInputDoesNotIncludeWorkflowInstructions(t *testing.T) {
	store := testStore(t)
	g, writer := workflowJourneyGateway(t, store)
	ctx := t.Context()
	var template agentsv1alpha1.WorkflowTemplate
	if err := writer.Get(ctx, client.ObjectKey{Namespace: _testNamespace, Name: _taskTestWorkflow}, &template); err != nil {
		t.Fatal(err)
	}
	template.Spec.Coordinator.Instructions = "Follow the configured assignment protocol"
	if err := writer.Update(ctx, &template); err != nil {
		t.Fatal(err)
	}
	record, _, err := g.createTask(ctx, requestScope{Namespace: _testNamespace, Subject: _testConversationSubject}, "instructions", TaskInput{Workflow: _taskTestWorkflow, Text: "User input"})
	if err != nil {
		t.Fatal(err)
	}
	if record.Task.Members[0].Text != "User input" {
		t.Fatal("workflow instructions were added to the bounded user input")
	}
	if !strings.Contains(*record.Task.Members[0].Policy.Agent.Instructions, template.Spec.Coordinator.Instructions) {
		t.Fatal("workflow instructions missing from policy")
	}
}

type taskHeldDeliveryStore struct{ Storage }

func (s taskHeldDeliveryStore) DeliveryAllowed(context.Context) error { return errRestoreHeld }
func (s taskHeldDeliveryStore) ClaimWorker(context.Context) (Storage, func(), error) {
	return nil, nil, errRestoreHeld
}
func TestTaskMessageReplayPrecedesRestoreHold(t *testing.T) {
	store := testStore(t)
	g, _ := workflowJourneyGateway(t, store)
	ctx := t.Context()
	scope := requestScope{Namespace: _testNamespace, Subject: _testConversationSubject}
	record, _, err := g.createTask(ctx, scope, "replay-held", TaskInput{Workflow: _taskTestWorkflow, Text: "Initial"})
	if err != nil {
		t.Fatal(err)
	}
	_, first, err := g.acceptTaskMutation(ctx, scope, record.UID, _taskMessage, "follow-up", "Follow up")
	if err != nil {
		t.Fatal(err)
	}
	g.store = taskHeldDeliveryStore{Storage: store}
	_, replay, err := g.acceptTaskMutation(ctx, scope, record.UID, _taskMessage, "follow-up", "Follow up")
	if err != nil || replay.ID != first.ID || !replay.ReceivedAt.Equal(first.ReceivedAt) {
		t.Fatalf("held replay changed receipt: %+v %v", replay, err)
	}
	if _, _, err := g.acceptTaskMutation(ctx, scope, record.UID, _taskMessage, "follow-up", "Changed text"); !errors.Is(err, errConflict) {
		t.Fatalf("changed replay: %v", err)
	}
	if _, _, err := g.acceptTaskMutation(ctx, scope, record.UID, _taskMessage, "fresh", "Fresh input"); !errors.Is(err, errRestoreHeld) {
		t.Fatalf("fresh input passed hold: %v", err)
	}
}

func TestTaskMemberRuntimeRelationshipsStayInGateway(t *testing.T) {
	store := testStore(t)
	g, _ := workflowJourneyGateway(t, store)
	ctx := t.Context()
	scope := requestScope{Namespace: _testNamespace, Subject: _testConversationSubject}
	record, _, err := g.createTask(ctx, scope, "relationships", TaskInput{Workflow: _taskTestWorkflow, Text: "Initial"})
	if err != nil {
		t.Fatal(err)
	}
	action := WorkflowAction{SessionUID: "root-uid", TurnID: "root-turn", CallID: "first-assignment"}
	if _, err := g.delegateTask(ctx, record, action, json.RawMessage(`{"role":"diagnose","text":"First assignment"}`)); err != nil {
		t.Fatal(err)
	}
	record = readWorkRecord(t, store, record.UID)
	member := record.Task.Members[1]
	if _, err := g.saveTaskResult(ctx, record, member, Record{}, WorkflowAction{SessionUID: _taskTestWorker, TurnID: "worker-turn", CallID: "save"}, json.RawMessage(`{"outcome":"success","outputs":[{"name":"diagnosis","content":"Saved findings"}]}`)); err != nil {
		t.Fatal(err)
	}
	record = readWorkRecord(t, store, record.UID)
	resultID := record.Task.Results[0].ID
	action.CallID = "with-evidence"
	input := WorkflowDelegateInput{Role: _taskTestDiagnose, Text: "Use prerequisite evidence", Dependencies: []string{resultID}}
	if _, err := g.delegateTask(ctx, record, action, mustJSON(t, input)); err != nil {
		t.Fatal(err)
	}
	record = readWorkRecord(t, store, record.UID)
	accepted := record.Task.Members[2]
	for _, intent := range record.Task.Members {
		policy := string(mustJSON(t, intent.Policy))
		if strings.Contains(policy, intent.ID) || strings.Contains(policy, resultID) {
			t.Fatal("runtime relationship leaked into Session policy")
		}
	}
	if accepted.Text != input.Text {
		t.Fatal("raw assignment changed")
	}
	if err := g.materializeTaskMember(ctx, record, accepted); err != nil {
		t.Fatal(err)
	}
	work := readWorkRecord(t, store, accepted.WorkID)
	if !strings.Contains(work.Work.Events[0].Text, resultID) || work.Work.Conversation.Digest != work.Work.Events[0].Digest {
		t.Fatal("gateway input lacks prerequisite context or digest agreement")
	}
	action.CallID = "oversize-context"
	input.Text = ""
	overhead := len(mustJSON(t, input))
	input.Text = strings.Repeat("x", _maxInputBytes-overhead)
	raw := mustJSON(t, input)
	if len(raw) > _maxInputBytes {
		t.Fatal("fixture exceeds argument limit before assembly")
	}
	if _, err := g.delegateTask(ctx, record, action, raw); !errors.Is(err, errWorkflowInput) {
		t.Fatalf("oversized assembled assignment accepted: %v", err)
	}
	latest := readWorkRecord(t, store, record.UID)
	if len(latest.Task.Members) != len(record.Task.Members) {
		t.Fatal("oversized assembly committed member intent")
	}
}

func TestTaskResultEquivalentReplayPreservesOriginalWithoutHistory(t *testing.T) {
	checkTaskResultEquivalentReplay(t, testStore(t))
}
func TestPostgreSQLTaskResultEquivalentReplayPreservesOriginalWithoutHistory(t *testing.T) {
	store, _ := postgresForTest(t)
	checkTaskResultEquivalentReplay(t, store)
}
func checkTaskResultEquivalentReplay(t *testing.T, store Storage) {
	t.Helper()
	ctx := t.Context()
	g, _ := workflowJourneyGateway(t, store)
	scope := requestScope{Namespace: _testNamespace, Subject: _testConversationSubject}
	initial, _, err := g.createTask(ctx, scope, "result-equivalence", TaskInput{Workflow: _taskTestWorkflow, Text: "Investigate committed evidence"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.delegateTask(ctx, initial, WorkflowAction{SessionUID: "parent-session", TurnID: "parent-turn", CallID: "delegate"}, json.RawMessage(`{"role":"diagnose","text":"Inspect"}`)); err != nil {
		t.Fatal(err)
	}
	stale := readWorkRecord(t, store, initial.UID)
	member := stale.Task.Members[1]
	action := WorkflowAction{SessionUID: "result-session", TurnID: "result-turn", CallID: "result-call"}
	source := json.RawMessage(`{"id":"source-command","type":"command_execution","command":"go test","output":"ok","exit_code":0,"status":"completed"}`)
	original := json.RawMessage("{\n \"outcome\":\"success\",\"outputs\":[{\"name\":\"diagnosis\",\"content\":\"Observed\",\"checks\":[{\"command\":\"go test\",\"output\":\"ok\",\"exit_code\":0,\"source_item_id\":\"source-command\"}]}]\n}")
	session := Record{}
	session.History.Items = []json.RawMessage{source}
	ack, err := g.saveTaskResult(ctx, stale, member, session, action, original)
	if err != nil {
		t.Fatal(err)
	}
	equivalent := json.RawMessage(`{"outputs":[{"checks":[{"source_item_id":"source-command","exit_code":0,"output":"ok","command":"go test"}],"content":"Observed","name":"diagnosis"}],"outcome":"success"}`)
	// The stale snapshot forces the transactional replay path; current history
	// intentionally lacks the source observation that authorized the first write.
	replay, err := g.saveTaskResult(ctx, stale, member, Record{}, action, equivalent)
	if err != nil || replay != ack {
		t.Fatalf("transactional replay lost committed result: %q %v", replay, err)
	}
	current := readWorkRecord(t, store, initial.UID)
	replay, err = g.saveTaskResult(ctx, current, member, Record{}, action, equivalent)
	if err != nil || replay != ack {
		t.Fatalf("snapshot replay lost committed result: %q %v", replay, err)
	}
	result := current.Task.Results[0]
	if result.Raw != string(original) || result.SHA256 == "" || len(current.Task.Results) != 1 || len(current.Task.Rejections) != 0 {
		t.Fatal("equivalent replay changed source bytes or recorded rejection")
	}
	changed := json.RawMessage(strings.Replace(string(equivalent), "Observed", "Different", 1))
	if _, err := g.saveTaskResult(ctx, current, member, Record{}, action, changed); !errors.Is(err, errConflict) {
		t.Fatalf("changed values replayed: %v", err)
	}
	otherAction := action
	otherAction.CallID = "different-call"
	if _, err := g.saveTaskResult(ctx, current, member, Record{}, otherAction, equivalent); !errors.Is(err, errConflict) {
		t.Fatalf("different action replayed: %v", err)
	}
	final := readWorkRecord(t, store, initial.UID)
	if final.Task.Results[0].Raw != result.Raw || final.Task.Results[0].SHA256 != result.SHA256 {
		t.Fatal("replay altered original evidence digest")
	}
}
