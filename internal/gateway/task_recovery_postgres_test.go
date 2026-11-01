package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	"github.com/softcane/codex-session-operator/internal/openaiapi"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	_taskRecoveryUIDField    = "kubernetes_uid"
	_taskRecoveryInflightUID = "inflight-task-session"
)

// taskRecoveryFault rolls back exactly one real transaction at the selected
// boundary. Earlier transactions remain committed; no durable flags are reset.
type taskRecoveryFault struct {
	Storage
	fired   bool
	matches func(Record, Record) bool
}

func (s *taskRecoveryFault) Update(ctx context.Context, uid string, change func(*Record) error) error {
	return s.Storage.Update(ctx, uid, func(record *Record) error {
		raw, err := json.Marshal(record)
		if err != nil {
			return err
		}
		var before Record
		if err := json.Unmarshal(raw, &before); err != nil {
			return err
		}
		if err := change(record); err != nil {
			return err
		}
		if !s.fired && s.matches(before, *record) {
			s.fired = true
			return io.ErrUnexpectedEOF
		}
		return nil
	})
}

func taskRecoveryRestart(t *testing.T, old *PostgreSQLStore, dsn string, previous *Gateway, writer *sessionWriter) (*Gateway, *PostgreSQLStore) {
	t.Helper()
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	store, err := OpenPostgreSQL(t.Context(), PostgreSQLConfig{DSN: dsn, MaxActive: 32, MaxRecords: 100000, AllowInsecureLocal: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	gateway, err := New(store, previous.reader, previous.provider, previous.subject, previous.functions)
	if err != nil {
		t.Fatal(err)
	}
	gateway.ctx = t.Context()
	if err := gateway.EnableProfiles(writer); err != nil {
		t.Fatal(err)
	}
	return gateway, store
}

func taskRecoverySetup(t *testing.T, store Storage) (*Gateway, *sessionWriter, Record, *agentsv1alpha1.Session, openaiapi.PendingAction) {
	t.Helper()
	gateway, writer := workflowJourneyGateway(t, store)
	scope := requestScope{Namespace: _testNamespace, Subject: _testConversationSubject}
	task, _, err := gateway.createTask(t.Context(), scope, "recovery-start", TaskInput{Workflow: _taskTestWorkflow, Text: _testFirstMessage})
	if err != nil {
		t.Fatal(err)
	}
	if err := gateway.syncTask(t.Context(), task.UID); err != nil {
		t.Fatal(err)
	}
	coordinator := readWorkRecord(t, store, task.Task.Members[0].WorkID)
	delegation := WorkflowAction{SessionUID: coordinator.Work.Attempts[0].SessionUID, TurnID: _testTurnID, CallID: "recovery-delegation"}
	if _, err := gateway.delegateTask(t.Context(), task, delegation, json.RawMessage(`{"role":"diagnose","text":"Inspect the failing path"}`)); err != nil {
		t.Fatal(err)
	}
	if err := gateway.syncTask(t.Context(), task.UID); err != nil {
		t.Fatal(err)
	}
	task = readWorkRecord(t, store, task.UID)
	member := task.Task.Members[1]
	work := readWorkRecord(t, store, member.WorkID)
	var session agentsv1alpha1.Session
	if err := writer.Get(t.Context(), client.ObjectKey{Namespace: task.Namespace, Name: work.Work.Attempts[0].SessionName}, &session); err != nil {
		t.Fatal(err)
	}
	session.Status.OpenAISessionID = _testRemoteID
	if err := writer.Update(t.Context(), &session); err != nil {
		t.Fatal(err)
	}
	if _, _, err := gateway.lookup(t.Context(), task.Namespace, session.Name); err != nil {
		t.Fatal(err)
	}
	pending := openaiapi.PendingAction{Type: _functionCall, Name: "workflow_result", TurnID: _testTurnID, CallID: _testCallID, Arguments: json.RawMessage(`{"outcome":"success","outputs":[{"name":"diagnosis","content":"Saved recovery evidence"}]}`)}
	turn := mustJSON(t, map[string]any{"id": pending.TurnID, "session_id": _testRemoteID, "subagent_id": nil})
	history := openaiapi.History{ID: _testRemoteID, Status: _testStorageRequiredAction, Metadata: map[string]string{_taskRecoveryUIDField: string(session.UID), "operator": "codex-session-operator"}, Turns: []json.RawMessage{turn}, Actions: []openaiapi.PendingAction{pending}}
	if err := gateway.recover(t.Context(), string(session.UID), history, false); err != nil {
		t.Fatal(err)
	}
	gateway.provider.(*simulator).history = history
	return gateway, writer, task, &session, pending
}

func TestPostgreSQLTaskRecoversResultBeforeActionAcknowledgement(t *testing.T) {
	store, dsn := postgresForTest(t)
	gateway, writer, task, session, pending := taskRecoverySetup(t, store)
	uid := string(session.UID)
	id := actionID(pending)
	fault := &taskRecoveryFault{Storage: store, matches: func(before, after Record) bool {
		return after.UID == uid && len(before.Actions[id].Result) == 0 && len(after.Actions[id].Result) > 0
	}}
	gateway.store = fault
	action := readWorkRecord(t, store, uid).Actions[id]
	if err := gateway.processWorkflowFunction(t.Context(), uid, id, pending, action); !errors.Is(err, io.ErrUnexpectedEOF) || !fault.fired {
		t.Fatalf("result acknowledgement fault not reached: %v", err)
	}
	saved := readWorkRecord(t, store, task.UID)
	if len(saved.Task.Results) != 1 {
		t.Fatal("evidence was not committed before acknowledgement failure")
	}
	result := saved.Task.Results[0]
	before := readWorkRecord(t, store, uid)
	if len(before.Actions[id].Result) != 0 || before.Actions[id].SubmissionKey != "" {
		t.Fatal("fault committed action acknowledgement")
	}
	gateway, store = taskRecoveryRestart(t, store, dsn, gateway, writer)
	action = readWorkRecord(t, store, uid).Actions[id]
	if err := gateway.processWorkflowFunction(t.Context(), uid, id, pending, action); err != nil {
		t.Fatal(err)
	}
	replay := readWorkRecord(t, store, uid).Actions[id]
	if err := gateway.processWorkflowFunction(t.Context(), uid, id, pending, replay); err != nil {
		t.Fatal(err)
	}
	recovered := readWorkRecord(t, store, task.UID)
	if len(recovered.Task.Results) != 1 || digestValue(recovered.Task.Results[0]) != digestValue(result) {
		t.Fatal("restart changed committed evidence")
	}
	record := readWorkRecord(t, store, uid)
	count := 0
	for _, submission := range record.Submissions {
		if submission.Key == replay.SubmissionKey {
			count++
		}
	}
	if count != 1 || replay.SubmissionKey != resultKey(uid, id) {
		t.Fatal("restart duplicated provider result submission")
	}
	if err := gateway.deliver(t.Context(), session, uid, record.History); err != nil {
		t.Fatal(err)
	}
	if err := gateway.deliver(t.Context(), session, uid, record.History); err != nil {
		t.Fatal(err)
	}
	if gateway.provider.(*simulator).commits != 1 {
		t.Fatal("provider result delivered more than once")
	}
}

func TestPostgreSQLTaskRecoversHandoffBeforeQueueAcknowledgement(t *testing.T) {
	store, dsn := postgresForTest(t)
	gateway, writer, task, session, pending := taskRecoverySetup(t, store)
	uid := string(session.UID)
	id := actionID(pending)
	if err := gateway.processWorkflowFunction(t.Context(), uid, id, pending, readWorkRecord(t, store, uid).Actions[id]); err != nil {
		t.Fatal(err)
	}
	task = readWorkRecord(t, store, task.UID)
	fault := &taskRecoveryFault{Storage: store, matches: func(before, after Record) bool {
		return after.UID == task.UID && before.Task != nil && len(before.Task.Results) == 1 && !before.Task.Results[0].HandoffQueued && after.Task.Results[0].HandoffQueued
	}}
	gateway.store = fault
	if err := gateway.queueTaskEvents(t.Context(), task); !errors.Is(err, io.ErrUnexpectedEOF) || !fault.fired {
		t.Fatalf("handoff acknowledgement fault not reached: %v", err)
	}
	coordinator := task.Task.Members[0]
	before := readWorkRecord(t, store, coordinator.WorkID)
	if len(before.Work.Events) != 2 || readWorkRecord(t, store, task.UID).Task.Results[0].HandoffQueued {
		t.Fatal("handoff boundary was not preserved")
	}
	gateway, store = taskRecoveryRestart(t, store, dsn, gateway, writer)
	if err := gateway.queueTaskEvents(t.Context(), readWorkRecord(t, store, task.UID)); err != nil {
		t.Fatal(err)
	}
	if err := gateway.queueTaskEvents(t.Context(), readWorkRecord(t, store, task.UID)); err != nil {
		t.Fatal(err)
	}
	after := readWorkRecord(t, store, coordinator.WorkID)
	if len(after.Work.Events) != 2 || digestValue(after.Work.Events) != digestValue(before.Work.Events) || !readWorkRecord(t, store, task.UID).Task.Results[0].HandoffQueued {
		t.Fatal("restart duplicated or changed handoff")
	}
}

// No remote controller runs in this test. The observer proves absence only for
// the exact claimed Session UID, rather than inventing an unrelated remote ID.
type taskRecoveryAbsentRemote struct {
	Provider
	expectedUID string
	queried     bool
}

func (p *taskRecoveryAbsentRemote) GetSession(context.Context, string) (openaiapi.Session, error) {
	return openaiapi.Session{}, errors.New("unexpected remote identity for Session without remote creation")
}
func (p *taskRecoveryAbsentRemote) FindSession(ctx context.Context, uid string) (openaiapi.Session, error) {
	if err := ctx.Err(); err != nil {
		return openaiapi.Session{}, err
	}
	if uid != p.expectedUID {
		return openaiapi.Session{}, errConflict
	}
	p.queried = true
	return openaiapi.Session{}, nil
}

func TestPostgreSQLTaskStopDuringClaimedSessionCreation(t *testing.T) {
	store, dsn := postgresForTest(t)
	gateway, writer := workflowJourneyGateway(t, store)
	observer := &taskRecoveryAbsentRemote{Provider: gateway.provider, expectedUID: _taskRecoveryInflightUID}
	gateway.provider = observer
	scope := requestScope{Namespace: _testNamespace, Subject: _testConversationSubject}
	task, _, err := gateway.createTask(t.Context(), scope, "stop-inflight-create", TaskInput{Workflow: _taskTestWorkflow, Text: _testFirstMessage})
	if err != nil {
		t.Fatal(err)
	}
	member := task.Task.Members[0]
	if err := gateway.materializeTaskMember(t.Context(), task, member); err != nil {
		t.Fatal(err)
	}
	claimed := make(chan error, 1)
	release := make(chan struct{})
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	writer.createRace = func(object client.Object) error {
		work, err := store.Get(ctx, member.WorkID)
		if err == nil && (!work.Work.Attempts[0].CreationClaimed || work.Work.Attempts[0].SessionUID != "") {
			err = errors.New("Session create ran without durable creation claim")
		}
		claimed <- err
		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}
		object.SetUID(types.UID(_taskRecoveryInflightUID))
		return writer.Client.Create(ctx, object)
	}
	completed := make(chan error, 1)
	go func() { completed <- gateway.syncWork(ctx, member.WorkID) }()
	select {
	case err := <-claimed:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if _, _, err := gateway.acceptTaskMutation(t.Context(), scope, task.UID, _taskStop, "stop-during-create", ""); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := <-completed; err != nil && !errors.Is(err, errClosed) {
		t.Fatal(err)
	}
	frozen := readWorkRecord(t, store, member.WorkID).Work.Attempts[0]
	if !frozen.CreationClaimed || frozen.SessionUID != _taskRecoveryInflightUID || writer.creates != 1 {
		t.Fatal("in-flight create lost frozen identity")
	}
	gateway, store = taskRecoveryRestart(t, store, dsn, gateway, writer)
	for range 3 {
		if err := gateway.syncTask(t.Context(), task.UID); err != nil {
			t.Fatal(err)
		}
	}
	if !observer.queried {
		t.Fatal("cleanup did not prove remote absence by exact claimed UID")
	}
	if gateway.provider.(*taskRecoveryAbsentRemote).Provider.(*simulator).posts != 0 {
		t.Fatal("stopped Session submitted remote input")
	}
	closed := readWorkRecord(t, store, task.UID)
	child := readWorkRecord(t, store, member.WorkID)
	if closed.Task.State != _taskClosed || child.Work.State != _workClosed || !child.Work.Attempts[0].Cleaned || child.Work.Attempts[0].SessionUID != frozen.SessionUID || writer.creates != 1 {
		t.Fatal("restart did not clean exact in-flight Session")
	}
	var sessions agentsv1alpha1.SessionList
	if err := writer.List(t.Context(), &sessions, client.InNamespace(task.Namespace)); err != nil || len(sessions.Items) != 0 {
		t.Fatalf("stopped Session still exists: %v", err)
	}
	if _, _, err := gateway.acceptTaskMutation(t.Context(), scope, task.UID, _taskMessage, _testStorageFresh, _testFirstMessage); !errors.Is(err, errClosed) {
		t.Fatalf("stopped task admitted input: %v", err)
	}
}

func TestPostgreSQLRestoredTaskDetailReportsGlobalHold(t *testing.T) {
	source, _ := postgresForTest(t)
	gateway, writer := workflowJourneyGateway(t, source)
	scope := requestScope{Namespace: _testNamespace, Subject: _testConversationSubject}
	task, _, err := gateway.createTask(t.Context(), scope, "restore-hold-read", TaskInput{Workflow: _taskTestWorkflow, Text: _testFirstMessage})
	if err != nil {
		t.Fatal(err)
	}
	backup, err := source.Backup(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	target, _ := postgresForTest(t)
	if _, err := target.Restore(t.Context(), backup); err != nil {
		t.Fatal(err)
	}
	restoredGateway, err := New(target, gateway.reader, gateway.provider, gateway.subject, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := restoredGateway.EnableProfiles(writer); err != nil {
		t.Fatal(err)
	}
	record := readWorkRecord(t, target, task.UID)
	if record.DeliveryHeld {
		t.Fatal("test requires global-only restore hold")
	}
	detail, err := restoredGateway.taskDetail(t.Context(), record)
	if err != nil {
		t.Fatal(err)
	}
	var observed struct {
		DeliveryHeld bool              `json:"delivery_held"`
		Members      []json.RawMessage `json:"members"`
	}
	if err := json.Unmarshal(mustJSON(t, detail), &observed); err != nil {
		t.Fatal(err)
	}
	if !observed.DeliveryHeld || len(observed.Members) != 1 {
		t.Fatal("task read hid restore hold or member intent")
	}
	if _, _, err := restoredGateway.acceptTaskMutation(t.Context(), scope, task.UID, _taskMessage, _testStorageFresh, _testFirstMessage); !errors.Is(err, errRestoreHeld) {
		t.Fatalf("restored task admitted input: %v", err)
	}
}
