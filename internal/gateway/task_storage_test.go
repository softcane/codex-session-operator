package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
)

const (
	_testStorageFresh          = "fresh"
	_testStorageReplayKey      = "held-message"
	_testStorageRequiredAction = "requires_action"
)

func TestTaskStorageStopFencesFreshChildAndPreservesCleanup(t *testing.T) {
	store := testStore(t)
	checkTaskStorageFence(t, store)
}

func TestPostgreSQLTaskStorageStopFence(t *testing.T) {
	store, _ := postgresForTest(t)
	checkTaskStorageFence(t, store)
}

func checkTaskStorageFence(t *testing.T, store Storage) {
	t.Helper()
	ctx := context.Background()
	fixture := taskStorageFixture(t, "fence")
	taskID := fixture.UID
	if err := store.Update(ctx, taskID, func(r *Record) error { *r = fixture; return nil }); err != nil {
		t.Fatal(err)
	}
	if err := store.Update(ctx, "child-storage", func(r *Record) error {
		r.Namespace = _testNamespace
		r.TaskMember = &TaskMemberBinding{TaskID: taskID, MemberID: fixture.Task.Members[0].ID, Role: fixture.Task.Members[0].Role}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Update(ctx, taskID, func(r *Record) error { r.Task.State = _taskStopping; return nil }); err != nil {
		t.Fatal(err)
	}
	err := store.Update(ctx, "child-storage", func(r *Record) error {
		return addSubmission(r, Submission{Key: _testStorageFresh, Text: _testStorageFresh})
	})
	if !errors.Is(err, errClosed) {
		t.Fatalf("fresh input after stop: %v", err)
	}
	if err := store.Update(ctx, "child-storage", func(r *Record) error { r.Closed = true; return nil }); err != nil {
		t.Fatalf("cleanup blocked: %v", err)
	}
	if err := store.Purge(ctx, "child-storage"); err == nil {
		t.Fatal("task child evidence purged")
	}
}

func TestTaskFreshEffectDetectsCreationClaims(t *testing.T) {
	before := Record{TaskMember: &TaskMemberBinding{TaskID: "task", MemberID: "storage-member"}, Work: &Work{Attempts: []WorkAttempt{{}}}}
	after := before
	after.Work = &Work{Attempts: []WorkAttempt{{CreationClaimed: true}}}
	if !taskFreshEffect(before, after) {
		t.Fatal("fresh creation claim not fenced")
	}
	after.Work = &Work{State: _workClosing, Attempts: []WorkAttempt{{}}}
	if taskFreshEffect(before, after) {
		t.Fatal("cleanup should remain allowed")
	}
}

func TestPostgreSQLTaskBackupRestoreAndActiveScan(t *testing.T) {
	store, _ := postgresForTestLimits(t, 1, 10)
	ctx := context.Background()
	fixture := taskStorageFixture(t, "backup")
	taskID := fixture.UID
	if err := store.Update(ctx, taskID, func(r *Record) error { *r = fixture; return nil }); err != nil {
		t.Fatal(err)
	}
	active, err := store.ActiveRecords(ctx)
	if err != nil || len(active) != 1 || active[0].Task == nil {
		t.Fatalf("task absent from active scan: %v %v", active, err)
	}
	if slots := activeSlots(active[0]); len(slots) != 1 {
		t.Fatalf("task consumes execution slots: %v", slots)
	}
	gateway, _ := testGateway(t, store, simulatorForTest())
	scope := requestScope{Namespace: fixture.Namespace, Subject: fixture.Task.Origin.Subject}
	accepted, receipt, err := gateway.acceptTaskMutation(ctx, scope, taskID, _taskMessage, _testStorageReplayKey, _testFirstMessage)
	if err != nil {
		t.Fatal(err)
	}
	fixture = accepted
	backup, err := store.Backup(ctx)
	if err != nil {
		t.Fatal(err)
	}
	restored, _ := postgresForTestLimits(t, 1, 10)
	if _, err := restored.Restore(ctx, backup); err != nil {
		t.Fatal(err)
	}
	saved, err := restored.Get(ctx, taskID)
	if err != nil || digestValue(saved.Task) != digestValue(fixture.Task) {
		t.Fatalf("lost task evidence: %v %v", saved, err)
	}
	restoredGateway, _ := testGateway(t, restored, simulatorForTest())
	_, replay, replayErr := restoredGateway.acceptTaskMutation(ctx, scope, taskID, _taskMessage, _testStorageReplayKey, _testFirstMessage)
	if replayErr != nil || digestValue(replay) != digestValue(receipt) {
		t.Fatalf("restore blocked exact message receipt replay: %v", replayErr)
	}
	if _, _, err := restoredGateway.acceptTaskMutation(ctx, scope, taskID, _taskMessage, _testStorageReplayKey, "changed"); !errors.Is(err, errConflict) {
		t.Fatalf("restore changed replay: %v", err)
	}
	if _, _, err := restoredGateway.acceptTaskMutation(ctx, scope, taskID, _taskMessage, _testStorageFresh, _testFirstMessage); !errors.Is(err, errRestoreHeld) {
		t.Fatalf("restore fresh message: %v", err)
	}
	err = restored.Update(ctx, taskID, func(r *Record) error {
		r.Task.Members = append(r.Task.Members, TaskMember{ID: _testStorageFresh})
		return nil
	})
	if !errors.Is(err, errRestoreHeld) {
		t.Fatalf("restore admitted fresh task effect: %v", err)
	}
	if err := restored.Archive(ctx, taskID); err == nil {
		t.Fatal("task evidence archived")
	}
}

func TestTaskStorageCannotReopenStoppedTask(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	if err := store.Update(ctx, "task-closed", func(r *Record) error {
		r.Namespace = _testNamespace
		r.Task = &Task{ID: r.UID, State: _taskClosed}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	err := store.Update(ctx, "task-closed", func(r *Record) error { r.Task.State = _taskOpen; return nil })
	if !errors.Is(err, errConflict) {
		t.Fatalf("stopped task reopened: %v", err)
	}
}

func TestPostgreSQLTaskSchemaMigratesLegacyRecords(t *testing.T) {
	store, dsn := postgresForTest(t)
	ctx := context.Background()
	if err := store.Update(ctx, "legacy", func(r *Record) error { r.Namespace = _testNamespace; return nil }); err != nil {
		t.Fatal(err)
	}
	_, err := store.pool.Exec(ctx, `ALTER TABLE gateway_records DROP CONSTRAINT gateway_record_version_check;
 UPDATE gateway_records SET data=convert_to((convert_from(data,'UTF8')::jsonb || '{"version":3}'::jsonb)::text,'UTF8');
 ALTER TABLE gateway_meta DROP CONSTRAINT gateway_meta_version_check;
 UPDATE gateway_meta SET version=1;
 ALTER TABLE gateway_meta ADD CONSTRAINT gateway_meta_version_check CHECK(version=1)`)
	if err != nil {
		t.Fatal(err)
	}
	upgraded, err := OpenPostgreSQL(ctx, PostgreSQLConfig{DSN: dsn, MaxActive: 32, MaxRecords: 100000, AllowInsecureLocal: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = upgraded.Close() }()
	legacy, err := upgraded.Get(ctx, "legacy")
	if err != nil || legacy.Version != _recordVersion {
		t.Fatalf("legacy migration: %v %v", legacy, err)
	}
	_, err = upgraded.pool.Exec(ctx, `UPDATE gateway_records SET data=convert_to((convert_from(data,'UTF8')::jsonb || '{"version":3}'::jsonb)::text,'UTF8') WHERE uid='legacy'`)
	if err == nil {
		t.Fatal("old binary record accepted after migration")
	}
}

func taskStorageFixture(t *testing.T, key string) Record {
	t.Helper()
	g, _, profile := profileGateway(t, testStore(t))
	selection, err := g.selectProfile(t.Context(), _testNamespace, profile.Name)
	if err != nil {
		t.Fatal(err)
	}
	input := TaskInput{Workflow: "workflow", Text: "inspect"}
	origin := ConversationOrigin{Subject: "caller", Key: key, Digest: taskDigest(input)}
	id := taskRecordID(_testNamespace, origin.Subject, key)
	memberID := taskMemberID(id, "coordinator")
	workID := taskWorkID(_testNamespace, id, memberID)
	spec := agentsv1alpha1.WorkflowTemplateSpec{Coordinator: agentsv1alpha1.WorkflowCoordinator{ProfileRef: agentsv1alpha1.AgentReference{Name: profile.Name}}}
	member := TaskMember{ID: memberID, Role: _taskCoordinator, IsCoordinator: true, Text: input.Text, WorkID: workID, Route: selection.route, Profile: selection.provenance, Policy: workPolicy(selection.route, workID, 1), CreatedAt: time.Now().UTC()}
	return Record{Version: _recordVersion, UID: id, Namespace: _testNamespace, Actions: map[string]Action{}, Notes: map[string]string{}, Task: &Task{ID: id, State: _taskOpen, Origin: origin, Workflow: TaskWorkflowSnapshot{Name: input.Workflow, UID: "workflow-uid", Generation: 1, ResourceVersion: "1", Spec: spec, Digest: agentsv1alpha1.WorkflowTemplateSpecDigest(spec)}, Members: []TaskMember{member}, Events: []TaskEvent{{ID: taskEventID(id, origin.Subject, key), Kind: _taskStart, Text: input.Text, Digest: origin.Digest, ReceivedAt: member.CreatedAt}}}}
}

func TestTaskStorageStopHasReservedRecordCapacity(t *testing.T) {
	record := taskStorageFixture(t, "capacity")
	raw, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	record.Task.LastError = strings.Repeat("x", _maxRecordBytes-len(raw)-1500)
	if err := validateRecord(record); err == nil {
		t.Fatal("open task consumed stop reserve")
	}
	record.Task.State = _taskStopping
	if err := validateRecord(record); err != nil {
		t.Fatalf("stop cannot use reserved bytes: %v", err)
	}
}

func TestTaskStorageQuiesceStoppedMember(t *testing.T) {
	checkTaskStorageQuiesce(t, testStore(t))
}
func TestPostgreSQLTaskStorageQuiesceStoppedMember(t *testing.T) {
	store, _ := postgresForTest(t)
	checkTaskStorageQuiesce(t, store)
}
func checkTaskStorageQuiesce(t *testing.T, store Storage) {
	t.Helper()
	for _, status := range []string{_testInProgress, _testStorageRequiredAction} {
		t.Run(status, func(t *testing.T) {
			childUID := _testUID + "-" + status
			fixture := taskStorageFixture(t, "quiesce-"+status)
			fixture.Task.State = _taskStopping
			if err := store.Update(t.Context(), fixture.UID, func(r *Record) error { *r = fixture; return nil }); err != nil {
				t.Fatal(err)
			}
			if err := store.Update(t.Context(), childUID, func(r *Record) error {
				r.Namespace = _testNamespace
				r.RemoteID = _testRemoteID
				r.TaskMember = &TaskMemberBinding{TaskID: fixture.UID, MemberID: fixture.Task.Members[0].ID, Role: fixture.Task.Members[0].Role}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			provider := simulatorForTest()
			provider.history.Status = status
			provider.history.Metadata["kubernetes_uid"] = childUID
			g, _ := testGateway(t, store, provider, testSession())
			if err := g.Quiesce(t.Context(), childUID, _testRemoteID); err != nil {
				t.Fatalf("cleanup after stop: %v", err)
			}
			child, err := store.Get(t.Context(), childUID)
			if err != nil || len(child.Submissions) == 0 || !child.Submissions[len(child.Submissions)-1].Accepted {
				t.Fatalf("cleanup cancellation missing: %v", err)
			}
			// Use a separate child identity for the next status, keeping immutable bindings.
			if err := store.Update(t.Context(), childUID, func(r *Record) error { r.TaskMember = nil; return nil }); !errors.Is(err, errConflict) {
				t.Fatalf("binding removal: %v", err)
			}
		})
	}
}

func TestPostgreSQLTaskSharesCoordinatorCapacity(t *testing.T) {
	store, _ := postgresForTestLimits(t, 1, 10)
	fixture := taskStorageFixture(t, "shared-slot")
	if err := store.Update(t.Context(), fixture.UID, func(r *Record) error { *r = fixture; return nil }); err != nil {
		t.Fatal(err)
	}
	g, _, _ := profileGateway(t, store)
	if err := g.materializeTaskMember(t.Context(), fixture, fixture.Task.Members[0]); err != nil {
		t.Fatalf("coordinator charged twice: %v", err)
	}
	slots := activeSlots(Record{TaskMember: &TaskMemberBinding{TaskID: fixture.UID, MemberID: fixture.Task.Members[0].ID}})
	if len(slots) != 1 || slots[0] != activeSlots(fixture)[0] {
		t.Fatalf("Session reservation does not share member slot: %v", slots)
	}
	second := taskStorageFixture(t, "capacity-blocked")
	if err := store.Update(t.Context(), second.UID, func(r *Record) error { *r = second; return nil }); err == nil {
		t.Fatal("second task bypassed installation capacity")
	}
}

func TestTaskStorageRejectsMalformedAggregate(t *testing.T) {
	tests := []struct {
		name   string
		change func(*Record)
	}{
		{"no coordinator", func(r *Record) { r.Task.Members = nil }},
		{"no initial event", func(r *Record) { r.Task.Events = nil }},
		{"wrong workflow digest", func(r *Record) { r.Task.Workflow.Digest = "wrong" }},
		{"wrong origin", func(r *Record) { r.Task.Origin.Key = "changed" }},
		{"duplicate member", func(r *Record) { r.Task.Members = append(r.Task.Members, r.Task.Members[0]) }},
		{"missing provenance", func(r *Record) { r.Task.Members[0].Profile = nil }},
		{"unbound result", func(r *Record) { r.Task.Results = []TaskResult{{ID: "unbound"}} }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			record := taskStorageFixture(t, test.name)
			test.change(&record)
			if err := validateRecord(record); err == nil {
				t.Fatal("malformed task accepted")
			}
		})
	}
}

// Only corruption tests remove this production guard, after proving it rejects old writers.
func dropVersionConstraintForCorruptionTest(t *testing.T, store *PostgreSQLStore, record Record) {
	t.Helper()
	record.Version = _recordVersion + 1
	if _, err := store.pool.Exec(t.Context(), "UPDATE gateway_records SET data=$1 WHERE uid=$2", mustJSON(t, record), record.UID); err == nil {
		t.Fatal("production database accepted incompatible record version")
	}
	if _, err := store.pool.Exec(t.Context(), "ALTER TABLE gateway_records DROP CONSTRAINT gateway_record_version_check"); err != nil {
		t.Fatal(safeSQLError(err))
	}
}

func TestPostgreSQLTaskStopSerializesCreationClaim(t *testing.T) {
	store, _ := postgresForTest(t)
	fixture := taskStorageFixture(t, "stop-race")
	if err := store.Update(t.Context(), fixture.UID, func(r *Record) error { *r = fixture; return nil }); err != nil {
		t.Fatal(err)
	}
	g, _, _ := profileGateway(t, store)
	member := fixture.Task.Members[0]
	if err := g.materializeTaskMember(t.Context(), fixture, member); err != nil {
		t.Fatal(err)
	}
	locked := make(chan struct{})
	release := make(chan struct{})
	stopped := make(chan error, 1)
	go func() {
		stopped <- store.Update(t.Context(), fixture.UID, func(r *Record) error { r.Task.State = _taskStopping; close(locked); <-release; return nil })
	}()
	<-locked
	claimed := make(chan error, 1)
	go func() {
		claimed <- store.Update(t.Context(), member.WorkID, func(r *Record) error { r.Work.Attempts[0].CreationClaimed = true; return nil })
	}()
	close(release)
	if err := <-stopped; err != nil {
		t.Fatal(err)
	}
	if err := <-claimed; !errors.Is(err, errClosed) {
		t.Fatalf("creation claim after stop: %v", err)
	}
	child, err := store.Get(t.Context(), member.WorkID)
	if err != nil || child.Work.Attempts[0].CreationClaimed {
		t.Fatalf("fresh creation persisted after stop: %v", err)
	}
}

func TestPostgreSQLTaskRestoreRejectsMalformedAggregate(t *testing.T) {
	store, _ := postgresForTest(t)
	malformed := taskStorageFixture(t, "malformed-backup")
	malformed.Task.Members = nil
	backup := StorageBackup{Version: 1, CreatedAt: time.Now().UTC(), Records: []Record{malformed}}
	backup.Digest = digestValue(backup.Records)
	if _, err := store.Restore(t.Context(), backup); err == nil {
		t.Fatal("restored task without coordinator")
	}
	records, err := store.Records(t.Context())
	if err != nil || len(records) != 0 {
		t.Fatalf("malformed restore committed partial data: %v", err)
	}
}

func TestTaskStorageNearCapacityMemberCanQuiesce(t *testing.T) {
	checkTaskStorageNearCapacityQuiesce(t, testStore(t))
}
func TestPostgreSQLTaskStorageNearCapacityMemberCanQuiesce(t *testing.T) {
	store, _ := postgresForTest(t)
	checkTaskStorageNearCapacityQuiesce(t, store)
}
func checkTaskStorageNearCapacityQuiesce(t *testing.T, store Storage) {
	t.Helper()
	fixture := taskStorageFixture(t, "member-capacity")
	fixture.Task.State = _taskStopping
	if err := store.Update(t.Context(), fixture.UID, func(r *Record) error { *r = fixture; return nil }); err != nil {
		t.Fatal(err)
	}
	child := Record{Version: _recordVersion, UID: _testUID, Namespace: _testNamespace, RemoteID: _testRemoteID, Actions: map[string]Action{}, Notes: map[string]string{}, TaskMember: &TaskMemberBinding{TaskID: fixture.UID, MemberID: fixture.Task.Members[0].ID, Role: fixture.Task.Members[0].Role}}
	child.History.Items = []json.RawMessage{json.RawMessage(`""`)}
	raw, err := json.Marshal(child)
	if err != nil {
		t.Fatal(err)
	}
	child.History.Items[0] = json.RawMessage(`"` + strings.Repeat("x", recordSizeLimit(child)-len(raw)) + `"`)
	if err := store.Update(t.Context(), child.UID, func(r *Record) error { *r = child; return nil }); err != nil {
		t.Fatalf("near-capacity member rejected: %v", err)
	}
	if err := store.Update(t.Context(), child.UID, func(r *Record) error { r.LastError = "overflow"; return nil }); err == nil {
		t.Fatal("open member consumed cleanup reserve")
	}
	provider := simulatorForTest()
	provider.history.Status = _testInProgress
	g, _ := testGateway(t, store, provider, testSession())
	if err := g.Quiesce(t.Context(), child.UID, _testRemoteID); err != nil {
		t.Fatalf("near-capacity cleanup: %v", err)
	}
	saved, err := store.Get(t.Context(), child.UID)
	if err != nil || !saved.Closed || len(saved.Submissions) != 1 || !saved.Submissions[0].Accepted {
		t.Fatalf("near-capacity cleanup not committed: %v", err)
	}
}

func TestTaskStorageDecodeRejectsMalformedTaskShape(t *testing.T) {
	record := taskStorageFixture(t, "corrupt-read")
	record.Task.Events = nil
	raw, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeSQLRecord(record.UID, record.Namespace, raw, nil); !errors.Is(err, errUnavailable) {
		t.Fatalf("decoded malformed task: %v", err)
	}
}

func TestPostgreSQLTaskReadersRejectCorruptCurrentVersionShape(t *testing.T) {
	store, _ := postgresForTest(t)
	fixture := taskStorageFixture(t, "corrupt-task-read")
	if err := store.Update(t.Context(), fixture.UID, func(r *Record) error { *r = fixture; return nil }); err != nil {
		t.Fatal(err)
	}
	fixture.Task.Events = nil
	if _, err := store.pool.Exec(t.Context(), "UPDATE gateway_records SET data=$1 WHERE uid=$2", mustJSON(t, fixture), fixture.UID); err != nil {
		t.Fatal(safeSQLError(err))
	}
	if _, err := store.Get(t.Context(), fixture.UID); !errors.Is(err, errUnavailable) {
		t.Fatalf("read trusted corrupt task: %v", err)
	}
	if _, err := store.ActiveRecords(t.Context()); !errors.Is(err, errUnavailable) {
		t.Fatalf("active scan trusted corrupt task: %v", err)
	}
	if _, err := store.RecordSummaries(t.Context(), fixture.Namespace, "task"); !errors.Is(err, errUnavailable) {
		t.Fatalf("summary trusted corrupt task: %v", err)
	}
	called := false
	if err := store.Update(t.Context(), fixture.UID, func(*Record) error { called = true; return nil }); !errors.Is(err, errUnavailable) || called {
		t.Fatalf("writer received corrupt task: %v", err)
	}
	gateway, _ := testGateway(t, store, simulatorForTest())
	scope := requestScope{Namespace: fixture.Namespace, Subject: fixture.Task.Origin.Subject}
	if _, _, err := gateway.createTask(t.Context(), scope, fixture.Task.Origin.Key, TaskInput{Workflow: fixture.Task.Workflow.Name, Text: "inspect"}); !errors.Is(err, errUnavailable) {
		t.Fatalf("create replay trusted corrupt task: %v", err)
	}
}

func TestPostgreSQLTaskRestoreBindsResultProducer(t *testing.T) {
	store, _ := postgresForTest(t)
	fixture := taskStorageFixture(t, "producer-backup")
	if err := store.Update(t.Context(), fixture.UID, func(r *Record) error { *r = fixture; return nil }); err != nil {
		t.Fatal(err)
	}
	gateway, _, _ := profileGateway(t, store)
	member := fixture.Task.Members[0]
	if err := gateway.materializeTaskMember(t.Context(), fixture, member); err != nil {
		t.Fatal(err)
	}
	if err := store.Update(t.Context(), member.WorkID, func(r *Record) error { r.Work.Attempts[0].SessionUID = _testUID; return nil }); err != nil {
		t.Fatal(err)
	}
	if err := store.Update(t.Context(), _testUID, func(r *Record) error {
		r.Namespace = fixture.Namespace
		r.TaskMember = &TaskMemberBinding{TaskID: fixture.UID, MemberID: member.ID, Role: member.Role}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	action := WorkflowAction{SessionUID: _testUID, TurnID: _testTurnID, CallID: _testCallID}
	if _, err := gateway.saveTaskResult(t.Context(), fixture, member, Record{}, action, json.RawMessage(`{"outcome":"success","outputs":[]}`)); err != nil {
		t.Fatal(err)
	}
	backup, err := store.Backup(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		edit func(*StorageBackup)
	}{
		{"valid capture before acknowledgement", func(*StorageBackup) {}},
		{"wrong producer UID", func(b *StorageBackup) {
			for i := range b.Records {
				if b.Records[i].Task != nil {
					b.Records[i].Task.Results[0].Action.SessionUID = "storage-wrong-producer"
				}
			}
		}},
		{"missing producer Session", func(b *StorageBackup) {
			for i, r := range b.Records {
				if r.UID == _testUID {
					b.Records = append(b.Records[:i], b.Records[i+1:]...)
					return
				}
			}
		}},
		{"wrong producer binding", func(b *StorageBackup) {
			for i := range b.Records {
				if b.Records[i].UID == _testUID {
					b.Records[i].TaskMember.Role = "foreign-role"
				}
			}
		}},
	}
	raw := mustJSON(t, backup)
	for i, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			var candidate StorageBackup
			if err := json.Unmarshal(raw, &candidate); err != nil {
				t.Fatal(err)
			}
			test.edit(&candidate)
			candidate.Digest = digestValue(candidate.Records)
			target, _ := postgresForTest(t)
			_, err := target.Restore(t.Context(), candidate)
			if (err == nil) != (i == 0) {
				t.Fatalf("producer restore result: %v", err)
			}
		})
	}
}

func TestTaskStorageStopFencesActionDecision(t *testing.T) {
	checkTaskStorageActionDecision(t, testStore(t))
}
func TestPostgreSQLTaskStorageStopFencesActionDecision(t *testing.T) {
	store, _ := postgresForTest(t)
	checkTaskStorageActionDecision(t, store)
}
func checkTaskStorageActionDecision(t *testing.T, store Storage) {
	t.Helper()
	fixture := taskStorageFixture(t, "approval-stop")
	if err := store.Update(t.Context(), fixture.UID, func(r *Record) error { *r = fixture; return nil }); err != nil {
		t.Fatal(err)
	}
	if err := store.Update(t.Context(), _testUID, func(r *Record) error {
		r.Namespace = fixture.Namespace
		r.TaskMember = &TaskMemberBinding{TaskID: fixture.UID, MemberID: fixture.Task.Members[0].ID, Role: fixture.Task.Members[0].Role}
		r.Actions[_testCallID] = Action{Active: true}
		r.Actions[_testTurnID] = Action{Active: true, Decision: _approve}
		return addSubmission(r, Submission{Key: _testCallID, Text: _testFirstMessage})
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Update(t.Context(), fixture.UID, func(r *Record) error { r.Task.State = _taskStopping; return nil }); err != nil {
		t.Fatal(err)
	}
	for _, decision := range []string{_approve, _deny} {
		err := store.Update(t.Context(), _testUID, func(r *Record) error {
			action := r.Actions[_testCallID]
			action.Decision = decision
			r.Actions[_testCallID] = action
			return nil
		})
		if !errors.Is(err, errClosed) {
			t.Fatalf("decision after stop admitted: %s %v", decision, err)
		}
	}
	if err := store.Update(t.Context(), _testUID, func(r *Record) error {
		action := r.Actions[_testTurnID]
		action.Decision = _approve
		r.Actions[_testTurnID] = action
		return nil
	}); err != nil {
		t.Fatalf("unchanged decision replay fenced: %v", err)
	}
	if err := store.Update(t.Context(), _testUID, func(r *Record) error {
		action := r.Actions[_testTurnID]
		action.Active = false
		r.Actions[_testTurnID] = action
		return nil
	}); err != nil {
		t.Fatalf("action observation fenced: %v", err)
	}
	if err := store.Update(t.Context(), _testUID, func(r *Record) error { r.Submissions[0].Accepted = true; return nil }); err != nil {
		t.Fatalf("accepted observation fenced: %v", err)
	}
}
