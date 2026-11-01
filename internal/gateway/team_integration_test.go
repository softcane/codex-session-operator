package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"testing"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	"github.com/softcane/codex-session-operator/internal/openaiapi"
)

func TestWorkflowActionRequiresExactParentTurn(t *testing.T) {
	tests := []struct {
		name    string
		turns   []json.RawMessage
		allowed bool
	}{
		{name: "parent", turns: []json.RawMessage{json.RawMessage(`{"id":"turn-1","session_id":"sess-1","subagent_id":null}`)}, allowed: true},
		{name: "native child", turns: []json.RawMessage{json.RawMessage(`{"id":"turn-1","session_id":"sess-1","subagent_id":"child"}`)}},
		{name: "missing attribution", turns: []json.RawMessage{json.RawMessage(`{"id":"turn-1","session_id":"sess-1"}`)}},
		{name: "wrong session", turns: []json.RawMessage{json.RawMessage(`{"id":"turn-1","session_id":"other","subagent_id":null}`)}},
		{name: "different turn", turns: []json.RawMessage{json.RawMessage(`{"id":"turn-2","session_id":"sess-1","subagent_id":null}`)}},
		{name: "ambiguous duplicate", turns: []json.RawMessage{
			json.RawMessage(`{"id":"turn-1","session_id":"sess-1","subagent_id":null}`),
			json.RawMessage(`{"id":"turn-1","session_id":"sess-1","subagent_id":"child"}`),
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			history := openaiapi.History{ID: _testRemoteID, Turns: tt.turns}
			action := openaiapi.PendingAction{Type: _functionCall, TurnID: _testTurnID, CallID: _testCallID}
			if err := verifyWorkflowRoot(history, action); (err == nil) != tt.allowed {
				t.Fatalf("root authorization error = %v, want allowed %v", err, tt.allowed)
			}
		})
	}
}

func TestWorkflowUnboundSessionCannotDelegate(t *testing.T) {
	store := testStore(t)
	g, _ := testGateway(t, store, simulatorForTest())
	pending := openaiapi.PendingAction{
		Type: _functionCall, Name: "workflow_delegate", TurnID: _testTurnID, CallID: _testCallID,
		Arguments: json.RawMessage(`{"role":"worker","text":"work","task_id":"forged-parent"}`),
	}
	id := actionID(pending)
	if err := store.Update(context.Background(), _testUID, func(r *Record) error {
		r.Namespace = _testNamespace
		r.Actions[id] = Action{Pending: pending, Active: true}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := g.processFunction(context.Background(), _testUID, id, pending, Action{Active: true}, _testNamespace); err != nil {
		t.Fatal(err)
	}
	record := readWorkRecord(t, store, _testUID)
	var result struct {
		Success bool   `json:"success"`
		Error   string `json:"error"`
	}
	if err := json.Unmarshal(record.Actions[id].Result, &result); err != nil {
		t.Fatal(err)
	}
	if result.Success || result.Error == "" {
		t.Fatalf("unbound delegation was not rejected: %+v", result)
	}
	records, err := store.Records(context.Background())
	if err != nil || len(records) != 1 {
		t.Fatalf("unbound call created work: records=%d, error=%v", len(records), err)
	}
}

func TestWorkflowMissingProvenanceRemainsUnknown(t *testing.T) {
	history := openaiapi.History{ID: _testRemoteID}
	pending := openaiapi.PendingAction{Type: _functionCall, TurnID: _testTurnID, CallID: _testCallID}
	if err := verifyWorkflowRoot(history, pending); !errors.Is(err, errUnavailable) {
		t.Fatalf("missing turn must remain unknown: %v", err)
	}
}

func TestTaskMemberAtSubmissionCapacityCanStop(t *testing.T) {
	checkTaskMemberSubmissionCapacity(t, testStore(t))
}

func TestPostgreSQLTaskMemberAtSubmissionCapacityCanStop(t *testing.T) {
	store, _ := postgresForTest(t)
	checkTaskMemberSubmissionCapacity(t, store)
}

func checkTaskMemberSubmissionCapacity(t *testing.T, store Storage) {
	t.Helper()
	parent := taskStorageFixture(t, "submission-capacity")
	if err := store.Update(t.Context(), parent.UID, func(r *Record) error { *r = parent; return nil }); err != nil {
		t.Fatal(err)
	}
	if err := store.Update(t.Context(), _testUID, func(r *Record) error {
		r.Namespace = _testNamespace
		r.RemoteID = _testRemoteID
		r.TaskMember = &TaskMemberBinding{
			TaskID: parent.UID, MemberID: parent.Task.Members[0].ID, Role: parent.Task.Members[0].Role,
		}
		for i := range _maxSubmissions {
			key := "input-" + strconv.Itoa(i)
			if err := addSubmission(r, Submission{Key: key, Text: "input"}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Update(t.Context(), parent.UID, func(r *Record) error { r.Task.State = _taskStopping; return nil }); err != nil {
		t.Fatal(err)
	}
	provider := simulatorForTest()
	provider.history.Status = _testInProgress
	g, _ := testGateway(t, store, provider)
	if err := g.Quiesce(t.Context(), _testUID, _testRemoteID); err != nil {
		t.Fatalf("full submission log blocked cleanup: %v", err)
	}
	record := readWorkRecord(t, store, _testUID)
	if !record.Closed || len(record.Submissions) != _maxSubmissions+1 || !record.Submissions[_maxSubmissions].Accepted {
		t.Fatal("reserved cleanup cancellation was not committed")
	}
}

func TestWorkOwnershipComparesToolSchemasByContent(t *testing.T) {
	session := testSession()
	session.Annotations = map[string]string{_workAnnotation: "owned-work"}
	tools := workflowTools(false)
	tools[0].Function.Parameters.Raw = []byte(`{"type":"object","maximum":9007199254740993}`)
	session.Spec.Agent = &agentsv1alpha1.SessionAgentOverrides{Tools: &tools}
	record := Record{UID: "owned-work", Namespace: session.Namespace}
	attempt := WorkAttempt{
		SessionName: session.Name, SessionUID: string(session.UID), Policy: *session.Spec.DeepCopy(),
	}
	tests := []struct {
		name    string
		schema  string
		allowed bool
	}{
		{name: "API server key order", schema: `{"maximum":9007199254740993,"type":"object"}`, allowed: true},
		{name: "changed large integer", schema: `{"maximum":9007199254740992,"type":"object"}`},
		{name: "changed schema type", schema: `{"maximum":9007199254740993,"type":"string"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			observed := session.DeepCopy()
			(*observed.Spec.Agent.Tools)[0].Function.Parameters.Raw = []byte(tt.schema)
			if err := checkWorkOwnership(record, attempt, observed); (err == nil) != tt.allowed {
				t.Fatalf("policy comparison error = %v, want allowed %v", err, tt.allowed)
			}
		})
	}
}
