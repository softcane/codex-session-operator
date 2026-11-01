package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	_testTaskDetailWork      = "work-detail"
	_testTaskDetailCompleted = "completed"
)

type taskDetailUnavailableReader struct{ client.Reader }

func (r taskDetailUnavailableReader) Get(context.Context, client.ObjectKey, client.Object, ...client.GetOption) error {
	return errors.New("offline")
}

func TestTaskDetailSeparatesSavedResultTurnAndCleanup(t *testing.T) {
	store := testStore(t)
	g, _, _ := profileGateway(t, store)
	task := Record{UID: "task-detail", Namespace: _testNamespace, Task: &Task{ID: "task-detail", State: _taskOpen, Members: []TaskMember{{ID: "member", WorkID: _testTaskDetailWork, Role: "diagnose"}}}}
	child := Record{UID: _testTaskDetailWork, Namespace: _testNamespace, Work: &Work{ID: _testTaskDetailWork, State: _workOpen, Conversation: &ConversationOrigin{}, Attempts: []WorkAttempt{{Number: 1, SessionName: "session", SessionUID: "session-uid", ConversationID: "conv-detail", CreationClaimed: true}}}}
	if err := store.Update(t.Context(), child.UID, func(r *Record) error { *r = child; return nil }); err != nil {
		t.Fatal(err)
	}
	if err := store.Update(t.Context(), "session-uid", func(r *Record) error {
		r.Namespace = _testNamespace
		r.History.Status = _testTaskDetailCompleted
		r.History.Turns = []json.RawMessage{json.RawMessage(`{"id":"turn","status":"completed","items":["private history"]}`)}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	g.reader = taskDetailUnavailableReader{Reader: g.reader}
	detail, err := g.taskDetail(t.Context(), task)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(detail)
	text := string(raw)
	for _, want := range []string{`"result_state":"missing"`, `"cleanup":"pending"`, `"connection":"unknown"`, `"observation_unavailable":true`, `"provider_status":"completed"`, `"conversation_url"`} {
		if !strings.Contains(text, want) {
			t.Fatalf("missing %s in %s", want, text)
		}
	}
	if strings.Contains(text, "private history") {
		t.Fatal("copied raw history")
	}
	task.Task.Results = []TaskResult{{ID: "result", MemberID: "member", Outcome: "success", Raw: "private full report"}}
	detail, err = g.taskDetail(t.Context(), task)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ = json.Marshal(detail)
	if !strings.Contains(string(raw), `"result_state":"final"`) || strings.Contains(string(raw), "private full report") {
		t.Fatalf("result projection: %s", raw)
	}
}

func TestTaskTurnSummariesPreserveNativeAttribution(t *testing.T) {
	summaries := taskTurnSummaries([]json.RawMessage{
		json.RawMessage(`{"id":"native","subagent_id":"child-agent","session_id":"provider-session","agent_id":"agent","status":"in_progress"}`),
		json.RawMessage(`{"id":"unattributed","status":"failed"}`),
	})
	if len(summaries) != 2 || summaries[0]["subagent_id"] != "child-agent" || summaries[0]["session_id"] != "provider-session" || summaries[0]["agent_id"] != "agent" || summaries[0]["attribution"] != "subagent" || summaries[1]["attribution"] != "unknown" {
		t.Fatalf("incorrect turn attribution: %#v", summaries)
	}
	if _, invented := summaries[0]["parent_id"]; invented {
		t.Fatal("invented unsupported parent identity")
	}
}

func TestTaskDetailRetainsAcceptedConfigurationBeforeProvisioning(t *testing.T) {
	g, _, _ := profileGateway(t, testStore(t))
	record := Record{Namespace: _testNamespace, Task: &Task{ID: "task-provenance", State: _taskOpen, Members: []TaskMember{{ID: "assignment", WorkID: "unmaterialized", Profile: &ProfileProvenance{Name: "accepted-profile", UID: "accepted-uid", Generation: 7}, Policy: agentsv1alpha1.SessionSpec{AgentRef: &agentsv1alpha1.AgentReference{Name: "accepted-agent"}, EnvironmentRef: &agentsv1alpha1.AgentReference{Name: "accepted-environment"}, SpendControl: &agentsv1alpha1.SessionSpendControl{Mode: "Unlimited"}}}}}}
	detail, err := g.taskDetail(t.Context(), record)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(detail)
	for _, want := range []string{`"name":"accepted-profile"`, `"uid":"accepted-uid"`, `"generation":7`, `"agent_ref":{"name":"accepted-agent"}`, `"environment_ref":{"name":"accepted-environment"}`, `"spend_control":{"mode":"Unlimited"}`} {
		if !strings.Contains(string(raw), want) {
			t.Fatalf("missing accepted provenance %s: %s", want, raw)
		}
	}
}
