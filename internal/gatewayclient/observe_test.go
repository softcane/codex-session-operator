package gatewayclient

import (
	"encoding/json"
	"testing"
)

func TestObservedRootsRejectsConflictingSnapshots(t *testing.T) {
	tests := []struct {
		name, turns string
		wantError   bool
		count       int
	}{
		{name: "prior success and failure", turns: `[{"id":"a","status":"completed"},{"id":"b","status":"failed"}]`, count: 2},
		{name: "subagent excluded", turns: `[{"id":"child","status":"completed","subagent_id":"sub-1"}]`, count: 0},
		{name: "duplicate id", turns: `[{"id":"a","status":"completed"},{"id":"a","status":"completed"}]`, count: 1},
		{name: "conflicting duplicate", turns: `[{"id":"a","status":"completed"},{"id":"a","status":"in_progress"}]`, wantError: true},
		{name: "missing id", turns: `[{"status":"completed"}]`, wantError: true},
		{name: "null subagent root", turns: `[{"id":"a","status":"completed","subagent_id":null}]`, count: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var detail conversationDetail
			if err := json.Unmarshal([]byte(`{"history":{"turns":`+tt.turns+`}}`), &detail); err != nil {
				t.Fatal(err)
			}
			roots, err := observedRoots(detail)
			if (err != nil) != tt.wantError || !tt.wantError && len(roots) != tt.count {
				t.Fatalf("roots=%v error=%v", roots, err)
			}
		})
	}
}

func TestNewRootOutcomesExcludeEveryBaselineIdentity(t *testing.T) {
	baseline := []rootTurn{{ID: "old-success", Status: _rootCompleted}, {ID: "old-failure", Status: "failed"}, {ID: "steered", Status: "in_progress"}}
	roots := []rootTurn{{ID: "steered", Status: _rootCompleted}, {ID: "old-failure", Status: "failed"},
		{ID: "new", Status: _rootCompleted}, {ID: "old-success", Status: _rootCompleted}, {ID: "queued", Status: "queued"}}
	outcomes := newRootOutcomes(roots, baseline)
	if len(outcomes) != 1 || outcomes[0].ID != "new" {
		t.Fatalf("old root qualified: %v", outcomes)
	}
}

func TestObservationStateUsesStructuredLifecycleAndIgnoresDisplayPhase(t *testing.T) {
	tests := []struct{ name, lifecycle, want string }{
		{name: "no lifecycle or phase required", want: _stateAccepted},
		{name: "preparing despite stale Ready phase", lifecycle: `{"execution_ready":false,"reason":"WorkspacePreparing"}`, want: _statePreparing},
		{name: "connecting", lifecycle: `{"execution_ready":false,"reason":"EnvironmentConnecting"}`, want: "connecting"},
		{name: "runtime unavailable", lifecycle: `{"execution_ready":false,"reason":"ExecutorObservationUnavailable"}`, want: _observationUnavailable},
		{name: "recovery hold", lifecycle: `{"execution_ready":false,"recovery":{"required":true,"reason":"CreationOutcomeUnknown"}}`, want: "recovery_hold"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body := `{"summary":{"state":"open","phase":"FutureDisplayValue"},"history":{"id":"remote"}`
			if tt.lifecycle != "" {
				body += `,"lifecycle":` + tt.lifecycle
			}
			body += `}`
			var detail conversationDetail
			if err := json.Unmarshal([]byte(body), &detail); err != nil {
				t.Fatal(err)
			}
			if got := observationState(detail, nil); got != tt.want {
				t.Fatalf("got=%s want=%s", got, tt.want)
			}
		})
	}
}
