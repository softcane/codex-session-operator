package gateway

import (
	"encoding/json"
	"strings"
	"testing"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
)

func TestWorkflowResultRejectsMissingOutputAndOversize(t *testing.T) {
	member := TaskMember{Role: _taskTestDiagnose}
	task := Task{Workflow: TaskWorkflowSnapshot{}}
	task.Workflow.Spec.Roles = append(task.Workflow.Spec.Roles, workflowTestRole())
	for _, raw := range []string{`{"outcome":"success","outputs":[]}`, `{"outcome":"success","outputs":[{"name":"diagnosis","content":"` + strings.Repeat("x", 256<<10) + `"}]}`} {
		if _, err := validateWorkflowResult(task, member, json.RawMessage(raw), nil); err == nil {
			t.Fatal("invalid result accepted")
		}
	}
}

func workflowTestRole() agentsv1alpha1.WorkflowRole {
	return agentsv1alpha1.WorkflowRole{Name: _taskTestDiagnose, Outputs: []string{"diagnosis"}}
}

func TestWorkflowChecksRequireSavedCommandEvidence(t *testing.T) {
	task := Task{}
	member := TaskMember{IsCoordinator: true}
	raw := json.RawMessage(`{"outcome":"success","outputs":[{"name":"notes","content":"observed","checks":[{"command":"go test ./...","output":"ok","exit_code":0,"source_item_id":"evidence"}]}]}`)
	for _, item := range []string{`{"id":"evidence","type":"message","content":"ok"}`, `{"id":"evidence","type":"command_execution","command":"other","output":"ok","exit_code":0}`, `{"id":"evidence","type":"command_execution","command":"go test ./...","output":"ok"}`} {
		if _, err := validateWorkflowResult(task, member, raw, []json.RawMessage{json.RawMessage(item)}); err == nil {
			t.Fatalf("unsubstantiated command accepted: %s", item)
		}
	}
	item := json.RawMessage(`{"id":"evidence","type":"command_execution","command":"go test ./...","output":"ok","exit_code":0,"status":"completed"}`)
	if _, err := validateWorkflowResult(task, member, raw, []json.RawMessage{item}); err != nil {
		t.Fatal(err)
	}
}

func TestCoordinatorResultDoesNotCoverConfiguredRoleDependency(t *testing.T) {
	task := Task{Members: []TaskMember{{ID: "root", Role: _taskCoordinator, IsCoordinator: true}, {ID: _taskTestWorker, Role: _taskCoordinator}}, Results: []TaskResult{{ID: "synthesis", MemberID: "root", Role: _taskCoordinator, Outcome: _taskOutcomeSuccess}, {ID: "assignment-result", MemberID: _taskTestWorker, Role: _taskCoordinator, Outcome: _taskOutcomeSuccess}}}
	if err := validateTaskDependencies(task, []string{_taskCoordinator}, []string{"synthesis"}); err == nil {
		t.Fatal("coordinator synthesis covered worker role dependency")
	}
	if err := validateTaskDependencies(task, []string{_taskCoordinator}, []string{"assignment-result"}); err != nil {
		t.Fatal(err)
	}
}

func TestWorkflowChecksRequireExplicitExitAndTerminalObservation(t *testing.T) {
	history := []json.RawMessage{json.RawMessage(`{"id":"evidence","type":"command_execution","command":"go test","output":"ok","exit_code":0,"status":"completed"}`)}
	missingExit := json.RawMessage(`{"outcome":"success","outputs":[{"name":"notes","content":"observed","checks":[{"command":"go test","output":"ok","source_item_id":"evidence"}]}]}`)
	if _, err := validateWorkflowResult(Task{}, TaskMember{IsCoordinator: true}, missingExit, history); err == nil {
		t.Fatal("missing exit_code accepted as zero")
	}
	exit := 0
	check := WorkflowCheck{Command: "go test", Output: "ok", ExitCode: &exit, SourceItemID: "evidence"}
	inProgress := []json.RawMessage{json.RawMessage(`{"id":"evidence","type":"command_execution","command":"go test","output":"ok","exit_code":0,"status":"in_progress"}`)}
	if workflowCheckEvidence(inProgress, check) {
		t.Fatal("nonterminal command accepted as observed completion")
	}
}

func TestWorkflowPatchRequiresExactGitRevision(t *testing.T) {
	for _, revision := range []string{"", "main", "HEAD", "abc123", strings.Repeat("g", 40), strings.Repeat("a", 41)} {
		if workflowExactGitRevision(revision) {
			t.Fatalf("nonexact revision accepted: %q", revision)
		}
	}
	for _, revision := range []string{strings.Repeat("a", 40), strings.Repeat("B", 64)} {
		if !workflowExactGitRevision(revision) {
			t.Fatalf("full object ID rejected: %q", revision)
		}
	}
}
