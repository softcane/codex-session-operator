package gateway

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	"github.com/softcane/codex-session-operator/internal/openaiapi"
)

const (
	_taskStart         = "start"
	_taskRoleField     = "role"
	_gatewayNoneStatus = "gateway.none"
	_echoFunction      = "gateway_echo"
	_noteFunction      = "gateway_record_note"
	_historyField      = "history"
	_computerApproval  = "computer_use_approval_request"
	_functionCall      = "function_call"
	_approve           = "approve"
	_deny              = "deny"
	_typeField         = "type"
	_closedStatus      = "closed"
)

const (
	_recordVersion  = 4
	_maxSubmissions = 1000
	_maxEvents      = 256
	_maxRecordBytes = 16 << 20
	_maxInputBytes  = 256 << 10
)

var (
	errConflict    = errors.New("idempotency key already has different input")
	errClosed      = errors.New("session is closed for input")
	errUnavailable = errors.New("durable gateway store unavailable")
)

// Submission persists the exact wire event before its first network request.
// Accepted describes HTTP acceptance; turn outcomes live in History.Turns.
type Submission struct {
	Key          string          `json:"key"`
	RemoteKey    string          `json:"remote_key"`
	ReplayDigest string          `json:"replay_digest,omitempty"`
	Event        json.RawMessage `json:"event,omitempty"`
	Text         string          `json:"text,omitempty"`
	Initial      bool            `json:"initial,omitempty"`
	TargetTurn   string          `json:"target_turn,omitempty"`
	Accepted     bool            `json:"accepted"`
	Attempts     int             `json:"attempts"`
	CreatedAt    time.Time       `json:"created_at"`
	AttemptedAt  *time.Time      `json:"attempted_at,omitempty"`
	Blocked      string          `json:"blocked,omitempty"`
}

// Action binds an approval and result to immutable provider call/request identity.
type Action struct {
	Pending       openaiapi.PendingAction `json:"pending"`
	Active        bool                    `json:"active"`
	Decision      string                  `json:"decision,omitempty"`
	DecidedBy     string                  `json:"decided_by,omitempty"`
	DecidedAt     *time.Time              `json:"decided_at,omitempty"`
	Result        json.RawMessage         `json:"result,omitempty"`
	SubmissionKey string                  `json:"submission_key,omitempty"`
	OperationID   string                  `json:"operation_id,omitempty"`
	AttemptedAt   *time.Time              `json:"attempted_at,omitempty"`
	Blocked       string                  `json:"blocked,omitempty"`
}

type TimelineEvent struct {
	Sequence uint64          `json:"sequence"`
	Data     json.RawMessage `json:"data"`
}

// Record is gateway application data. No field is copied to a CRD.
type Record struct {
	Task         *Task              `json:"task,omitempty"`
	TaskMember   *TaskMemberBinding `json:"task_member,omitempty"`
	Archived     bool               `json:"archived,omitempty"`
	DeliveryHeld bool               `json:"delivery_held,omitempty"`
	Work         *Work              `json:"work,omitempty"`
	Version      int                `json:"version"`
	Namespace    string             `json:"namespace"`
	Name         string             `json:"name"`
	UID          string             `json:"uid"`
	RemoteID     string             `json:"remote_id,omitempty"`
	Closed       bool               `json:"closed"`
	// TerminalReason is a safe observation hint; Closed remains the input fence.
	TerminalReason   string            `json:"terminal_reason,omitempty"`
	InitialKey       string            `json:"initial_key,omitempty"`
	Submissions      []Submission      `json:"submissions"`
	Actions          map[string]Action `json:"actions"`
	Notes            map[string]string `json:"notes"`
	History          openaiapi.History `json:"history"`
	Sequence         uint64            `json:"sequence"`
	Events           []TimelineEvent   `json:"events"`
	RecoveryRequired bool              `json:"recovery_required"`
	LastError        string            `json:"last_error,omitempty"`
}

func addSubmission(r *Record, submission Submission) error {
	if submission.RemoteKey == "" {
		submission.RemoteKey = wireKey(r.UID, submission.Key)
	}
	if r.Closed {
		return errClosed
	}
	for _, previous := range r.Submissions {
		if previous.Key != submission.Key {
			continue
		}
		if previous.Text != submission.Text || previous.Initial != submission.Initial || previous.TargetTurn != submission.TargetTurn || string(previous.Event) != string(submission.Event) {
			return errConflict
		}
		return nil
	}
	if len(r.Submissions) >= _maxSubmissions {
		return errors.New("submission capacity reached")
	}
	r.Submissions = append(r.Submissions, submission)
	return nil
}

func actionID(a openaiapi.PendingAction) string {
	if a.Type == _functionCall {
		return a.TurnID + ":" + a.CallID
	}
	if a.Type == _computerApproval {
		return a.TurnID + ":" + a.RequestID
	}
	return "environment:" + a.EnvironmentID
}

func appendEvent(r *Record, data json.RawMessage) {
	r.Sequence++
	r.Events = append(r.Events, TimelineEvent{Sequence: r.Sequence, Data: data})
	if len(r.Events) > _maxEvents {
		r.Events = r.Events[len(r.Events)-_maxEvents:]
	}
}

func inputEvent(text string) json.RawMessage {
	raw, _ := json.Marshal(map[string]any{_typeField: "agent.session.input.message", "input": []any{map[string]any{_taskRoleField: "user", "content": []any{map[string]any{_typeField: "input_text", "text": text}}}}})
	return raw
}

func resultKey(uid, id string) string {
	sum := sha256.Sum256([]byte(uid + "/" + id))
	return fmt.Sprintf("action-%x", sum[:])
}

// wireKey prevents collisions when callers reuse convenient keys across Sessions.
func wireKey(uid, key string) string {
	sum := sha256.Sum256([]byte(uid + "/submission/" + key))
	return "msg-" + hex.EncodeToString(sum[:])
}

// submissionDigest excludes mutable delivery state and the initial-to-hosted
// event transition. Archive retains this fingerprint after erasing payloads.
func submissionDigest(submission Submission) string {
	event := ""
	if submission.Text == "" {
		event = string(submission.Event)
	}
	return digestValue([]string{submission.Text, submission.TargetTurn, event})
}

// taskFreshEffect identifies admissions and one-time delivery/creation claims.
// Observation and cleanup updates remain possible after stop.
func taskFreshEffect(before, after Record) bool {
	if after.Task != nil {
		if before.Task == nil {
			return true
		}
		if len(after.Task.Members) > len(before.Task.Members) || len(after.Task.Results) > len(before.Task.Results) {
			return true
		}
		if len(after.Task.Events) > len(before.Task.Events) && after.Task.State == _taskOpen {
			return true
		}
	}
	if after.TaskMember == nil {
		return false
	}
	if before.TaskMember == nil && after.Work != nil {
		return true
	}
	if len(after.Submissions) > len(before.Submissions) && !taskCleanupCancellation(before, after) {
		return true
	}
	for i, submission := range after.Submissions {
		if i < len(before.Submissions) && (submission.Attempts > before.Submissions[i].Attempts || before.Submissions[i].AttemptedAt == nil && submission.AttemptedAt != nil) {
			return true
		}
	}
	for id, action := range after.Actions {
		old := before.Actions[id]
		if action.Decision != old.Decision {
			return true
		}
		if old.AttemptedAt == nil && action.AttemptedAt != nil {
			return true
		}
	}
	if after.Work != nil {
		if before.Work == nil || len(after.Work.Attempts) > len(before.Work.Attempts) {
			return true
		}
		for i, attempt := range after.Work.Attempts {
			if !before.Work.Attempts[i].CreationClaimed && attempt.CreationClaimed {
				return true
			}
		}
		if len(after.Work.Events) > len(before.Work.Events) && after.Work.State == _workOpen {
			return true
		}
	}
	return false
}

func taskAuthorityAllowed(parent Record, child Record) error {
	binding := child.TaskMember
	if binding == nil || parent.Task == nil || parent.UID != binding.TaskID || parent.Namespace != child.Namespace {
		return errConflict
	}
	if parent.DeliveryHeld {
		return errRestoreHeld
	}
	if parent.Task.State != _taskOpen {
		return errClosed
	}
	for _, member := range parent.Task.Members {
		if member.ID == binding.MemberID && member.Role == binding.Role {
			if child.Work != nil && member.WorkID != child.UID {
				return errConflict
			}
			return nil
		}
	}
	return errConflict
}

func taskIdentityPreserved(before, after Record) bool {
	if before.TaskMember != nil && digestValue(before.TaskMember) != digestValue(after.TaskMember) {
		return false
	}
	if before.Task == nil {
		return true
	}
	if after.Task == nil || before.Task.ID != after.Task.ID || digestValue(before.Task.Workflow) != digestValue(after.Task.Workflow) || digestValue(before.Task.Origin) != digestValue(after.Task.Origin) {
		return false
	}
	if before.Task.State == _taskClosed && after.Task.State != _taskClosed || before.Task.State == _taskStopping && after.Task.State == _taskOpen {
		return false
	}
	if len(after.Task.Members) < len(before.Task.Members) || len(after.Task.Results) < len(before.Task.Results) || len(after.Task.Events) < len(before.Task.Events) {
		return false
	}
	for i, member := range before.Task.Members {
		if digestValue(member) != digestValue(after.Task.Members[i]) {
			return false
		}
	}
	for i, result := range before.Task.Results {
		next := after.Task.Results[i]
		result.HandoffQueued = false
		next.HandoffQueued = false
		if digestValue(result) != digestValue(next) {
			return false
		}
	}
	for i, event := range before.Task.Events {
		next := after.Task.Events[i]
		event.Queued = false
		next.Queued = false
		if digestValue(event) != digestValue(next) {
			return false
		}
	}
	return true
}

// The reserved cancellation is cleanup, never caller input. Restore holds still apply.
func taskCleanupCancellation(before, after Record) bool {
	if !after.Closed || after.TaskMember == nil || len(after.Submissions) != len(before.Submissions)+1 {
		return false
	}
	submission := after.Submissions[len(before.Submissions)]
	return submission.Key == resultKey(after.UID, "cleanup-cancel") && submission.RemoteKey == wireKey(after.UID, submission.Key) && submission.Text == "" && submission.TargetTurn == "" && !submission.Initial && string(submission.Event) == `{"type":"agent.session.input.cancel"}`
}

func copyCleanupTask(task *Task) *Task {
	if task == nil {
		return nil
	}
	copy := *task
	copy.State = _taskOpen
	return &copy
}

func validateTaskRecord(record Record) error {
	if record.TaskMember != nil && (record.TaskMember.TaskID == "" || record.TaskMember.MemberID == "" || record.TaskMember.Role == "") {
		return errConflict
	}
	if record.Task == nil {
		return nil
	}
	task := record.Task
	if task.ID != record.UID || record.Work != nil || record.TaskMember != nil || task.State != _taskOpen && task.State != _taskStopping && task.State != _taskClosed {
		return errConflict
	}
	if task.Origin.Subject == "" || task.Origin.Key == "" || task.ID != taskRecordID(record.Namespace, task.Origin.Subject, task.Origin.Key) {
		return errConflict
	}
	workflow := task.Workflow
	if workflow.Name == "" || workflow.UID == "" || workflow.Generation < 1 || workflow.ResourceVersion == "" || workflow.Digest != agentsv1alpha1.WorkflowTemplateSpecDigest(workflow.Spec) {
		return errConflict
	}
	if err := agentsv1alpha1.ValidateWorkflowTemplateSpec(workflow.Spec); err != nil {
		return errConflict
	}
	if len(task.Events) == 0 || len(task.Members) == 0 || !task.Members[0].IsCoordinator || task.Events[0].Kind != _taskStart || task.Events[0].Digest != task.Origin.Digest {
		return errConflict
	}
	if task.Origin.Digest != taskDigest(TaskInput{Workflow: workflow.Name, Text: task.Events[0].Text}) {
		return errConflict
	}
	if err := validateTaskMembers(record); err != nil {
		return err
	}
	return validateTaskEvidence(*task)
}

func validateTaskMembers(record Record) error {
	seen := make(map[string]bool)
	for i, member := range record.Task.Members {
		if seen[member.ID] || member.ID == "" || member.WorkID != taskWorkID(record.Namespace, record.UID, member.ID) || strings.TrimSpace(member.Text) == "" || member.CreatedAt.IsZero() {
			return errConflict
		}
		if _, err := taskMemberInitialText(member); err != nil {
			return errConflict
		}
		seen[member.ID] = true
		if i > 0 && (member.IsCoordinator || member.Action.SessionUID == "" || member.Action.TurnID == "" || member.Action.CallID == "") {
			return errConflict
		}
		expectedProfile := record.Task.Workflow.Spec.Coordinator.ProfileRef.Name
		found := i == 0
		for _, role := range record.Task.Workflow.Spec.Roles {
			if !member.IsCoordinator && role.Name == member.Role {
				expectedProfile = role.ProfileRef.Name
				found = true
				if !member.IsCoordinator {
					if err := validateTaskDependencies(*record.Task, role.DependsOn, member.Dependencies); err != nil {
						return errConflict
					}
				}
			}
		}
		if !found || member.Route.ID != expectedProfile || member.Profile == nil || member.Profile.Name != expectedProfile {
			return errConflict
		}
		attempt := WorkAttempt{Policy: member.Policy, Profile: member.Profile}
		synthetic := Record{Namespace: record.Namespace, Work: &Work{RouteID: member.Route.ID, Conversation: &ConversationOrigin{}}}
		if err := validateProfileProvenance(synthetic, attempt); err != nil {
			return err
		}
	}
	return nil
}

func validateTaskEvidence(task Task) error {
	events := make(map[string]bool)
	for i, event := range task.Events {
		if event.ID == "" || events[event.ID] || event.Digest == "" || event.ReceivedAt.IsZero() || event.Kind != _taskStart && event.Kind != _taskMessage && event.Kind != _taskStop {
			return errConflict
		}
		if i > 0 && (event.Kind == _taskStart || event.Digest != taskDigest([]string{event.Kind, event.Text})) {
			return errConflict
		}
		events[event.ID] = true
	}
	results := make(map[string]bool)
	for _, result := range task.Results {
		member, ok := taskMember(task, result.MemberID)
		sum := sha256.Sum256([]byte(result.Raw))
		if !ok || results[result.ID] || result.ID != "result-"+taskDigest([]string{task.ID, member.ID}) || result.Role != member.Role || result.Action.SessionUID == "" || result.Action.TurnID == "" || result.Action.CallID == "" || result.ReceivedAt.IsZero() || hex.EncodeToString(sum[:]) != result.SHA256 {
			return errConflict
		}
		var input WorkflowResultInput
		if decodeWorkflowArguments(json.RawMessage(result.Raw), &input) != nil || input.Outcome != result.Outcome || result.Verification != "reported" {
			return errConflict
		}
		if _, err := validateWorkflowResult(task, member, json.RawMessage(result.Raw), result.SourceEvidence); err != nil {
			return errConflict
		}
		results[result.ID] = true
	}
	return nil
}

// Keep one bounded cleanup receipt available inside the existing record ceiling.
func recordSizeLimit(record Record) int {
	if record.Task != nil && record.Task.State == _taskOpen || record.TaskMember != nil && !closedForPurge(record) {
		return _maxRecordBytes - 2048
	}
	return _maxRecordBytes
}
