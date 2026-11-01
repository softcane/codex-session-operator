package gateway

import (
	"encoding/json"
	"time"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
)

const (
	_taskOpen           = "open"
	_taskStopping       = "stopping"
	_taskClosed         = "closed"
	_taskCoordinator    = "coordinator"
	_taskStop           = "stop"
	_taskMessage        = "message"
	_taskOutcomeSuccess = "success"
	_maxTaskResultBytes = 256 << 10
)

// Task keeps accepted workflow configuration and evidence independently of execution cleanup.
type Task struct {
	ID         string                `json:"id"`
	State      string                `json:"state"`
	Workflow   TaskWorkflowSnapshot  `json:"workflow"`
	Origin     ConversationOrigin    `json:"origin"`
	Events     []TaskEvent           `json:"events"`
	Members    []TaskMember          `json:"members"`
	Rejections []TaskResultRejection `json:"rejections,omitempty"`
	Results    []TaskResult          `json:"results"`
	LastError  string                `json:"last_error,omitempty"`
}

type TaskWorkflowSnapshot struct {
	Name            string                              `json:"name"`
	UID             string                              `json:"uid"`
	Generation      int64                               `json:"generation"`
	ResourceVersion string                              `json:"resource_version"`
	Digest          string                              `json:"digest"`
	Spec            agentsv1alpha1.WorkflowTemplateSpec `json:"spec"`
}

type TaskEvent struct {
	ID         string    `json:"id"`
	Digest     string    `json:"digest"`
	Kind       string    `json:"kind"`
	Text       string    `json:"text,omitempty"`
	ReceivedAt time.Time `json:"received_at"`
	Queued     bool      `json:"queued"`
}

// TaskMember is a committed creation intent; its Work record may not exist yet.
type TaskMember struct {
	ID            string                     `json:"id"`
	IsCoordinator bool                       `json:"is_coordinator"`
	Role          string                     `json:"role"`
	Text          string                     `json:"text"`
	Dependencies  []string                   `json:"dependencies,omitempty"`
	WorkID        string                     `json:"work_id"`
	Route         profileRoute               `json:"route"`
	Profile       *ProfileProvenance         `json:"profile"`
	Policy        agentsv1alpha1.SessionSpec `json:"policy"`
	Action        WorkflowAction             `json:"action"`
	CreatedAt     time.Time                  `json:"created_at"`
}

type TaskMemberBinding struct {
	TaskID   string `json:"task_id"`
	MemberID string `json:"member_id"`
	Role     string `json:"role"`
}

// WorkflowAction is supplied only after authoritative root action provenance verification.
type WorkflowAction struct {
	SessionUID string `json:"session_uid"`
	TurnID     string `json:"turn_id"`
	CallID     string `json:"call_id"`
}

type TaskResult struct {
	ID             string            `json:"id"`
	MemberID       string            `json:"member_id"`
	Role           string            `json:"role"`
	Action         WorkflowAction    `json:"action"`
	Outcome        string            `json:"outcome"`
	Raw            string            `json:"raw"`
	SHA256         string            `json:"sha256"`
	ReceivedAt     time.Time         `json:"received_at"`
	SourceEvidence []json.RawMessage `json:"source_evidence,omitempty"`
	Verification   string            `json:"verification"`
	HandoffQueued  bool              `json:"handoff_queued"`
}

type TaskReceipt struct {
	TaskID     string    `json:"task_id"`
	TaskURL    string    `json:"task_url"`
	EventID    string    `json:"event_id"`
	ReceivedAt time.Time `json:"received_at"`
	Acceptance string    `json:"acceptance"`
}

type WorkflowDelegateInput struct {
	Role         string   `json:"role"`
	Text         string   `json:"text"`
	Dependencies []string `json:"dependencies,omitempty"`
}

type WorkflowResultInput struct {
	Outcome     string           `json:"outcome"`
	Outputs     []WorkflowOutput `json:"outputs"`
	References  []string         `json:"references,omitempty"`
	Limitations string           `json:"limitations,omitempty"`
}

type WorkflowOutput struct {
	Name         string          `json:"name"`
	Content      string          `json:"content"`
	Kind         string          `json:"kind,omitempty"`
	BaseRevision string          `json:"base_revision,omitempty"`
	SHA256       string          `json:"sha256,omitempty"`
	Checks       []WorkflowCheck `json:"checks,omitempty"`
	Limitations  string          `json:"limitations,omitempty"`
}

type WorkflowCheck struct {
	Command  string `json:"command"`
	Output   string `json:"output"`
	ExitCode *int   `json:"exit_code"`
	// SourceItemID points to a saved provider history item containing the observation.
	SourceItemID string `json:"source_item_id"`
}

// TaskResultRejection is a bounded observation; it never finalizes an assignment.
type TaskResultRejection struct {
	MemberID   string         `json:"member_id"`
	Action     WorkflowAction `json:"action"`
	State      string         `json:"state"`
	Reason     string         `json:"reason"`
	ReceivedAt time.Time      `json:"received_at"`
}
