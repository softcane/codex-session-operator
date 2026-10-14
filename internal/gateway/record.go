package gateway

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/softcane/codex-session-operator/internal/openaiapi"
)

const (
	_noneMode          = "none"
	_gatewayNoneStatus = "gateway.none"
	_echoFunction      = "gateway_echo"
	_noteFunction      = "gateway_record_note"
	_historyField      = "history"
	_computerApproval  = "computer_use_approval_request"
	_functionCall      = "function_call"
	_approve           = "approve"
	_deny              = "deny"
	_typeField         = "type"
)

const (
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
	Key         string          `json:"key"`
	RemoteKey   string          `json:"remote_key"`
	Event       json.RawMessage `json:"event,omitempty"`
	Text        string          `json:"text,omitempty"`
	Initial     bool            `json:"initial,omitempty"`
	TargetTurn  string          `json:"target_turn,omitempty"`
	Accepted    bool            `json:"accepted"`
	Attempts    int             `json:"attempts"`
	CreatedAt   time.Time       `json:"created_at"`
	AttemptedAt *time.Time      `json:"attempted_at,omitempty"`
	Blocked     string          `json:"blocked,omitempty"`
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
	Archived         bool              `json:"archived,omitempty"`
	DeliveryHeld     bool              `json:"delivery_held,omitempty"`
	Issue            *IssueSource      `json:"issue,omitempty"`
	Work             *Work             `json:"work,omitempty"`
	Version          int               `json:"version"`
	Namespace        string            `json:"namespace"`
	Name             string            `json:"name"`
	UID              string            `json:"uid"`
	RemoteID         string            `json:"remote_id,omitempty"`
	Closed           bool              `json:"closed"`
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
	raw, _ := json.Marshal(map[string]any{_typeField: "agent.session.input.message", "input": []any{map[string]any{"role": "user", "content": []any{map[string]any{_typeField: "input_text", "text": text}}}}})
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
