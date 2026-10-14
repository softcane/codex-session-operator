package gateway

import (
	"errors"
	"time"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
)

const (
	_workAnnotation  = "agents.softcane.io/work-source"
	_workOpen        = "open"
	_workClosing     = "closing"
	_workClosed      = "closed"
	_workTask        = "task"
	_workEvidence    = "evidence"
	_workClose       = "close"
	_workRestart     = "restart"
	_maxWorkRoutes   = 256
	_maxWorkAttempts = 16
)

var errWorkMissing = errors.New("work is not registered")

// WorkRoute binds trusted source routing to a tested Agent/Environment pair.
// Incoming events cannot select namespaces, runtime images, or Session overrides.
type WorkRoute struct {
	ID           string                              `json:"id"`
	Namespace    string                              `json:"namespace"`
	Source       string                              `json:"source"`
	Agent        string                              `json:"agent"`
	Environment  string                              `json:"environment"`
	Vaults       []string                            `json:"vaults,omitempty"`
	SpendControl *agentsv1alpha1.SessionSpendControl `json:"spend_control"`
}

// WorkInput is the normalized connector/UI event. Route configuration owns policy.
type WorkInput struct {
	RouteID string `json:"route_id"`
	WorkID  string `json:"work_id"`
	EventID string `json:"event_id"`
	Kind    string `json:"kind"`
	Text    string `json:"text,omitempty"`
}

// WorkEvent retains the canonical delivery identity and exact accepted input.
type WorkEvent struct {
	ID         string    `json:"id"`
	Digest     string    `json:"digest"`
	Kind       string    `json:"kind"`
	Text       string    `json:"text,omitempty"`
	ReceivedAt time.Time `json:"received_at"`
	Attempt    int       `json:"attempt"`
	Queued     bool      `json:"queued"`
}

// WorkAttempt freezes selection and ownership before any Kubernetes create.
type WorkAttempt struct {
	Number          int                        `json:"number"`
	ConversationID  string                     `json:"conversation_id,omitempty"`
	SessionName     string                     `json:"session_name"`
	SessionUID      string                     `json:"session_uid,omitempty"`
	Phase           string                     `json:"phase,omitempty"`
	Policy          agentsv1alpha1.SessionSpec `json:"policy"`
	CreationClaimed bool                       `json:"creation_claimed"`
	InitialQueued   bool                       `json:"initial_queued"`
	InputsArchived  bool                       `json:"inputs_archived,omitempty"`
	Cleaned         bool                       `json:"cleaned"`
}

// ConversationOrigin binds first-message retries independently of route changes.
// Subject scopes creation keys; namespace authorization governs later access.
type ConversationOrigin struct {
	Subject string `json:"subject"`
	Key     string `json:"key"`
	Digest  string `json:"digest"`
	Title   string `json:"title,omitempty"`
}

// Work is application data and a replay tombstone; it is never stored in a CRD.
type Work struct {
	Conversation *ConversationOrigin `json:"conversation,omitempty"`
	ID           string              `json:"id"`
	RouteID      string              `json:"route_id"`
	Source       string              `json:"source"`
	ExternalID   string              `json:"work_id"`
	State        string              `json:"state"`
	Attempts     []WorkAttempt       `json:"attempts"`
	Events       []WorkEvent         `json:"events"`
	LastError    string              `json:"last_error,omitempty"`
}
