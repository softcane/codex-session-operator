package gateway

import (
	"context"
	"errors"
	"slices"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ConversationLifecycle separates durable input permission from execution and
// recovery. These observations never clear claims or infer turn completion.
type ConversationLifecycle struct {
	DeliveryHeld        bool                 `json:"delivery_held"`
	ExecutionReady      bool                 `json:"execution_ready"`
	DeliveryObservation string               `json:"delivery_observation"`
	Reason              string               `json:"reason"`
	Recovery            ConversationRecovery `json:"recovery"`
	Actions             ConversationActions  `json:"actions"`
}

type ConversationRecovery struct {
	Required bool   `json:"required"`
	Reason   string `json:"reason,omitempty"`
}

type ConversationActions struct {
	Continue   bool `json:"continue"`
	Close      bool `json:"close"`
	StartFresh bool `json:"start_fresh"`
}

func (g *Gateway) conversationLifecycle(ctx context.Context, record Record, attempt WorkAttempt,
	detail ConversationDetail, child Record) (state ConversationLifecycle) {
	state = ConversationLifecycle{Reason: "Provisioning", DeliveryObservation: _deliveryRuntimeOnly}
	// Runtime holds stop delivery while Continue still accepts durable queuing.
	// Restore permission is evaluated separately before adding this observation.
	defer func() {
		if state.Actions.Continue && (!state.ExecutionReady || state.DeliveryObservation == _deliveryLocalWorkerHeld) {
			state.DeliveryHeld = true
		}
	}()
	state.DeliveryHeld = record.DeliveryHeld || child.DeliveryHeld
	deliveryErr := g.deliveryEnabled(ctx)
	if errors.Is(deliveryErr, errRestoreHeld) {
		state.DeliveryHeld = true
	}
	active := attempt.Number == len(record.Work.Attempts)
	state.Actions = g.conversationActions(ctx, record, attempt, child, state.DeliveryHeld, deliveryErr)
	state.Recovery = conversationRecovery(attempt, detail, child)
	switch {
	case state.DeliveryHeld:
		state.Reason = "RestoreReconciliationRequired"
		state.Recovery = ConversationRecovery{Required: true, Reason: state.Reason}
		return state
	case deliveryErr != nil:
		state.Reason = "DeliveryObservationUnavailable"
		return state
	case !active || record.Work.State == _workClosed:
		state.Reason = "Closed"
		return state
	case record.Work.State == _workClosing:
		state.Reason = "CleanupPending"
		return state
	case child.Closed:
		state.Reason = child.TerminalReason
		if state.Reason == "" {
			state.Reason = "SessionClosed"
		}
		return state
	case attempt.SessionUID == "":
		return state
	}
	var session agentsv1alpha1.Session
	if err := g.reader.Get(ctx, client.ObjectKey{Namespace: record.Namespace, Name: attempt.SessionName}, &session); err != nil {
		state.Reason = "SessionObservationUnavailable"
		return state
	}
	if err := checkWorkOwnership(record, attempt, &session); err != nil {
		state.Reason = "SessionIdentityMismatch"
		state.Recovery = ConversationRecovery{Required: true, Reason: state.Reason}
		return state
	}
	if session.Status.OpenAISessionID == "" {
		return state
	}
	if !spendControlReady(&session) {
		state.Reason = "SpendControlPending"
		return state
	}
	state.ExecutionReady, state.Reason = g.computeReady(ctx, &session)
	state.DeliveryObservation = g.deliveryObservation(&session)
	if state.ExecutionReady {
		state.Reason = "ExecutionReady"
	}
	return state
}

func (g *Gateway) conversationActions(ctx context.Context, record Record, attempt WorkAttempt, child Record, held bool, deliveryErr error) ConversationActions {
	var actions ConversationActions
	active := attempt.Number == len(record.Work.Attempts)
	actions.Continue = active && record.Work.State == _workOpen && !child.Closed &&
		!held && deliveryErr == nil
	actions.Close = active && record.Work.State != _workClosed && !held && deliveryErr == nil
	if active && record.Work.State == _workClosed && !held && deliveryErr == nil &&
		!slices.ContainsFunc(record.Work.Attempts, func(a WorkAttempt) bool { return !a.Cleaned }) {
		_, err := g.selectProfile(ctx, record.Namespace, record.Work.RouteID)
		actions.StartFresh = err == nil
	}
	return actions
}

func conversationRecovery(attempt WorkAttempt, detail ConversationDetail, child Record) ConversationRecovery {
	var recovery ConversationRecovery
	if detail.RecoveryRequired {
		recovery = ConversationRecovery{Required: true, Reason: "HistoryRecoveryPending"}
	}
	if attempt.CreationClaimed && attempt.SessionUID == "" {
		recovery = ConversationRecovery{Required: true, Reason: "CreationOutcomeUnknown"}
	}
	for _, submission := range child.Submissions {
		if submission.Blocked != "" && !submission.Accepted {
			recovery = ConversationRecovery{Required: true, Reason: "DeliveryAuditRequired"}
		}
	}
	for _, action := range child.Actions {
		if action.Blocked != "" {
			recovery = ConversationRecovery{Required: true, Reason: "ActionAuditRequired"}
		}
	}
	return recovery
}
