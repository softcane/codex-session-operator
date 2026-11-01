package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	"github.com/softcane/codex-session-operator/internal/openaiapi"
)

func isWorkflowFunction(name string) bool {
	switch name {
	case "workflow_delegate", "workflow_result", "workflow_read_result":
		return true
	default:
		return false
	}
}

// A function call is authoritative only for the exact saved parent turn. Missing
// attribution is uncertainty, not permission to act as the Session's parent.
func verifyWorkflowRoot(history openaiapi.History, action openaiapi.PendingAction) error {
	if history.ID == "" || action.Type != _functionCall || action.TurnID == "" || action.CallID == "" {
		return errConflict
	}
	matches := 0
	for _, raw := range history.Turns {
		var turn struct {
			ID         string          `json:"id"`
			SessionID  string          `json:"session_id"`
			SubagentID json.RawMessage `json:"subagent_id"`
		}
		if json.Unmarshal(raw, &turn) != nil {
			return errUnavailable
		}
		if turn.ID != action.TurnID {
			continue
		}
		if turn.SessionID == "" || len(turn.SubagentID) == 0 {
			return errUnavailable
		}
		if turn.SessionID != history.ID || string(turn.SubagentID) != "null" {
			return errConflict
		}
		matches++
	}
	if matches != 1 {
		return errUnavailable
	}
	return nil
}

func (g *Gateway) taskMemberInputOpen(ctx context.Context, namespace string, binding *TaskMemberBinding) error {
	if binding == nil {
		return nil
	}
	record, err := g.lookupTask(ctx, namespace, binding.TaskID)
	if err != nil {
		return err
	}
	if record.DeliveryHeld {
		return errRestoreHeld
	}
	if record.Task.State != _taskOpen {
		return errClosed
	}
	member, found := taskMember(*record.Task, binding.MemberID)
	if !found || member.Role != binding.Role {
		return errConflict
	}
	return nil
}

func (g *Gateway) sessionTaskBinding(ctx context.Context, session *agentsv1alpha1.Session) (*TaskMemberBinding, error) {
	record, found, err := g.workRecord(ctx, session)
	if err != nil || !found || record.TaskMember == nil {
		return nil, err
	}
	if record.Work == nil || len(record.Work.Attempts) != 1 {
		return nil, errConflict
	}
	if err := checkWorkOwnership(record, record.Work.Attempts[0], session); err != nil {
		return nil, err
	}
	binding := *record.TaskMember
	return &binding, nil
}

func (g *Gateway) processWorkflowFunction(
	ctx context.Context, uid, id string, pending openaiapi.PendingAction, action Action,
) error {
	record, err := g.store.Get(ctx, uid)
	if err != nil {
		return err
	}
	if record.TaskMember == nil {
		return g.saveResult(ctx, uid, id, toolResult(pending, false, "Workflow caller identity is not verified"))
	}
	if err := verifyWorkflowRoot(record.History, pending); err != nil {
		if errors.Is(err, errConflict) {
			return g.saveResult(ctx, uid, id, toolResult(pending, false, "Workflow caller is not the verified Session parent"))
		}
		return err
	}
	if err := g.taskMemberInputOpen(ctx, record.Namespace, record.TaskMember); err != nil {
		return err
	}
	if len(action.Result) != 0 {
		return g.saveResult(ctx, uid, id, action.Result)
	}
	if err := g.store.Update(ctx, uid, func(r *Record) error {
		if stoppedRecord(*r) {
			return errClosed
		}
		current := r.Actions[id]
		if !current.Active {
			return errConflict
		}
		current.OperationID = resultKey(uid, id)
		if current.AttemptedAt == nil {
			now := time.Now().UTC()
			current.AttemptedAt = &now
		}
		r.Actions[id] = current
		return nil
	}); err != nil {
		return err
	}
	provenance := WorkflowAction{SessionUID: uid, TurnID: pending.TurnID, CallID: pending.CallID}
	output, err := g.workflowTool(ctx, provenance, pending.Name, pending.Arguments)
	if err != nil {
		if errors.Is(err, errWorkflowInput) {
			return g.saveResult(ctx, uid, id, toolResult(pending, false, err.Error()))
		}
		if errors.Is(err, errConflict) {
			return g.saveResult(ctx, uid, id, toolResult(pending, false, "Workflow request conflicts with saved work or evidence"))
		}
		return err
	}
	if len(output) > _maxInputBytes {
		return errors.New("workflow output exceeds gateway limit")
	}
	return g.saveResult(ctx, uid, id, toolResult(pending, true, output))
}
