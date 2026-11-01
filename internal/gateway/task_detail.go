package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"time"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const _taskObservationPending = "pending"

// taskDetail projects identities and observations, leaving full evidence at result/history URLs.
func (g *Gateway) taskDetail(ctx context.Context, record Record) (any, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if record.Task == nil {
		return nil, errors.New("task required")
	}
	task := record.Task
	members := make([]map[string]any, 0, len(task.Members))
	results := make([]map[string]any, 0, len(task.Results))
	for _, result := range task.Results {
		results = append(results, map[string]any{"id": result.ID, "member_id": result.MemberID, "role": result.Role, "outcome": result.Outcome, "sha256": result.SHA256, "bytes": len(result.Raw), "received_at": result.ReceivedAt, "verification": result.Verification, "handoff_queued": result.HandoffQueued, "url": "/v1/namespaces/" + record.Namespace + "/tasks/" + task.ID + "/results/" + result.ID})
	}
	for _, member := range task.Members {
		detail := map[string]any{"id": member.ID, "work_id": member.WorkID, "role": member.Role, "is_coordinator": member.IsCoordinator, "result_state": "missing", "provisioning": _taskObservationPending, "connection": "unknown", "cleanup": _taskObservationPending, "provider_turns": []map[string]string{}}
		detail["profile"] = copyProfileProvenance(member.Profile)
		detail["accepted_policy"] = map[string]any{"agent_ref": member.Policy.AgentRef, "environment_ref": member.Policy.EnvironmentRef, "vault_refs": member.Policy.VaultRefs, "spend_control": member.Policy.SpendControl}
		for _, result := range task.Results {
			if result.MemberID == member.ID {
				detail["result_state"] = "final"
				detail["result_id"] = result.ID
				detail["outcome"] = result.Outcome
			}
		}
		for _, rejection := range task.Rejections {
			if rejection.MemberID == member.ID {
				detail["last_result_rejection"] = rejection
			}
		}
		child, err := g.store.GetInNamespace(ctx, record.Namespace, member.WorkID)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
		if child.Work == nil || len(child.Work.Attempts) == 0 {
			if task.State != _taskOpen {
				detail["provisioning"] = "not_started"
				detail["cleanup"] = "not_required"
			}
			members = append(members, detail)
			continue
		}
		attempt := child.Work.Attempts[len(child.Work.Attempts)-1]
		detail["state"] = child.Work.State
		detail["session_name"], detail["session_uid"] = attempt.SessionName, attempt.SessionUID
		detail["conversation_id"] = attempt.ConversationID
		detail["conversation_url"] = conversationURL(record.Namespace, attempt.ConversationID)
		detail["phase"], detail["last_error"] = attempt.Phase, child.Work.LastError
		detail["cleaned"] = attempt.Cleaned
		if attempt.Cleaned {
			detail["cleanup"] = "confirmed"
		}
		if attempt.CreationClaimed {
			detail["provisioning"] = "claimed"
		}
		if attempt.SessionUID != "" {
			detail["provisioning"] = "created"
			history, historyErr := g.store.GetInNamespace(ctx, record.Namespace, attempt.SessionUID)
			if historyErr != nil && !errors.Is(historyErr, os.ErrNotExist) {
				return nil, historyErr
			}
			detail["provider_status"] = history.History.Status
			detail["provider_turns"] = taskTurnSummaries(history.History.Turns)
			detail["recovery_required"] = history.RecoveryRequired || child.DeliveryHeld || history.DeliveryHeld
			if history.LastError != "" {
				detail["last_error"] = history.LastError
			}
		}
		if attempt.Cleaned {
			detail["connection"] = "closed"
			members = append(members, detail)
			continue
		}
		var session agentsv1alpha1.Session
		err = g.reader.Get(ctx, client.ObjectKey{Namespace: record.Namespace, Name: attempt.SessionName}, &session)
		switch {
		case apierrors.IsNotFound(err):
			detail["observation_unavailable"] = true
			detail["observation_reason"] = "SessionMissing"
		case err != nil:
			detail["observation_unavailable"] = true
			detail["observation_reason"] = "SessionObservationUnavailable"
		case checkWorkOwnership(child, attempt, &session) != nil:
			detail["observation_unavailable"] = true
			detail["observation_reason"] = "SessionIdentityMismatch"
		default:
			detail["session_uid"] = string(session.UID)
			detail["session_url"] = "/v1/namespaces/" + record.Namespace + "/sessions/" + session.Name
			detail["provider_session_id"] = session.Status.OpenAISessionID
			detail["phase"] = sessionDisplayPhase(&session)
			ready, reason := g.computeReady(ctx, &session)
			detail["execution_ready"], detail["connection_reason"] = ready, reason
			detail["connection"] = _taskObservationPending
			if ready {
				detail["connection"] = "ready"
			}
			detail["delivery_observation"] = g.deliveryObservation(&session)
		}
		members = append(members, detail)
	}
	return map[string]any{"id": task.ID, "state": task.State, "workflow": map[string]any{"name": task.Workflow.Name, "uid": task.Workflow.UID, "generation": task.Workflow.Generation, "digest": task.Workflow.Digest}, "members": members, "results": results, "last_error": task.LastError, "delivery_held": record.DeliveryHeld}, nil
}

func taskTurnSummaries(turns []json.RawMessage) []map[string]string {
	summaries := make([]map[string]string, 0, len(turns))
	for _, raw := range turns {
		var turn struct {
			ID         string `json:"id"`
			Status     string `json:"status"`
			SubagentID string `json:"subagent_id"`
			SessionID  string `json:"session_id"`
			AgentID    string `json:"agent_id"`
		}
		if json.Unmarshal(raw, &turn) != nil {
			continue
		}
		attribution := "unknown"
		if turn.SubagentID != "" {
			attribution = "subagent"
		}
		summaries = append(summaries, map[string]string{"id": turn.ID, "status": turn.Status, "subagent_id": turn.SubagentID, "session_id": turn.SessionID, "agent_id": turn.AgentID, "attribution": attribution})
	}
	return summaries
}
