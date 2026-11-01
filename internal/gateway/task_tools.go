package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

// workflowTool accepts authority from the dispatcher, never from tool arguments.
func (g *Gateway) workflowTool(ctx context.Context, action WorkflowAction, name string, args json.RawMessage) (string, error) {
	if action.SessionUID == "" || action.TurnID == "" || action.CallID == "" {
		return "", errors.New("trusted workflow action required")
	}
	session, err := g.store.Get(ctx, action.SessionUID)
	if err != nil {
		return "", err
	}
	binding := session.TaskMember
	if binding == nil {
		binding, err = g.workflowSessionBinding(ctx, session.Namespace, action.SessionUID)
		if err != nil {
			return "", err
		}
	}
	record, err := g.lookupTask(ctx, session.Namespace, binding.TaskID)
	if err != nil {
		return "", err
	}
	member, ok := taskMember(*record.Task, binding.MemberID)
	if !ok {
		return "", errConflict
	}
	if name == "workflow_read_result" {
		var input struct {
			ResultID string `json:"result_id"`
		}
		if err := decodeWorkflowArguments(args, &input); err != nil {
			return "", err
		}
		result, ok := taskResult(*record.Task, input.ResultID)
		if !ok {
			return "", workflowInputError("result not saved in this task")
		}
		return result.Raw, nil
	}
	switch name {
	case "workflow_delegate":
		if !member.IsCoordinator {
			return "", workflowInputError("only coordinator may delegate")
		}
		return g.delegateTask(ctx, record, action, args)
	case "workflow_result":
		return g.saveTaskResult(ctx, record, member, session, action, args)
	default:
		return "", workflowInputError("unknown workflow tool")
	}
}

func (g *Gateway) workflowSessionBinding(ctx context.Context, namespace, uid string) (*TaskMemberBinding, error) {
	records, err := g.activeRecords(ctx)
	if err != nil {
		return nil, err
	}
	for _, record := range records {
		if record.Namespace != namespace || record.Work == nil || record.TaskMember == nil {
			continue
		}
		for _, attempt := range record.Work.Attempts {
			if attempt.SessionUID == uid {
				copy := *record.TaskMember
				return &copy, nil
			}
		}
	}
	return nil, errors.New("session is not a workflow member")
}

func (g *Gateway) delegateTask(ctx context.Context, record Record, action WorkflowAction, args json.RawMessage) (string, error) {
	var input WorkflowDelegateInput
	if err := decodeWorkflowArguments(args, &input); err != nil {
		return "", err
	}
	if strings.TrimSpace(input.Text) == "" {
		return "", workflowInputError("assignment text required")
	}
	key := taskDigest(action)
	id := taskMemberID(record.UID, key)
	digest := taskDigest(input)
	if previous, ok := taskMember(*record.Task, id); ok {
		if taskDigest(WorkflowDelegateInput{Role: previous.Role, Text: previous.Text, Dependencies: previous.Dependencies}) != digest {
			return "", errConflict
		}
		return workflowMemberReceipt(previous)
	}
	if err := g.taskAdmissionOpen(ctx, record); err != nil {
		return "", err
	}
	var profile, instructions string
	var dependsOn []string
	for _, role := range record.Task.Workflow.Spec.Roles {
		if role.Name == input.Role {
			profile = role.ProfileRef.Name
			instructions = role.Instructions
			dependsOn = role.DependsOn
			break
		}
	}
	if profile == "" {
		return "", workflowInputError("role not configured")
	}
	if err := validateTaskDependencies(*record.Task, dependsOn, input.Dependencies); err != nil {
		return "", err
	}
	selection, err := g.selectProfile(ctx, record.Namespace, profile)
	if err != nil {
		return "", err
	}
	member := TaskMember{ID: id, Role: input.Role, Text: input.Text, Dependencies: input.Dependencies, WorkID: taskWorkID(record.Namespace, record.UID, id), Route: selection.route, Profile: selection.provenance, Action: action, CreatedAt: time.Now().UTC()}
	if _, err := taskMemberInitialText(member); err != nil {
		return "", err
	}
	member.Policy = workPolicy(selection.route, member.WorkID, 1)
	if err := g.workflowMemberPolicy(ctx, record.Namespace, &member, instructions, record.Task.Workflow.Spec); err != nil {
		return "", err
	}
	err = g.store.Update(ctx, record.UID, func(r *Record) error {
		if previous, ok := taskMember(*r.Task, id); ok {
			if taskDigest(WorkflowDelegateInput{Role: previous.Role, Text: previous.Text, Dependencies: previous.Dependencies}) != digest {
				return errConflict
			}
			member = previous
			return nil
		}
		if r.Task.State != _taskOpen {
			return errClosed
		}
		if r.DeliveryHeld {
			return errRestoreHeld
		}
		if err := validateTaskDependencies(*r.Task, dependsOn, input.Dependencies); err != nil {
			return err
		}
		r.Task.Members = append(r.Task.Members, member)
		return nil
	})
	if err != nil {
		return "", err
	}
	return workflowMemberReceipt(member)
}

func validateTaskDependencies(task Task, roles, ids []string) error {
	seen := map[string]bool{}
	covered := map[string]bool{}
	for _, id := range ids {
		if seen[id] {
			return workflowInputError("duplicate dependency result")
		}
		seen[id] = true
		result, ok := taskResult(task, id)
		if !ok || result.Outcome != _taskOutcomeSuccess {
			return workflowInputError("dependency requires an explicit saved successful result")
		}
		if producer, ok := taskMember(task, result.MemberID); ok && !producer.IsCoordinator {
			covered[producer.Role] = true
		}
	}
	for _, role := range roles {
		if !covered[role] {
			return workflowInputError("required role dependency result missing")
		}
	}
	return nil
}

func (g *Gateway) saveTaskResult(ctx context.Context, record Record, member TaskMember, session Record, action WorkflowAction, args json.RawMessage) (string, error) {
	id := "result-" + taskDigest([]string{record.UID, member.ID})
	if previous, ok := taskResult(*record.Task, id); ok {
		if previous.Action != action || !sameJSON(json.RawMessage(previous.Raw), args) {
			return "", errConflict
		}
		return workflowResultReceipt(previous)
	}
	// A concurrent replica may already have committed this result. Defer both
	// validation and admission errors until the atomic replay check below.
	input, validationErr := validateWorkflowResult(*record.Task, member, args, session.History.Items)
	admissionErr := g.taskAdmissionOpen(ctx, record)
	sum := sha256.Sum256(args)
	result := TaskResult{ID: id, MemberID: member.ID, Role: member.Role, Action: action, Outcome: input.Outcome, Verification: "reported", SourceEvidence: workflowSourceEvidence(input, session.History.Items), Raw: string(args), SHA256: hex.EncodeToString(sum[:]), ReceivedAt: time.Now().UTC()}
	err := g.store.Update(ctx, record.UID, func(r *Record) error {
		if previous, ok := taskResult(*r.Task, id); ok {
			if previous.Action != action || !sameJSON(json.RawMessage(previous.Raw), args) {
				return errConflict
			}
			result = previous
			return nil
		}
		if validationErr != nil {
			return validationErr
		}
		if admissionErr != nil {
			return admissionErr
		}
		if r.Task.State != _taskOpen {
			return errClosed
		}
		if r.DeliveryHeld {
			return errRestoreHeld
		}
		if _, err := validateWorkflowResult(*r.Task, member, args, session.History.Items); err != nil {
			return err
		}
		r.Task.Results = append(r.Task.Results, result)
		return nil
	})
	if err != nil {
		if errors.Is(err, errWorkflowInput) {
			if saveErr := g.recordTaskResultRejection(ctx, record.UID, member.ID, action, err); saveErr != nil {
				return "", saveErr
			}
		}
		return "", err
	}
	return workflowResultReceipt(result)
}

func workflowJSON(value any) (string, error) {
	raw, err := json.Marshal(value)
	return string(raw), err
}

func workflowMemberReceipt(member TaskMember) (string, error) {
	return workflowJSON(map[string]string{"member_id": member.ID, _taskRoleField: member.Role, "conversation_record_id": member.WorkID})
}
func workflowResultReceipt(result TaskResult) (string, error) {
	return workflowJSON(map[string]string{"result_id": result.ID, "outcome": result.Outcome, "sha256": result.SHA256})
}
func workflowSourceEvidence(input WorkflowResultInput, items []json.RawMessage) []json.RawMessage {
	wanted := map[string]bool{}
	for _, output := range input.Outputs {
		for _, check := range output.Checks {
			wanted[check.SourceItemID] = true
		}
	}
	var evidence []json.RawMessage
	for _, raw := range items {
		var item struct {
			ID string `json:"id"`
		}
		if json.Unmarshal(raw, &item) == nil && wanted[item.ID] {
			evidence = append(evidence, append(json.RawMessage(nil), raw...))
		}
	}
	return evidence
}

func (g *Gateway) taskAdmissionOpen(ctx context.Context, record Record) error {
	if record.Task.State != _taskOpen {
		return errClosed
	}
	if record.DeliveryHeld {
		return errRestoreHeld
	}
	return g.deliveryEnabled(ctx)
}
func (g *Gateway) recordTaskResultRejection(ctx context.Context, taskID, memberID string, action WorkflowAction, cause error) error {
	return g.store.Update(ctx, taskID, func(r *Record) error {
		if r.Task.State != _taskOpen {
			return errClosed
		}
		if r.DeliveryHeld {
			return errRestoreHeld
		}
		observation := TaskResultRejection{MemberID: memberID, Action: action, State: "incomplete", Reason: cause.Error(), ReceivedAt: time.Now().UTC()}
		for i, previous := range r.Task.Rejections {
			if previous.MemberID == memberID {
				if previous.Action == action && previous.Reason == observation.Reason {
					return nil
				}
				r.Task.Rejections[i] = observation
				return nil
			}
		}
		r.Task.Rejections = append(r.Task.Rejections, observation)
		return nil
	})
}
