package gateway

import (
	"context"
	"errors"
	"net/http"
	"os"
	"strings"
	"time"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	"k8s.io/apimachinery/pkg/util/validation"
)

type TaskInput struct {
	Workflow string `json:"workflow"`
	Text     string `json:"text"`
}

func taskRecordID(namespace, subject, key string) string {
	return "task-" + taskDigest([]string{namespace, subject, key})
}
func taskMemberID(taskID, key string) string { return "member-" + taskDigest([]string{taskID, key}) }
func taskWorkID(namespace, taskID, memberID string) string {
	return conversationRecordID(namespace, "workflow:"+taskID, memberID)
}
func taskEventID(taskID, subject, key string) string {
	return "event-" + taskDigest([]string{taskID, subject, key})
}

func (g *Gateway) createTask(ctx context.Context, scope requestScope, key string, input TaskInput) (Record, TaskEvent, error) {
	id := taskRecordID(scope.Namespace, scope.Subject, key)
	digest := taskDigest(input)
	if current, err := g.store.GetInNamespace(ctx, scope.Namespace, id); err == nil {
		if current.Task == nil || current.Task.Origin.Digest != digest {
			return Record{}, TaskEvent{}, errConflict
		}
		return current, current.Task.Events[0], nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return Record{}, TaskEvent{}, err
	}
	if len(validation.IsDNS1123Subdomain(input.Workflow)) != 0 || strings.TrimSpace(input.Text) == "" || len(input.Text) > _maxInputBytes {
		return Record{}, TaskEvent{}, workflowInputError("valid workflow and task text required")
	}
	template, err := g.workflow(ctx, scope.Namespace, input.Workflow)
	if err != nil {
		return Record{}, TaskEvent{}, err
	}
	selection, err := g.selectProfile(ctx, scope.Namespace, template.Spec.Coordinator.ProfileRef.Name)
	if err != nil {
		return Record{}, TaskEvent{}, err
	}
	if err := g.deliveryEnabled(ctx); err != nil {
		return Record{}, TaskEvent{}, err
	}
	event := TaskEvent{ID: taskEventID(id, scope.Subject, key), Digest: digest, Kind: _taskStart, Text: input.Text, ReceivedAt: time.Now().UTC()}
	memberID := taskMemberID(id, "coordinator")
	member := TaskMember{ID: memberID, IsCoordinator: true, Role: _taskCoordinator, Text: input.Text, WorkID: taskWorkID(scope.Namespace, id, memberID), Route: selection.route, Profile: selection.provenance, CreatedAt: event.ReceivedAt}
	if _, err := taskMemberInitialText(member); err != nil {
		return Record{}, TaskEvent{}, err
	}
	member.Policy = workPolicy(selection.route, member.WorkID, 1)
	if err := g.workflowMemberPolicy(ctx, scope.Namespace, &member, template.Spec.Coordinator.Instructions, template.Spec); err != nil {
		return Record{}, TaskEvent{}, err
	}
	err = g.store.Update(ctx, id, func(record *Record) error {
		if record.Task != nil {
			if record.Task.Origin.Digest != digest {
				return errConflict
			}
			event = record.Task.Events[0]
			return nil
		}
		if record.Namespace != "" || record.DeliveryHeld || record.Closed {
			return errConflict
		}
		record.Namespace = scope.Namespace
		record.Task = &Task{ID: id, State: _taskOpen, Origin: ConversationOrigin{Subject: scope.Subject, Key: key, Digest: digest}, Workflow: TaskWorkflowSnapshot{Name: template.Name, UID: string(template.UID), Generation: template.Generation, ResourceVersion: template.ResourceVersion, Digest: agentsv1alpha1.WorkflowTemplateSpecDigest(template.Spec), Spec: template.Spec}, Events: []TaskEvent{event}, Members: []TaskMember{member}}
		return nil
	})
	if err != nil {
		return Record{}, TaskEvent{}, err
	}
	record, err := g.store.GetInNamespace(ctx, scope.Namespace, id)
	return record, event, err
}

func (g *Gateway) lookupTask(ctx context.Context, namespace, id string) (Record, error) {
	record, err := g.store.GetInNamespace(ctx, namespace, id)
	if err != nil {
		return Record{}, err
	}
	if record.Task == nil {
		return Record{}, os.ErrNotExist
	}
	return record, nil
}

func (g *Gateway) acceptTaskMutation(ctx context.Context, scope requestScope, id, kind, key, text string) (Record, TaskEvent, error) {
	event := TaskEvent{ID: taskEventID(id, scope.Subject, key), Kind: kind, Text: text, Digest: taskDigest([]string{kind, text}), ReceivedAt: time.Now().UTC()}
	current, err := g.lookupTask(ctx, scope.Namespace, id)
	if err != nil {
		return Record{}, TaskEvent{}, err
	}
	for _, previous := range current.Task.Events {
		if previous.ID == event.ID {
			if previous.Digest != event.Digest {
				return Record{}, TaskEvent{}, errConflict
			}
			return current, previous, nil
		}
	}
	if kind != _taskStop {
		if strings.TrimSpace(text) == "" || len(text) > _maxInputBytes {
			return Record{}, TaskEvent{}, workflowInputError("valid workflow and task text required")
		}
		if err := g.deliveryEnabled(ctx); err != nil {
			return Record{}, TaskEvent{}, err
		}
	}
	err = g.store.Update(ctx, id, func(record *Record) error {
		if record.Task == nil || record.Namespace != scope.Namespace {
			return os.ErrNotExist
		}
		for _, previous := range record.Task.Events {
			if previous.ID == event.ID {
				if previous.Digest != event.Digest {
					return errConflict
				}
				event = previous
				return nil
			}
		}
		if record.DeliveryHeld {
			return errRestoreHeld
		}
		if record.Task.State != _taskOpen {
			return errClosed
		}
		if kind != _taskStop && len(record.Task.Events) >= _maxSubmissions {
			return errors.New("task input capacity reached")
		}
		if kind == _taskStop {
			record.Task.State = _taskStopping
		} else if kind != _taskMessage {
			return errConflict
		}
		record.Task.Events = append(record.Task.Events, event)
		return nil
	})
	if err != nil {
		return Record{}, TaskEvent{}, err
	}
	record, err := g.lookupTask(ctx, scope.Namespace, id)
	return record, event, err
}

func (g *Gateway) serveTasks(w http.ResponseWriter, r *http.Request, parts []string, scope requestScope) bool {
	if len(parts) < 4 || parts[3] != "tasks" {
		return false
	}
	key := r.Header.Get("Idempotency-Key")
	if r.Method == http.MethodPost && !validKey(key) {
		http.Error(w, "Idempotency-Key required", http.StatusBadRequest)
		return true
	}
	if len(parts) == 4 && r.Method == http.MethodPost {
		var input TaskInput
		if err := decodeConversation(w, r, &input); err != nil {
			http.Error(w, "Invalid task input", http.StatusBadRequest)
			return true
		}
		record, event, err := g.createTask(r.Context(), scope, key, input)
		if err != nil {
			writeTaskError(w, err)
		} else {
			writeTaskReceipt(w, record, event)
		}
		return true
	}
	if len(parts) < 5 {
		methodError(w)
		return true
	}
	if r.Method == http.MethodGet {
		record, err := g.lookupTask(r.Context(), scope.Namespace, parts[4])
		if err != nil {
			writeTaskError(w, err)
			return true
		}
		if len(parts) == 5 {
			detail, err := g.taskDetail(r.Context(), record)
			if err != nil {
				writeTaskError(w, err)
			} else {
				writeJSON(w, http.StatusOK, detail)
			}
			return true
		}
		if len(parts) == 7 && parts[5] == "results" {
			result, ok := taskResult(*record.Task, parts[6])
			if !ok {
				http.NotFound(w, r)
			} else {
				writeJSON(w, http.StatusOK, result)
			}
			return true
		}
	}
	if len(parts) == 6 && r.Method == http.MethodPost {
		kind := parts[5]
		var input struct {
			Text string `json:"text"`
		}
		var err error
		switch kind {
		case _taskStop:
			err = decodeConversation(w, r, &struct{}{})
		case _messagesSegment:
			kind = _taskMessage
			err = decodeConversation(w, r, &input)
		default:
			http.NotFound(w, r)
			return true
		}
		if err != nil {
			http.Error(w, "Invalid task input", http.StatusBadRequest)
			return true
		}
		record, event, err := g.acceptTaskMutation(r.Context(), scope, parts[4], kind, key, input.Text)
		if err != nil {
			writeTaskError(w, err)
		} else {
			writeTaskReceipt(w, record, event)
		}
		return true
	}
	methodError(w)
	return true
}

func writeTaskReceipt(w http.ResponseWriter, record Record, event TaskEvent) {
	writeJSON(w, http.StatusAccepted, TaskReceipt{TaskID: record.UID, TaskURL: "/v1/namespaces/" + record.Namespace + "/tasks/" + record.UID, EventID: event.ID, ReceivedAt: event.ReceivedAt, Acceptance: "committed"})
}

func writeTaskError(w http.ResponseWriter, err error) {
	if errors.Is(err, errWorkflowInput) {
		http.Error(w, "Invalid task input", http.StatusBadRequest)
		return
	}
	writeConversationError(w, err)
}
