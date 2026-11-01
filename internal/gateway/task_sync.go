package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"time"
	"unicode/utf8"
)

// syncTask recovers committed member intents before creating or closing execution.
func (g *Gateway) syncTask(ctx context.Context, id string) error {
	record, err := g.store.Get(ctx, id)
	if err != nil {
		return err
	}
	if record.Task == nil {
		return errors.New("task required")
	}
	if record.DeliveryHeld {
		return errRestoreHeld
	}
	if record.Task.State == _taskClosed {
		return nil
	}
	allClosed := true
	var failures []error
	for _, member := range record.Task.Members {
		child, err := g.store.Get(ctx, member.WorkID)
		if errors.Is(err, os.ErrNotExist) {
			if record.Task.State != _taskOpen {
				continue
			}
			if err := g.materializeTaskMember(ctx, record, member); err != nil {
				failures = append(failures, err)
				allClosed = false
				continue
			}
			child, err = g.store.Get(ctx, member.WorkID)
		}
		if err != nil {
			failures = append(failures, err)
			allClosed = false
			continue
		}
		if record.Task.State != _taskOpen && child.Work.State == _workOpen {
			if err := g.store.Update(ctx, member.WorkID, func(r *Record) error { r.Work.State = _workClosing; return nil }); err != nil {
				failures = append(failures, err)
				allClosed = false
				continue
			}
		}
		if err := g.syncWork(ctx, member.WorkID); err != nil {
			failures = append(failures, err)
			allClosed = false
			continue
		}
		latest, err := g.store.Get(ctx, member.WorkID)
		if err != nil {
			failures = append(failures, err)
			allClosed = false
			continue
		}
		if latest.Work.State != _workClosed {
			allClosed = false
		}
	}
	if record.Task.State != _taskOpen {
		if allClosed {
			return g.store.Update(ctx, id, func(r *Record) error { r.Task.State = _taskClosed; r.Task.LastError = ""; return nil })
		}
		return errors.Join(failures...)
	}
	syncErr := errors.Join(append(failures, g.queueTaskEvents(ctx, record))...)
	if syncErr == nil && record.Task.LastError != "" {
		return g.store.Update(ctx, id, func(r *Record) error { r.Task.LastError = ""; return nil })
	}
	return syncErr
}

func (g *Gateway) materializeTaskMember(ctx context.Context, task Record, member TaskMember) error {
	initialText, err := taskMemberInitialText(member)
	if err != nil {
		return err
	}
	return g.store.Update(ctx, member.WorkID, func(r *Record) error {
		if r.Work != nil {
			if r.TaskMember == nil || r.TaskMember.TaskID != task.UID || r.TaskMember.MemberID != member.ID {
				return errConflict
			}
			return nil
		}
		if r.Namespace != "" || r.Closed || r.DeliveryHeld {
			return errConflict
		}
		r.Namespace = task.Namespace
		origin := ConversationOrigin{Subject: "workflow:" + task.UID, Key: member.ID, Digest: taskDigest(initialText)}
		r.Work = &Work{ID: member.WorkID, RouteID: member.Route.ID, Source: member.Route.Source, ExternalID: member.WorkID, Conversation: &origin}
		startWorkAttempt(r, member.Route)
		r.Work.Attempts[0].Policy = *member.Policy.DeepCopy()
		bindProfile(r, member.Profile)
		r.TaskMember = &TaskMemberBinding{TaskID: task.UID, MemberID: member.ID, Role: member.Role}
		event := WorkEvent{ID: "initial-" + member.ID, Digest: taskDigest(initialText), Kind: _workTask, Text: initialText, ReceivedAt: member.CreatedAt}
		return applyWorkEvent(r, member.Route, conversationMutation{WorkID: member.WorkID, Kind: _workTask}, &event)
	})
}

func (g *Gateway) queueTaskEvents(ctx context.Context, record Record) error {
	if len(record.Task.Members) == 0 {
		return errors.New("task coordinator missing")
	}
	coordinator := record.Task.Members[0]
	for _, event := range record.Task.Events {
		if event.Kind != _taskMessage || event.Queued {
			continue
		}
		if err := g.queueTaskInput(ctx, coordinator, "task-"+event.ID, event.Text, event.ReceivedAt); err != nil {
			return err
		}
		if err := g.store.Update(ctx, record.UID, func(r *Record) error {
			for i := range r.Task.Events {
				if r.Task.Events[i].ID == event.ID {
					r.Task.Events[i].Queued = true
				}
			}
			return nil
		}); err != nil {
			return err
		}
	}
	for _, result := range record.Task.Results {
		if result.HandoffQueued || result.MemberID == coordinator.ID {
			continue
		}
		text := "Saved workflow result " + result.ID + " from assignment " + result.MemberID + " (" + result.Role + ") has outcome " + result.Outcome + ". Read the exact saved evidence with workflow_read_result."
		if err := g.queueTaskInput(ctx, coordinator, "result-"+result.ID, text, result.ReceivedAt); err != nil {
			return err
		}
		if err := g.store.Update(ctx, record.UID, func(r *Record) error {
			for i := range r.Task.Results {
				if r.Task.Results[i].ID == result.ID {
					r.Task.Results[i].HandoffQueued = true
				}
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

func (g *Gateway) queueTaskInput(ctx context.Context, member TaskMember, id, text string, received time.Time) error {
	return g.store.Update(ctx, member.WorkID, func(r *Record) error {
		event := WorkEvent{ID: id, Digest: taskDigest(text), Kind: _workEvidence, Text: text, ReceivedAt: received}
		return applyWorkEvent(r, member.Route, conversationMutation{WorkID: member.WorkID, Kind: _workEvidence}, &event)
	})
}

// taskMemberInitialText keeps runtime evidence relationships in durable gateway input.
func taskMemberInitialText(member TaskMember) (string, error) {
	text := member.Text
	if len(member.Dependencies) > 0 {
		raw, _ := json.Marshal(member.Dependencies)
		text += "\n\nExact prerequisite saved result IDs: " + string(raw) + ". Read these with workflow_read_result before using their evidence."
	}
	if !utf8.ValidString(text) || len(text) > _maxInputBytes {
		return "", workflowInputError("complete assignment input including prerequisite IDs must fit 256 KiB")
	}
	return text, nil
}
