package gateway

import (
	"context"
	"errors"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// conversationObservation is the live Kubernetes state of an active attempt.
type conversationObservation struct {
	SessionURL             string
	Phase                  string
	LastError              string
	ObservationUnavailable bool
}

func applyWorkEvent(record *Record, route profileRoute, input conversationMutation, event *WorkEvent) error {
	if record.Work == nil || record.Work.Conversation == nil {
		return errConflict
	}
	work := record.Work
	for _, previous := range work.Events {
		if previous.ID != event.ID {
			continue
		}
		if previous.Digest != event.Digest {
			return errConflict
		}
		*event = previous
		return nil
	}
	if record.Namespace != route.Namespace || work.RouteID != route.ID ||
		work.Source != route.Source || work.ExternalID != input.WorkID {
		return errConflict
	}
	if len(work.Events) >= _maxSubmissions {
		return errors.New("conversation input capacity reached")
	}
	switch input.Kind {
	case _workTask, _workEvidence:
		if work.State != _workOpen {
			return errClosed
		}
	case _workClose:
		if work.State == _workOpen {
			work.State = _workClosing
		}
	case _workRestart:
		if work.State != _workClosed || len(work.Attempts) >= _maxWorkAttempts {
			return errConflict
		}
		for _, attempt := range work.Attempts {
			if !attempt.Cleaned {
				return errConflict
			}
		}
		startWorkAttempt(record, route)
	default:
		return errConflict
	}
	if len(work.Attempts) > 0 {
		event.Attempt = work.Attempts[len(work.Attempts)-1].Number
	}
	work.Events = append(work.Events, *event)
	return nil
}

func (g *Gateway) observeConversation(ctx context.Context, record Record) conversationObservation {
	work := record.Work
	summary := conversationObservation{LastError: work.LastError}
	if len(work.Attempts) == 0 {
		return summary
	}
	attempt := work.Attempts[len(work.Attempts)-1]
	summary.Phase = attempt.Phase
	var session agentsv1alpha1.Session
	err := g.reader.Get(ctx, client.ObjectKey{Namespace: record.Namespace, Name: attempt.SessionName}, &session)
	if apierrors.IsNotFound(err) {
		return summary
	}
	if err != nil {
		summary.ObservationUnavailable = true
		if summary.LastError == "" {
			summary.LastError = "Session status could not be observed"
		}
		return summary
	}
	if attempt.SessionUID == "" || string(session.UID) != attempt.SessionUID || session.Annotations[_workAnnotation] != work.ID {
		return summary
	}
	summary.Phase = sessionDisplayPhase(&session)
	summary.SessionURL = "/v1/namespaces/" + record.Namespace + "/sessions/" + attempt.SessionName
	return summary
}

func startWorkAttempt(record *Record, route profileRoute) {
	work := record.Work
	number := len(work.Attempts) + 1
	name := workSessionName(work.ID, number)
	attempt := WorkAttempt{
		Number:         number,
		ConversationID: conversationID(record.UID, number),
		SessionName:    name,
		Policy:         workPolicy(route, work.ID, number),
	}
	work.Attempts = append(work.Attempts, attempt)
	work.State = _workOpen
	work.LastError = ""
	record.Name = name
}
