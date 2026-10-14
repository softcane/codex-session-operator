package gateway

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"slices"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const _workSegment = "work"

// WorkSummary identifies the active attempt without exposing the task text.
type WorkSummary struct {
	ID                     string `json:"id"`
	RouteID                string `json:"route_id"`
	WorkID                 string `json:"work_id"`
	Source                 string `json:"source"`
	State                  string `json:"state"`
	Attempt                int    `json:"attempt"`
	SessionName            string `json:"session_name,omitempty"`
	SessionUID             string `json:"session_uid,omitempty"`
	SessionURL             string `json:"session_url,omitempty"`
	Phase                  string `json:"phase,omitempty"`
	LastError              string `json:"last_error,omitempty"`
	ObservationUnavailable bool   `json:"observation_unavailable,omitempty"`
}

// WorkReceipt is persisted acceptance, not proof that an agent turn completed.
// ID is the internal work identity; WorkID is the caller's external identity.
type WorkReceipt struct {
	ID         string    `json:"id"`
	RouteID    string    `json:"route_id"`
	WorkID     string    `json:"work_id"`
	EventID    string    `json:"event_id"`
	Attempt    int       `json:"attempt"`
	ReceivedAt time.Time `json:"received_at"`
	WorkURL    string    `json:"work_url"`
}

func (g *Gateway) handleWorkEvents(w http.ResponseWriter, r *http.Request, principal Principal) {
	if r.Method != http.MethodPost {
		methodError(w)
		return
	}
	input, err := decodeWorkInput(w, r)
	if err != nil {
		http.Error(w, "Invalid work event", http.StatusBadRequest)
		return
	}
	route, ok, err := g.workRouteForEvent(r.Context(), input, principal.Namespace)
	if err != nil {
		writeError(w, err)
		return
	}
	if !ok {
		http.NotFound(w, r)
		return
	}
	raw, err := json.Marshal(input)
	if err != nil {
		writeError(w, err)
		return
	}
	sum := sha256.Sum256(raw)
	event := WorkEvent{ID: input.EventID, Digest: hex.EncodeToString(sum[:]), Kind: input.Kind,
		Text: input.Text, ReceivedAt: time.Now().UTC()}
	record, receipt, err := g.acceptWorkEvent(r.Context(), route, input, event)
	if err != nil {
		if errors.Is(err, errWorkMissing) || errors.Is(err, errClosed) {
			http.Error(w, "Work is unknown or closed; an eligible closed attempt requires explicit restart", http.StatusConflict)
			return
		}
		writeError(w, err)
		return
	}
	writeWorkReceipt(w, record, receipt)
}

func (g *Gateway) workRouteForEvent(ctx context.Context, input WorkInput, namespace string) (WorkRoute, bool, error) {
	// Stopping already-accepted work remains possible after its route is retired.
	// Use the retained source identity, never a changed route's runtime policy.
	if input.Kind == _workClose {
		records, err := g.recordSummaries(ctx, namespace, "work")
		if err != nil {
			return WorkRoute{}, false, err
		}
		var retained *WorkRoute
		for _, record := range records {
			if record.Work == nil || record.Namespace != namespace ||
				record.Work.RouteID != input.RouteID || record.Work.ExternalID != input.WorkID {
				continue
			}
			if record.Work.Conversation != nil {
				return WorkRoute{}, false, errConflict
			}
			if retained != nil {
				return WorkRoute{}, false, errConflict
			}
			retained = &WorkRoute{ID: record.Work.RouteID, Namespace: namespace, Source: record.Work.Source}
		}
		if retained != nil {
			return *retained, true, nil
		}
	}
	route, ok := g.workRoutes[input.RouteID]
	return route, ok && route.Namespace == namespace, nil
}

func (g *Gateway) acceptWorkEvent(ctx context.Context, route WorkRoute, input WorkInput, event WorkEvent) (Record, WorkEvent, error) {
	id := workIdentity(route.Namespace, route.ID, route.Source, input.WorkID)
	// Closures fence already-running Session sends. Serialize two-gate operations
	// across work and issue sources, whose identities share the same hash shards.
	if input.Kind == _workClose {
		g.issueCloseMu.Lock()
		defer g.issueCloseMu.Unlock()
	}
	g.workMu.Lock()
	defer g.workMu.Unlock()
	gate := g.gate(id)
	gate.Lock()
	defer gate.Unlock()
	record, previous, found, err := g.findWorkEvent(ctx, route, id, event)
	if err != nil || found {
		return record, previous, err
	}
	current, err := g.store.GetInNamespace(ctx, route.Namespace, id)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return Record{}, WorkEvent{}, err
	}
	if input.Kind == _workClose && current.Work != nil && len(current.Work.Attempts) > 0 {
		uid := current.Work.Attempts[len(current.Work.Attempts)-1].SessionUID
		if uid != "" {
			inputGate := g.gate(uid)
			if inputGate != gate {
				inputGate.Lock()
				defer inputGate.Unlock()
			}
		}
	}
	if err := g.store.Update(ctx, id, func(record *Record) error {
		return applyWorkEvent(record, route, input, &event)
	}); err != nil {
		return Record{}, WorkEvent{}, err
	}
	record, err = g.store.GetInNamespace(ctx, route.Namespace, id)
	return record, event, err
}

func (g *Gateway) findWorkEvent(ctx context.Context, route WorkRoute, id string, event WorkEvent) (Record, WorkEvent, bool, error) {
	if store, ok := g.store.(IndexedStorage); ok {
		r, previous, found, err := store.LookupWorkEvent(ctx, route.Namespace, route.ID, event.ID)
		if err == nil && found && (r.UID != id || previous.Digest != event.Digest) {
			return Record{}, WorkEvent{}, false, errConflict
		}
		return r, previous, found, err
	}
	records, err := g.store.RecordsInNamespace(ctx, route.Namespace)
	if err != nil {
		return Record{}, WorkEvent{}, false, err
	}
	// Event IDs are route-scoped, so retrying one delivery with another work ID
	// cannot accidentally create a second Session.
	for _, record := range records {
		if record.Work == nil || record.Work.Conversation != nil || record.Namespace != route.Namespace || record.Work.RouteID != route.ID {
			continue
		}
		for _, previous := range record.Work.Events {
			if previous.ID != event.ID {
				continue
			}
			if record.UID != id || previous.Digest != event.Digest {
				return Record{}, WorkEvent{}, false, errConflict
			}
			return record, previous, true, nil
		}
	}
	return Record{}, WorkEvent{}, false, nil
}

func applyWorkEvent(record *Record, route WorkRoute, input WorkInput, event *WorkEvent) error {
	if record.Work == nil {
		if input.Kind != _workTask && input.Kind != _workClose {
			return errWorkMissing
		}
		record.Namespace = route.Namespace
		record.Work = &Work{ID: record.UID, RouteID: route.ID, Source: route.Source,
			ExternalID: input.WorkID, State: _workClosed}
		if input.Kind == _workTask {
			startWorkAttempt(record, route)
		}
	}
	work := record.Work
	for _, previous := range work.Events {
		if previous.ID == event.ID {
			if previous.Digest != event.Digest {
				return errConflict
			}
			*event = previous
			return nil
		}
	}
	if record.Namespace != route.Namespace || work.RouteID != route.ID ||
		work.Source != route.Source || work.ExternalID != input.WorkID {
		return errConflict
	}
	if len(work.Events) >= _maxSubmissions {
		return errors.New("work event capacity reached")
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
	}
	if len(work.Attempts) > 0 {
		event.Attempt = work.Attempts[len(work.Attempts)-1].Number
	}
	work.Events = append(work.Events, *event)
	return nil
}

func (g *Gateway) handleWorkCollection(w http.ResponseWriter, r *http.Request, principal Principal) {
	if r.Method != http.MethodGet {
		methodError(w)
		return
	}
	records, err := g.recordSummaries(r.Context(), principal.Namespace, "work")
	if err != nil {
		writeError(w, err)
		return
	}
	items := make([]WorkSummary, 0)
	observationCtx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	for _, record := range records {
		if record.Work == nil || record.Work.Conversation != nil || record.Namespace != principal.Namespace {
			continue
		}
		summary, _ := g.summarizeWork(observationCtx, record)
		items = append(items, summary)
	}
	slices.SortFunc(items, func(a, b WorkSummary) int { return cmp.Compare(a.ID, b.ID) })
	writeJSON(w, http.StatusOK, map[string]any{_workSegment: items})
}

func (g *Gateway) handleWork(w http.ResponseWriter, r *http.Request, parts []string, principal Principal) {
	if len(parts) != 5 {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet {
		methodError(w)
		return
	}
	record, err := g.store.GetInNamespace(r.Context(), principal.Namespace, parts[4])
	if errors.Is(err, os.ErrNotExist) || err == nil &&
		(record.Work == nil || record.Namespace != principal.Namespace) {
		http.NotFound(w, r)
		return
	}
	if err != nil {
		writeError(w, err)
		return
	}
	observationCtx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	summary, status := g.summarizeWork(observationCtx, record)
	var conversation *Record
	if summary.SessionUID != "" {
		value, err := g.store.GetInNamespace(r.Context(), principal.Namespace, summary.SessionUID)
		if err == nil {
			conversation = &value
		} else if !errors.Is(err, os.ErrNotExist) {
			writeError(w, err)
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"summary": summary, _workSegment: record.Work,
		"status": status, "conversation": conversation})
}

func (g *Gateway) summarizeWork(ctx context.Context, record Record) (WorkSummary, *agentsv1alpha1.SessionStatus) {
	work := record.Work
	summary := WorkSummary{ID: work.ID, RouteID: work.RouteID, WorkID: work.ExternalID,
		Source: work.Source, State: work.State, LastError: work.LastError}
	if len(work.Attempts) == 0 {
		return summary, nil
	}
	attempt := work.Attempts[len(work.Attempts)-1]
	summary.Attempt = attempt.Number
	summary.SessionName = attempt.SessionName
	summary.SessionUID = attempt.SessionUID
	summary.Phase = attempt.Phase
	var session agentsv1alpha1.Session
	err := g.reader.Get(ctx, client.ObjectKey{Namespace: record.Namespace, Name: attempt.SessionName}, &session)
	if apierrors.IsNotFound(err) {
		return summary, nil
	}
	if err != nil {
		summary.ObservationUnavailable = true
		if summary.LastError == "" {
			summary.LastError = "Session status could not be observed"
		}
		return summary, nil
	}
	if attempt.SessionUID == "" || string(session.UID) != attempt.SessionUID || session.Annotations[_workAnnotation] != work.ID {
		return summary, nil
	}
	summary.Phase = session.Status.Phase
	summary.SessionURL = "/v1/namespaces/" + record.Namespace + "/sessions/" + attempt.SessionName
	return summary, &session.Status
}

func startWorkAttempt(record *Record, route WorkRoute) {
	work := record.Work
	number := len(work.Attempts) + 1
	name := workSessionName(work.ID, number)
	attempt := WorkAttempt{Number: number, SessionName: name, Policy: workPolicy(route, work.ID, number)}
	if work.Conversation != nil {
		attempt.ConversationID = conversationID(record.UID, number)
	}
	work.Attempts = append(work.Attempts, attempt)
	work.State = _workOpen
	work.LastError = ""
	record.Name = name
}

func writeWorkReceipt(w http.ResponseWriter, record Record, event WorkEvent) {
	link := "/v1/namespaces/" + record.Namespace + "/work/" + record.Work.ID
	w.Header().Set("Location", link)
	writeJSON(w, http.StatusAccepted, WorkReceipt{ID: record.Work.ID, RouteID: record.Work.RouteID,
		WorkID: record.Work.ExternalID, EventID: event.ID, Attempt: event.Attempt,
		ReceivedAt: event.ReceivedAt, WorkURL: link})
}

func decodeWorkInput(w http.ResponseWriter, r *http.Request) (WorkInput, error) {
	r.Body = http.MaxBytesReader(w, r.Body, _maxInputBytes+1024)
	raw, err := io.ReadAll(r.Body)
	if err != nil || !utf8.Valid(raw) {
		return WorkInput{}, errors.New("invalid work event encoding or size")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var input WorkInput
	if err := decoder.Decode(&input); err != nil {
		return WorkInput{}, err
	}
	var trailing any
	if !errors.Is(decoder.Decode(&trailing), io.EOF) {
		return WorkInput{}, errors.New("unexpected trailing JSON")
	}
	input.WorkID = strings.TrimSpace(input.WorkID)
	input.Text = strings.ReplaceAll(input.Text, "\r\n", "\n")
	if !validWorkIdentifier(input.RouteID) || !validWorkIdentifier(input.EventID) || len(input.EventID) > 128 ||
		input.WorkID == "" || len(input.WorkID) > 256 || len(input.Text) > _maxInputBytes {
		return WorkInput{}, errors.New("invalid work event fields")
	}
	for _, char := range input.WorkID {
		if unicode.IsControl(char) {
			return WorkInput{}, errors.New("invalid work identity")
		}
	}
	switch input.Kind {
	case _workClose:
		if input.Text != "" {
			return WorkInput{}, errors.New("close event forbids text")
		}
	case _workTask, _workEvidence, _workRestart:
		if strings.TrimSpace(input.Text) == "" {
			return WorkInput{}, errors.New("work text required")
		}
	default:
		return WorkInput{}, errors.New("unsupported work event")
	}
	return input, nil
}
