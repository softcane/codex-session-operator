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
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	"github.com/softcane/codex-session-operator/internal/openaiapi"
)

const (
	_conversationsSegment = "conversations"
	_maxConversationPage  = 100
	_messagesSegment      = "messages"
	_moreField            = "more"
)

// ConversationInput selects trusted policy through a route and supplies first input.
// Callers cannot override the route's Agent, Environment, Vaults or spending choice.
type ConversationInput struct {
	RouteID string `json:"route_id"`
	Text    string `json:"text"`
	Title   string `json:"title,omitempty"`
}

// ConversationReceipt confirms a committed input, not a completed agent turn.
// Event timestamps and selected attempt remain unchanged on exact retries.
type ConversationReceipt struct {
	ConversationID  string    `json:"conversation_id"`
	ConversationURL string    `json:"conversation_url"`
	RouteID         string    `json:"route_id"`
	Attempt         int       `json:"attempt"`
	EventID         string    `json:"event_id"`
	ReceivedAt      time.Time `json:"received_at"`
	Acceptance      string    `json:"acceptance"`
}

// ConversationSummary exposes a selected attempt and its frozen safe policy.
type ConversationSummary struct {
	Profile                *ProfileProvenance                  `json:"profile,omitempty"`
	ConversationID         string                              `json:"conversation_id"`
	ConversationURL        string                              `json:"conversation_url"`
	RouteID                string                              `json:"route_id"`
	Title                  string                              `json:"title,omitempty"`
	Attempt                int                                 `json:"attempt"`
	Active                 bool                                `json:"active"`
	State                  string                              `json:"state"`
	Cleaned                bool                                `json:"cleaned"`
	Archived               bool                                `json:"archived"`
	Agent                  string                              `json:"agent"`
	Environment            string                              `json:"environment"`
	Vaults                 []string                            `json:"vaults,omitempty"`
	SpendControl           *agentsv1alpha1.SessionSpendControl `json:"spend_control"`
	SessionName            string                              `json:"session_name,omitempty"`
	SessionUID             string                              `json:"session_uid,omitempty"`
	SessionURL             string                              `json:"session_url,omitempty"`
	Phase                  string                              `json:"phase,omitempty"`
	LastError              string                              `json:"last_error,omitempty"`
	ObservationUnavailable bool                                `json:"observation_unavailable,omitempty"`
}

// ConversationDetail retains history after the backing Session has been deleted.
type ConversationDetail struct {
	Lifecycle        ConversationLifecycle `json:"lifecycle"`
	Summary          ConversationSummary   `json:"summary"`
	Inputs           []WorkEvent           `json:"inputs"`
	History          openaiapi.History     `json:"history"`
	Submissions      []Submission          `json:"submissions"`
	Actions          map[string]Action     `json:"actions"`
	RecoveryRequired bool                  `json:"recovery_required"`
	LastError        string                `json:"last_error,omitempty"`
	Archived         bool                  `json:"archived"`
	InputsArchived   bool                  `json:"inputs_archived"`
	HistoryArchived  bool                  `json:"history_archived"`
}

func (g *Gateway) serveConversations(w http.ResponseWriter, r *http.Request, parts []string, principal Principal) bool {
	if len(parts) < 4 || len(parts) > 6 || parts[0] != "v1" || parts[1] != _namespacesSegment ||
		parts[2] != principal.Namespace || parts[3] != _conversationsSegment {
		return false
	}
	if len(parts) == 4 {
		g.handleConversationCollection(w, r, principal)
	} else {
		g.handleConversation(w, r, parts, principal)
	}
	return true
}

func (g *Gateway) handleConversationCollection(w http.ResponseWriter, r *http.Request, principal Principal) {
	switch r.Method {
	case http.MethodPost:
		var input ConversationInput
		if err := decodeConversation(w, r, &input); err != nil || !validConversationInput(input) {
			http.Error(w, "Invalid conversation input", http.StatusBadRequest)
			return
		}
		input.Text = strings.ReplaceAll(input.Text, "\r\n", "\n")
		key := r.Header.Get("Idempotency-Key")
		if !validKey(key) {
			http.Error(w, "Idempotency-Key required (1–128 ASCII key characters)", http.StatusBadRequest)
			return
		}
		record, event, err := g.createConversation(r.Context(), principal, key, input)
		if err != nil {
			writeConversationError(w, err)
			return
		}
		writeConversationReceipt(w, record, event)
	case http.MethodGet:
		g.handleConversationList(w, r, principal)
	default:
		methodError(w)
	}
}

func (g *Gateway) handleConversationList(w http.ResponseWriter, r *http.Request, principal Principal) {
	after, limit, err := conversationPageQuery(r)
	if err != nil {
		http.Error(w, "Invalid conversation page cursor or limit", http.StatusBadRequest)
		return
	}
	entries, err := g.listConversationAttempts(r.Context(), principal.Namespace, after, limit)
	if err != nil {
		writeError(w, err)
		return
	}
	more := len(entries) > limit
	if more {
		entries = entries[:limit]
	}
	items := make([]ConversationSummary, 0, len(entries))
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	for _, entry := range entries {
		items = append(items, g.summarizeConversation(ctx, entry.Record, entry.Attempt))
	}
	next := ""
	if more {
		next = entries[len(entries)-1].Attempt.ConversationID
	}
	writeJSON(w, http.StatusOK, map[string]any{_conversationsSegment: items, _moreField: more, "next_cursor": next})
}

func (g *Gateway) listConversationAttempts(ctx context.Context, namespace, after string, limit int) ([]ConversationEntry, error) {
	if store, ok := g.store.(ConversationStorage); ok {
		return store.ListConversationAttempts(ctx, namespace, after, limit)
	}
	// Test-only stores use the namespace scan. Production storage pages directly
	// through the public-ID index without reading all retained conversations.
	records, err := g.store.RecordsInNamespace(ctx, namespace)
	if err != nil {
		return nil, err
	}
	var entries []ConversationEntry
	for _, record := range records {
		if record.Namespace != namespace || record.Work == nil || record.Work.Conversation == nil {
			continue
		}
		for _, attempt := range record.Work.Attempts {
			if attempt.ConversationID > after {
				entries = append(entries, ConversationEntry{Record: record, Attempt: attempt})
			}
		}
	}
	slices.SortFunc(entries, func(a, b ConversationEntry) int {
		return cmp.Compare(a.Attempt.ConversationID, b.Attempt.ConversationID)
	})
	if len(entries) > limit+1 {
		entries = entries[:limit+1]
	}
	return entries, nil
}

func conversationPageQuery(r *http.Request) (string, int, error) {
	query := r.URL.Query()
	limit := _maxConversationPage
	if values, ok := query["limit"]; ok {
		if len(values) != 1 {
			return "", 0, errors.New("one conversation page limit required")
		}
		parsed, err := strconv.Atoi(values[0])
		if err != nil || parsed < 1 || parsed > _maxConversationPage {
			return "", 0, errors.New("invalid conversation page limit")
		}
		limit = parsed
	}
	after := query.Get("after")
	if values, ok := query["after"]; ok && (len(values) != 1 || !validConversationID(after)) {
		return "", 0, errors.New("invalid conversation page cursor")
	}
	return after, limit, nil
}

func (g *Gateway) createConversation(ctx context.Context, principal Principal, key string, input ConversationInput) (Record, WorkEvent, error) {
	id := conversationRecordID(principal.Namespace, principal.Subject, key)
	digest := conversationDigest(input)
	g.workMu.Lock()
	defer g.workMu.Unlock()
	gate := g.gate(id)
	gate.Lock()
	defer gate.Unlock()
	current, err := g.store.GetInNamespace(ctx, principal.Namespace, id)
	if err == nil {
		event, err := creationReplay(current, principal.Subject, key, digest)
		return current, event, err
	}
	if !errors.Is(err, os.ErrNotExist) {
		return Record{}, WorkEvent{}, err
	}
	selection, err := g.selectProfile(ctx, principal.Namespace, input.RouteID)
	if err != nil {
		return Record{}, WorkEvent{}, err
	}
	route := selection.route
	if err := g.deliveryEnabled(ctx); err != nil {
		return Record{}, WorkEvent{}, err
	}
	event := WorkEvent{ID: conversationEventID(principal.Namespace, id, principal.Subject, key, "create"),
		Digest: digest, Kind: _workTask, Text: input.Text, ReceivedAt: time.Now().UTC()}
	if err := g.store.Update(ctx, id, func(record *Record) error {
		// Storage serializes competing replicas. Recheck identity inside the commit.
		if record.Work != nil {
			previous, err := creationReplay(*record, principal.Subject, key, digest)
			event = previous
			return err
		}
		if record.Namespace != "" || record.DeliveryHeld || record.Closed {
			return errConflict
		}
		record.Namespace = route.Namespace
		record.Work = &Work{ID: id, RouteID: route.ID, Source: route.Source, ExternalID: id,
			Conversation: &ConversationOrigin{Subject: principal.Subject, Key: key, Digest: digest, Title: input.Title}}
		startWorkAttempt(record, route)
		if err := applyWorkEvent(record, route, conversationMutation{WorkID: id, Kind: _workTask}, &event); err != nil {
			return err
		}
		bindProfile(record, selection.provenance)
		return nil
	}); err != nil {
		return Record{}, WorkEvent{}, err
	}
	record, err := g.store.GetInNamespace(ctx, principal.Namespace, id)
	return record, event, err
}

func (g *Gateway) handleConversation(w http.ResponseWriter, r *http.Request, parts []string, principal Principal) {
	if len(parts) == 5 && r.Method == http.MethodGet {
		g.handleConversationDetail(w, r, principal, parts[4])
		return
	}
	if len(parts) != 6 {
		methodError(w)
		return
	}
	kind := ""
	switch parts[5] {
	case _messagesSegment:
		kind = _workEvidence
	case _workClose:
		kind = _workClose
	case "restart":
		kind = _workRestart
	default:
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		methodError(w)
		return
	}
	key := r.Header.Get("Idempotency-Key")
	if !validKey(key) {
		http.Error(w, "Idempotency-Key required (1–128 ASCII key characters)", http.StatusBadRequest)
		return
	}
	var input struct {
		Text string `json:"text"`
	}
	var err error
	if kind == _workClose {
		err = decodeConversation(w, r, &struct{}{})
	} else {
		err = decodeConversation(w, r, &input)
		if strings.TrimSpace(input.Text) == "" || len(input.Text) > _maxInputBytes {
			err = errors.New("conversation text required")
		}
	}
	if err != nil {
		http.Error(w, "Invalid conversation input", http.StatusBadRequest)
		return
	}
	input.Text = strings.ReplaceAll(input.Text, "\r\n", "\n")
	record, event, err := g.acceptConversationMutation(r.Context(), principal, parts[4], kind, key, input.Text)
	if err != nil {
		writeConversationError(w, err)
		return
	}
	writeConversationReceipt(w, record, event)
}

func (g *Gateway) acceptConversationMutation(ctx context.Context, principal Principal, publicID, kind, key, text string) (Record, WorkEvent, error) {
	initial, _, err := g.lookupConversation(ctx, principal.Namespace, publicID)
	if err != nil {
		return Record{}, WorkEvent{}, err
	}
	// Every mutation can take a conversation gate followed by a Session gate.
	g.workMu.Lock()
	defer g.workMu.Unlock()
	gate := g.gate(initial.UID)
	gate.Lock()
	defer gate.Unlock()
	current, err := g.store.GetInNamespace(ctx, principal.Namespace, initial.UID)
	if err != nil {
		return Record{}, WorkEvent{}, err
	}
	event := WorkEvent{ID: conversationEventID(principal.Namespace, current.UID, principal.Subject, key, "mutation"),
		Digest: conversationDigest([3]string{publicID, kind, text}), Kind: kind, Text: text, ReceivedAt: time.Now().UTC()}
	if previous, found, err := conversationReplay(current, event); found || err != nil {
		return current, previous, err
	}
	attempt, ok := conversationAttempt(current, publicID)
	if !ok {
		return Record{}, WorkEvent{}, os.ErrNotExist
	}
	// Work closure and Session delivery share the same two-gate lock order.
	if attempt.SessionUID != "" {
		inputGate := g.gate(attempt.SessionUID)
		if inputGate != gate {
			inputGate.Lock()
			defer inputGate.Unlock()
		}
		if err := g.conversationInputHeld(ctx, principal.Namespace, attempt.SessionUID, kind); err != nil {
			return Record{}, WorkEvent{}, err
		}
	}
	if err := g.deliveryEnabled(ctx); err != nil {
		return Record{}, WorkEvent{}, err
	}
	if current.DeliveryHeld {
		return Record{}, WorkEvent{}, errRestoreHeld
	}
	if attempt.Number != len(current.Work.Attempts) {
		return Record{}, WorkEvent{}, errClosed
	}
	route := profileRoute{ID: current.Work.RouteID, Namespace: current.Namespace, Source: current.Work.Source}
	var provenance *ProfileProvenance
	if kind == _workRestart {
		selection, err := g.selectProfile(ctx, current.Namespace, route.ID)
		if err != nil {
			return Record{}, WorkEvent{}, err
		}
		route = selection.route
		// The accepted source identity remains immutable across Profile updates.
		route.Source = current.Work.Source
		provenance = selection.provenance
	}
	if err := g.store.Update(ctx, current.UID, func(record *Record) error {
		if previous, found, err := conversationReplay(*record, event); found || err != nil {
			event = previous
			return err
		}
		if record.DeliveryHeld {
			return errRestoreHeld
		}
		selected, ok := conversationAttempt(*record, publicID)
		if !ok || selected.Number != len(record.Work.Attempts) {
			return errClosed
		}
		if err := applyWorkEvent(record, route, conversationMutation{WorkID: record.Work.ExternalID, Kind: kind}, &event); err != nil {
			return err
		}
		if kind == _workRestart {
			bindProfile(record, provenance)
		}
		return nil
	}); err != nil {
		return Record{}, WorkEvent{}, err
	}
	record, err := g.store.GetInNamespace(ctx, principal.Namespace, current.UID)
	return record, event, err
}

func (g *Gateway) conversationInputHeld(ctx context.Context, namespace, uid, kind string) error {
	record, err := g.store.GetInNamespace(ctx, namespace, uid)
	if errors.Is(err, os.ErrNotExist) {
		return nil // The provisioner may have saved the UID before creating its input record.
	}
	if err != nil {
		return err
	}
	if record.DeliveryHeld {
		return errRestoreHeld
	}
	if record.Closed && kind == _workEvidence {
		return errClosed
	}
	return nil
}

func (g *Gateway) lookupConversation(ctx context.Context, namespace, id string) (Record, WorkAttempt, error) {
	if !validConversationID(id) {
		return Record{}, WorkAttempt{}, os.ErrNotExist
	}
	if store, ok := g.store.(ConversationStorage); ok {
		return store.LookupConversation(ctx, namespace, id)
	}
	records, err := g.store.RecordsInNamespace(ctx, namespace)
	if err != nil {
		return Record{}, WorkAttempt{}, err
	}
	for _, record := range records {
		if record.Namespace != namespace {
			continue
		}
		if attempt, ok := conversationAttempt(record, id); ok {
			return record, attempt, nil
		}
	}
	return Record{}, WorkAttempt{}, os.ErrNotExist
}

func (g *Gateway) handleConversationDetail(w http.ResponseWriter, r *http.Request, principal Principal, id string) {
	record, attempt, err := g.lookupConversation(r.Context(), principal.Namespace, id)
	if err != nil {
		writeConversationError(w, err)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	detail := ConversationDetail{Summary: g.summarizeConversation(ctx, record, attempt)}
	detail.InputsArchived = detail.Summary.Archived
	detail.Archived = detail.InputsArchived
	var child Record
	for _, event := range record.Work.Events {
		if event.Attempt == attempt.Number &&
			(event.Kind == _workTask || event.Kind == _workRestart || event.Kind == _workEvidence) {
			detail.Inputs = append(detail.Inputs, event)
		}
	}
	if attempt.SessionUID != "" {
		conversation, err := g.store.GetInNamespace(r.Context(), principal.Namespace, attempt.SessionUID)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			writeError(w, err)
			return
		}
		detail.History = conversation.History
		child = conversation
		detail.Submissions = conversation.Submissions
		detail.Actions = conversation.Actions
		detail.RecoveryRequired = conversation.RecoveryRequired
		detail.LastError = conversation.LastError
		detail.HistoryArchived = conversation.Archived
		detail.Archived = detail.InputsArchived || detail.HistoryArchived
		detail.Summary.Archived = detail.Archived
	}
	detail.Lifecycle = g.conversationLifecycle(ctx, record, attempt, detail, child)
	writeJSON(w, http.StatusOK, detail)
}

func (g *Gateway) summarizeConversation(ctx context.Context, record Record, attempt WorkAttempt) ConversationSummary {
	work := record.Work
	summary := ConversationSummary{ConversationID: attempt.ConversationID,
		Profile:         copyProfileProvenance(attempt.Profile),
		ConversationURL: conversationURL(record.Namespace, attempt.ConversationID), RouteID: work.RouteID,
		Title: work.Conversation.Title, Attempt: attempt.Number, Active: attempt.Number == len(work.Attempts),
		State: _workClosed, Cleaned: attempt.Cleaned, Archived: attempt.InputsArchived,
		SessionName: attempt.SessionName, SessionUID: attempt.SessionUID,
		Phase: attempt.Phase, SpendControl: attempt.Policy.SpendControl.DeepCopy()}
	if attempt.Policy.AgentRef != nil {
		summary.Agent = attempt.Policy.AgentRef.Name
	}
	if attempt.Policy.EnvironmentRef != nil {
		summary.Environment = attempt.Policy.EnvironmentRef.Name
	}
	for _, vault := range attempt.Policy.VaultRefs {
		summary.Vaults = append(summary.Vaults, vault.Name)
	}
	if !summary.Active {
		return summary
	}
	observed := g.observeConversation(ctx, record)
	summary.State = work.State
	summary.Phase = observed.Phase
	summary.SessionURL = observed.SessionURL
	summary.LastError = observed.LastError
	summary.ObservationUnavailable = observed.ObservationUnavailable
	return summary
}

func creationReplay(record Record, subject, key, digest string) (WorkEvent, error) {
	if record.Work == nil || record.Work.Conversation == nil {
		return WorkEvent{}, errConflict
	}
	origin := record.Work.Conversation
	if origin.Subject != subject || origin.Key != key || origin.Digest != digest {
		return WorkEvent{}, errConflict
	}
	for _, event := range record.Work.Events {
		if event.Attempt == 1 && event.Kind == _workTask && event.Digest == digest {
			return event, nil
		}
	}
	return WorkEvent{}, errUnavailable
}

func conversationReplay(record Record, event WorkEvent) (WorkEvent, bool, error) {
	if record.Work == nil || record.Work.Conversation == nil {
		return WorkEvent{}, false, errConflict
	}
	for _, previous := range record.Work.Events {
		if previous.ID != event.ID {
			continue
		}
		if previous.Digest != event.Digest {
			return WorkEvent{}, false, errConflict
		}
		return previous, true, nil
	}
	return WorkEvent{}, false, nil
}

func conversationAttempt(record Record, id string) (WorkAttempt, bool) {
	if record.Work == nil || record.Work.Conversation == nil {
		return WorkAttempt{}, false
	}
	for _, attempt := range record.Work.Attempts {
		if attempt.ConversationID == id {
			return attempt, true
		}
	}
	return WorkAttempt{}, false
}

func conversationRecordID(namespace, subject, key string) string {
	return "convwork-" + conversationDigest([3]string{namespace, subject, key})
}

func conversationID(uid string, attempt int) string {
	return "conv-" + conversationDigest([2]string{uid, strconv.Itoa(attempt)})
}

func conversationEventID(namespace, uid, subject, key, scope string) string {
	return "conv-event-" + conversationDigest([5]string{namespace, uid, subject, key, scope})
}

func conversationDigest(value any) string {
	raw, _ := json.Marshal(value) // Only concrete strings, arrays and input structs are passed.
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func validConversationID(id string) bool {
	if len(id) != len("conv-")+sha256.Size*2 || !strings.HasPrefix(id, "conv-") {
		return false
	}
	_, err := hex.DecodeString(strings.TrimPrefix(id, "conv-"))
	return err == nil
}

func conversationURL(namespace, id string) string {
	return "/v1/namespaces/" + namespace + "/" + _conversationsSegment + "/" + id
}

func writeConversationReceipt(w http.ResponseWriter, record Record, event WorkEvent) {
	for _, attempt := range record.Work.Attempts {
		if attempt.Number != event.Attempt {
			continue
		}
		link := conversationURL(record.Namespace, attempt.ConversationID)
		w.Header().Set("Location", link)
		writeJSON(w, http.StatusAccepted, ConversationReceipt{ConversationID: attempt.ConversationID,
			ConversationURL: link, RouteID: record.Work.RouteID, Attempt: attempt.Number,
			EventID: event.ID, ReceivedAt: event.ReceivedAt, Acceptance: "committed"})
		return
	}
	writeError(w, errUnavailable)
}

func validConversationInput(input ConversationInput) bool {
	if !validWorkIdentifier(input.RouteID) || strings.TrimSpace(input.Text) == "" ||
		len(input.Text) > _maxInputBytes || len(input.Title) > 256 {
		return false
	}
	for _, char := range input.Title {
		if unicode.IsControl(char) {
			return false
		}
	}
	return true
}

func decodeConversation(w http.ResponseWriter, r *http.Request, target any) error {
	r.Body = http.MaxBytesReader(w, r.Body, _maxInputBytes+1024)
	raw, err := io.ReadAll(r.Body)
	if err != nil || !utf8.Valid(raw) {
		return errors.New("invalid conversation encoding or size")
	}
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || raw[0] != '{' {
		return errors.New("conversation input must be a JSON object")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing any
	if !errors.Is(decoder.Decode(&trailing), io.EOF) {
		return errors.New("unexpected trailing JSON")
	}
	return nil
}

func writeConversationError(w http.ResponseWriter, err error) {
	if unavailable, ok := errors.AsType[*profileUnavailableError](err); ok {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{_errorField: "profile_unavailable", "reason": unavailable.reason})
		return
	}
	if errors.Is(err, os.ErrNotExist) {
		http.Error(w, "Conversation or profile not found", http.StatusNotFound)
		return
	}
	writeError(w, err)
}
