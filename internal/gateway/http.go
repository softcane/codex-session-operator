package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// requestScope carries the namespace selected by the URL and stable service
// attribution used for durable replay. Access control belongs outside the gateway.
type requestScope struct {
	Subject   string
	Namespace string
}

const (
	_namespacesSegment   = "namespaces"
	_sessionsSegment     = "sessions"
	_recordsSegment      = "records"
	_eventsSegment       = "events"
	_maxNamespaceStreams = 4
)

type streamScope struct {
	namespace string
	subject   string
}

// Event streams share a bounded quota within each namespace.
func (g *Gateway) reserveStream(scope requestScope) (func(), bool) {
	identity := streamScope{namespace: scope.Namespace, subject: scope.Subject}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.streamCounts[identity] >= _maxNamespaceStreams {
		return nil, false
	}
	select {
	case g.streamSlots <- struct{}{}:
		g.streamCounts[identity]++
	default:
		return nil, false
	}
	return func() {
		g.mu.Lock()
		defer g.mu.Unlock()
		g.streamCounts[identity]--
		if g.streamCounts[identity] == 0 {
			delete(g.streamCounts, identity)
		}
		<-g.streamSlots
	}, true
}

func isEventStream(method string, parts []string) bool {
	return method == http.MethodGet && len(parts) == 6 && parts[0] == "v1" &&
		parts[1] == _namespacesSegment && parts[3] == _sessionsSegment && parts[5] == _eventsSegment
}

// Ready becomes true only after the leader's server has opened its listener and
// its manager-owned worker has started. Generic manager health precedes leadership.
func (g *Gateway) Ready(*http.Request) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.ctx == nil || g.ctx.Err() != nil {
		return errors.New("gateway is not running")
	}
	return nil
}

func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/readyz" {
		if r.Method != http.MethodGet {
			methodError(w)
			return
		}
		if err := g.Ready(r); err != nil {
			http.Error(w, "Gateway worker is not ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		return
	}
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 3 || parts[0] != "v1" || parts[1] != _namespacesSegment || len(validation.IsDNS1123Label(parts[2])) != 0 {
		http.NotFound(w, r)
		return
	}
	scope := requestScope{Namespace: parts[2], Subject: g.subject}
	if isEventStream(r.Method, parts) {
		release, ok := g.reserveStream(scope)
		if !ok {
			http.Error(w, "Gateway stream capacity reached", http.StatusServiceUnavailable)
			return
		}
		defer release()
		g.serveScoped(w, r, parts, scope)
		return
	}
	select {
	case g.requestSlots <- struct{}{}:
		defer func() { <-g.requestSlots }()
	default:
		http.Error(w, "Gateway capacity reached", http.StatusServiceUnavailable)
		return
	}
	g.serveScoped(w, r, parts, scope)
}

func (g *Gateway) serveScoped(w http.ResponseWriter, r *http.Request, parts []string, scope requestScope) {
	if g.serveConversations(w, r, parts, scope) {
		return
	}
	if g.serveNamespaceCollection(w, r, parts, scope) {
		return
	}
	if len(parts) == 5 && parts[0] == "v1" && parts[1] == _namespacesSegment &&
		parts[2] == scope.Namespace && parts[3] == _profilesSegment {
		g.handleProfiles(w, r, parts, scope)
		return
	}
	if len(parts) == 5 && parts[0] == "v1" && parts[1] == _namespacesSegment && parts[2] == scope.Namespace && parts[3] == _recordsSegment {
		g.handleAdminRecords(w, r, parts, scope)
		return
	}
	if len(parts) < 5 || len(parts) > 7 || parts[0] != "v1" || parts[1] != _namespacesSegment || parts[3] != _sessionsSegment || parts[2] != scope.Namespace {
		http.Error(w, "Session not found", http.StatusNotFound)
		return
	}
	g.serveSession(w, r, parts, scope)
}

func (g *Gateway) serveNamespaceCollection(w http.ResponseWriter, r *http.Request, parts []string, scope requestScope) bool {
	if len(parts) != 4 || parts[0] != "v1" || parts[1] != _namespacesSegment || parts[2] != scope.Namespace {
		return false
	}
	switch parts[3] {
	case _identitySegment:
		g.handleIdentity(w, r, scope)
		return true
	case "catalog":
		g.handleCatalog(w, r, scope)
		return true
	case _profilesSegment:
		g.handleProfiles(w, r, parts, scope)
		return true
	case _sessionsSegment:
		g.handleSessionCollection(w, r, scope)
		return true
	case _recordsSegment:
		g.handleAdminRecords(w, r, parts, scope)
		return true
	}
	return false
}

func (g *Gateway) handleIdentity(w http.ResponseWriter, r *http.Request, scope requestScope) {
	if r.Method != http.MethodGet {
		methodError(w)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{_callerSubjectField: scope.Subject, "namespace": scope.Namespace})
}

func (g *Gateway) serveSession(w http.ResponseWriter, r *http.Request, parts []string, scope requestScope) {
	session, record, err := g.lookup(r.Context(), parts[2], parts[4])
	if err != nil {
		writeError(w, err)
		return
	}
	if len(parts) == 5 {
		if r.Method != http.MethodGet {
			methodError(w)
			return
		}
		writeJSON(w, http.StatusOK, record)
		return
	}
	switch parts[5] {
	case "messages":
		if len(parts) != 6 {
			methodError(w)
			return
		}
		g.handleMessage(w, r, session)
	case "cancel":
		if len(parts) != 6 {
			methodError(w)
			return
		}
		g.handleCancel(w, r, session)

	case _historyField:
		if len(parts) != 6 || r.Method != http.MethodGet {
			methodError(w)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{_historyField: record.History, "recovery_required": record.RecoveryRequired, "last_error": record.LastError})
	case "actions":
		g.handleAction(w, r, parts, session, record, scope)

	case _eventsSegment:
		if len(parts) != 6 || r.Method != http.MethodGet {
			methodError(w)
			return
		}
		g.serveEvents(w, r, scope.Namespace, record.UID)
	default:
		http.NotFound(w, r)
	}
}

func spendControlReady(session *agentsv1alpha1.Session) bool {
	condition := meta.FindStatusCondition(session.Status.Conditions, "Ready")
	if condition != nil && condition.Status == metav1.ConditionFalse &&
		strings.HasPrefix(condition.Reason, "SpendControl") {
		return false
	}
	if session.Status.SpendControlAttemptedDigest != "" {
		return false
	}
	if session.Spec.SpendControl == nil || session.Spec.SpendControl.Mode != agentsv1alpha1.SessionSpendModeLimited {
		return true
	}
	return condition != nil && condition.Status == metav1.ConditionTrue && condition.ObservedGeneration == session.Generation
}

func (g *Gateway) handleMessage(w http.ResponseWriter, r *http.Request, session *agentsv1alpha1.Session) {
	if r.Method != http.MethodPost {
		methodError(w)
		return
	}
	var input struct {
		Text string `json:"text"`
	}
	if err := decode(w, r, &input); err != nil || strings.TrimSpace(input.Text) == "" || len(input.Text) > _maxInputBytes {
		http.Error(w, "Invalid text input", http.StatusBadRequest)
		return
	}
	key := r.Header.Get("Idempotency-Key")
	if !validKey(key) {
		http.Error(w, "Idempotency-Key required (1–128 ASCII key characters)", http.StatusBadRequest)
		return
	}
	gate := g.gate(string(session.UID))
	gate.Lock()
	defer gate.Unlock()
	current, err := g.currentInputSession(r.Context(), session)
	if err != nil {
		writeError(w, err)
		return
	}
	session = current
	record, err := g.store.GetInNamespace(r.Context(), session.Namespace, string(session.UID))
	if err != nil {
		writeError(w, err)
		return
	}
	// A none Environment requires its first input in the atomic create request.
	// Later input waits for spend-control readback and current-generation readiness.
	firstNoneInput := session.Status.OpenAISessionID == "" && (record.History.Status == _gatewayNoneStatus ||
		session.Status.EnvironmentSnapshot != nil && session.Status.EnvironmentSnapshot.Profile.Mode == agentsv1alpha1.EnvironmentModeNone) &&
		(record.InitialKey == "" || record.InitialKey == key)
	if !spendControlReady(session) && !firstNoneInput {
		http.Error(w, "Session spend control is not verified", http.StatusServiceUnavailable)
		return
	}
	err = g.enqueue(r.Context(), session, key, input.Text, "", nil)
	if err != nil {
		writeError(w, err)
		return
	}
	receipt, err := g.store.GetInNamespace(r.Context(), session.Namespace, string(session.UID))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, receipt.Submissions)
}
func (g *Gateway) handleCancel(w http.ResponseWriter, r *http.Request, session *agentsv1alpha1.Session) {
	if r.Method != http.MethodPost {
		methodError(w)
		return
	}
	var input struct {
		TurnID string `json:"turn_id"`
	}
	if decode(w, r, &input) != nil || input.TurnID == "" || !validKey(r.Header.Get("Idempotency-Key")) {
		http.Error(w, "Turn ID and Idempotency-Key required", http.StatusBadRequest)
		return
	}
	gate := g.gate(string(session.UID))
	gate.Lock()
	defer gate.Unlock()
	current, err := g.currentInputSession(r.Context(), session)
	if err != nil {
		writeError(w, err)
		return
	}
	session = current
	err = g.enqueue(r.Context(), session, r.Header.Get("Idempotency-Key"), "", input.TurnID, json.RawMessage(`{"type":"agent.session.input.cancel"}`))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]bool{"queued": true})
}
func (g *Gateway) handleAction(w http.ResponseWriter, r *http.Request, parts []string, session *agentsv1alpha1.Session, record Record, scope requestScope) {
	if len(parts) == 6 && r.Method == http.MethodGet {
		writeJSON(w, http.StatusOK, record.Actions)
		return
	}
	if len(parts) != 7 || r.Method != http.MethodPost {
		methodError(w)
		return
	}
	var input struct {
		Decision string `json:"decision"`
	}
	if decode(w, r, &input) != nil || (input.Decision != _approve && input.Decision != _deny) {
		http.Error(w, "Decision must be approve or deny", http.StatusBadRequest)
		return
	}
	gate := g.gate(string(session.UID))
	gate.Lock()
	defer gate.Unlock()
	if _, err := g.currentInputSession(r.Context(), session); err != nil {
		writeError(w, err)
		return
	}
	err := g.decide(r.Context(), string(session.UID), parts[6], input.Decision, scope.Subject)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]bool{"saved": true})
}

func (g *Gateway) lookup(ctx context.Context, namespace, name string) (*agentsv1alpha1.Session, Record, error) {
	var session agentsv1alpha1.Session
	if err := g.reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &session); err != nil {
		return nil, Record{}, err
	}
	if session.UID == "" {
		return nil, Record{}, errClosed
	}
	uid := string(session.UID)
	if err := g.store.Update(ctx, uid, func(r *Record) error {
		if r.Namespace != "" && (r.Namespace != namespace || r.Name != name) {
			return errors.New("session identity mismatch")
		}
		r.Namespace = namespace
		r.Name = name
		return nil
	}); err != nil {
		return nil, Record{}, err
	}
	if session.Status.OpenAISessionID == "" && session.Spec.EnvironmentRef != nil && session.Status.EnvironmentSnapshot == nil {
		var environment agentsv1alpha1.Environment
		if err := g.reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: session.Spec.EnvironmentRef.Name}, &environment); err != nil {
			return nil, Record{}, err
		}
		if environment.Spec.Mode == agentsv1alpha1.EnvironmentModeNone {
			if err := g.store.Update(ctx, uid, func(r *Record) error {
				if r.History.ID == "" {
					r.History.Status = _gatewayNoneStatus
				}
				return nil
			}); err != nil {
				return nil, Record{}, err
			}
		}
	}
	record, err := g.store.GetInNamespace(ctx, namespace, uid)
	return &session, record, err
}

// The caller holds the per-Session gate. Re-read before persisting a mutation so
// a close, deletion or name reused with another UID cannot accept stale input.
// Reads still expose retained history independently of the mutation policy.
func (g *Gateway) currentInputSession(ctx context.Context, session *agentsv1alpha1.Session) (*agentsv1alpha1.Session, error) {
	var current agentsv1alpha1.Session
	if err := g.reader.Get(ctx, client.ObjectKeyFromObject(session), &current); err != nil {
		return nil, err
	}
	if current.UID != session.UID || !current.DeletionTimestamp.IsZero() {
		return nil, errClosed
	}
	record, err := g.store.Get(ctx, string(current.UID))
	if err != nil {
		return nil, err
	}
	if record.Closed {
		return nil, errClosed
	}
	if record.DeliveryHeld {
		return nil, errRestoreHeld
	}
	if err := g.sessionInputOpen(ctx, &current); err != nil {
		return nil, err
	}
	return &current, nil
}

func (g *Gateway) sessionInputOpen(_ context.Context, session *agentsv1alpha1.Session) error {
	condition := meta.FindStatusCondition(session.Status.Conditions, _readyCondition)
	if condition == nil || condition.Status != metav1.ConditionFalse {
		return nil
	}
	// These observations make delivery unsafe even with unlimited spend. A
	// connecting runtime or temporary observation outage retains durable queuing.
	switch condition.Reason {
	case _remoteSessionMissingReason, "OwnershipMismatch", "ConfigurationMismatch",
		"InvalidConfiguration", "RemoteSessionFailed",
		"InvalidRemoteSession", "HostedSetupFailed":
		return errUnavailable
	default:
		return nil
	}
}

func confirmedMissingRemote(session *agentsv1alpha1.Session) bool {
	condition := meta.FindStatusCondition(session.Status.Conditions, _readyCondition)
	return session.Status.OpenAISessionID != "" && condition != nil &&
		condition.Status == metav1.ConditionFalse && condition.Reason == _remoteSessionMissingReason &&
		condition.ObservedGeneration == session.Generation
}

func (g *Gateway) enqueue(ctx context.Context, session *agentsv1alpha1.Session, key, text, targetTurn string, event json.RawMessage) error {
	return g.store.Update(ctx, string(session.UID), func(r *Record) error {
		// Initial classification is retained when an HTTP retry arrives after creation.
		for _, submission := range r.Submissions {
			if submission.Key == key {
				if r.Archived && submission.ReplayDigest != "" {
					candidate := Submission{Text: text, TargetTurn: targetTurn, Event: event}
					if submissionDigest(candidate) != submission.ReplayDigest {
						return errConflict
					}
					return nil
				}
				if submission.Text != text || submission.TargetTurn != targetTurn {
					return errConflict
				}
				return nil
			}
		}
		initial := false
		if session.Status.OpenAISessionID == "" && session.Spec.EnvironmentRef != nil {
			if session.Status.EnvironmentSnapshot != nil {
				initial = session.Status.EnvironmentSnapshot.Profile.Mode == agentsv1alpha1.EnvironmentModeNone
			} else {
				initial = r.InitialKey != "" || r.History.Status == _gatewayNoneStatus
			}
		}
		if targetTurn != "" && session.Status.OpenAISessionID == "" {
			return errors.New("no remote turn to cancel")
		}
		if initial && r.InitialKey != "" {
			initial = false
		}
		if initial && session.Status.CreationAttempted {
			return errors.New("creation was already claimed without gateway input; audit required")
		}
		if event == nil && !initial {
			event = inputEvent(text)
		}
		if err := addSubmission(r, Submission{Key: key, Text: text, TargetTurn: targetTurn, Event: event, Initial: initial, CreatedAt: time.Now().UTC()}); err != nil {
			return err
		}
		if initial {
			r.InitialKey = key
		}
		return nil
	})
}

func (g *Gateway) decide(ctx context.Context, uid, id, decision, subject string) error {
	return g.store.Update(ctx, uid, func(r *Record) error {
		if r.Closed {
			return errClosed
		}
		action, ok := r.Actions[id]
		if !ok || !action.Active {
			return errors.New("action is no longer pending")
		}
		if action.Decision != "" {
			if action.Decision == decision {
				return nil
			}
			return errConflict
		}
		if len(action.Result) > 0 || action.SubmissionKey != "" {
			return errConflict
		}
		if action.Pending.Type == _computerApproval {
			if _, err := approvalEvent(action.Pending, decision); err != nil {
				return err
			}
		}
		now := time.Now().UTC()
		action.Decision = decision
		action.DecidedBy = subject
		action.DecidedAt = &now
		r.Actions[id] = action
		return nil
	})
}

func (g *Gateway) serveEvents(w http.ResponseWriter, r *http.Request, namespace, uid string) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unavailable", http.StatusServiceUnavailable)
		return
	}
	cursor := uint64(0)
	if header := r.Header.Get("Last-Event-ID"); header != "" {
		parsed, err := strconv.ParseUint(header, 10, 64)
		if err != nil {
			http.Error(w, "Invalid event cursor", http.StatusBadRequest)
			return
		}
		cursor = parsed
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	controller := http.NewResponseController(w)
	initialized := false
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		record, err := g.store.GetInNamespace(r.Context(), namespace, uid)
		if err != nil {
			return
		}
		_ = controller.SetWriteDeadline(time.Now().Add(10 * time.Second))
		if !initialized || cursor > record.Sequence || len(record.Events) > 0 && cursor+1 < record.Events[0].Sequence {
			raw, _ := json.Marshal(map[string]any{_typeField: "gateway.recovery.snapshot", _historyField: record.History, "actions": record.Actions, _closedStatus: record.Closed, "recovery_required": record.RecoveryRequired})
			if _, err := io.WriteString(w, "id: "+strconv.FormatUint(record.Sequence, 10)+"\nevent: snapshot\ndata: "+string(raw)+"\n\n"); err != nil {
				return
			}
			cursor = record.Sequence
			initialized = true
		}
		for _, event := range record.Events {
			if event.Sequence <= cursor {
				continue
			}
			if _, err := io.WriteString(w, "id: "+strconv.FormatUint(event.Sequence, 10)+"\nevent: update\ndata: "+string(event.Data)+"\n\n"); err != nil {
				return
			}
			cursor = event.Sequence
		}
		if _, err := io.WriteString(w, ": heartbeat\n\n"); err != nil {
			return
		}
		flusher.Flush()
		if record.Closed {
			return
		}
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
		}
	}
}

func validKey(key string) bool {
	if len(key) == 0 || len(key) > 128 || strings.HasPrefix(key, "action-") {
		return false
	}
	for _, r := range key {
		if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') && !strings.ContainsRune("-_.:", r) {
			return false
		}
	}
	return true
}

func decode(w http.ResponseWriter, r *http.Request, target any) error {
	r.Body = http.MaxBytesReader(w, r.Body, _maxInputBytes+1024)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("unexpected trailing JSON")
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func methodError(w http.ResponseWriter) {
	http.Error(w, "Unsupported method", http.StatusMethodNotAllowed)
}
func writeError(w http.ResponseWriter, err error) {
	switch {
	case apierrors.IsNotFound(err):
		http.Error(w, "Session not found", http.StatusNotFound)
	case errors.Is(err, errConflict):
		http.Error(w, "Idempotency or decision conflict", http.StatusConflict)
	case errors.Is(err, errClosed):
		http.Error(w, "Session is closed", http.StatusGone)
	default:
		http.Error(w, "Gateway request could not be persisted or resolved", http.StatusServiceUnavailable)
	}
}

// Server joins the HTTP server and worker on shutdown. Supplying a certificate
// and key enables TLS; otherwise it serves HTTP.
type Server struct {
	Gateway  *Gateway
	Address  string
	CertFile string
	KeyFile  string
}

func (s *Server) NeedLeaderElection() bool { return true }
func (s *Server) Start(ctx context.Context) error {
	if (s.CertFile == "") != (s.KeyFile == "") {
		return errors.New("gateway TLS requires both certificate and key")
	}
	listener, err := net.Listen("tcp", s.Address)
	if err != nil {
		return err
	}
	defer func() { _ = listener.Close() }()
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	server := gatewayHTTPServer(workerCtx, s.Gateway.InstrumentedHandler())
	results := make(chan error, 2) // One completion from each of the two joined tasks.
	var wg sync.WaitGroup
	wg.Go(func() { results <- s.Gateway.Start(workerCtx) })
	wg.Go(func() {
		if s.CertFile != "" {
			results <- server.ServeTLS(listener, s.CertFile, s.KeyFile)
			return
		}
		results <- server.Serve(listener)
	})
	select {
	case <-ctx.Done():
		err = nil
	case err = <-results:
	}
	cancel()
	_ = server.Close()
	wg.Wait()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// Finite ordinary writes cannot retain request slots indefinitely. SSE handlers
// refresh their own shorter write deadlines as each event batch is sent.
// HTTP/1 is also the explicit protocol policy used by the manager's listeners.
func gatewayHTTPServer(ctx context.Context, handler http.Handler) *http.Server {
	protocols := &http.Protocols{}
	protocols.SetHTTP1(true)
	return &http.Server{
		Handler: handler, Protocols: protocols,
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second,
		WriteTimeout: 30 * time.Second, IdleTimeout: time.Minute, MaxHeaderBytes: 16 << 10,
		BaseContext: func(net.Listener) context.Context { return ctx },
	}
}
