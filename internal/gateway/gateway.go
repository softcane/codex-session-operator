package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync"
	"time"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	"github.com/softcane/codex-session-operator/internal/openaiapi"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Provider is the gateway's message I/O boundary, independent of lifecycle POSTs.
type Provider interface {
	SubmitInput(context.Context, string, string, json.RawMessage) error
	SavedHistory(context.Context, string) (openaiapi.History, error)
	OpenMessageStream(context.Context, string) (openaiapi.MessageStream, error)
}

var _ Provider = (*openaiapi.Client)(nil)

// Function handlers must recover by OperationID after a lost response or restart.
// Consequential handlers always require approval. Never register a non-idempotent
// external side effect here without downstream deduplication or outcome lookup.
type Function struct {
	Consequential bool
	// DownstreamDeduplicates attests that external writes recover by Operation.ID.
	DownstreamDeduplicates bool
	Execute                func(context.Context, Operation) (string, error)
}

type Operation struct {
	ID        string
	Namespace string
	Arguments json.RawMessage
}

type watch struct {
	ctx                 context.Context
	environmentID       string
	environmentRevision uint64
	environmentBlocked  bool
	verifiedRevision    uint64
	environmentVerified bool
	cancel              context.CancelFunc
	done                chan struct{}
	stream              openaiapi.MessageStream
	once                sync.Once
}

// Gateway owns bounded streams and the durable outbox. Start must be joined by
// its manager; HTTP handlers enqueue work and do not own background execution.
type Gateway struct {
	sessionWriter client.Client
	profiles      *ProfileResolver
	workMu        sync.Mutex
	store         Storage
	reader        client.Reader
	provider      Provider
	functions     map[string]Function
	subject       string
	mu            sync.Mutex
	ctx           context.Context
	worker        *Gateway
	gates         *[64]sync.Mutex
	watches       map[string]*watch
	wg            sync.WaitGroup
	streamSlots   chan struct{}
	requestSlots  chan struct{}
	streamCounts  map[streamScope]int
	metricsOnce   sync.Once
	metrics       *gatewayMetrics
}

func New(store Storage, reader client.Reader, provider Provider, subject string, functions map[string]Function) (*Gateway, error) {
	if store == nil || reader == nil || provider == nil {
		return nil, errors.New("gateway dependencies required")
	}
	copied := make(map[string]Function, len(functions))
	for name, function := range functions {
		if name == _echoFunction || name == _noteFunction || function.Execute == nil || function.Consequential && !function.DownstreamDeduplicates {
			return nil, errors.New("invalid function registration")
		}
		copied[name] = function
	}
	if subject == "" {
		subject = "gateway"
	}
	if len(subject) > 256 || strings.TrimSpace(subject) != subject || strings.ContainsAny(subject, "\r\n\t") {
		return nil, errors.New("invalid gateway replay subject")
	}
	// Ordinary requests retain capacity even when every stream slot is occupied.
	// Both admission paths reject excess requests instead of queueing goroutines.
	return &Gateway{store: store, reader: reader, provider: provider, subject: subject, functions: copied,
		profiles: &ProfileResolver{reader: reader},
		gates:    &[64]sync.Mutex{}, watches: make(map[string]*watch),
		requestSlots: make(chan struct{}, 64), streamSlots: make(chan struct{}, 32),
		streamCounts: make(map[streamScope]int)}, nil
}

func (g *Gateway) gate(uid string) *sync.Mutex {
	sum := sha256.Sum256([]byte(uid))
	return &g.gates[sum[0]%uint8(len(g.gates))]
}

func (g *Gateway) NeedLeaderElection() bool { return true }

func (g *Gateway) Start(ctx context.Context) error {
	var err error
	if store, ok := g.store.(DeliveryStorage); ok {
		err = g.startShared(ctx, store)
	} else {
		err = g.runWorker(ctx)
	}
	// Storage honors cancellation; stopping the service is still a normal exit.
	if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
		return nil
	}
	return err
}

func (g *Gateway) runWorker(ctx context.Context) error {
	metrics := g.metricState()
	metrics.workerContext.Store(&workerMetricContext{ctx: ctx})
	metrics.workerLeader.Store(1)
	if owned, ok := g.store.(*PostgreSQLStore); ok && owned.owner != "" {
		metrics.workerEpoch.Store(owned.epoch)
	}
	g.mu.Lock()
	g.ctx = ctx
	g.mu.Unlock()
	defer func() {
		metrics.workerLeader.Store(0)
		g.mu.Lock()
		g.ctx = nil
		watches := g.watches
		g.watches = make(map[string]*watch)
		g.mu.Unlock()
		for _, watch := range watches {
			watch.stop()
		}
		g.wg.Wait()
	}()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		g.maintainStore(ctx)
		records, err := g.activeRecords(ctx)
		if err != nil {
			return err
		}
		for _, record := range records {
			if ctx.Err() != nil {
				return nil
			}
			if record.Closed || record.DeliveryHeld {
				continue
			}
			if record.Work != nil {
				if err := g.syncWork(ctx, record.UID); err != nil {
					if writeErr := g.store.Update(ctx, record.UID, func(r *Record) error {
						r.Work.LastError = "Work provisioning or delivery pending; inspect the Session and creation claim"
						return nil
					}); writeErr != nil {
						return writeErr
					}
				}
				continue
			}
			if err := g.sync(ctx, record.UID); err != nil {
				if writeErr := g.store.Update(ctx, record.UID, func(r *Record) error {
					r.LastError = "Recovery or delivery pending; remote details withheld"
					return nil
				}); writeErr != nil {
					return writeErr
				}
			}
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

// InitialInput is read by the lifecycle controller before its optimistic claim.
// Only the first persisted message is eligible for none-mode Session creation.
func (g *Gateway) InitialInput(ctx context.Context, uid string) (string, string, error) {
	if err := g.deliveryEnabled(ctx); err != nil {
		return "", "", err
	}
	record, err := g.store.Get(ctx, uid)
	if errors.Is(err, os.ErrNotExist) {
		return "", "", nil
	}
	if err != nil {
		return "", "", err
	}
	if record.DeliveryHeld {
		return "", "", errRestoreHeld
	}
	if record.Closed {
		return "", "", errClosed
	}
	for _, submission := range record.Submissions {
		if submission.Key == record.InitialKey && submission.Initial {
			key := submission.RemoteKey
			if key == "" {
				key = submission.Key
			}
			return submission.Text, key, nil
		}
	}
	return "", "", nil
}

// Quiesce fences this process's submissions/functions before lifecycle cleanup.
// The caller's deletion timestamp also prevents new HTTP input and worker sends.
func (g *Gateway) Quiesce(ctx context.Context, uid, remoteID string) error {
	if err := g.deliveryEnabled(ctx); err != nil {
		return err
	}
	if record, err := g.store.Get(ctx, uid); err == nil && record.DeliveryHeld {
		return errRestoreHeld
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := g.store.Get(ctx, uid); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	gate := g.gate(uid)
	gate.Lock()
	defer gate.Unlock()
	if err := g.store.Update(ctx, uid, func(r *Record) error { r.Closed = true; return nil }); err != nil {
		return err
	}
	g.stopWatch(uid)
	if remoteID == "" {
		return nil
	}
	history, err := g.provider.SavedHistory(ctx, remoteID)
	if errors.Is(err, openaiapi.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if !historyOwnedBy(history, uid, remoteID, "") {
		return errors.New("cleanup Session identity mismatch")
	}
	if history.Status != "in_progress" && history.Status != "requires_action" {
		return nil
	}
	key := resultKey(uid, "cleanup-cancel")
	event := json.RawMessage(`{"type":"agent.session.input.cancel"}`)
	remoteKey := wireKey(uid, key)
	if err := g.store.Update(ctx, uid, func(r *Record) error {
		for _, submission := range r.Submissions {
			if submission.Key == key {
				remoteKey = submission.RemoteKey
				if remoteKey == "" {
					remoteKey = submission.Key
				}
				return nil
			}
		}
		if len(r.Submissions) >= _maxSubmissions {
			return errors.New("cleanup submission capacity reached")
		}
		r.Submissions = append(r.Submissions, Submission{Key: key, RemoteKey: wireKey(uid, key), Event: event, CreatedAt: time.Now().UTC()})
		return nil
	}); err != nil {
		return err
	}
	if err := g.provider.SubmitInput(ctx, remoteID, remoteKey, event); err != nil {
		return err
	}
	return g.store.Update(ctx, uid, func(r *Record) error {
		for i := range r.Submissions {
			if r.Submissions[i].Key == key {
				r.Submissions[i].Accepted = true
			}
		}
		return nil
	})
}

func (g *Gateway) stopWatch(uid string) {
	g.mu.Lock()
	worker := g.worker
	g.mu.Unlock()
	if worker != nil {
		worker.stopWatch(uid)
	}
	g.mu.Lock()
	w := g.watches[uid]
	delete(g.watches, uid)
	g.mu.Unlock()
	if w != nil {
		w.stop()
		<-w.done
	}
}

func (g *Gateway) ensureWatch(uid, id string) error {
	g.mu.Lock()
	if w := g.watches[uid]; w != nil {
		select {
		case <-w.done:
			delete(g.watches, uid)
		default:
			g.mu.Unlock()
			return nil
		}
	}
	ctx := g.ctx
	g.mu.Unlock()
	if ctx == nil || ctx.Err() != nil {
		return errors.New("gateway worker is not running")
	}
	streamCtx, cancel := context.WithCancel(ctx)
	stream, err := g.provider.OpenMessageStream(streamCtx, id)
	if err != nil {
		cancel()
		return err
	}
	w := &watch{ctx: streamCtx, cancel: cancel, done: make(chan struct{}), stream: stream}
	g.mu.Lock()
	if g.ctx == nil || g.ctx.Err() != nil {
		g.mu.Unlock()
		cancel()
		_ = stream.Close()
		return context.Canceled
	}
	g.watches[uid] = w
	metrics := g.metricState()
	metrics.messageStream.Add(1)
	g.wg.Go(func() {
		defer close(w.done)
		defer metrics.messageStream.Add(-1)
		defer w.stop()
		for stream.Next() {
			event := stream.Event()
			var identity struct {
				SessionID string `json:"session_id"`
			}
			if json.Unmarshal(event, &identity) != nil || identity.SessionID != id {
				continue
			}
			g.observeExecutionEvent(w, event)
			if err := g.store.Update(streamCtx, uid, func(r *Record) error {
				if r.Closed {
					return errClosed
				}
				appendEvent(r, event)
				return nil
			}); err != nil {
				break
			}
		}
		// Revoke the live execution proof before a detached storage write can wait.
		w.stop()
		// Persist the recovery marker even when stream cancellation ended the watch.
		// Storage adds its own short deadline; this never starts remote delivery.
		_ = g.store.Update(context.WithoutCancel(streamCtx), uid, func(r *Record) error { r.RecoveryRequired = true; return nil })
	})
	g.mu.Unlock()
	return nil
}

func (g *Gateway) sync(ctx context.Context, uid string) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	gate := g.gate(uid)
	gate.Lock()
	defer gate.Unlock()
	record, err := g.store.Get(ctx, uid)
	if err != nil {
		return err
	}
	if stoppedRecord(record) {
		return nil
	}
	var session agentsv1alpha1.Session
	err = g.reader.Get(ctx, client.ObjectKey{Namespace: record.Namespace, Name: record.Name}, &session)
	if apierrors.IsNotFound(err) || (err == nil && (string(session.UID) != uid || !session.DeletionTimestamp.IsZero())) {
		g.stopWatch(uid)
		return g.store.Update(ctx, uid, func(r *Record) error {
			r.Closed = true
			r.TerminalReason = "SessionMissing"
			return nil
		})
	}
	if err != nil {
		return err
	}
	id := session.Status.OpenAISessionID
	if id != "" && record.RemoteID != "" && record.RemoteID != id {
		return errors.New("remote identity changed")
	}
	if confirmedMissingRemote(&session) {
		return errors.Join(openaiapi.ErrNotFound, g.sealMissingRemote(ctx, uid))
	}
	if err := g.sessionInputOpen(ctx, &session); err != nil {
		g.stopWatch(uid)
		return err
	}
	if id == "" {
		return nil
	}
	history, err := g.provider.SavedHistory(ctx, id)
	if errors.Is(err, openaiapi.ErrNotFound) {
		return errors.Join(err, g.sealMissingRemote(ctx, uid))
	}
	if err != nil {
		return err
	}
	if !historyOwnedBy(history, uid, id, session.Status.OwnerScope) {
		g.stopWatch(uid)
		return errors.New("history Session identity mismatch")
	}

	if err := g.store.Update(ctx, uid, func(r *Record) error {
		r.RemoteID = id
		for i := range r.Submissions {
			if r.Submissions[i].Initial {
				if session.Status.EnvironmentSnapshot != nil && session.Status.EnvironmentSnapshot.Profile.Mode == agentsv1alpha1.EnvironmentModeNone {
					r.Submissions[i].Accepted = true
				} else {
					r.Submissions[i].Initial = false
					r.Submissions[i].Event = inputEvent(r.Submissions[i].Text)
				}
			}
		}
		return nil
	}); err != nil {
		return err
	}
	// Subscribe before sending. Failed subscription still permits saved-state
	// recovery and keyed delivery, with an explicit recovery indicator.
	streamErr := g.ensureWatch(uid, id)
	g.bindExecutionWatch(uid, session.Status.EnvironmentID)

	if err := g.recover(ctx, uid, history, streamErr != nil); err != nil {
		return err
	}
	var beforeActions agentsv1alpha1.Session
	if err := g.reader.Get(ctx, client.ObjectKeyFromObject(&session), &beforeActions); err != nil {
		return err
	}
	if string(beforeActions.UID) != uid || !beforeActions.DeletionTimestamp.IsZero() {
		return errClosed
	}
	if !sameRemoteSession(&session, &beforeActions) {
		return errors.New("remote identity changed before action execution")
	}
	if err := g.sessionInputOpen(ctx, &beforeActions); err != nil {
		return err
	}
	if err := g.processActionsIfReady(ctx, &beforeActions, uid, history); err != nil {
		return err
	}
	return g.deliver(ctx, &session, uid, history)
}

func (g *Gateway) sealMissingRemote(ctx context.Context, uid string) error {
	g.stopWatch(uid)
	return g.store.Update(ctx, uid, func(r *Record) error {
		// Provider 404 and a current controller 404 observation seal the same
		// identity. Only explicit new work may create another Session.
		r.Closed = true
		r.TerminalReason = _remoteSessionMissingReason
		r.RecoveryRequired = true
		r.LastError = "Remote Session is missing; saved history retained and delivery stopped"
		for id, action := range r.Actions {
			action.Active = false
			r.Actions[id] = action
		}
		return nil
	})
}

func (g *Gateway) processActionsIfReady(ctx context.Context, session *agentsv1alpha1.Session, uid string, history openaiapi.History) error {
	if !spendControlReady(session) {
		return nil
	}
	return g.processActions(ctx, uid, history)
}

func spendHeld(session *agentsv1alpha1.Session, submission Submission) bool {
	return submission.TargetTurn == "" && !spendControlReady(session)
}

func (g *Gateway) deliveryAllowed(ctx context.Context, session *agentsv1alpha1.Session, uid string, submission Submission) (bool, error) {
	// Re-read the primary before every send so deletion, UID recreation and spend
	// readiness changes are observed as close to the provider call as possible.
	var current agentsv1alpha1.Session
	if err := g.reader.Get(ctx, client.ObjectKeyFromObject(session), &current); err != nil {
		return false, err
	}
	if string(current.UID) != uid || !current.DeletionTimestamp.IsZero() {
		return false, errClosed
	}
	if !sameRemoteSession(session, &current) {
		return false, errors.New("remote identity changed before input delivery")
	}
	if err := g.sessionInputOpen(ctx, &current); err != nil {
		return false, err
	}
	if submission.TargetTurn == "" {
		if ready, _ := g.executionReady(ctx, &current); !ready {
			return false, nil
		}
	}
	return !spendHeld(&current, submission), nil
}

func (g *Gateway) deliver(ctx context.Context, session *agentsv1alpha1.Session, uid string, history openaiapi.History) error {

	record, err := g.store.Get(ctx, uid)
	if err != nil {
		return err
	}
	sent := 0
	for i, submission := range record.Submissions {
		if err := ctx.Err(); err != nil {
			return err
		}
		if sent >= 16 {
			break
		}
		if submission.Accepted || submission.Initial || submission.Blocked != "" {
			continue
		}
		if spendHeld(session, submission) {
			continue
		}
		if strings.HasPrefix(submission.Key, "action-") {
			pending := false
			for _, action := range record.Actions {
				if action.SubmissionKey == submission.Key && action.Active {
					pending = true
				}
			}
			if !pending {
				if err := g.store.Update(ctx, uid, func(r *Record) error {
					r.Submissions[i].Blocked = "Action no longer pending; result delivery will not be replayed"
					return nil
				}); err != nil {
					return err
				}
				continue
			}
		}
		if submission.AttemptedAt != nil && time.Since(*submission.AttemptedAt) > 10*time.Minute {
			return g.store.Update(ctx, uid, func(r *Record) error {
				r.Submissions[i].Blocked = "Retry window ended; audit provider acceptance before retry"
				return nil
			})
		}
		cancelActive, err := g.cancellationTargetCurrent(ctx, session, uid, history.ID, submission, i)
		if err != nil {
			return err
		}
		if !cancelActive {
			continue
		}
		allowed, err := g.deliveryAllowed(ctx, session, uid, submission)
		if err != nil {
			return err
		}
		if !allowed {
			continue
		}
		if err := g.store.Update(ctx, uid, func(r *Record) error {
			now := time.Now().UTC()
			if stoppedRecord(*r) {
				return errClosed
			}
			r.Submissions[i].Attempts++
			if r.Submissions[i].AttemptedAt == nil {
				r.Submissions[i].AttemptedAt = &now
			}
			return nil
		}); err != nil {
			return err
		}
		// The database claim may have waited behind another transaction. Re-read
		// compute and Session identity after that wait, before any provider POST.
		allowed, err = g.claimedSendAllowed(ctx, session, uid, submission)
		if err != nil || !allowed {
			return err
		}
		sent++
		if err := g.provider.SubmitInput(ctx, session.Status.OpenAISessionID, submission.RemoteKey, submission.Event); err != nil {
			return err
		}
		if err := g.store.Update(ctx, uid, func(r *Record) error { r.Submissions[i].Accepted = true; r.LastError = ""; return nil }); err != nil {
			return err
		}
	}
	return nil
}

func (g *Gateway) cancellationTargetCurrent(ctx context.Context, session *agentsv1alpha1.Session,
	uid, historyID string, submission Submission, index int) (bool, error) {
	if submission.TargetTurn == "" {
		return true, nil
	}
	fresh, err := g.provider.SavedHistory(ctx, session.Status.OpenAISessionID)
	if err != nil {
		return false, err
	}
	if !historyOwnedBy(fresh, uid, historyID, session.Status.OwnerScope) {
		return false, errors.New("cancellation ownership changed")
	}
	if activeTurn(fresh, submission.TargetTurn) {
		return true, nil
	}
	err = g.store.Update(ctx, uid, func(r *Record) error {
		r.Submissions[index].Blocked = "Target turn ended; cancellation will not be retried"
		return nil
	})
	return false, err
}

func historyOwnedBy(history openaiapi.History, uid, remoteID, ownerScope string) bool {
	return history.ID == remoteID && history.Metadata["kubernetes_uid"] == uid &&
		history.Metadata["operator"] == "codex-session-operator" &&
		(ownerScope == "" || history.Metadata["owner_scope"] == ownerScope)
}

func (g *Gateway) recover(ctx context.Context, uid string, history openaiapi.History, degraded bool) error {
	return g.store.Update(ctx, uid, func(r *Record) error {
		if !reflect.DeepEqual(r.History, history) || r.RecoveryRequired {
			raw, _ := json.Marshal(map[string]any{_typeField: "gateway.history.snapshot", _historyField: history})
			appendEvent(r, raw)
		}
		r.History = history
		r.RecoveryRequired = degraded
		r.LastError = ""
		for id, action := range r.Actions {
			action.Active = false
			r.Actions[id] = action
		}
		for _, pending := range history.Actions {
			id := actionID(pending)
			action, found := r.Actions[id]
			if found && !sameAction(action.Pending, pending) {
				return errors.New("provider action ID reused with different arguments")
			}
			if !found && len(r.Actions) >= _maxSubmissions {
				return errors.New("action capacity reached")
			}
			action.Pending = pending
			action.Active = true
			r.Actions[id] = action
		}
		return nil
	})
}

func activeTurn(history openaiapi.History, id string) bool {
	for _, raw := range history.Turns {
		var turn struct {
			ID         string  `json:"id"`
			Status     string  `json:"status"`
			SubagentID *string `json:"subagent_id"`
		}
		if json.Unmarshal(raw, &turn) == nil && turn.ID == id && turn.SubagentID == nil {
			return turn.Status == "in_progress" || turn.Status == "waiting" || turn.Status == "queued"
		}
	}
	return false
}

// ValidateTools prevents starting work the registered gateway cannot answer.
func (g *Gateway) ValidateTools(ctx context.Context, fields map[string]any) error {
	if err := g.deliveryEnabled(ctx); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	tools, ok := fields["tools"].([]any)
	if !ok && fields["tools"] != nil {
		return errors.New("invalid tool configuration")
	}
	for _, raw := range tools {
		tool, ok := raw.(map[string]any)
		if !ok {
			return errors.New("invalid tool configuration")
		}
		if tool[_typeField] != "function" {
			continue
		}
		name, _ := tool["name"].(string)
		if name != _echoFunction && name != _noteFunction {
			if _, ok := g.functions[name]; !ok {
				return fmt.Errorf("function %q has no authorized gateway handler", name)
			}
		}
	}
	return nil
}

func (g *Gateway) processActions(ctx context.Context, uid string, history openaiapi.History) error {
	for _, pending := range history.Actions {
		if pending.Type == "environment_connection" {
			continue
		} // Lifecycle owns executor reconnection.
		if pending.Type != _functionCall && pending.Type != _computerApproval {
			return errors.New("unsupported required action")
		}
		if pending.TurnID == "" || pending.Type == _functionCall && pending.CallID == "" || pending.Type == _computerApproval && pending.RequestID == "" {
			return errors.New("invalid required action identity")
		}
		record, err := g.store.Get(ctx, uid)
		if err != nil {
			return err
		}
		if stoppedRecord(record) {
			return errClosed
		}
		id := actionID(pending)
		action := record.Actions[id]
		if !action.Active || action.SubmissionKey != "" {
			continue
		}
		var current agentsv1alpha1.Session
		if err := g.reader.Get(ctx, client.ObjectKey{Namespace: record.Namespace, Name: record.Name}, &current); err != nil {
			return err
		}
		if string(current.UID) != uid || !current.DeletionTimestamp.IsZero() {
			return errClosed
		}
		if current.Status.OpenAISessionID != history.ID {
			return errors.New("remote identity changed before action execution")
		}
		if err := g.sessionInputOpen(ctx, &current); err != nil {
			return err
		}
		if !spendControlReady(&current) {
			continue
		}
		if pending.Type == _functionCall && g.consequentialFunction(pending.Name) {
			if ready, _ := g.executionReady(ctx, &current); !ready {
				continue
			}
		}
		if pending.Type == _computerApproval {
			if action.Decision == "" {
				continue
			}
			event, err := approvalEvent(pending, action.Decision)
			if err != nil {
				return err
			}
			if err := g.saveResult(ctx, uid, id, event); err != nil {
				return err
			}
			continue
		}
		if err := g.processFunction(ctx, uid, id, pending, action, record.Namespace); err != nil {
			return err
		}
	}
	return nil
}

func (g *Gateway) processFunction(ctx context.Context, uid, id string, pending openaiapi.PendingAction, action Action, namespace string) error {

	function, registered := g.functions[pending.Name]
	consequential := g.consequentialFunction(pending.Name)
	if consequential && action.Decision == "" {
		return nil
	}
	if consequential && action.Decision != _deny && !g.currentExecutionAllowed(ctx, uid) {
		return nil
	}
	if len(action.Result) == 0 {
		success := true
		output := ""
		if action.Decision == _deny {
			success = false
			output = "Action denied by an authorized approver"
		} else {
			switch pending.Name {
			case _echoFunction:
				output = string(pending.Arguments)
			case _noteFunction:
				var args struct {
					Note string `json:"note"`
				}
				if json.Unmarshal(pending.Arguments, &args) != nil || strings.TrimSpace(args.Note) == "" || len(args.Note) > 4096 {
					success = false
					output = "Invalid note argument"
				} else {
					// This reference write action and its result commit in ONE local transaction.
					event := toolResult(pending, true, "Note recorded")
					if err := g.store.Update(ctx, uid, func(r *Record) error {
						r.Notes[id] = args.Note
						a := r.Actions[id]
						a.Result = event
						r.Actions[id] = a
						return nil
					}); err != nil {
						return err
					}
					action.Result = event
				}
			default:
				if !registered {
					success = false
					output = "Function has no authorized handler"
				} else {
					// Persist a stable operation claim before any external side effect.
					if action.Blocked != "" {
						return errUnavailable
					}
					if err := g.store.Update(ctx, uid, func(r *Record) error {
						if stoppedRecord(*r) {
							return errClosed
						}
						a := r.Actions[id]
						if !a.Active || a.Decision != action.Decision {
							return errConflict
						}
						a.OperationID = resultKey(uid, id)
						now := time.Now().UTC()
						if a.AttemptedAt == nil {
							a.AttemptedAt = &now
						}
						r.Actions[id] = a
						return nil
					}); err != nil {
						return err
					}
					callCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
					if !g.currentExecutionAllowed(callCtx, uid) {
						cancel()
						return nil
					}
					value, err := function.Execute(callCtx, Operation{ID: resultKey(uid, id), Namespace: namespace, Arguments: append(json.RawMessage(nil), pending.Arguments...)})
					cancel()
					if err != nil {
						return errors.New("function outcome unknown; recovery by durable operation ID required")
					}
					if len(value) > _maxInputBytes {
						return errors.New("function output exceeds gateway limit")
					}
					output = value
				}
			}
		}
		if len(action.Result) == 0 {
			action.Result = toolResult(pending, success, output)
		}
	}
	if err := g.saveResult(ctx, uid, id, action.Result); err != nil {
		return err
	}
	return nil
}

func (g *Gateway) consequentialFunction(name string) bool {
	function, registered := g.functions[name]
	return name == _noteFunction || function.Consequential || !registered && name != _echoFunction
}

func (g *Gateway) saveResult(ctx context.Context, uid, id string, event json.RawMessage) error {
	return g.store.Update(ctx, uid, func(r *Record) error {
		action := r.Actions[id]
		key := resultKey(uid, id)
		if err := addSubmission(r, Submission{Key: key, RemoteKey: wireKey(uid, key), Event: event, CreatedAt: time.Now().UTC()}); err != nil {
			return err
		}
		action.Result = event
		action.SubmissionKey = key
		r.Actions[id] = action
		return nil
	})
}

func (w *watch) stop() {
	w.once.Do(func() {
		w.cancel()
		// Close is the stream contract's guarantee that a blocked Next exits.
		_ = w.stream.Close()
	})
}

func toolResult(p openaiapi.PendingAction, success bool, output string) json.RawMessage {
	event := map[string]any{_typeField: "agent.session.input.tool_result", "turn_id": p.TurnID, "call_id": p.CallID, "success": success}
	key := "output"
	if !success {
		key = _errorField
	}
	event[key] = output
	raw, _ := json.Marshal(event)
	return raw
}

func approvalEvent(p openaiapi.PendingAction, decision string) (json.RawMessage, error) {
	var request struct {
		Type string `json:"type"`
	}
	if json.Unmarshal(p.Request, &request) != nil {
		return nil, errors.New("invalid approval request")
	}
	response := map[string]any{_typeField: request.Type}
	switch request.Type {
	case "browser_origin_access":
		response["decision"] = decision
	case "browser_authentication":
		if decision != _deny {
			return nil, errors.New("interactive authentication is unsupported; deny to cancel")
		}
		response["action"] = "cancel"
	default:
		return nil, errors.New("unsupported computer approval request")
	}
	raw, _ := json.Marshal(map[string]any{_typeField: "agent.session.input.computer_use_approval_request_result", "request_id": p.RequestID, "response": response})
	return raw, nil
}

// sameAction ignores JSON formatting and object-key order while preserving number
// lexemes, exact IDs and values. RawMessage is compacted by durable serialization.
func sameAction(a, b openaiapi.PendingAction) bool {
	return a.Type == b.Type && a.TurnID == b.TurnID && a.CallID == b.CallID && a.RequestID == b.RequestID &&
		a.Name == b.Name && a.EnvironmentID == b.EnvironmentID && sameJSON(a.Arguments, b.Arguments) && sameJSON(a.Request, b.Request)
}

func sameJSON(a, b json.RawMessage) bool {
	if len(a) == 0 || len(b) == 0 {
		return len(a) == len(b)
	}
	var left, right any
	decoder := json.NewDecoder(strings.NewReader(string(a)))
	decoder.UseNumber()
	if decoder.Decode(&left) != nil {
		return false
	}
	decoder = json.NewDecoder(strings.NewReader(string(b)))
	decoder.UseNumber()
	if decoder.Decode(&right) != nil {
		return false
	}
	return reflect.DeepEqual(left, right)
}

func stoppedRecord(r Record) bool { return r.Closed || r.DeliveryHeld }
