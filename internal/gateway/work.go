package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	"github.com/softcane/codex-session-operator/internal/openaiapi"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ParseWorkRoutes rejects unsupported fields and incomplete trusted policy.
// Routes contain references rather than credentials or task-provided overrides.
func ParseWorkRoutes(raw []byte) ([]WorkRoute, error) {
	if len(raw) > _maxInputBytes {
		return nil, errors.New("work route configuration exceeds limit")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var routes []WorkRoute
	if decoder.Decode(&routes) != nil {
		return nil, errors.New("invalid work route configuration")
	}
	var trailing any
	if !errors.Is(decoder.Decode(&trailing), io.EOF) {
		return nil, errors.New("invalid work route configuration")
	}
	if _, err := copyWorkRoutes(routes); err != nil {
		return nil, err
	}
	return routes, nil
}

// EnableWork is startup configuration. It retains an isolated copy of each route.
func (g *Gateway) EnableWork(writer client.Client, routes []WorkRoute) error {
	if writer == nil {
		return errors.New("work Session writer required")
	}
	copied, err := copyWorkRoutes(routes)
	if err != nil {
		return err
	}
	if g.sessionWriter == nil {
		g.sessionWriter = writer
	}
	g.workRoutes = copied
	return nil
}

func copyWorkRoutes(routes []WorkRoute) (map[string]WorkRoute, error) {
	if len(routes) == 0 || len(routes) > _maxWorkRoutes {
		return nil, errors.New("work configuration requires 1–256 routes")
	}
	copied := make(map[string]WorkRoute, len(routes))
	for _, route := range routes {
		if !validWorkIdentifier(route.ID) || !validWorkIdentifier(route.Source) ||
			len(validation.IsDNS1123Label(route.Namespace)) != 0 ||
			len(validation.IsDNS1123Subdomain(route.Agent)) != 0 ||
			len(validation.IsDNS1123Subdomain(route.Environment)) != 0 || len(route.Vaults) > 16 {
			return nil, errors.New("invalid work route configuration")
		}
		if _, present, err := openaiapi.SessionSpendControlField(route.SpendControl); err != nil || !present {
			return nil, errors.New("work route requires a valid explicit spend choice")
		}
		if _, exists := copied[route.ID]; exists {
			return nil, errors.New("duplicate work route")
		}
		vaults := make(map[string]bool, len(route.Vaults))
		for _, name := range route.Vaults {
			if len(validation.IsDNS1123Subdomain(name)) != 0 || vaults[name] {
				return nil, errors.New("invalid work Vault reference")
			}
			vaults[name] = true
		}
		route.Vaults = append([]string(nil), route.Vaults...)
		route.SpendControl = route.SpendControl.DeepCopy()
		copied[route.ID] = route
	}
	return copied, nil
}

func validWorkIdentifier(value string) bool {
	if len(value) == 0 || len(value) > 256 {
		return false
	}
	for _, char := range value {
		if (char < 'a' || char > 'z') && (char < 'A' || char > 'Z') &&
			(char < '0' || char > '9') && !strings.ContainsRune("-_.:", char) {
			return false
		}
	}
	return true
}

func workIdentity(namespace, routeID, source, externalID string) string {
	// Length-delimited JSON avoids collisions between arbitrary external IDs and
	// routing components. Only the opaque digest crosses into Kubernetes metadata.
	raw, _ := json.Marshal([4]string{namespace, routeID, source, externalID})
	sum := sha256.Sum256(raw)
	return "work-" + hex.EncodeToString(sum[:])
}

func workSessionName(id string, attempt int) string {
	sum := sha256.Sum256([]byte(id))
	return "work-" + hex.EncodeToString(sum[:20]) + "-a" + strconv.Itoa(attempt)
}

func workPolicy(route WorkRoute, id string, attempt int) agentsv1alpha1.SessionSpec {
	metadata := map[string]string{"work_id": id, "work_attempt": strconv.Itoa(attempt)}
	policy := agentsv1alpha1.SessionSpec{
		AgentRef:           &agentsv1alpha1.AgentReference{Name: route.Agent},
		EnvironmentRef:     &agentsv1alpha1.AgentReference{Name: route.Environment},
		WorkspaceDirectory: "/workspace",
		SpendControl:       route.SpendControl.DeepCopy(),
		Metadata:           &metadata,
	}
	for _, name := range route.Vaults {
		policy.VaultRefs = append(policy.VaultRefs, agentsv1alpha1.AgentReference{Name: name})
	}
	return policy
}

func (g *Gateway) syncWork(ctx context.Context, id string) error {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	gate := g.gate(id)
	gate.Lock()
	defer gate.Unlock()
	record, err := g.store.Get(ctx, id)
	if err != nil {
		return err
	}
	if record.Work == nil {
		return errors.New("work configuration required")
	}
	if record.Work.State == _workClosed {
		return nil
	}
	if g.sessionWriter == nil || len(record.Work.Attempts) == 0 {
		return errors.New("work configuration required")
	}
	if record.Work.State != _workOpen && record.Work.State != _workClosing {
		return errors.New("invalid work state")
	}
	attempt := record.Work.Attempts[len(record.Work.Attempts)-1]
	var session agentsv1alpha1.Session
	err = g.reader.Get(ctx, client.ObjectKey{Namespace: record.Namespace, Name: attempt.SessionName}, &session)
	if apierrors.IsNotFound(err) {
		if attempt.CreationClaimed && attempt.SessionUID == "" {
			return errors.New("work Session creation outcome unknown; audit required")
		}
		if record.Work.State == _workClosing || attempt.SessionUID != "" {
			return g.finishWork(ctx, record, attempt)
		}
		session, err = g.createWorkSession(ctx, record, attempt)
		if err != nil {
			return err
		}
	} else if err != nil {
		return err
	} else if !attempt.CreationClaimed {
		return errors.New("unclaimed work Session exists; ownership audit required")
	}
	if err := checkWorkOwnership(record, attempt, &session); err != nil {
		return err
	}
	if attempt.SessionUID == "" || attempt.Phase != session.Status.Phase {
		if err := g.store.Update(ctx, id, func(r *Record) error {
			r.Work.Attempts[len(r.Work.Attempts)-1].SessionUID = string(session.UID)
			r.Work.Attempts[len(r.Work.Attempts)-1].Phase = session.Status.Phase
			return nil
		}); err != nil {
			return err
		}
	}
	if record.Work.State == _workClosing {
		uid := session.UID
		if err := g.sessionWriter.Delete(ctx, &session, &client.DeleteOptions{
			Preconditions: &metav1.Preconditions{UID: &uid},
		}); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
		return nil
	}
	if !session.DeletionTimestamp.IsZero() {
		return g.store.Update(ctx, id, func(r *Record) error { r.Work.State = _workClosing; return nil })
	}
	return g.queueWorkEvents(ctx, record, attempt, &session)
}

func (g *Gateway) createWorkSession(ctx context.Context, record Record, attempt WorkAttempt) (agentsv1alpha1.Session, error) {
	if attempt.CreationClaimed {
		return agentsv1alpha1.Session{}, errors.New("work Session creation outcome unknown; audit required")
	}
	if err := g.store.Update(ctx, record.UID, func(r *Record) error {
		r.Work.Attempts[len(r.Work.Attempts)-1].CreationClaimed = true
		return nil
	}); err != nil {
		return agentsv1alpha1.Session{}, err
	}
	session := agentsv1alpha1.Session{
		ObjectMeta: metav1.ObjectMeta{Namespace: record.Namespace, Name: attempt.SessionName,
			Annotations: map[string]string{_workAnnotation: record.UID}},
		Spec: *attempt.Policy.DeepCopy(),
	}
	if err := g.sessionWriter.Create(ctx, &session); err != nil {
		// An AlreadyExists response retains the claim for exact-identity discovery.
		// Transport failures also retain it; absence never authorizes a second POST.
		if apierrors.IsForbidden(err) || apierrors.IsInvalid(err) || apierrors.IsBadRequest(err) || apierrors.IsNotFound(err) {
			return agentsv1alpha1.Session{}, errors.Join(err, g.store.Update(ctx, record.UID, func(r *Record) error {
				r.Work.Attempts[len(r.Work.Attempts)-1].CreationClaimed = false
				return nil
			}))
		}
		return agentsv1alpha1.Session{}, err
	}
	return session, nil
}

func (g *Gateway) finishWork(ctx context.Context, record Record, attempt WorkAttempt) error {
	// Persist closing before closed even when deletion happened outside the gateway.
	if record.Work.State != _workClosing {
		if err := g.store.Update(ctx, record.UID, func(r *Record) error {
			r.Work.State = _workClosing
			return nil
		}); err != nil {
			return err
		}
	}
	if attempt.SessionUID != "" {
		conversation, err := g.store.Get(ctx, attempt.SessionUID)
		if errors.Is(err, os.ErrNotExist) {
			conversation = Record{UID: attempt.SessionUID}
		} else if err != nil {
			return err
		}
		if err := g.verifyRemoteAbsence(ctx, conversation); err != nil {
			return err
		}
	}
	return g.store.Update(ctx, record.UID, func(r *Record) error {
		current := &r.Work.Attempts[len(r.Work.Attempts)-1]
		if current.Number != attempt.Number {
			return errConflict
		}
		current.Cleaned = true
		r.Work.State = _workClosed
		r.Work.LastError = ""
		return nil
	})
}

func (g *Gateway) queueWorkEvents(ctx context.Context, record Record, attempt WorkAttempt, session *agentsv1alpha1.Session) error {
	resolved, _, err := g.lookup(ctx, record.Namespace, attempt.SessionName)
	if err != nil {
		return err
	}
	if err := checkWorkOwnership(record, attempt, resolved); err != nil || resolved.UID != session.UID {
		return errors.New("work Session changed before input delivery")
	}
	initialQueued := attempt.InitialQueued
	for _, event := range record.Work.Events {
		if event.Attempt != attempt.Number || event.Queued ||
			(event.Kind != _workTask && event.Kind != _workRestart && event.Kind != _workEvidence) {
			continue
		}
		if !initialQueued && event.Kind == _workEvidence {
			return errors.New("work initial task must be queued before evidence")
		}
		if err := g.enqueue(ctx, resolved, workEventKey(record.UID, attempt.Number, event.ID), event.Text, "", nil); err != nil {
			return err
		}
		if err := g.store.Update(ctx, record.UID, func(r *Record) error {
			for i := range r.Work.Events {
				if r.Work.Events[i].ID == event.ID && r.Work.Events[i].Attempt == attempt.Number {
					r.Work.Events[i].Queued = true
				}
			}
			r.Work.Attempts[len(r.Work.Attempts)-1].InitialQueued = true
			r.Work.LastError = ""
			return nil
		}); err != nil {
			return err
		}
		initialQueued = true
	}
	if record.Work.LastError != "" {
		return g.store.Update(ctx, record.UID, func(r *Record) error { r.Work.LastError = ""; return nil })
	}
	return nil
}

func workEventKey(id string, attempt int, eventID string) string {
	raw, _ := json.Marshal([3]string{id, strconv.Itoa(attempt), eventID})
	sum := sha256.Sum256(raw)
	return "work-event-" + hex.EncodeToString(sum[:])
}

func checkWorkOwnership(record Record, attempt WorkAttempt, session *agentsv1alpha1.Session) error {
	if session.UID == "" || record.Namespace != session.Namespace || attempt.SessionName != session.Name ||
		session.Annotations[_workAnnotation] != record.UID ||
		(attempt.SessionUID != "" && attempt.SessionUID != string(session.UID)) ||
		!reflect.DeepEqual(session.Spec, attempt.Policy) {
		return errors.New("work Session ownership or policy mismatch")
	}
	return nil
}

// workRecord falls back to the durable attempt index if a marker is removed.
// All prior attempts remain indexed to prevent reopening them through direct input.
func (g *Gateway) workRecord(ctx context.Context, session *agentsv1alpha1.Session) (Record, bool, error) {
	if id := session.Annotations[_workAnnotation]; id != "" {
		record, err := g.store.Get(ctx, id)
		return record, true, err
	}
	records, err := g.recordSummaries(ctx, session.Namespace, _workSegment)
	if err != nil {
		return Record{}, false, err
	}
	for _, record := range records {
		if record.Work == nil || record.Namespace != session.Namespace {
			continue
		}
		for _, attempt := range record.Work.Attempts {
			if attempt.SessionName == session.Name || (attempt.SessionUID != "" && attempt.SessionUID == string(session.UID)) {
				return record, true, nil
			}
		}
	}
	return Record{}, false, nil
}

func (g *Gateway) workSessionOpen(ctx context.Context, session *agentsv1alpha1.Session) error {
	record, found, err := g.workRecord(ctx, session)
	if err != nil || !found {
		return err
	}
	if record.Work == nil || record.Work.State != _workOpen || len(record.Work.Attempts) == 0 {
		return errClosed
	}
	attempt := record.Work.Attempts[len(record.Work.Attempts)-1]
	if !attempt.CreationClaimed || !attempt.InitialQueued || attempt.Cleaned ||
		attempt.SessionUID == "" || !session.DeletionTimestamp.IsZero() {
		return errUnavailable
	}
	return checkWorkOwnership(record, attempt, session)
}

func (g *Gateway) validateWorkSession(ctx context.Context, session *agentsv1alpha1.Session, environment *agentsv1alpha1.SessionEnvironmentSnapshot) error {
	record, found, err := g.workRecord(ctx, session)
	if err != nil || !found {
		return err
	}
	if record.Work == nil || record.Work.State != _workOpen || len(record.Work.Attempts) == 0 {
		return errClosed
	}
	attempt := record.Work.Attempts[len(record.Work.Attempts)-1]
	if !attempt.CreationClaimed || attempt.Cleaned || attempt.SessionUID == "" ||
		environment == nil || attempt.Policy.EnvironmentRef == nil ||
		environment.Name != attempt.Policy.EnvironmentRef.Name || !session.DeletionTimestamp.IsZero() {
		return errors.New("work Session requires its selected Environment and bound identity")
	}
	return checkWorkOwnership(record, attempt, session)
}
