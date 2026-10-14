package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	_workWorkerVault       = "evidence-vault"
	_workWorkerEnvironment = "runtime-v1"
	_workWorkerUnlimited   = "unlimited"
)

type workWorkerWriter struct {
	client.Client
	store       Storage
	workID      string
	creates     int
	deletes     int
	failCreate  error
	loseCreate  bool
	failDelete  bool
	claimAbsent bool
}

func (w *workWorkerWriter) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	w.creates++
	record, err := w.store.Get(context.Background(), w.workID)
	if err != nil {
		return err
	}
	if !record.Work.Attempts[len(record.Work.Attempts)-1].CreationClaimed {
		w.claimAbsent = true
	}
	if w.failCreate != nil {
		return w.failCreate
	}
	obj.SetUID(types.UID("work-session-" + strconv.Itoa(w.creates)))
	if err := w.Client.Create(ctx, obj, opts...); err != nil {
		return err
	}
	if w.loseCreate {
		w.loseCreate = false
		return io.ErrUnexpectedEOF
	}
	return nil
}

func (w *workWorkerWriter) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	w.deletes++
	if w.failDelete {
		return io.ErrUnexpectedEOF
	}
	return w.Client.Delete(ctx, obj, opts...)
}

func workerWorkRoute() WorkRoute {
	return WorkRoute{ID: "worker-route", Namespace: _testNamespace, Source: "ci", Agent: "investigator-v1",
		Environment: _workWorkerEnvironment, Vaults: []string{_workWorkerVault},
		SpendControl: &agentsv1alpha1.SessionSpendControl{Mode: _workWorkerUnlimited}}
}

func workerWorkGateway(t *testing.T, store Storage, mode string) (*Gateway, *workWorkerWriter, string) {
	t.Helper()
	route := workerWorkRoute()
	environment := &agentsv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Namespace: route.Namespace, Name: route.Environment},
		Spec: agentsv1alpha1.EnvironmentSpec{Mode: mode}}
	provider := simulatorForTest()
	provider.remotePresent = false
	g, k8s := testGateway(t, store, provider, environment)
	id := workIdentity(route.Namespace, route.ID, route.Source, "pipeline-123")
	writer := &workWorkerWriter{Client: k8s, store: store, workID: id}
	if err := g.EnableWork(writer, []WorkRoute{route}); err != nil {
		t.Fatal(err)
	}
	if err := store.Update(t.Context(), id, func(record *Record) error {
		record.Namespace = route.Namespace
		record.Name = workSessionName(id, 1)
		record.Work = &Work{ID: id, RouteID: route.ID, Source: route.Source, ExternalID: "pipeline-123", State: _workOpen,
			Attempts: []WorkAttempt{{Number: 1, SessionName: record.Name, Policy: workPolicy(route, id, 1)}},
			Events:   []WorkEvent{workerWorkEvent("task-1", _workTask, "Investigate this failing CI run.", 1)}}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return g, writer, id
}

func workerWorkEvent(id, kind, text string, attempt int) WorkEvent {
	sum := sha256.Sum256([]byte(id + "/" + kind + "/" + text))
	return WorkEvent{ID: id, Kind: kind, Text: text, Attempt: attempt,
		Digest: hex.EncodeToString(sum[:]), ReceivedAt: time.Now().UTC()}
}

func workerWorkRecord(t *testing.T, store Storage, id string) Record {
	t.Helper()
	record, err := store.Get(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return record
}

func workerWorkSession(t *testing.T, writer client.Reader, record Record) *agentsv1alpha1.Session {
	t.Helper()
	attempt := record.Work.Attempts[len(record.Work.Attempts)-1]
	var session agentsv1alpha1.Session
	if err := writer.Get(context.Background(), client.ObjectKey{Namespace: record.Namespace, Name: attempt.SessionName}, &session); err != nil {
		t.Fatal(err)
	}
	return &session
}

func TestWorkRoutesRejectUnknownPolicyAndCopySelection(t *testing.T) {
	route := workerWorkRoute()
	limit := int64(100)
	route.SpendControl = &agentsv1alpha1.SessionSpendControl{Mode: "limited", LimitCents: &limit}
	routes, err := ParseWorkRoutes(mustJSON(t, []WorkRoute{route}))
	if err != nil || len(routes) != 1 {
		t.Fatal(routes, err)
	}
	tests := []struct {
		name string
		raw  string
	}{
		{name: "secret", raw: `[{"id":"x","secret":"private"}]`},
		{name: "trailing", raw: string(mustJSON(t, []WorkRoute{route})) + `{}`},
		{name: "empty", raw: `[]`},
		{name: "missing spend", raw: `[{"id":"x","source":"ci","namespace":"team","agent":"a","environment":"e"}]`},
		{name: "cross namespace", raw: strings.ReplaceAll(string(mustJSON(t, []WorkRoute{route})), route.Agent, "other/agent")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := ParseWorkRoutes([]byte(tt.raw)); err == nil {
				t.Fatal("invalid route accepted")
			}
		})
	}
	g, k8s := testGateway(t, testStore(t), simulatorForTest())
	if err := g.EnableWork(k8s, []WorkRoute{route}); err != nil {
		t.Fatal(err)
	}
	route.Vaults[0] = "changed"
	*route.SpendControl.LimitCents = 200
	selected := g.workRoutes[route.ID]
	if selected.Vaults[0] != _workWorkerVault || *selected.SpendControl.LimitCents != 100 {
		t.Fatal("retained mutable route configuration")
	}
	policy := workPolicy(selected, "work-opaque", 1)
	policy.VaultRefs[0].Name = "another"
	*policy.SpendControl.LimitCents = 300
	if selected.Vaults[0] != _workWorkerVault || *selected.SpendControl.LimitCents != 100 || policy.Agent != nil {
		t.Fatal("policy aliases route or overrides saved Agent")
	}
}

func TestWorkWorkerReusesSessionAndQueuesTaskBeforeEvidence(t *testing.T) {
	store := testStore(t)
	g, writer, id := workerWorkGateway(t, store, "selfHosted")
	if err := store.Update(t.Context(), id, func(record *Record) error {
		record.Work.Events = append(record.Work.Events, workerWorkEvent("logs-1", _workEvidence, "Additional log evidence.", 1))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if err := g.syncWork(context.Background(), id); err != nil {
			t.Fatal(err)
		}
	}
	record := workerWorkRecord(t, store, id)
	attempt := record.Work.Attempts[0]
	conversation := workerWorkRecord(t, store, attempt.SessionUID)
	session := workerWorkSession(t, writer, record)
	if writer.creates != 1 || writer.claimAbsent || !attempt.InitialQueued || len(conversation.Submissions) != 2 {
		t.Fatal("work was duplicated or not durably claimed", writer.creates, writer.claimAbsent, attempt, conversation.Submissions)
	}
	if conversation.Submissions[0].Text != record.Work.Events[0].Text || conversation.Submissions[1].Text != record.Work.Events[1].Text {
		t.Fatal("evidence arrived before task")
	}
	if !reflect.DeepEqual(session.Spec, attempt.Policy) || session.Spec.Agent != nil || session.Spec.EnvironmentRef.Name != _workWorkerEnvironment {
		t.Fatal("selected configuration changed")
	}
	for _, event := range record.Work.Events {
		if !event.Queued || !validKey(workEventKey(id, 1, event.ID)) {
			t.Fatal("event not queued with bounded key")
		}
	}
	if err := g.workSessionOpen(t.Context(), session); err != nil {
		t.Fatal(err)
	}
	if err := g.validateWorkSession(t.Context(), session, &agentsv1alpha1.SessionEnvironmentSnapshot{Name: _workWorkerEnvironment, Profile: agentsv1alpha1.EnvironmentSpec{Mode: "selfHosted"}}); err != nil {
		t.Fatal(err)
	}
	session.Status.Phase = _readyCondition
	if err := writer.Update(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	if err := g.syncWork(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	if workerWorkRecord(t, store, id).Work.Attempts[0].Phase != _readyCondition {
		t.Fatal("verified Session phase not exposed on work")
	}
}

func TestWorkWorkerNoneModeReservesInitialInput(t *testing.T) {
	store := testStore(t)
	g, writer, id := workerWorkGateway(t, store, _noneMode)
	writer.loseCreate = true
	if err := g.syncWork(context.Background(), id); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatal(err)
	}
	record := workerWorkRecord(t, store, id)
	session := workerWorkSession(t, writer, record)
	if err := g.workSessionOpen(t.Context(), session); err == nil {
		t.Fatal("direct input can replace initial task before binding")
	}
	if err := g.syncWork(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	record = workerWorkRecord(t, store, id)
	conversation := workerWorkRecord(t, store, record.Work.Attempts[0].SessionUID)
	if len(conversation.Submissions) != 1 || !conversation.Submissions[0].Initial || conversation.InitialKey == "" {
		t.Fatal("none-mode input not reserved", conversation.Submissions)
	}
	text, key, err := g.InitialInput(context.Background(), record.Work.Attempts[0].SessionUID)
	if err != nil || text != record.Work.Events[0].Text || key != conversation.Submissions[0].RemoteKey {
		t.Fatal(text, key, err)
	}
	if err := g.workSessionOpen(t.Context(), workerWorkSession(t, writer, record)); err != nil {
		t.Fatal(err)
	}
}

func TestWorkWorkerRecoversAfterCommitCreateAndEnqueue(t *testing.T) {
	store, dsn := postgresForTest(t, 32)
	g, writer, id := workerWorkGateway(t, store, _noneMode)
	writer.loseCreate = true
	if err := g.syncWork(context.Background(), id); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err := OpenPostgreSQL(context.Background(), PostgreSQLConfig{DSN: dsn, AllowInsecureLocal: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	g.store = store
	writer.store = store
	if err := g.syncWork(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	if err := store.Update(t.Context(), id, func(record *Record) error {
		record.Work.Attempts[0].InitialQueued = false
		record.Work.Events[0].Queued = false
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := g.syncWork(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	record := workerWorkRecord(t, store, id)
	conversation := workerWorkRecord(t, store, record.Work.Attempts[0].SessionUID)
	if writer.creates != 1 || len(conversation.Submissions) != 1 || !record.Work.Attempts[0].InitialQueued {
		t.Fatal("restart repeated creation or task")
	}
}

func TestWorkWorkerDoesNotRepeatAmbiguousBeforeCommitCreate(t *testing.T) {
	store := testStore(t)
	g, writer, id := workerWorkGateway(t, store, "selfHosted")
	writer.failCreate = io.ErrUnexpectedEOF
	if err := g.syncWork(context.Background(), id); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatal(err)
	}
	writer.failCreate = nil
	for range 3 {
		if err := g.syncWork(context.Background(), id); err == nil {
			t.Fatal("ambiguous absent create was retried")
		}
	}
	if writer.creates != 1 || !workerWorkRecord(t, store, id).Work.Attempts[0].CreationClaimed {
		t.Fatal("lost durable creation claim")
	}
}

func TestWorkWorkerRetriesDefinitiveCreateRejection(t *testing.T) {
	store := testStore(t)
	g, writer, id := workerWorkGateway(t, store, "selfHosted")
	writer.failCreate = apierrors.NewForbidden(schema.GroupResource{Resource: _sessionsSegment}, "fixture", errors.New("denied"))
	if err := g.syncWork(context.Background(), id); !apierrors.IsForbidden(err) {
		t.Fatal(err)
	}
	if workerWorkRecord(t, store, id).Work.Attempts[0].CreationClaimed {
		t.Fatal("definitive create rejection retained claim")
	}
	writer.failCreate = nil
	if err := g.syncWork(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	if writer.creates != 2 {
		t.Fatal(writer.creates)
	}
}

func TestWorkWorkerClosesAndRequiresExplicitNewAttempt(t *testing.T) {
	store := testStore(t)
	g, writer, id := workerWorkGateway(t, store, "selfHosted")
	if err := g.syncWork(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	previous := workerWorkSession(t, writer, workerWorkRecord(t, store, id))
	if err := store.Update(t.Context(), id, func(record *Record) error { record.Work.State = _workClosing; return nil }); err != nil {
		t.Fatal(err)
	}
	writer.failDelete = true
	if err := g.syncWork(context.Background(), id); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatal(err)
	}
	if err := g.workSessionOpen(t.Context(), previous); !errors.Is(err, errClosed) {
		t.Fatal(err)
	}
	writer.failDelete = false
	for range 3 {
		if err := g.syncWork(context.Background(), id); err != nil {
			t.Fatal(err)
		}
	}
	record := workerWorkRecord(t, store, id)
	if record.Work.State != _workClosed || !record.Work.Attempts[0].Cleaned || writer.creates != 1 {
		t.Fatal("close replaced Session or lost replay record")
	}
	if err := store.Update(t.Context(), id, func(record *Record) error {
		record.Work.State = _workOpen
		record.Name = workSessionName(id, 2)
		record.Work.Attempts = append(record.Work.Attempts, WorkAttempt{Number: 2, SessionName: record.Name,
			Policy: workPolicy(workerWorkRoute(), id, 2)})
		record.Work.Events = append(record.Work.Events, workerWorkEvent("restart-1", _workRestart, "Reinvestigate with new evidence.", 2))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := g.syncWork(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	if writer.creates != 2 || workerWorkRecord(t, store, id).Work.Attempts[1].SessionUID == string(previous.UID) {
		t.Fatal("explicit restart did not create independent attempt")
	}
	if err := g.workSessionOpen(t.Context(), previous); err == nil {
		t.Fatal("prior attempt permits direct delivery")
	}
	delete(previous.Annotations, _workAnnotation)
	if err := g.workSessionOpen(t.Context(), previous); err == nil {
		t.Fatal("removed annotation bypasses prior attempt index")
	}
}

func TestWorkWorkerNeverReplacesDeletedBoundSession(t *testing.T) {
	store := testStore(t)
	g, writer, id := workerWorkGateway(t, store, "selfHosted")
	if err := g.syncWork(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	record := workerWorkRecord(t, store, id)
	session := workerWorkSession(t, writer, record)
	if err := writer.Client.Delete(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := g.syncWork(context.Background(), id); err != nil {
			t.Fatal(err)
		}
	}
	if writer.creates != 1 || workerWorkRecord(t, store, id).Work.State != _workClosed {
		t.Fatal("bound Session was silently replaced")
	}
}

func TestWorkWorkerRejectsForeignObjectAndUIDRecreation(t *testing.T) {
	t.Run("unclaimed foreign object", func(t *testing.T) {
		store := testStore(t)
		g, writer, id := workerWorkGateway(t, store, "selfHosted")
		record := workerWorkRecord(t, store, id)
		foreign := &agentsv1alpha1.Session{ObjectMeta: metav1.ObjectMeta{Namespace: record.Namespace, Name: record.Name, UID: "foreign"},
			Spec: *record.Work.Attempts[0].Policy.DeepCopy()}
		if err := writer.Client.Create(context.Background(), foreign); err != nil {
			t.Fatal(err)
		}
		if err := g.syncWork(context.Background(), id); err == nil || writer.creates != 0 {
			t.Fatal("foreign object adopted", err)
		}
	})
	t.Run("bound UID recreated", func(t *testing.T) {
		store := testStore(t)
		g, writer, id := workerWorkGateway(t, store, "selfHosted")
		if err := g.syncWork(context.Background(), id); err != nil {
			t.Fatal(err)
		}
		record := workerWorkRecord(t, store, id)
		original := workerWorkSession(t, writer, record)
		if err := writer.Client.Delete(context.Background(), original); err != nil {
			t.Fatal(err)
		}
		recreated := original.DeepCopy()
		recreated.UID = "foreign-recreation"
		recreated.ResourceVersion = ""
		if err := writer.Client.Create(context.Background(), recreated); err != nil {
			t.Fatal(err)
		}
		if err := g.syncWork(context.Background(), id); err == nil || writer.creates != 1 {
			t.Fatal("recreated UID adopted", err)
		}
		if err := g.workSessionOpen(t.Context(), recreated); err == nil {
			t.Fatal("recreated UID permits direct delivery")
		}
	})
}

func TestWorkWorkerCloseBeforeTaskTombstoneNeedsNoSession(t *testing.T) {
	store := testStore(t)
	g, writer, id := workerWorkGateway(t, store, "selfHosted")
	if err := store.Update(t.Context(), id, func(record *Record) error {
		record.Name = ""
		record.Work.State = _workClosed
		record.Work.Attempts = nil
		record.Work.Events = []WorkEvent{workerWorkEvent("close-1", _workClose, "", 0)}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := g.syncWork(context.Background(), id); err != nil || writer.creates != 0 {
		t.Fatal("close-before-task tombstone provisioned work", err)
	}
}

func TestWorkWorkerClosingAmbiguousCreateRequiresAudit(t *testing.T) {
	store := testStore(t)
	g, writer, id := workerWorkGateway(t, store, "selfHosted")
	writer.failCreate = io.ErrUnexpectedEOF
	if err := g.syncWork(context.Background(), id); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatal(err)
	}
	if err := store.Update(t.Context(), id, func(record *Record) error { record.Work.State = _workClosing; return nil }); err != nil {
		t.Fatal(err)
	}
	if err := g.syncWork(context.Background(), id); err == nil {
		t.Fatal("ambiguous create marked cleaned")
	}
	record := workerWorkRecord(t, store, id)
	if record.Work.State != _workClosing || record.Work.Attempts[0].Cleaned || writer.creates != 1 {
		t.Fatal("ambiguous create can be restarted")
	}
}

func TestWorkWorkerForcedDeletionDoesNotForgetRemoteSession(t *testing.T) {
	store := testStore(t)
	g, writer, id := workerWorkGateway(t, store, "selfHosted")
	if err := g.syncWork(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	record := workerWorkRecord(t, store, id)
	session := workerWorkSession(t, writer, record)
	if err := writer.Client.Delete(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	provider, ok := g.provider.(*simulator)
	if !ok {
		t.Fatal("unexpected test provider")
	}
	provider.remotePresent = true
	if err := g.syncWork(context.Background(), id); !errors.Is(err, errRemotePurgeBlocked) {
		t.Fatal(err)
	}
	record = workerWorkRecord(t, store, id)
	if record.Work.State != _workClosing || record.Work.Attempts[0].Cleaned || writer.creates != 1 {
		t.Fatal("forced deletion abandoned remote Session")
	}
	provider.remotePresent = false
	if err := g.syncWork(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	if !workerWorkRecord(t, store, id).Work.Attempts[0].Cleaned {
		t.Fatal("remote absence proof did not permit cleanup")
	}
}
