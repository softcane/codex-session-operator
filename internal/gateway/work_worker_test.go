package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"reflect"
	"strconv"
	"testing"
	"time"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	"github.com/softcane/codex-session-operator/internal/openaiapi"
	"github.com/softcane/codex-session-operator/internal/sandbox"
	"github.com/softcane/codex-session-operator/internal/testutil"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	_workWorkerVault       = _profileTestVault
	_workWorkerEnvironment = _profileTestEnvironment
	_workWorkerUnlimited   = _testWorkUnlimited
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

func workerWorkRoute() profileRoute {
	return profileRoute{ID: _profileTestName, Namespace: _testNamespace, Source: _profileSourceName, Agent: _profileTestAgent,
		Environment: _workWorkerEnvironment, Vaults: []string{_workWorkerVault},
		SpendControl: &agentsv1alpha1.SessionSpendControl{Mode: _workWorkerUnlimited}}
}

func workerPolicyEnvironment() *agentsv1alpha1.SessionEnvironmentSnapshot {
	return &agentsv1alpha1.SessionEnvironmentSnapshot{Name: _workWorkerEnvironment, UID: _profileTestEnvironmentUID}
}

func workerWorkGateway(t *testing.T, store Storage, mode string) (*Gateway, *workWorkerWriter, string) {
	t.Helper()
	_, objects := readyProfileObjects(_testNamespace)
	var selfHosted *agentsv1alpha1.Environment
	for _, object := range objects {
		if environment, ok := object.(*agentsv1alpha1.Environment); ok {
			environment.Spec.Mode = agentsv1alpha1.EnvironmentMode(mode)
			if mode == _selfHostedMode {
				environment.Spec.SelfHosted = &agentsv1alpha1.SelfHostedEnvironment{SandboxPoolRef: agentsv1alpha1.AgentReference{Name: "codex-go-v1"}, Workspace: agentsv1alpha1.SelfHostedWorkspace{Directory: _executionTestWorkspace, DeletionPolicy: _testDeletePolicy}}
				selfHosted = environment
			}
		}
	}
	if selfHosted != nil {
		pool, template := testutil.NewSandboxPool(_testNamespace)
		objects = append(objects, pool, template)
	}
	provider := simulatorForTest()
	provider.remotePresent = false
	g, k8s := testGateway(t, store, provider, objects...)
	if selfHosted != nil {
		snapshot, err := sandbox.ResolvePool(t.Context(), k8s, _testNamespace, selfHosted.Spec.SelfHosted)
		if err != nil {
			t.Fatal(err)
		}
		selfHosted.Status.SandboxPool = snapshot
		selfHosted.Status.ConfigurationDigest = openaiapi.SandboxEnvironmentDigest(selfHosted.Spec, snapshot)
		if err := k8s.Update(t.Context(), selfHosted); err != nil {
			t.Fatal(err)
		}
	}
	writer := &workWorkerWriter{Client: k8s, store: store}
	if err := g.EnableProfiles(writer); err != nil {
		t.Fatal(err)
	}
	record, _, err := g.createConversation(t.Context(), requestScope{Namespace: _testNamespace, Subject: _testConversationSubject},
		"worker-route", ConversationInput{RouteID: _profileTestName, Text: "Investigate this failing CI run."})
	if err != nil {
		t.Fatal(err)
	}
	writer.workID = record.UID
	return g, writer, record.UID
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

func TestWorkWorkerReusesSessionAndQueuesTaskBeforeEvidence(t *testing.T) {
	store := testStore(t)
	g, writer, id := workerWorkGateway(t, store, _selfHostedMode)
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
	if err := g.ValidateSessionPolicy(t.Context(), session, &agentsv1alpha1.SessionEnvironmentSnapshot{Name: _workWorkerEnvironment, UID: _profileTestEnvironmentUID, Profile: agentsv1alpha1.EnvironmentSpec{Mode: _selfHostedMode}}); err != nil {
		t.Fatal(err)
	}
	session.Status.Conditions = []metav1.Condition{{Type: _readyCondition, Status: metav1.ConditionTrue, Reason: "Observed", ObservedGeneration: session.Generation, LastTransitionTime: metav1.Now()}}
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
	g, writer, id := workerWorkGateway(t, store, string(agentsv1alpha1.EnvironmentModeNone))
	writer.loseCreate = true
	if err := g.syncWork(context.Background(), id); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatal(err)
	}
	record := workerWorkRecord(t, store, id)
	session := workerWorkSession(t, writer, record)
	if err := g.ValidateSessionPolicy(t.Context(), session, workerPolicyEnvironment()); err == nil {
		t.Fatal("unbound work Session passed controller creation policy")
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
	if err := g.ValidateSessionPolicy(t.Context(), workerWorkSession(t, writer, record), workerPolicyEnvironment()); err != nil {
		t.Fatal(err)
	}
}

func TestWorkWorkerRecoversAfterCommitCreateAndEnqueue(t *testing.T) {
	store, dsn := postgresForTest(t)
	g, writer, id := workerWorkGateway(t, store, string(agentsv1alpha1.EnvironmentModeNone))
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
	g, writer, id := workerWorkGateway(t, store, _selfHostedMode)
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
	g, writer, id := workerWorkGateway(t, store, _selfHostedMode)
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
	g, writer, id := workerWorkGateway(t, store, _selfHostedMode)
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
	if err := g.ValidateSessionPolicy(t.Context(), previous, workerPolicyEnvironment()); !errors.Is(err, errClosed) {
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
			ConversationID: conversationID(id, 2), Policy: workPolicy(workerWorkRoute(), id, 2),
			Profile: copyProfileProvenance(record.Work.Attempts[0].Profile)})
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
	if err := g.ValidateSessionPolicy(t.Context(), previous, workerPolicyEnvironment()); err == nil {
		t.Fatal("prior attempt passed controller policy")
	}
	delete(previous.Annotations, _workAnnotation)
	if err := g.ValidateSessionPolicy(t.Context(), previous, workerPolicyEnvironment()); err == nil {
		t.Fatal("removed annotation bypasses prior attempt index")
	}
}

func TestWorkWorkerNeverReplacesDeletedBoundSession(t *testing.T) {
	store := testStore(t)
	g, writer, id := workerWorkGateway(t, store, _selfHostedMode)
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
		g, writer, id := workerWorkGateway(t, store, _selfHostedMode)
		record := workerWorkRecord(t, store, id)
		foreign := &agentsv1alpha1.Session{ObjectMeta: metav1.ObjectMeta{Namespace: record.Namespace, Name: record.Name, UID: _testForeignUID},
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
		g, writer, id := workerWorkGateway(t, store, _selfHostedMode)
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
		if err := g.ValidateSessionPolicy(t.Context(), recreated, workerPolicyEnvironment()); err == nil {
			t.Fatal("recreated UID passed controller policy")
		}
	})
}

func TestWorkWorkerCloseBeforeTaskTombstoneNeedsNoSession(t *testing.T) {
	store := testStore(t)
	g, writer, id := workerWorkGateway(t, store, _selfHostedMode)
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
	g, writer, id := workerWorkGateway(t, store, _selfHostedMode)
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
	g, writer, id := workerWorkGateway(t, store, _selfHostedMode)
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
