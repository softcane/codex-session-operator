package controller

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	"github.com/softcane/codex-session-operator/internal/openaiapi"
	"github.com/softcane/codex-session-operator/internal/sandbox"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

const _preparationURL = "https://github.com/fixture/project.git"
const _preparationSourceUID = "source-uid"
const _preparationReplacingReason = "ExecutorReplacing"
const _executorIDMutation = "container ID"
const _executorStartMutation = "start time"

type preparationOutageProvider struct {
	SessionProvider
	unavailable bool
}

func (p *preparationOutageProvider) GetSession(ctx context.Context, id string) (openaiapi.Session, error) {
	if p.unavailable {
		return openaiapi.Session{}, errors.New("provider observation unavailable")
	}
	return p.SessionProvider.GetSession(ctx, id)
}

func runningExecutorStatus(pod *corev1.Pod) corev1.ContainerStatus {
	return corev1.ContainerStatus{Name: pod.Spec.Containers[0].Name, ContainerID: "containerd://" + string(pod.UID), Ready: true,
		State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(time.Unix(1700000000, 0))}}}
}

func preparationFixture(t *testing.T) (*SessionReconciler, *agentsv1alpha1.Session, *connectionStub) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := agentsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	profile := &agentsv1alpha1.SelfHostedEnvironment{Image: testExecutorImage, Architecture: "amd64", WorkspaceDirectory: defaultWorkspace, WorkspaceSize: "1Gi",
		WorkspaceStorage: &agentsv1alpha1.WorkspaceStorage{Type: sandbox.WorkspaceStorageEphemeral},
		Repository:       &agentsv1alpha1.RepositorySpec{URL: _preparationURL, Commit: strings.Repeat("a", 40)}}
	agentConfiguration := map[string]any{agentModelField: openaiapi.SessionModel}
	session := &agentsv1alpha1.Session{ObjectMeta: metav1.ObjectMeta{Name: "prepared", Namespace: "tenant", UID: "session-uid", Generation: 1},
		Spec: agentsv1alpha1.SessionSpec{AgentRef: &agentsv1alpha1.AgentReference{Name: "prepared-agent"},
			EnvironmentRef: &agentsv1alpha1.AgentReference{Name: "prepared-environment"},
			SpendControl:   &agentsv1alpha1.SessionSpendControl{Mode: spendModeUnlimited}},
		Status: agentsv1alpha1.SessionStatus{ExecutorImage: testExecutorImage, OpenAISessionID: "sess-prepared", EnvironmentID: "env-prepared",
			RemoteURL: "https://provider.example/connect", OwnerScope: testAgentScope,
			AgentSnapshot: &agentsv1alpha1.SessionAgentSnapshot{Name: "prepared-agent", OpenAIAgentID: testAgentID,
				Model: openaiapi.SessionModel, ConfigurationDigest: openaiapi.AgentConfigurationDigest(agentConfiguration)},
			CreationOnlyDigest:  openaiapi.SessionCreationOnlyDigest(nil),
			CreationAgentDigest: openaiapi.SessionImmutableAgentDigest(agentConfiguration),
			EnvironmentSnapshot: &agentsv1alpha1.SessionEnvironmentSnapshot{Name: "prepared-environment",
				Profile: agentsv1alpha1.EnvironmentSpec{Mode: "selfHosted", DeletionPolicy: deletePolicy, SelfHosted: profile}}}}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: sandbox.ExecutorSecretName, Namespace: "tenant", UID: "credential-uid", ResourceVersion: "1"},
		Type: corev1.SecretTypeOpaque, Data: map[string][]byte{sandbox.ExecutorSecretKey: []byte("restricted-fixture")}}
	store := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&corev1.Pod{}, &agentsv1alpha1.Session{}, &agentsv1alpha1.Environment{}).
		WithObjects(session, secret).Build()
	observer := &connectionStub{state: openaiapi.Connection{Subscribed: true, Epoch: "stream-1"}}
	reconciler := &SessionReconciler{Client: store, APIReader: store, Scheme: scheme, Connections: observer,
		RepositoryURLs: []string{_preparationURL}, ExecutorImages: []string{testExecutorImage}}
	return reconciler, session, observer
}

func preparationRemote(session *agentsv1alpha1.Session) openaiapi.Session {
	return openaiapi.Session{ID: session.Status.OpenAISessionID, EnvironmentID: session.Status.EnvironmentID,
		RemoteURL: session.Status.RemoteURL, OwnerUID: string(session.UID), OwnerScope: testAgentScope,
		Model: openaiapi.SessionModel, SelfHosted: true, WorkspaceDirectory: defaultWorkspace,
		EnvironmentType: executorRuntimeSelfHostedType, AgentID: testAgentID,
		AgentConfiguration: map[string]any{agentModelField: openaiapi.SessionModel},
		Metadata:           map[string]string{"kubernetes_uid": string(session.UID), "operator": "codex-session-operator", "owner_scope": testAgentScope}}
}

func preparationStep(t *testing.T, reconciler *SessionReconciler, session *agentsv1alpha1.Session) {
	t.Helper()
	if _, _, err := reconciler.reconcileExecutor(t.Context(), session); err != nil {
		t.Fatal(err)
	}
}

func preparationPod(t *testing.T, reconciler *SessionReconciler, session *agentsv1alpha1.Session) *corev1.Pod {
	t.Helper()
	var pod corev1.Pod
	if err := reconciler.APIReader.Get(t.Context(), client.ObjectKey{Namespace: session.Namespace, Name: sandbox.PodName(session)}, &pod); err != nil {
		t.Fatal(err)
	}
	return &pod
}

func healthyPreparationPod(t *testing.T, reconciler *SessionReconciler, pod *corev1.Pod, uid string, exit int32) {
	t.Helper()
	pod.UID = types.UID(uid)
	if err := reconciler.Update(t.Context(), pod); err != nil {
		t.Fatal(err)
	}
	pod.Status.Phase = corev1.PodRunning
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{runningExecutorStatus(pod)}
	pod.Status.InitContainerStatuses = []corev1.ContainerStatus{{Name: sandbox.RepositoryInitContainerName, State: corev1.ContainerState{
		Terminated: &corev1.ContainerStateTerminated{ExitCode: exit}}}}
	if err := reconciler.Client.Status().Update(t.Context(), pod); err != nil {
		t.Fatal(err)
	}
}

func TestWorkspacePreparationUsesFreshPodAndInvalidatesReplacement(t *testing.T) {
	r, session, observer := preparationFixture(t)
	preparationStep(t, r, session)
	first := preparationPod(t, r, session)
	healthyPreparationPod(t, r, first, "pod-one", 0)
	observer.state.Connected, observer.state.Sequence = true, 1
	// A stale cache misses the actual Pod; authoritative observation must still prove it.
	r.Client = &missingPodClient{Client: r.Client}
	preparationStep(t, r, session)
	if session.Status.WorkspacePreparation == nil || session.Status.WorkspacePreparation.PodUID != "pod-one" ||
		!meta.IsStatusConditionTrue(session.Status.Conditions, "WorkspacePrepared") || !meta.IsStatusConditionTrue(session.Status.Conditions, readyCondition) {
		t.Fatal("fresh successful Pod did not establish preparation")
	}
	if session.Status.WorkspacePreparation.RepositoryDigest != sandbox.RepositoryDigest(session.Status.EnvironmentSnapshot.Profile.SelfHosted.Repository) {
		t.Fatal("preparation omitted pinned repository evidence")
	}
	if err := r.Delete(t.Context(), first, client.GracePeriodSeconds(0)); err != nil {
		t.Fatal(err)
	}
	preparationStep(t, r, session)
	if session.Status.WorkspacePreparation != nil || !meta.IsStatusConditionFalse(session.Status.Conditions, "WorkspacePrepared") {
		t.Fatal("deleted Pod retained preparation evidence")
	}
	replacement := preparationPod(t, r, session)
	healthyPreparationPod(t, r, replacement, "pod-two", 0)
	preparationStep(t, r, session)
	if session.Status.WorkspacePreparation == nil || session.Status.WorkspacePreparation.PodUID != "pod-two" || meta.IsStatusConditionTrue(session.Status.Conditions, readyCondition) {
		t.Fatal("replacement reused the old Pod connection event")
	}
	observer.state.Sequence++
	preparationStep(t, r, session)
	if !meta.IsStatusConditionTrue(session.Status.Conditions, readyCondition) {
		t.Fatal("new prepared Pod with fresh connection remained held")
	}
}

func TestWorkspacePreparationRejectsFailedInitAndForeignPod(t *testing.T) {
	r, session, observer := preparationFixture(t)
	preparationStep(t, r, session)
	pod := preparationPod(t, r, session)
	healthyPreparationPod(t, r, pod, "pod-one", 1)
	observer.state.Connected, observer.state.Sequence = true, 1
	preparationStep(t, r, session)
	if session.Status.WorkspacePreparation != nil || meta.FindStatusCondition(session.Status.Conditions, readyCondition).Reason != "WorkspacePreparationFailed" {
		t.Fatal("failed init plus PodReady authorized execution")
	}
	pod = preparationPod(t, r, session)
	pod.OwnerReferences[0].UID = "foreign-session"
	if err := r.Update(t.Context(), pod); err != nil {
		t.Fatal(err)
	}
	preparationStep(t, r, session)
	if session.Status.WorkspacePreparation != nil || meta.FindStatusCondition(session.Status.Conditions, readyCondition).Reason != "ExecutorOwnershipMismatch" {
		t.Fatal("foreign Pod established preparation")
	}
}

func TestWorkspacePreparationReplacesRestartedExecutorWithinSamePod(t *testing.T) {
	r, session, observer := preparationFixture(t)
	preparationStep(t, r, session)
	pod := preparationPod(t, r, session)
	healthyPreparationPod(t, r, pod, "first-pod", 0)
	observer.state.Connected, observer.state.Sequence = true, 1
	preparationStep(t, r, session)
	if session.Status.WorkspacePreparation == nil {
		t.Fatal("healthy first instance was not prepared")
	}
	pod = preparationPod(t, r, session)
	pod.Status.ContainerStatuses[0].RestartCount = 1
	pod.Status.ContainerStatuses[0].ContainerID = "containerd://restarted"
	pod.Status.ContainerStatuses[0].State.Running.StartedAt = metav1.NewTime(time.Unix(1700000001, 0))
	if err := r.Status().Update(t.Context(), pod); err != nil {
		t.Fatal(err)
	}
	preparationStep(t, r, session)
	if session.Status.WorkspacePreparation != nil || meta.IsStatusConditionTrue(session.Status.Conditions, readyCondition) {
		t.Fatal("restarted process reused initial preparation or connection proof")
	}
	if meta.FindStatusCondition(session.Status.Conditions, readyCondition).Reason != _preparationReplacingReason {
		t.Fatal("restarted executor did not request a fresh Pod baseline")
	}
}

func TestReconcileRejectsChangedExecutorIdentityWithResetRestartCount(t *testing.T) {
	for _, field := range []string{_executorIDMutation, _executorStartMutation} {
		t.Run(field, func(t *testing.T) {
			r, session, observer := preparationFixture(t)
			lifecycle := &sessionProviderStub{remote: preparationRemote(session)}
			r.OpenAI = lifecycle
			r.Configuration = &configuredSessionStub{lifecycle: lifecycle}
			request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(session)}
			reconcile := func() *agentsv1alpha1.Session {
				t.Helper()
				if _, err := r.Reconcile(t.Context(), request); err != nil {
					t.Fatal(err)
				}
				var current agentsv1alpha1.Session
				if err := r.APIReader.Get(t.Context(), request.NamespacedName, &current); err != nil {
					t.Fatal(err)
				}
				return &current
			}
			reconcile() // Persist the finalizer before runtime work.
			reconcile() // Create the initial executor.
			pod := preparationPod(t, r, session)
			healthyPreparationPod(t, r, pod, "integrated-pod", 0)
			observer.state.Connected, observer.state.Sequence = true, 1
			prepared := reconcile()
			if prepared.Status.WorkspacePreparation == nil || !meta.IsStatusConditionTrue(prepared.Status.Conditions, readyCondition) {
				t.Fatal("full lifecycle did not establish first process preparation")
			}
			pod = preparationPod(t, r, session)
			if field == _executorIDMutation {
				pod.Status.ContainerStatuses[0].ContainerID = "containerd://new-process-with-reset-count"
			} else {
				pod.Status.ContainerStatuses[0].State.Running.StartedAt = metav1.NewTime(time.Unix(1700000001, 0))
			}
			if err := r.Status().Update(t.Context(), pod); err != nil {
				t.Fatal(err)
			}
			held := reconcile()
			if held.Status.WorkspacePreparation != nil || meta.IsStatusConditionTrue(held.Status.Conditions, readyCondition) ||
				meta.FindStatusCondition(held.Status.Conditions, readyCondition).Reason != _preparationReplacingReason {
				t.Fatal("full reconciliation lost prior process identity before comparison")
			}
		})
	}
}

func TestReconcileKeepsInstanceBaselineAcrossProviderObservationOutage(t *testing.T) {
	for _, field := range []string{_executorIDMutation, _executorStartMutation} {
		t.Run(field, func(t *testing.T) {
			r, session, observer := preparationFixture(t)
			lifecycle := &sessionProviderStub{remote: preparationRemote(session)}
			provider := &preparationOutageProvider{SessionProvider: lifecycle}
			r.OpenAI = provider
			r.Configuration = &configuredSessionStub{lifecycle: lifecycle}
			request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(session)}
			read := func() *agentsv1alpha1.Session {
				t.Helper()
				var current agentsv1alpha1.Session
				if err := r.APIReader.Get(t.Context(), request.NamespacedName, &current); err != nil {
					t.Fatal(err)
				}
				return &current
			}
			reconcile := func() {
				t.Helper()
				if _, err := r.Reconcile(t.Context(), request); err != nil {
					t.Fatal(err)
				}
			}
			reconcile()
			reconcile()
			pod := preparationPod(t, r, session)
			healthyPreparationPod(t, r, pod, "outage-pod", 0)
			observer.state.Connected, observer.state.Sequence = true, 1
			reconcile()
			pod = preparationPod(t, r, session)
			baseline := pod.Annotations[sandbox.ExecutorInstanceAnnotation]
			if baseline == "" || read().Status.WorkspacePreparation == nil || !meta.IsStatusConditionTrue(read().Status.Conditions, readyCondition) {
				t.Fatal("first ready process did not durably record its instance baseline")
			}
			provider.unavailable = true
			if _, err := r.Reconcile(t.Context(), request); err == nil {
				t.Fatal("provider observation outage was not exercised")
			}
			if held := read(); held.Status.WorkspacePreparation != nil || meta.IsStatusConditionTrue(held.Status.Conditions, workspacePreparedCondition) {
				t.Fatal("observation outage retained execution eligibility")
			}
			pod = preparationPod(t, r, session)
			if pod.Annotations[sandbox.ExecutorInstanceAnnotation] != baseline {
				t.Fatal("outage lost the independent instance baseline")
			}
			if field == _executorIDMutation {
				pod.Status.ContainerStatuses[0].ContainerID = "containerd://changed-during-outage"
			} else {
				pod.Status.ContainerStatuses[0].State.Running.StartedAt = metav1.NewTime(time.Unix(1700000001, 0))
			}
			if err := r.Status().Update(t.Context(), pod); err != nil {
				t.Fatal(err)
			}
			provider.unavailable = false
			reconcile()
			if held := read(); held.Status.WorkspacePreparation != nil || meta.IsStatusConditionTrue(held.Status.Conditions, readyCondition) ||
				meta.FindStatusCondition(held.Status.Conditions, readyCondition).Reason != _preparationReplacingReason {
				t.Fatal("recovery reused an old connected event for a changed instance with restart count zero")
			}
			reconcile()
			replacement := preparationPod(t, r, session)
			healthyPreparationPod(t, r, replacement, "after-outage-pod", 0)
			reconcile()
			if meta.IsStatusConditionTrue(read().Status.Conditions, readyCondition) {
				t.Fatal("replacement reused connection evidence from before the outage")
			}
			observer.state.Sequence++
			reconcile()
			if !meta.IsStatusConditionTrue(read().Status.Conditions, readyCondition) {
				t.Fatal("fresh prepared replacement and fresh connection remained held")
			}
		})
	}
}

func TestRepositoryEnvironmentReadinessFreezesSafeSourceVersions(t *testing.T) {
	r, session, _ := preparationFixture(t)
	const private = "repository-source-private-value"
	profile := session.Status.EnvironmentSnapshot.Profile.DeepCopy()
	profile.SelfHosted.Repository.CredentialSecretRef = &agentsv1alpha1.RepositoryCredentialSecretReference{Name: sandbox.RepositorySecretName, Key: testVaultTokenKey}
	environment := &agentsv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "repository", Namespace: session.Namespace, UID: "environment-uid", Generation: 1, Finalizers: []string{environmentFinalizer}}, Spec: *profile}
	if err := r.Create(t.Context(), environment); err != nil {
		t.Fatal(err)
	}
	environments := &EnvironmentReconciler{Client: r.Client, APIReader: r.APIReader, OwnerScope: "scope", ExecutorImages: []string{testExecutorImage}, RepositoryURLs: r.RepositoryURLs}
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(environment)}
	if _, err := environments.Reconcile(t.Context(), request); err == nil {
		t.Fatal("missing repository credential did not hold readiness")
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: sandbox.RepositorySecretName, Namespace: session.Namespace, UID: _preparationSourceUID}, Type: corev1.SecretTypeOpaque, Data: map[string][]byte{testVaultTokenKey: []byte(private)}}
	if err := r.Create(t.Context(), secret); err != nil {
		t.Fatal(err)
	}
	if _, err := environments.Reconcile(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	if err := r.APIReader.Get(t.Context(), request.NamespacedName, environment); err != nil {
		t.Fatal(err)
	}
	if !meta.IsStatusConditionTrue(environment.Status.Conditions, readyCondition) || environment.Status.SourceVersion == nil || environment.Status.SourceVersion.UID != _preparationSourceUID {
		t.Fatal("repository Ready did not pin protected source metadata")
	}
	encoded, err := json.Marshal(environment.Status)
	if err != nil || strings.Contains(string(encoded), private) {
		t.Fatal("protected repository contents entered Environment status")
	}
	r.Environments, r.AgentOwnerScope = &environmentProviderStub{}, "scope"
	session.Spec.EnvironmentRef = &agentsv1alpha1.AgentReference{Name: environment.Name}
	snapshot, _, reason, _, err := r.resolveEnvironment(context.Background(), session)
	if err != nil || reason != "" || snapshot.SourceVersion.UID != _preparationSourceUID {
		t.Fatal("repository snapshot lost source provenance", reason, err)
	}
	beforeDigest := environment.Status.ConfigurationDigest
	secret.Data[testVaultTokenKey] = []byte("rotated-private-value")
	if err := r.Update(t.Context(), secret); err != nil {
		t.Fatal(err)
	}
	if _, _, reason, _, err := r.resolveEnvironment(t.Context(), session); err != nil || reason != environmentNotReadyReason {
		t.Fatal("stale source acknowledgment permitted a new snapshot", reason, err)
	}
	if _, err := environments.Reconcile(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	if err := r.APIReader.Get(t.Context(), request.NamespacedName, environment); err != nil || environment.Status.ConfigurationDigest == beforeDigest {
		t.Fatal("rotation did not refresh Environment source acknowledgment", err)
	}
	if sandbox.RepositoryDigest(snapshot.Profile.SelfHosted.Repository) != sandbox.RepositoryDigest(environment.Spec.SelfHosted.Repository) {
		t.Fatal("credential rotation changed checkout identity")
	}
}
