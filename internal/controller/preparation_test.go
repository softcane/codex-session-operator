package controller

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	"github.com/softcane/codex-session-operator/internal/openaiapi"
	"github.com/softcane/codex-session-operator/internal/sandbox"
	"github.com/softcane/codex-session-operator/internal/testutil"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	sandboxv1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	extensions "sigs.k8s.io/agent-sandbox/extensions/api/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type fixtureUIDClient struct{ client.Client }

func (c fixtureUIDClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	if obj.GetUID() == "" {
		obj.SetUID(types.UID(uuid.NewString()))
	}
	return c.Client.Create(ctx, obj, opts...)
}

func runningExecutorStatus(pod *corev1.Pod) corev1.ContainerStatus {
	return corev1.ContainerStatus{Name: pod.Spec.Containers[0].Name, ContainerID: "containerd://" + string(pod.UID), Ready: true, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(time.Unix(1700000000, 0))}}}
}

func preparationFixture(t *testing.T) (*SessionReconciler, *agentsv1alpha1.Session, *connectionStub) {
	t.Helper()
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, agentsv1alpha1.AddToScheme, sandboxv1.AddToScheme, extensions.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	pool, template := testutil.NewSandboxPool("tenant")
	profile := &agentsv1alpha1.SelfHostedEnvironment{SandboxPoolRef: agentsv1alpha1.AgentReference{Name: pool.Name}, Workspace: agentsv1alpha1.SelfHostedWorkspace{Directory: "/workspace", DeletionPolicy: "Delete"}}
	session := &agentsv1alpha1.Session{ObjectMeta: metav1.ObjectMeta{Name: "prepared", Namespace: "tenant", UID: "session-uid", Generation: 1}, Status: agentsv1alpha1.SessionStatus{OpenAISessionID: "sess-prepared", EnvironmentID: "env-prepared", RemoteURL: "https://provider.example/connect", EnvironmentSnapshot: &agentsv1alpha1.SessionEnvironmentSnapshot{Profile: agentsv1alpha1.EnvironmentSpec{Mode: "SelfHosted", SelfHosted: profile, DeletionPolicy: "Delete"}}}}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: sandbox.ExecutorSecretName, Namespace: session.Namespace, UID: "credential-uid", ResourceVersion: "1"}, Type: corev1.SecretTypeOpaque, Data: map[string][]byte{sandbox.ExecutorSecretKey: []byte("restricted-fixture")}}
	store := fixtureUIDClient{fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&agentsv1alpha1.Session{}, &corev1.Pod{}, &extensions.SandboxClaim{}, &sandboxv1.Sandbox{}).WithObjects(session, secret, pool, template).Build()}
	snapshot, err := sandbox.ResolvePool(t.Context(), store, session.Namespace, profile)
	if err != nil {
		t.Fatal(err)
	}
	session.Status.EnvironmentSnapshot.SandboxPool = snapshot
	if err := store.Status().Update(t.Context(), session); err != nil {
		t.Fatal(err)
	}
	observer := &connectionStub{state: openaiapi.Connection{Subscribed: true, Epoch: "stream-1"}}
	r := &SessionReconciler{Client: store, APIReader: store, Scheme: scheme, SandboxPools: []string{pool.Name}, Connections: observer}
	return r, session, observer
}

func preparationStep(t *testing.T, r *SessionReconciler, s *agentsv1alpha1.Session) {
	t.Helper()
	if err := reconcileTestSandboxInfrastructure(t.Context(), r.Client, s.Namespace); err != nil {
		t.Fatal(err)
	}
	if _, err := r.reconcileExecutor(t.Context(), s); err != nil {
		t.Fatal(err)
	}
	if err := r.Status().Update(t.Context(), s); err != nil {
		if !apierrors.IsConflict(err) {
			t.Fatal(err)
		}
		if err := r.Get(t.Context(), client.ObjectKeyFromObject(s), s); err != nil {
			t.Fatal(err)
		}
	}
	if err := reconcileTestSandboxInfrastructure(t.Context(), r.Client, s.Namespace); err != nil {
		t.Fatal(err)
	}
}
func preparationPod(t *testing.T, r *SessionReconciler, s *agentsv1alpha1.Session) *corev1.Pod {
	t.Helper()
	var pod corev1.Pod
	if err := r.Get(t.Context(), client.ObjectKey{Namespace: s.Namespace, Name: sandbox.ClaimName(s)}, &pod); err != nil {
		t.Fatal(err)
	}
	return &pod
}
func healthyPreparationPod(t *testing.T, r *SessionReconciler, pod *corev1.Pod) {
	t.Helper()
	pod.Status.Phase = corev1.PodRunning
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{runningExecutorStatus(pod)}
	for _, init := range pod.Spec.InitContainers {
		pod.Status.InitContainerStatuses = append(pod.Status.InitContainerStatuses, corev1.ContainerStatus{Name: init.Name, State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0}}})
	}
	if err := r.Status().Update(t.Context(), pod); err != nil {
		t.Fatal(err)
	}
}
