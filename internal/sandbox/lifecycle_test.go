package sandbox

import (
	"reflect"
	"testing"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	"github.com/softcane/codex-session-operator/internal/testutil"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	extensionsv1beta1 "sigs.k8s.io/agent-sandbox/extensions/api/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

const (
	_testWorkspace = "/workspace"
	_testOther     = "other"
	_testToken     = "token"
)

func poolFixture(t *testing.T) (*agentsv1alpha1.Session, *extensionsv1beta1.SandboxTemplate) {
	t.Helper()
	pool, template := testutil.NewSandboxPool("tenant")
	profile := &agentsv1alpha1.SelfHostedEnvironment{SandboxPoolRef: agentsv1alpha1.AgentReference{Name: pool.Name}, Workspace: agentsv1alpha1.SelfHostedWorkspace{Directory: _testWorkspace, DeletionPolicy: WorkspaceClaimDelete}}
	scheme := k8sruntime.NewScheme()
	if err := extensionsv1beta1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pool, template).Build()
	snapshot, err := ResolvePool(t.Context(), reader, "tenant", profile)
	if err != nil {
		t.Fatal(err)
	}
	session := &agentsv1alpha1.Session{ObjectMeta: metav1.ObjectMeta{Name: "task", Namespace: "tenant", UID: "session-uid"}, Status: agentsv1alpha1.SessionStatus{
		EnvironmentID: "environment-1", RemoteURL: "wss://example.invalid/executor",
		EnvironmentSnapshot: &agentsv1alpha1.SessionEnvironmentSnapshot{Profile: agentsv1alpha1.EnvironmentSpec{Mode: "SelfHosted", SelfHosted: profile}, SandboxPool: snapshot},
	}}
	return session, template
}

func runtimeFixture(t *testing.T) (*agentsv1alpha1.Session, *extensionsv1beta1.SandboxTemplate, *extensionsv1beta1.SandboxClaim, *sandboxv1beta1.Sandbox, *corev1.Pod) {
	t.Helper()
	session, template := poolFixture(t)
	claim, err := Claim(session, template, "connection-config")
	if err != nil {
		t.Fatal(err)
	}
	claim.UID = "claim-uid"
	claim.Status.SandboxStatus.Name = claim.Name
	runtime := &sandboxv1beta1.Sandbox{ObjectMeta: metav1.ObjectMeta{Name: claim.Name, Namespace: claim.Namespace, UID: "sandbox-uid", OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(claim, extensionsv1beta1.GroupVersion.WithKind("SandboxClaim"))}}, Spec: sandboxv1beta1.SandboxSpec{OperatingMode: sandboxv1beta1.SandboxOperatingModeRunning, SandboxBlueprint: *ExpectedBlueprint(template, claim)}}
	pod, err := ExpectedPod(runtime)
	if err != nil {
		t.Fatal(err)
	}
	pod.UID = "pod-uid"
	pod.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(runtime, sandboxv1beta1.GroupVersion.WithKind("Sandbox"))}
	session.Status.SandboxClaimName, session.Status.SandboxClaimUID = claim.Name, string(claim.UID)
	session.Status.SandboxName, session.Status.SandboxUID = runtime.Name, string(runtime.UID)
	return session, template, claim, runtime, pod
}

func TestClaimRequestsSessionBoundColdStart(t *testing.T) {
	session, template := poolFixture(t)
	init := *template.Spec.PodTemplate.Spec.Containers[0].DeepCopy()
	init.Name = RepositoryInitContainerName
	init.Env = nil
	template.Spec.PodTemplate.Spec.InitContainers = []corev1.Container{init}
	claim, err := Claim(session, template, "config-digest")
	if err != nil {
		t.Fatal(err)
	}
	if claim.Name != ClaimName(session) || !metav1.IsControlledBy(claim, session) || claim.Spec.WarmPoolRef.Name != template.Name || len(claim.Spec.VolumeClaimTemplates) != 0 {
		t.Fatal("claim did not delegate compute/storage to the frozen pool")
	}
	if claim.Annotations[ConfigAnnotation] != "config-digest" || claim.Spec.AdditionalPodMetadata.Annotations[ConfigAnnotation] != "config-digest" || claim.Spec.AdditionalPodMetadata.Labels[PodSessionUIDLabel] != string(session.UID) {
		t.Fatal("claim omitted preparation metadata")
	}
	want := []extensionsv1beta1.EnvVar{
		{Name: "CODEX_ENVIRONMENT_ID", Value: session.Status.EnvironmentID, ContainerName: ExecutorContainerName},
		{Name: "CODEX_REMOTE_URL", Value: session.Status.RemoteURL, ContainerName: ExecutorContainerName},
		{Name: "CODEX_SESSION_UID", Value: string(session.UID), ContainerName: ExecutorContainerName},
		{Name: "SESSION_UID", Value: string(session.UID), ContainerName: RepositoryInitContainerName},
	}
	if !reflect.DeepEqual(claim.Spec.Env, want) {
		t.Fatalf("unexpected claim environment: %#v", claim.Spec.Env)
	}
	if len(template.Spec.PodTemplate.Spec.InitContainers[0].Env) != 0 {
		t.Fatal("claim construction mutated administrator template")
	}
}

func TestOwnershipChainRequiresEveryFrozenUID(t *testing.T) {
	session, _, claim, runtime, pod := runtimeFixture(t)
	if !OwnsExecutor(session, claim, runtime, pod) {
		t.Fatal("valid claim ownership chain rejected")
	}
	tests := []struct {
		name   string
		mutate func(*agentsv1alpha1.Session, *extensionsv1beta1.SandboxClaim, *sandboxv1beta1.Sandbox, *corev1.Pod)
	}{
		{name: "claim replacement", mutate: func(_ *agentsv1alpha1.Session, c *extensionsv1beta1.SandboxClaim, _ *sandboxv1beta1.Sandbox, _ *corev1.Pod) {
			c.UID = "replacement"
		}},
		{name: "Sandbox replacement", mutate: func(_ *agentsv1alpha1.Session, _ *extensionsv1beta1.SandboxClaim, r *sandboxv1beta1.Sandbox, _ *corev1.Pod) {
			r.UID = "replacement"
		}},
		{name: "forged Pod owner", mutate: func(_ *agentsv1alpha1.Session, _ *extensionsv1beta1.SandboxClaim, _ *sandboxv1beta1.Sandbox, p *corev1.Pod) {
			p.OwnerReferences[0].UID = "different-owner"
		}},
		{name: "suspended", mutate: func(_ *agentsv1alpha1.Session, _ *extensionsv1beta1.SandboxClaim, r *sandboxv1beta1.Sandbox, _ *corev1.Pod) {
			r.Spec.OperatingMode = sandboxv1beta1.SandboxOperatingModeSuspended
		}},
		{name: "claim deleting", mutate: func(_ *agentsv1alpha1.Session, c *extensionsv1beta1.SandboxClaim, _ *sandboxv1beta1.Sandbox, _ *corev1.Pod) {
			c.DeletionTimestamp = &metav1.Time{Time: metav1.Now().Time}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, c, r, p := session.DeepCopy(), claim.DeepCopy(), runtime.DeepCopy(), pod.DeepCopy()
			tt.mutate(s, c, r, p)
			if OwnsExecutor(s, c, r, p) {
				t.Fatal("changed ownership or lifecycle accepted")
			}
		})
	}
}

func TestRuntimeRejectsClaimAndTemplateDrift(t *testing.T) {
	session, template, claim, runtime, _ := runtimeFixture(t)
	if !RuntimeMatches(session, claim, runtime, template) {
		t.Fatal("valid native runtime rejected")
	}
	retained := sandboxv1beta1.ShutdownPolicyRetain
	runtime.Spec.ShutdownPolicy = &retained
	if !RuntimeMatches(session, claim, runtime, template) {
		t.Fatal("native default shutdown policy rejected")
	}
	tests := []struct {
		name   string
		mutate func(*extensionsv1beta1.SandboxClaim, *sandboxv1beta1.Sandbox)
	}{
		{name: "connection changed", mutate: func(c *extensionsv1beta1.SandboxClaim, _ *sandboxv1beta1.Sandbox) { c.Spec.Env[0].Value = _testOther }},
		{name: "image changed", mutate: func(_ *extensionsv1beta1.SandboxClaim, r *sandboxv1beta1.Sandbox) {
			r.Spec.PodTemplate.Spec.Containers[0].Image = _testOther
		}},
		{name: "network selector changed", mutate: func(_ *extensionsv1beta1.SandboxClaim, r *sandboxv1beta1.Sandbox) {
			delete(r.Spec.PodTemplate.ObjectMeta.Labels, sandboxv1beta1.SandboxTemplateRefHashLabel)
		}},
		{name: "claim expiration", mutate: func(c *extensionsv1beta1.SandboxClaim, _ *sandboxv1beta1.Sandbox) {
			c.Spec.Lifecycle = &extensionsv1beta1.Lifecycle{ShutdownTime: &metav1.Time{Time: metav1.Now().Time}}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, r := claim.DeepCopy(), runtime.DeepCopy()
			tt.mutate(c, r)
			if RuntimeMatches(session, c, r, template) {
				t.Fatal("changed native runtime accepted")
			}
		})
	}
}

func TestExpectedPodUsesNativeWorkspaceAndDetectsFullSpecDrift(t *testing.T) {
	session, _, _, runtime, pod := runtimeFixture(t)
	name, err := WorkspaceMount(pod, session.Status.EnvironmentSnapshot.Profile.SelfHosted)
	if err != nil || name != WorkspaceClaimName(runtime) {
		t.Fatalf("native PVC mapping: %q %v", name, err)
	}
	expected, err := ExpectedPod(runtime)
	if err != nil {
		t.Fatal(err)
	}
	pod.Spec.NodeName = "scheduled-node"
	pod.Spec.SchedulerName = "default-scheduler"
	pod.Spec.EnableServiceLinks = new(true)
	pod.Spec.TerminationGracePeriodSeconds = new(int64(30))
	pod.Spec.Containers[0].ImagePullPolicy = corev1.PullIfNotPresent
	if !ExecutorMatches(pod, expected) {
		t.Fatal("normal Kubernetes defaults treated as Pod drift")
	}
	pod.Spec.HostAliases = []corev1.HostAlias{{IP: "127.0.0.1", Hostnames: []string{"api.openai.com"}}}
	if ExecutorMatches(pod, expected) {
		t.Fatal("unapproved Pod fields were not detected")
	}
}

func TestExpectedPodReplacesPlaceholderUsingNativeVolumeOrdering(t *testing.T) {
	_, _, _, runtime, _ := runtimeFixture(t)
	runtime.Spec.PodTemplate.Spec.Volumes = []corev1.Volume{
		{Name: "workspace", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
		{Name: "tmp", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
	}
	pod, err := ExpectedPod(runtime)
	if err != nil {
		t.Fatal(err)
	}
	if len(pod.Spec.Volumes) != 2 || pod.Spec.Volumes[0].Name != "tmp" || pod.Spec.Volumes[1].PersistentVolumeClaim == nil {
		t.Fatal("expected Pod volume ordering differs from native reconciliation")
	}
	if runtime.Spec.PodTemplate.Spec.Volumes[0].EmptyDir == nil {
		t.Fatal("verification mutated the native Sandbox")
	}
	pod.Labels[extensionsv1beta1.SandboxIDLabel] = "forged-claim"
	expected, err := ExpectedPod(runtime)
	if err != nil {
		t.Fatal(err)
	}
	if ExecutorMatches(pod, expected) {
		t.Fatal("Pod label drift escaped network policy verification")
	}
}
