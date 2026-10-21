package gateway

import (
	"encoding/json"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	"github.com/softcane/codex-session-operator/internal/openaiapi"
	"github.com/softcane/codex-session-operator/internal/sandbox"
	"github.com/softcane/codex-session-operator/internal/testutil"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	extensionsv1beta1 "sigs.k8s.io/agent-sandbox/extensions/api/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

const (
	_executionTestEnvironmentID = "env-execution"
	_executionTestWorkspace     = "/workspace"
	_executionForeignClaim      = "foreign-claim"
)

func executionSessionAndPod(t *testing.T) (*agentsv1alpha1.Session, *corev1.Pod) {
	t.Helper()
	session := testSession()
	session.Generation = 1
	profile := &agentsv1alpha1.SelfHostedEnvironment{SandboxPoolRef: agentsv1alpha1.AgentReference{Name: "codex-go-v1"},
		Workspace: agentsv1alpha1.SelfHostedWorkspace{Directory: _executionTestWorkspace, DeletionPolicy: _testDeletePolicy}}
	session.Status.EnvironmentSnapshot = &agentsv1alpha1.SessionEnvironmentSnapshot{
		Profile: agentsv1alpha1.EnvironmentSpec{Mode: _selfHostedMode, SelfHosted: profile}}
	session.Status.EnvironmentID = _executionTestEnvironmentID
	session.Status.RemoteURL = "wss://example.com/executor"
	return prepareExecutionSession(t, session)
}

func prepareExecutionSession(t *testing.T, session *agentsv1alpha1.Session) (*agentsv1alpha1.Session, *corev1.Pod) {
	t.Helper()
	pool, template := executionPool(session.Namespace)
	scheme := k8sruntime.NewScheme()
	if err := extensionsv1beta1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pool, template).Build()
	resolved, err := sandbox.ResolvePool(t.Context(), reader, session.Namespace, session.Status.EnvironmentSnapshot.Profile.SelfHosted)
	if err != nil {
		t.Fatal(err)
	}
	session.Status.EnvironmentSnapshot.SandboxPool = resolved
	claim, runtime := executionRuntime(t, session, template)
	pod, err := sandbox.ExpectedPod(runtime)
	if err != nil {
		t.Fatal(err)
	}
	pod.UID = "prepared-pod"
	pod.OwnerReferences = []metav1.OwnerReference{{APIVersion: sandboxv1beta1.GroupVersion.String(), Kind: "Sandbox",
		Name: runtime.Name, UID: runtime.UID, Controller: new(true)}}
	session.Status.SandboxClaimName, session.Status.SandboxClaimUID = claim.Name, string(claim.UID)
	session.Status.SandboxName, session.Status.SandboxUID = runtime.Name, string(runtime.UID)
	session.Status.PodName, session.Status.ExecutorPodUID = pod.Name, string(pod.UID)
	session.Status.WorkspaceClaimUID = "prepared-workspace"
	session.Status.ExecutorConfigDigest = "connection-config"
	session.Status.WorkspacePreparation = &agentsv1alpha1.SessionWorkspacePreparationStatus{
		SandboxClaimUID: string(claim.UID), SandboxUID: string(runtime.UID), PodUID: string(pod.UID),
		ClaimUID: session.Status.WorkspaceClaimUID, ConfigDigest: sandbox.ConfigDigest(runtime),
		ContainerID: "containerd://prepared-executor", StartTime: metav1.NewTime(time.Date(2026, 10, 14, 10, 0, 0, 0, time.UTC))}
	session.Status.Conditions = []metav1.Condition{
		{Type: _readyCondition, Status: metav1.ConditionTrue, ObservedGeneration: 1},
		{Type: "WorkspacePrepared", Status: metav1.ConditionTrue, ObservedGeneration: 1},
		{Type: "EnvironmentConnected", Status: metav1.ConditionTrue, ObservedGeneration: 1}}
	pod.Status.Phase = corev1.PodRunning
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "executor", Ready: true, ContainerID: session.Status.WorkspacePreparation.ContainerID,
		State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: session.Status.WorkspacePreparation.StartTime}}}}
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	for _, init := range pod.Spec.InitContainers {
		pod.Status.InitContainerStatuses = append(pod.Status.InitContainerStatuses, corev1.ContainerStatus{Name: init.Name,
			State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0}}})
	}
	return session, pod
}

func executionPool(namespace string) (*extensionsv1beta1.SandboxWarmPool, *extensionsv1beta1.SandboxTemplate) {
	pool, template := testutil.NewSandboxPool(namespace)
	template.Spec.PodTemplate.Spec.DNSPolicy = corev1.DNSNone
	template.Spec.PodTemplate.Spec.DNSConfig = &corev1.PodDNSConfig{Nameservers: []string{"8.8.8.8", "1.1.1.1"}}
	initializer := template.Spec.PodTemplate.Spec.Containers[0].DeepCopy()
	initializer.Name, initializer.Command, initializer.Args, initializer.Env = "repository-prepare", []string{"/workspace-bootstrap"}, nil, nil
	template.Spec.PodTemplate.Spec.InitContainers = []corev1.Container{*initializer}
	return pool, template
}

func executionRuntime(t *testing.T, session *agentsv1alpha1.Session, template *extensionsv1beta1.SandboxTemplate) (*extensionsv1beta1.SandboxClaim, *sandboxv1beta1.Sandbox) {
	t.Helper()
	claim, err := sandbox.Claim(session, template, "connection-config")
	if err != nil {
		t.Fatal(err)
	}
	claim.UID = "prepared-claim"
	claim.OwnerReferences = currentOwner(session)
	name := claim.Name
	claim.Status.SandboxStatus.Name = name
	blueprint := sandbox.ExpectedBlueprint(template, claim)
	runtime := &sandboxv1beta1.Sandbox{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: session.Namespace, UID: "prepared-sandbox",
		OwnerReferences: []metav1.OwnerReference{{APIVersion: extensionsv1beta1.GroupVersion.String(), Kind: "SandboxClaim", Name: claim.Name, UID: claim.UID, Controller: new(true)}}},
		Spec: sandboxv1beta1.SandboxSpec{OperatingMode: sandboxv1beta1.SandboxOperatingModeRunning, SandboxBlueprint: *blueprint}}
	return claim, runtime
}

func executionObjects(t *testing.T, session *agentsv1alpha1.Session, pod *corev1.Pod) []client.Object {
	t.Helper()
	pool, template := executionPool(session.Namespace)
	claim, runtime := executionRuntime(t, session, template)
	workspace := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: sandbox.WorkspaceClaimName(runtime), Namespace: session.Namespace, UID: "prepared-workspace",
		OwnerReferences: []metav1.OwnerReference{{APIVersion: sandboxv1beta1.GroupVersion.String(), Kind: "Sandbox", Name: runtime.Name, UID: runtime.UID, Controller: new(true)}}}}
	return []client.Object{session, pod, pool, template, claim, runtime, workspace}
}

func TestSelfHostedExecutionRequiresCurrentOwnedPreparedConnectedPod(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*agentsv1alpha1.Session, *corev1.Pod)
	}{
		{name: "ready without proof", mutate: func(s *agentsv1alpha1.Session, _ *corev1.Pod) { s.Status.WorkspacePreparation = nil }},
		{name: "stale recorded pod", mutate: func(s *agentsv1alpha1.Session, _ *corev1.Pod) { s.Status.ExecutorPodUID = "previous-pod" }},
		{name: "missing source baseline", mutate: func(s *agentsv1alpha1.Session, _ *corev1.Pod) { s.Status.ExecutorConfigDigest = "" }},
		{name: "wrong source baseline", mutate: func(s *agentsv1alpha1.Session, _ *corev1.Pod) { s.Status.ExecutorConfigDigest = "previous-config" }},
		{name: "wrong sandbox proof", mutate: func(s *agentsv1alpha1.Session, _ *corev1.Pod) {
			s.Status.WorkspacePreparation.SandboxUID = "previous-sandbox"
		}},
		{name: "replacement", mutate: func(_ *agentsv1alpha1.Session, p *corev1.Pod) { p.UID = "replacement-pod" }},
		{name: "container restarted", mutate: func(_ *agentsv1alpha1.Session, p *corev1.Pod) { p.Status.ContainerStatuses[0].RestartCount = 1 }},
		{name: "container not ready", mutate: func(_ *agentsv1alpha1.Session, p *corev1.Pod) { p.Status.ContainerStatuses[0].Ready = false }},
		{name: "container replaced", mutate: func(_ *agentsv1alpha1.Session, p *corev1.Pod) {
			p.Status.ContainerStatuses[0].ContainerID = "replacement-container"
		}},
		{name: "foreign owner", mutate: func(_ *agentsv1alpha1.Session, p *corev1.Pod) { p.OwnerReferences[0].UID = _testForeignUID }},
		{name: "changed configuration", mutate: func(_ *agentsv1alpha1.Session, p *corev1.Pod) { p.Spec.Containers[0].Image = "changed" }},
		{name: "changed spec", mutate: func(_ *agentsv1alpha1.Session, p *corev1.Pod) {
			p.Spec.Containers[0].EnvFrom = []corev1.EnvFromSource{{}}
		}},
		{name: "init missing", mutate: func(_ *agentsv1alpha1.Session, p *corev1.Pod) { p.Status.InitContainerStatuses = nil }},
		{name: "init failed", mutate: func(_ *agentsv1alpha1.Session, p *corev1.Pod) {
			p.Status.InitContainerStatuses[0].State.Terminated.ExitCode = 1
		}},
		{name: "not running", mutate: func(_ *agentsv1alpha1.Session, p *corev1.Pod) { p.Status.Phase = corev1.PodPending }},
		{name: "not ready", mutate: func(_ *agentsv1alpha1.Session, p *corev1.Pod) { p.Status.Conditions[0].Status = corev1.ConditionFalse }},
		{name: "stale preparation", mutate: func(s *agentsv1alpha1.Session, _ *corev1.Pod) { s.Status.Conditions[1].ObservedGeneration = 0 }},
		{name: "stale connection", mutate: func(s *agentsv1alpha1.Session, _ *corev1.Pod) { s.Status.Conditions[2].ObservedGeneration = 0 }},
		{name: "wrong workspace", mutate: func(s *agentsv1alpha1.Session, _ *corev1.Pod) {
			s.Status.WorkspacePreparation.ClaimUID = _executionForeignClaim
		}},
		{name: "wrong claim proof", mutate: func(s *agentsv1alpha1.Session, _ *corev1.Pod) {
			s.Status.WorkspacePreparation.SandboxClaimUID = _executionForeignClaim
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			session, pod := executionSessionAndPod(t)
			original := pod.DeepCopy()
			originalSession := session.DeepCopy()
			tt.mutate(session, pod)
			store := testStore(t)
			provider := simulatorForTest()
			g, writer := testGateway(t, store, provider, executionObjects(t, session, pod)...)
			response := request(g, http.MethodPost, _testMessagePath, "prepared-message", `{"text":"Run tests"}`)
			if response.Code != http.StatusAccepted {
				t.Fatal("runtime hold rejected durable input", response.Code, response.Body.String())
			}
			if err := g.sync(t.Context(), _testUID); err != nil {
				t.Fatal(err)
			}
			if provider.posts != 0 || mustRecord(t, store).Submissions[0].Attempts != 0 {
				t.Fatal("stale preparation authorized provider delivery")
			}
			var currentPod corev1.Pod
			if err := writer.Get(t.Context(), client.ObjectKeyFromObject(pod), &currentPod); err != nil {
				t.Fatal(err)
			}
			original.ResourceVersion = currentPod.ResourceVersion
			// UID replacement is repaired through a new controller proof, not UID mutation.
			original.UID = currentPod.UID
			originalSession.Status.WorkspacePreparation.PodUID = string(currentPod.UID)
			originalSession.Status.ExecutorPodUID = string(currentPod.UID)
			updateExecutionPod(t, writer, original)
			var currentSession agentsv1alpha1.Session
			if err := writer.Get(t.Context(), client.ObjectKeyFromObject(session), &currentSession); err != nil {
				t.Fatal(err)
			}
			originalSession.ResourceVersion = currentSession.ResourceVersion
			if err := writer.Update(t.Context(), originalSession); err != nil {
				t.Fatal(err)
			}
			if err := g.sync(t.Context(), _testUID); err != nil {
				t.Fatal(err)
			}
			if provider.commits != 1 || mustRecord(t, store).Submissions[0].RemoteKey != wireKey(_testUID, "prepared-message") {
				_, reason := g.executionReady(t.Context(), originalSession)
				t.Fatal("verified runtime did not release original durable input once", reason)
			}
		})
	}
}

func TestSelfHostedPreparationHoldsConsequentialActionsButAllowsCancellation(t *testing.T) {
	session, pod := executionSessionAndPod(t)
	pod.Status.InitContainerStatuses[0].State.Terminated.ExitCode = 1
	store := testStore(t)
	provider := simulatorForTest()
	provider.history.Actions = []openaiapi.PendingAction{scenarioPendingAction()}
	provider.history.Turns = []json.RawMessage{json.RawMessage(`{"id":"turn-1","status":"in_progress","subagent_id":null}`)}
	g, writer := testGateway(t, store, provider, executionObjects(t, session, pod)...)
	if _, _, err := g.lookup(t.Context(), _testNamespace, _testSessionName); err != nil {
		t.Fatal(err)
	}
	if err := g.sync(t.Context(), _testUID); err != nil {
		t.Fatal(err)
	}
	if err := g.decide(t.Context(), _testUID, actionID(scenarioPendingAction()), _approve, _testApprover); err != nil {
		t.Fatal(err)
	}
	if err := g.sync(t.Context(), _testUID); err != nil {
		t.Fatal(err)
	}
	if len(mustRecord(t, store).Notes) != 0 || provider.posts != 0 {
		t.Fatal("unprepared executor authorized an approved side effect")
	}
	response := request(g, http.MethodPost, _scenarioSessionPath+"/cancel", "held-cancel", `{"turn_id":"turn-1"}`)
	if response.Code != http.StatusAccepted {
		t.Fatal("preparation blocked targeted cancellation", response.Code)
	}
	if err := g.sync(t.Context(), _testUID); err != nil {
		t.Fatal(err)
	}
	if provider.posts != 1 || len(mustRecord(t, store).Notes) != 0 {
		t.Fatal("cancellation bypass ran ordinary held work")
	}
	var current corev1.Pod
	if err := writer.Get(t.Context(), client.ObjectKeyFromObject(pod), &current); err != nil {
		t.Fatal(err)
	}
	current.Status.InitContainerStatuses[0].State.Terminated.ExitCode = 0
	updateExecutionPod(t, writer, &current)
	if err := g.sync(t.Context(), _testUID); err != nil {
		t.Fatal(err)
	}
	if provider.posts != 2 || len(mustRecord(t, store).Notes) != 1 {
		t.Fatal("prepared runtime did not release approved action once")
	}
}

func TestSelfHostedCloseBeforePreparationNeverDeliversQueuedTask(t *testing.T) {
	session, pod := executionSessionAndPod(t)
	pod.Status.InitContainerStatuses = nil
	store := testStore(t)
	provider := simulatorForTest()
	g, writer := testGateway(t, store, provider, executionObjects(t, session, pod)...)
	if response := request(g, http.MethodPost, _testMessagePath, "close-before-prepare", `{"text":"Do not run after close"}`); response.Code != http.StatusAccepted {
		t.Fatal(response.Code)
	}
	if err := g.Quiesce(t.Context(), _testUID, _testRemoteID); err != nil {
		t.Fatal(err)
	}
	var current corev1.Pod
	if err := writer.Get(t.Context(), client.ObjectKeyFromObject(pod), &current); err != nil {
		t.Fatal(err)
	}
	for _, init := range current.Spec.InitContainers {
		current.Status.InitContainerStatuses = append(current.Status.InitContainerStatuses, corev1.ContainerStatus{Name: init.Name,
			State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0}}})
	}
	updateExecutionPod(t, writer, &current)
	if err := g.sync(t.Context(), _testUID); err != nil {
		t.Fatal(err)
	}
	if provider.posts != 0 || !mustRecord(t, store).Closed {
		t.Fatal("closing released previously queued ordinary input")
	}
}

func updateExecutionPod(t *testing.T, writer client.Client, pod *corev1.Pod) {
	t.Helper()
	status := *pod.Status.DeepCopy()
	if err := writer.Update(t.Context(), pod); err != nil {
		t.Fatal(err)
	}
	pod.Status = status
	if err := writer.Status().Update(t.Context(), pod); err != nil {
		t.Fatal(err)
	}
}
func TestSelfHostedExecutionRequiresFrozenSandboxChainAndWorkspace(t *testing.T) {
	tests := []struct {
		name   string
		mutate func([]client.Object) []client.Object
	}{
		{name: "claim missing", mutate: func(objects []client.Object) []client.Object {
			return slices.DeleteFunc(objects, func(object client.Object) bool { _, ok := object.(*extensionsv1beta1.SandboxClaim); return ok })
		}},
		{name: "claim config baseline changed", mutate: func(objects []client.Object) []client.Object {
			claim := executionObject[*extensionsv1beta1.SandboxClaim](t, objects)
			claim.Annotations[sandbox.ConfigAnnotation] = "foreign-config"
			return objects
		}},
		{name: "claim env changed", mutate: func(objects []client.Object) []client.Object {
			claim := executionObject[*extensionsv1beta1.SandboxClaim](t, objects)
			claim.Spec.Env[0].Value = "foreign-environment"
			return objects
		}},
		{name: "claim replaced", mutate: func(objects []client.Object) []client.Object {
			claim := executionObject[*extensionsv1beta1.SandboxClaim](t, objects)
			claim.UID = "replacement-claim"
			return objects
		}},
		{name: "foreign claim owner", mutate: func(objects []client.Object) []client.Object {
			claim := executionObject[*extensionsv1beta1.SandboxClaim](t, objects)
			claim.OwnerReferences[0].UID = "foreign-session"
			return objects
		}},
		{name: "claim reassigned", mutate: func(objects []client.Object) []client.Object {
			claim := executionObject[*extensionsv1beta1.SandboxClaim](t, objects)
			claim.Status.SandboxStatus.Name = "foreign-sandbox"
			return objects
		}},
		{name: "claim terminating", mutate: func(objects []client.Object) []client.Object {
			claim := executionObject[*extensionsv1beta1.SandboxClaim](t, objects)
			claim.DeletionTimestamp = new(metav1.Now())
			claim.Finalizers = []string{"test/hold"}
			return objects
		}},
		{name: "sandbox replaced", mutate: func(objects []client.Object) []client.Object {
			runtime := executionObject[*sandboxv1beta1.Sandbox](t, objects)
			runtime.UID = "replacement-sandbox"
			return objects
		}},
		{name: "foreign sandbox owner", mutate: func(objects []client.Object) []client.Object {
			runtime := executionObject[*sandboxv1beta1.Sandbox](t, objects)
			runtime.OwnerReferences[0].UID = _executionForeignClaim
			return objects
		}},
		{name: "sandbox suspended", mutate: func(objects []client.Object) []client.Object {
			runtime := executionObject[*sandboxv1beta1.Sandbox](t, objects)
			runtime.Spec.OperatingMode = sandboxv1beta1.SandboxOperatingModeSuspended
			return objects
		}},
		{name: "sandbox changed", mutate: func(objects []client.Object) []client.Object {
			runtime := executionObject[*sandboxv1beta1.Sandbox](t, objects)
			runtime.Spec.PodTemplate.Spec.Containers[0].Image = "foreign@sha256:" + strings.Repeat("b", 64)
			return objects
		}},
		{name: "template replaced", mutate: func(objects []client.Object) []client.Object {
			template := executionObject[*extensionsv1beta1.SandboxTemplate](t, objects)
			template.UID = "replacement-template"
			return objects
		}},
		{name: "pool replaced", mutate: func(objects []client.Object) []client.Object {
			pool := executionObject[*extensionsv1beta1.SandboxWarmPool](t, objects)
			pool.UID = "replacement-pool"
			return objects
		}},
		{name: "workspace replaced", mutate: func(objects []client.Object) []client.Object {
			workspace := executionObject[*corev1.PersistentVolumeClaim](t, objects)
			workspace.UID = "replacement-workspace"
			return objects
		}},
		{name: "foreign workspace owner", mutate: func(objects []client.Object) []client.Object {
			workspace := executionObject[*corev1.PersistentVolumeClaim](t, objects)
			workspace.OwnerReferences[0].UID = "foreign-sandbox"
			return objects
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			session, pod := executionSessionAndPod(t)
			objects := tt.mutate(executionObjects(t, session, pod))
			store := testStore(t)
			provider := simulatorForTest()
			g, _ := testGateway(t, store, provider, objects...)
			response := request(g, http.MethodPost, _testMessagePath, "sandbox-chain", `{"text":"Run tests"}`)
			if response.Code != http.StatusAccepted {
				t.Fatal(response.Code, response.Body.String())
			}
			if err := g.sync(t.Context(), _testUID); err != nil {
				t.Fatal(err)
			}
			if provider.posts != 0 || mustRecord(t, store).Submissions[0].Attempts != 0 {
				t.Fatal("broken sandbox identity authorized execution")
			}
		})
	}
}

func executionObject[T client.Object](t *testing.T, objects []client.Object) T {
	t.Helper()
	for _, object := range objects {
		if selected, ok := object.(T); ok {
			return selected
		}
	}
	t.Fatal("execution fixture object is missing")
	var zero T
	return zero
}
