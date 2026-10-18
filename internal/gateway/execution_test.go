package gateway

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	"github.com/softcane/codex-session-operator/internal/openaiapi"
	"github.com/softcane/codex-session-operator/internal/sandbox"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const _executionTestEnvironmentID = "env-execution"

func executionSessionAndPod(t *testing.T) (*agentsv1alpha1.Session, *corev1.Pod) {
	t.Helper()
	session := testSession()
	session.Generation = 1
	profile := &agentsv1alpha1.SelfHostedEnvironment{Image: "executor@sha256:" + strings.Repeat("a", 64),
		Architecture: "amd64", WorkspaceDirectory: "/workspace", WorkspaceSize: "1Gi",
		Repository: &agentsv1alpha1.RepositorySpec{URL: "https://example.com/project.git", Commit: strings.Repeat("a", 40)}}
	session.Status.EnvironmentSnapshot = &agentsv1alpha1.SessionEnvironmentSnapshot{
		Profile: agentsv1alpha1.EnvironmentSpec{Mode: _selfHostedMode, SelfHosted: profile}}
	session.Status.ExecutorImage = profile.Image
	session.Status.EnvironmentID = _executionTestEnvironmentID
	session.Status.RemoteURL = "wss://example.com/executor"
	session.Status.PodName = sandbox.PodName(session)
	session.Status.WorkspacePreparation = &agentsv1alpha1.SessionWorkspacePreparationStatus{
		PodUID: "prepared-pod", ConfigDigest: "prepared-config", RepositoryDigest: sandbox.RepositoryDigest(profile.Repository), InitialCommit: profile.Repository.Commit,
		ContainerID: "containerd://prepared-executor", StartedAt: metav1.NewTime(time.Date(2026, 10, 14, 10, 0, 0, 0, time.UTC))}
	session.Status.Conditions = []metav1.Condition{
		{Type: _readyCondition, Status: metav1.ConditionTrue, ObservedGeneration: 1},
		{Type: "WorkspacePrepared", Status: metav1.ConditionTrue, ObservedGeneration: 1},
		{Type: "EnvironmentConnected", Status: metav1.ConditionTrue, ObservedGeneration: 1}}
	pod, err := sandbox.Pod(session, "/workspace", "prepared-config")
	if err != nil {
		t.Fatal(err)
	}
	pod.UID = "prepared-pod"
	pod.OwnerReferences = []metav1.OwnerReference{{APIVersion: agentsv1alpha1.SchemeGroupVersion.String(), Kind: "Session",
		Name: session.Name, UID: session.UID, Controller: new(true)}}
	pod.Status.Phase = corev1.PodRunning
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "executor", Ready: true, ContainerID: session.Status.WorkspacePreparation.ContainerID,
		State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: session.Status.WorkspacePreparation.StartedAt}}}}
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	for _, init := range pod.Spec.InitContainers {
		pod.Status.InitContainerStatuses = append(pod.Status.InitContainerStatuses, corev1.ContainerStatus{Name: init.Name,
			State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0}}})
	}
	return session, pod
}

func TestSelfHostedExecutionRequiresCurrentOwnedPreparedConnectedPod(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*agentsv1alpha1.Session, *corev1.Pod)
	}{
		{name: "replacement", mutate: func(_ *agentsv1alpha1.Session, p *corev1.Pod) { p.UID = "replacement-pod" }},
		{name: "container restarted", mutate: func(_ *agentsv1alpha1.Session, p *corev1.Pod) { p.Status.ContainerStatuses[0].RestartCount = 1 }},
		{name: "container not ready", mutate: func(_ *agentsv1alpha1.Session, p *corev1.Pod) { p.Status.ContainerStatuses[0].Ready = false }},
		{name: "container replaced", mutate: func(_ *agentsv1alpha1.Session, p *corev1.Pod) {
			p.Status.ContainerStatuses[0].ContainerID = "replacement-container"
		}},
		{name: "foreign owner", mutate: func(_ *agentsv1alpha1.Session, p *corev1.Pod) { p.OwnerReferences[0].UID = _testForeignUID }},
		{name: "changed configuration", mutate: func(_ *agentsv1alpha1.Session, p *corev1.Pod) { p.Annotations[sandbox.ConfigAnnotation] = "changed" }},
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
			s.Status.WorkspacePreparation.ClaimUID = "foreign-claim"
		}},
		{name: "wrong repository", mutate: func(s *agentsv1alpha1.Session, _ *corev1.Pod) {
			s.Status.WorkspacePreparation.RepositoryDigest = "foreign-repository"
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
			g, writer := testGateway(t, store, provider, session, pod)
			response := request(g, http.MethodPost, _testMessagePath, _token, "prepared-message", `{"text":"Run tests"}`)
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
	g, writer := testGateway(t, store, provider, session, pod)
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
	response := request(g, http.MethodPost, _scenarioSessionPath+"/cancel", _token, "held-cancel", `{"turn_id":"turn-1"}`)
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
	g, writer := testGateway(t, store, provider, session, pod)
	if response := request(g, http.MethodPost, _testMessagePath, _token, "close-before-prepare", `{"text":"Do not run after close"}`); response.Code != http.StatusAccepted {
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
