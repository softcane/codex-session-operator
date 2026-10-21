package controller

import (
	"context"
	"slices"
	"testing"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	"github.com/softcane/codex-session-operator/internal/sandbox"
	"github.com/softcane/codex-session-operator/internal/testutil"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	extensionsv1beta1 "sigs.k8s.io/agent-sandbox/extensions/api/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

const (
	_workspaceLifecycleNamespace  = "workspace-lifecycle"
	_workspaceLifecycleSession    = "session"
	_workspaceLifecycleUID        = "session-uid"
	_workspaceLifecycleConfig     = "source-config"
	_workspaceLifecycleForeignUID = "foreign-uid"
)

type workspaceLifecycleFixture struct {
	reconciler *SessionReconciler
	writer     *workspaceLifecycleClient
	session    *agentsv1alpha1.Session
	claim      *extensionsv1beta1.SandboxClaim
	sandbox    *sandboxv1beta1.Sandbox
	pod        *corev1.Pod
	workspace  *corev1.PersistentVolumeClaim
	template   *extensionsv1beta1.SandboxTemplate
}

type workspaceLifecycleClient struct {
	client.Client
	deletions []client.DeleteOptions
}

func (c *workspaceLifecycleClient) Delete(ctx context.Context, object client.Object, options ...client.DeleteOption) error {
	var applied client.DeleteOptions
	for _, option := range options {
		option.ApplyToDelete(&applied)
	}
	c.deletions = append(c.deletions, applied)
	return c.Client.Delete(ctx, object, options...)
}

func newWorkspaceLifecycleFixture(t *testing.T, policy string) *workspaceLifecycleFixture {
	t.Helper()
	scheme := k8sruntime.NewScheme()
	for _, register := range []func(*k8sruntime.Scheme) error{corev1.AddToScheme, agentsv1alpha1.AddToScheme, sandboxv1beta1.AddToScheme, extensionsv1beta1.AddToScheme} {
		if err := register(scheme); err != nil {
			t.Fatal(err)
		}
	}
	pool, template := testutil.NewSandboxPool(_workspaceLifecycleNamespace)
	profile := &agentsv1alpha1.SelfHostedEnvironment{SandboxPoolRef: agentsv1alpha1.AgentReference{Name: pool.Name}, Workspace: agentsv1alpha1.SelfHostedWorkspace{Directory: defaultWorkspace, DeletionPolicy: agentsv1alpha1.DeletionPolicy(policy)}}
	writer := &workspaceLifecycleClient{Client: fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&agentsv1alpha1.Session{}).WithObjects(pool, template).Build()}
	poolSnapshot, err := sandbox.ResolvePool(t.Context(), writer, _workspaceLifecycleNamespace, profile)
	if err != nil {
		t.Fatal(err)
	}
	session := &agentsv1alpha1.Session{ObjectMeta: metav1.ObjectMeta{Name: _workspaceLifecycleSession, Namespace: _workspaceLifecycleNamespace, UID: _workspaceLifecycleUID}, Status: agentsv1alpha1.SessionStatus{
		EnvironmentID: "environment-1", RemoteURL: "wss://example.test/executor", ExecutorConfigDigest: _workspaceLifecycleConfig,
		EnvironmentSnapshot: &agentsv1alpha1.SessionEnvironmentSnapshot{Profile: agentsv1alpha1.EnvironmentSpec{Mode: testSelfHostedMode, SelfHosted: profile}, SandboxPool: poolSnapshot}}}
	claim, err := sandbox.Claim(session, template, _workspaceLifecycleConfig)
	if err != nil {
		t.Fatal(err)
	}
	claim.UID = "claim-uid"
	claim.Status.SandboxStatus.Name = claim.Name
	runtime := &sandboxv1beta1.Sandbox{ObjectMeta: metav1.ObjectMeta{Name: claim.Name, Namespace: session.Namespace, UID: "sandbox-uid",
		OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(claim, extensionsv1beta1.GroupVersion.WithKind("SandboxClaim"))}},
		Spec: sandboxv1beta1.SandboxSpec{OperatingMode: sandboxv1beta1.SandboxOperatingModeRunning, SandboxBlueprint: *sandbox.ExpectedBlueprint(template, claim)}}
	pod, err := sandbox.ExpectedPod(runtime)
	if err != nil {
		t.Fatal(err)
	}
	pod.UID = "pod-uid"
	pod.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(runtime, sandboxv1beta1.GroupVersion.WithKind("Sandbox"))}
	workspace := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: sandbox.WorkspaceClaimName(runtime), Namespace: session.Namespace, UID: "workspace-uid",
		OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(runtime, sandboxv1beta1.GroupVersion.WithKind("Sandbox"))}}, Spec: *runtime.Spec.VolumeClaimTemplates[0].Spec.DeepCopy()}
	session.Status.SandboxClaimName, session.Status.SandboxClaimUID = claim.Name, string(claim.UID)
	session.Status.SandboxName, session.Status.SandboxUID = runtime.Name, string(runtime.UID)
	session.Status.PodName, session.Status.ExecutorPodUID = pod.Name, string(pod.UID)
	session.Status.WorkspaceClaimUID = string(workspace.UID)
	for _, object := range []client.Object{session, claim, runtime, pod, workspace} {
		if err := writer.Create(t.Context(), object); err != nil {
			t.Fatal(err)
		}
	}
	return &workspaceLifecycleFixture{reconciler: &SessionReconciler{Client: writer, APIReader: writer, Scheme: scheme}, writer: writer, session: session, claim: claim, sandbox: runtime, pod: pod, workspace: workspace, template: template}
}

func TestRetainedWorkspaceOrphansOnlyStoppedSandboxAndDeletesClaim(t *testing.T) {
	fixture := newWorkspaceLifecycleFixture(t, sandbox.WorkspaceClaimRetain)
	for range 2 {
		pending, err := fixture.reconciler.finalizeWorkspaceClaim(t.Context(), fixture.session)
		if err != nil || !pending {
			t.Fatal("retention failed to wait for compute shutdown", pending, err)
		}
		if err := fixture.writer.Get(t.Context(), client.ObjectKeyFromObject(fixture.sandbox), fixture.sandbox); err != nil {
			t.Fatal(err)
		}
		if fixture.sandbox.Spec.OperatingMode != sandboxv1beta1.SandboxOperatingModeSuspended || !metav1.IsControlledBy(fixture.sandbox, fixture.claim) {
			t.Fatal("retention detached a sandbox while its Pod still exists")
		}
	}
	if err := fixture.writer.Client.Delete(t.Context(), fixture.pod); err != nil {
		t.Fatal(err)
	}
	pending, err := fixture.reconciler.finalizeWorkspaceClaim(t.Context(), fixture.session)
	if err != nil || !pending {
		t.Fatal("retention did not persist orphaning before claim deletion", err)
	}
	if err := fixture.writer.Get(t.Context(), client.ObjectKeyFromObject(fixture.sandbox), fixture.sandbox); err != nil {
		t.Fatal(err)
	}
	if len(fixture.sandbox.OwnerReferences) != 0 || fixture.sandbox.Annotations[sandbox.WorkspaceRetainedAnnotation] != string(fixture.session.UID) {
		t.Fatal("stopped sandbox was not durably retained")
	}
	if len(fixture.writer.deletions) != 0 {
		t.Fatal("claim deleted before retained runtime was detached")
	}
	pending, err = fixture.reconciler.finalizeWorkspaceClaim(t.Context(), fixture.session)
	if err != nil || !pending {
		t.Fatal("claim cleanup was not requested", err)
	}
	assertWorkspaceClaimDeletion(t, fixture)
	if err := fixture.writer.Get(t.Context(), client.ObjectKeyFromObject(fixture.claim), &extensionsv1beta1.SandboxClaim{}); !apierrors.IsNotFound(err) {
		t.Fatal("live claim can recreate retained compute", err)
	}
	pending, err = fixture.reconciler.finalizeWorkspaceClaim(t.Context(), fixture.session)
	if err != nil || pending {
		t.Fatal("retained workspace cleanup never completed", err)
	}
	if err := fixture.writer.Get(t.Context(), client.ObjectKeyFromObject(fixture.workspace), fixture.workspace); err != nil {
		t.Fatal("retained files were removed", err)
	}
	if !metav1.IsControlledBy(fixture.workspace, fixture.sandbox) {
		t.Fatal("workspace lost its stopped sandbox owner")
	}
	if err := fixture.writer.Get(t.Context(), client.ObjectKeyFromObject(fixture.sandbox), fixture.sandbox); err != nil || fixture.sandbox.Spec.OperatingMode != sandboxv1beta1.SandboxOperatingModeSuspended {
		t.Fatal("retained runtime disappeared or resumed", err)
	}
}

func TestDeleteWorkspaceRequestsForegroundCascadeAndWaitsForAbsence(t *testing.T) {
	fixture := newWorkspaceLifecycleFixture(t, sandbox.WorkspaceClaimDelete)
	fixture.claim.Finalizers = []string{"test/foreground-cleanup"}
	if err := fixture.writer.Update(t.Context(), fixture.claim); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		pending, err := fixture.reconciler.finalizeWorkspaceClaim(t.Context(), fixture.session)
		if err != nil || !pending {
			t.Fatal("cleanup stopped waiting for foreground deletion", pending, err)
		}
	}
	assertWorkspaceClaimDeletion(t, fixture)
	if len(fixture.writer.deletions) != 1 {
		t.Fatal("repeated cleanup issued a second delete")
	}
	if err := fixture.writer.Get(t.Context(), client.ObjectKeyFromObject(fixture.claim), fixture.claim); err != nil || fixture.claim.DeletionTimestamp.IsZero() {
		t.Fatal("claim was not held during cascade", err)
	}
	for _, object := range []client.Object{fixture.pod, fixture.workspace, fixture.sandbox} {
		if err := fixture.writer.Client.Delete(t.Context(), object); err != nil {
			t.Fatal(err)
		}
	}
	fixture.claim.Finalizers = nil
	if err := fixture.writer.Update(t.Context(), fixture.claim); err != nil {
		t.Fatal(err)
	}
	pending, err := fixture.reconciler.finalizeWorkspaceClaim(t.Context(), fixture.session)
	if err != nil || pending {
		t.Fatal("physically absent runtime kept cleanup pending", err)
	}
}

func assertWorkspaceClaimDeletion(t *testing.T, fixture *workspaceLifecycleFixture) {
	t.Helper()
	if len(fixture.writer.deletions) != 1 {
		t.Fatal("expected one native claim deletion")
	}
	options := fixture.writer.deletions[0]
	if options.PropagationPolicy == nil || *options.PropagationPolicy != metav1.DeletePropagationForeground || options.Preconditions == nil || options.Preconditions.UID == nil || *options.Preconditions.UID != fixture.claim.UID {
		t.Fatal("claim deletion lacks foreground propagation or frozen UID")
	}
}

func TestWorkspaceObservationRejectsReplacedOrForeignStorage(t *testing.T) {
	for _, name := range []string{"replaced UID", "foreign workspace owner", "changed storage"} {
		t.Run(name, func(t *testing.T) {
			fixture := newWorkspaceLifecycleFixture(t, sandbox.WorkspaceClaimRetain)
			switch name {
			case "replaced UID":
				if err := fixture.writer.Client.Delete(t.Context(), fixture.workspace); err != nil {
					t.Fatal(err)
				}
				fixture.workspace.UID, fixture.workspace.ResourceVersion = _workspaceLifecycleForeignUID, ""
				if err := fixture.writer.Create(t.Context(), fixture.workspace); err != nil {
					t.Fatal(err)
				}
			case "foreign workspace owner":
				fixture.workspace.OwnerReferences[0].UID = _workspaceLifecycleForeignUID
			case "changed storage":
				fixture.workspace.Spec.StorageClassName = new("foreign-storage")
			}
			if err := fixture.writer.Update(t.Context(), fixture.workspace); err != nil {
				t.Fatal(err)
			}
			if err := fixture.reconciler.observeWorkspace(t.Context(), fixture.session, fixture.sandbox, fixture.pod); err == nil {
				t.Fatal("unverified workspace was adopted")
			}
			proof := &agentsv1alpha1.SessionWorkspacePreparationStatus{SandboxUID: string(fixture.sandbox.UID), ClaimUID: fixture.session.Status.WorkspaceClaimUID}
			if fixture.reconciler.revalidatePreparedClaim(t.Context(), fixture.session, proof) {
				t.Fatal("revalidation accepted a changed workspace after preparation")
			}
		})
	}
}

func TestRetainRejectsChangedWorkspaceBeforeOrphaning(t *testing.T) {
	fixture := newWorkspaceLifecycleFixture(t, sandbox.WorkspaceClaimRetain)
	fixture.sandbox.Spec.OperatingMode = sandboxv1beta1.SandboxOperatingModeSuspended
	if err := fixture.writer.Update(t.Context(), fixture.sandbox); err != nil {
		t.Fatal(err)
	}
	if err := fixture.writer.Client.Delete(t.Context(), fixture.pod); err != nil {
		t.Fatal(err)
	}
	fixture.session.Status.WorkspaceClaimUID = _workspaceLifecycleForeignUID
	pending, err := fixture.reconciler.finalizeWorkspaceClaim(t.Context(), fixture.session)
	if err == nil || !pending {
		t.Fatal("retention adopted a replacement workspace")
	}
	if len(fixture.writer.deletions) != 0 {
		t.Fatal("failed retention deleted the native claim")
	}
	if err := fixture.writer.Get(t.Context(), client.ObjectKeyFromObject(fixture.sandbox), fixture.sandbox); err != nil {
		t.Fatal(err)
	}
	if !metav1.IsControlledBy(fixture.sandbox, fixture.claim) {
		t.Fatal("failed retention orphaned unverified storage")
	}
}

func TestSessionFinalizeRetainsWorkspaceAndReleasesFinalizer(t *testing.T) {
	for _, earlyClose := range []bool{false, true} {
		t.Run(map[bool]string{false: "observed allocation", true: "close before allocation observation"}[earlyClose], func(t *testing.T) {
			fixture := newWorkspaceLifecycleFixture(t, sandbox.WorkspaceClaimRetain)
			if earlyClose {
				fixture.session.Status.SandboxName, fixture.session.Status.SandboxUID, fixture.session.Status.PodName = "", "", ""
				fixture.session.Status.WorkspaceClaimUID, fixture.session.Status.ExecutorPodUID = "", ""
				if err := fixture.writer.Status().Update(t.Context(), fixture.session); err != nil {
					t.Fatal(err)
				}
				fixture.claim.Status.SandboxStatus.Name = ""
				if err := fixture.writer.Update(t.Context(), fixture.claim); err != nil {
					t.Fatal(err)
				}
			}
			fixture.session.Finalizers = []string{sessionFinalizer}
			if err := fixture.writer.Update(t.Context(), fixture.session); err != nil {
				t.Fatal(err)
			}
			completed := false
			for range 10 {
				if err := fixture.writer.Get(t.Context(), client.ObjectKeyFromObject(fixture.session), fixture.session); err != nil {
					t.Fatal(err)
				}
				if _, err := fixture.reconciler.finalize(t.Context(), fixture.session); err != nil {
					t.Fatal("full Retain finalization blocked", err)
				}
				if err := fixture.writer.Get(t.Context(), client.ObjectKeyFromObject(fixture.sandbox), fixture.sandbox); err != nil {
					t.Fatal("retained Sandbox disappeared", err)
				}
				if fixture.sandbox.Spec.OperatingMode != sandboxv1beta1.SandboxOperatingModeSuspended {
					t.Fatal("finalization left compute running")
				}
				// Emulate the upstream controller honoring suspension, independently of us.
				if err := fixture.writer.Client.Delete(t.Context(), fixture.pod); err != nil && !apierrors.IsNotFound(err) {
					t.Fatal(err)
				}
				if !slices.Contains(fixture.session.Finalizers, sessionFinalizer) {
					completed = true
					break
				}
			}
			if !completed {
				t.Fatal("retained allocation kept Session finalizer indefinitely")
			}
			if err := fixture.writer.Get(t.Context(), client.ObjectKeyFromObject(fixture.claim), &extensionsv1beta1.SandboxClaim{}); !apierrors.IsNotFound(err) {
				t.Fatal("finalizer released while recreating Claim remains", err)
			}
			if len(fixture.sandbox.OwnerReferences) != 0 || fixture.sandbox.Annotations[sandbox.WorkspaceRetainedAnnotation] != string(fixture.session.UID) {
				t.Fatal("retained Sandbox was not safely detached")
			}
			if err := fixture.writer.Get(t.Context(), client.ObjectKeyFromObject(fixture.workspace), fixture.workspace); err != nil || string(fixture.workspace.UID) != fixture.session.Status.WorkspaceClaimUID {
				t.Fatal("retention lost or failed to freeze the original files", err)
			}
		})
	}
}

func TestEarlyRetainCloseDeletesUnallocatedClaim(t *testing.T) {
	fixture := newWorkspaceLifecycleFixture(t, sandbox.WorkspaceClaimRetain)
	for _, object := range []client.Object{fixture.pod, fixture.workspace, fixture.sandbox} {
		if err := fixture.writer.Client.Delete(t.Context(), object); err != nil {
			t.Fatal(err)
		}
	}
	fixture.session.Status.SandboxName, fixture.session.Status.SandboxUID, fixture.session.Status.PodName, fixture.session.Status.WorkspaceClaimUID = "", "", "", ""
	fixture.claim.Status.SandboxStatus.Name = ""
	if err := fixture.writer.Update(t.Context(), fixture.claim); err != nil {
		t.Fatal(err)
	}
	if pending, err := fixture.reconciler.finalizeWorkspaceClaim(t.Context(), fixture.session); err != nil || !pending {
		t.Fatal("unallocated Claim was not deleted", pending, err)
	}
	if err := fixture.writer.Get(t.Context(), client.ObjectKeyFromObject(fixture.claim), &extensionsv1beta1.SandboxClaim{}); !apierrors.IsNotFound(err) {
		t.Fatal("early close leaked an unallocated Claim", err)
	}
}
