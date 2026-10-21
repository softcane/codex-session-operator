package controller

import (
	"context"
	"errors"
	"testing"

	"github.com/softcane/codex-session-operator/internal/openaiapi"
	"github.com/softcane/codex-session-operator/internal/sandbox"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	sandboxv1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	extensions "sigs.k8s.io/agent-sandbox/extensions/api/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const _nativeClaimKind = "claim"
const _changedIdentityUID = "changed"

type connectionStub struct {
	state openaiapi.Connection
	err   error
}

func (c *connectionStub) Observe(_, _ string) (openaiapi.Connection, error) { return c.state, c.err }
func (*connectionStub) Forget(string)                                       {}

func TestClaimProvisioningFreezesIdentitiesAndChecksConnection(t *testing.T) {
	r, s, observer := preparationFixture(t)
	for range 5 {
		preparationStep(t, r, s)
	}
	var claim extensions.SandboxClaim
	if err := r.Get(t.Context(), client.ObjectKey{Namespace: s.Namespace, Name: sandbox.ClaimName(s)}, &claim); err != nil {
		t.Fatal(err)
	}
	if !metav1.IsControlledBy(&claim, s) || claim.UID == "" || s.Status.SandboxClaimUID != string(claim.UID) {
		t.Fatal("claim identity was not frozen")
	}
	if len(claim.Spec.Env) != 3 || claim.Spec.Lifecycle != nil || len(claim.Spec.VolumeClaimTemplates) != 0 {
		t.Fatal("claim must only contain safe binding values")
	}
	pod := preparationPod(t, r, s)
	healthyPreparationPod(t, r, pod)
	preparationStep(t, r, s)
	if meta.IsStatusConditionTrue(s.Status.Conditions, readyCondition) {
		t.Fatal("Pod health authorized disconnected input")
	}
	observer.state.Connected = true
	observer.state.Sequence++
	preparationStep(t, r, s)
	if !meta.IsStatusConditionTrue(s.Status.Conditions, readyCondition) || s.Status.WorkspacePreparation == nil {
		t.Fatal("prepared connected runtime did not become ready")
	}
	oldClaim, oldRuntime, oldWorkspace := s.Status.SandboxClaimUID, s.Status.SandboxUID, s.Status.WorkspaceClaimUID
	observer.state.Connected = false
	preparationStep(t, r, s)
	if meta.IsStatusConditionTrue(s.Status.Conditions, readyCondition) {
		t.Fatal("disconnect retained execution readiness")
	}
	if oldClaim != s.Status.SandboxClaimUID || oldRuntime != s.Status.SandboxUID || oldWorkspace != s.Status.WorkspaceClaimUID {
		t.Fatal("connection change replaced native state")
	}
}

func TestCredentialRotationRestartsSameSandboxAndWorkspace(t *testing.T) {
	r, s, observer := preparationFixture(t)
	for range 5 {
		preparationStep(t, r, s)
	}
	pod := preparationPod(t, r, s)
	healthyPreparationPod(t, r, pod)
	observer.state.Connected = true
	observer.state.Sequence = 1
	preparationStep(t, r, s)
	claimUID, runtimeUID, workspaceUID := s.Status.SandboxClaimUID, s.Status.SandboxUID, s.Status.WorkspaceClaimUID
	oldPodUID := pod.UID
	var credential corev1.Secret
	if err := r.Get(t.Context(), client.ObjectKey{Namespace: s.Namespace, Name: sandbox.ExecutorSecretName}, &credential); err != nil {
		t.Fatal(err)
	}
	credential.Data[sandbox.ExecutorSecretKey] = []byte("rotated-restricted-fixture")
	if err := r.Update(t.Context(), &credential); err != nil {
		t.Fatal(err)
	}
	for range 6 {
		preparationStep(t, r, s)
	}
	replacement := preparationPod(t, r, s)
	if replacement.UID == oldPodUID || claimUID != s.Status.SandboxClaimUID || runtimeUID != s.Status.SandboxUID || workspaceUID != s.Status.WorkspaceClaimUID {
		t.Fatal("rotation must replace compute and preserve allocation/storage")
	}
	if s.Status.WorkspacePreparation != nil {
		t.Fatal("old process proof survived rotation")
	}
	healthyPreparationPod(t, r, replacement)
	observer.state.Sequence++
	preparationStep(t, r, s)
	if !meta.IsStatusConditionTrue(s.Status.Conditions, readyCondition) {
		t.Fatal("rotated executor did not recover")
	}
}

func TestMissingOrReplacedNativeAllocationIsNeverAdopted(t *testing.T) {
	for _, kind := range []string{_nativeClaimKind, "sandbox", "workspace"} {
		t.Run(kind, func(t *testing.T) {
			r, s, _ := preparationFixture(t)
			for range 5 {
				preparationStep(t, r, s)
			}
			var object client.Object
			switch kind {
			case _nativeClaimKind:
				object = &extensions.SandboxClaim{ObjectMeta: metav1.ObjectMeta{Name: s.Status.SandboxClaimName, Namespace: s.Namespace}}
			case "sandbox":
				object = &sandboxv1.Sandbox{ObjectMeta: metav1.ObjectMeta{Name: s.Status.SandboxName, Namespace: s.Namespace}}
			default:
				object = &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "workspace-" + s.Status.SandboxName, Namespace: s.Namespace}}
			}
			if err := r.Get(t.Context(), client.ObjectKeyFromObject(object), object); err != nil {
				t.Fatal(err)
			}
			if err := r.Delete(t.Context(), object, client.GracePeriodSeconds(0)); err != nil {
				t.Fatal(err)
			}
			_, err := r.reconcileExecutor(t.Context(), s)
			if err == nil && meta.IsStatusConditionTrue(s.Status.Conditions, readyCondition) {
				t.Fatal("missing allocation authorized execution")
			}
			if kind == _nativeClaimKind {
				var current extensions.SandboxClaim
				if err := r.Get(t.Context(), client.ObjectKeyFromObject(object), &current); !apierrors.IsNotFound(err) {
					t.Fatal("missing claim was silently replaced", err)
				}
			}
		})
	}
}

func TestCredentialRemovalStopsComputeDuringProviderOutage(t *testing.T) {
	r, s, _ := preparationFixture(t)
	for range 5 {
		preparationStep(t, r, s)
	}
	var credential corev1.Secret
	if err := r.Get(t.Context(), client.ObjectKey{Namespace: s.Namespace, Name: sandbox.ExecutorSecretName}, &credential); err != nil {
		t.Fatal(err)
	}
	if err := r.Delete(t.Context(), &credential); err != nil {
		t.Fatal(err)
	}
	secret, err := r.executorCredential(t.Context(), s)
	if err != nil || secret != nil {
		t.Fatal("credential removal handling failed", err)
	}
	var runtime sandboxv1.Sandbox
	if err := r.Get(t.Context(), client.ObjectKey{Namespace: s.Namespace, Name: s.Status.SandboxName}, &runtime); err != nil {
		t.Fatal(err)
	}
	if runtime.Spec.OperatingMode != sandboxv1.SandboxOperatingModeSuspended {
		t.Fatal("revoked compute was not suspended")
	}
	if s.Status.WorkspacePreparation != nil {
		t.Fatal("revocation retained workspace proof")
	}
}

func TestConnectionObservationErrorsHoldDelivery(t *testing.T) {
	r, s, observer := preparationFixture(t)
	for range 5 {
		preparationStep(t, r, s)
	}
	observer.err = errors.New("stream unavailable")
	_, err := r.reconcileExecutor(context.Background(), s)
	if err == nil || meta.IsStatusConditionTrue(s.Status.Conditions, readyCondition) {
		t.Fatal("observation error authorized execution")
	}
}
