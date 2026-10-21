package controller

import (
	"context"
	"errors"
	"reflect"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	"github.com/softcane/codex-session-operator/internal/sandbox"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	extensionsv1beta1 "sigs.k8s.io/agent-sandbox/extensions/api/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func (r *SessionReconciler) observeWorkspace(ctx context.Context, session *agentsv1alpha1.Session, runtime *sandboxv1beta1.Sandbox, pod *corev1.Pod) error {
	name, err := sandbox.WorkspaceMount(pod, session.Status.EnvironmentSnapshot.Profile.SelfHosted)
	if err != nil {
		return err
	}
	if name != sandbox.WorkspaceClaimName(runtime) {
		return errors.New("workspace does not belong to the allocated sandbox")
	}
	var claim corev1.PersistentVolumeClaim
	if err := r.reader().Get(ctx, client.ObjectKey{Namespace: session.Namespace, Name: name}, &claim); err != nil {
		return err
	}
	if !claim.DeletionTimestamp.IsZero() || !metav1.IsControlledBy(&claim, runtime) || string(claim.UID) == "" {
		return errors.New("sandbox workspace identity is unavailable")
	}
	if session.Status.WorkspaceClaimUID != "" && session.Status.WorkspaceClaimUID != string(claim.UID) {
		return errors.New("sandbox workspace was replaced; automatic adoption is blocked")
	}
	if len(runtime.Spec.VolumeClaimTemplates) != 1 || !workspaceClaimMatches(&claim, runtime.Spec.VolumeClaimTemplates[0].Spec) {
		return errors.New("sandbox workspace differs from its frozen template")
	}
	session.Status.WorkspaceClaimUID = string(claim.UID)
	return nil
}

func workspaceClaimMatches(current *corev1.PersistentVolumeClaim, expected corev1.PersistentVolumeClaimSpec) bool {
	actual := current.Spec.DeepCopy()
	desired := expected.DeepCopy()
	actual.VolumeName = ""
	if actual.VolumeMode != nil && *actual.VolumeMode == corev1.PersistentVolumeFilesystem {
		actual.VolumeMode = nil
	}
	if desired.VolumeMode != nil && *desired.VolumeMode == corev1.PersistentVolumeFilesystem {
		desired.VolumeMode = nil
	}
	return reflect.DeepEqual(actual, desired)
}

func (r *SessionReconciler) revalidatePreparedClaim(ctx context.Context, s *agentsv1alpha1.Session, proof *agentsv1alpha1.SessionWorkspacePreparationStatus) bool {
	var runtime sandboxv1beta1.Sandbox
	if r.reader().Get(ctx, client.ObjectKey{Namespace: s.Namespace, Name: s.Status.SandboxName}, &runtime) != nil || string(runtime.UID) != proof.SandboxUID || proof.ClaimUID != s.Status.WorkspaceClaimUID {
		return false
	}
	var claim corev1.PersistentVolumeClaim
	return r.reader().Get(ctx, client.ObjectKey{Namespace: s.Namespace, Name: sandbox.WorkspaceClaimName(&runtime)}, &claim) == nil &&
		string(claim.UID) == proof.ClaimUID && claim.DeletionTimestamp.IsZero() && metav1.IsControlledBy(&claim, &runtime) &&
		len(runtime.Spec.VolumeClaimTemplates) == 1 && workspaceClaimMatches(&claim, runtime.Spec.VolumeClaimTemplates[0].Spec)
}

func (r *SessionReconciler) preparedRuntimeMatches(ctx context.Context, s *agentsv1alpha1.Session, proof *agentsv1alpha1.SessionWorkspacePreparationStatus) bool {
	var runtime sandboxv1beta1.Sandbox
	return r.reader().Get(ctx, client.ObjectKey{Namespace: s.Namespace, Name: s.Status.SandboxName}, &runtime) == nil &&
		runtime.DeletionTimestamp.IsZero() && string(runtime.UID) == proof.SandboxUID && sandbox.ConfigDigest(&runtime) == proof.ConfigDigest &&
		runtime.Spec.PodTemplate.ObjectMeta.Annotations[sandbox.ConfigAnnotation] == s.Status.ExecutorConfigDigest
}

// finalizeWorkspaceClaim runs only after input is quiesced, compute is absent,
// and the remote Session is observed absent. Retention preserves a suspended
// Sandbox/PVC without a live Claim that could recreate Running compute.
func (r *SessionReconciler) finalizeWorkspaceClaim(ctx context.Context, s *agentsv1alpha1.Session) (bool, error) {
	if s.Status.EnvironmentSnapshot == nil || s.Status.EnvironmentSnapshot.Profile.SelfHosted == nil {
		return false, nil
	}
	claim, err := r.workspaceCleanupClaim(ctx, s)
	if err != nil {
		return true, err
	}
	if sandbox.WorkspacePolicy(s) == sandbox.WorkspaceClaimRetain {
		pending, err := r.retainWorkspace(ctx, s, claim)
		if err != nil || pending {
			return pending, err
		}
	}
	if claim == nil {
		return r.executorStillPresent(ctx, s)
	}
	if claim.DeletionTimestamp.IsZero() {
		if err := r.Delete(ctx, claim, client.Preconditions{UID: &claim.UID}, client.PropagationPolicy(metav1.DeletePropagationForeground)); err != nil {
			return true, client.IgnoreNotFound(err)
		}
	}
	return true, nil
}

func (r *SessionReconciler) workspaceCleanupClaim(ctx context.Context, s *agentsv1alpha1.Session) (*extensionsv1beta1.SandboxClaim, error) {
	var claim extensionsv1beta1.SandboxClaim
	key := client.ObjectKey{Namespace: s.Namespace, Name: sandbox.ClaimName(s)}
	if err := r.reader().Get(ctx, key, &claim); err != nil {
		return nil, client.IgnoreNotFound(err)
	}
	if !metav1.IsControlledBy(&claim, s) || s.Status.SandboxClaimUID != "" && string(claim.UID) != s.Status.SandboxClaimUID {
		return nil, errors.New("sandbox claim ownership changed during cleanup")
	}
	return &claim, nil
}

func (r *SessionReconciler) retainWorkspace(ctx context.Context, s *agentsv1alpha1.Session, claim *extensionsv1beta1.SandboxClaim) (bool, error) {
	runtime, retained, err := r.retentionSandbox(ctx, s, claim)
	if err != nil {
		return true, err
	}
	if runtime == nil {
		return false, nil
	}
	if s.Status.SandboxUID == "" {
		s.Status.SandboxName, s.Status.SandboxUID, s.Status.PodName = runtime.Name, string(runtime.UID), runtime.Name
		return true, nil // Persist identity before detaching the retained allocation.
	}
	if runtime.Spec.OperatingMode != sandboxv1beta1.SandboxOperatingModeSuspended {
		return true, r.suspendSandbox(ctx, runtime)
	}
	var pod corev1.Pod
	if err := r.reader().Get(ctx, client.ObjectKey{Namespace: s.Namespace, Name: runtime.Name}, &pod); err == nil {
		return true, nil
	} else if !apierrors.IsNotFound(err) {
		return true, err
	}
	observed, err := r.observeRetainedWorkspace(ctx, s, runtime)
	if err != nil || observed {
		return true, err
	}
	if retained {
		return false, nil
	}
	return true, r.orphanRetainedSandbox(ctx, s, runtime)
}

func (r *SessionReconciler) retentionSandbox(ctx context.Context, s *agentsv1alpha1.Session, claim *extensionsv1beta1.SandboxClaim) (*sandboxv1beta1.Sandbox, bool, error) {
	name := s.Status.SandboxName
	if name == "" && claim != nil {
		name = claim.Status.SandboxStatus.Name
	}
	if name == "" && claim != nil {
		name = claim.Name
	}
	if name == "" {
		return nil, false, nil
	}
	var runtime sandboxv1beta1.Sandbox
	if err := r.reader().Get(ctx, client.ObjectKey{Namespace: s.Namespace, Name: name}, &runtime); err != nil {
		if apierrors.IsNotFound(err) && s.Status.SandboxUID == "" {
			return nil, false, nil
		}
		return nil, false, err
	}
	if string(runtime.UID) != s.Status.SandboxUID && s.Status.SandboxUID != "" {
		return nil, false, errors.New("retained sandbox identity changed")
	}
	if !runtime.DeletionTimestamp.IsZero() {
		return nil, false, errors.New("retained sandbox is deleting")
	}
	retained := runtime.Annotations[sandbox.WorkspaceRetainedAnnotation] == string(s.UID) && len(runtime.OwnerReferences) == 0
	if !retained && (claim == nil || !metav1.IsControlledBy(&runtime, claim)) {
		return nil, false, errors.New("retained sandbox ownership is unverified")
	}
	return &runtime, retained, nil
}

// observeRetainedWorkspace pins an existing native claim before ownership changes.
// True requests status persistence on a separate reconciliation before orphaning.
func (r *SessionReconciler) observeRetainedWorkspace(ctx context.Context, s *agentsv1alpha1.Session, runtime *sandboxv1beta1.Sandbox) (bool, error) {
	var pvc corev1.PersistentVolumeClaim
	if err := r.reader().Get(ctx, client.ObjectKey{Namespace: s.Namespace, Name: sandbox.WorkspaceClaimName(runtime)}, &pvc); err != nil {
		if apierrors.IsNotFound(err) && s.Status.WorkspaceClaimUID == "" {
			return false, nil
		}
		return false, err
	}
	if pvc.UID == "" || !metav1.IsControlledBy(&pvc, runtime) || !pvc.DeletionTimestamp.IsZero() ||
		s.Status.WorkspaceClaimUID != "" && string(pvc.UID) != s.Status.WorkspaceClaimUID {
		return false, errors.New("retained workspace identity changed")
	}
	if len(runtime.Spec.VolumeClaimTemplates) != 1 || !workspaceClaimMatches(&pvc, runtime.Spec.VolumeClaimTemplates[0].Spec) {
		return false, errors.New("retained workspace configuration changed")
	}
	if s.Status.WorkspaceClaimUID == "" {
		s.Status.WorkspaceClaimUID = string(pvc.UID)
		return true, nil
	}
	return false, nil
}

func (r *SessionReconciler) orphanRetainedSandbox(ctx context.Context, s *agentsv1alpha1.Session, runtime *sandboxv1beta1.Sandbox) error {
	base := runtime.DeepCopy()
	runtime.OwnerReferences = nil
	if runtime.Annotations == nil {
		runtime.Annotations = make(map[string]string)
	}
	if runtime.Labels == nil {
		runtime.Labels = make(map[string]string)
	}
	runtime.Annotations[sandbox.WorkspaceRetainedAnnotation] = string(s.UID)
	runtime.Labels[sandbox.WorkspaceSessionUIDLabel] = string(s.UID)
	return r.Patch(ctx, runtime, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{}))
}
