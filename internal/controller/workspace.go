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
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// reconcileWorkspaceClaim observes a UID-frozen claim before mounting it. The
// second return reports an internal status commit; callers must not patch again
// using their older optimistic-lock base.
func (r *SessionReconciler) reconcileWorkspaceClaim(ctx context.Context, session *agentsv1alpha1.Session) (bool, bool, error) {
	storage := sandbox.PersistentWorkspace(session)
	if storage == nil {
		return true, false, nil
	}
	if err := sandbox.ValidateWorkspaceStorage(storage, r.WorkspaceStorageClasses); err != nil {
		executorState(session, "Failed", "WorkspaceStorageInvalid", "Frozen workspace storage class or policy is not approved")
		return false, false, nil
	}
	desired, err := sandbox.WorkspaceClaim(session)
	if err != nil {
		executorState(session, "Failed", "WorkspaceStorageInvalid", "Frozen workspace claim configuration is invalid")
		return false, false, nil
	}
	if storage.ClaimDeletionPolicy == deletePolicy {
		if err := controllerutil.SetControllerReference(session, desired, r.Scheme); err != nil {
			return false, false, err
		}
	}
	current := &corev1.PersistentVolumeClaim{}
	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}
	err = reader.Get(ctx, client.ObjectKeyFromObject(desired), current)
	if apierrors.IsNotFound(err) {
		return r.createMissingWorkspaceClaim(ctx, session, desired, storage)
	}
	if err != nil {
		return false, false, err
	}
	if storage.ClaimDeletionPolicy == retainPolicy && session.Status.WorkspaceClaimUID == "" &&
		current.Labels[sandbox.WorkspaceSessionUIDLabel] == string(session.UID) && session.Status.WorkspaceClaimCreationAttempted {
		executorState(session, "Failed", "WorkspaceClaimOutcomeUnknown", "Retained claim exists but its UID was not durably recorded; audit is required")
		return false, false, nil
	}
	if current.Labels[sandbox.WorkspaceSessionUIDLabel] != string(session.UID) ||
		(storage.ClaimDeletionPolicy == deletePolicy && !metav1.IsControlledBy(current, session)) ||
		(storage.ClaimDeletionPolicy == retainPolicy && (!session.Status.WorkspaceClaimCreationAttempted ||
			len(current.OwnerReferences) != 0 || session.Status.WorkspaceClaimUID == "")) {
		executorState(session, "Failed", "WorkspaceOwnershipMismatch", "Workspace claim belongs to another owner; adoption is blocked")
		return false, false, nil
	}
	if !current.DeletionTimestamp.IsZero() || !workspaceClaimMatches(current, desired) {
		executorState(session, "Failed", "WorkspaceClaimChanged", "Frozen workspace claim was changed or is deleting")
		return false, false, nil
	}
	if session.Status.WorkspaceClaimUID != "" && session.Status.WorkspaceClaimUID != string(current.UID) {
		executorState(session, "Failed", "WorkspaceClaimReplaced", "Workspace claim UID changed; automatic adoption is blocked")
		return false, false, nil
	}
	if session.Status.WorkspaceClaimUID == "" {
		session.Status.WorkspaceClaimUID = string(current.UID)
		executorState(session, "Provisioning", "WorkspaceClaimObserved", "Workspace claim identity is frozen before executor start")
		return false, false, nil
	}
	return true, false, nil
}

func (r *SessionReconciler) createMissingWorkspaceClaim(ctx context.Context, session *agentsv1alpha1.Session,
	desired *corev1.PersistentVolumeClaim, storage *agentsv1alpha1.WorkspaceStorage) (bool, bool, error) {
	if session.Status.WorkspaceClaimUID != "" {
		executorState(session, "Failed", "WorkspaceClaimMissing", "Frozen workspace claim disappeared; automatic replacement would lose files")
		return false, false, nil
	}
	if storage.ClaimDeletionPolicy != retainPolicy {
		if err := r.Create(ctx, desired); err != nil && !apierrors.IsAlreadyExists(err) {
			return false, false, err
		}
		executorState(session, "Provisioning", "WorkspaceClaimProvisioning", "Workspace claim creation requested")
		return false, false, nil
	}
	if session.Status.WorkspaceClaimCreationAttempted {
		executorState(session, "Failed", "WorkspaceClaimOutcomeUnknown", "Retained claim create outcome is unknown; audit is required")
		return false, false, nil
	}
	base := session.DeepCopy()
	session.Status.WorkspaceClaimCreationAttempted = true
	executorState(session, "Provisioning", "WorkspaceClaimCreating", "Retained workspace claim creation has been claimed")
	if err := r.patchStatus(ctx, session, base); err != nil {
		return false, false, err
	}
	claimed := session.DeepCopy()
	if err := r.Create(ctx, desired); err != nil {
		return false, true, err
	}
	if desired.UID == "" {
		return false, true, errors.New("created retained workspace claim has no UID")
	}
	session.Status.WorkspaceClaimUID = string(desired.UID)
	executorState(session, "Provisioning", "WorkspaceClaimObserved", "Retained workspace claim identity is frozen")
	return false, true, r.patchStatus(ctx, session, claimed)
}

func workspaceClaimMatches(current, desired *corev1.PersistentVolumeClaim) bool {
	actual := current.Spec.DeepCopy()
	actual.VolumeName = "" // Binding assigns a volume after claim creation.
	if actual.VolumeMode != nil && *actual.VolumeMode == corev1.PersistentVolumeFilesystem {
		actual.VolumeMode = nil // API default.
	}
	return reflect.DeepEqual(actual, &desired.Spec)
}

// finalizeWorkspaceClaim runs only after the executor Pod has disappeared and the
// remote Session has been observed absent. Retain claims never have a Session
func (r *SessionReconciler) finalizeWorkspaceClaim(ctx context.Context, session *agentsv1alpha1.Session) (bool, error) {
	storage := sandbox.PersistentWorkspace(session)
	if storage == nil {
		return false, nil
	}
	claim := &corev1.PersistentVolumeClaim{}
	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}
	err := reader.Get(ctx, client.ObjectKey{Namespace: session.Namespace, Name: sandbox.WorkspaceClaimNameForUID(string(session.UID))}, claim)
	if apierrors.IsNotFound(err) {
		if storage.ClaimDeletionPolicy == retainPolicy && session.Status.WorkspaceClaimCreationAttempted {
			return true, errors.New("retained workspace claim is missing; audit the creation outcome or data loss")
		}
		return false, nil
	}
	if err != nil {
		return true, err
	}
	if session.Status.WorkspaceClaimUID != "" && session.Status.WorkspaceClaimUID != string(claim.UID) {
		return true, errors.New("workspace claim UID differs from the frozen Session identity")
	}
	if claim.Labels[sandbox.WorkspaceSessionUIDLabel] != string(session.UID) ||
		(storage.ClaimDeletionPolicy == deletePolicy && !metav1.IsControlledBy(claim, session)) ||
		(storage.ClaimDeletionPolicy == retainPolicy && (!session.Status.WorkspaceClaimCreationAttempted ||
			len(claim.OwnerReferences) != 0 || session.Status.WorkspaceClaimUID == "")) {
		return true, errors.New("workspace claim ownership could not be verified")
	}
	if storage.ClaimDeletionPolicy == deletePolicy && session.Status.WorkspaceClaimUID == "" {
		session.Status.WorkspaceClaimUID = string(claim.UID)
		return true, nil
	}
	if storage.ClaimDeletionPolicy == deletePolicy {
		if claim.DeletionTimestamp.IsZero() {
			if err := r.Delete(ctx, claim, client.Preconditions{UID: &claim.UID}); err != nil && !apierrors.IsNotFound(err) {
				return true, err
			}
		}
		return true, nil
	}
	if storage.ClaimDeletionPolicy != retainPolicy || session.Status.WorkspaceClaimUID != string(claim.UID) {
		return true, errors.New("workspace retention requires a frozen claim UID")
	}
	if claim.Annotations[sandbox.WorkspaceRetainedAnnotation] == string(session.UID) {
		return false, nil
	}
	base := claim.DeepCopy()
	if claim.Annotations == nil {
		claim.Annotations = make(map[string]string, 1)
	}
	claim.Annotations[sandbox.WorkspaceRetainedAnnotation] = string(session.UID)
	if err := r.Patch(ctx, claim, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})); err != nil {
		return true, err
	}
	return true, nil
}
