package controller

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"strings"
	"time"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	"github.com/softcane/codex-session-operator/internal/openaiapi"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const retainPolicy = "Retain"

const vaultFinalizer = "agents.softcane.io/remote-vault"

// VaultProvider owns the project-scoped remote Vault lifecycle.
type VaultProvider interface {
	CreateVault(context.Context, string, string, agentsv1alpha1.VaultSpec) (openaiapi.Vault, error)
	GetVault(context.Context, string) (openaiapi.Vault, error)
	UpdateVault(context.Context, string, *string, bool, *map[string]string) (openaiapi.Vault, error)
	FindVault(context.Context, string, string) (openaiapi.Vault, error)
	DeleteVault(context.Context, string) error
}

var (
	_ VaultProvider        = (*openaiapi.Client)(nil)
	_ reconcile.Reconciler = (*VaultReconciler)(nil)
)

// VaultReconciler protects remote identity and referenced credentials.
type VaultReconciler struct {
	client.Client

	APIReader  client.Reader
	OpenAI     VaultProvider
	OwnerScope string
}

// +kubebuilder:rbac:groups=agents.softcane.io,resources=vaults,verbs=get;list;watch;patch
// +kubebuilder:rbac:groups=agents.softcane.io,resources=vaults/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=agents.softcane.io,resources=vaults/finalizers,verbs=update;patch
// +kubebuilder:rbac:groups=agents.softcane.io,resources=vaultcredentials,verbs=get;list;watch

func (r *VaultReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var v agentsv1alpha1.Vault
	if err := r.reader().Get(ctx, req.NamespacedName, &v); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !v.DeletionTimestamp.IsZero() && !controllerutil.ContainsFinalizer(&v, vaultFinalizer) {
		return ctrl.Result{}, nil
	}
	if r.OpenAI == nil {
		return ctrl.Result{}, errors.New("vault provider is not configured")
	}
	if v.Status.OwnerScope != "" && v.Status.OwnerScope != r.OwnerScope {
		return r.problem(ctx, &v, "OwnershipScopeChanged", "Restore the installation/project scope", nil)
	}
	if !v.DeletionTimestamp.IsZero() {
		return r.finalize(ctx, &v)
	}
	if r.OwnerScope == "" || len(r.OwnerScope) > 512 {
		return r.problem(ctx, &v, "OwnerScopeRequired", "Configure installation/project scope", nil)
	}
	if v.Spec.Name != nil {
		if err := openaiapi.ValidateVaultName(*v.Spec.Name); err != nil {
			return r.problem(ctx, &v, invalidConfigurationReason, err.Error(), nil)
		}
	}
	if v.Spec.Name != nil && v.Spec.ClearName {
		return r.problem(ctx, &v, invalidConfigurationReason, "Vault name and clearName conflict", nil)
	}
	if _, err := openaiapi.VaultMetadata(v.Spec.Metadata, nil, nil, string(v.UID), r.OwnerScope); err != nil {
		return r.problem(ctx, &v, invalidConfigurationReason, err.Error(), nil)
	}
	if !controllerutil.ContainsFinalizer(&v, vaultFinalizer) {
		base := v.DeepCopy()
		controllerutil.AddFinalizer(&v, vaultFinalizer)
		return ctrl.Result{RequeueAfter: time.Second}, r.Patch(ctx, &v, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{}))
	}
	if v.Status.OpenAIVaultID != "" {
		return r.observe(ctx, &v)
	}
	if v.Status.CreationAttempted {
		return r.discover(ctx, &v)
	}
	base := v.DeepCopy()
	v.Status.CreationAttempted, v.Status.OwnerScope = true, r.OwnerScope
	v.Status.ManagedMetadataKeys = metadataKeys(v.Spec.Metadata)
	vaultCondition(&v, metav1.ConditionUnknown, "CreatingVault", "Remote creation has been claimed")
	if err := r.status(ctx, &v, base); err != nil {
		return ctrl.Result{}, err
	}
	remote, err := r.OpenAI.CreateVault(ctx, string(v.UID), r.OwnerScope, v.Spec)
	if err != nil {
		return r.problem(ctx, &v, creationOutcomeUnknownReason, "Discover unique owned Vault before taking further action", err)
	}
	return r.record(ctx, &v, remote)
}
func (r *VaultReconciler) observe(ctx context.Context, v *agentsv1alpha1.Vault) (ctrl.Result, error) {
	remote, err := r.OpenAI.GetVault(ctx, v.Status.OpenAIVaultID)
	if err != nil {
		return r.problem(ctx, v, "ObservationFailed", "Could not observe Vault; no replacement will be created", err)
	}
	if !ownedVault(v, remote) {
		return r.problem(ctx, v, "OwnershipMismatch", "Vault ownership could not be verified", nil)
	}
	expectedMetadata, err := openaiapi.VaultMetadata(v.Spec.Metadata, remote.Metadata, v.Status.ManagedMetadataKeys, string(v.UID), r.OwnerScope)
	if err != nil {
		return r.problem(ctx, v, invalidConfigurationReason, err.Error(), nil)
	}
	nameChanged := v.Spec.Name != nil && remote.Name != strings.TrimSpace(*v.Spec.Name) || v.Spec.ClearName && remote.Name != ""
	metadataChanged := !reflect.DeepEqual(expectedMetadata, remote.Metadata)
	if nameChanged || metadataChanged {
		var name *string
		clearName := false
		if nameChanged {
			name, clearName = v.Spec.Name, v.Spec.ClearName
		}
		var metadata *map[string]string
		if metadataChanged {
			metadata = &expectedMetadata
		}
		// Persist key ownership before the write so a lost response and later spec
		// change cannot turn our committed keys into unmanaged metadata.
		base := v.DeepCopy()
		v.Status.ManagedMetadataKeys = pendingMetadataKeys(v.Spec.Metadata, remote.Metadata, v.Status.ManagedMetadataKeys)
		vaultCondition(v, metav1.ConditionUnknown, "UpdatePending", "Vault update is pending public readback")
		if err := r.status(ctx, v, base); err != nil {
			return ctrl.Result{}, err
		}
		updated, err := r.OpenAI.UpdateVault(ctx, remote.ID, name, clearName, metadata)
		if err != nil {
			return r.problem(ctx, v, "UpdateOutcomeUnknown", "Observe the owned Vault before retrying its update", err)
		}
		if !ownedVault(v, updated) {
			return r.problem(ctx, v, "OwnershipMismatch", "Vault update returned an unexpected owner", nil)
		}
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}
	base := v.DeepCopy()
	if v.Spec.Metadata != nil {
		keys := make([]string, 0, len(*v.Spec.Metadata))
		for key := range *v.Spec.Metadata {
			keys = append(keys, key)
		}
		slices.Sort(keys)
		v.Status.ManagedMetadataKeys = keys
	}
	v.Status.ObservedGeneration = v.Generation
	vaultCondition(v, metav1.ConditionTrue, "VaultObserved", "Owned Vault public configuration has been observed")
	return ctrl.Result{RequeueAfter: observationInterval}, r.status(ctx, v, base)
}

func (r *VaultReconciler) reader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}
func ownedVault(v *agentsv1alpha1.Vault, remote openaiapi.Vault) bool {
	return remote.ID != "" && remote.OwnerUID == string(v.UID) && remote.OwnerScope == v.Status.OwnerScope && (v.Status.OpenAIVaultID == "" || v.Status.OpenAIVaultID == remote.ID)
}
func (r *VaultReconciler) discover(ctx context.Context, v *agentsv1alpha1.Vault) (ctrl.Result, error) {
	remote, err := r.OpenAI.FindVault(ctx, string(v.UID), v.Status.OwnerScope)
	if err != nil || remote.ID == "" {
		return r.problem(ctx, v, creationOutcomeUnknownReason, "No unique owned Vault discovered; finalizer retained for audit", err)
	}
	return r.record(ctx, v, remote)
}
func (r *VaultReconciler) record(ctx context.Context, v *agentsv1alpha1.Vault, remote openaiapi.Vault) (ctrl.Result, error) {
	if !ownedVault(v, remote) {
		return r.problem(ctx, v, "OwnershipMismatch", "Remote Vault identity could not be verified", nil)
	}
	base := v.DeepCopy()
	v.Status.OpenAIVaultID = remote.ID
	vaultCondition(v, metav1.ConditionFalse, "ObservationPending", "Remote identity saved; awaiting observation")
	return ctrl.Result{RequeueAfter: time.Second}, r.status(ctx, v, base)
}
func (r *VaultReconciler) finalize(ctx context.Context, v *agentsv1alpha1.Vault) (ctrl.Result, error) {
	if v.Spec.DeletionPolicy == retainPolicy {
		return r.release(ctx, v)
	}
	used, err := vaultHasSessions(ctx, r.reader(), v.Namespace, v.Name, string(v.UID))
	if err != nil {
		return r.problem(ctx, v, "ReferenceCheckFailed", "Could not check Session references", err)
	}
	if used {
		return r.problem(ctx, v, "VaultInUse", "Remove referencing Sessions before deleting Vault", nil)
	}
	var credentials agentsv1alpha1.VaultCredentialList
	if err := r.reader().List(ctx, &credentials, client.InNamespace(v.Namespace), client.Limit(1000)); err != nil {
		return r.problem(ctx, v, "ReferenceCheckFailed", "Could not check credential references", err)
	}
	if credentials.Continue != "" {
		return r.problem(ctx, v, "ReferenceCheckFailed", "Credential scan exceeded limit; cleanup blocked", nil)
	}
	for _, credential := range credentials.Items {
		if credential.Spec.VaultRef.Name == v.Name || credential.Status.VaultUID == string(v.UID) {
			return r.problem(ctx, v, "CredentialsInUse", "Remove credential resources before deleting Vault", nil)
		}
	}
	if v.Status.OpenAIVaultID == "" {
		if v.Status.CreationAttempted {
			return r.discover(ctx, v)
		}
		return r.release(ctx, v)
	}
	remote, err := r.OpenAI.GetVault(ctx, v.Status.OpenAIVaultID)
	if errors.Is(err, openaiapi.ErrNotFound) {
		return r.release(ctx, v)
	}
	if err != nil {
		return r.problem(ctx, v, "CleanupFailed", "Could not observe Vault for cleanup", err)
	}
	if !ownedVault(v, remote) {
		return r.problem(ctx, v, "OwnershipMismatch", "Vault cleanup ownership mismatch", nil)
	}
	if err := r.OpenAI.DeleteVault(ctx, remote.ID); err != nil {
		return r.problem(ctx, v, "CleanupFailed", "Could not confirm Vault deletion", err)
	}
	return r.problem(ctx, v, "CleanupPending", "Waiting to observe remote Vault absence", nil)
}
func (r *VaultReconciler) release(ctx context.Context, v *agentsv1alpha1.Vault) (ctrl.Result, error) {
	base := v.DeepCopy()
	controllerutil.RemoveFinalizer(v, vaultFinalizer)
	return ctrl.Result{}, r.Patch(ctx, v, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{}))
}
func (r *VaultReconciler) problem(ctx context.Context, v *agentsv1alpha1.Vault, reason, message string, cause error) (ctrl.Result, error) {
	base := v.DeepCopy()
	value := metav1.ConditionFalse
	if reason == creationOutcomeUnknownReason {
		value = metav1.ConditionUnknown
	}
	vaultCondition(v, value, reason, message)
	return ctrl.Result{RequeueAfter: observationInterval}, errors.Join(cause, r.status(ctx, v, base))
}
func vaultCondition(v *agentsv1alpha1.Vault, value metav1.ConditionStatus, reason, message string) {
	meta.SetStatusCondition(&v.Status.Conditions, metav1.Condition{Type: readyCondition, Status: value, Reason: reason, Message: message, ObservedGeneration: v.Generation})
}
func (r *VaultReconciler) status(ctx context.Context, v, base *agentsv1alpha1.Vault) error {
	if reflect.DeepEqual(v.Status, base.Status) {
		return nil
	}
	return r.Status().Patch(ctx, v, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{}))
}
func (r *VaultReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).For(&agentsv1alpha1.Vault{}).
		Watches(&agentsv1alpha1.Session{}, handler.EnqueueRequestsFromMapFunc(sessionVaultRequests)).
		Watches(&agentsv1alpha1.VaultCredential{}, handler.EnqueueRequestsFromMapFunc(func(_ context.Context, obj client.Object) []reconcile.Request {
			credential, ok := obj.(*agentsv1alpha1.VaultCredential)
			if !ok {
				return nil
			}
			return []reconcile.Request{{NamespacedName: client.ObjectKey{Namespace: obj.GetNamespace(), Name: credential.Spec.VaultRef.Name}}}
		})).Named("vault").Complete(r)
}

func vaultHasSessions(ctx context.Context, reader client.Reader, namespace, name, uid string) (bool, error) {
	token := ""
	for range 10 {
		var sessions agentsv1alpha1.SessionList
		if err := reader.List(ctx, &sessions, client.InNamespace(namespace), client.Limit(100), client.Continue(token)); err != nil {
			return false, err
		}
		for _, s := range sessions.Items {
			for _, ref := range s.Spec.VaultRefs {
				if ref.Name == name {
					return true, nil
				}
			}
			for _, snapshot := range s.Status.VaultSnapshots {
				if snapshot.UID == uid || snapshot.Name == name {
					return true, nil
				}
			}
		}
		if sessions.Continue == "" {
			return false, nil
		}
		token = sessions.Continue
	}
	return false, errors.New("vault Session reference scan limit reached; cleanup blocked")
}
func sessionVaultRequests(_ context.Context, obj client.Object) []reconcile.Request {
	s, ok := obj.(*agentsv1alpha1.Session)
	if !ok {
		return nil
	}
	names := make(map[string]bool)
	for _, ref := range s.Spec.VaultRefs {
		names[ref.Name] = true
	}
	for _, snapshot := range s.Status.VaultSnapshots {
		names[snapshot.Name] = true
	}
	result := make([]reconcile.Request, 0, len(names))
	for name := range names {
		result = append(result, reconcile.Request{NamespacedName: client.ObjectKey{Namespace: s.Namespace, Name: name}})
	}
	return result
}
