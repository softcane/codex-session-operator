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
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sfields "k8s.io/apimachinery/pkg/fields"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

const credentialFinalizer = "agents.softcane.io/remote-vault-credential"

// CredentialProvider supports write-only credentials without exposing SDK errors.
type CredentialProvider interface {
	GetVault(context.Context, string) (openaiapi.Vault, error)
	ListCredentials(context.Context, string) ([]openaiapi.Credential, error)
	CreateCredential(context.Context, string, string, map[string]string, map[string]any) (openaiapi.Credential, error)
	GetCredential(context.Context, string, string) (openaiapi.Credential, error)
	FindCredential(context.Context, string, string, string) (openaiapi.Credential, error)
	UpdateCredential(context.Context, string, string, map[string]any) (openaiapi.Credential, error)
	DeleteCredential(context.Context, string, string) error
}

var (
	_ CredentialProvider   = (*openaiapi.Client)(nil)
	_ reconcile.Reconciler = (*VaultCredentialReconciler)(nil)
)

// VaultCredentialReconciler acknowledges writes using protected source versions only.
type VaultCredentialReconciler struct {
	client.Client

	APIReader  client.Reader
	OpenAI     CredentialProvider
	OwnerScope string
}

// +kubebuilder:rbac:groups=agents.softcane.io,resources=vaultcredentials,verbs=get;list;watch;patch
// +kubebuilder:rbac:groups=agents.softcane.io,resources=vaultcredentials/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=agents.softcane.io,resources=vaultcredentials/finalizers,verbs=update;patch
// +kubebuilder:rbac:groups="",resources=secrets,resourceNames=codex-vault,verbs=get;list;watch

func (r *VaultCredentialReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var c agentsv1alpha1.VaultCredential
	if err := r.reader().Get(ctx, req.NamespacedName, &c); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !c.DeletionTimestamp.IsZero() && !controllerutil.ContainsFinalizer(&c, credentialFinalizer) {
		return ctrl.Result{}, nil
	}
	if r.OpenAI == nil {
		return ctrl.Result{}, errors.New("credential provider is not configured")
	}
	if c.Status.OwnerScope != "" && c.Status.OwnerScope != r.OwnerScope {
		return r.problem(ctx, &c, "OwnershipScopeChanged", "Restore installation/project scope", nil)
	}
	if !c.DeletionTimestamp.IsZero() {
		return r.finalize(ctx, &c)
	}
	if r.OwnerScope == "" || len(r.OwnerScope) > 512 {
		return r.problem(ctx, &c, "OwnerScopeRequired", "Configure installation/project scope", nil)
	}
	if err := openaiapi.ValidateVaultName(c.Spec.Name); err != nil {
		return r.problem(ctx, &c, invalidConfigurationReason, err.Error(), nil)
	}
	v, message, err := r.resolveVault(ctx, &c)
	if message != "" {
		return r.problem(ctx, &c, "VaultNotReady", message, err)
	}

	secret, versions, err := readCredentialSource(ctx, r.reader(), c.Namespace)
	if err != nil {
		return r.problem(ctx, &c, "ProtectedSourceUnavailable", "Could not resolve protected credential source", client.IgnoreNotFound(err))
	}
	auth, err := openaiapi.CredentialAuth(c.Spec.Auth, secret, false)
	if err != nil {
		return r.problem(ctx, &c, invalidConfigurationReason, err.Error(), nil)
	}
	if !controllerutil.ContainsFinalizer(&c, credentialFinalizer) {
		base := c.DeepCopy()
		controllerutil.AddFinalizer(&c, credentialFinalizer)
		return ctrl.Result{RequeueAfter: time.Second}, r.Patch(ctx, &c, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{}))
	}
	if c.Status.OpenAICredentialID != "" {
		return r.sync(ctx, &c, secret, versions)
	}
	if c.Status.CreationAttempted {
		return r.discover(ctx, &c)
	}
	metadata, err := openaiapi.VaultMetadata(c.Spec.Metadata, nil, nil, string(c.UID), r.OwnerScope)
	if err != nil {
		return r.problem(ctx, &c, invalidConfigurationReason, err.Error(), nil)
	}
	base := c.DeepCopy()
	c.Status.CreationAttempted, c.Status.OwnerScope = true, r.OwnerScope
	c.Status.VaultUID, c.Status.OpenAIVaultID = string(v.UID), v.Status.OpenAIVaultID
	c.Status.ManagedMetadataKeys = metadataKeys(c.Spec.Metadata)
	credentialCondition(&c, metav1.ConditionUnknown, "CreatingCredential", "Remote creation has been claimed")
	if err := r.status(ctx, &c, base); err != nil {
		return ctrl.Result{}, err
	}
	remote, err := r.OpenAI.CreateCredential(ctx, c.Status.OpenAIVaultID, c.Spec.Name, metadata, auth)
	if err != nil {
		return r.problem(ctx, &c, creationOutcomeUnknownReason, "Discover unique owned credential before further action", err)
	}
	return r.record(ctx, &c, remote, versions)
}
func (r *VaultCredentialReconciler) resolveVault(ctx context.Context, c *agentsv1alpha1.VaultCredential) (*agentsv1alpha1.Vault, string, error) {
	var v agentsv1alpha1.Vault
	if err := r.reader().Get(ctx, client.ObjectKey{Namespace: c.Namespace, Name: c.Spec.VaultRef.Name}, &v); err != nil {
		return nil, "Could not read referenced Vault", client.IgnoreNotFound(err)
	}
	ready := meta.FindStatusCondition(v.Status.Conditions, readyCondition)
	if !v.DeletionTimestamp.IsZero() || ready == nil || ready.Status != metav1.ConditionTrue || ready.ObservedGeneration != v.Generation || v.Status.OwnerScope != r.OwnerScope || v.Status.OpenAIVaultID == "" {
		return nil, "Vault must be Ready, current, owned and not deleting", nil
	}
	if c.Status.CreationAttempted && (c.Status.VaultUID != string(v.UID) || c.Status.OpenAIVaultID != v.Status.OpenAIVaultID) {
		return nil, "Referenced Vault was replaced; create a new credential", nil
	}
	remoteVault, err := r.OpenAI.GetVault(ctx, v.Status.OpenAIVaultID)
	if err != nil || !ownedVault(&v, remoteVault) {
		return nil, "Could not verify remote Vault ownership", err
	}
	return &v, "", nil
}

func (r *VaultCredentialReconciler) reader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}
func readCredentialSource(ctx context.Context, reader client.Reader, namespace string) (*corev1.Secret, []agentsv1alpha1.CredentialSourceVersion, error) {
	var secret corev1.Secret
	if err := reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: openaiapi.VaultSecretName}, &secret); err != nil {
		return nil, nil, err
	}
	if !secret.DeletionTimestamp.IsZero() || secret.UID == "" || secret.ResourceVersion == "" {
		return nil, nil, errors.New("protected credential source is unavailable")
	}
	return &secret, []agentsv1alpha1.CredentialSourceVersion{{Name: secret.Name, UID: string(secret.UID), ResourceVersion: secret.ResourceVersion}}, nil
}
func ownedCredential(c *agentsv1alpha1.VaultCredential, remote openaiapi.Credential) bool {
	return remote.ID != "" && remote.OwnerUID == string(c.UID) && remote.OwnerScope == c.Status.OwnerScope && remote.VaultID == c.Status.OpenAIVaultID && (c.Status.OpenAICredentialID == "" || c.Status.OpenAICredentialID == remote.ID)
}
func (r *VaultCredentialReconciler) discover(ctx context.Context, c *agentsv1alpha1.VaultCredential) (ctrl.Result, error) {
	remote, err := r.OpenAI.FindCredential(ctx, c.Status.OpenAIVaultID, string(c.UID), c.Status.OwnerScope)
	if err != nil || remote.ID == "" {
		return r.problem(ctx, c, creationOutcomeUnknownReason, "No unique owned credential discovered; audit required", err)
	}
	// A recovered create response cannot acknowledge which protected source was committed.
	return r.record(ctx, c, remote, nil)
}
func (r *VaultCredentialReconciler) record(ctx context.Context, c *agentsv1alpha1.VaultCredential, remote openaiapi.Credential, versions []agentsv1alpha1.CredentialSourceVersion) (ctrl.Result, error) {
	if !ownedCredential(c, remote) {
		return r.problem(ctx, c, "OwnershipMismatch", "Remote credential identity could not be verified", nil)
	}
	base := c.DeepCopy()
	c.Status.OpenAICredentialID = remote.ID
	if versions != nil && openaiapi.CredentialPublicMatches(c.Spec.Auth, remote) {
		c.Status.SourceVersions = slices.Clone(versions)
		c.Status.ConfigurationDigest = openaiapi.CredentialDigest(c.Spec, versions)
	}
	credentialCondition(c, metav1.ConditionFalse, "ObservationPending", "Remote identity saved; awaiting public readback")
	return ctrl.Result{RequeueAfter: time.Second}, r.status(ctx, c, base)
}
func (r *VaultCredentialReconciler) sync(ctx context.Context, c *agentsv1alpha1.VaultCredential, secret *corev1.Secret, versions []agentsv1alpha1.CredentialSourceVersion) (ctrl.Result, error) {
	remote, err := r.OpenAI.GetCredential(ctx, c.Status.OpenAIVaultID, c.Status.OpenAICredentialID)
	if err != nil {
		return r.problem(ctx, c, "ObservationFailed", "Could not observe credential; no replacement will be created", err)
	}
	if !ownedCredential(c, remote) || remote.Name != strings.TrimSpace(c.Spec.Name) || !openaiapi.CredentialPublicMatches(c.Spec.Auth, remote) {
		return r.problem(ctx, c, "OwnershipMismatch", "Credential public identity differs from the claimed resource", nil)
	}
	metadata, err := openaiapi.VaultMetadata(c.Spec.Metadata, remote.Metadata, c.Status.ManagedMetadataKeys, string(c.UID), c.Status.OwnerScope)
	if err != nil {
		return r.problem(ctx, c, invalidConfigurationReason, err.Error(), nil)
	}
	digest := openaiapi.CredentialDigest(c.Spec, versions)
	fields := make(map[string]any)
	if !reflect.DeepEqual(metadata, remote.Metadata) {
		fields[agentMetadataField] = metadata
	}
	if c.Status.ConfigurationDigest != digest {
		auth, err := openaiapi.CredentialAuth(c.Spec.Auth, secret, true)
		if err != nil {
			return r.problem(ctx, c, invalidConfigurationReason, err.Error(), nil)
		}
		fields["auth"] = auth
	}
	if len(fields) > 0 {
		base := c.DeepCopy()
		c.Status.ManagedMetadataKeys = pendingMetadataKeys(c.Spec.Metadata, remote.Metadata, c.Status.ManagedMetadataKeys)
		credentialCondition(c, metav1.ConditionFalse, "RotatingCredential", "Credential write is pending acknowledgment")
		if err := r.status(ctx, c, base); err != nil {
			return ctrl.Result{}, err
		}
		updated, err := r.OpenAI.UpdateCredential(ctx, c.Status.OpenAIVaultID, remote.ID, fields)
		if err != nil {
			return r.problem(ctx, c, "RotationOutcomeUnknown", "Observe public identity before resupplying protected values", err)
		}
		if !ownedCredential(c, updated) || !openaiapi.CredentialPublicMatches(c.Spec.Auth, updated) {
			return r.problem(ctx, c, "OwnershipMismatch", "Updated credential identity mismatch", nil)
		}
		base = c.DeepCopy()
		c.Status.SourceVersions = slices.Clone(versions)
		c.Status.ConfigurationDigest = digest
		credentialCondition(c, metav1.ConditionFalse, "ObservationPending", "Protected write acknowledged; public readback is pending")
		return ctrl.Result{RequeueAfter: time.Second}, r.status(ctx, c, base)
	}
	base := c.DeepCopy()
	c.Status.ObservedGeneration = c.Generation
	c.Status.ManagedMetadataKeys = appliedMetadataKeys(c.Spec.Metadata, c.Status.ManagedMetadataKeys)
	credentialCondition(c, metav1.ConditionTrue, "CredentialWriteAcknowledged", "Public identity observed and protected source write acknowledged; GET cannot verify secret content")
	return ctrl.Result{RequeueAfter: observationInterval}, r.status(ctx, c, base)
}
func (r *VaultCredentialReconciler) finalize(ctx context.Context, c *agentsv1alpha1.VaultCredential) (ctrl.Result, error) {
	if c.Spec.DeletionPolicy == retainPolicy {
		return r.release(ctx, c)
	}
	used, err := vaultHasSessions(ctx, r.reader(), c.Namespace, c.Spec.VaultRef.Name, c.Status.VaultUID)
	if err != nil {
		return r.problem(ctx, c, "ReferenceCheckFailed", "Could not check referencing Sessions", err)
	}
	if used {
		return r.problem(ctx, c, "CredentialInUse", "Remove Sessions referencing this Vault before deleting its credential", nil)
	}
	if c.Status.OpenAICredentialID == "" {
		if c.Status.CreationAttempted {
			return r.discover(ctx, c)
		}
		return r.release(ctx, c)
	}
	remote, err := r.OpenAI.GetCredential(ctx, c.Status.OpenAIVaultID, c.Status.OpenAICredentialID)
	if errors.Is(err, openaiapi.ErrNotFound) {
		return r.release(ctx, c)
	}
	if err != nil {
		return r.problem(ctx, c, "CleanupFailed", "Could not observe credential for cleanup", err)
	}
	if !ownedCredential(c, remote) {
		return r.problem(ctx, c, "OwnershipMismatch", "Credential cleanup ownership mismatch", nil)
	}
	if err := r.OpenAI.DeleteCredential(ctx, c.Status.OpenAIVaultID, remote.ID); err != nil {
		return r.problem(ctx, c, "CleanupFailed", "Could not confirm credential deletion", err)
	}
	return r.problem(ctx, c, "CleanupPending", "Waiting to observe remote credential absence", nil)
}
func (r *VaultCredentialReconciler) release(ctx context.Context, c *agentsv1alpha1.VaultCredential) (ctrl.Result, error) {
	base := c.DeepCopy()
	controllerutil.RemoveFinalizer(c, credentialFinalizer)
	return ctrl.Result{}, r.Patch(ctx, c, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{}))
}
func (r *VaultCredentialReconciler) problem(ctx context.Context, c *agentsv1alpha1.VaultCredential, reason, message string, cause error) (ctrl.Result, error) {
	base := c.DeepCopy()
	value := metav1.ConditionFalse
	if reason == creationOutcomeUnknownReason {
		value = metav1.ConditionUnknown
	}
	credentialCondition(c, value, reason, message)
	return ctrl.Result{RequeueAfter: observationInterval}, errors.Join(cause, r.status(ctx, c, base))
}
func credentialCondition(c *agentsv1alpha1.VaultCredential, value metav1.ConditionStatus, reason, message string) {
	meta.SetStatusCondition(&c.Status.Conditions, metav1.Condition{Type: readyCondition, Status: value, Reason: reason, Message: message, ObservedGeneration: c.Generation})
	meta.SetStatusCondition(&c.Status.Conditions, metav1.Condition{Type: "SecretMaterialVerified", Status: metav1.ConditionUnknown, Reason: "WriteOnlyReadback", Message: "GET never returns credential values; only acknowledged source versions are tracked", ObservedGeneration: c.Generation})
}
func (r *VaultCredentialReconciler) status(ctx context.Context, c, base *agentsv1alpha1.VaultCredential) error {
	if reflect.DeepEqual(c.Status, base.Status) {
		return nil
	}
	return r.Status().Patch(ctx, c, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{}))
}
func (r *VaultCredentialReconciler) SetupWithManager(mgr ctrl.Manager) error {
	protectedCache, err := cache.New(mgr.GetConfig(), cache.Options{Scheme: mgr.GetScheme(), ReaderFailOnMissingInformer: true, ByObject: map[client.Object]cache.ByObject{&corev1.Secret{}: {Field: k8sfields.OneTermEqualSelector("metadata.name", openaiapi.VaultSecretName)}}})
	if err != nil {
		return err
	}
	if err := mgr.Add(protectedCache); err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).For(&agentsv1alpha1.VaultCredential{}).
		Watches(&agentsv1alpha1.Vault{}, handler.EnqueueRequestsFromMapFunc(r.vaultRequests)).
		Watches(&agentsv1alpha1.Session{}, handler.EnqueueRequestsFromMapFunc(r.sessionRequests)).
		WatchesRawSource(source.Kind(protectedCache, &corev1.Secret{}, handler.TypedEnqueueRequestsFromMapFunc(r.secretRequests))).Named("vaultcredential").Complete(r)
}
func (r *VaultCredentialReconciler) vaultRequests(ctx context.Context, obj client.Object) []reconcile.Request {
	return r.credentialRequests(ctx, obj.GetNamespace(), obj.GetName())
}
func (r *VaultCredentialReconciler) secretRequests(ctx context.Context, secret *corev1.Secret) []reconcile.Request {
	return r.credentialRequests(ctx, secret.Namespace, "")
}
func (r *VaultCredentialReconciler) sessionRequests(ctx context.Context, obj client.Object) []reconcile.Request {
	requests := sessionVaultRequests(ctx, obj)
	result := make([]reconcile.Request, 0, len(requests))
	for _, request := range requests {
		result = append(result, r.credentialRequests(ctx, request.Namespace, request.Name)...)
	}
	return result
}
func (r *VaultCredentialReconciler) credentialRequests(ctx context.Context, namespace, vaultName string) []reconcile.Request {
	var credentials agentsv1alpha1.VaultCredentialList
	if err := r.List(ctx, &credentials, client.InNamespace(namespace)); err != nil {
		return nil
	}
	result := make([]reconcile.Request, 0, len(credentials.Items))
	for i := range credentials.Items {
		c := &credentials.Items[i]
		if vaultName == "" || c.Spec.VaultRef.Name == vaultName {
			result = append(result, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(c)})
		}
	}
	return result
}
