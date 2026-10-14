package controller

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"time"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	"github.com/softcane/codex-session-operator/internal/openaiapi"
	"github.com/softcane/codex-session-operator/internal/sandbox"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

const (
	creationOutcomeUnknownReason = "CreationOutcomeUnknown"
	invalidConfigurationReason   = "InvalidConfiguration"
	openaiHostedMode             = "openaiHosted"
	environmentNotReadyReason    = "EnvironmentNotReady"
	remoteFailedStatus           = "failed"
	defaultWorkspace             = "/workspace"
)

const environmentFinalizer = "agents.softcane.io/environment"
const environmentReferenceIndex = "spec.environmentRef.name"

// TemplateProvider owns hosted configuration, never local compute.
type TemplateProvider interface {
	CreateTemplate(context.Context, map[string]any) (openaiapi.Template, error)
	GetTemplate(context.Context, string) (openaiapi.Template, error)
	UpdateTemplate(context.Context, string, map[string]any) (openaiapi.Template, error)
	FindTemplateCandidates(context.Context, string) ([]string, error)
	DeleteTemplate(context.Context, string) error
}

var (
	_ TemplateProvider     = (*openaiapi.Client)(nil)
	_ reconcile.Reconciler = (*EnvironmentReconciler)(nil)
)

// EnvironmentReconciler reconciles reusable configuration and protected references.
type EnvironmentReconciler struct {
	client.Client

	APIReader                        client.Reader
	OpenAI                           TemplateProvider
	OwnerScope                       string
	GatewayEnabled                   bool
	ExecutorImages, CapabilityImages []string
	WorkspaceStorageClasses          []string
}

// +kubebuilder:rbac:groups=agents.softcane.io,resources=environments,verbs=get;list;watch;patch
// +kubebuilder:rbac:groups=agents.softcane.io,resources=environments/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=agents.softcane.io,resources=environments/finalizers,verbs=update;patch
// +kubebuilder:rbac:groups="",resources=secrets,resourceNames=codex-environment,verbs=get;list;watch

func (r *EnvironmentReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var environment agentsv1alpha1.Environment
	if err := r.reader().Get(ctx, req.NamespacedName, &environment); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if environment.UID == "" {
		return ctrl.Result{}, errors.New("kubernetes Environment UID is missing")
	}
	if !environment.DeletionTimestamp.IsZero() {
		if !controllerutil.ContainsFinalizer(&environment, environmentFinalizer) {
			return ctrl.Result{}, nil
		}
		return r.finalizeEnvironment(ctx, &environment)
	}
	if environment.Spec.Mode == _noneMode && !r.GatewayEnabled {
		return r.environmentProblem(ctx, &environment, "GatewayRequired", "None environments require initial gateway input", nil)
	}
	if environment.Status.OwnerScope != "" && environment.Status.OwnerScope != r.OwnerScope {
		return r.environmentProblem(ctx, &environment, "OwnershipScopeChanged", "Restore installation/project scope before reconciling", nil)
	}
	if r.OwnerScope == "" || len(r.OwnerScope) > 512 {
		return r.environmentProblem(ctx, &environment, "OwnerScopeRequired", "Configure installation/project scope", nil)
	}
	if !controllerutil.ContainsFinalizer(&environment, environmentFinalizer) {
		base := environment.DeepCopy()
		controllerutil.AddFinalizer(&environment, environmentFinalizer)
		return ctrl.Result{RequeueAfter: time.Second}, r.Patch(ctx, &environment, environmentPatch(base))
	}
	if environment.Spec.Mode == _noneMode {
		base := environment.DeepCopy()
		environment.Status.OwnerScope = r.OwnerScope
		environment.Status.ConfigurationDigest = openaiapi.EnvironmentDigest(environment.Spec, nil)
		environmentCondition(&environment, metav1.ConditionTrue, "GatewayProfileValidated", "None mode waits for initial input in the durable gateway store")
		return ctrl.Result{RequeueAfter: observationInterval}, r.patchEnvironmentStatus(ctx, &environment, base)
	}
	if environment.Spec.Mode == "selfHosted" {
		if err := sandbox.ValidateProfile(environment.Spec.SelfHosted, r.ExecutorImages, r.CapabilityImages, r.WorkspaceStorageClasses); err != nil {
			return r.environmentProblem(ctx, &environment, invalidConfigurationReason, err.Error(), nil)
		}
		base := environment.DeepCopy()
		environment.Status.OwnerScope = r.OwnerScope
		environment.Status.ConfigurationDigest = openaiapi.EnvironmentDigest(environment.Spec, nil)
		environmentCondition(&environment, metav1.ConditionTrue, "ProfileValidated", "Local profile is validated; each Session owns its compute")
		return ctrl.Result{RequeueAfter: observationInterval}, r.patchEnvironmentStatus(ctx, &environment, base)
	}
	return r.reconcileHostedEnvironment(ctx, &environment)
}

func (r *EnvironmentReconciler) reconcileHostedEnvironment(ctx context.Context, environment *agentsv1alpha1.Environment) (ctrl.Result, error) {
	if environment.Spec.Mode != openaiHostedMode || environment.Spec.OpenAIHosted == nil {
		return r.environmentProblem(ctx, environment, invalidConfigurationReason, "Environment union is invalid", nil)
	}
	if r.OpenAI == nil {
		return ctrl.Result{}, errors.New("OpenAI template provider is not configured")
	}
	if environment.Status.OpenAITemplateID == "" && environment.Status.CreationAttempted {
		return r.auditEnvironment(ctx, environment)
	}
	secret, sourceVersion, err := readEnvironmentSource(ctx, r.reader(), environment.Namespace, environment.Spec.OpenAIHosted)
	if err != nil {
		return r.environmentProblem(ctx, environment, "ProtectedSourceUnavailable", "Protected Environment source is unavailable or invalid", err)
	}
	name := openaiapi.ManagedTemplateName(environment.Spec.OpenAIHosted.Name, string(environment.UID), r.OwnerScope)
	configuration, err := openaiapi.HostedConfiguration(environment.Spec.OpenAIHosted, secret, name)
	if err != nil {
		return r.environmentProblem(ctx, environment, invalidConfigurationReason, err.Error(), nil)
	}
	digest := openaiapi.EnvironmentDigest(environment.Spec, sourceVersion)
	templateDigest := openaiapi.TemplateConfigurationDigest(environment.Spec.OpenAIHosted, sourceVersion)
	if environment.Status.OpenAITemplateID == "" {
		base := environment.DeepCopy()
		environment.Status.CreationAttempted = true
		environment.Status.OwnerScope = r.OwnerScope
		environment.Status.CreationName = name
		environmentCondition(environment, metav1.ConditionUnknown, "CreatingTemplate", "One remote template creation has been claimed")
		if err := r.patchEnvironmentStatus(ctx, environment, base); err != nil {
			return ctrl.Result{}, err
		}
		remote, err := r.OpenAI.CreateTemplate(ctx, configuration)
		if err != nil {
			return r.environmentProblem(ctx, environment, creationOutcomeUnknownReason, "Template creation is ambiguous; inspect bounded audit candidates before identity repair", err)
		}
		if remote.ID == "" || !ownedTemplate(environment, remote) {
			return r.environmentProblem(ctx, environment, creationOutcomeUnknownReason, "Template response lacks a verifiable identity; audit required", nil)
		}
		base = environment.DeepCopy()
		environment.Status.OpenAITemplateID = remote.ID
		environment.Status.ConfigurationDigest = digest
		environment.Status.TemplateConfigurationDigest = templateDigest
		environment.Status.SourceVersion = sourceVersion
		environmentCondition(environment, metav1.ConditionFalse, "ObservationPending", "Template write accepted; observation is pending")
		return ctrl.Result{RequeueAfter: time.Second}, r.patchEnvironmentStatus(ctx, environment, base)
	}
	remote, err := r.OpenAI.GetTemplate(ctx, environment.Status.OpenAITemplateID)
	if err != nil {
		return r.environmentProblem(ctx, environment, "ObservationFailed", "Could not observe hosted template; no replacement will be created", err)
	}
	if remote.ID != environment.Status.OpenAITemplateID || !ownedTemplate(environment, remote) {
		return r.environmentProblem(ctx, environment, "OwnershipMismatch", "Template identity marker does not match; mutation is blocked", nil)
	}
	if templateDigest != environment.Status.TemplateConfigurationDigest || !openaiapi.TemplateMatches(configuration, remote) {
		base := environment.DeepCopy()
		environmentCondition(environment, metav1.ConditionFalse, "UpdatingTemplate", "Complete template replacement is pending")
		if err := r.patchEnvironmentStatus(ctx, environment, base); err != nil {
			return ctrl.Result{}, err
		}
		updated, err := r.OpenAI.UpdateTemplate(ctx, remote.ID, configuration)
		if err != nil {
			return r.environmentProblem(ctx, environment, "UpdateOutcomeUnknown", "Update outcome is unconfirmed; observation precedes retry", err)
		}
		if updated.ID != remote.ID || !ownedTemplate(environment, updated) {
			return r.environmentProblem(ctx, environment, "OwnershipMismatch", "Updated template identity does not match", nil)
		}
		base = environment.DeepCopy()
		environment.Status.ConfigurationDigest = digest
		environment.Status.TemplateConfigurationDigest = templateDigest
		environment.Status.SourceVersion = sourceVersion
		environmentCondition(environment, metav1.ConditionFalse, "ObservationPending", "Template write accepted; observation is pending")
		return ctrl.Result{RequeueAfter: time.Second}, r.patchEnvironmentStatus(ctx, environment, base)
	}
	base := environment.DeepCopy()
	environment.Status.ConfigurationDigest = digest
	environmentCondition(environment, metav1.ConditionTrue, "TemplateApplied", "Observable configuration matches and protected writes were acknowledged")
	meta.SetStatusCondition(&environment.Status.Conditions, metav1.Condition{Type: "ConfidentialContentVerified", Status: metav1.ConditionUnknown, Reason: "ProviderRedactsContent",
		Message: "GET cannot verify environment values, commands, archive contents or inline bytes", ObservedGeneration: environment.Generation})
	return ctrl.Result{RequeueAfter: observationInterval}, r.patchEnvironmentStatus(ctx, environment, base)
}

func (r *EnvironmentReconciler) reader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}
func environmentPatch(base *agentsv1alpha1.Environment) client.Patch {
	return client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})
}
func (r *EnvironmentReconciler) patchEnvironmentStatus(ctx context.Context, environment, base *agentsv1alpha1.Environment) error {
	if reflect.DeepEqual(environment.Status, base.Status) {
		return nil
	}
	return r.Status().Patch(ctx, environment, environmentPatch(base))
}
func environmentCondition(environment *agentsv1alpha1.Environment, status metav1.ConditionStatus, reason, message string) {
	meta.SetStatusCondition(&environment.Status.Conditions, metav1.Condition{Type: readyCondition, Status: status, Reason: reason, Message: message, ObservedGeneration: environment.Generation})
}
func (r *EnvironmentReconciler) environmentProblem(ctx context.Context, environment *agentsv1alpha1.Environment, reason, message string, cause error) (ctrl.Result, error) {
	base := environment.DeepCopy()
	status := metav1.ConditionFalse
	if reason == creationOutcomeUnknownReason {
		status = metav1.ConditionUnknown
	}
	environmentCondition(environment, status, reason, message)
	return ctrl.Result{RequeueAfter: observationInterval}, errors.Join(cause, r.patchEnvironmentStatus(ctx, environment, base))
}
func ownedTemplate(environment *agentsv1alpha1.Environment, remote openaiapi.Template) bool {
	return strings.HasSuffix(remote.Name, openaiapi.TemplateIdentity(string(environment.UID), environment.Status.OwnerScope))
}
func (r *EnvironmentReconciler) auditEnvironment(ctx context.Context, environment *agentsv1alpha1.Environment) (ctrl.Result, error) {
	ids, err := r.OpenAI.FindTemplateCandidates(ctx, environment.Status.CreationName)
	if err != nil {
		return r.environmentProblem(ctx, environment, creationOutcomeUnknownReason, "Template discovery is incomplete; no candidate can be adopted", err)
	}
	base := environment.DeepCopy()
	environment.Status.AuditCandidateIDs = ids
	environmentCondition(environment, metav1.ConditionUnknown, creationOutcomeUnknownReason, "Name matches are audit candidates only; independently verify before repairing status ID")
	return ctrl.Result{RequeueAfter: observationInterval}, r.patchEnvironmentStatus(ctx, environment, base)
}

func readEnvironmentSource(ctx context.Context, reader client.Reader, namespace string, h *agentsv1alpha1.HostedEnvironment) (*corev1.Secret, *agentsv1alpha1.EnvironmentSourceVersion, error) {
	if !openaiapi.HostedNeedsSecret(h) {
		return nil, nil, nil
	}
	var secret corev1.Secret
	if err := reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: openaiapi.EnvironmentSecretName}, &secret); err != nil {
		return nil, nil, err
	}
	if secret.Type != corev1.SecretTypeOpaque {
		return nil, nil, errors.New("protected Environment source must be an Opaque Secret")
	}
	if !secret.DeletionTimestamp.IsZero() {
		return nil, nil, errors.New("protected Environment source is deleting")
	}
	return &secret, &agentsv1alpha1.EnvironmentSourceVersion{UID: string(secret.UID), ResourceVersion: secret.ResourceVersion}, nil
}

func (r *EnvironmentReconciler) finalizeEnvironment(ctx context.Context, environment *agentsv1alpha1.Environment) (ctrl.Result, error) {
	if environment.Spec.DeletionPolicy == retainPolicy {
		return r.releaseEnvironment(ctx, environment)
	}
	inUse, err := r.environmentHasSessions(ctx, environment)
	if err != nil {
		return r.environmentProblem(ctx, environment, "ReferenceCheckFailed", "Could not check Session references", err)
	}
	if inUse {
		return r.environmentProblem(ctx, environment, "EnvironmentInUse", "Remove current and frozen Session references before deleting Environment", nil)
	}
	if environment.Spec.Mode != openaiHostedMode {
		return r.releaseEnvironment(ctx, environment)
	}
	if environment.Status.OwnerScope != r.OwnerScope {
		return r.environmentProblem(ctx, environment, "OwnershipScopeChanged", "Restore owner scope before remote cleanup", nil)
	}
	if r.OpenAI == nil {
		return ctrl.Result{}, errors.New("OpenAI template provider is not configured")
	}
	if environment.Status.OpenAITemplateID == "" {
		if environment.Status.CreationAttempted {
			return r.auditEnvironment(ctx, environment)
		}
		return r.releaseEnvironment(ctx, environment)
	}
	remote, err := r.OpenAI.GetTemplate(ctx, environment.Status.OpenAITemplateID)
	if errors.Is(err, openaiapi.ErrNotFound) {
		return r.releaseEnvironment(ctx, environment)
	}
	if err != nil {
		return r.environmentProblem(ctx, environment, "CleanupFailed", "Could not observe template for cleanup", err)
	}
	if remote.ID != environment.Status.OpenAITemplateID || !ownedTemplate(environment, remote) {
		return r.environmentProblem(ctx, environment, "OwnershipMismatch", "Template cleanup ownership could not be verified", nil)
	}
	if err := r.OpenAI.DeleteTemplate(ctx, remote.ID); err != nil {
		return r.environmentProblem(ctx, environment, "CleanupFailed", "Template deletion is unconfirmed", err)
	}
	return r.environmentProblem(ctx, environment, "CleanupPending", "Waiting for template absence observation", nil)
}
func (r *EnvironmentReconciler) environmentHasSessions(ctx context.Context, environment *agentsv1alpha1.Environment) (bool, error) {
	token := ""
	for range 10 {
		var sessions agentsv1alpha1.SessionList
		if err := r.reader().List(ctx, &sessions, client.InNamespace(environment.Namespace), client.Limit(100), client.Continue(token)); err != nil {
			return false, err
		}
		for _, session := range sessions.Items {
			if (session.Spec.EnvironmentRef != nil && session.Spec.EnvironmentRef.Name == environment.Name) || (session.Status.EnvironmentSnapshot != nil && session.Status.EnvironmentSnapshot.UID == string(environment.UID)) {
				return true, nil
			}
		}
		if sessions.Continue == "" {
			return false, nil
		}
		token = sessions.Continue
	}
	return false, errors.New("environment reference scan limit reached; cleanup blocked")
}
func (r *EnvironmentReconciler) releaseEnvironment(ctx context.Context, environment *agentsv1alpha1.Environment) (ctrl.Result, error) {
	base := environment.DeepCopy()
	controllerutil.RemoveFinalizer(environment, environmentFinalizer)
	return ctrl.Result{}, r.Patch(ctx, environment, environmentPatch(base))
}

const protectedEnvironmentIndex = "spec.openaiHosted.protectedSource"

func (r *EnvironmentReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &agentsv1alpha1.Environment{}, protectedEnvironmentIndex, func(obj client.Object) []string {
		environment, ok := obj.(*agentsv1alpha1.Environment)
		if !ok || !openaiapi.HostedNeedsSecret(environment.Spec.OpenAIHosted) {
			return nil
		}
		return []string{openaiapi.EnvironmentSecretName}
	}); err != nil {
		return err
	}
	// Kubernetes field selectors have no OR for two Secret names. A separate
	// manager-owned cache keeps both watches restricted to their respective names.
	protectedCache, err := cache.New(mgr.GetConfig(), cache.Options{Scheme: mgr.GetScheme(), ReaderFailOnMissingInformer: true,
		ByObject: map[client.Object]cache.ByObject{&corev1.Secret{}: {Field: fields.OneTermEqualSelector("metadata.name", openaiapi.EnvironmentSecretName)}}})
	if err != nil {
		return err
	}
	if err := mgr.Add(protectedCache); err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).For(&agentsv1alpha1.Environment{}).
		Watches(&agentsv1alpha1.Session{}, handler.EnqueueRequestsFromMapFunc(sessionEnvironmentRequests)).
		WatchesRawSource(source.Kind(protectedCache, &corev1.Secret{}, handler.TypedEnqueueRequestsFromMapFunc(r.protectedEnvironmentRequests))).
		Named("environment").Complete(r)
}

func (r *EnvironmentReconciler) protectedEnvironmentRequests(ctx context.Context, secret *corev1.Secret) []reconcile.Request {
	var environments agentsv1alpha1.EnvironmentList
	if err := r.List(ctx, &environments, client.InNamespace(secret.Namespace), client.MatchingFields{protectedEnvironmentIndex: openaiapi.EnvironmentSecretName}); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, 0, len(environments.Items))
	for i := range environments.Items {
		requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&environments.Items[i])})
	}
	return requests
}

func sessionEnvironmentRequests(_ context.Context, obj client.Object) []reconcile.Request {
	session, ok := obj.(*agentsv1alpha1.Session)
	if !ok {
		return nil
	}
	names := make(map[string]bool)
	if session.Spec.EnvironmentRef != nil {
		names[session.Spec.EnvironmentRef.Name] = true
	}
	if session.Status.EnvironmentSnapshot != nil {
		names[session.Status.EnvironmentSnapshot.Name] = true
	}
	requests := make([]reconcile.Request, 0, len(names))
	for name := range names {
		requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKey{Namespace: session.Namespace, Name: name}})
	}
	return requests
}
