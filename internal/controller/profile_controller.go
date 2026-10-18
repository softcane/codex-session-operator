package controller

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	"github.com/softcane/codex-session-operator/internal/openaiapi"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	_profileVaultReferenceIndex = "spec.vaultRefs.name"
	_profileReadyReason         = "ProfileReady"
	_profileReadyMessage        = "Referenced resources are current, Ready and owned; readiness does not establish task execution"
)

// ProfileReconciler observes published recipes. It creates no remote objects,
// finalizers, owner references or Sessions; accepted work owns its own lifecycle.
type ProfileReconciler struct {
	client.Client

	APIReader  client.Reader
	OwnerScope string
}

var _ reconcile.Reconciler = (*ProfileReconciler)(nil)

// +kubebuilder:rbac:groups=agents.softcane.io,resources=profiles,verbs=get;list;watch
// +kubebuilder:rbac:groups=agents.softcane.io,resources=profiles/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=agents.softcane.io,resources=agents;environments;vaults,verbs=get;list;watch

func (r *ProfileReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var profile agentsv1alpha1.Profile
	if err := r.reader().Get(ctx, req.NamespacedName, &profile); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !profile.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}
	base := profile.DeepCopy()
	profile.Status.ObservedGeneration = profile.Generation
	profile.Status.PolicyDigest = agentsv1alpha1.ProfilePolicyDigest(profile.Spec)
	profile.Status.Agent, profile.Status.Environment, profile.Status.Vaults = nil, nil, nil
	var reason, message string
	var cause error
	if err := validateProfilePolicy(profile.Spec); err != nil {
		reason, message = invalidConfigurationReason, err.Error()
	} else if len(r.OwnerScope) > 512 {
		reason, message = invalidConfigurationReason, "Installation/project owner scope exceeds its supported length"
	} else {
		reason, message, cause = r.observeProfileDependencies(ctx, &profile)
	}
	status := metav1.ConditionTrue
	if reason != _profileReadyReason {
		status = metav1.ConditionFalse
	}
	meta.SetStatusCondition(&profile.Status.Conditions, metav1.Condition{Type: readyCondition, Status: status,
		Reason: reason, Message: message, ObservedGeneration: profile.Generation})
	var patchErr error
	if !reflect.DeepEqual(profile.Status, base.Status) {
		patchErr = r.Status().Patch(ctx, &profile, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{}))
	}
	return ctrl.Result{RequeueAfter: observationInterval}, errors.Join(cause, patchErr)
}

func (r *ProfileReconciler) reader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

func validateProfilePolicy(spec agentsv1alpha1.ProfileSpec) error {
	if len(validation.IsDNS1123Subdomain(spec.AgentRef.Name)) != 0 ||
		len(validation.IsDNS1123Subdomain(spec.EnvironmentRef.Name)) != 0 {
		return errors.New("profile requires valid same-namespace Agent and Environment references")
	}
	if _, present, err := openaiapi.SessionSpendControlField(&spec.SpendControl); err != nil || !present {
		return errors.New("profile requires an explicit valid spending policy")
	}
	if len(spec.VaultRefs) > 16 {
		return errors.New("profile supports at most 16 Vault references")
	}
	seen := make(map[string]bool, len(spec.VaultRefs))
	for _, ref := range spec.VaultRefs {
		if len(validation.IsDNS1123Subdomain(ref.Name)) != 0 || seen[ref.Name] {
			return errors.New("profile Vault references must be valid and distinct")
		}
		seen[ref.Name] = true
	}
	return nil
}

func (r *ProfileReconciler) observeProfileDependencies(ctx context.Context, profile *agentsv1alpha1.Profile) (string, string, error) {
	reason, message := _profileReadyReason, _profileReadyMessage
	var causes []error
	observe := func(kind, name string, object client.Object) *agentsv1alpha1.ProfileDependencyStatus {
		dependency, problem, err := r.observeProfileDependency(ctx, profile.Namespace, kind, name, object)
		if problem != "" && reason == _profileReadyReason {
			reason, message = problem, kind+" dependency must be current, Ready, owned and not deleting"
		}
		if err != nil {
			causes = append(causes, err)
		}
		return dependency
	}
	profile.Status.Agent = observe("Agent", profile.Spec.AgentRef.Name, &agentsv1alpha1.Agent{})
	profile.Status.Environment = observe("Environment", profile.Spec.EnvironmentRef.Name, &agentsv1alpha1.Environment{})
	vaults := slices.Clone(profile.Spec.VaultRefs)
	slices.SortFunc(vaults, func(a, b agentsv1alpha1.AgentReference) int { return cmp.Compare(a.Name, b.Name) })
	for _, ref := range vaults {
		if dependency := observe("Vault", ref.Name, &agentsv1alpha1.Vault{}); dependency != nil {
			profile.Status.Vaults = append(profile.Status.Vaults, *dependency)
		}
	}
	return reason, message, errors.Join(causes...)
}

func (r *ProfileReconciler) observeProfileDependency(ctx context.Context, namespace, kind, name string, object client.Object) (*agentsv1alpha1.ProfileDependencyStatus, string, error) {
	if err := r.reader().Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, object); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, kind + "NotReady", nil
		}
		return nil, "DependencyObservationFailed", fmt.Errorf("read Profile %s dependency: %w", kind, err)
	}
	dependency := &agentsv1alpha1.ProfileDependencyStatus{Name: object.GetName(), UID: string(object.GetUID()), Generation: object.GetGeneration()}
	var conditions []metav1.Condition
	var scope string
	var configured bool
	switch value := object.(type) {
	case *agentsv1alpha1.Agent:
		conditions, scope, configured = value.Status.Conditions, value.Status.OwnerScope, value.Status.OpenAIAgentID != ""
	case *agentsv1alpha1.Environment:
		conditions, scope, configured = value.Status.Conditions, value.Status.OwnerScope, value.Status.ConfigurationDigest != ""
	case *agentsv1alpha1.Vault:
		conditions, scope, configured = value.Status.Conditions, value.Status.OwnerScope, value.Status.OpenAIVaultID != ""
	}
	if r.OwnerScope != "" && scope != "" && scope != r.OwnerScope {
		return dependency, "OwnershipMismatch", nil
	}
	ready := meta.FindStatusCondition(conditions, readyCondition)
	if object.GetUID() == "" || !object.GetDeletionTimestamp().IsZero() || !configured || ready == nil ||
		(r.OwnerScope != "" && scope != r.OwnerScope) ||
		ready.Status != metav1.ConditionTrue || ready.ObservedGeneration != object.GetGeneration() {
		return dependency, kind + "NotReady", nil
	}
	return dependency, "", nil
}

// SetupWithManager deliberately observes status-only changes as well as spec
// changes. Dependency replacement is visible even when its name is unchanged.
func (r *ProfileReconciler) SetupWithManager(mgr ctrl.Manager) error {
	for _, field := range []string{agentReferenceIndex, environmentReferenceIndex, _profileVaultReferenceIndex} {
		if err := mgr.GetFieldIndexer().IndexField(context.Background(), &agentsv1alpha1.Profile{}, field, func(object client.Object) []string {
			return profileReferences(object, field)
		}); err != nil {
			return err
		}
	}
	return ctrl.NewControllerManagedBy(mgr).For(&agentsv1alpha1.Profile{}).
		Watches(&agentsv1alpha1.Agent{}, handler.EnqueueRequestsFromMapFunc(r.dependencyProfileRequests)).
		Watches(&agentsv1alpha1.Environment{}, handler.EnqueueRequestsFromMapFunc(r.dependencyProfileRequests)).
		Watches(&agentsv1alpha1.Vault{}, handler.EnqueueRequestsFromMapFunc(r.dependencyProfileRequests)).
		Named("profile").Complete(r)
}

func profileReferences(object client.Object, field string) []string {
	profile, ok := object.(*agentsv1alpha1.Profile)
	if !ok {
		return nil
	}
	switch field {
	case agentReferenceIndex:
		return []string{profile.Spec.AgentRef.Name}
	case environmentReferenceIndex:
		return []string{profile.Spec.EnvironmentRef.Name}
	case _profileVaultReferenceIndex:
		names := make([]string, 0, len(profile.Spec.VaultRefs))
		for _, ref := range profile.Spec.VaultRefs {
			names = append(names, ref.Name)
		}
		return names
	default:
		return nil
	}
}

func (r *ProfileReconciler) dependencyProfileRequests(ctx context.Context, object client.Object) []reconcile.Request {
	var field string
	switch object.(type) {
	case *agentsv1alpha1.Agent:
		field = agentReferenceIndex
	case *agentsv1alpha1.Environment:
		field = environmentReferenceIndex
	case *agentsv1alpha1.Vault:
		field = _profileVaultReferenceIndex
	default:
		return nil
	}
	var profiles agentsv1alpha1.ProfileList
	if err := r.List(ctx, &profiles, client.InNamespace(object.GetNamespace()), client.MatchingFields{field: object.GetName()}); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, 0, len(profiles.Items))
	for i := range profiles.Items {
		requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&profiles.Items[i])})
	}
	return requests
}
