package controller

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	_workflowProfileReferenceIndex = "workflow.profileRefs.name"
	_workflowReadyReason           = "WorkflowTemplateReady"
)

// WorkflowTemplateReconciler observes live Profiles without owning or starting work.
type WorkflowTemplateReconciler struct {
	client.Client

	APIReader client.Reader
}

var _ reconcile.Reconciler = (*WorkflowTemplateReconciler)(nil)

// +kubebuilder:rbac:groups=agents.softcane.io,resources=workflowtemplates,verbs=get;list;watch
// +kubebuilder:rbac:groups=agents.softcane.io,resources=workflowtemplates/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=agents.softcane.io,resources=profiles,verbs=get;list;watch

func (r *WorkflowTemplateReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}
	var workflow agentsv1alpha1.WorkflowTemplate
	if err := reader.Get(ctx, req.NamespacedName, &workflow); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !workflow.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}
	base := workflow.DeepCopy()
	workflow.Status.ObservedGeneration = workflow.Generation
	workflow.Status.SpecDigest = agentsv1alpha1.WorkflowTemplateSpecDigest(workflow.Spec)
	workflow.Status.Profiles = nil
	reason, message := _workflowReadyReason, "Referenced Profiles are current and Ready; applying a template starts no task"
	var causes []error
	if err := agentsv1alpha1.ValidateWorkflowTemplateSpec(workflow.Spec); err != nil {
		reason, message = invalidConfigurationReason, err.Error()
	} else {
		for _, name := range workflowProfileReferences(&workflow) {
			var profile agentsv1alpha1.Profile
			if err := reader.Get(ctx, client.ObjectKey{Namespace: workflow.Namespace, Name: name}, &profile); err != nil {
				if !apierrors.IsNotFound(err) {
					causes = append(causes, fmt.Errorf("read WorkflowTemplate Profile dependency: %w", err))
				}
				if reason == _workflowReadyReason {
					reason, message = "ProfileNotReady", "Referenced Profile is missing or could not be observed"
				}
				continue
			}
			workflow.Status.Profiles = append(workflow.Status.Profiles, agentsv1alpha1.ProfileDependencyStatus{
				Name: name, UID: string(profile.UID), Generation: profile.Generation,
			})
			ready := meta.FindStatusCondition(profile.Status.Conditions, readyCondition)
			if profile.UID == "" || !profile.DeletionTimestamp.IsZero() || profile.Status.ObservedGeneration != profile.Generation ||
				profile.Status.PolicyDigest != agentsv1alpha1.ProfilePolicyDigest(profile.Spec) ||
				ready == nil || ready.Status != metav1.ConditionTrue || ready.ObservedGeneration != profile.Generation {
				if reason == _workflowReadyReason {
					reason, message = "ProfileNotReady", "Referenced Profiles must be current, Ready and not deleting"
				}
			}
		}
	}
	status := metav1.ConditionFalse
	if reason == _workflowReadyReason {
		status = metav1.ConditionTrue
	}
	meta.SetStatusCondition(&workflow.Status.Conditions, metav1.Condition{
		Type: readyCondition, Status: status, Reason: reason, Message: message, ObservedGeneration: workflow.Generation,
	})
	if !reflect.DeepEqual(base.Status, workflow.Status) {
		causes = append(causes, r.Status().Patch(ctx, &workflow, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})))
	}
	return ctrl.Result{RequeueAfter: observationInterval}, errors.Join(causes...)
}

func workflowProfileReferences(object client.Object) []string {
	workflow, ok := object.(*agentsv1alpha1.WorkflowTemplate)
	if !ok {
		return nil
	}
	names := make([]string, 0, 1+len(workflow.Spec.Roles))
	names = append(names, workflow.Spec.Coordinator.ProfileRef.Name)
	for _, role := range workflow.Spec.Roles {
		names = append(names, role.ProfileRef.Name)
	}
	slices.Sort(names)
	return slices.Compact(names)
}

func (r *WorkflowTemplateReconciler) profileWorkflowRequests(ctx context.Context, object client.Object) []reconcile.Request {
	var workflows agentsv1alpha1.WorkflowTemplateList
	if err := r.List(ctx, &workflows, client.InNamespace(object.GetNamespace()), client.MatchingFields{_workflowProfileReferenceIndex: object.GetName()}); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, 0, len(workflows.Items))
	for i := range workflows.Items {
		requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&workflows.Items[i])})
	}
	return requests
}

// SetupWithManager includes status updates so Profile dependency changes propagate.
func (r *WorkflowTemplateReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &agentsv1alpha1.WorkflowTemplate{}, _workflowProfileReferenceIndex, workflowProfileReferences); err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).For(&agentsv1alpha1.WorkflowTemplate{}).
		Watches(&agentsv1alpha1.Profile{}, handler.EnqueueRequestsFromMapFunc(r.profileWorkflowRequests)).
		Named("workflowtemplate").Complete(r)
}
