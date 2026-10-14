package controller

import (
	"context"
	"errors"
	"reflect"
	"slices"
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

const agentMetadataField = "metadata"

const agentFinalizer = "agents.softcane.io/remote-agent"
const agentReferenceIndex = "spec.agentRef.name"

// AgentProvider is the saved configuration lifecycle boundary.
type AgentProvider interface {
	CreateAgent(context.Context, string, string, map[string]any) (openaiapi.Agent, error)
	GetAgent(context.Context, string) (openaiapi.Agent, error)
	FindAgent(context.Context, string, string) (openaiapi.Agent, error)
	UpdateAgent(context.Context, string, map[string]any) (openaiapi.Agent, error)
	DeleteAgent(context.Context, string) error
}

var (
	_ AgentProvider        = (*openaiapi.Client)(nil)
	_ reconcile.Reconciler = (*AgentReconciler)(nil)
)

// AgentReconciler owns saved Agent identity, synchronization and cleanup.
type AgentReconciler struct {
	client.Client

	APIReader client.Reader
	OpenAI    AgentProvider
	// OwnerScope must identify this installation and OpenAI project; freeze it per CR.
	OwnerScope string
}

// +kubebuilder:rbac:groups=agents.softcane.io,resources=agents,verbs=get;list;watch;patch
// +kubebuilder:rbac:groups=agents.softcane.io,resources=agents/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=agents.softcane.io,resources=agents/finalizers,verbs=update;patch

// Reconcile observes ownership before any mutation and never repeats create.
func (r *AgentReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var agent agentsv1alpha1.Agent
	if err := r.reader().Get(ctx, req.NamespacedName, &agent); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !agent.DeletionTimestamp.IsZero() && !controllerutil.ContainsFinalizer(&agent, agentFinalizer) {
		return ctrl.Result{}, nil
	}
	if r.OpenAI == nil {
		return ctrl.Result{}, errors.New("OpenAI Agent provider is not configured")
	}
	if agent.Status.OwnerScope != "" && agent.Status.OwnerScope != r.OwnerScope {
		return r.agentProblem(ctx, &agent, "OwnershipScopeChanged", "Restore the installation/project owner scope before reconciling", nil)
	}
	if !agent.DeletionTimestamp.IsZero() {
		return r.finalizeAgent(ctx, &agent)
	}
	if r.OwnerScope == "" || len(r.OwnerScope) > 512 {
		return r.agentProblem(ctx, &agent, "OwnerScopeRequired", "Configure a stable installation/project owner scope", nil)
	}
	desired, err := openaiapi.AgentConfiguration(agent.Spec)
	if err != nil {
		return r.agentProblem(ctx, &agent, invalidConfigurationReason, err.Error(), nil)
	}
	if !controllerutil.ContainsFinalizer(&agent, agentFinalizer) {
		base := agent.DeepCopy()
		controllerutil.AddFinalizer(&agent, agentFinalizer)
		return ctrl.Result{RequeueAfter: time.Second}, r.Patch(ctx, &agent, agentPatch(base))
	}
	if agent.Status.OpenAIAgentID != "" {
		remote, err := r.OpenAI.GetAgent(ctx, agent.Status.OpenAIAgentID)
		if err != nil {
			return r.agentProblem(ctx, &agent, "ObservationFailed", "Could not observe remote Agent; no replacement will be created", err)
		}
		return r.syncAgent(ctx, &agent, remote, desired)
	}
	if agent.Status.CreationAttempted {
		return r.discoverAgent(ctx, &agent)
	}
	if agent.UID == "" {
		return ctrl.Result{}, errors.New("kubernetes Agent UID is missing")
	}
	base := agent.DeepCopy()
	agent.Status.CreationAttempted, agent.Status.OwnerScope = true, r.OwnerScope
	agent.Status.ManagedMetadataKeys = metadataKeys(agent.Spec.Metadata)
	agent.Status.ReasoningConfigurationDigest = openaiapi.AgentReasoningDigest(desired)
	agentCondition(&agent, metav1.ConditionUnknown, "CreatingAgent", "Remote creation has been claimed")
	if err := r.patchAgentStatus(ctx, &agent, base); err != nil {
		return ctrl.Result{}, err
	}
	remote, err := r.OpenAI.CreateAgent(ctx, string(agent.UID), agent.Status.OwnerScope, desired)
	if err != nil {
		return r.agentProblem(ctx, &agent, creationOutcomeUnknownReason, "Creation outcome is unknown; discovery must resolve it before further action", err)
	}
	return r.recordAgent(ctx, &agent, remote)
}

func (r *AgentReconciler) reader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

func (r *AgentReconciler) discoverAgent(ctx context.Context, agent *agentsv1alpha1.Agent) (ctrl.Result, error) {
	remote, err := r.OpenAI.FindAgent(ctx, string(agent.UID), agent.Status.OwnerScope)
	if err != nil || remote.ID == "" {
		return r.agentProblem(ctx, agent, creationOutcomeUnknownReason, "No unique owned Agent discovered; finalizer retained for audit", err)
	}
	return r.recordAgent(ctx, agent, remote)
}

func ownedAgent(agent *agentsv1alpha1.Agent, remote openaiapi.Agent) bool {
	return remote.ID != "" && remote.OwnerUID == string(agent.UID) && remote.OwnerScope == agent.Status.OwnerScope &&
		(agent.Status.OpenAIAgentID == "" || agent.Status.OpenAIAgentID == remote.ID)
}

func (r *AgentReconciler) recordAgent(ctx context.Context, agent *agentsv1alpha1.Agent, remote openaiapi.Agent) (ctrl.Result, error) {
	if !ownedAgent(agent, remote) {
		return r.agentProblem(ctx, agent, "OwnershipMismatch", "Remote Agent ownership could not be verified", nil)
	}
	base := agent.DeepCopy()
	agent.Status.OpenAIAgentID = remote.ID
	agentCondition(agent, metav1.ConditionFalse, "ObservationPending", "Remote identity saved; configuration must be observed")
	return ctrl.Result{RequeueAfter: time.Second}, r.patchAgentStatus(ctx, agent, base)
}

func (r *AgentReconciler) syncAgent(ctx context.Context, agent *agentsv1alpha1.Agent, remote openaiapi.Agent, desired map[string]any) (ctrl.Result, error) {
	if !ownedAgent(agent, remote) {
		return r.agentProblem(ctx, agent, "OwnershipMismatch", "Remote Agent ownership could not be verified", nil)
	}
	// A null reset resolves to model-specific defaults that GET cannot distinguish
	// from explicit effort. Acknowledge it only after a confirmed update response.
	reasoningDigest := openaiapi.AgentReasoningDigest(desired)
	if agent.Spec.ResetReasoning && reasoningDigest == agent.Status.ReasoningConfigurationDigest {
		delete(desired, "reasoning")
	}
	changes, err := openaiapi.AgentChanges(desired, remote, agent.Status.ManagedMetadataKeys)
	if err != nil {
		return r.agentProblem(ctx, agent, invalidConfigurationReason, err.Error(), nil)
	}
	if reasoningDigest != agent.Status.ReasoningConfigurationDigest && reasoningDigest != "" {
		changes["reasoning"] = desired["reasoning"]
	}
	if len(changes) > 0 {
		base := agent.DeepCopy()
		// Keep both old and new keys claimed before updating, so a lost response cannot lose key ownership.
		keys := metadataKeys(agent.Spec.Metadata)
		if existing, ok := remote.Config[agentMetadataField].(map[string]any); ok {
			for _, key := range agent.Status.ManagedMetadataKeys {
				if _, present := existing[key]; present {
					keys = append(keys, key)
				}
			}
		}
		slices.Sort(keys)
		agent.Status.ManagedMetadataKeys = slices.Compact(keys)
		agentCondition(agent, metav1.ConditionFalse, "UpdatingAgent", "Saved configuration is being updated")
		if err := r.patchAgentStatus(ctx, agent, base); err != nil {
			return ctrl.Result{}, err
		}
		updated, err := r.OpenAI.UpdateAgent(ctx, remote.ID, changes)
		if err != nil {
			reason := "UpdateOutcomeUnknown"
			if failure, ok := errors.AsType[*openaiapi.AgentRequestError](err); ok && (failure.StatusCode == 400 || failure.StatusCode == 422) {
				reason = "UpdateRejected"
			}
			return r.agentProblem(ctx, agent, reason, "Update did not return a confirmed result; observe before retrying", err)
		}
		if !ownedAgent(agent, updated) {
			return r.agentProblem(ctx, agent, "OwnershipMismatch", "Updated Agent ownership could not be verified", nil)
		}
		if _, updatedReasoning := changes["reasoning"]; updatedReasoning {
			base := agent.DeepCopy()
			agent.Status.ReasoningConfigurationDigest = reasoningDigest
			agent.Status.ObservedGeneration = agent.Generation
			agentCondition(agent, metav1.ConditionFalse, "ObservationPending", "Update accepted; waiting for configuration observation")
			return ctrl.Result{RequeueAfter: time.Second}, r.patchAgentStatus(ctx, agent, base)
		}
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}
	base := agent.DeepCopy()
	agent.Status.ObservedGeneration = agent.Generation
	agent.Status.ManagedMetadataKeys = appliedMetadataKeys(agent.Spec.Metadata, agent.Status.ManagedMetadataKeys)
	agent.Status.ReasoningConfigurationDigest = reasoningDigest
	agentCondition(agent, metav1.ConditionTrue, "AgentSynced", "Saved Agent configuration has been observed")
	return ctrl.Result{RequeueAfter: observationInterval}, r.patchAgentStatus(ctx, agent, base)
}

func (r *AgentReconciler) finalizeAgent(ctx context.Context, agent *agentsv1alpha1.Agent) (ctrl.Result, error) {
	if agent.Spec.DeletionPolicy == retainPolicy {
		return r.releaseAgent(ctx, agent)
	}
	referenced, err := r.agentHasSessions(ctx, agent)
	if err != nil {
		return r.agentProblem(ctx, agent, "ReferenceCheckFailed", "Could not check referencing Sessions", err)
	}
	if referenced {
		return r.agentProblem(ctx, agent, "AgentInUse", "Referencing Sessions must be removed before deleting the saved Agent", nil)
	}
	if agent.Status.OpenAIAgentID == "" {
		if agent.Status.CreationAttempted {
			return r.discoverAgent(ctx, agent)
		}
		return r.releaseAgent(ctx, agent)
	}
	remote, err := r.OpenAI.GetAgent(ctx, agent.Status.OpenAIAgentID)
	if errors.Is(err, openaiapi.ErrNotFound) {
		return r.releaseAgent(ctx, agent)
	}
	if err != nil {
		return r.agentProblem(ctx, agent, "CleanupFailed", "Could not observe remote Agent for cleanup", err)
	}
	if !ownedAgent(agent, remote) {
		return r.agentProblem(ctx, agent, "OwnershipMismatch", "Remote ownership could not be verified; cleanup is blocked", nil)
	}
	if err := r.OpenAI.DeleteAgent(ctx, remote.ID); err != nil {
		return r.agentProblem(ctx, agent, "CleanupFailed", "Could not confirm remote Agent deletion", err)
	}
	_, err = r.agentProblem(ctx, agent, "CleanupPending", "Waiting to observe remote Agent absence", nil)
	return ctrl.Result{RequeueAfter: time.Second}, err
}

func (r *AgentReconciler) agentHasSessions(ctx context.Context, agent *agentsv1alpha1.Agent) (bool, error) {
	// Direct paginated reads avoid a stale cache missing a newly claimed Session.
	token := ""
	for range 10 {
		var sessions agentsv1alpha1.SessionList
		if err := r.reader().List(ctx, &sessions, client.InNamespace(agent.Namespace), client.Limit(100), client.Continue(token)); err != nil {
			return false, err
		}
		for _, session := range sessions.Items {
			if (session.Spec.AgentRef != nil && session.Spec.AgentRef.Name == agent.Name) ||
				(session.Status.AgentSnapshot != nil && session.Status.AgentSnapshot.UID == string(agent.UID)) {
				return true, nil
			}
		}
		if sessions.Continue == "" {
			return false, nil
		}
		token = sessions.Continue
	}
	return false, errors.New("session reference scan limit reached; cleanup blocked")
}

func (r *AgentReconciler) releaseAgent(ctx context.Context, agent *agentsv1alpha1.Agent) (ctrl.Result, error) {
	base := agent.DeepCopy()
	controllerutil.RemoveFinalizer(agent, agentFinalizer)
	return ctrl.Result{}, r.Patch(ctx, agent, agentPatch(base))
}

func (r *AgentReconciler) agentProblem(ctx context.Context, agent *agentsv1alpha1.Agent, reason, message string, cause error) (ctrl.Result, error) {
	base := agent.DeepCopy()
	status := metav1.ConditionFalse
	if reason == creationOutcomeUnknownReason {
		status = metav1.ConditionUnknown
	}
	agentCondition(agent, status, reason, message)
	return ctrl.Result{RequeueAfter: observationInterval}, errors.Join(cause, r.patchAgentStatus(ctx, agent, base))
}

func agentCondition(agent *agentsv1alpha1.Agent, status metav1.ConditionStatus, reason, message string) {
	meta.SetStatusCondition(&agent.Status.Conditions, metav1.Condition{Type: readyCondition, Status: status, Reason: reason, Message: message, ObservedGeneration: agent.Generation})
}

func agentPatch(base *agentsv1alpha1.Agent) client.Patch {
	return client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})
}

func (r *AgentReconciler) patchAgentStatus(ctx context.Context, agent, base *agentsv1alpha1.Agent) error {
	if reflect.DeepEqual(agent.Status, base.Status) {
		return nil
	}
	return r.Status().Patch(ctx, agent, agentPatch(base))
}

// SetupWithManager watches Session references so deletion can progress on release.
func (r *AgentReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).For(&agentsv1alpha1.Agent{}).
		Watches(&agentsv1alpha1.Session{}, handler.EnqueueRequestsFromMapFunc(sessionAgentRequests)).
		Named("agent").Complete(r)
}

func sessionAgentRequests(_ context.Context, obj client.Object) []reconcile.Request {
	session, ok := obj.(*agentsv1alpha1.Session)
	if !ok {
		return nil
	}
	names := make(map[string]bool)
	if session.Spec.AgentRef != nil {
		names[session.Spec.AgentRef.Name] = true
	}
	if session.Status.AgentSnapshot != nil {
		names[session.Status.AgentSnapshot.Name] = true
	}
	requests := make([]reconcile.Request, 0, len(names))
	for name := range names {
		requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKey{Namespace: session.Namespace, Name: name}})
	}
	return requests
}

func metadataKeys(metadata *map[string]string) []string {
	if metadata == nil {
		return nil
	}
	keys := make([]string, 0, len(*metadata))
	for key := range *metadata {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}

// Omitted metadata preserves ownership; an explicit replacement selects new keys.
func appliedMetadataKeys(metadata *map[string]string, claimed []string) []string {
	if metadata == nil {
		return slices.Clone(claimed)
	}
	return metadataKeys(metadata)
}
