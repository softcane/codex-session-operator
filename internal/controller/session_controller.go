/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"time"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	"github.com/softcane/codex-session-operator/internal/openaiapi"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const agentNotReadyReason = "AgentNotReady"

const sessionFinalizer = "agents.softcane.io/remote-session"
const readyCondition = "Ready"
const configurationMismatchReason = "ConfigurationMismatch"
const ownershipMismatchReason = "OwnershipMismatch"
const connectedCondition = "EnvironmentConnected"
const observationInterval = 30 * time.Second

// SessionProvider is the controller's external lifecycle boundary.
type SessionProvider interface {
	CreateSelfHostedSession(context.Context, string, string) (openaiapi.Session, error)
	CreateAgentSession(context.Context, string, string, string) (openaiapi.Session, error)
	GetSession(context.Context, string) (openaiapi.Session, error)
	FindSession(context.Context, string) (openaiapi.Session, error)
	DeleteSession(context.Context, string) error
}

// SavedAgentReader supplies fresh effective configuration before a Session claim.
type SavedAgentReader interface {
	GetAgent(context.Context, string) (openaiapi.Agent, error)
}

// EnvironmentObserver reads live connection events independently of reconciliation.
type EnvironmentObserver interface {
	Observe(string, string) (openaiapi.Connection, error)
	Forget(string)
}

var (
	_ SessionProvider      = (*openaiapi.Client)(nil)
	_ SavedAgentReader     = (*openaiapi.Client)(nil)
	_ EnvironmentObserver  = (*openaiapi.Connections)(nil)
	_ reconcile.Reconciler = (*SessionReconciler)(nil)
)

// SessionReconciler reconciles a Session object
type SessionReconciler struct {
	client.Client

	Scheme *runtime.Scheme
	// APIReader bypasses stale cache reads for the one-time creation claim.
	APIReader                        client.Reader
	OpenAI                           SessionProvider
	Agents                           SavedAgentReader
	Environments                     EnvironmentSessionProvider
	Configuration                    SessionConfigurationProvider
	Gateway                          SessionGateway
	InitialSessions                  InitialSessionProvider
	Vaults                           CredentialProvider
	ExecutorImages, CapabilityImages []string
	WorkspaceStorageClasses          []string
	AgentOwnerScope                  string
	Connections                      EnvironmentObserver
	ExecutorImage                    string
	// ApplicationKeyHash prevents accidental reuse of the manager credential.
	ApplicationKeyHash [32]byte
}

// +kubebuilder:rbac:groups=agents.softcane.io,resources=sessions,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=agents.softcane.io,resources=sessions/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=agents.softcane.io,resources=sessions/finalizers,verbs=update
// +kubebuilder:rbac:groups=agents.softcane.io,resources=agents,verbs=get;list;watch
// +kubebuilder:rbac:groups=agents.softcane.io,resources=environments,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create;patch;delete
// +kubebuilder:rbac:groups="",resources=secrets,resourceNames=codex-executor,verbs=get;list;watch

// Reconcile maintains one remote Session without replaying an ambiguous create.
func (r *SessionReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var session agentsv1alpha1.Session
	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}
	if err := reader.Get(ctx, req.NamespacedName, &session); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !session.DeletionTimestamp.IsZero() && !controllerutil.ContainsFinalizer(&session, sessionFinalizer) {
		return ctrl.Result{}, nil
	}
	if r.OpenAI == nil {
		return ctrl.Result{}, errors.New("OpenAI Session provider is not configured")
	}
	if !session.DeletionTimestamp.IsZero() {
		return r.finalize(ctx, &session)
	}
	if !controllerutil.ContainsFinalizer(&session, sessionFinalizer) {
		base := session.DeepCopy()
		controllerutil.AddFinalizer(&session, sessionFinalizer)
		return ctrl.Result{RequeueAfter: time.Second}, r.Patch(ctx, &session, optimisticPatch(base))
	}
	if session.Status.OpenAISessionID != "" {
		// Credential removal must stop compute even during a provider outage.
		if session.Status.ExecutorImage != "" {
			base := session.DeepCopy()
			secret, err := r.executorCredential(ctx, &session)
			if secret == nil || err != nil {
				return ctrl.Result{RequeueAfter: observationInterval}, errors.Join(err, r.patchStatus(ctx, &session, base))
			}
		}
		remote, err := r.OpenAI.GetSession(ctx, session.Status.OpenAISessionID)
		if err != nil {
			reason := "ObservationFailed"
			if errors.Is(err, openaiapi.ErrNotFound) {
				reason = "RemoteSessionMissing"
			}
			return r.problem(ctx, &session, reason, "Could not observe the remote Session; no replacement will be created", err)
		}
		// Validate immutable creation inputs before a supported settings update
		// can publish UpdatingSession or mutate a definitively unsafe Session.
		if reason, message := r.sessionInvariantProblem(ctx, &session, remote); reason != "" {
			return r.rejectRemoteSession(ctx, &session, session.DeepCopy(), reason, message)
		}
		if result, handled, err := r.syncSessionSettings(ctx, &session, remote); handled {
			return result, err
		}
		return r.track(ctx, &session, remote)
	}
	if session.Status.CreationAttempted {
		return r.recoverCreation(ctx, &session)
	}
	return r.createSession(ctx, &session)
}

func (r *SessionReconciler) createSession(ctx context.Context, session *agentsv1alpha1.Session) (ctrl.Result, error) {
	workspace := effectiveWorkspace(session)
	if !strings.HasPrefix(workspace, "/") || len(workspace) > 4096 {
		return r.problem(ctx, session, invalidConfigurationReason, "Workspace must be an absolute path of at most 4096 characters", nil)
	}
	if session.UID == "" {
		return ctrl.Result{}, errors.New("kubernetes Session UID is missing")
	}
	if _, _, err := openaiapi.SessionSpendControlField(session.Spec.SpendControl); err != nil {
		return r.problem(ctx, session, invalidConfigurationReason, "Session spend control is invalid", nil)
	}

	environmentSnapshot, environmentFields, reason, message, err := r.resolveEnvironment(ctx, session)
	if reason != "" {
		return r.problem(ctx, session, reason, message, err)
	}
	if policy, ok := r.Gateway.(investigationPolicy); ok {
		if err := policy.ValidateInvestigation(ctx, session, environmentSnapshot); err != nil {
			return r.problem(ctx, session, "InvestigationPolicyBlocked", "Gateway work policy no longer permits creation", err)
		}
	} else if session.Annotations["agents.softcane.io/issue-source"] != "" ||
		session.Annotations["agents.softcane.io/work-source"] != "" {
		return r.problem(ctx, session, "InvestigationPolicyRequired", "Gateway-managed work requires the gateway policy gate", nil)
	}
	if environmentSnapshot != nil {
		workspace = defaultWorkspace
		if environmentSnapshot.Profile.SelfHosted != nil {
			workspace = environmentSnapshot.Profile.SelfHosted.WorkspaceDirectory
		}
	}
	snapshot, reason, message, err := r.resolveAgent(ctx, session, environmentSnapshot)
	if reason != "" {
		return r.problem(ctx, session, reason, message, err)
	}

	overrides, effectiveAgent, err := r.configuredAgent(ctx, session, snapshot)
	if err != nil {
		return r.problem(ctx, session, invalidConfigurationReason, "Could not resolve effective Session Agent configuration", err)
	}
	if reason, message := r.gatewayAgentBlock(ctx, effectiveAgent, environmentSnapshot); reason != "" {
		return r.problem(ctx, session, reason, message, nil)
	}
	vaultSnapshots, vaultIDs, message, err := r.resolveVaults(ctx, session, environmentSnapshot, effectiveAgent)
	if message != "" {
		return r.problem(ctx, session, "VaultNotReady", message, err)
	}
	none := environmentSnapshot != nil && environmentSnapshot.Profile.Mode == _noneMode
	initialText, initialKey, inputErr := r.initialGatewayInput(ctx, session, environmentSnapshot)
	if inputErr != nil {
		return r.problem(ctx, session, "GatewayInputUnavailable", "Could not read durable initial gateway input", inputErr)
	}
	if none && initialText == "" {
		return r.problem(ctx, session, "GatewayInputRequired", "Waiting for durable first message in gateway store", nil)
	}
	if (sessionConfigured(session) || none) && (r.Configuration == nil || r.AgentOwnerScope == "") {
		return r.problem(ctx, session, "SessionProviderRequired", "Configure Session creation and installation/project scope", nil)
	}
	if _, err := openaiapi.VaultMetadata(session.Spec.Metadata, nil, nil, string(session.UID), r.AgentOwnerScope); err != nil {
		return r.problem(ctx, session, invalidConfigurationReason, err.Error(), nil)
	}

	return r.claimSession(ctx, session, sessionCreation{workspace: workspace, environmentSnapshot: environmentSnapshot,
		environmentFields: environmentFields, agentSnapshot: snapshot, vaultSnapshots: vaultSnapshots, vaultIDs: vaultIDs,
		effectiveAgent: effectiveAgent, overrides: overrides, none: none, initialText: initialText, initialKey: initialKey})
}

type sessionCreation struct {
	workspace                 string
	environmentSnapshot       *agentsv1alpha1.SessionEnvironmentSnapshot
	environmentFields         map[string]any
	agentSnapshot             *agentsv1alpha1.SessionAgentSnapshot
	vaultSnapshots            []agentsv1alpha1.SessionVaultSnapshot
	vaultIDs                  []string
	effectiveAgent, overrides map[string]any
	none                      bool
	initialText, initialKey   string
}

func (r *SessionReconciler) claimSession(ctx context.Context, session *agentsv1alpha1.Session, creation sessionCreation) (ctrl.Result, error) {
	var err error

	// The optimistic status write is the one-time claim. Only its successful
	// writer sends POST. Never put this POST inside a Kubernetes conflict retry.
	base := session.DeepCopy()
	session.Status.CreationAttempted = true
	if session.Spec.SpendControl != nil {
		session.Status.SpendControlAttemptedDigest = spendControlDigest(session.Spec.SpendControl)
	}
	session.Status.CreationWorkspaceDirectory = creation.workspace
	session.Status.AgentSnapshot = creation.agentSnapshot
	session.Status.EnvironmentSnapshot = creation.environmentSnapshot
	session.Status.VaultSnapshots = creation.vaultSnapshots
	if sessionConfigured(session) || creation.none {
		session.Status.OwnerScope = r.AgentOwnerScope
		session.Status.CreationAgentDigest = openaiapi.SessionImmutableAgentDigest(creation.effectiveAgent)
		session.Status.CreationOnlyDigest = openaiapi.SessionCreationOnlyDigest(session.Spec.Agent)
		session.Status.ManagedMetadataKeys = metadataKeys(session.Spec.Metadata)
		session.Status.AppliedMutableDigest = openaiapi.SessionMutableDigest(session.Spec.Agent)
	}
	if creation.environmentSnapshot != nil && creation.environmentSnapshot.Profile.SelfHosted != nil {
		session.Status.ExecutorImage = creation.environmentSnapshot.Profile.SelfHosted.Image
	}
	setState(session, "CreatingSession", metav1.ConditionUnknown, "CreatingSession", "Remote creation has been claimed")
	if err := r.patchStatus(ctx, session, base); err != nil {
		return ctrl.Result{}, err
	}
	var remote openaiapi.Session
	if sessionConfigured(session) || creation.none {
		agentID := ""
		if creation.agentSnapshot != nil {
			agentID = creation.agentSnapshot.OpenAIAgentID
		}
		if creation.environmentSnapshot == nil {
			creation.environmentFields = map[string]any{_wireType: "self_hosted", "workspace_directory": creation.workspace}
		}
		if creation.none {
			remote, err = r.InitialSessions.CreateInitialSession(ctx, string(session.UID), session.Status.OwnerScope, agentID, creation.environmentFields, creation.overrides, creation.vaultIDs, session.Spec.Metadata, session.Spec.SpendControl, creation.initialText, creation.initialKey)
		} else {
			remote, err = r.Configuration.CreateConfiguredSession(ctx, string(session.UID), session.Status.OwnerScope, agentID, creation.environmentFields, creation.overrides, creation.vaultIDs, session.Spec.Metadata, session.Spec.SpendControl)
		}
	} else if creation.environmentSnapshot != nil {
		agentID := ""
		if creation.agentSnapshot != nil {
			agentID = creation.agentSnapshot.OpenAIAgentID
		}
		remote, err = r.Environments.CreateEnvironmentSession(ctx, string(session.UID), agentID, creation.environmentFields)
	} else if creation.agentSnapshot == nil {
		remote, err = r.OpenAI.CreateSelfHostedSession(ctx, string(session.UID), creation.workspace)
	} else {
		remote, err = r.OpenAI.CreateAgentSession(ctx, string(session.UID), creation.workspace, creation.agentSnapshot.OpenAIAgentID)
	}
	if err != nil {
		if errors.Is(err, openaiapi.ErrSpendControlUnavailable) {
			return r.problem(ctx, session, spendUnavailableReason, "OpenAI project has not enabled Session spending limits; creation claim is retained for audit", err)
		}
		return r.problem(ctx, session, creationOutcomeUnknownReason, "Create outcome is unknown; discover by Kubernetes UID before taking further action", err)
	}
	// CREATE can outlive a Kubernetes deletion or spec edit. Observe the current
	// object before provisioning compute, using its persisted creation snapshots.
	var current agentsv1alpha1.Session
	if err := r.reader().Get(ctx, client.ObjectKeyFromObject(session), &current); err != nil {
		return ctrl.Result{}, err
	}
	if current.UID != session.UID {
		return ctrl.Result{}, errors.New("session identity changed during remote creation; audit the original Kubernetes UID")
	}
	return r.track(ctx, &current, remote)
}

func (r *SessionReconciler) recoverCreation(ctx context.Context, session *agentsv1alpha1.Session) (ctrl.Result, error) {
	remote, err := r.OpenAI.FindSession(ctx, string(session.UID))
	if err != nil || remote.ID == "" {
		if previous := meta.FindStatusCondition(session.Status.Conditions, "RemoteSessionCreated"); previous != nil && previous.Reason == spendUnavailableReason {
			return r.problem(ctx, session, spendUnavailableReason, "OpenAI project has not enabled Session spending limits; no replacement create will be sent until audited", err)
		}
		return r.problem(ctx, session, creationOutcomeUnknownReason, "No unique remote Session was discovered; create is blocked and the finalizer is retained for audit", err)
	}
	if !session.DeletionTimestamp.IsZero() {
		if remote.OwnerUID != string(session.UID) || (session.Status.OwnerScope != "" && remote.OwnerScope != session.Status.OwnerScope) {
			return r.problem(ctx, session, ownershipMismatchReason, "Discovered Session belongs to another Kubernetes UID", nil)
		}
		base := session.DeepCopy()
		session.Status.OpenAISessionID = remote.ID
		setState(session, "Terminating", metav1.ConditionUnknown, "CleanupPending", "Discovered Session is pending cleanup")
		return ctrl.Result{RequeueAfter: time.Second}, r.patchStatus(ctx, session, base)
	}
	return r.track(ctx, session, remote)
}

func (r *SessionReconciler) track(ctx context.Context, session *agentsv1alpha1.Session, remote openaiapi.Session) (ctrl.Result, error) {
	if !ownedSession(session, remote) {
		return r.problem(ctx, session, ownershipMismatchReason, "Remote Session ownership or identity does not match", nil)
	}
	// Persist the owned ID even if the rest of the response is unusable, so that
	// finalization can still clean it up. Never persist an unsafe connection URL.
	base := session.DeepCopy()
	setSessionConfigurationCondition(session)
	session.Status.OpenAISessionID = remote.ID
	if !session.DeletionTimestamp.IsZero() {
		setState(session, "Terminating", metav1.ConditionUnknown, "CleanupPending", "Created Session is pending cleanup")
		return ctrl.Result{RequeueAfter: time.Second}, r.patchStatus(ctx, session, base)
	}
	if reason, message := r.sessionInvariantProblem(ctx, session, remote); reason != "" {
		return r.rejectRemoteSession(ctx, session, base, reason, message)
	}
	if spend := session.Spec.SpendControl; spend != nil && !spendControlMatches(spend, remote) {
		return r.rejectRemoteSession(ctx, session, base, "SpendControlUnverified", "OpenAI did not confirm the requested Session spend limit")
	}
	session.Status.SpendControlAttemptedDigest = ""
	if session.Status.EnvironmentSnapshot != nil && session.Status.EnvironmentSnapshot.Profile.Mode == _noneMode {
		setState(session, "Ready", metav1.ConditionTrue, "RemoteSessionCreated", "Remote Session exists without an executor")
		meta.SetStatusCondition(&session.Status.Conditions, metav1.Condition{Type: readyCondition, Status: metav1.ConditionTrue, Reason: "NoEnvironmentRequired", Message: "Gateway owns messages and required actions", ObservedGeneration: session.Generation})
		meta.SetStatusCondition(&session.Status.Conditions, metav1.Condition{Type: connectedCondition, Status: metav1.ConditionUnknown, Reason: "NoEnvironmentRequired", Message: "None mode has no environment connection", ObservedGeneration: session.Generation})
		return ctrl.Result{RequeueAfter: observationInterval}, r.patchStatus(ctx, session, base)
	}
	if session.Status.EnvironmentSnapshot != nil && session.Status.EnvironmentSnapshot.Profile.Mode == openaiHostedMode {
		return r.trackHosted(ctx, session, remote, base)
	}
	session.Status.EnvironmentID = remote.EnvironmentID
	session.Status.RemoteURL = remote.RemoteURL
	setState(session, "Connecting", metav1.ConditionTrue, "RemoteSessionCreated", "Remote Session exists; executor connection is pending")
	result, committed, err := r.reconcileExecutor(ctx, session)
	if committed {
		return result, err
	}
	return result, errors.Join(err, r.patchStatus(ctx, session, base))
}

func ownedSession(session *agentsv1alpha1.Session, remote openaiapi.Session) bool {
	return remote.ID != "" && remote.OwnerUID == string(session.UID) &&
		(session.Status.OwnerScope == "" || remote.OwnerScope == session.Status.OwnerScope) &&
		(session.Status.OpenAISessionID == "" || remote.ID == session.Status.OpenAISessionID)
}

// Spend limits and reasoning effort are mutable and deliberately excluded.
// Creation responses, recovered Sessions, and ordinary observations share these
// checks so an update cannot bypass the invariants needed to execute safely.
func (r *SessionReconciler) sessionInvariantProblem(ctx context.Context, session *agentsv1alpha1.Session, remote openaiapi.Session) (string, string) {
	if !ownedSession(session, remote) {
		return ownershipMismatchReason, "Remote Session ownership or identity does not match"
	}
	if profile := session.Status.EnvironmentSnapshot; profile != nil && profile.Profile.Mode == _noneMode {
		if remote.EnvironmentType != _noneMode || !phase4AgentMatches(session, remote) || !phase4VaultsMatch(session, remote) {
			return configurationMismatchReason, "Remote none-mode configuration differs from frozen creation inputs"
		}
		if remote.Status == remoteFailedStatus {
			return "RemoteSessionFailed", "OpenAI reports a failed Session"
		}
		return "", ""
	}
	if profile := session.Status.EnvironmentSnapshot; profile != nil && profile.Profile.Mode == openaiHostedMode {
		if !phase4AgentMatches(session, remote) || !phase4VaultsMatch(session, remote) {
			return configurationMismatchReason, "Remote Agent differs from the frozen creation configuration"
		}
		if message := hostedSessionConfigurationProblem(session, remote); message != "" {
			return configurationMismatchReason, message
		}
		if remote.Status == remoteFailedStatus {
			return "HostedSetupFailed", "OpenAI reports hosted setup failure; provider details are withheld"
		}
		return "", ""
	}
	if err := remote.ValidateConnection(); err != nil {
		return "InvalidRemoteSession", "Remote Session returned invalid connection details"
	}
	if session.Status.EnvironmentID != "" && session.Status.EnvironmentID != remote.EnvironmentID {
		return configurationMismatchReason, "Remote environment identity changed after creation"
	}
	if !phase4AgentMatches(session, remote) || !phase4VaultsMatch(session, remote) || remote.WorkspaceDirectory != effectiveWorkspace(session) {
		return configurationMismatchReason, "Remote configuration differs from desired configuration; no replacement will be created"
	}
	if reason, message := r.sessionGatewayBlock(ctx, remote, session.Status.EnvironmentSnapshot); session.Status.AgentSnapshot != nil && reason != "" {
		return reason, message
	}
	if !sessionEnvironmentMatches(session, remote) {
		return configurationMismatchReason, "Remote capability paths differ from the frozen profile"
	}
	if remote.Status == remoteFailedStatus {
		return "RemoteSessionFailed", "OpenAI reports a failed Session"
	}
	return "", ""
}

func (r *SessionReconciler) rejectRemoteSession(ctx context.Context, session, base *agentsv1alpha1.Session, reason, message string) (ctrl.Result, error) {
	setState(session, "Failed", metav1.ConditionFalse, reason, message)
	return ctrl.Result{RequeueAfter: observationInterval}, errors.Join(r.stopSessionExecutor(ctx, session), r.patchStatus(ctx, session, base))
}

// A definitive remote failure revokes compute, while its workspace and frozen
// remote identity remain available for explicit cleanup or audit. Observation
// outages leave compute alone because they do not establish remote absence.
func (r *SessionReconciler) stopSessionExecutor(ctx context.Context, session *agentsv1alpha1.Session) error {
	if r.Connections != nil {
		r.Connections.Forget(session.Status.OpenAISessionID)
	}
	_, err := r.deleteExecutor(ctx, session)
	return err
}

func (r *SessionReconciler) finalize(ctx context.Context, session *agentsv1alpha1.Session) (ctrl.Result, error) {
	if r.Connections != nil {
		r.Connections.Forget(session.Status.OpenAISessionID)
	}
	// Stop execution even when remote deletion fails. Wait for observed Pod absence
	// before releasing the finalizer; owner GC remains the secondary cleanup path.
	pending, err := r.deleteExecutor(ctx, session)
	if err != nil {
		return r.problem(ctx, session, "CleanupFailed", "Could not delete executor Pod", err)
	}
	if r.Gateway != nil {
		if err := r.Gateway.Quiesce(ctx, string(session.UID), session.Status.OpenAISessionID); err != nil {
			return r.problem(ctx, session, "GatewayCleanupPending", "Could not stop durable gateway input before cleanup", err)
		}
	}
	if session.Status.OpenAISessionID == "" && session.Status.CreationAttempted {
		return r.recoverCreation(ctx, session)
	}
	if session.Status.OpenAISessionID != "" {
		remote, err := r.OpenAI.GetSession(ctx, session.Status.OpenAISessionID)
		if err != nil && !errors.Is(err, openaiapi.ErrNotFound) {
			return r.problem(ctx, session, "CleanupFailed", "Could not observe remote Session for cleanup", err)
		}
		if err == nil {
			if remote.OwnerUID != string(session.UID) || remote.ID != session.Status.OpenAISessionID || (session.Status.OwnerScope != "" && remote.OwnerScope != session.Status.OwnerScope) {
				return r.problem(ctx, session, ownershipMismatchReason, "Remote Session ownership could not be verified; cleanup is blocked", nil)
			}
			if err := r.OpenAI.DeleteSession(ctx, remote.ID); err != nil && !errors.Is(err, openaiapi.ErrNotFound) {
				return r.problem(ctx, session, "CleanupFailed", "Remote deletion failed; finalizer is retained for retry", err)
			}
			if _, err := r.OpenAI.GetSession(ctx, remote.ID); !errors.Is(err, openaiapi.ErrNotFound) {
				return r.problem(ctx, session, "CleanupPending", "Waiting for observed remote Session absence", err)
			}
		}
	}
	if pending {
		base := session.DeepCopy()
		setState(session, "Terminating", metav1.ConditionTrue, "ExecutorTerminating", "Waiting for executor Pod removal")
		return ctrl.Result{RequeueAfter: time.Second}, r.patchStatus(ctx, session, base)
	}
	base := session.DeepCopy()
	claimPending, claimErr := r.finalizeWorkspaceClaim(ctx, session)
	if claimErr != nil {
		return r.problem(ctx, session, "WorkspaceCleanupFailed", "Could not finish workspace claim cleanup", claimErr)
	}
	if claimPending {
		setState(session, "Terminating", metav1.ConditionTrue, "WorkspaceCleanupPending", "Waiting for workspace claim deletion or retention")
		return ctrl.Result{RequeueAfter: time.Second}, r.patchStatus(ctx, session, base)
	}
	base = session.DeepCopy()
	controllerutil.RemoveFinalizer(session, sessionFinalizer)
	return ctrl.Result{}, r.Patch(ctx, session, optimisticPatch(base))
}

func (r *SessionReconciler) problem(ctx context.Context, session *agentsv1alpha1.Session, reason, message string, cause error) (ctrl.Result, error) {
	if reason == "RemoteSessionMissing" || reason == ownershipMismatchReason {
		cause = errors.Join(cause, r.stopSessionExecutor(ctx, session))
	}
	base := session.DeepCopy()
	phase := "Failed"
	status := metav1.ConditionFalse
	if reason == creationOutcomeUnknownReason {
		phase, status = "CreatingSession", metav1.ConditionUnknown
	}
	if !session.DeletionTimestamp.IsZero() {
		phase = "Terminating"
	}
	setState(session, phase, status, reason, message)
	err := r.patchStatus(ctx, session, base)
	return ctrl.Result{RequeueAfter: observationInterval}, errors.Join(cause, err)
}

func (r *SessionReconciler) patchStatus(ctx context.Context, session, base *agentsv1alpha1.Session) error {
	for i := range session.Status.Conditions {
		condition := &session.Status.Conditions[i]
		if previous := meta.FindStatusCondition(base.Status.Conditions, condition.Type); previous != nil && previous.Status == condition.Status {
			condition.LastTransitionTime = previous.LastTransitionTime
		}
	}
	if reflect.DeepEqual(session.Status, base.Status) {
		return nil
	}
	return r.Status().Patch(ctx, session, optimisticPatch(base))
}

func optimisticPatch(base *agentsv1alpha1.Session) client.Patch {
	return client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})
}

func effectiveWorkspace(session *agentsv1alpha1.Session) string {
	if session.Status.CreationWorkspaceDirectory != "" {
		return session.Status.CreationWorkspaceDirectory
	}
	if session.Spec.WorkspaceDirectory == "" {
		return defaultWorkspace
	}
	return session.Spec.WorkspaceDirectory
}

func setState(session *agentsv1alpha1.Session, phase string, status metav1.ConditionStatus, reason, message string) {
	session.Status.Phase = phase
	meta.SetStatusCondition(&session.Status.Conditions, metav1.Condition{
		Type: "RemoteSessionCreated", Status: status, Reason: reason,
		Message: message, ObservedGeneration: session.Generation,
	})
	meta.SetStatusCondition(&session.Status.Conditions, metav1.Condition{
		Type: readyCondition, Status: metav1.ConditionFalse, Reason: reason,
		Message: message, ObservedGeneration: session.Generation,
	})
	meta.SetStatusCondition(&session.Status.Conditions, metav1.Condition{
		Type: connectedCondition, Status: metav1.ConditionUnknown, Reason: reason,
		Message: "A live OpenAI environment connection has not been confirmed", ObservedGeneration: session.Generation,
	})
}

// SetupWithManager sets up the controller with the Manager.
func (r *SessionReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &agentsv1alpha1.Session{}, agentReferenceIndex, func(obj client.Object) []string {
		session, ok := obj.(*agentsv1alpha1.Session)
		if !ok || session.Spec.AgentRef == nil {
			return nil
		}
		return []string{session.Spec.AgentRef.Name}
	}); err != nil {
		return err
	}
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &agentsv1alpha1.Session{}, environmentReferenceIndex, func(obj client.Object) []string {
		session, ok := obj.(*agentsv1alpha1.Session)
		if !ok || session.Spec.EnvironmentRef == nil {
			return nil
		}
		return []string{session.Spec.EnvironmentRef.Name}
	}); err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&agentsv1alpha1.Session{}).
		Owns(&corev1.Pod{}).
		Owns(&corev1.PersistentVolumeClaim{}).
		Watches(&agentsv1alpha1.Agent{}, handler.EnqueueRequestsFromMapFunc(r.agentSessionRequests)).
		Watches(&agentsv1alpha1.Environment{}, handler.EnqueueRequestsFromMapFunc(r.environmentSessionRequests)).
		Watches(&agentsv1alpha1.Vault{}, handler.EnqueueRequestsFromMapFunc(r.vaultSessionRequests)).
		Watches(&agentsv1alpha1.VaultCredential{}, handler.EnqueueRequestsFromMapFunc(r.vaultSessionRequests)).
		Watches(&corev1.Secret{}, r.executorSecretHandler()).
		Named("session").
		Complete(r)
}

func (r *SessionReconciler) resolveAgent(ctx context.Context, session *agentsv1alpha1.Session, environment *agentsv1alpha1.SessionEnvironmentSnapshot) (*agentsv1alpha1.SessionAgentSnapshot, string, string, error) {
	if session.Spec.AgentRef == nil {
		return nil, "", "", nil
	}
	if r.Agents == nil || r.AgentOwnerScope == "" {
		return nil, "AgentProviderRequired", "Saved Agent provider and installation/project scope must be configured", nil
	}
	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}
	var agent agentsv1alpha1.Agent
	if err := reader.Get(ctx, client.ObjectKey{Namespace: session.Namespace, Name: session.Spec.AgentRef.Name}, &agent); err != nil {
		return nil, agentNotReadyReason, "Referenced Agent could not be read", client.IgnoreNotFound(err)
	}
	condition := meta.FindStatusCondition(agent.Status.Conditions, readyCondition)
	if !agent.DeletionTimestamp.IsZero() || condition == nil || condition.Status != metav1.ConditionTrue ||
		condition.ObservedGeneration != agent.Generation || agent.Status.OpenAIAgentID == "" || agent.Status.OwnerScope != r.AgentOwnerScope {
		return nil, agentNotReadyReason, "Referenced Agent must be current, Ready, owned and not deleting", nil
	}
	remote, err := r.Agents.GetAgent(ctx, agent.Status.OpenAIAgentID)
	if err != nil {
		return nil, agentNotReadyReason, "Referenced remote Agent could not be observed", err
	}
	if !ownedAgent(&agent, remote) {
		return nil, ownershipMismatchReason, "Referenced remote Agent ownership does not match", nil
	}
	desired, err := openaiapi.AgentConfiguration(agent.Spec)
	if err != nil {
		return nil, invalidConfigurationReason, err.Error(), nil
	}
	if agent.Spec.ResetReasoning && openaiapi.AgentReasoningDigest(desired) == agent.Status.ReasoningConfigurationDigest {
		delete(desired, "reasoning")
	}
	changes, err := openaiapi.AgentChanges(desired, remote, agent.Status.ManagedMetadataKeys)
	if err != nil {
		return nil, invalidConfigurationReason, err.Error(), nil
	}
	if len(changes) > 0 {
		return nil, agentNotReadyReason, "Referenced Agent configuration has drifted", nil
	}
	if reason, message := r.gatewayAgentBlock(ctx, remote.Config, environment); !sessionConfigured(session) && reason != "" {
		return nil, reason, message, nil
	}
	return &agentsv1alpha1.SessionAgentSnapshot{Name: agent.Name, UID: string(agent.UID), Generation: agent.Generation,
		OpenAIAgentID: remote.ID, Model: remote.Model, ConfigurationDigest: openaiapi.AgentConfigurationDigest(remote.Config)}, "", "", nil
}

func sessionModel(session *agentsv1alpha1.Session) string {
	if session.Status.AgentSnapshot != nil {
		return session.Status.AgentSnapshot.Model
	}
	return openaiapi.SessionModel
}

func setSessionConfigurationCondition(session *agentsv1alpha1.Session) {
	name := ""
	if session.Spec.AgentRef != nil {
		name = session.Spec.AgentRef.Name
	}
	selected := ""
	if session.Status.AgentSnapshot != nil {
		selected = session.Status.AgentSnapshot.Name
	}
	workspace := session.Spec.WorkspaceDirectory
	if workspace == "" {
		workspace = defaultWorkspace
	}
	environmentName, selectedEnvironment := "", ""
	if session.Spec.EnvironmentRef != nil {
		environmentName = session.Spec.EnvironmentRef.Name
	}
	if session.Status.EnvironmentSnapshot != nil {
		selectedEnvironment = session.Status.EnvironmentSnapshot.Name
	}
	changed := phase4CreationChanged(session) || name != selected || environmentName != selectedEnvironment || (session.Status.EnvironmentSnapshot != nil && workspace != defaultWorkspace) || (session.Status.EnvironmentSnapshot == nil && session.Status.CreationWorkspaceDirectory != "" && workspace != session.Status.CreationWorkspaceDirectory)
	status, reason, message := metav1.ConditionTrue, "CreationConfigurationCurrent", "Session uses its frozen creation configuration"
	if changed {
		status, reason, message = metav1.ConditionFalse, "RequiresNewSession", "Session reference or legacy workspace changed; create a new Session to apply it"
	}
	meta.SetStatusCondition(&session.Status.Conditions, metav1.Condition{Type: "ConfigurationCurrent", Status: status,
		Reason: reason, Message: message, ObservedGeneration: session.Generation})
}

func (r *SessionReconciler) agentSessionRequests(ctx context.Context, obj client.Object) []reconcile.Request {
	var sessions agentsv1alpha1.SessionList
	if err := r.List(ctx, &sessions, client.InNamespace(obj.GetNamespace()), client.MatchingFields{agentReferenceIndex: obj.GetName()}); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, 0, len(sessions.Items))
	for _, session := range sessions.Items {
		requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&session)})
	}
	return requests
}
