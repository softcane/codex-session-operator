package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"slices"
	"strconv"
	"time"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	"github.com/softcane/codex-session-operator/internal/openaiapi"
	"github.com/softcane/codex-session-operator/internal/sandbox"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const workspacePreparedCondition = "WorkspacePrepared"
const executorRuntimeSelfHostedType = "self_hosted"

type executorRuntimeConnectionProvider interface {
	ConnectionState(context.Context, string) (openaiapi.HistoryEnvironment, error)
}

var _ executorRuntimeConnectionProvider = (*openaiapi.Client)(nil)

func executorState(s *agentsv1alpha1.Session, phase, reason, message string) {
	s.Status.Phase = phase
	s.Status.WorkspacePreparation = nil
	meta.SetStatusCondition(&s.Status.Conditions, metav1.Condition{
		Type: workspacePreparedCondition, Status: metav1.ConditionFalse, Reason: reason,
		Message: "Current executor workspace preparation has not been confirmed", ObservedGeneration: s.Generation})
	meta.SetStatusCondition(&s.Status.Conditions, metav1.Condition{
		Type: readyCondition, Status: metav1.ConditionFalse, Reason: reason, Message: message, ObservedGeneration: s.Generation})
}

func credentialState(s *agentsv1alpha1.Session, reason, message string) {
	executorState(s, "Provisioning", reason, message)
	meta.SetStatusCondition(&s.Status.Conditions, metav1.Condition{Type: connectedCondition, Status: metav1.ConditionUnknown,
		Reason: reason, Message: "Executor credential is unavailable or invalid", ObservedGeneration: s.Generation})
}

func (r *SessionReconciler) reconcileExecutor(ctx context.Context, s *agentsv1alpha1.Session) (ctrl.Result, bool, error) {
	wait := ctrl.Result{RequeueAfter: observationInterval}
	previousPreparation := s.Status.WorkspacePreparation
	executorState(s, "Provisioning", "WorkspacePreparationPending", "Waiting for current executor preparation evidence")
	if err := sandbox.ValidateRepositoryApproval(frozenRepository(s), r.RepositoryURLs); err != nil {
		executorState(s, "Failed", "RepositoryConfigurationInvalid", "Frozen repository source is no longer approved")
		return wait, false, nil
	}
	if s.Status.ExecutorImage == "" {
		executorState(s, "Failed", "ExecutorImageMissing", "Frozen Environment executor image is missing")
		return wait, false, nil
	}
	secret, err := r.executorCredential(ctx, s)
	if secret == nil || err != nil {
		return wait, false, err
	}
	repositorySource, available, err := r.executorRepositoryCredential(ctx, s)
	if !available || err != nil {
		return wait, false, err
	}
	if r.Connections == nil {
		executorState(s, "Connecting", "EnvironmentObserverMissing", "Configure the live OpenAI environment observer")
		return wait, false, nil
	}
	connection, err := r.Connections.Observe(s.Status.OpenAISessionID, s.Status.EnvironmentID)
	if err != nil {
		executorState(s, "Connecting", "EnvironmentObservationFailed", "Could not observe the OpenAI environment connection")
		return wait, false, err
	}
	if !connection.Subscribed {
		executorState(s, "Connecting", "EnvironmentObservationPending", "Waiting for a live stream subscription before starting compute")
		return ctrl.Result{RequeueAfter: time.Second}, false, nil
	}
	claimReady, committed, err := r.reconcileWorkspaceClaim(ctx, s)
	if err != nil {
		return wait, committed, err
	}
	if !claimReady {
		return ctrl.Result{RequeueAfter: time.Second}, committed, nil
	}
	pod, err := sandbox.Pod(s, effectiveWorkspace(s), "")
	if err != nil {
		executorState(s, "Failed", "ExecutorConfigurationInvalid", "Executor image or workspace configuration is invalid")
		return wait, false, nil
	}
	// Hash public configuration, not key material. Secret resourceVersion makes
	// rotation restart the executor without exposing the credential in status.
	config, err := executorConfigurationDigest(pod, secret, repositorySource, connection.Epoch)
	if err != nil {
		return wait, false, err
	}
	pod.Annotations[sandbox.ConfigAnnotation] = config
	if err := controllerutil.SetControllerReference(s, pod, r.Scheme); err != nil {
		return wait, false, err
	}
	s.Status.PodName = pod.Name
	current := &corev1.Pod{}
	if err := r.reader().Get(ctx, client.ObjectKeyFromObject(pod), current); err != nil {
		if !apierrors.IsNotFound(err) {
			return wait, false, err
		}
		pod.Annotations["agents.softcane.io/connection-sequence"] = strconv.FormatInt(connection.Sequence, 10)
		if err := r.Create(ctx, pod); err != nil && !apierrors.IsAlreadyExists(err) {
			return wait, false, err
		}
		executorState(s, "Provisioning", "ExecutorProvisioning", "Executor Pod creation requested; waiting for observation")
		return ctrl.Result{RequeueAfter: time.Second}, false, nil
	}
	if !metav1.IsControlledBy(current, s) {
		executorState(s, "Failed", "ExecutorOwnershipMismatch", "Executor Pod belongs to another owner; adoption is blocked")
		return wait, false, nil
	}
	if !current.DeletionTimestamp.IsZero() {
		executorState(s, "Provisioning", "ExecutorTerminating", "Waiting for the previous executor Pod to disappear before replacement")
		return ctrl.Result{RequeueAfter: time.Second}, false, nil
	}
	if executorNeedsReplacement(current, pod) || executorInstanceChanged(current, previousPreparation) {
		if err := r.Delete(ctx, current, client.Preconditions{UID: &current.UID}); err != nil {
			return wait, false, client.IgnoreNotFound(err)
		}
		message := "Replacing the previous executor Pod"
		if sandbox.PersistentWorkspace(s) == nil {
			message += "; its temporary workspace will be lost"
		}
		executorState(s, "Provisioning", "ExecutorReplacing", message)
		return ctrl.Result{RequeueAfter: time.Second}, false, nil
	}
	return wait, false, r.observeReadyExecutor(ctx, s, current, connection)
}

func (r *SessionReconciler) observeReadyExecutor(ctx context.Context, s *agentsv1alpha1.Session,
	pod *corev1.Pod, connection openaiapi.Connection) error {
	if err := r.pinExecutorInstance(ctx, s, pod); err != nil {
		return err
	}
	baseline, parseErr := strconv.ParseInt(pod.Annotations["agents.softcane.io/connection-sequence"], 10, 64)
	confirmed := parseErr == nil && baseline >= 0 && connection.Connected && connection.Sequence > baseline
	executorState(s, "Connecting", "EnvironmentConnecting", "Waiting for Pod health and a verified OpenAI environment connection")
	prepared := observeExecutorPreparation(s, pod)
	runtimeConfirmed := false
	if prepared && !confirmed && parseErr == nil && baseline >= 0 {
		runtimeConfirmed = r.recoverExecutorConnection(ctx, s, pod, connection)
		confirmed = runtimeConfirmed
	}
	connected := metav1.ConditionFalse
	reason, message := "EnvironmentObserved", "Connection state observed through a live OpenAI event stream"
	if confirmed {
		connected = metav1.ConditionTrue
	}
	if runtimeConfirmed {
		reason, message = "EnvironmentRuntimeObserved", "Current OpenAI runtime connection verified for this prepared executor"
	}
	meta.SetStatusCondition(&s.Status.Conditions, metav1.Condition{Type: connectedCondition, Status: connected,
		Reason: reason, Message: message, ObservedGeneration: s.Generation})
	if prepared && confirmed {
		s.Status.Phase = "Ready"
		meta.SetStatusCondition(&s.Status.Conditions, metav1.Condition{Type: readyCondition, Status: metav1.ConditionTrue,
			Reason: connectedCondition, Message: "Executor Pod is healthy and OpenAI reports its environment connected", ObservedGeneration: s.Generation})
	}
	return nil
}

// Idle replacement need not emit a new connected event on an existing stream.
// A current runtime read can recover that observation; every intervening stream
// transition, loss, or compute identity change vetoes its use.
func (r *SessionReconciler) recoverExecutorConnection(ctx context.Context, session *agentsv1alpha1.Session,
	pod *corev1.Pod, snapshot openaiapi.Connection) bool {
	provider, ok := r.OpenAI.(executorRuntimeConnectionProvider)
	if !ok || !snapshot.Subscribed || snapshot.Epoch == "" || snapshot.Sequence < 0 {
		return false
	}
	before, err := r.Connections.Observe(session.Status.OpenAISessionID, session.Status.EnvironmentID)
	if err != nil || before != snapshot {
		return false
	}
	readCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	state, err := provider.ConnectionState(readCtx, session.Status.EnvironmentID)
	if err != nil || readCtx.Err() != nil || state.ID != session.Status.EnvironmentID || state.Type != executorRuntimeSelfHostedType || state.Status != "connected" {
		return false
	}
	if !r.revalidatePreparedExecutor(readCtx, session, pod, snapshot.Epoch) {
		executorState(session, "Connecting", "ExecutorRevalidationPending", "Current prepared executor identity could not be revalidated")
		return false
	}
	after, err := r.Connections.Observe(session.Status.OpenAISessionID, session.Status.EnvironmentID)
	return err == nil && readCtx.Err() == nil && after == snapshot
}

func (r *SessionReconciler) revalidatePreparedExecutor(ctx context.Context, session *agentsv1alpha1.Session,
	expected *corev1.Pod, epoch string) bool {
	proof := session.Status.WorkspacePreparation
	if proof == nil {
		return false
	}
	var currentSession agentsv1alpha1.Session
	if err := r.reader().Get(ctx, client.ObjectKeyFromObject(session), &currentSession); err != nil ||
		currentSession.UID != session.UID || !currentSession.DeletionTimestamp.IsZero() ||
		currentSession.Status.OpenAISessionID != session.Status.OpenAISessionID || currentSession.Status.EnvironmentID != session.Status.EnvironmentID ||
		currentSession.Status.WorkspaceClaimUID != session.Status.WorkspaceClaimUID {
		return false
	}
	var current corev1.Pod
	if err := r.reader().Get(ctx, client.ObjectKeyFromObject(expected), &current); err != nil ||
		current.UID != expected.UID || string(current.UID) != proof.PodUID || !metav1.IsControlledBy(&current, session) || !current.DeletionTimestamp.IsZero() {
		return false
	}
	if current.Annotations[sandbox.ConfigAnnotation] != proof.ConfigDigest || !sandbox.ExecutorMatches(&current, expected) ||
		current.Annotations["agents.softcane.io/connection-sequence"] != expected.Annotations["agents.softcane.io/connection-sequence"] ||
		current.Status.Phase != corev1.PodRunning || !podReady(&current) || !sandbox.InitContainersPrepared(&current) {
		return false
	}
	instance, observed := sandbox.ExecutorInstance(&current)
	if !observed || instance.ContainerID != proof.ContainerID || instance.RestartCount != proof.RestartCount ||
		!instance.State.Running.StartedAt.Equal(&proof.StartedAt) ||
		current.Annotations[sandbox.ExecutorInstanceAnnotation] != sandbox.ExecutorInstanceDigest(&current) {
		return false
	}
	if !r.revalidateExecutorCredentials(ctx, session, expected, epoch) {
		return false
	}
	return r.revalidatePreparedClaim(ctx, session, proof)
}

func executorConfigurationDigest(pod *corev1.Pod, secret *corev1.Secret,
	repositorySource *agentsv1alpha1.EnvironmentSourceVersion, epoch string) (string, error) {
	configuration := []any{pod.Spec, secret.UID, secret.ResourceVersion, epoch}
	if repositorySource != nil {
		configuration = append(configuration, repositorySource)
	}
	encoded, err := json.Marshal(configuration)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func (r *SessionReconciler) revalidateExecutorCredentials(ctx context.Context, session *agentsv1alpha1.Session,
	expected *corev1.Pod, epoch string) bool {
	secret, err := r.executorCredential(ctx, session)
	if secret == nil || err != nil {
		return false
	}
	source, available, err := r.executorRepositoryCredential(ctx, session)
	if !available || err != nil {
		return false
	}
	desired, err := sandbox.Pod(session, effectiveWorkspace(session), "")
	if err != nil {
		return false
	}
	digest, err := executorConfigurationDigest(desired, secret, source, epoch)
	return err == nil && digest == expected.Annotations[sandbox.ConfigAnnotation]
}

func (r *SessionReconciler) revalidatePreparedClaim(ctx context.Context, session *agentsv1alpha1.Session,
	proof *agentsv1alpha1.SessionWorkspacePreparationStatus) bool {
	storage := sandbox.PersistentWorkspace(session)
	if storage == nil {
		return true
	}
	desired, err := sandbox.WorkspaceClaim(session)
	if err != nil || desired == nil {
		return false
	}
	var claim corev1.PersistentVolumeClaim
	if err := r.reader().Get(ctx, client.ObjectKeyFromObject(desired), &claim); err != nil || string(claim.UID) != proof.ClaimUID ||
		proof.ClaimUID != session.Status.WorkspaceClaimUID || !claim.DeletionTimestamp.IsZero() ||
		claim.Labels[sandbox.WorkspaceSessionUIDLabel] != string(session.UID) || !workspaceClaimMatches(&claim, desired) {
		return false
	}
	if storage.ClaimDeletionPolicy == sandbox.WorkspaceClaimDelete {
		return metav1.IsControlledBy(&claim, session)
	}
	return storage.ClaimDeletionPolicy == sandbox.WorkspaceClaimRetain && session.Status.WorkspaceClaimCreationAttempted && len(claim.OwnerReferences) == 0
}

func (r *SessionReconciler) pinExecutorInstance(ctx context.Context, session *agentsv1alpha1.Session, pod *corev1.Pod) error {
	if pod.Annotations[sandbox.ExecutorInstanceAnnotation] != "" || pod.Status.Phase != corev1.PodRunning ||
		!podReady(pod) || !sandbox.InitContainersPrepared(pod) {
		return nil
	}
	if _, observed := sandbox.ExecutorInstance(pod); !observed {
		return nil
	}
	// Persist process identity before publishing eligibility. The optimistic patch
	// cannot replace a different Pod at the same name or overwrite concurrent state.
	base := pod.DeepCopy()
	digest := sandbox.ExecutorInstanceDigest(pod)
	if digest == "" {
		return errors.New("executor Pod identity is missing")
	}
	pod.Annotations[sandbox.ExecutorInstanceAnnotation] = digest
	if err := r.Patch(ctx, pod, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})); err != nil {
		return err
	}
	if pod.UID != base.UID || !metav1.IsControlledBy(pod, session) ||
		pod.Annotations[sandbox.ExecutorInstanceAnnotation] != digest || sandbox.ExecutorInstanceDigest(pod) != digest {
		return errors.New("executor identity changed during instance observation")
	}
	return nil
}

func frozenRepository(session *agentsv1alpha1.Session) *agentsv1alpha1.RepositorySpec {
	if session.Status.EnvironmentSnapshot == nil || session.Status.EnvironmentSnapshot.Profile.SelfHosted == nil {
		return nil
	}
	return session.Status.EnvironmentSnapshot.Profile.SelfHosted.Repository
}

func observeExecutorPreparation(s *agentsv1alpha1.Session, pod *corev1.Pod) bool {
	if !sandbox.InitContainersPrepared(pod) {
		executorState(s, "Provisioning", "WorkspacePreparing", "Waiting for required workspace preparation to finish")
		for _, status := range pod.Status.InitContainerStatuses {
			if status.State.Terminated != nil && status.State.Terminated.ExitCode != 0 ||
				status.State.Waiting != nil && status.State.Waiting.Reason == "CrashLoopBackOff" {
				executorState(s, "Provisioning", "WorkspacePreparationFailed", "Required workspace preparation failed; protected details are withheld")
				break
			}
		}
		return false
	}
	instance, observed := sandbox.ExecutorInstance(pod)
	instanceDigest := sandbox.ExecutorInstanceDigest(pod)
	if pod.Status.Phase == corev1.PodRunning && podReady(pod) && observed &&
		instanceDigest != "" && pod.Annotations[sandbox.ExecutorInstanceAnnotation] == instanceDigest {
		proof := &agentsv1alpha1.SessionWorkspacePreparationStatus{
			PodUID: string(pod.UID), ClaimUID: s.Status.WorkspaceClaimUID, ConfigDigest: pod.Annotations[sandbox.ConfigAnnotation],
			ContainerID: instance.ContainerID, RestartCount: instance.RestartCount, StartedAt: instance.State.Running.StartedAt,
		}
		if snapshot := s.Status.EnvironmentSnapshot; snapshot != nil && snapshot.Profile.SelfHosted != nil && snapshot.Profile.SelfHosted.Repository != nil {
			proof.RepositoryDigest = sandbox.RepositoryDigest(snapshot.Profile.SelfHosted.Repository)
			proof.InitialCommit = snapshot.Profile.SelfHosted.Repository.Commit
		}
		s.Status.WorkspacePreparation = proof
		meta.SetStatusCondition(&s.Status.Conditions, metav1.Condition{Type: workspacePreparedCondition, Status: metav1.ConditionTrue,
			Reason: "WorkspacePreparationObserved", Message: "Current owned executor Pod completed all required preparation", ObservedGeneration: s.Generation})
	}
	return s.Status.WorkspacePreparation != nil
}

func executorInstanceChanged(pod *corev1.Pod, previous *agentsv1alpha1.SessionWorkspacePreparationStatus) bool {
	if baseline := pod.Annotations[sandbox.ExecutorInstanceAnnotation]; baseline != "" && baseline != sandbox.ExecutorInstanceDigest(pod) {
		return true
	}
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name != pod.Spec.Containers[0].Name {
			continue
		}
		if status.RestartCount > 0 {
			return true
		}
		if previous != nil && previous.PodUID == string(pod.UID) && previous.ContainerID != "" &&
			(status.ContainerID != previous.ContainerID || status.State.Running == nil || !status.State.Running.StartedAt.Equal(&previous.StartedAt)) {
			return true
		}
	}
	return false
}

func executorNeedsReplacement(current, desired *corev1.Pod) bool {
	return current.Annotations[sandbox.ConfigAnnotation] != desired.Annotations[sandbox.ConfigAnnotation] ||
		!executorMatches(current, desired) || current.Status.Phase == corev1.PodFailed || current.Status.Phase == corev1.PodSucceeded
}

func podReady(pod *corev1.Pod) bool {
	return slices.ContainsFunc(pod.Status.Conditions, func(c corev1.PodCondition) bool { return c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue })
}

// Compare owned fields while tolerating API defaults such as termination-message
// settings. Extra containers, envFrom, mounts, or privileges are not accepted.
func executorMatches(current, desired *corev1.Pod) bool {
	return sandbox.ExecutorMatches(current, desired)
}

func (r *SessionReconciler) deleteExecutor(ctx context.Context, s *agentsv1alpha1.Session) (bool, error) {
	// Finalizer release and credential revocation require authoritative absence;
	// informer lag must not hide a running executor.
	var pod corev1.Pod
	if err := r.reader().Get(ctx, client.ObjectKey{Namespace: s.Namespace, Name: sandbox.PodName(s)}, &pod); err != nil {
		return false, client.IgnoreNotFound(err)
	}
	if !metav1.IsControlledBy(&pod, s) {
		return false, nil
	}
	if pod.DeletionTimestamp.IsZero() {
		if err := r.Delete(ctx, &pod, client.Preconditions{UID: &pod.UID}); err != nil {
			return true, client.IgnoreNotFound(err)
		}
	}
	return true, nil
}

func (r *SessionReconciler) executorSecretHandler() handler.EventHandler {
	return handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []reconcile.Request {
		if obj.GetName() != sandbox.ExecutorSecretName {
			return nil
		}
		var sessions agentsv1alpha1.SessionList
		if err := r.List(ctx, &sessions, client.InNamespace(obj.GetNamespace())); err != nil {
			return nil
		}
		requests := make([]reconcile.Request, 0, len(sessions.Items))
		for i := range sessions.Items {
			requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&sessions.Items[i])})
		}
		return requests
	})
}

func (r *SessionReconciler) executorCredential(ctx context.Context, s *agentsv1alpha1.Session) (*corev1.Secret, error) {
	secret := &corev1.Secret{}
	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}
	if err := reader.Get(ctx, client.ObjectKey{Namespace: s.Namespace, Name: sandbox.ExecutorSecretName}, secret); err != nil {
		credentialState(s, "ExecutorCredentialMissing", "Provision the separate codex-executor Secret in the Session namespace")
		// Revoke an existing Pod's credential access when its Secret is removed.
		if apierrors.IsNotFound(err) {
			_, deleteErr := r.deleteExecutor(ctx, s)
			return nil, deleteErr
		}
		return nil, err
	}
	if !secret.DeletionTimestamp.IsZero() || secret.Type != corev1.SecretTypeOpaque ||
		len(secret.Data) != 1 || len(secret.Data[sandbox.ExecutorSecretKey]) == 0 ||
		sha256.Sum256(secret.Data[sandbox.ExecutorSecretKey]) == r.ApplicationKeyHash {
		credentialState(s, "ExecutorCredentialInvalid", "Executor Secret must contain only a nonempty CODEX_API_KEY distinct from the application key")
		_, err := r.deleteExecutor(ctx, s)
		return nil, err
	}
	return secret, nil
}

// Executor commands only receive repository credentials through an explicit frozen
// opt-in. Rotation replaces the Pod; changing the protected source UID never adopts it.
func (r *SessionReconciler) executorRepositoryCredential(ctx context.Context, session *agentsv1alpha1.Session) (*agentsv1alpha1.EnvironmentSourceVersion, bool, error) {
	repository := frozenRepository(session)
	if repository == nil || !repository.MountCredentialsInExecutor {
		return nil, true, nil
	}
	source, err := readRepositorySource(ctx, r.reader(), session.Namespace, repository)
	frozen := session.Status.EnvironmentSnapshot.SourceVersion
	if err == nil && source != nil && frozen != nil && source.UID == frozen.UID {
		return source, true, nil
	}
	credentialState(session, "RepositoryCredentialUnavailable", "The frozen repository credential is unavailable or has changed identity")
	_, deleteErr := r.deleteExecutor(ctx, session)
	return nil, false, deleteErr
}

func (r *SessionReconciler) repositorySecretRequests(ctx context.Context, secret *corev1.Secret) []reconcile.Request {
	if secret.Name != sandbox.RepositorySecretName {
		return nil
	}
	var sessions agentsv1alpha1.SessionList
	if err := r.List(ctx, &sessions, client.InNamespace(secret.Namespace)); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, 0, len(sessions.Items))
	for i := range sessions.Items {
		repository := frozenRepository(&sessions.Items[i])
		if repository != nil && repository.MountCredentialsInExecutor {
			requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&sessions.Items[i])})
		}
	}
	return requests
}
