package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"time"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	"github.com/softcane/codex-session-operator/internal/openaiapi"
	"github.com/softcane/codex-session-operator/internal/sandbox"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	extensionsv1beta1 "sigs.k8s.io/agent-sandbox/extensions/api/v1beta1"
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

func executorState(s *agentsv1alpha1.Session, reason, message string) {

	s.Status.WorkspacePreparation = nil
	meta.SetStatusCondition(&s.Status.Conditions, metav1.Condition{
		Type: workspacePreparedCondition, Status: metav1.ConditionFalse, Reason: reason,
		Message: "Current executor workspace preparation has not been confirmed", ObservedGeneration: s.Generation})
	meta.SetStatusCondition(&s.Status.Conditions, metav1.Condition{
		Type: readyCondition, Status: metav1.ConditionFalse, Reason: reason, Message: message, ObservedGeneration: s.Generation})
}

func credentialState(s *agentsv1alpha1.Session, reason, message string) {
	executorState(s, reason, message)
	meta.SetStatusCondition(&s.Status.Conditions, metav1.Condition{Type: connectedCondition, Status: metav1.ConditionUnknown,
		Reason: reason, Message: "Executor credential is unavailable or invalid", ObservedGeneration: s.Generation})
}

func (r *SessionReconciler) reconcileExecutor(ctx context.Context, s *agentsv1alpha1.Session) (ctrl.Result, error) {
	wait := ctrl.Result{RequeueAfter: observationInterval}
	previous := s.Status.WorkspacePreparation
	executorState(s, "WorkspacePreparationPending", "Waiting for current sandbox preparation evidence")
	template, resolved, err := r.resolveExecutorTemplate(ctx, s)
	if err != nil || template == nil {
		return wait, err
	}
	sources, ready, err := r.resolveExecutorSources(ctx, s, template)
	if err != nil || !ready {
		return wait, err
	}
	connection, subscribed, err := r.executorSubscription(s)
	if err != nil || !subscribed {
		return wait, err
	}
	config, err := claimConfigurationDigest(resolved, sources, connection.Epoch)
	if err != nil {
		return wait, err
	}
	s.Status.ExecutorConfigDigest = config
	claim, err := r.reconcileSandboxClaim(ctx, s, template)
	if err != nil || claim == nil {
		return wait, err
	}
	runtime, err := r.observeAllocatedSandbox(ctx, s, claim, template)
	if err != nil || runtime == nil {
		return wait, err
	}
	return wait, r.reconcileAllocatedExecutor(ctx, s, claim, runtime, connection, previous)
}

func (r *SessionReconciler) resolveExecutorTemplate(ctx context.Context, s *agentsv1alpha1.Session) (*extensionsv1beta1.SandboxTemplate, *agentsv1alpha1.SandboxPoolSnapshot, error) {
	profile := s.Status.EnvironmentSnapshot.Profile.SelfHosted
	if profile == nil || !slices.Contains(r.SandboxPools, profile.SandboxPoolRef.Name) {
		_, err := r.deleteExecutor(ctx, s)
		executorState(s, "SandboxPoolNotApproved", "The selected sandbox pool is not approved")
		return nil, nil, err
	}
	resolved, err := sandbox.ResolvePool(ctx, r.reader(), s.Namespace, profile)
	if err != nil || !reflect.DeepEqual(resolved, s.Status.EnvironmentSnapshot.SandboxPool) {
		_, stopErr := r.deleteExecutor(ctx, s)
		executorState(s, "SandboxConfigurationChanged", "The frozen sandbox setup is unavailable or changed")
		return nil, nil, errors.Join(err, stopErr)
	}
	var template extensionsv1beta1.SandboxTemplate
	if err := r.reader().Get(ctx, client.ObjectKey{Namespace: s.Namespace, Name: resolved.TemplateName}, &template); err != nil {
		return nil, nil, err
	}
	return &template, resolved, nil
}

func (r *SessionReconciler) resolveExecutorSources(ctx context.Context, s *agentsv1alpha1.Session, template *extensionsv1beta1.SandboxTemplate) ([]agentsv1alpha1.EnvironmentSourceVersion, bool, error) {
	secret, err := r.executorCredential(ctx, s)
	if secret == nil || err != nil {
		return nil, false, err
	}
	hadSources := len(s.Status.SandboxSecretSources) > 0
	sources, err := r.sandboxCredentials(ctx, s, template)
	if err != nil {
		_, stopErr := r.deleteExecutor(ctx, s)
		credentialState(s, "SandboxCredentialUnavailable", "A frozen sandbox credential is missing or changed identity")
		return nil, false, errors.Join(err, stopErr)
	}
	if !hadSources && len(s.Status.SandboxSecretSources) > 0 {
		return nil, false, nil
	}
	return sources, true, nil
}

func (r *SessionReconciler) executorSubscription(s *agentsv1alpha1.Session) (openaiapi.Connection, bool, error) {
	if r.Connections == nil {
		executorState(s, "EnvironmentObserverMissing", "Configure the live OpenAI environment observer")
		return openaiapi.Connection{}, false, nil
	}
	connection, err := r.Connections.Observe(s.Status.OpenAISessionID, s.Status.EnvironmentID)
	if err != nil || !connection.Subscribed {
		executorState(s, "EnvironmentObservationPending", "Waiting for a live OpenAI stream subscription")
		return connection, false, err
	}
	return connection, true, nil
}

func (r *SessionReconciler) reconcileSandboxClaim(ctx context.Context, s *agentsv1alpha1.Session, template *extensionsv1beta1.SandboxTemplate) (*extensionsv1beta1.SandboxClaim, error) {
	desired, err := sandbox.Claim(s, template, s.Status.ExecutorConfigDigest)
	if err != nil {
		return nil, err
	}
	if err := controllerutil.SetControllerReference(s, desired, r.Scheme); err != nil {
		return nil, err
	}
	s.Status.SandboxClaimName = desired.Name
	var claim extensionsv1beta1.SandboxClaim
	if err := r.reader().Get(ctx, client.ObjectKeyFromObject(desired), &claim); err != nil {
		if !apierrors.IsNotFound(err) {
			return nil, err
		}
		if s.Status.SandboxClaimUID != "" {
			executorState(s, "SandboxClaimMissing", "The frozen sandbox claim disappeared; automatic replacement is blocked")
			return nil, nil
		}
		if err := r.Create(ctx, desired); err != nil {
			return nil, client.IgnoreAlreadyExists(err)
		}
		s.Status.SandboxClaimUID = string(desired.UID)
		executorState(s, "ExecutorProvisioning", "Sandbox claim created; waiting for allocation")
		return nil, nil
	}
	if !claimBindingMatches(s, &claim, desired) {
		executorState(s, "SandboxClaimChanged", "Sandbox claim identity, binding or lifecycle changed")
		_, stopErr := r.deleteExecutor(ctx, s)
		return nil, stopErr
	}
	s.Status.SandboxClaimUID = string(claim.UID)
	if claim.Status.SandboxStatus.Name == "" {
		return nil, nil
	}
	return &claim, nil
}

func claimBindingMatches(s *agentsv1alpha1.Session, current, desired *extensionsv1beta1.SandboxClaim) bool {
	return metav1.IsControlledBy(current, s) && current.DeletionTimestamp.IsZero() &&
		(s.Status.SandboxClaimUID == "" || s.Status.SandboxClaimUID == string(current.UID)) &&
		reflect.DeepEqual(current.Spec.Env, desired.Spec.Env) && current.Spec.WarmPoolRef == desired.Spec.WarmPoolRef &&
		current.Spec.Lifecycle == nil && len(current.Spec.VolumeClaimTemplates) == 0
}

func (r *SessionReconciler) observeAllocatedSandbox(ctx context.Context, s *agentsv1alpha1.Session, claim *extensionsv1beta1.SandboxClaim, template *extensionsv1beta1.SandboxTemplate) (*sandboxv1beta1.Sandbox, error) {
	var runtime sandboxv1beta1.Sandbox
	if err := r.reader().Get(ctx, client.ObjectKey{Namespace: s.Namespace, Name: claim.Status.SandboxStatus.Name}, &runtime); err != nil {
		return nil, client.IgnoreNotFound(err)
	}
	if !metav1.IsControlledBy(&runtime, claim) || !runtime.DeletionTimestamp.IsZero() ||
		s.Status.SandboxUID != "" && s.Status.SandboxUID != string(runtime.UID) {
		executorState(s, "SandboxIdentityChanged", "The allocated sandbox identity changed; adoption is blocked")
		_, stopErr := r.deleteExecutor(ctx, s)
		return nil, stopErr
	}
	s.Status.SandboxName, s.Status.SandboxUID, s.Status.PodName = runtime.Name, string(runtime.UID), runtime.Name
	if !sandbox.RuntimeMatches(s, claim, &runtime, template) {
		executorState(s, "SandboxConfigurationChanged", "Allocated sandbox differs from its frozen template and claim")
		_, stopErr := r.deleteExecutor(ctx, s)
		return nil, stopErr
	}
	return &runtime, nil
}

func (r *SessionReconciler) reconcileAllocatedExecutor(ctx context.Context, s *agentsv1alpha1.Session, claim *extensionsv1beta1.SandboxClaim, runtime *sandboxv1beta1.Sandbox, connection openaiapi.Connection, previous *agentsv1alpha1.SessionWorkspacePreparationStatus) error {
	var pod corev1.Pod
	podErr := r.reader().Get(ctx, client.ObjectKey{Namespace: s.Namespace, Name: runtime.Name}, &pod)
	if podErr != nil && !apierrors.IsNotFound(podErr) {
		return podErr
	}
	restarting, err := r.restartAllocatedSandbox(ctx, s, claim, runtime, connection, podErr != nil)
	if err != nil || restarting {
		return err
	}
	if podErr != nil {
		return nil
	}
	if !sandbox.OwnsExecutor(s, claim, runtime, &pod) {
		executorState(s, "ExecutorOwnershipMismatch", "Executor ownership differs from the frozen sandbox chain")
		return nil
	}
	expected, err := sandbox.ExpectedPod(runtime)
	if err != nil {
		return err
	}
	if !sandbox.ExecutorMatches(&pod, expected) || pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded || executorInstanceChanged(&pod, previous) {
		return r.suspendSandbox(ctx, runtime)
	}
	if !pod.DeletionTimestamp.IsZero() {
		return nil
	}
	if err := r.observeWorkspace(ctx, s, runtime, &pod); err != nil {
		_, stopErr := r.deleteExecutor(ctx, s)
		return errors.Join(err, stopErr)
	}
	if s.Status.ExecutorPodUID != string(pod.UID) {
		s.Status.ExecutorConnectionSequence = connection.Sequence
	}
	s.Status.ExecutorPodUID = string(pod.UID)
	err = r.observeReadyExecutor(ctx, s, &pod, connection)
	if s.Status.WorkspacePreparation != nil {
		s.Status.WorkspacePreparation.ConfigDigest = sandbox.ConfigDigest(runtime)
	}
	return err
}

func (r *SessionReconciler) restartAllocatedSandbox(ctx context.Context, s *agentsv1alpha1.Session, claim *extensionsv1beta1.SandboxClaim, runtime *sandboxv1beta1.Sandbox, connection openaiapi.Connection, podAbsent bool) (bool, error) {
	config := s.Status.ExecutorConfigDigest
	if runtime.Spec.PodTemplate.ObjectMeta.Annotations[sandbox.ConfigAnnotation] == config && runtime.Spec.OperatingMode != sandboxv1beta1.SandboxOperatingModeSuspended {
		return false, nil
	}
	if runtime.Spec.OperatingMode != sandboxv1beta1.SandboxOperatingModeSuspended {
		return true, r.suspendSandbox(ctx, runtime)
	}
	if !podAbsent {
		return true, nil
	}
	if claim.Spec.AdditionalPodMetadata.Annotations[sandbox.ConfigAnnotation] != config {
		base := claim.DeepCopy()
		claim.Annotations[sandbox.ConfigAnnotation] = config
		claim.Spec.AdditionalPodMetadata.Annotations[sandbox.ConfigAnnotation] = config
		return true, r.Patch(ctx, claim, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{}))
	}
	if runtime.Spec.PodTemplate.ObjectMeta.Annotations[sandbox.ConfigAnnotation] != config {
		return true, nil
	}
	// Upstream has propagated the new metadata; the same allocation may restart.
	base := runtime.DeepCopy()
	runtime.Spec.OperatingMode = sandboxv1beta1.SandboxOperatingModeRunning
	s.Status.ExecutorPodUID = ""
	s.Status.ExecutorConnectionSequence = connection.Sequence
	return true, r.Patch(ctx, runtime, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{}))
}

func (r *SessionReconciler) observeReadyExecutor(ctx context.Context, s *agentsv1alpha1.Session,
	pod *corev1.Pod, connection openaiapi.Connection) error {
	if err := r.pinExecutorInstance(ctx, s, pod); err != nil {
		return err
	}
	baseline := s.Status.ExecutorConnectionSequence
	confirmed := baseline >= 0 && connection.Connected && connection.Sequence > baseline
	executorState(s, "EnvironmentConnecting", "Waiting for Pod health and a verified OpenAI environment connection")
	prepared := observeExecutorPreparation(s, pod)
	if prepared {
		var runtime sandboxv1beta1.Sandbox
		if err := r.reader().Get(ctx, client.ObjectKey{Namespace: s.Namespace, Name: s.Status.SandboxName}, &runtime); err != nil {
			return err
		}
		s.Status.WorkspacePreparation.ConfigDigest = sandbox.ConfigDigest(&runtime)
	}
	runtimeConfirmed := false
	if prepared && !confirmed && baseline >= 0 {
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
		executorState(session, "ExecutorRevalidationPending", "Current prepared executor identity could not be revalidated")
		return false
	}
	after, err := r.Connections.Observe(session.Status.OpenAISessionID, session.Status.EnvironmentID)
	return err == nil && readCtx.Err() == nil && after == snapshot
}

func (r *SessionReconciler) revalidatePreparedExecutor(ctx context.Context, session *agentsv1alpha1.Session,
	expected *corev1.Pod, epoch string) bool {
	proof := session.Status.WorkspacePreparation
	if proof == nil || proof.SandboxUID != session.Status.SandboxUID {
		return false
	}
	var currentSession agentsv1alpha1.Session
	if err := r.reader().Get(ctx, client.ObjectKeyFromObject(session), &currentSession); err != nil ||
		currentSession.UID != session.UID || !currentSession.DeletionTimestamp.IsZero() ||
		currentSession.Status.OpenAISessionID != session.Status.OpenAISessionID || currentSession.Status.EnvironmentID != session.Status.EnvironmentID ||
		currentSession.Status.WorkspaceClaimUID != session.Status.WorkspaceClaimUID || currentSession.Status.SandboxUID != session.Status.SandboxUID {
		return false
	}
	var current corev1.Pod
	if err := r.reader().Get(ctx, client.ObjectKeyFromObject(expected), &current); err != nil ||
		current.UID != expected.UID || string(current.UID) != proof.PodUID || !r.ownsExecutor(ctx, session, &current) || !current.DeletionTimestamp.IsZero() {
		return false
	}
	if !r.preparedRuntimeMatches(ctx, session, proof) || !sandbox.ExecutorMatches(&current, expected) ||
		current.Status.Phase != corev1.PodRunning || !podReady(&current) || !sandbox.InitContainersPrepared(&current) {
		return false
	}
	instance, observed := sandbox.ExecutorInstance(&current)
	if !observed || instance.ContainerID != proof.ContainerID || instance.RestartCount != proof.RestartCount ||
		!instance.State.Running.StartedAt.Equal(&proof.StartTime) ||
		current.Annotations[sandbox.ExecutorInstanceAnnotation] != sandbox.ExecutorInstanceDigest(&current) {
		return false
	}
	if !r.revalidateExecutorCredentials(ctx, session, expected, epoch) {
		return false
	}
	return r.revalidatePreparedClaim(ctx, session, proof)
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
	if pod.UID != base.UID || !r.ownsExecutor(ctx, session, pod) ||
		pod.Annotations[sandbox.ExecutorInstanceAnnotation] != digest || sandbox.ExecutorInstanceDigest(pod) != digest {
		return errors.New("executor identity changed during instance observation")
	}
	return nil
}

func observeExecutorPreparation(s *agentsv1alpha1.Session, pod *corev1.Pod) bool {
	if !sandbox.InitContainersPrepared(pod) {
		executorState(s, "WorkspacePreparing", "Waiting for required workspace preparation to finish")
		for _, status := range pod.Status.InitContainerStatuses {
			if status.State.Terminated != nil && status.State.Terminated.ExitCode != 0 ||
				status.State.Waiting != nil && status.State.Waiting.Reason == "CrashLoopBackOff" {
				executorState(s, "WorkspacePreparationFailed", "Required workspace preparation failed; protected details are withheld")
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
			SandboxClaimUID: s.Status.SandboxClaimUID, SandboxUID: s.Status.SandboxUID, PodUID: string(pod.UID), ClaimUID: s.Status.WorkspaceClaimUID, ConfigDigest: pod.Annotations[sandbox.ConfigAnnotation],
			ContainerID: instance.ContainerID, RestartCount: instance.RestartCount, StartTime: instance.State.Running.StartedAt,
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
			(status.ContainerID != previous.ContainerID || status.State.Running == nil || !status.State.Running.StartedAt.Equal(&previous.StartTime)) {
			return true
		}
	}
	return false
}

func podReady(pod *corev1.Pod) bool {
	return slices.ContainsFunc(pod.Status.Conditions, func(c corev1.PodCondition) bool { return c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue })
}

func claimConfigurationDigest(snapshot *agentsv1alpha1.SandboxPoolSnapshot, sources []agentsv1alpha1.EnvironmentSourceVersion, epoch string) (string, error) {
	encoded, err := json.Marshal([]any{snapshot, sources, epoch})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func (r *SessionReconciler) sandboxCredentials(ctx context.Context, session *agentsv1alpha1.Session, template *extensionsv1beta1.SandboxTemplate) ([]agentsv1alpha1.EnvironmentSourceVersion, error) {
	var versions []agentsv1alpha1.EnvironmentSourceVersion
	var identities []agentsv1alpha1.SandboxSecretSource
	for _, name := range sandbox.SecretNames(template) {
		var secret corev1.Secret
		if err := r.reader().Get(ctx, client.ObjectKey{Namespace: session.Namespace, Name: name}, &secret); err != nil {
			return nil, err
		}
		if secret.Type != corev1.SecretTypeOpaque || !secret.DeletionTimestamp.IsZero() || secret.UID == "" {
			return nil, errors.New("sandbox credential is unavailable")
		}
		for _, key := range sandbox.SecretKeys(template)[name] {
			if len(secret.Data[key]) == 0 {
				return nil, errors.New("sandbox credential key is absent")
			}
		}
		identities = append(identities, agentsv1alpha1.SandboxSecretSource{Name: name, UID: string(secret.UID)})
		versions = append(versions, agentsv1alpha1.EnvironmentSourceVersion{UID: string(secret.UID), ResourceVersion: secret.ResourceVersion})
	}
	if len(session.Status.SandboxSecretSources) == 0 {
		if session.Status.SandboxClaimUID != "" {
			return nil, errors.New("sandbox credential identities were not frozen before execution")
		}
		session.Status.SandboxSecretSources = identities
	} else if !reflect.DeepEqual(identities, session.Status.SandboxSecretSources) {
		return nil, errors.New("sandbox credential identity changed")
	}
	return versions, nil
}

func (r *SessionReconciler) suspendSandbox(ctx context.Context, runtime *sandboxv1beta1.Sandbox) error {
	if runtime.Spec.OperatingMode == sandboxv1beta1.SandboxOperatingModeSuspended {
		return nil
	}
	base := runtime.DeepCopy()
	runtime.Spec.OperatingMode = sandboxv1beta1.SandboxOperatingModeSuspended
	return r.Patch(ctx, runtime, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{}))
}

// deleteExecutor stops compute without deleting its native workspace. Its callers
// include credential revocation and definitive provider failure, not just closure.
func (r *SessionReconciler) deleteExecutor(ctx context.Context, s *agentsv1alpha1.Session) (bool, error) {
	var claim extensionsv1beta1.SandboxClaim
	name := s.Status.SandboxClaimName
	if name == "" {
		name = sandbox.ClaimName(s)
	}
	if err := r.reader().Get(ctx, client.ObjectKey{Namespace: s.Namespace, Name: name}, &claim); err != nil {
		if !apierrors.IsNotFound(err) {
			return true, err
		}
		return r.executorStillPresent(ctx, s)
	}
	if !metav1.IsControlledBy(&claim, s) || s.Status.SandboxClaimUID != "" && string(claim.UID) != s.Status.SandboxClaimUID {
		return true, errors.New("sandbox claim ownership changed")
	}
	name = claim.Status.SandboxStatus.Name
	if name == "" {
		name = s.Status.SandboxName
	}
	if name == "" {
		name = claim.Name
	}
	var runtime sandboxv1beta1.Sandbox
	if err := r.reader().Get(ctx, client.ObjectKey{Namespace: s.Namespace, Name: name}, &runtime); err != nil {
		if !apierrors.IsNotFound(err) {
			return true, err
		}
		return r.executorStillPresent(ctx, s)
	}
	if !metav1.IsControlledBy(&runtime, &claim) {
		if runtime.Annotations[sandbox.WorkspaceRetainedAnnotation] != string(s.UID) || len(runtime.OwnerReferences) != 0 || string(runtime.UID) != s.Status.SandboxUID || runtime.Spec.OperatingMode != sandboxv1beta1.SandboxOperatingModeSuspended {
			return true, errors.New("sandbox owner changed")
		}
	}
	if err := r.suspendSandbox(ctx, &runtime); err != nil {
		return true, err
	}
	var pod corev1.Pod
	if err := r.reader().Get(ctx, client.ObjectKey{Namespace: s.Namespace, Name: runtime.Name}, &pod); err != nil {
		return false, client.IgnoreNotFound(err)
	}
	if !metav1.IsControlledBy(&pod, &runtime) {
		return true, errors.New("executor owner changed")
	}
	return true, nil
}

func (r *SessionReconciler) executorStillPresent(ctx context.Context, s *agentsv1alpha1.Session) (bool, error) {
	if s.Status.PodName == "" {
		return false, nil
	}
	var pod corev1.Pod
	if err := r.reader().Get(ctx, client.ObjectKey{Namespace: s.Namespace, Name: s.Status.PodName}, &pod); err != nil {
		return false, client.IgnoreNotFound(err)
	}
	owner := metav1.GetControllerOf(&pod)
	if owner == nil || string(owner.UID) != s.Status.SandboxUID {
		return true, errors.New("executor identity changed during cleanup")
	}
	return true, nil
}

func (r *SessionReconciler) ownsExecutor(ctx context.Context, s *agentsv1alpha1.Session, pod *corev1.Pod) bool {
	var claim extensionsv1beta1.SandboxClaim
	var runtime sandboxv1beta1.Sandbox
	return r.reader().Get(ctx, client.ObjectKey{Namespace: s.Namespace, Name: s.Status.SandboxClaimName}, &claim) == nil &&
		r.reader().Get(ctx, client.ObjectKey{Namespace: s.Namespace, Name: s.Status.SandboxName}, &runtime) == nil && sandbox.OwnsExecutor(s, &claim, &runtime, pod)
}

func (r *SessionReconciler) revalidateExecutorCredentials(ctx context.Context, s *agentsv1alpha1.Session, pod *corev1.Pod, epoch string) bool {
	secret, err := r.executorCredential(ctx, s)
	if err != nil || secret == nil {
		return false
	}
	profile := s.Status.EnvironmentSnapshot.Profile.SelfHosted
	snapshot, err := sandbox.ResolvePool(ctx, r.reader(), s.Namespace, profile)
	if err != nil || !reflect.DeepEqual(snapshot, s.Status.EnvironmentSnapshot.SandboxPool) {
		return false
	}
	var template extensionsv1beta1.SandboxTemplate
	if r.reader().Get(ctx, client.ObjectKey{Namespace: s.Namespace, Name: snapshot.TemplateName}, &template) != nil {
		return false
	}
	versions, err := r.sandboxCredentials(ctx, s, &template)
	if err != nil {
		return false
	}
	config, err := claimConfigurationDigest(snapshot, versions, epoch)
	return err == nil && config == s.Status.ExecutorConfigDigest && config == pod.Annotations[sandbox.ConfigAnnotation]
}

func (r *SessionReconciler) sandboxSessionRequests(ctx context.Context, obj client.Object) []reconcile.Request {
	owner := metav1.GetControllerOf(obj)
	if owner == nil || owner.APIVersion != extensionsv1beta1.GroupVersion.String() || owner.Kind != "SandboxClaim" {
		return nil
	}
	var claim extensionsv1beta1.SandboxClaim
	if r.Get(ctx, client.ObjectKey{Namespace: obj.GetNamespace(), Name: owner.Name}, &claim) != nil || claim.UID != owner.UID {
		return nil
	}
	parent := metav1.GetControllerOf(&claim)
	if parent == nil || parent.APIVersion != agentsv1alpha1.SchemeGroupVersion.String() || parent.Kind != "Session" {
		return nil
	}
	return []reconcile.Request{{NamespacedName: client.ObjectKey{Namespace: claim.Namespace, Name: parent.Name}}}
}

func (r *SessionReconciler) executorPodRequests(ctx context.Context, obj client.Object) []reconcile.Request {
	owner := metav1.GetControllerOf(obj)
	if owner == nil || owner.APIVersion != sandboxv1beta1.GroupVersion.String() || owner.Kind != "Sandbox" {
		return nil
	}
	var runtime sandboxv1beta1.Sandbox
	if r.Get(ctx, client.ObjectKey{Namespace: obj.GetNamespace(), Name: owner.Name}, &runtime) != nil || runtime.UID != owner.UID {
		return nil
	}
	return r.sandboxSessionRequests(ctx, &runtime)
}

func (r *SessionReconciler) executorSecretHandler() handler.EventHandler {
	return handler.EnqueueRequestsFromMapFunc(r.executorSecretRequests)
}
func (r *SessionReconciler) executorSecretRequests(ctx context.Context, obj client.Object) []reconcile.Request {
	if obj.GetName() != sandbox.ExecutorSecretName && obj.GetName() != "codex-repository" {
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

// preflightSandboxCredentials revokes local execution before any remote provider
// read, so an OpenAI outage cannot prolong access to a removed protected source.
func (r *SessionReconciler) preflightSandboxCredentials(ctx context.Context, s *agentsv1alpha1.Session) error {
	snapshot := s.Status.EnvironmentSnapshot.SandboxPool
	if snapshot == nil || len(s.Status.SandboxSecretSources) == 0 {
		return nil
	}
	current, err := sandbox.ResolvePool(ctx, r.reader(), s.Namespace, s.Status.EnvironmentSnapshot.Profile.SelfHosted)
	if err != nil || !reflect.DeepEqual(current, snapshot) || !slices.Contains(r.SandboxPools, snapshot.Name) {
		_, stopErr := r.deleteExecutor(ctx, s)
		executorState(s, "SandboxConfigurationChanged", "The frozen sandbox setup is unavailable or changed")
		return errors.Join(errors.New("frozen sandbox setup is unavailable or changed"), stopErr)
	}
	var template extensionsv1beta1.SandboxTemplate
	if err := r.reader().Get(ctx, client.ObjectKey{Namespace: s.Namespace, Name: snapshot.TemplateName}, &template); err != nil {
		_, stopErr := r.deleteExecutor(ctx, s)
		executorState(s, "SandboxConfigurationChanged", "The frozen sandbox template is unavailable")
		return errors.Join(err, stopErr)
	}
	versions, err := r.sandboxCredentials(ctx, s, &template)
	if err != nil {
		_, stopErr := r.deleteExecutor(ctx, s)
		credentialState(s, "SandboxCredentialUnavailable", "A frozen protected source is unavailable or changed identity")
		return errors.Join(err, stopErr)
	}
	if r.Connections != nil && s.Status.ExecutorConfigDigest != "" {
		connection, observeErr := r.Connections.Observe(s.Status.OpenAISessionID, s.Status.EnvironmentID)
		if observeErr == nil && connection.Subscribed {
			config, digestErr := claimConfigurationDigest(snapshot, versions, connection.Epoch)
			if digestErr != nil {
				return digestErr
			}
			if config != s.Status.ExecutorConfigDigest {
				executorState(s, "ExecutorReplacing", "Protected source rotation requires a new executor process")
				_, stopErr := r.deleteExecutor(ctx, s)
				return stopErr
			}
		}
	}
	return nil
}
