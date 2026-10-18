package gateway

import (
	"context"
	"encoding/json"
	"slices"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	"github.com/softcane/codex-session-operator/internal/openaiapi"
	"github.com/softcane/codex-session-operator/internal/sandbox"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type executionConnectionProvider interface {
	ConnectionState(context.Context, string) (openaiapi.HistoryEnvironment, error)
}

var _ executionConnectionProvider = (*openaiapi.Client)(nil)

const (
	_deliveryRuntimeOnly            = "runtime_only"
	_deliveryLocalWorker            = "local_worker"
	_deliveryLocalWorkerHeld        = "local_worker_held"
	_deliveryObservationNotRequired = "not_required"
)

// executionReady is distinct from HTTP admission and spending. Every normal
// self-hosted send rechecks the current Pod; a stale Ready condition cannot
// authorize a replacement or a Pod whose preparation has failed.
func (g *Gateway) executionReady(ctx context.Context, session *agentsv1alpha1.Session) (bool, string) {
	ctx, cancel := context.WithTimeout(ctx, _profileTimeout)
	defer cancel()
	ready, reason := g.preparedComputeReady(ctx, session)
	if !ready || !sessionUsesSelfHosted(session) {
		return ready, reason
	}
	return g.liveExecutionConnection(ctx, session)
}

// computeReady is a read-only physical observation. It does not transfer a
// different replica's stream capability or authorize provider input delivery.
func (g *Gateway) computeReady(ctx context.Context, session *agentsv1alpha1.Session) (bool, string) {
	ctx, cancel := context.WithTimeout(ctx, _profileTimeout)
	defer cancel()
	ready, reason := g.preparedComputeReady(ctx, session)
	if !ready || !sessionUsesSelfHosted(session) {
		return ready, reason
	}
	return g.runtimeConnectionReady(ctx, session)
}

func (g *Gateway) preparedComputeReady(ctx context.Context, session *agentsv1alpha1.Session) (bool, string) {
	snapshot := session.Status.EnvironmentSnapshot
	if snapshot == nil {
		return g.unfrozenExecutionReady(ctx, session)
	}
	if snapshot.Profile.Mode != _selfHostedMode {
		return true, ""
	}
	proof := session.Status.WorkspacePreparation
	if proof == nil || proof.PodUID == "" || proof.ConfigDigest == "" ||
		!currentConditionTrue(session, "WorkspacePrepared") {
		return false, _workspacePreparingReason
	}
	if !currentConditionTrue(session, "EnvironmentConnected") {
		return false, "EnvironmentConnecting"
	}
	if session.Status.PodName != sandbox.PodName(session) || !session.DeletionTimestamp.IsZero() {
		return false, "ExecutorIdentityMismatch"
	}
	var pod corev1.Pod
	if err := g.reader.Get(ctx, client.ObjectKey{Namespace: session.Namespace, Name: session.Status.PodName}, &pod); err != nil {
		return false, "ExecutorObservationUnavailable"
	}
	if !metav1.IsControlledBy(&pod, session) || string(pod.UID) != proof.PodUID ||
		!pod.DeletionTimestamp.IsZero() {
		return false, "ExecutorIdentityMismatch"
	}
	if pod.Annotations[sandbox.ConfigAnnotation] != proof.ConfigDigest ||
		proof.ClaimUID != session.Status.WorkspaceClaimUID {
		return false, "WorkspaceIdentityMismatch"
	}
	profile := snapshot.Profile.SelfHosted
	if profile == nil {
		return false, "ExecutorConfigurationMismatch"
	}
	if profile.Repository != nil && (proof.RepositoryDigest != sandbox.RepositoryDigest(profile.Repository) ||
		proof.InitialCommit != profile.Repository.Commit) {
		return false, "WorkspaceIdentityMismatch"
	}
	desired, err := sandbox.Pod(session, profile.WorkspaceDirectory, proof.ConfigDigest)
	if err != nil || !sandbox.ExecutorMatches(&pod, desired) {
		return false, "ExecutorConfigurationMismatch"
	}
	if pod.Status.Phase != corev1.PodRunning || !sandbox.InitContainersPrepared(&pod) ||
		!slices.ContainsFunc(pod.Status.Conditions, func(c corev1.PodCondition) bool {
			return c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue
		}) {
		return false, _workspacePreparingReason
	}
	if !preparedInstanceMatches(proof, &pod) {
		return false, "ExecutorInstanceUnverified"
	}
	return true, ""
}

func preparedInstanceMatches(proof *agentsv1alpha1.SessionWorkspacePreparationStatus, pod *corev1.Pod) bool {
	instance, ok := sandbox.ExecutorInstance(pod)
	return ok && proof.ContainerID == instance.ContainerID && proof.RestartCount == instance.RestartCount &&
		proof.StartedAt.Equal(&instance.State.Running.StartedAt)
}

func (g *Gateway) finalSendAllowed(session *agentsv1alpha1.Session, submission Submission) bool {
	return submission.TargetTurn != "" || !sessionUsesSelfHosted(session) || g.executionWatchAvailable(session)
}

func (g *Gateway) claimedSendAllowed(ctx context.Context, session *agentsv1alpha1.Session, uid string,
	submission Submission) (bool, error) {
	// Recheck durable closure/holds and the owned worker epoch after the claim.
	// The metadata lock may have been released to a concurrent cleanup or restore.
	record, err := g.store.Get(ctx, uid)
	if err != nil {
		return false, err
	}
	if stoppedRecord(record) {
		return false, errClosed
	}
	allowed, err := g.deliveryAllowed(ctx, session, uid, submission)
	return allowed && g.finalSendAllowed(session, submission), err
}

func (g *Gateway) unfrozenExecutionReady(ctx context.Context, session *agentsv1alpha1.Session) (bool, string) {
	if session.Spec.EnvironmentRef == nil {
		return true, "" // Hosted Sessions do not have a self-hosted runtime reference.
	}
	var environment agentsv1alpha1.Environment
	if err := g.reader.Get(ctx, client.ObjectKey{Namespace: session.Namespace, Name: session.Spec.EnvironmentRef.Name}, &environment); err != nil {
		return false, "RuntimeObservationUnavailable"
	}
	if environment.Spec.Mode != _selfHostedMode {
		return true, ""
	}
	return false, _workspacePreparingReason
}

func (g *Gateway) liveExecutionConnection(ctx context.Context, session *agentsv1alpha1.Session) (bool, string) {
	g.mu.Lock()
	worker := g.worker
	if worker != nil {
		g.mu.Unlock()
		return worker.liveExecutionConnection(ctx, session)
	}
	w := g.watches[string(session.UID)]
	if w == nil || w.ctx == nil || w.ctx.Err() != nil {
		g.mu.Unlock()
		return false, "EnvironmentStreamUnavailable"
	}
	if w.environmentID == "" {
		w.environmentID = session.Status.EnvironmentID
	}
	if w.environmentID == "" || w.environmentID != session.Status.EnvironmentID {
		g.mu.Unlock()
		return false, "EnvironmentIdentityMismatch"
	}
	revision := w.environmentRevision
	g.mu.Unlock()
	if ready, reason := g.runtimeConnectionReady(ctx, session); !ready {
		return false, reason
	}
	// An event arriving during the read wins over the older response. Stream
	// cancellation revokes eligibility immediately, without waiting for SQL.
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.watches[string(session.UID)] != w || w.ctx.Err() != nil || w.environmentRevision != revision {
		return false, "EnvironmentStreamUnavailable"
	}
	// A positive current read started after the last transition supersedes that
	// older observation. An intervening negative always changes the revision.
	w.environmentBlocked = false
	w.environmentVerified, w.verifiedRevision = true, revision
	return true, ""
}

func (g *Gateway) runtimeConnectionReady(ctx context.Context, session *agentsv1alpha1.Session) (bool, string) {
	observer, ok := g.provider.(executionConnectionProvider)
	if !ok {
		return false, "EnvironmentObservationUnavailable"
	}
	state, err := observer.ConnectionState(ctx, session.Status.EnvironmentID)
	if err != nil || state.ID != session.Status.EnvironmentID || state.Type != _selfHostedProviderType || state.Status != _connectedStatus {
		return false, "EnvironmentConnectionUnverified"
	}
	return true, ""
}

func (g *Gateway) deliveryObservation(session *agentsv1alpha1.Session) string {
	if !sessionUsesSelfHosted(session) {
		return _deliveryObservationNotRequired
	}
	g.mu.Lock()
	worker := g.worker
	if worker != nil {
		g.mu.Unlock()
		return worker.deliveryObservation(session)
	}
	w := g.watches[string(session.UID)]
	g.mu.Unlock()
	if w == nil {
		return _deliveryRuntimeOnly
	}
	if g.executionWatchAvailable(session) {
		return _deliveryLocalWorker
	}
	return _deliveryLocalWorkerHeld
}

func sessionUsesSelfHosted(session *agentsv1alpha1.Session) bool {
	return session.Status.EnvironmentSnapshot != nil && session.Status.EnvironmentSnapshot.Profile.Mode == _selfHostedMode
}

func sameRemoteSession(expected, current *agentsv1alpha1.Session) bool {
	return expected.Status.OpenAISessionID == current.Status.OpenAISessionID &&
		expected.Status.EnvironmentID == current.Status.EnvironmentID
}

func (g *Gateway) bindExecutionWatch(uid, environmentID string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if w := g.watches[uid]; w != nil && w.environmentID == "" {
		w.environmentID = environmentID
	}
}

func (g *Gateway) executionWatchAvailable(session *agentsv1alpha1.Session) bool {
	g.mu.Lock()
	worker := g.worker
	if worker != nil {
		g.mu.Unlock()
		return worker.executionWatchAvailable(session)
	}
	defer g.mu.Unlock()
	w := g.watches[string(session.UID)]
	return w != nil && w.ctx != nil && w.ctx.Err() == nil && !w.environmentBlocked &&
		w.environmentVerified && w.verifiedRevision == w.environmentRevision &&
		w.environmentID == session.Status.EnvironmentID
}

func (g *Gateway) currentExecutionAllowed(ctx context.Context, uid string) bool {
	record, err := g.store.Get(ctx, uid)
	if err != nil || stoppedRecord(record) {
		return false
	}
	var session agentsv1alpha1.Session
	if err := g.reader.Get(ctx, client.ObjectKey{Namespace: record.Namespace, Name: record.Name}, &session); err != nil {
		return false
	}
	if string(session.UID) != uid || !session.DeletionTimestamp.IsZero() ||
		record.RemoteID != "" && session.Status.OpenAISessionID != record.RemoteID || !spendControlReady(&session) ||
		g.sessionInputOpen(ctx, &session) != nil {
		return false
	}
	ready, _ := g.executionReady(ctx, &session)
	return ready && (!sessionUsesSelfHosted(&session) || g.executionWatchAvailable(&session))
}

func (g *Gateway) observeExecutionEvent(w *watch, raw json.RawMessage) {
	var event struct {
		Type          string                       `json:"type"`
		EnvironmentID string                       `json:"environment_id"`
		Environment   openaiapi.HistoryEnvironment `json:"environment"`
	}
	if json.Unmarshal(raw, &event) != nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if event.Type == _errorField || event.Type == "agent.session.failed" {
		w.environmentRevision++
		w.environmentBlocked = true
		return
	}
	if w.environmentID == "" {
		return // The following authoritative read establishes the selected identity.
	}
	if event.Type == "agent.session.environment.reset" && event.EnvironmentID == w.environmentID {
		w.environmentRevision++
		w.environmentBlocked = true
		return
	}
	if event.Environment.ID != w.environmentID || event.Environment.Type != _selfHostedProviderType {
		return
	}
	switch event.Type {
	case "agent.session.environment.connected":
		w.environmentRevision++
		w.environmentBlocked = event.Environment.Status != _connectedStatus
	case "agent.session.environment.disconnected", "agent.session.environment.pending", "agent.session.environment.failed":
		w.environmentRevision++
		w.environmentBlocked = true
	}
}

func currentConditionTrue(session *agentsv1alpha1.Session, name string) bool {
	condition := meta.FindStatusCondition(session.Status.Conditions, name)
	return condition != nil && condition.Status == metav1.ConditionTrue &&
		condition.ObservedGeneration == session.Generation
}
