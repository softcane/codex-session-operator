package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"slices"
	"strconv"
	"time"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	"github.com/softcane/codex-session-operator/internal/sandbox"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func executorState(s *agentsv1alpha1.Session, phase, reason, message string) {
	s.Status.Phase = phase
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
	if s.Status.ExecutorImage == "" {
		if err := sandbox.ValidateImage(r.ExecutorImage); err != nil {
			executorState(s, "Connecting", "ExecutorImageMissing", "Configure a platform executor image pinned by digest")
			return wait, false, nil
		}
		s.Status.ExecutorImage = r.ExecutorImage
		executorState(s, "Provisioning", "ExecutorImageSelected", "Executor image digest is frozen; provisioning will follow")
		return ctrl.Result{RequeueAfter: time.Second}, false, nil
	}
	secret, err := r.executorCredential(ctx, s)
	if secret == nil || err != nil {
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
	config, err := json.Marshal([]any{pod.Spec, secret.UID, secret.ResourceVersion, connection.Epoch})
	if err != nil {
		return wait, false, err
	}
	digest := sha256.Sum256(config)
	pod.Annotations[sandbox.ConfigAnnotation] = hex.EncodeToString(digest[:])
	if err := controllerutil.SetControllerReference(s, pod, r.Scheme); err != nil {
		return wait, false, err
	}
	s.Status.PodName = pod.Name
	current := &corev1.Pod{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(pod), current); err != nil {
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
	if executorNeedsReplacement(current, pod) {
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
	baseline, parseErr := strconv.ParseInt(current.Annotations["agents.softcane.io/connection-sequence"], 10, 64)
	confirmed := parseErr == nil && baseline >= 0 && connection.Connected && connection.Sequence > baseline
	connected := metav1.ConditionFalse
	if confirmed {
		connected = metav1.ConditionTrue
	}
	meta.SetStatusCondition(&s.Status.Conditions, metav1.Condition{Type: connectedCondition, Status: connected,
		Reason: "EnvironmentObserved", Message: "Connection state observed through a live OpenAI event stream", ObservedGeneration: s.Generation})
	executorState(s, "Connecting", "EnvironmentConnecting", "Waiting for Pod health and an OpenAI connected event")
	if current.Status.Phase == corev1.PodRunning && podReady(current) && confirmed {
		s.Status.Phase = "Ready"
		meta.SetStatusCondition(&s.Status.Conditions, metav1.Condition{Type: readyCondition, Status: metav1.ConditionTrue,
			Reason: connectedCondition, Message: "Executor Pod is healthy and OpenAI reports its environment connected", ObservedGeneration: s.Generation})
	}
	return wait, false, nil
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
	if len(current.Spec.Containers) != 1 || len(current.Spec.InitContainers) != len(desired.Spec.InitContainers) || len(current.Spec.EphemeralContainers) != 0 {
		return false
	}
	container := current.Spec.Containers[0].DeepCopy()
	container.TerminationMessagePath = ""
	container.TerminationMessagePolicy = ""
	desiredContainer := desired.Spec.Containers[0].DeepCopy()
	desiredContainer.TerminationMessagePath = ""
	desiredContainer.TerminationMessagePolicy = ""
	for i := range current.Spec.InitContainers {
		actual := current.Spec.InitContainers[i].DeepCopy()
		expected := desired.Spec.InitContainers[i].DeepCopy()
		actual.TerminationMessagePath, actual.TerminationMessagePolicy = "", ""
		expected.TerminationMessagePath, expected.TerminationMessagePolicy = "", ""
		if !reflect.DeepEqual(actual, expected) {
			return false
		}
	}
	return reflect.DeepEqual(current.Spec.NodeSelector, desired.Spec.NodeSelector) && reflect.DeepEqual(*container, *desiredContainer) &&
		reflect.DeepEqual(current.Spec.Volumes, desired.Spec.Volumes) &&
		reflect.DeepEqual(current.Spec.SecurityContext, desired.Spec.SecurityContext) &&
		!ptr.Deref(current.Spec.AutomountServiceAccountToken, true) &&
		!current.Spec.HostNetwork && !current.Spec.HostPID && !current.Spec.HostIPC &&
		!ptr.Deref(current.Spec.ShareProcessNamespace, false) && (current.Spec.ServiceAccountName == "default" || current.Spec.ServiceAccountName == "") &&
		current.Spec.RestartPolicy == desired.Spec.RestartPolicy &&
		reflect.DeepEqual(current.Spec.TerminationGracePeriodSeconds, desired.Spec.TerminationGracePeriodSeconds)
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
