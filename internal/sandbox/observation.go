package sandbox

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"slices"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"
)

const ExecutorInstanceAnnotation = "agents.softcane.io/executor-instance"

// ExecutorInstance requires a healthy first executor instance; restarted instances
// require a replacement Pod and a fresh connection baseline before execution.
func ExecutorInstance(pod *corev1.Pod) (*corev1.ContainerStatus, bool) {
	status := executorIdentity(pod)
	if status == nil || !status.Ready || status.RestartCount != 0 {
		return nil, false
	}
	return status.DeepCopy(), true
}

// ExecutorInstanceDigest identifies a process generation independently of current
// execution eligibility. Pod UID prevents reusing the baseline on replacement.
func ExecutorInstanceDigest(pod *corev1.Pod) string {
	status := executorIdentity(pod)
	if pod.UID == "" || status == nil {
		return ""
	}
	raw, _ := json.Marshal([3]string{string(pod.UID), status.ContainerID,
		status.State.Running.StartedAt.Time.UTC().Format(time.RFC3339Nano)})
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func executorIdentity(pod *corev1.Pod) *corev1.ContainerStatus {
	if len(pod.Spec.Containers) != 1 || len(pod.Status.ContainerStatuses) != 1 {
		return nil
	}
	index := slices.IndexFunc(pod.Status.ContainerStatuses, func(status corev1.ContainerStatus) bool {
		return status.Name == pod.Spec.Containers[0].Name
	})
	if index < 0 {
		return nil
	}
	status := &pod.Status.ContainerStatuses[index]
	if status.ContainerID == "" || status.State.Running == nil || status.State.Running.StartedAt.IsZero() {
		return nil
	}
	return status
}

// ExecutorMatches compares every owned Pod field while tolerating API defaults.
func ExecutorMatches(current, desired *corev1.Pod) bool {
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
