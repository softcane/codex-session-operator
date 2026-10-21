package sandbox

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"slices"
	"time"

	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
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

// ExecutorMatches checks the complete execution specification after known Kubernetes defaults.
// Scheduling decisions and standard eviction tolerations are API observations, not drift.
func ExecutorMatches(current, desired *corev1.Pod) bool {
	if current == nil || desired == nil {
		return false
	}
	for key, value := range desired.Labels {
		if current.Labels[key] != value {
			return false
		}
	}
	for key, value := range desired.Annotations {
		if current.Annotations[key] != value {
			return false
		}
	}
	actual, expected := current.Spec.DeepCopy(), desired.Spec.DeepCopy()
	normalizePod(actual)
	normalizePod(expected)
	return apiequality.Semantic.DeepEqual(actual, expected)
}

func normalizePod(pod *corev1.PodSpec) {
	pod.NodeName = ""
	if pod.ServiceAccountName == "default" {
		pod.ServiceAccountName = ""
	}
	pod.DeprecatedServiceAccount = ""
	if pod.DNSPolicy == "" {
		pod.DNSPolicy = corev1.DNSClusterFirst
	}
	if pod.RestartPolicy == "" {
		pod.RestartPolicy = corev1.RestartPolicyAlways
	}
	if pod.SchedulerName == "" {
		pod.SchedulerName = "default-scheduler"
	}
	if pod.TerminationGracePeriodSeconds == nil {
		pod.TerminationGracePeriodSeconds = new(int64(30))
	}
	if pod.EnableServiceLinks == nil {
		pod.EnableServiceLinks = new(true)
	}
	if pod.PreemptionPolicy == nil {
		policy := corev1.PreemptLowerPriority
		pod.PreemptionPolicy = &policy
	}
	if pod.Priority != nil && *pod.Priority == 0 {
		pod.Priority = nil
	}
	pod.Tolerations = slices.DeleteFunc(pod.Tolerations, func(t corev1.Toleration) bool {
		return (t.Key == "node.kubernetes.io/not-ready" || t.Key == "node.kubernetes.io/unreachable") && t.Operator == corev1.TolerationOpExists && t.Effect == corev1.TaintEffectNoExecute && t.TolerationSeconds != nil && *t.TolerationSeconds == 300
	})
	for i := range pod.Containers {
		normalizeContainer(&pod.Containers[i])
	}
	for i := range pod.InitContainers {
		normalizeContainer(&pod.InitContainers[i])
	}
	for i := range pod.Volumes {
		volume := &pod.Volumes[i]
		if volume.Secret != nil && volume.Secret.DefaultMode == nil {
			volume.Secret.DefaultMode = new(int32(0o644))
		}
		if volume.ConfigMap != nil && volume.ConfigMap.DefaultMode == nil {
			volume.ConfigMap.DefaultMode = new(int32(0o644))
		}
		if volume.Projected != nil && volume.Projected.DefaultMode == nil {
			volume.Projected.DefaultMode = new(int32(0o644))
		}
	}
}

func normalizeContainer(container *corev1.Container) {
	if container.ImagePullPolicy == "" {
		container.ImagePullPolicy = corev1.PullIfNotPresent
	}
	if container.TerminationMessagePath == "" {
		container.TerminationMessagePath = "/dev/termination-log"
	}
	if container.TerminationMessagePolicy == "" {
		container.TerminationMessagePolicy = corev1.TerminationMessageReadFile
	}
	for i := range container.Ports {
		if container.Ports[i].Protocol == "" {
			container.Ports[i].Protocol = corev1.ProtocolTCP
		}
	}
	for i := range container.Env {
		source := container.Env[i].ValueFrom
		if source != nil && source.FieldRef != nil && source.FieldRef.APIVersion == "" {
			source.FieldRef.APIVersion = "v1"
		}
	}
	for _, probe := range []*corev1.Probe{container.ReadinessProbe, container.LivenessProbe, container.StartupProbe} {
		if probe == nil {
			continue
		}
		if probe.TimeoutSeconds == 0 {
			probe.TimeoutSeconds = 1
		}
		if probe.PeriodSeconds == 0 {
			probe.PeriodSeconds = 10
		}
		if probe.SuccessThreshold == 0 {
			probe.SuccessThreshold = 1
		}
		if probe.FailureThreshold == 0 {
			probe.FailureThreshold = 3
		}
		if probe.HTTPGet != nil && probe.HTTPGet.Scheme == "" {
			probe.HTTPGet.Scheme = corev1.URISchemeHTTP
		}
	}
}
