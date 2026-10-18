package sandbox

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const _observationContainerID = "containerd://first"
const _observationForeign = "foreign"
const _observationExecutorName = "executor"

func TestExecutorInstanceRequiresCurrentRunningIdentity(t *testing.T) {
	pod := &corev1.Pod{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: _observationExecutorName}}},
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{Name: _observationExecutorName, Ready: true, ContainerID: _observationContainerID,
			State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(time.Unix(1700000000, 0))}}}}}}
	instance, observed := ExecutorInstance(pod)
	if !observed || instance.ContainerID != _observationContainerID {
		t.Fatal("valid first running instance was not observed")
	}
	instance.ContainerID = "mutated-return"
	if pod.Status.ContainerStatuses[0].ContainerID != _observationContainerID {
		t.Fatal("returned instance exposed mutable input")
	}
	for _, name := range []string{"missing", _observationForeign, "unready", "restarted", "unknown start", "waiting"} {
		t.Run(name, func(t *testing.T) {
			copyPod := pod.DeepCopy()
			switch name {
			case "missing":
				copyPod.Status.ContainerStatuses = nil
			case _observationForeign:
				copyPod.Status.ContainerStatuses[0].Name = _observationForeign
			case "unready":
				copyPod.Status.ContainerStatuses[0].Ready = false
			case "restarted":
				copyPod.Status.ContainerStatuses[0].RestartCount = 1
			case "unknown start":
				copyPod.Status.ContainerStatuses[0].State.Running.StartedAt = metav1.Time{}
			case "waiting":
				copyPod.Status.ContainerStatuses[0].State.Running = nil
			}
			if _, observed := ExecutorInstance(copyPod); observed {
				t.Fatal("unproven executor instance was accepted")
			}
		})
	}
}

func TestExecutorInstanceDigestSeparatesIdentityFromEligibility(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{UID: "baseline-pod"}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: _observationExecutorName}}},
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{Name: _observationExecutorName, Ready: true, ContainerID: _observationContainerID,
			State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(time.Unix(1700000000, 0))}}}}}}
	baseline := ExecutorInstanceDigest(pod)
	if len(baseline) != 64 {
		t.Fatal("instance baseline is not a bounded safe digest")
	}
	copyPod := pod.DeepCopy()
	copyPod.Status.ContainerStatuses[0].Ready = false
	if ExecutorInstanceDigest(copyPod) != baseline {
		t.Fatal("eligibility loss discarded the process baseline")
	}
	for _, field := range []string{"Pod UID", "container ID", "start time"} {
		t.Run(field, func(t *testing.T) {
			copyPod := pod.DeepCopy()
			switch field {
			case "Pod UID":
				copyPod.UID = "different-pod"
			case "container ID":
				copyPod.Status.ContainerStatuses[0].ContainerID = "different-container"
			case "start time":
				copyPod.Status.ContainerStatuses[0].State.Running.StartedAt = metav1.NewTime(time.Unix(1700000001, 0))
			}
			if ExecutorInstanceDigest(copyPod) == baseline {
				t.Fatal("different execution identity reused the instance baseline")
			}
		})
	}
}
