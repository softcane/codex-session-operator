package sandbox

import (
	"slices"

	corev1 "k8s.io/api/core/v1"
)

const RepositoryInitContainerName = "repository-prepare"

// InitContainersPrepared requires successful observations of every configured init container.
func InitContainersPrepared(pod *corev1.Pod) bool {
	if len(pod.Status.InitContainerStatuses) != len(pod.Spec.InitContainers) {
		return false
	}
	for _, container := range pod.Spec.InitContainers {
		if !slices.ContainsFunc(pod.Status.InitContainerStatuses, func(status corev1.ContainerStatus) bool {
			return status.Name == container.Name && status.State.Terminated != nil && status.State.Terminated.ExitCode == 0
		}) {
			return false
		}
	}
	return true
}
