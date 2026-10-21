package sandbox

import (
	"errors"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
)

const (
	WorkspaceSessionUIDLabel    = "agents.softcane.io/session-uid"
	WorkspaceRetainedAnnotation = "agents.softcane.io/workspace-retained"
	WorkspaceClaimDelete        = "Delete"
	WorkspaceClaimRetain        = "Retain"
)

// WorkspacePolicy returns frozen retention intent rather than consulting a mutable Environment.
func WorkspacePolicy(session *agentsv1alpha1.Session) agentsv1alpha1.DeletionPolicy {
	if session.Status.EnvironmentSnapshot == nil || session.Status.EnvironmentSnapshot.Profile.SelfHosted == nil {
		return ""
	}
	return session.Status.EnvironmentSnapshot.Profile.SelfHosted.Workspace.DeletionPolicy
}

// WorkspaceClaimName follows Agent Sandbox's native PVC naming contract.
func WorkspaceClaimName(runtime *sandboxv1beta1.Sandbox) string { return "workspace-" + runtime.Name }

// WorkspaceMount finds the native PVC mounted at the selected executor directory.
func WorkspaceMount(pod *corev1.Pod, profile *agentsv1alpha1.SelfHostedEnvironment) (string, error) {
	if pod == nil || profile == nil || len(pod.Spec.Containers) != 1 {
		return "", errors.New("executor workspace configuration is absent")
	}
	volumeName := ""
	for _, mount := range pod.Spec.Containers[0].VolumeMounts {
		if mount.MountPath == profile.Workspace.Directory {
			if volumeName != "" || mount.ReadOnly || mount.SubPathExpr != "" {
				return "", errors.New("executor workspace mount is ambiguous or read-only")
			}
			volumeName = mount.Name
		}
	}
	for _, volume := range pod.Spec.Volumes {
		if volume.Name == volumeName && volume.PersistentVolumeClaim != nil && !volume.PersistentVolumeClaim.ReadOnly && volume.PersistentVolumeClaim.ClaimName != "" {
			return volume.PersistentVolumeClaim.ClaimName, nil
		}
	}
	return "", errors.New("executor workspace must use its native persistent claim")
}
