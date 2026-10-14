package sandbox

import (
	"errors"
	"path"
	"slices"
	"strings"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

const (
	capabilityMountPath  = "/capabilities"
	capabilityVolumeName = "capabilities"
)

// ValidateProfile enforces fixed platform security and approved image contracts.
func ValidateProfile(profile *agentsv1alpha1.SelfHostedEnvironment, executorImages, capabilityImages, storageClasses []string) error {
	if profile == nil {
		return errors.New("self-hosted profile is missing")
	}
	if ValidateImage(profile.Image) != nil || !slices.Contains(executorImages, profile.Image) {
		return errors.New("executor image must be pinned and approved by platform configuration")
	}
	if profile.Architecture != "amd64" && profile.Architecture != "arm64" {
		return errors.New("executor architecture must be explicit")
	}
	if err := ValidateWorkspace(profile.WorkspaceDirectory); err != nil {
		return err
	}
	size, err := resource.ParseQuantity(profile.WorkspaceSize)
	if err != nil || size.Sign() <= 0 || size.Cmp(resource.MustParse("16Gi")) > 0 {
		return errors.New("workspace size must be positive and at most 16Gi")
	}
	if err := ValidateWorkspaceStorage(profile.WorkspaceStorage, storageClasses); err != nil {
		return err
	}
	if _, err := ProfileResources(profile.Resources); err != nil {
		return err
	}
	for _, capability := range profile.Capabilities {
		if ValidateImage(capability.Image) != nil || !slices.Contains(capabilityImages, capability.Image) {
			return errors.New("capability image must be pinned and approved by platform configuration")
		}
		if capability.Path != "/capabilities/"+capability.Name || (capability.Type != "plugin" && capability.Type != "skills") {
			return errors.New("capability path must select its unique registered root")
		}
	}
	return nil
}

// ValidateWorkspaceStorage requires an approved class and an explicit PVC policy.
func ValidateWorkspaceStorage(storage *agentsv1alpha1.WorkspaceStorage, storageClasses []string) error {
	if storage == nil {
		return nil // Existing Environment profiles keep ephemeral workspaces.
	}
	switch storage.Type {
	case WorkspaceStorageEphemeral:
		if storage.StorageClassName != "" || storage.ClaimDeletionPolicy != "" {
			return errors.New("ephemeral workspace cannot configure a storage class or claim deletion policy")
		}
	case WorkspaceStoragePersistent:
		if storage.StorageClassName == "" || storage.StorageClassName == "*" || !slices.Contains(storageClasses, storage.StorageClassName) {
			return errors.New("persistent workspace requires a platform-approved storage class")
		}
		if storage.ClaimDeletionPolicy != WorkspaceClaimDelete && storage.ClaimDeletionPolicy != WorkspaceClaimRetain {
			return errors.New("persistent workspace requires an explicit Delete or Retain claim policy")
		}
	default:
		return errors.New("workspace storage type must be Ephemeral or Persistent")
	}
	return nil
}

// ValidateWorkspace excludes paths already used by the platform/runtime.
func ValidateWorkspace(workspace string) error {
	if !strings.HasPrefix(workspace, "/") || path.Clean(workspace) != workspace || workspace == "/" {
		return errors.New("executor workspace must be a clean absolute directory other than root")
	}
	for _, reserved := range []string{"/home", "/tmp", "/usr", "/bin", "/sbin", "/etc", "/dev", "/proc", "/sys", "/run", capabilityMountPath} {
		if workspace == reserved || strings.HasPrefix(workspace, reserved+"/") {
			return errors.New("executor workspace overlaps a reserved directory")
		}
	}
	return nil
}

// ProfileResources bounds both scheduling requests and execution limits.
func ProfileResources(config *agentsv1alpha1.ExecutorResources) (corev1.ResourceRequirements, error) {
	values := agentsv1alpha1.ExecutorResources{CPURequest: "100m", CPULimit: "1", MemoryRequest: "128Mi", MemoryLimit: "512Mi"}
	if config != nil {
		values = *config
	}
	requestCPU, e1 := resource.ParseQuantity(values.CPURequest)
	limitCPU, e2 := resource.ParseQuantity(values.CPULimit)
	requestMemory, e3 := resource.ParseQuantity(values.MemoryRequest)
	limitMemory, e4 := resource.ParseQuantity(values.MemoryLimit)
	if errors.Join(e1, e2, e3, e4) != nil || requestCPU.Sign() <= 0 || requestMemory.Sign() <= 0 || requestCPU.Cmp(limitCPU) > 0 || requestMemory.Cmp(limitMemory) > 0 ||
		limitCPU.Cmp(resource.MustParse("8")) > 0 || limitMemory.Cmp(resource.MustParse("16Gi")) > 0 {
		return corev1.ResourceRequirements{}, errors.New("executor resources must be positive requests within limits of 8 CPU and 16Gi memory")
	}
	return corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: requestCPU, corev1.ResourceMemory: requestMemory}, Limits: corev1.ResourceList{corev1.ResourceCPU: limitCPU, corev1.ResourceMemory: limitMemory}}, nil
}

// ApplyProfile copies frozen fields into a fresh Pod. No profile lookup occurs here.
func ApplyProfile(pod *corev1.Pod, profile *agentsv1alpha1.SelfHostedEnvironment, sessionUID string) error {
	if profile == nil {
		return nil
	}
	resources, err := ProfileResources(profile.Resources)
	if err != nil {
		return err
	}
	pod.Spec.Containers[0].Resources = resources
	pod.Spec.NodeSelector = map[string]string{"kubernetes.io/arch": profile.Architecture}
	size, err := resource.ParseQuantity(profile.WorkspaceSize)
	if err != nil {
		return errors.New("invalid frozen workspace size")
	}
	if profile.WorkspaceStorage != nil && profile.WorkspaceStorage.Type == WorkspaceStoragePersistent {
		if sessionUID == "" || profile.WorkspaceStorage.StorageClassName == "" {
			return errors.New("persistent workspace claim configuration is incomplete")
		}
		pod.Spec.Volumes[0].VolumeSource = corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: WorkspaceClaimNameForUID(sessionUID)}}
	} else {
		pod.Spec.Volumes[0].EmptyDir.SizeLimit = &size
	}
	if len(profile.Capabilities) == 0 {
		return nil
	}
	pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{Name: capabilityVolumeName, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: new(resource.MustParse("1Gi"))}}})
	pod.Spec.Containers[0].VolumeMounts = append(pod.Spec.Containers[0].VolumeMounts, corev1.VolumeMount{Name: capabilityVolumeName, MountPath: capabilityMountPath, ReadOnly: true})
	for _, capability := range profile.Capabilities {
		// All interpolated path components are constrained by admission and validation.
		check := "test -d " + capability.Path + "/skills"
		if capability.Type == "plugin" {
			check = "test -f " + capability.Path + "/.codex-plugin/plugin.json"
		}
		command := "set -eu; mkdir -p " + capability.Path + "; cp -R /bundle/. " + capability.Path + "/; " + check
		pod.Spec.InitContainers = append(pod.Spec.InitContainers, corev1.Container{Name: "capability-" + capability.Name, Image: capability.Image, ImagePullPolicy: corev1.PullIfNotPresent,
			Command: []string{"sh", "-c", command}, SecurityContext: pod.Spec.Containers[0].SecurityContext.DeepCopy(), Resources: resources,
			VolumeMounts: []corev1.VolumeMount{{Name: capabilityVolumeName, MountPath: capabilityMountPath}}})
	}
	return nil
}
