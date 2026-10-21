package sandbox

import (
	"errors"
	"slices"

	corev1 "k8s.io/api/core/v1"
	extensionsv1beta1 "sigs.k8s.io/agent-sandbox/extensions/api/v1beta1"
)

const RepositorySecretName = "codex-repository"

// SecretNames identifies protected sources without retrieving their values.
func SecretNames(template *extensionsv1beta1.SandboxTemplate) []string {
	names := make(map[string]struct{})
	if template == nil {
		return nil
	}
	pod := &template.Spec.PodTemplate.Spec
	for _, container := range append(slices.Clone(pod.Containers), pod.InitContainers...) {
		for _, env := range container.Env {
			if env.ValueFrom != nil && env.ValueFrom.SecretKeyRef != nil {
				names[env.ValueFrom.SecretKeyRef.Name] = struct{}{}
			}
		}
	}
	for _, volume := range pod.Volumes {
		if volume.Secret != nil {
			names[volume.Secret.SecretName] = struct{}{}
		}
		if volume.Projected != nil {
			for _, source := range volume.Projected.Sources {
				if source.Secret != nil {
					names[source.Secret.Name] = struct{}{}
				}
			}
		}
	}
	result := make([]string, 0, len(names))
	for name := range names {
		result = append(result, name)
	}
	slices.Sort(result)
	return result
}

func validateSecretReferences(template *extensionsv1beta1.SandboxTemplate) error {
	for _, name := range SecretNames(template) {
		if name != ExecutorSecretName && name != RepositorySecretName {
			return errors.New("sandbox template Secret references must select codex-executor or codex-repository")
		}
	}
	pod := &template.Spec.PodTemplate.Spec
	if len(pod.ImagePullSecrets) != 0 {
		return errors.New("sandbox template cannot attach untracked image-pull credentials")
	}
	for _, volume := range pod.Volumes {
		if volume.EmptyDir == nil && volume.Secret == nil && volume.DownwardAPI == nil && volume.Projected == nil {
			return errors.New("sandbox template volumes must use native workspace storage, emptyDir, tracked Secrets or downward API")
		}
		if volume.Projected != nil {
			for _, source := range volume.Projected.Sources {
				if source.Secret == nil && source.DownwardAPI == nil {
					return errors.New("sandbox projected sources must use tracked Secrets or downward API")
				}
			}
		}
	}
	return validateExecutorSecretIsolation(pod)
}

func validateExecutorSecretIsolation(pod *corev1.PodSpec) error {
	executorVolumes := make(map[string]struct{})
	for _, volume := range pod.Volumes {
		hasExecutorSecret := volume.Secret != nil && volume.Secret.SecretName == ExecutorSecretName
		if volume.Projected != nil {
			for _, source := range volume.Projected.Sources {
				if source.Secret != nil && source.Secret.Name == ExecutorSecretName {
					hasExecutorSecret = true
				}
			}
		}
		if hasExecutorSecret {
			executorVolumes[volume.Name] = struct{}{}
		}
	}
	for _, container := range pod.InitContainers {
		for _, env := range container.Env {
			if env.ValueFrom != nil && env.ValueFrom.SecretKeyRef != nil && env.ValueFrom.SecretKeyRef.Name == ExecutorSecretName {
				return errors.New("init containers cannot read the restricted executor credential")
			}
		}
		for _, mount := range container.VolumeMounts {
			if _, exists := executorVolumes[mount.Name]; exists {
				return errors.New("init containers cannot mount the restricted executor credential")
			}
		}
	}
	return nil
}

// SecretKeys identifies selected keys so callers can validate their availability.
// Complete Secret mounts add no individual key requirements.
func SecretKeys(template *extensionsv1beta1.SandboxTemplate) map[string][]string {
	keys := make(map[string][]string)
	if template == nil {
		return keys
	}
	for _, container := range append(slices.Clone(template.Spec.PodTemplate.Spec.Containers), template.Spec.PodTemplate.Spec.InitContainers...) {
		for _, env := range container.Env {
			if env.ValueFrom != nil && env.ValueFrom.SecretKeyRef != nil {
				ref := env.ValueFrom.SecretKeyRef
				keys[ref.Name] = append(keys[ref.Name], ref.Key)
			}
		}
	}
	add := func(name string, items []corev1.KeyToPath) {
		for _, item := range items {
			keys[name] = append(keys[name], item.Key)
		}
		if _, ok := keys[name]; !ok {
			keys[name] = nil
		}
	}
	for _, volume := range template.Spec.PodTemplate.Spec.Volumes {
		if volume.Secret != nil {
			add(volume.Secret.SecretName, volume.Secret.Items)
		}
		if volume.Projected != nil {
			for _, source := range volume.Projected.Sources {
				if source.Secret != nil {
					add(source.Secret.Name, source.Secret.Items)
				}
			}
		}
	}
	return keys
}
