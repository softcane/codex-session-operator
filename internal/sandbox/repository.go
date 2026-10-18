package sandbox

import (
	"errors"
	"slices"
	"strings"
	"unicode/utf8"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	"github.com/softcane/codex-session-operator/internal/repository"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/validation"
)

const (
	RepositorySecretName           = "codex-repository"
	RepositoryInitContainerName    = "repository-prepare"
	_repositoryStorageRoot         = "/workspace-storage"
	_repositoryCredentialDirectory = "/repository-credentials"
	_repositoryCredentialVolume    = "repository-credentials"
	_repositoryTemporaryMount      = "/tmp"
	_repositoryTemporaryVolume     = "tmp"
	_repositoryWorkspaceVolume     = "workspace"
)

// ValidateRepository restricts repository and credential selection to the typed contract.
func ValidateRepository(spec *agentsv1alpha1.RepositorySpec) error {
	if spec == nil {
		return nil
	}
	if err := repository.Validate(spec.URL, spec.Commit); err != nil {
		return err
	}
	if spec.MountCredentialsInExecutor && spec.CredentialSecretRef == nil {
		return errors.New("executor repository credential mounting requires a protected credential reference")
	}
	if ref := spec.CredentialSecretRef; ref != nil {
		if ref.Name != RepositorySecretName || len(validation.IsConfigMapKey(ref.Key)) != 0 || ref.Key == "" ||
			(ref.UsernameKey != "" && (len(validation.IsConfigMapKey(ref.UsernameKey)) != 0 || ref.UsernameKey == ref.Key)) {
			return errors.New("repository credential reference must select distinct keys in codex-repository")
		}
	}
	return nil
}

// ValidateRepositoryApproval checks exact public URL approval independently of image approval.
func ValidateRepositoryApproval(spec *agentsv1alpha1.RepositorySpec, approvedURLs []string) error {
	if err := ValidateRepository(spec); err != nil {
		return err
	}
	if spec != nil && !slices.Contains(approvedURLs, spec.URL) {
		return errors.New("repository URL is not approved by platform configuration")
	}
	return nil
}

// RepositoryDigest excludes credentials and their source versions from checkout identity.
func RepositoryDigest(spec *agentsv1alpha1.RepositorySpec) string {
	if spec == nil {
		return ""
	}
	return repository.Digest(spec.URL, spec.Commit)
}

// RepositoryCredentialSource validates protected material without returning its values.
func RepositoryCredentialSource(spec *agentsv1alpha1.RepositorySpec, secret *corev1.Secret) (*agentsv1alpha1.EnvironmentSourceVersion, error) {
	if err := ValidateRepository(spec); err != nil {
		return nil, err
	}
	if spec == nil || spec.CredentialSecretRef == nil {
		return nil, nil
	}
	if secret == nil || secret.Name != RepositorySecretName || secret.Type != corev1.SecretTypeOpaque ||
		!secret.DeletionTimestamp.IsZero() || secret.UID == "" || secret.ResourceVersion == "" {
		return nil, errors.New("protected repository credential source is unavailable")
	}
	ref := spec.CredentialSecretRef
	for _, key := range []string{ref.Key, ref.UsernameKey} {
		if key == "" {
			continue
		}
		value := string(secret.Data[key])
		if len(value) == 0 || len(value) > 65536 || !utf8.ValidString(value) || strings.ContainsAny(value, "\r\n\x00") {
			return nil, errors.New("protected repository credential source is absent or invalid")
		}
	}
	return &agentsv1alpha1.EnvironmentSourceVersion{UID: string(secret.UID), ResourceVersion: secret.ResourceVersion}, nil
}

func applyRepository(pod *corev1.Pod, spec *agentsv1alpha1.RepositorySpec, sessionUID string) error {
	if spec == nil {
		return nil
	}
	if err := ValidateRepository(spec); err != nil {
		return err
	}
	if sessionUID == "" {
		return errors.New("repository preparation requires a Session UID")
	}
	executor := &pod.Spec.Containers[0]
	executor.VolumeMounts[0].SubPath = repository.ProjectDirectory
	container := corev1.Container{Name: RepositoryInitContainerName, Image: executor.Image,
		ImagePullPolicy: corev1.PullIfNotPresent, Command: []string{"/workspace-bootstrap"},
		Args:            []string{"--storage-root", _repositoryStorageRoot, "--session-uid", sessionUID, "--url", spec.URL, "--commit", spec.Commit},
		Env:             []corev1.EnvVar{{Name: "TMPDIR", Value: _repositoryTemporaryMount}},
		SecurityContext: executor.SecurityContext.DeepCopy(), Resources: *executor.Resources.DeepCopy(),
		VolumeMounts: []corev1.VolumeMount{{Name: _repositoryWorkspaceVolume, MountPath: _repositoryStorageRoot}, {Name: _repositoryTemporaryVolume, MountPath: _repositoryTemporaryMount}},
	}
	if ref := spec.CredentialSecretRef; ref != nil {
		items := []corev1.KeyToPath{{Key: ref.Key, Path: "token"}}
		if ref.UsernameKey != "" {
			items = append(items, corev1.KeyToPath{Key: ref.UsernameKey, Path: "username"})
		}
		pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{Name: _repositoryCredentialVolume, VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
			SecretName: RepositorySecretName, DefaultMode: new(int32(0o440)), Items: items, Optional: new(true),
		}}})
		container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{Name: _repositoryCredentialVolume, MountPath: _repositoryCredentialDirectory, ReadOnly: true})
		container.Args = append(container.Args, "--credential-directory", _repositoryCredentialDirectory)
		if spec.MountCredentialsInExecutor {
			executor.VolumeMounts = append(executor.VolumeMounts, corev1.VolumeMount{
				Name: _repositoryCredentialVolume, MountPath: _repositoryCredentialDirectory, ReadOnly: true,
			})
			executor.Env = append(executor.Env,
				corev1.EnvVar{Name: "GIT_ASKPASS", Value: "/workspace-bootstrap"},
				corev1.EnvVar{Name: "GIT_TERMINAL_PROMPT", Value: "0"},
				corev1.EnvVar{Name: "WORKSPACE_BOOTSTRAP_CREDENTIAL_DIRECTORY", Value: _repositoryCredentialDirectory},
				corev1.EnvVar{Name: "WORKSPACE_BOOTSTRAP_REPOSITORY_URL", Value: spec.URL},
			)
		}
	}
	pod.Spec.InitContainers = append(pod.Spec.InitContainers, container)
	return nil
}

// InitContainersPrepared requires successful observations of every required init container.
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
