package sandbox

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const _repositoryArchitecture = "arm64"

const _repositorySelfHostedMode = "selfHosted"

const _repositoryTokenKey = "token"

const _repositoryURL = "https://github.com/fixture/project.git"

const _repositoryTemporaryEnvName = "TMPDIR"

func TestRepositoryPodIsolationAndPreparedObservations(t *testing.T) {
	repo := &agentsv1alpha1.RepositorySpec{URL: _repositoryURL, Commit: strings.Repeat("a", 40),
		CredentialSecretRef: &agentsv1alpha1.RepositoryCredentialSecretReference{Name: RepositorySecretName, Key: "access-token", UsernameKey: "git-user"}}
	image := "example.com/executor@sha256:" + strings.Repeat("a", 64)
	session := &agentsv1alpha1.Session{ObjectMeta: metav1.ObjectMeta{Namespace: "repository-tenant", UID: "repository-session"},
		Status: agentsv1alpha1.SessionStatus{ExecutorImage: image, EnvironmentSnapshot: &agentsv1alpha1.SessionEnvironmentSnapshot{
			Profile: agentsv1alpha1.EnvironmentSpec{Mode: _repositorySelfHostedMode, SelfHosted: &agentsv1alpha1.SelfHostedEnvironment{
				Image: image, Architecture: _repositoryArchitecture, WorkspaceDirectory: defaultWorkspaceDirectory, WorkspaceSize: testWorkspaceSize, Repository: repo,
				WorkspaceStorage: &agentsv1alpha1.WorkspaceStorage{Type: WorkspaceStoragePersistent, StorageClassName: approvedStorageClass, ClaimDeletionPolicy: WorkspaceClaimDelete},
			}},
		}}}
	pod, err := Pod(session, defaultWorkspaceDirectory, "digest")
	if err != nil {
		t.Fatal(err)
	}
	if len(pod.Spec.InitContainers) != 1 {
		t.Fatal("repository has no required init container")
	}
	init := pod.Spec.InitContainers[0]
	if init.Name != RepositoryInitContainerName || init.Image != image || len(init.Env) != 1 || len(init.EnvFrom) != 0 ||
		init.Command[0] != "/workspace-bootstrap" || init.SecurityContext.ReadOnlyRootFilesystem == nil || !*init.SecurityContext.ReadOnlyRootFilesystem ||
		pod.Spec.Containers[0].VolumeMounts[0].SubPath != "project" || !init.VolumeMounts[2].ReadOnly {
		t.Fatal("preparation image, credential or filesystem boundary changed")
	}
	if init.Env[0].Name != _repositoryTemporaryEnvName || init.Env[0].Value != _repositoryTemporaryMount {
		t.Fatal("repository preparation did not select its approved writable temporary mount")
	}
	for _, mount := range pod.Spec.Containers[0].VolumeMounts {
		if mount.Name == _repositoryCredentialVolume {
			t.Fatal("executor can access repository credentials")
		}
	}
	if InitContainersPrepared(pod) {
		t.Fatal("unobserved init was accepted")
	}
	pod.Status.InitContainerStatuses = []corev1.ContainerStatus{{Name: RepositoryInitContainerName,
		State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 1}}}}
	if InitContainersPrepared(pod) {
		t.Fatal("failed init was accepted")
	}
	pod.Status.InitContainerStatuses[0].State.Terminated.ExitCode = 0
	if !InitContainersPrepared(pod) {
		t.Fatal("successful required init was not accepted")
	}
	pod.Status.InitContainerStatuses[0].Name = "foreign"
	if InitContainersPrepared(pod) {
		t.Fatal("foreign init status was accepted")
	}
	if !ExecutorMatches(pod, pod) {
		t.Fatal("desired owned Pod did not match")
	}
	mutated := pod.DeepCopy()
	mutated.Spec.InitContainers[0].Env = []corev1.EnvVar{{Name: "FOREIGN", Value: "true"}}
	if ExecutorMatches(mutated, pod) {
		t.Fatal("unchanged digest annotation hid mutated init configuration")
	}
}

func TestRepositoryPreparationTemporaryDirectoryDoesNotOverrideTaskEnvironment(t *testing.T) {
	tests := []struct {
		name string
		env  []corev1.EnvVar
	}{
		{name: "image defaults"},
		{name: "task workspace temporary directory", env: []corev1.EnvVar{{Name: _repositoryTemporaryEnvName, Value: "/workspace/.tmp"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pod := &corev1.Pod{Spec: corev1.PodSpec{Containers: []corev1.Container{{
				Image: "example.com/task@sha256:" + strings.Repeat("a", 64), Env: slices.Clone(tt.env),
				VolumeMounts: []corev1.VolumeMount{{Name: _repositoryWorkspaceVolume, MountPath: "/workspace"}},
			}}}}
			repository := &agentsv1alpha1.RepositorySpec{URL: _repositoryURL, Commit: strings.Repeat("a", 40)}
			if err := applyRepository(pod, repository, "temporary-directory-session"); err != nil {
				t.Fatal(err)
			}
			init := pod.Spec.InitContainers[0]
			if len(init.Env) != 1 || init.Env[0].Name != _repositoryTemporaryEnvName || init.Env[0].Value != _repositoryTemporaryMount {
				t.Fatal("repository preparation inherited an image-specific task temporary directory")
			}
			mounted := slices.ContainsFunc(init.VolumeMounts, func(mount corev1.VolumeMount) bool {
				return mount.Name == _repositoryTemporaryVolume && mount.MountPath == init.Env[0].Value && !mount.ReadOnly
			})
			if !mounted {
				t.Fatal("repository temporary directory is not backed by its writable approved mount")
			}
			if !slices.Equal(pod.Spec.Containers[0].Env, tt.env) {
				t.Fatal("repository preparation overrode the executor task environment")
			}
		})
	}
}

func TestRepositoryApprovalSecretValidationAndSourceIndependence(t *testing.T) {
	spec := &agentsv1alpha1.RepositorySpec{URL: _repositoryURL, Commit: strings.Repeat("a", 40),
		CredentialSecretRef: &agentsv1alpha1.RepositoryCredentialSecretReference{Name: RepositorySecretName, Key: _repositoryTokenKey}}
	if ValidateRepositoryApproval(spec, nil) == nil || ValidateRepositoryApproval(spec, []string{"*"}) == nil || ValidateRepositoryApproval(spec, []string{_repositoryURL}) != nil {
		t.Fatal("repository approval did not require an exact URL")
	}
	const private = "private-source-value"
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: RepositorySecretName, UID: "source-uid", ResourceVersion: "1"},
		Type: corev1.SecretTypeOpaque, Data: map[string][]byte{_repositoryTokenKey: []byte(private)}}
	source, err := RepositoryCredentialSource(spec, secret)
	if err != nil || source.UID != "source-uid" || source.ResourceVersion != "1" {
		t.Fatal("protected source version was not validated", err)
	}
	encoded, err := json.Marshal(source)
	if err != nil || strings.Contains(string(encoded), private) {
		t.Fatal("protected contents entered public provenance")
	}
	digest := RepositoryDigest(spec)
	spec.CredentialSecretRef.Key = "rotated-token"
	if RepositoryDigest(spec) != digest {
		t.Fatal("credential rotation changed checkout identity")
	}
	for _, name := range []string{"foreign Secret", "newline", "missing key", "same username key"} {
		t.Run(name, func(t *testing.T) {
			copySpec, copySecret := spec.DeepCopy(), secret.DeepCopy()
			copySpec.CredentialSecretRef.Key = "token"
			switch name {
			case "foreign Secret":
				copySpec.CredentialSecretRef.Name = "unrelated"
			case "newline":
				copySecret.Data[_repositoryTokenKey] = []byte(private + "\n")
			case "missing key":
				copySecret.Data = nil
			case "same username key":
				copySpec.CredentialSecretRef.UsernameKey = _repositoryTokenKey
			}
			if _, err := RepositoryCredentialSource(copySpec, copySecret); err == nil || strings.Contains(err.Error(), private) {
				t.Fatal("unsafe source was accepted or leaked")
			}
		})
	}
}

func TestRepositoryExecutorCredentialMountRequiresExplicitOptIn(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "preparation only", true: "executor opted in"}[enabled], func(t *testing.T) {
			image := "example.com/executor@sha256:" + strings.Repeat("a", 64)
			repository := &agentsv1alpha1.RepositorySpec{
				URL: _repositoryURL, Commit: strings.Repeat("a", 40), MountCredentialsInExecutor: enabled,
				CredentialSecretRef: &agentsv1alpha1.RepositoryCredentialSecretReference{Name: RepositorySecretName, Key: _repositoryTokenKey},
			}
			session := &agentsv1alpha1.Session{ObjectMeta: metav1.ObjectMeta{UID: "runtime-credentials"},
				Status: agentsv1alpha1.SessionStatus{ExecutorImage: image, EnvironmentSnapshot: &agentsv1alpha1.SessionEnvironmentSnapshot{
					Profile: agentsv1alpha1.EnvironmentSpec{Mode: _repositorySelfHostedMode, SelfHosted: &agentsv1alpha1.SelfHostedEnvironment{
						Image: image, Architecture: _repositoryArchitecture, WorkspaceDirectory: defaultWorkspaceDirectory,
						WorkspaceSize: testWorkspaceSize, Repository: repository,
					}},
				}}}
			pod, err := Pod(session, defaultWorkspaceDirectory, "digest")
			if err != nil {
				t.Fatal(err)
			}
			executor := pod.Spec.Containers[0]
			mounted := false
			for _, mount := range executor.VolumeMounts {
				if mount.Name == _repositoryCredentialVolume {
					mounted = mount.ReadOnly && mount.MountPath == _repositoryCredentialDirectory
				}
			}
			if mounted != enabled {
				t.Fatal("repository credential mount did not match explicit opt-in")
			}
			environment := make(map[string]string, len(executor.Env))
			for _, variable := range executor.Env {
				environment[variable.Name] = variable.Value
			}
			if enabled {
				if environment["GIT_ASKPASS"] != "/workspace-bootstrap" || environment["GIT_TERMINAL_PROMPT"] != "0" ||
					environment["WORKSPACE_BOOTSTRAP_CREDENTIAL_DIRECTORY"] != _repositoryCredentialDirectory ||
					environment["WORKSPACE_BOOTSTRAP_REPOSITORY_URL"] != _repositoryURL {
					t.Fatal("runtime Git did not use scoped file-based authentication")
				}
			} else if _, exists := environment["GIT_ASKPASS"]; exists {
				t.Fatal("preparation-only executor received repository authentication configuration")
			}
		})
	}
	if ValidateRepository(&agentsv1alpha1.RepositorySpec{
		URL: _repositoryURL, Commit: strings.Repeat("a", 40), MountCredentialsInExecutor: true,
	}) == nil {
		t.Fatal("executor credential access without a protected source was accepted")
	}
}
