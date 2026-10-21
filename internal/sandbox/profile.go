package sandbox

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"regexp"
	"slices"
	"strings"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/utils/ptr"
	extensionsv1beta1 "sigs.k8s.io/agent-sandbox/extensions/api/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const ManagedSetupLabel = "agents.softcane.io/managed"

var _imageDigest = regexp.MustCompile(`^[^\s@]+@sha256:[a-f0-9]{64}$`)

// ValidateProfile validates the operator's small reference-based self-hosted API.
func ValidateProfile(profile *agentsv1alpha1.SelfHostedEnvironment) error {
	if profile == nil {
		return errors.New("self-hosted profile is missing")
	}
	if profile.SandboxPoolRef.Name == "" || len(validation.IsDNS1123Subdomain(profile.SandboxPoolRef.Name)) != 0 {
		return errors.New("self-hosted environment requires a same-namespace Sandbox pool")
	}
	if err := ValidateWorkspace(profile.Workspace.Directory); err != nil {
		return err
	}
	if profile.Workspace.DeletionPolicy != WorkspaceClaimDelete && profile.Workspace.DeletionPolicy != WorkspaceClaimRetain {
		return errors.New("workspace policy must explicitly select Delete or Retain")
	}
	for _, directory := range profile.CapabilityDirectories {
		if !strings.HasPrefix(directory, "/") || path.Clean(directory) != directory || directory == "/" {
			return errors.New("capability directories must be clean absolute paths other than root")
		}
	}
	return nil
}

// ResolvePool reads native infrastructure authoritatively and returns only safe identities.
// The caller separately enforces the installation's pool-name allowlist.
func ResolvePool(ctx context.Context, reader client.Reader, namespace string, profile *agentsv1alpha1.SelfHostedEnvironment) (*agentsv1alpha1.SandboxPoolSnapshot, error) {
	if err := ValidateProfile(profile); err != nil {
		return nil, err
	}
	var pool extensionsv1beta1.SandboxWarmPool
	if err := reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: profile.SandboxPoolRef.Name}, &pool); err != nil {
		return nil, fmt.Errorf("read Sandbox pool: %w", err)
	}
	var template extensionsv1beta1.SandboxTemplate
	if err := reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: pool.Spec.TemplateRef.Name}, &template); err != nil {
		return nil, fmt.Errorf("read Sandbox template: %w", err)
	}
	if err := ValidatePool(profile, &pool, &template); err != nil {
		return nil, err
	}
	return &agentsv1alpha1.SandboxPoolSnapshot{
		Name: pool.Name, UID: string(pool.UID), Generation: pool.Generation,
		TemplateName: template.Name, TemplateUID: string(template.UID), TemplateGeneration: template.Generation,
		ConfigurationDigest: digest(struct {
			Pool     extensionsv1beta1.SandboxWarmPoolSpec
			Template extensionsv1beta1.SandboxTemplateSpec
		}{Pool: pool.Spec, Template: template.Spec}),
	}, nil
}

// ValidatePool requires on-demand execution and the executor's security and storage contract.
func ValidatePool(profile *agentsv1alpha1.SelfHostedEnvironment, pool *extensionsv1beta1.SandboxWarmPool, template *extensionsv1beta1.SandboxTemplate) error {
	if err := ValidateProfile(profile); err != nil {
		return err
	}
	if pool == nil || template == nil || pool.UID == "" || template.UID == "" || pool.Namespace != template.Namespace ||
		!pool.DeletionTimestamp.IsZero() || !template.DeletionTimestamp.IsZero() || pool.Name != profile.SandboxPoolRef.Name || pool.Spec.TemplateRef.Name != template.Name {
		return errors.New("sandbox pool and template identities are unavailable")
	}
	if pool.Labels[ManagedSetupLabel] != "true" || template.Labels[ManagedSetupLabel] != "true" {
		return errors.New("sandbox pool and template must opt into the managed immutable-configuration policy")
	}
	if pool.Spec.Replicas == nil || *pool.Spec.Replicas != 0 {
		return errors.New("sandbox pool must explicitly set replicas to zero for Session-bound cold starts")
	}
	if template.Spec.EnvVarsInjectionPolicy != extensionsv1beta1.EnvVarsInjectionPolicyAllowed {
		return errors.New("sandbox template must allow new environment variables without allowing overrides")
	}
	pod := &template.Spec.PodTemplate.Spec
	if len(pod.Containers) != 1 || pod.Containers[0].Name != ExecutorContainerName || len(pod.EphemeralContainers) != 0 {
		return errors.New("sandbox template must contain exactly one executor container")
	}
	if err := validatePodSecurity(pod); err != nil {
		return err
	}
	if err := validateSecretReferences(template); err != nil {
		return err
	}
	if template.Spec.Service == nil || *template.Spec.Service {
		return errors.New("sandbox template must disable its inbound service")
	}
	if err := validateExecutorConnection(&pod.Containers[0], profile.Workspace.Directory); err != nil {
		return err
	}
	containers := append(slices.Clone(pod.Containers), pod.InitContainers...)
	for _, container := range containers {
		if err := validateContainer(container); err != nil {
			return err
		}
		if container.Name == RepositoryInitContainerName {
			for _, env := range container.Env {
				if env.Name == "SESSION_UID" {
					return errors.New("repository preparation cannot predefine SESSION_UID")
				}
			}
		}
	}
	return validateWorkspaceTemplate(template, profile.Workspace.Directory)
}

func validatePodSecurity(pod *corev1.PodSpec) error {
	if ptr.Deref(pod.AutomountServiceAccountToken, true) || pod.HostNetwork || pod.HostPID || pod.HostIPC || ptr.Deref(pod.ShareProcessNamespace, false) ||
		(pod.ServiceAccountName != "" && pod.ServiceAccountName != "default") {
		return errors.New("sandbox template cannot expose host namespaces or service-account credentials")
	}
	security := pod.SecurityContext
	if security == nil || len(security.Sysctls) != 0 || !ptr.Deref(security.RunAsNonRoot, false) || ptr.Deref(security.RunAsUser, 0) <= 0 || security.SeccompProfile == nil || security.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault {
		return errors.New("sandbox template requires a non-root user and RuntimeDefault seccomp")
	}
	if pod.RestartPolicy != "" && pod.RestartPolicy != corev1.RestartPolicyAlways {
		return errors.New("executor restart policy must be Always")
	}
	return nil
}

func validateExecutorConnection(executor *corev1.Container, workspace string) error {
	if !slices.Equal(executor.Command, []string{"codex", "exec-server"}) || !slices.Equal(executor.Args, []string{"--remote", "$(CODEX_REMOTE_URL)", "--environment-id", "$(CODEX_ENVIRONMENT_ID)"}) || executor.WorkingDir != workspace {
		return errors.New("sandbox executor must use codex exec-server with injected connection settings and the selected working directory")
	}
	credential := false
	for _, env := range executor.Env {
		if slices.Contains([]string{"CODEX_ENVIRONMENT_ID", "CODEX_REMOTE_URL", "CODEX_SESSION_UID"}, env.Name) {
			return errors.New("sandbox template cannot predefine Session-specific environment variables")
		}
		if env.Name == ExecutorSecretKey {
			ref := env.ValueFrom
			credential = env.Value == "" && ref != nil && ref.SecretKeyRef != nil && ref.SecretKeyRef.Name == ExecutorSecretName && ref.SecretKeyRef.Key == ExecutorSecretKey && !ptr.Deref(ref.SecretKeyRef.Optional, false)
		}
	}
	if !credential {
		return errors.New("sandbox executor requires the restricted codex-executor credential reference")
	}
	return nil
}

func validateWorkspaceTemplate(template *extensionsv1beta1.SandboxTemplate, workspace string) error {
	pod := &template.Spec.PodTemplate.Spec
	if len(template.Spec.VolumeClaimTemplates) != 1 || template.Spec.VolumeClaimTemplates[0].Name != "workspace" {
		return errors.New("sandbox template requires exactly one native workspace volume claim template")
	}
	storage := &template.Spec.VolumeClaimTemplates[0].Spec
	size := storage.Resources.Requests[corev1.ResourceStorage]
	if storage.StorageClassName == nil || *storage.StorageClassName == "" || len(validation.IsDNS1123Subdomain(*storage.StorageClassName)) != 0 ||
		size.Sign() <= 0 || size.Cmp(resource.MustParse("16Gi")) > 0 || len(storage.AccessModes) != 1 || storage.AccessModes[0] != corev1.ReadWriteOnce || storage.Selector != nil || storage.VolumeName != "" || storage.DataSource != nil || storage.DataSourceRef != nil || (storage.VolumeMode != nil && *storage.VolumeMode != corev1.PersistentVolumeFilesystem) {
		return errors.New("workspace template requires a fresh ReadWriteOnce claim with an explicit class and at most 16Gi")
	}
	mounted := false
	for _, mount := range pod.Containers[0].VolumeMounts {
		if mount.MountPath == workspace {
			mounted = mount.Name == "workspace" && !mount.ReadOnly && mount.SubPathExpr == ""
		}
	}
	if !mounted {
		return errors.New("executor must mount its native workspace claim at the selected working directory")
	}
	for _, volume := range pod.Volumes {
		if volume.HostPath != nil || volume.PersistentVolumeClaim != nil {
			return errors.New("template volumes cannot reference host paths or existing workspace claims")
		}
	}
	return nil
}

func validateContainer(container corev1.Container) error {
	if err := ValidateImage(container.Image); err != nil {
		return err
	}
	if len(container.EnvFrom) != 0 {
		return errors.New("sandbox containers cannot use envFrom with Session injection")
	}
	seen := make(map[string]struct{}, len(container.Env))
	for _, env := range container.Env {
		if _, exists := seen[env.Name]; exists {
			return errors.New("sandbox container environment names must be unique")
		}
		seen[env.Name] = struct{}{}
		if env.ValueFrom != nil && env.ValueFrom.ConfigMapKeyRef != nil {
			return errors.New("sandbox containers cannot use untracked ConfigMap values")
		}
	}
	security := container.SecurityContext
	if security == nil || ptr.Deref(security.AllowPrivilegeEscalation, true) || ptr.Deref(security.Privileged, false) || !ptr.Deref(security.ReadOnlyRootFilesystem, false) ||
		security.Capabilities == nil || len(security.Capabilities.Add) != 0 || !slices.Contains(security.Capabilities.Drop, corev1.Capability("ALL")) {
		return errors.New("sandbox containers require read-only roots, dropped capabilities and no privilege escalation")
	}
	if security.ProcMount != nil && *security.ProcMount != corev1.DefaultProcMount {
		return errors.New("sandbox containers cannot expose unmasked proc")
	}
	if security.SeccompProfile != nil && security.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault {
		return errors.New("sandbox container seccomp cannot weaken RuntimeDefault")
	}
	if ptr.Deref(security.RunAsUser, 1000) <= 0 || !ptr.Deref(security.RunAsNonRoot, true) {
		return errors.New("sandbox containers cannot override the non-root user")
	}
	requests, limits := container.Resources.Requests, container.Resources.Limits
	for _, name := range []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory} {
		request, limit := requests[name], limits[name]
		if request.Sign() <= 0 || request.Cmp(limit) > 0 {
			return errors.New("sandbox containers require positive CPU and memory requests within explicit limits")
		}
	}
	cpu, memory := limits[corev1.ResourceCPU], limits[corev1.ResourceMemory]
	if cpu.Cmp(resource.MustParse("8")) > 0 || memory.Cmp(resource.MustParse("16Gi")) > 0 {
		return errors.New("sandbox container limits exceed 8 CPU or 16Gi memory")
	}
	return nil
}

// ValidateImage requires an immutable image reference. Pool approval is separate.
func ValidateImage(image string) error {
	if !_imageDigest.MatchString(image) {
		return errors.New("sandbox images must be pinned by sha256 digest")
	}
	return nil
}

// ValidateWorkspace excludes platform/runtime directories.
func ValidateWorkspace(workspace string) error {
	if !strings.HasPrefix(workspace, "/") || path.Clean(workspace) != workspace || workspace == "/" {
		return errors.New("executor workspace must be a clean absolute directory other than root")
	}
	for _, reserved := range []string{"/home", "/tmp", "/usr", "/bin", "/sbin", "/etc", "/dev", "/proc", "/sys", "/run", "/capabilities"} {
		if workspace == reserved || strings.HasPrefix(workspace, reserved+"/") {
			return errors.New("executor workspace overlaps a reserved directory")
		}
	}
	return nil
}

func digest(value any) string {
	raw, _ := json.Marshal(value)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
