package sandbox

import (
	"errors"
	"fmt"
	"hash/fnv"
	"maps"
	"reflect"
	"slices"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	extensionsv1beta1 "sigs.k8s.io/agent-sandbox/extensions/api/v1beta1"
)

const (
	ExecutorContainerName = "executor"
	ExecutorSecretName    = "codex-executor"
	ExecutorSecretKey     = "CODEX_API_KEY"
	ConfigAnnotation      = "agents.softcane.io/executor-config"
	PodSessionUIDLabel    = "sandbox.users.io/session-uid"
)

// ClaimName is stable across retries and distinct for every Session UID.
func ClaimName(session *agentsv1alpha1.Session) string { return "executor-" + string(session.UID) }

// Claim requests a cold-started native Sandbox; it never constructs a Pod.
func Claim(session *agentsv1alpha1.Session, template *extensionsv1beta1.SandboxTemplate, config string) (*extensionsv1beta1.SandboxClaim, error) {
	snapshot := session.Status.EnvironmentSnapshot
	if snapshot == nil || snapshot.Profile.SelfHosted == nil || snapshot.SandboxPool == nil || template == nil ||
		session.UID == "" || session.Status.EnvironmentID == "" || session.Status.RemoteURL == "" || config == "" {
		return nil, errors.New("session sandbox claim requires frozen configuration and remote connection identities")
	}
	if snapshot.SandboxPool.TemplateName != template.Name || snapshot.SandboxPool.TemplateUID != string(template.UID) || snapshot.SandboxPool.TemplateGeneration != template.Generation {
		return nil, errors.New("sandbox template differs from the frozen Session identity")
	}
	env := []extensionsv1beta1.EnvVar{
		{Name: "CODEX_ENVIRONMENT_ID", Value: session.Status.EnvironmentID, ContainerName: ExecutorContainerName},
		{Name: "CODEX_REMOTE_URL", Value: session.Status.RemoteURL, ContainerName: ExecutorContainerName},
		{Name: "CODEX_SESSION_UID", Value: string(session.UID), ContainerName: ExecutorContainerName},
	}
	for _, container := range template.Spec.PodTemplate.Spec.InitContainers {
		if container.Name == RepositoryInitContainerName {
			env = append(env, extensionsv1beta1.EnvVar{Name: "SESSION_UID", Value: string(session.UID), ContainerName: RepositoryInitContainerName})
		}
	}
	return &extensionsv1beta1.SandboxClaim{
		ObjectMeta: metav1.ObjectMeta{Name: ClaimName(session), Namespace: session.Namespace,
			Annotations:     map[string]string{ConfigAnnotation: config},
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(session, agentsv1alpha1.SchemeGroupVersion.WithKind("Session"))}},
		Spec: extensionsv1beta1.SandboxClaimSpec{
			WarmPoolRef:           extensionsv1beta1.SandboxWarmPoolRef{Name: snapshot.SandboxPool.Name},
			AdditionalPodMetadata: sandboxv1beta1.PodMetadata{Labels: map[string]string{PodSessionUIDLabel: string(session.UID)}, Annotations: map[string]string{ConfigAnnotation: config}},
			Env:                   env,
		},
	}, nil
}

// OwnsExecutor verifies the frozen Session -> Claim -> Sandbox -> Pod ownership chain.
func OwnsExecutor(session *agentsv1alpha1.Session, claim *extensionsv1beta1.SandboxClaim, runtime *sandboxv1beta1.Sandbox, pod *corev1.Pod) bool {
	if session == nil || claim == nil || runtime == nil || pod == nil {
		return false
	}
	claimOwned := claim.Namespace == session.Namespace && claim.Name == ClaimName(session) &&
		session.Status.SandboxClaimName == claim.Name && session.Status.SandboxClaimUID != "" &&
		session.Status.SandboxClaimUID == string(claim.UID) && metav1.IsControlledBy(claim, session) && claim.DeletionTimestamp.IsZero()
	runtimeOwned := runtime.Namespace == claim.Namespace && runtime.Name == claim.Name && claim.Status.SandboxStatus.Name == runtime.Name &&
		session.Status.SandboxName == runtime.Name && session.Status.SandboxUID != "" && session.Status.SandboxUID == string(runtime.UID) &&
		metav1.IsControlledBy(runtime, claim) && runtime.DeletionTimestamp.IsZero() && runtime.Spec.OperatingMode == sandboxv1beta1.SandboxOperatingModeRunning
	podOwned := pod.Namespace == runtime.Namespace && pod.Name == runtime.Name && metav1.IsControlledBy(pod, runtime) && pod.DeletionTimestamp.IsZero()
	return claimOwned && runtimeOwned && podOwned
}

// RuntimeMatches rejects changes to the claim or the template-derived execution configuration.
// Live pool identity validation is separately performed by ResolvePool and the frozen snapshot.
func RuntimeMatches(session *agentsv1alpha1.Session, claim *extensionsv1beta1.SandboxClaim, runtime *sandboxv1beta1.Sandbox, template *extensionsv1beta1.SandboxTemplate) bool {
	if session.Status.EnvironmentSnapshot == nil || session.Status.EnvironmentSnapshot.SandboxPool == nil || claim == nil || runtime == nil || template == nil {
		return false
	}
	expectedClaim, err := Claim(session, template, claim.Annotations[ConfigAnnotation])
	if err != nil || !apiequality.Semantic.DeepEqual(expectedClaim.Spec.Env, claim.Spec.Env) || expectedClaim.Spec.WarmPoolRef != claim.Spec.WarmPoolRef ||
		!maps.Equal(expectedClaim.Spec.AdditionalPodMetadata.Labels, claim.Spec.AdditionalPodMetadata.Labels) ||
		len(claim.Spec.VolumeClaimTemplates) != 0 || claim.Spec.Lifecycle != nil {
		return false
	}
	expected := ExpectedBlueprint(template, claim)
	if !apiequality.Semantic.DeepEqual(expected.PodTemplate.Spec, runtime.Spec.PodTemplate.Spec) ||
		!apiequality.Semantic.DeepEqual(expected.VolumeClaimTemplates, runtime.Spec.VolumeClaimTemplates) ||
		!reflect.DeepEqual(expected.Service, runtime.Spec.Service) {
		return false
	}
	if !maps.Equal(expected.PodTemplate.ObjectMeta.Labels, runtime.Spec.PodTemplate.ObjectMeta.Labels) {
		return false
	}
	for key, value := range expected.PodTemplate.ObjectMeta.Annotations {
		if key == ConfigAnnotation {
			continue
		}
		if runtime.Spec.PodTemplate.ObjectMeta.Annotations[key] != value {
			return false
		}
	}
	return runtime.Spec.ShutdownTime == nil && (runtime.Spec.ShutdownPolicy == nil || *runtime.Spec.ShutdownPolicy == sandboxv1beta1.ShutdownPolicyRetain)
}

// ExpectedBlueprint mirrors upstream claim injection and secure defaults for verification.
// It does not create compute resources.
func ExpectedBlueprint(template *extensionsv1beta1.SandboxTemplate, claim *extensionsv1beta1.SandboxClaim) *sandboxv1beta1.SandboxBlueprint {
	if template == nil || claim == nil {
		return nil
	}
	expected := template.Spec.SandboxBlueprint.DeepCopy()
	injectClaimEnv(&expected.PodTemplate.Spec, claim.Spec.Env)
	secureDefaults(template, &expected.PodTemplate.Spec)
	labels := expected.PodTemplate.ObjectMeta.Labels
	if labels == nil {
		labels = make(map[string]string)
	}
	labels[extensionsv1beta1.SandboxIDLabel] = string(claim.UID)
	if value := claim.Labels[sandboxv1beta1.CreatedByLabel]; value != "" {
		labels[sandboxv1beta1.CreatedByLabel] = value
	} else {
		delete(labels, sandboxv1beta1.CreatedByLabel)
	}
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(template.Name))
	labels[sandboxv1beta1.SandboxTemplateRefHashLabel] = fmt.Sprintf("%08x", hash.Sum32())
	maps.Copy(labels, claim.Spec.AdditionalPodMetadata.Labels)
	expected.PodTemplate.ObjectMeta.Labels = labels
	annotations := expected.PodTemplate.ObjectMeta.Annotations
	if annotations == nil {
		annotations = make(map[string]string)
	}
	maps.Copy(annotations, claim.Spec.AdditionalPodMetadata.Annotations)
	expected.PodTemplate.ObjectMeta.Annotations = annotations
	return expected
}

// ConfigDigest binds preparation evidence to one Sandbox and its complete blueprint.
func ConfigDigest(runtime *sandboxv1beta1.Sandbox) string {
	return digest(struct {
		UID       string
		Blueprint sandboxv1beta1.SandboxBlueprint
	}{UID: string(runtime.UID), Blueprint: runtime.Spec.SandboxBlueprint})
}

// ExpectedPod materializes Agent Sandbox's native volume names for drift verification.
// Creation and replacement of the real Pod remain entirely upstream-owned.
func ExpectedPod(runtime *sandboxv1beta1.Sandbox) (*corev1.Pod, error) {
	if runtime == nil || runtime.Name == "" {
		return nil, errors.New("sandbox identity is missing")
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: runtime.Name, Namespace: runtime.Namespace,
		Labels: maps.Clone(runtime.Spec.PodTemplate.ObjectMeta.Labels), Annotations: maps.Clone(runtime.Spec.PodTemplate.ObjectMeta.Annotations)}, Spec: *runtime.Spec.PodTemplate.Spec.DeepCopy()}
	// Native reconciliation removes placeholder volumes before appending claims.
	for _, template := range runtime.Spec.VolumeClaimTemplates {
		pod.Spec.Volumes = slices.DeleteFunc(pod.Spec.Volumes, func(volume corev1.Volume) bool { return volume.Name == template.Name })
	}
	for _, template := range runtime.Spec.VolumeClaimTemplates {
		pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{Name: template.Name,
			VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: template.Name + "-" + runtime.Name}}})
	}
	return pod, nil
}

func injectClaimEnv(pod *corev1.PodSpec, envs []extensionsv1beta1.EnvVar) {
	for _, env := range envs {
		for i := range pod.Containers {
			if pod.Containers[i].Name == env.ContainerName {
				pod.Containers[i].Env = append(pod.Containers[i].Env, corev1.EnvVar{Name: env.Name, Value: env.Value})
			}
		}
		for i := range pod.InitContainers {
			if pod.InitContainers[i].Name == env.ContainerName {
				pod.InitContainers[i].Env = append(pod.InitContainers[i].Env, corev1.EnvVar{Name: env.Name, Value: env.Value})
			}
		}
	}
}

func secureDefaults(template *extensionsv1beta1.SandboxTemplate, pod *corev1.PodSpec) {
	if pod.AutomountServiceAccountToken == nil {
		pod.AutomountServiceAccountToken = new(false)
	}
	managed := template.Spec.NetworkPolicyManagement == "" || template.Spec.NetworkPolicyManagement == extensionsv1beta1.NetworkPolicyManagementManaged
	if managed && template.Spec.NetworkPolicy == nil && pod.DNSPolicy == "" {
		pod.DNSPolicy = corev1.DNSNone
		pod.DNSConfig = &corev1.PodDNSConfig{Nameservers: []string{"8.8.8.8", "1.1.1.1"}}
	}
}
