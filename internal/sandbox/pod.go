package sandbox

import (
	"errors"
	"regexp"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ExecutorSecretName is platform configuration, never a tenant-selected Secret.
const ExecutorSecretName = "codex-executor"
const ExecutorSecretKey = "CODEX_API_KEY"
const ConfigAnnotation = "agents.softcane.io/executor-config"

var imageDigest = regexp.MustCompile(`^[^\s@]+@sha256:[a-f0-9]{64}$`)

// ValidateImage requires an immutable image reference. Approval of that digest
// is a separate platform policy check; this function validates its shape only.
func ValidateImage(image string) error {
	if !imageDigest.MatchString(image) {
		return errors.New("executor image must be pinned by sha256 digest")
	}
	return nil
}

// PodName is stable across retries and distinct for a recreated Session UID.
func PodName(session *agentsv1alpha1.Session) string { return "executor-" + string(session.UID) }

// Pod constructs the complete platform-owned executor specification.
func Pod(session *agentsv1alpha1.Session, workspace, config string) (*corev1.Pod, error) {
	if err := ValidateImage(session.Status.ExecutorImage); err != nil {
		return nil, err
	}
	if err := ValidateWorkspace(workspace); err != nil {
		return nil, err
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: PodName(session), Namespace: session.Namespace,
			Labels:      map[string]string{"agents.softcane.io/session-uid": string(session.UID)},
			Annotations: map[string]string{ConfigAnnotation: config}},
		Spec: corev1.PodSpec{
			AutomountServiceAccountToken: new(false), RestartPolicy: corev1.RestartPolicyAlways,
			TerminationGracePeriodSeconds: new(int64(10)),
			SecurityContext: &corev1.PodSecurityContext{RunAsNonRoot: new(true), RunAsUser: new(int64(1000)),
				RunAsGroup: new(int64(1000)), FSGroup: new(int64(1000)),
				SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault}},
			Containers: []corev1.Container{{Name: "executor", Image: session.Status.ExecutorImage,
				ImagePullPolicy: corev1.PullIfNotPresent, WorkingDir: workspace,
				Command: []string{"codex", "exec-server"},
				Args:    []string{"--remote", session.Status.RemoteURL, "--environment-id", session.Status.EnvironmentID},
				Env: []corev1.EnvVar{{Name: ExecutorSecretKey, ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: ExecutorSecretName}, Key: ExecutorSecretKey}}}},
				SecurityContext: &corev1.SecurityContext{AllowPrivilegeEscalation: new(false), ReadOnlyRootFilesystem: new(true),
					Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}},
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m"), corev1.ResourceMemory: resource.MustParse("128Mi")},
					Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("512Mi")}},
				VolumeMounts: []corev1.VolumeMount{{Name: "workspace", MountPath: workspace}, {Name: "home", MountPath: "/home/node"}, {Name: "tmp", MountPath: "/tmp"}},
			}},
			Volumes: []corev1.Volume{
				{Name: "workspace", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: new(resource.MustParse("1Gi"))}}},
				{Name: "home", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: new(resource.MustParse("128Mi"))}}},
				{Name: "tmp", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: new(resource.MustParse("128Mi"))}}},
			},
		},
	}
	if session.Status.EnvironmentSnapshot != nil {
		if err := ApplyProfile(pod, session.Status.EnvironmentSnapshot.Profile.SelfHosted, string(session.UID)); err != nil {
			return nil, err
		}
		if profile := session.Status.EnvironmentSnapshot.Profile.SelfHosted; profile != nil {
			if err := applyRepository(pod, profile.Repository, string(session.UID)); err != nil {
				return nil, err
			}
		}
	}
	return pod, nil
}
