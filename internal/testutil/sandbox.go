// Package testutil supplies shared native Agent Sandbox test fixtures.
package testutil

import (
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	sandboxv1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	extensions "sigs.k8s.io/agent-sandbox/extensions/api/v1beta1"
)

const sandboxPoolName = "codex-go-v1"

// NewSandboxPool returns an on-demand pool and a valid restricted executor template.
func NewSandboxPool(namespace string) (*extensions.SandboxWarmPool, *extensions.SandboxTemplate) {
	pool := &extensions.SandboxWarmPool{ObjectMeta: metav1.ObjectMeta{Name: sandboxPoolName, Namespace: namespace, Labels: map[string]string{"agents.softcane.io/managed": "true"}, UID: "pool-uid", Generation: 1}, Spec: extensions.SandboxWarmPoolSpec{Replicas: new(int32), TemplateRef: extensions.SandboxTemplateRef{Name: sandboxPoolName}}}
	template := &extensions.SandboxTemplate{ObjectMeta: metav1.ObjectMeta{Name: sandboxPoolName, Namespace: namespace, Labels: map[string]string{"agents.softcane.io/managed": "true"}, UID: "template-uid", Generation: 1}, Spec: extensions.SandboxTemplateSpec{
		EnvVarsInjectionPolicy:     extensions.EnvVarsInjectionPolicyAllowed,
		VolumeClaimTemplatesPolicy: extensions.VolumeClaimTemplatesPolicyDisallowed,
		SandboxBlueprint: sandboxv1.SandboxBlueprint{Service: new(false), PodTemplate: sandboxv1.PodTemplate{Spec: corev1.PodSpec{
			AutomountServiceAccountToken: new(false), RestartPolicy: corev1.RestartPolicyAlways,
			SecurityContext: &corev1.PodSecurityContext{RunAsNonRoot: new(true), RunAsUser: new(int64(1000)), RunAsGroup: new(int64(1000)), FSGroup: new(int64(1000)), SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault}},
			Containers: []corev1.Container{{Name: "executor", Image: "executor@sha256:" + strings.Repeat("a", 64), WorkingDir: "/workspace", Command: []string{"codex", "exec-server"}, Args: []string{"--remote", "$(CODEX_REMOTE_URL)", "--environment-id", "$(CODEX_ENVIRONMENT_ID)"},
				Env:             []corev1.EnvVar{{Name: "CODEX_API_KEY", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "codex-executor"}, Key: "CODEX_API_KEY"}}}},
				SecurityContext: &corev1.SecurityContext{AllowPrivilegeEscalation: new(false), ReadOnlyRootFilesystem: new(true), Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}},
				Resources:       corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m"), corev1.ResourceMemory: resource.MustParse("128Mi")}, Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("512Mi")}},
				VolumeMounts:    []corev1.VolumeMount{{Name: "workspace", MountPath: "/workspace"}},
			}},
		}}, VolumeClaimTemplates: []sandboxv1.PersistentVolumeClaimTemplate{{EmbeddedObjectMetadata: sandboxv1.EmbeddedObjectMetadata{Name: "workspace"}, Spec: corev1.PersistentVolumeClaimSpec{AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, StorageClassName: new("standard"), Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")}}}}}},
	}}
	return pool, template
}
