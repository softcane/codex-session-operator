package sandbox

import (
	"strings"
	"testing"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	testWorkspaceSize         = "2Gi"
	testCapabilityPath        = "/capabilities/helper"
	defaultWorkspaceDirectory = "/workspace"
)

func TestFrozenProfilePodContract(t *testing.T) {
	image := "registry.example/executor@sha256:" + strings.Repeat("a", 64)
	bundle := "registry.example/capability@sha256:" + strings.Repeat("b", 64)
	profile := &agentsv1alpha1.SelfHostedEnvironment{Image: image, Architecture: "arm64", WorkspaceDirectory: "/work", WorkspaceSize: testWorkspaceSize,
		Capabilities: []agentsv1alpha1.LocalCapability{{Name: "helper", Image: bundle, Path: testCapabilityPath, Type: "plugin"}}}
	if err := ValidateProfile(profile, []string{image}, []string{bundle}, nil); err != nil {
		t.Fatal(err)
	}
	s := &agentsv1alpha1.Session{ObjectMeta: metav1.ObjectMeta{UID: "session-uid", Namespace: "tenant"}, Status: agentsv1alpha1.SessionStatus{ExecutorImage: image,
		EnvironmentSnapshot: &agentsv1alpha1.SessionEnvironmentSnapshot{Profile: agentsv1alpha1.EnvironmentSpec{Mode: "selfHosted", SelfHosted: profile}}}}
	pod, err := Pod(s, "/work", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(pod.Spec.InitContainers) != 1 || pod.Spec.InitContainers[0].Image != bundle || len(pod.Spec.InitContainers[0].Env) != 0 || pod.Spec.Containers[0].VolumeMounts[3].ReadOnly != true ||
		pod.Spec.Volumes[0].EmptyDir.SizeLimit.String() != testWorkspaceSize || pod.Spec.NodeSelector["kubernetes.io/arch"] != "arm64" {
		t.Fatal("pinned startup or workspace contract lost")
	}
	profile.WorkspaceSize = "3Gi"
	profile.Capabilities[0].Image = "changed"
	if pod.Spec.Volumes[0].EmptyDir.SizeLimit.String() != testWorkspaceSize || pod.Spec.InitContainers[0].Image != bundle {
		t.Fatal("Pod construction retained mutable input aliases")
	}
	if pod.Spec.AutomountServiceAccountToken == nil || *pod.Spec.AutomountServiceAccountToken || pod.Spec.HostNetwork || pod.Spec.InitContainers[0].SecurityContext.AllowPrivilegeEscalation == nil || *pod.Spec.InitContainers[0].SecurityContext.AllowPrivilegeEscalation {
		t.Fatal("platform security contract lost")
	}
}

func TestProfileRestrictions(t *testing.T) {
	image := "registry.example/executor@sha256:" + strings.Repeat("a", 64)
	tests := []struct {
		name   string
		change func(*agentsv1alpha1.SelfHostedEnvironment)
	}{
		{name: "floating image", change: func(p *agentsv1alpha1.SelfHostedEnvironment) { p.Image = "executor:latest" }},
		{name: "unapproved image", change: func(p *agentsv1alpha1.SelfHostedEnvironment) { p.Image = "other@sha256:" + strings.Repeat("b", 64) }},
		{name: "workspace traversal", change: func(p *agentsv1alpha1.SelfHostedEnvironment) { p.WorkspaceDirectory = "/work/../etc" }},
		{name: "capability overlap", change: func(p *agentsv1alpha1.SelfHostedEnvironment) { p.WorkspaceDirectory = testCapabilityPath }},
		{name: "oversized workspace", change: func(p *agentsv1alpha1.SelfHostedEnvironment) { p.WorkspaceSize = "17Gi" }},
		{name: "invalid resources", change: func(p *agentsv1alpha1.SelfHostedEnvironment) {
			p.Resources = &agentsv1alpha1.ExecutorResources{CPURequest: "2", CPULimit: "1", MemoryRequest: "1Gi", MemoryLimit: testWorkspaceSize}
		}},
		{name: "unapproved capability", change: func(p *agentsv1alpha1.SelfHostedEnvironment) {
			p.Capabilities = []agentsv1alpha1.LocalCapability{{Name: "helper", Image: image, Path: testCapabilityPath, Type: "plugin"}}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			profile := &agentsv1alpha1.SelfHostedEnvironment{Image: image, Architecture: "amd64", WorkspaceDirectory: defaultWorkspaceDirectory, WorkspaceSize: "1Gi"}
			tt.change(profile)
			if ValidateProfile(profile, []string{image}, nil, nil) == nil {
				t.Fatal("restricted profile accepted")
			}
		})
	}
}
