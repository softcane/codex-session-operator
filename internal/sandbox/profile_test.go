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
	for _, variable := range pod.Spec.Containers[0].Env[1:] {
		if !strings.HasPrefix(variable.Value, "/work/.cache/") || variable.ValueFrom != nil {
			t.Fatal("Go cache must use the configured writable workspace")
		}
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

func TestCapabilityNameRestrictions(t *testing.T) {
	const (
		mode          = "selfHosted"
		architecture  = "amd64"
		workspaceSize = "1Gi"
	)
	image := "registry.example/executor@sha256:" + strings.Repeat("a", 64)
	for _, tt := range []struct {
		name  string
		valid bool
	}{
		{name: "h", valid: true},
		{name: "helper-1", valid: true},
		{name: strings.Repeat("h", 32), valid: true},
		{name: "helper-"},
		{name: "1helper"},
		{name: "Helper"},
		{name: "helper_name"},
		{name: strings.Repeat("h", 33)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			profile := &agentsv1alpha1.SelfHostedEnvironment{
				Image: image, Architecture: architecture, WorkspaceDirectory: defaultWorkspaceDirectory, WorkspaceSize: workspaceSize,
				Capabilities: []agentsv1alpha1.LocalCapability{{Name: tt.name, Path: "/capabilities/" + tt.name, Image: image, Type: "skills"}},
			}
			err := ValidateProfile(profile, []string{image}, []string{image}, nil)
			if (err == nil) != tt.valid {
				t.Fatalf("capability admission validity: %v", err)
			}
			session := &agentsv1alpha1.Session{ObjectMeta: metav1.ObjectMeta{UID: "capability-session"}, Status: agentsv1alpha1.SessionStatus{
				ExecutorImage: image, EnvironmentSnapshot: &agentsv1alpha1.SessionEnvironmentSnapshot{
					Profile: agentsv1alpha1.EnvironmentSpec{Mode: mode, SelfHosted: profile},
				},
			}}
			_, err = Pod(session, defaultWorkspaceDirectory, "")
			if (err == nil) != tt.valid {
				t.Fatalf("frozen capability Pod validity: %v", err)
			}
		})
	}
}
