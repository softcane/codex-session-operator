package controller

import (
	"testing"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
)

const testRequestedCapabilityPath = "/workspace/team-skills"

func TestHostedGeneratedCapabilityPaths(t *testing.T) {
	profile := &agentsv1alpha1.HostedEnvironment{Plugins: []agentsv1alpha1.HostedInlineCapability{{Name: "helper"}}, CapabilityDirectories: []string{testRequestedCapabilityPath}}
	installed := "/workspace/.managed-agents/plugins/helper-62ecf718b14083f8"
	tests := []struct {
		name  string
		paths []string
		want  bool
	}{
		{name: "requested plus installed", paths: []string{testRequestedCapabilityPath, installed}, want: true},
		{name: "missing installed", paths: []string{testRequestedCapabilityPath}},
		{name: "foreign plugin", paths: []string{testRequestedCapabilityPath, "/workspace/.managed-agents/plugins/other-62ecf718b14083f8"}},
		{name: "traversal", paths: []string{testRequestedCapabilityPath, installed + "/../escape"}},
		{name: "duplicate", paths: []string{testRequestedCapabilityPath, installed, installed}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if hostedCapabilityPathsMatch(profile, tt.paths) != tt.want {
				t.Fatal("runtime capability path validation mismatch")
			}
		})
	}
}
