package openaiapi

import (
	"encoding/json"
	"reflect"
	"slices"
	"testing"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
)

const (
	collectionDomainA    = "a.example.com"
	collectionDomainB    = "b.example.com"
	collectionWorkspaceA = "/workspace/a"
	collectionWorkspaceB = "/workspace/b"
	collectionFileIDType = agentsv1alpha1.HostedFileTypeFileID
)

func TestEnvironmentUnorderedCollections(t *testing.T) {
	original := agentsv1alpha1.EnvironmentSpec{Mode: agentsv1alpha1.EnvironmentModeOpenAIHosted,
		OpenAIHosted: &agentsv1alpha1.HostedEnvironment{Network: agentsv1alpha1.HostedNetwork{Access: restrictedNetworkAccess,
			AllowedDomains: []string{collectionDomainB, collectionDomainA}}, CapabilityDirectories: []string{collectionWorkspaceB, collectionWorkspaceA},
			Env:           []agentsv1alpha1.HostedVariable{{Name: "B", SecretKey: "b"}, {Name: "A", SecretKey: "a"}},
			Files:         []agentsv1alpha1.HostedFile{{Type: collectionFileIDType, Path: collectionWorkspaceB, FileID: "file_b"}, {Type: collectionFileIDType, Path: collectionWorkspaceA, FileID: "file_a"}},
			Plugins:       []agentsv1alpha1.HostedInlineCapability{{Name: "b"}, {Name: "a"}},
			SetupCommands: []agentsv1alpha1.HostedSetupCommand{{SecretKey: "first"}, {SecretKey: "second"}},
		}}
	before := original.DeepCopy()
	reordered := original.DeepCopy()
	h := reordered.OpenAIHosted
	slices.Reverse(h.Network.AllowedDomains)
	slices.Reverse(h.CapabilityDirectories)
	slices.Reverse(h.Env)
	slices.Reverse(h.Files)
	slices.Reverse(h.Plugins)
	if EnvironmentDigest(original, nil) != EnvironmentDigest(*reordered, nil) || TemplateConfigurationDigest(original.OpenAIHosted, nil) != TemplateConfigurationDigest(h, nil) {
		t.Fatal("set/map ordering changed an Environment digest")
	}
	if !reflect.DeepEqual(&original, before) {
		t.Fatal("digest mutated caller configuration")
	}
	h.Files[0].FileID = "different-file-id"
	if EnvironmentDigest(original, nil) == EnvironmentDigest(*reordered, nil) {
		t.Fatal("file identity difference disappeared")
	}
	h = original.OpenAIHosted.DeepCopy()
	slices.Reverse(h.SetupCommands)
	if EnvironmentDigest(original, nil) == EnvironmentDigest(agentsv1alpha1.EnvironmentSpec{Mode: original.Mode, OpenAIHosted: h}, nil) {
		t.Fatal("ordered setup commands lost their order")
	}
	self := agentsv1alpha1.EnvironmentSpec{Mode: agentsv1alpha1.EnvironmentModeSelfHosted, SelfHosted: &agentsv1alpha1.SelfHostedEnvironment{CapabilityDirectories: []string{"/capabilities/b", "/capabilities/a"}}}
	other := self.DeepCopy()
	slices.Reverse(other.SelfHosted.CapabilityDirectories)
	if SandboxEnvironmentDigest(self, nil) != SandboxEnvironmentDigest(*other, nil) {
		t.Fatal("capability set order changed Sandbox digest")
	}
}

func TestObservableEnvironmentUnorderedCollections(t *testing.T) {
	desired := map[string]any{networkField: map[string]any{"access": restrictedNetworkAccess, allowedDomainsField: []string{collectionDomainB, collectionDomainA}},
		capabilityDirectoriesField: []string{collectionWorkspaceB, collectionWorkspaceA}, filesField: []any{map[string]any{typeField: "file_id", pathField: collectionWorkspaceB, fileIDField: "b"}, map[string]any{typeField: "file_id", pathField: collectionWorkspaceA, fileIDField: "a"}},
		pluginsField: []any{map[string]any{nameField: "b", typeField: inlineType}, map[string]any{nameField: "a", typeField: inlineType}}}
	raw, _ := json.Marshal(desired)
	var observed map[string]any
	if err := json.Unmarshal(raw, &observed); err != nil {
		t.Fatal(err)
	}
	slices.Reverse(observed[networkField].(map[string]any)[allowedDomainsField].([]any))
	for _, key := range []string{capabilityDirectoriesField, filesField, pluginsField} {
		slices.Reverse(observed[key].([]any))
	}
	if !TemplateMatches(desired, Template{Config: observed}) || ObservableEnvironmentDigest(desired) != ObservableEnvironmentDigest(observed) {
		t.Fatal("provider set/map ordering changed configuration identity")
	}
	// Redacted projections still distinguish real safe changes.
	observed[filesField].([]any)[0].(map[string]any)[fileIDField] = "changed"
	if TemplateMatches(desired, Template{Config: observed}) || ObservableEnvironmentDigest(desired) == ObservableEnvironmentDigest(observed) {
		t.Fatal("file identity difference disappeared from safe readback")
	}
	after, _ := json.Marshal(desired)
	if string(raw) != string(after) {
		t.Fatal("projection mutated caller configuration")
	}
}

func TestCredentialHostSetIdentity(t *testing.T) {
	spec := agentsv1alpha1.VaultCredentialSpec{Auth: agentsv1alpha1.VaultCredentialAuth{Type: environmentCredentialType, EnvironmentVariable: &agentsv1alpha1.VaultEnvironmentVariable{VariableName: "TEST_TOKEN", Networking: agentsv1alpha1.VaultCredentialNetworking{Type: limitedNetworkingType, AllowedHosts: []string{collectionDomainB, collectionDomainA}}}}}
	before := spec.DeepCopy()
	reordered := spec.DeepCopy()
	slices.Reverse(reordered.Auth.EnvironmentVariable.Networking.AllowedHosts)
	remote := Credential{Auth: map[string]any{typeField: environmentCredentialType, "secret_name": "TEST_TOKEN", "networking": map[string]any{typeField: limitedNetworkingType, "allowed_hosts": reordered.Auth.EnvironmentVariable.Networking.AllowedHosts}}}
	if !CredentialPublicMatches(spec.Auth, remote) || CredentialDigest(spec, nil) != CredentialDigest(*reordered, nil) {
		t.Fatal("host set order changed credential identity")
	}
	if !reflect.DeepEqual(&spec, before) {
		t.Fatal("credential canonicalization mutated caller")
	}
	reordered.Auth.EnvironmentVariable.Networking.AllowedHosts[0] = "different.example.com"
	if CredentialDigest(spec, nil) == CredentialDigest(*reordered, nil) {
		t.Fatal("host membership change disappeared")
	}
}

func TestMalformedObservableStringSetDoesNotMatchEmptySet(t *testing.T) {
	desired := map[string]any{capabilityDirectoriesField: []string{}}
	malformed := map[string]any{capabilityDirectoriesField: []any{123}}
	if TemplateMatches(desired, Template{Config: malformed}) {
		t.Fatal("invalid provider set was coerced to an empty valid set")
	}
}
