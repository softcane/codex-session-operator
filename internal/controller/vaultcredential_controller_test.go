package controller

import (
	"fmt"
	"testing"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
)

const testVaultTransportField = "transport"
const testSelfHostedMode = "SelfHosted"
const testRestrictedAccess = "restricted"

func TestHostedCredentialsRejectSelfHostedAndNetworkMismatches(t *testing.T) {
	c := agentsv1alpha1.VaultCredential{Spec: agentsv1alpha1.VaultCredentialSpec{Auth: agentsv1alpha1.VaultCredentialAuth{Type: "environment_variable", EnvironmentVariable: &agentsv1alpha1.VaultEnvironmentVariable{VariableName: "CONTROLLED_TOKEN", Networking: agentsv1alpha1.VaultCredentialNetworking{Type: testLimitedMode, AllowedHosts: []string{"api.example.com"}}}}}}
	tests := []struct {
		name      string
		profile   *agentsv1alpha1.SessionEnvironmentSnapshot
		wantError bool
	}{
		{name: "missing environment snapshot", wantError: true},
		{name: "explicit self hosted", profile: &agentsv1alpha1.SessionEnvironmentSnapshot{Profile: agentsv1alpha1.EnvironmentSpec{Mode: testSelfHostedMode, SelfHosted: &agentsv1alpha1.SelfHostedEnvironment{}}}, wantError: true},
		{name: "disabled network", profile: &agentsv1alpha1.SessionEnvironmentSnapshot{Profile: agentsv1alpha1.EnvironmentSpec{Mode: openaiHostedMode, OpenAIHosted: &agentsv1alpha1.HostedEnvironment{Network: agentsv1alpha1.HostedNetwork{Access: "disabled"}}}}, wantError: true},
		{name: "missing allowed host", profile: &agentsv1alpha1.SessionEnvironmentSnapshot{Profile: agentsv1alpha1.EnvironmentSpec{Mode: openaiHostedMode, OpenAIHosted: &agentsv1alpha1.HostedEnvironment{Network: agentsv1alpha1.HostedNetwork{Access: testRestrictedAccess, AllowedDomains: []string{"other.example.com"}}}}}, wantError: true},
		{name: "allowed hosted host", profile: &agentsv1alpha1.SessionEnvironmentSnapshot{Profile: agentsv1alpha1.EnvironmentSpec{Mode: openaiHostedMode, OpenAIHosted: &agentsv1alpha1.HostedEnvironment{Network: agentsv1alpha1.HostedNetwork{Access: testRestrictedAccess, AllowedDomains: []string{"api.example.com"}}}}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := resolveSessionCredentials([]agentsv1alpha1.VaultCredential{c}, tt.profile, nil); (got != "") != tt.wantError {
				t.Fatal("hosted credential boundary was not enforced")
			}
		})
	}
}

func TestSessionResolvesUniqueMCPSelectionAndRejectsAmbiguity(t *testing.T) {
	url := "https://mcp.example.com/selection"
	credential := agentsv1alpha1.VaultCredential{Spec: agentsv1alpha1.VaultCredentialSpec{Auth: agentsv1alpha1.VaultCredentialAuth{Type: testStaticBearerType, StaticBearer: &agentsv1alpha1.VaultStaticBearer{ServerURL: url}}}, Status: agentsv1alpha1.VaultCredentialStatus{OpenAICredentialID: "cred_unique"}}
	tool := map[string]any{agentTypeField: "mcp", testVaultTransportField: map[string]any{agentTypeField: "http", "server_url": url}}
	agent := map[string]any{"tools": []any{tool}}
	if message := resolveSessionCredentials([]agentsv1alpha1.VaultCredential{credential}, nil, agent); message != "" {
		t.Fatal(message)
	}
	if tool["credential_id"] != "cred_unique" {
		t.Fatal("automatic credential selection was not frozen for readback")
	}
	delete(tool, "credential_id")
	second := credential
	second.Status.OpenAICredentialID = "cred_second"
	if resolveSessionCredentials([]agentsv1alpha1.VaultCredential{credential, second}, nil, agent) == "" {
		t.Fatal("ambiguous credential selection was accepted")
	}
	tool["connection_origin"] = testEnvironmentOrigin
	if resolveSessionCredentials([]agentsv1alpha1.VaultCredential{credential}, nil, agent) != "" || tool["credential_id"] != nil {
		t.Fatal("environment-origin tool incorrectly selected a Vault credential")
	}
}

func TestPendingMetadataKeysPrunesAbsentKeysAfterAmbiguousWrites(t *testing.T) {
	old := make([]string, 0, 26)
	current := make(map[string]string, 13)
	desired := make(map[string]string, 13)
	for i := range 13 {
		old = append(old, fmt.Sprintf("old-%d", i), fmt.Sprintf("committed-%d", i))
		current[fmt.Sprintf("committed-%d", i)] = "present"
		desired[fmt.Sprintf("next-%d", i)] = "desired"
	}
	claimed := pendingMetadataKeys(&desired, current, old)
	if len(claimed) != 26 {
		t.Fatal("pending metadata ownership must remain within its bounded union")
	}
	for _, key := range claimed {
		if len(key) >= 4 && key[:4] == "old-" {
			t.Fatal("confirmed absent keys remained claimed")
		}
	}
	if len(old) != 26 || len(current) != 13 {
		t.Fatal("metadata ownership inputs were mutated")
	}
}
