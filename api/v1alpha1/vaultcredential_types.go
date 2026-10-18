/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// VaultCredentialSpec contains public configuration and scoped protected references.
type VaultCredentialSpec struct {
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="vaultRef is immutable"
	VaultRef AgentReference `json:"vaultRef"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=256
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="credential name is immutable"
	Name string `json:"name"`
	// +optional
	// +kubebuilder:validation:MaxProperties=13
	// +kubebuilder:validation:XValidation:rule="self.all(k, size(k) >= 1 && size(k) <= 64 && size(self[k]) <= 512 && !(k in ['kubernetes_uid', 'operator', 'owner_scope']))",message="metadata exceeds limits or uses ownership keys"
	Metadata *map[string]string  `json:"metadata,omitempty"`
	Auth     VaultCredentialAuth `json:"auth"`
	// +kubebuilder:validation:Enum=Delete;Retain
	DeletionPolicy string `json:"deletionPolicy"`
}

// CredentialSecretReference selects a key in the namespace's protected Vault source.
type CredentialSecretReference struct {
	// +kubebuilder:validation:Enum=codex-vault
	Name string `json:"name"`
	// +kubebuilder:validation:Pattern="^[a-zA-Z0-9._-]{1,253}$"
	Key string `json:"key"`
}

// VaultCredentialAuth is a typed, immutable authentication-method union.
// +kubebuilder:validation:XValidation:rule="(self.type == 'static_bearer') == has(self.staticBearer)",message="staticBearer branch must match type"
// +kubebuilder:validation:XValidation:rule="(self.type == 'mcp_oauth') == has(self.mcpOAuth)",message="mcpOAuth branch must match type"
// +kubebuilder:validation:XValidation:rule="(self.type == 'environment_variable') == has(self.environmentVariable)",message="environmentVariable branch must match type"
type VaultCredentialAuth struct {
	// +kubebuilder:validation:Enum=static_bearer;mcp_oauth;environment_variable
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="auth type is immutable"
	Type string `json:"type"`
	// +optional
	StaticBearer *VaultStaticBearer `json:"staticBearer,omitempty"`
	// +optional
	MCPOAuth *VaultMCPOAuth `json:"mcpOAuth,omitempty"`
	// +optional
	EnvironmentVariable *VaultEnvironmentVariable `json:"environmentVariable,omitempty"`
}

// VaultStaticBearer authenticates service-origin HTTP MCP only.
type VaultStaticBearer struct {
	// +kubebuilder:validation:MaxLength=2048
	// +kubebuilder:validation:Pattern="^https://[^/@?#]+(/[^?#]*)?$"
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="MCP URL is immutable"
	ServerURL      string                    `json:"serverURL"`
	TokenSecretRef CredentialSecretReference `json:"tokenSecretRef"`
}

// VaultMCPOAuth stores a grant obtained by the application.
// +kubebuilder:validation:XValidation:rule="has(self.refresh) == has(oldSelf.refresh)",message="refresh configuration presence is immutable"
type VaultMCPOAuth struct {
	// +kubebuilder:validation:MaxLength=2048
	// +kubebuilder:validation:Pattern="^https://[^/@?#]+(/[^?#]*)?$"
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="MCP URL is immutable"
	ServerURL            string                    `json:"serverURL"`
	AccessTokenSecretRef CredentialSecretReference `json:"accessTokenSecretRef"`
	// ExpiresAt omission clears expiry when the access token is replaced.
	// +optional
	ExpiresAt *metav1.Time `json:"expiresAt,omitempty"`
	// +optional
	Refresh *VaultOAuthRefresh `json:"refresh,omitempty"`
}

// VaultOAuthRefresh keeps grant identity fixed and permits supported secret/scope rotation.
// +kubebuilder:validation:XValidation:rule="!(has(self.scope) && has(self.clearScope) && self.clearScope)",message="scope and clearScope conflict"
// +kubebuilder:validation:XValidation:rule="has(self.resource) == has(oldSelf.resource) && (!has(self.resource) || self.resource == oldSelf.resource)",message="resource is immutable"
type VaultOAuthRefresh struct {
	// +kubebuilder:validation:MaxLength=2048
	// +kubebuilder:validation:Pattern="^https://[^/@?#]+(/[^?#]*)?$"
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="token endpoint is immutable"
	TokenEndpoint string `json:"tokenEndpoint"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=512
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="client ID is immutable"
	ClientID              string                    `json:"clientID"`
	RefreshTokenSecretRef CredentialSecretReference `json:"refreshTokenSecretRef"`
	// +optional
	// +kubebuilder:validation:MaxLength=2048
	Resource *string `json:"resource,omitempty"`
	// +optional
	// +kubebuilder:validation:MaxLength=2048
	Scope *string `json:"scope,omitempty"`
	// +optional
	ClearScope        bool                   `json:"clearScope,omitempty"`
	TokenEndpointAuth VaultTokenEndpointAuth `json:"tokenEndpointAuth"`
}

// VaultTokenEndpointAuth covers each documented OAuth client authentication method.
// +kubebuilder:validation:XValidation:rule="(self.type != 'none') == has(self.clientSecretRef)",message="basic/post require a protected client secret; none forbids it"
type VaultTokenEndpointAuth struct {
	// +kubebuilder:validation:Enum=none;client_secret_basic;client_secret_post
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="token endpoint authentication method is immutable"
	Type string `json:"type"`
	// +optional
	ClientSecretRef *CredentialSecretReference `json:"clientSecretRef,omitempty"`
}

// VaultEnvironmentVariable supplies a hosted sandbox placeholder, never Pod credentials.
type VaultEnvironmentVariable struct {
	// +kubebuilder:validation:Pattern="^[A-Za-z_][A-Za-z0-9_]{0,127}$"
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="secretName is immutable"
	SecretName           string                    `json:"secretName"`
	SecretValueSecretRef CredentialSecretReference `json:"secretValueSecretRef"`
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="networking is immutable"
	Networking VaultCredentialNetworking `json:"networking"`
}

// VaultCredentialNetworking controls where hosted proxy substitution is allowed.
// +kubebuilder:validation:XValidation:rule="(self.type == 'limited') == has(self.allowedHosts)",message="limited requires hosts; unrestricted forbids hosts"
type VaultCredentialNetworking struct {
	// +kubebuilder:validation:Enum=limited;unrestricted
	Type string `json:"type"`
	// +optional
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=16
	// +listType=set
	AllowedHosts []string `json:"allowedHosts,omitempty"`
}

// CredentialSourceVersion exposes source identity, never secret bytes or their hash.
type CredentialSourceVersion struct {
	Name            string `json:"name"`
	UID             string `json:"uid"`
	ResourceVersion string `json:"resourceVersion"`
}

// VaultCredentialStatus records write acknowledgments and public identity.
type VaultCredentialStatus struct {
	CreationAttempted  bool   `json:"creationAttempted,omitempty"`
	OwnerScope         string `json:"ownerScope,omitempty"`
	VaultUID           string `json:"vaultUID,omitempty"`
	OpenAIVaultID      string `json:"openaiVaultID,omitempty"`
	OpenAICredentialID string `json:"openaiCredentialID,omitempty"`
	ObservedGeneration int64  `json:"observedGeneration,omitempty"`
	// ConfigurationDigest hashes only the public spec and source identities.
	ConfigurationDigest string `json:"configurationDigest,omitempty"`
	// +optional
	// +kubebuilder:validation:MaxItems=1
	SourceVersions []CredentialSourceVersion `json:"sourceVersions,omitempty"`
	// +optional
	// +kubebuilder:validation:MaxItems=26
	// +listType=set
	ManagedMetadataKeys []string `json:"managedMetadataKeys,omitempty"`
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:validation:XValidation:rule="has(self.spec.deletionPolicy)",message="VaultCredentials require deletionPolicy"

// VaultCredential is the Schema for the vaultcredentials API
type VaultCredential struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of VaultCredential
	// +required
	Spec VaultCredentialSpec `json:"spec"`

	// status defines the observed state of VaultCredential
	// +optional
	Status VaultCredentialStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// VaultCredentialList contains a list of VaultCredential
type VaultCredentialList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []VaultCredential `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &VaultCredential{}, &VaultCredentialList{})
		return nil
	})
}
