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

// EnvironmentSpec is a reusable recipe, never an active runtime.
// +kubebuilder:validation:XValidation:rule="self.mode == oldSelf.mode",message="Environment mode is immutable"
// +kubebuilder:validation:XValidation:rule="(self.mode == 'selfHosted') == has(self.selfHosted)",message="selfHosted branch must match mode"
// +kubebuilder:validation:XValidation:rule="(self.mode == 'openaiHosted') == has(self.openaiHosted)",message="openaiHosted branch must match mode"
type EnvironmentSpec struct {
	// +kubebuilder:validation:Enum=selfHosted;openaiHosted;none
	Mode string `json:"mode"`
	// +optional
	SelfHosted *SelfHostedEnvironment `json:"selfHosted,omitempty"`
	// +optional
	OpenAIHosted *HostedEnvironment `json:"openaiHosted,omitempty"`
	// +kubebuilder:validation:Enum=Delete;Retain
	DeletionPolicy string `json:"deletionPolicy"`
}

// SelfHostedEnvironment describes platform-approved local compute.
type SelfHostedEnvironment struct {
	// +kubebuilder:validation:Pattern="^[^\\s@]+@sha256:[a-f0-9]{64}$"
	// +kubebuilder:validation:MaxLength=512
	Image string `json:"image"`
	// +kubebuilder:validation:Enum=amd64;arm64
	Architecture string `json:"architecture"`
	// +kubebuilder:default="/workspace"
	// +kubebuilder:validation:Pattern="^/"
	// +kubebuilder:validation:MaxLength=4096
	// +optional
	WorkspaceDirectory string `json:"workspaceDirectory,omitempty"`
	// WorkspaceSize bounds an ephemeral volume or requests a persistent claim.
	// +kubebuilder:default="1Gi"
	// +kubebuilder:validation:MaxLength=32
	// +optional
	WorkspaceSize string `json:"workspaceSize,omitempty"`
	// WorkspaceStorage selects whether files survive executor Pod replacement.
	// Omission selects an ephemeral workspace with the configured size.
	// +optional
	WorkspaceStorage *WorkspaceStorage `json:"workspaceStorage,omitempty"`
	// Repository prepares a trusted checkout before the executor starts.
	// Changes apply only to future Sessions; replacement Pods preserve existing files.
	// +optional
	Repository *RepositorySpec `json:"repository,omitempty"`
	// +optional
	Resources *ExecutorResources `json:"resources,omitempty"`
	// +kubebuilder:validation:MaxItems=16
	// +listType=map
	// +listMapKey=name
	// +optional
	Capabilities []LocalCapability `json:"capabilities,omitempty"`
}

// RepositorySpec pins a platform-approved, credential-free HTTPS repository.
// +kubebuilder:validation:XValidation:rule="!has(self.mountCredentialsInExecutor) || !self.mountCredentialsInExecutor || has(self.credentialSecretRef)",message="executor credential mounting requires credentialSecretRef"
type RepositorySpec struct {
	// URL must also be approved by the operator's exact repository URL allowlist.
	// +kubebuilder:validation:Pattern="^https://[^/@?#]+/[^?#]+$"
	// +kubebuilder:validation:MaxLength=2048
	URL string `json:"url"`
	// Commit is a complete immutable SHA-1 object identity, never a branch or tag.
	// +kubebuilder:validation:Pattern="^[a-f0-9]{40}$"
	Commit string `json:"commit"`
	// CredentialSecretRef selects protected material mounted into preparation by default.
	// +optional
	CredentialSecretRef *RepositoryCredentialSecretReference `json:"credentialSecretRef,omitempty"`
	// MountCredentialsInExecutor explicitly exposes the selected repository credential
	// files to task commands. Omission keeps credentials in preparation only.
	// Credential permissions must be restricted by the Git hosting provider.
	// +optional
	MountCredentialsInExecutor bool `json:"mountCredentialsInExecutor,omitempty"`
}

// RepositoryCredentialSecretReference cannot select arbitrary namespace Secrets.
// +kubebuilder:validation:XValidation:rule="!has(self.usernameKey) || self.usernameKey != self.key",message="repository token and username keys must be distinct"
type RepositoryCredentialSecretReference struct {
	// +kubebuilder:validation:Enum=codex-repository
	Name string `json:"name"`
	// Key selects the access token; its value is never placed in public configuration.
	// +kubebuilder:validation:Pattern="^[a-zA-Z0-9._-]{1,253}$"
	Key string `json:"key"`
	// UsernameKey selects an optional username; omission uses x-access-token.
	// +kubebuilder:validation:Pattern="^[a-zA-Z0-9._-]{1,253}$"
	// +optional
	UsernameKey string `json:"usernameKey,omitempty"`
}

// WorkspaceStorage controls the Session-owned workspace claim lifecycle.
// +kubebuilder:validation:XValidation:rule="self.type == 'Persistent' ? has(self.storageClassName) && has(self.claimDeletionPolicy) : !has(self.storageClassName) && !has(self.claimDeletionPolicy)",message="Persistent storage requires a class and claim deletion policy; Ephemeral permits neither"
type WorkspaceStorage struct {
	// +kubebuilder:validation:Enum=Ephemeral;Persistent
	Type string `json:"type"`
	// StorageClassName must also be approved by the operator allowlist.
	// +kubebuilder:validation:Pattern="^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$"
	// +kubebuilder:validation:MaxLength=253
	// +optional
	StorageClassName string `json:"storageClassName,omitempty"`
	// ClaimDeletionPolicy deletes or retains the PVC when its Session is deleted.
	// A StorageClass separately controls whether its backing volume is reclaimed.
	// +kubebuilder:validation:Enum=Delete;Retain
	// +optional
	ClaimDeletionPolicy string `json:"claimDeletionPolicy,omitempty"`
}

// ExecutorResources permits CPU and memory only; controller enforces platform bounds.
type ExecutorResources struct {
	// +kubebuilder:default="100m"
	// +kubebuilder:validation:MaxLength=32
	// +optional
	CPURequest string `json:"cpuRequest,omitempty"`
	// +kubebuilder:default="1"
	// +kubebuilder:validation:MaxLength=32
	// +optional
	CPULimit string `json:"cpuLimit,omitempty"`
	// +kubebuilder:default="128Mi"
	// +kubebuilder:validation:MaxLength=32
	// +optional
	MemoryRequest string `json:"memoryRequest,omitempty"`
	// +kubebuilder:default="512Mi"
	// +kubebuilder:validation:MaxLength=32
	// +optional
	MemoryLimit string `json:"memoryLimit,omitempty"`
}

// LocalCapability is a versioned /bundle image copied before the executor starts.
type LocalCapability struct {
	// +kubebuilder:validation:Pattern="^[a-z]([a-z0-9-]{0,30}[a-z0-9])?$"
	Name string `json:"name"`
	// +kubebuilder:validation:Pattern="^[^\\s@]+@sha256:[a-f0-9]{64}$"
	// +kubebuilder:validation:MaxLength=512
	Image string `json:"image"`
	// Path selects the actual plugin root, or a directory containing skills.
	// +kubebuilder:validation:Pattern="^/capabilities/[a-z]([a-z0-9-]{0,30}[a-z0-9])?$"
	Path string `json:"path"`
	// +kubebuilder:validation:Enum=plugin;skills
	Type string `json:"type"`
}

// HostedEnvironment owns the full remote template configuration.
type HostedEnvironment struct {
	// Optional display prefix; the controller retains an ownership suffix.
	// +kubebuilder:validation:MaxLength=128
	// +optional
	Name string `json:"name,omitempty"`
	// +kubebuilder:validation:Enum=small;medium;large
	// +kubebuilder:default=medium
	// +optional
	ContainerSize string `json:"containerSize,omitempty"`
	// +optional
	Desktop bool          `json:"desktop,omitempty"`
	Network HostedNetwork `json:"network"`
	// +optional
	Packages *HostedPackages `json:"packages,omitempty"`
	// +kubebuilder:validation:MaxItems=32
	// +listType=set
	// +kubebuilder:validation:items:Pattern="^/workspace/"
	// +kubebuilder:validation:items:MaxLength=4096
	// +optional
	CapabilityDirectories []string `json:"capabilityDirectories,omitempty"`
	// +kubebuilder:validation:MaxItems=64
	// +listType=map
	// +listMapKey=name
	// +optional
	Env []HostedVariable `json:"env,omitempty"`
	// +kubebuilder:validation:MaxItems=64
	// +listType=map
	// +listMapKey=path
	// +optional
	Files []HostedFile `json:"files,omitempty"`
	// +kubebuilder:validation:MaxItems=16
	// +listType=map
	// +listMapKey=name
	// +optional
	Plugins []HostedInlineCapability `json:"plugins,omitempty"`
	// +kubebuilder:validation:MaxItems=16
	// +listType=atomic
	// +optional
	Skills []HostedSkill `json:"skills,omitempty"`
	// +kubebuilder:validation:MaxItems=32
	// +listType=atomic
	// +optional
	SetupCommands []HostedSetupCommand `json:"setupCommands,omitempty"`
}

// HostedNetwork sets explicit policy independently of beta/GA defaults.
// +kubebuilder:validation:XValidation:rule="self.access == 'restricted' || (size(self.allowedDomains) == 0 && size(self.blockedDomains) == 0)",message="Domains require restricted access"
// +kubebuilder:validation:XValidation:rule="size(self.allowedDomains) == 0 || size(self.blockedDomains) == 0",message="Allow and block lists cannot be combined"
// +kubebuilder:validation:XValidation:rule="self.access != 'restricted' || size(self.allowedDomains) + size(self.blockedDomains) > 0",message="Restricted access requires domains"
type HostedNetwork struct {
	// +kubebuilder:validation:Enum=enabled;disabled;restricted
	Access string `json:"access"`
	// +kubebuilder:validation:MaxItems=100
	// +kubebuilder:validation:items:Pattern="^[a-zA-Z0-9]([a-zA-Z0-9.-]*[a-zA-Z0-9])?$"
	// +kubebuilder:validation:items:MaxLength=253
	// +listType=set
	// +kubebuilder:default={}
	// +optional
	AllowedDomains []string `json:"allowedDomains,omitempty"`
	// +kubebuilder:validation:MaxItems=100
	// +kubebuilder:validation:items:Pattern="^[a-zA-Z0-9]([a-zA-Z0-9.-]*[a-zA-Z0-9])?$"
	// +kubebuilder:validation:items:MaxLength=253
	// +listType=set
	// +kubebuilder:default={}
	// +optional
	BlockedDomains []string `json:"blockedDomains,omitempty"`
}

// HostedPackages contains non-secret package specifiers.
type HostedPackages struct {
	// +kubebuilder:validation:MaxItems=64
	// +kubebuilder:validation:items:MaxLength=256
	// +optional
	Python []string `json:"python,omitempty"`
	// +kubebuilder:validation:MaxItems=64
	// +kubebuilder:validation:items:MaxLength=256
	// +optional
	System []string `json:"system,omitempty"`
	// +kubebuilder:validation:MaxItems=64
	// +kubebuilder:validation:items:MaxLength=256
	// +optional
	NPM []string `json:"npm,omitempty"`
}

// +kubebuilder:validation:XValidation:rule="self.name != 'PATH' && self.name != 'OPENAI_API_KEY' && !self.name.startsWith('CODEX_')",message="Runtime-reserved environment names are restricted"
// HostedVariable reads values from the platform's codex-environment Secret.
type HostedVariable struct {
	// +kubebuilder:validation:Pattern="^[A-Za-z_][A-Za-z0-9_]{0,127}$"
	Name string `json:"name"`
	// +kubebuilder:validation:Pattern="^[a-zA-Z0-9._-]{1,253}$"
	SecretKey string `json:"secretKey"`
}

// HostedFile never stores inline bytes in the API.
// +kubebuilder:validation:XValidation:rule="(self.type == 'fileID') == has(self.fileID)",message="fileID branch must match type"
// +kubebuilder:validation:XValidation:rule="(self.type == 'inline') == has(self.secretKey)",message="inline branch must match type"
type HostedFile struct {
	// +kubebuilder:validation:Enum=fileID;inline
	Type string `json:"type"`
	// +kubebuilder:validation:Pattern="^/workspace/"
	// +kubebuilder:validation:MaxLength=4096
	Path string `json:"path"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=256
	// +optional
	FileID string `json:"fileID,omitempty"`
	// +kubebuilder:validation:Pattern="^[a-zA-Z0-9._-]{1,253}$"
	// +optional
	SecretKey string `json:"secretKey,omitempty"`
}

// HostedArchive pins a protected ZIP to an immutable Secret object version.
type HostedArchive struct {
	// +kubebuilder:validation:Pattern="^[a-zA-Z0-9._-]{1,253}$"
	SecretKey string `json:"secretKey"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=128
	SecretUID string `json:"secretUID"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=128
	SecretResourceVersion string `json:"secretResourceVersion"`
}

// HostedInlineCapability carries safe metadata and a protected archive reference.
type HostedInlineCapability struct {
	// +kubebuilder:validation:Pattern="^[a-z][a-z0-9-]{0,63}$"
	Name string `json:"name"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=2048
	Description string        `json:"description"`
	Source      HostedArchive `json:"source"`
}

// HostedSkill selects an exact API version or a pinned protected archive.
// +kubebuilder:validation:XValidation:rule="(self.type == 'skillReference') == (has(self.skillID) && has(self.version))",message="skillReference requires ID and concrete version"
// +kubebuilder:validation:XValidation:rule="(self.type == 'inline') == has(self.inline)",message="inline branch must match type"
// +kubebuilder:validation:XValidation:rule="self.type == 'skillReference' || (!has(self.skillID) && !has(self.version))",message="inline skill cannot carry reference fields"
type HostedSkill struct {
	// +kubebuilder:validation:Enum=skillReference;inline
	Type string `json:"type"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=256
	// +optional
	SkillID string `json:"skillID,omitempty"`
	// +kubebuilder:validation:Pattern="^[1-9][0-9]*$"
	// +kubebuilder:validation:MaxLength=20
	// +optional
	Version string `json:"version,omitempty"`
	// +optional
	Inline *HostedInlineCapability `json:"inline,omitempty"`
}

// HostedSetupCommand reads its command from the protected platform Secret.
type HostedSetupCommand struct {
	// +kubebuilder:validation:Pattern="^[a-zA-Z0-9._-]{1,253}$"
	SecretKey string `json:"secretKey"`
	// +kubebuilder:default="/workspace"
	// +kubebuilder:validation:Pattern="^/workspace($|/)"
	// +kubebuilder:validation:MaxLength=4096
	// +optional
	Cwd string `json:"cwd,omitempty"`
}

// EnvironmentSourceVersion identifies protected material without hashing values.
type EnvironmentSourceVersion struct {
	UID             string `json:"uid"`
	ResourceVersion string `json:"resourceVersion"`
}

// EnvironmentStatus records identity, acknowledged intent and audit candidates.
type EnvironmentStatus struct {
	CreationAttempted   bool   `json:"creationAttempted,omitempty"`
	OwnerScope          string `json:"ownerScope,omitempty"`
	CreationName        string `json:"creationName,omitempty"`
	OpenAITemplateID    string `json:"openaiTemplateID,omitempty"`
	ConfigurationDigest string `json:"configurationDigest,omitempty"`
	// TemplateConfigurationDigest excludes container size and local deletion policy.
	TemplateConfigurationDigest string `json:"templateConfigurationDigest,omitempty"`
	// +optional
	SourceVersion *EnvironmentSourceVersion `json:"sourceVersion,omitempty"`
	// Audit candidates are never adopted automatically.
	// +kubebuilder:validation:MaxItems=1000
	// +optional
	AuditCandidateIDs []string `json:"auditCandidateIDs,omitempty"`
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:validation:XValidation:rule="has(self.spec.deletionPolicy)",message="Environments require deletionPolicy"

// Environment is the Schema for the environments API
type Environment struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of Environment
	// +required
	Spec EnvironmentSpec `json:"spec"`

	// status defines the observed state of Environment
	// +optional
	Status EnvironmentStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// EnvironmentList contains a list of Environment
type EnvironmentList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []Environment `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &Environment{}, &EnvironmentList{})
		return nil
	})
}
