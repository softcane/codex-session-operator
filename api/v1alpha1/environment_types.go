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

// EnvironmentMode selects a runtime; provider wire literals are translated by the adapter.
type EnvironmentMode string

const (
	EnvironmentModeSelfHosted   EnvironmentMode = "SelfHosted"
	EnvironmentModeOpenAIHosted EnvironmentMode = "OpenAIHosted"
	EnvironmentModeNone         EnvironmentMode = "None"
)

// DeletionPolicy selects deletion or administrative retention of owned resources.
type DeletionPolicy string

const (
	DeletionPolicyDelete DeletionPolicy = "Delete"
	DeletionPolicyRetain DeletionPolicy = "Retain"
)

// EnvironmentSpec is a reusable recipe, never an active runtime.
// +kubebuilder:validation:XValidation:rule="self.mode == oldSelf.mode",message="Environment mode is immutable"
// +kubebuilder:validation:XValidation:rule="(self.mode == 'SelfHosted') == has(self.selfHosted)",message="selfHosted branch must match mode"
// +kubebuilder:validation:XValidation:rule="(self.mode == 'OpenAIHosted') == has(self.openaiHosted)",message="openaiHosted branch must match mode"
type EnvironmentSpec struct {
	// +kubebuilder:validation:Enum=SelfHosted;OpenAIHosted;None
	// +required
	Mode EnvironmentMode `json:"mode"`
	// +optional
	SelfHosted *SelfHostedEnvironment `json:"selfHosted,omitempty"`
	// +optional
	OpenAIHosted *HostedEnvironment `json:"openaiHosted,omitempty"`
	// +kubebuilder:validation:Enum=Delete;Retain
	// +required
	DeletionPolicy DeletionPolicy `json:"deletionPolicy"`
}

// SelfHostedEnvironment selects an administrator-approved Agent Sandbox pool.
// Compute, repository preparation and storage configuration belong to its template.
type SelfHostedEnvironment struct {
	// SandboxPoolRef selects a same-namespace SandboxWarmPool.
	// +required
	SandboxPoolRef AgentReference `json:"sandboxPoolRef"`
	// Workspace configures the workspace location and cleanup policy.
	// +required
	Workspace SelfHostedWorkspace `json:"workspace"`
	// CapabilityDirectories names prepared installation paths in the template.
	// +kubebuilder:validation:MaxItems=32
	// +kubebuilder:validation:items:Pattern="^/[^/]+(/[^/]+)*$"
	// +kubebuilder:validation:XValidation:rule="self.all(d, d.split('/').all(p, p != '.' && p != '..'))",message="capability paths must be clean absolute directories"
	// +kubebuilder:validation:items:MaxLength=4096
	// +listType=set
	// +optional
	CapabilityDirectories []string `json:"capabilityDirectories,omitempty"`
}

// SelfHostedWorkspace configures files managed by the selected Sandbox template.
// +kubebuilder:validation:XValidation:rule="!['/home', '/tmp', '/usr', '/bin', '/sbin', '/etc', '/dev', '/proc', '/sys', '/run', '/capabilities'].exists(p, self.directory == p || self.directory.startsWith(p + '/'))",message="workspace directory overlaps a reserved runtime directory"
type SelfHostedWorkspace struct {
	// Directory must be a clean absolute directory other than root.
	// +kubebuilder:default="/workspace"
	// +kubebuilder:validation:Pattern="^/[^/]+(/[^/]+)*$"
	// +kubebuilder:validation:MaxLength=4096
	// +kubebuilder:validation:XValidation:rule="self.split('/').all(p, p != '.' && p != '..')",message="directory must be a clean absolute path"
	// +optional
	Directory string `json:"directory,omitempty"`
	// DeletionPolicy deletes the Sandbox and files, or retains a suspended workspace.
	// +kubebuilder:validation:Enum=Delete;Retain
	// +required
	DeletionPolicy DeletionPolicy `json:"deletionPolicy"`
}

// SandboxPoolSnapshot freezes public identities and configuration of approved infrastructure.
// Template configuration remains in native Agent Sandbox resources.
type SandboxPoolSnapshot struct {
	// +required
	Name string `json:"name"`
	// +required
	UID string `json:"uid"`
	// +required
	Generation int64 `json:"generation"`
	// +required
	TemplateName string `json:"templateName"`
	// +required
	TemplateUID string `json:"templateUID"`
	// +required
	TemplateGeneration int64 `json:"templateGeneration"`
	// +required
	ConfigurationDigest string `json:"configurationDigest"`
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
	Desktop bool `json:"desktop,omitempty"`
	// +required
	Network HostedNetwork `json:"network"`
	// +optional
	Packages *HostedPackages `json:"packages,omitempty"`
	// +kubebuilder:validation:MaxItems=32
	// +listType=set
	// +kubebuilder:validation:items:Pattern="^/workspace/[^/]+(/[^/]+)*$"
	// +kubebuilder:validation:XValidation:rule="self.all(d, d.split('/').all(p, p != '.' && p != '..'))",message="capability paths must stay inside workspace"
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
	// +required
	Access string `json:"access"`
	// +kubebuilder:validation:MaxItems=100
	// +kubebuilder:validation:items:Pattern="^[a-zA-Z0-9]([-a-zA-Z0-9]*[a-zA-Z0-9])?(\\.[a-zA-Z0-9]([-a-zA-Z0-9]*[a-zA-Z0-9])?)*$"
	// +kubebuilder:validation:items:MaxLength=253
	// +listType=set
	// +kubebuilder:default={}
	// +optional
	// +kubebuilder:validation:items:MinLength=1
	// +kubebuilder:validation:items:MaxLength=256
	// +kubebuilder:validation:XValidation:rule="self.all(h, !h.contains('..'))",message="domains must be exact host names"
	AllowedDomains []string `json:"allowedDomains,omitempty"`
	// +kubebuilder:validation:MaxItems=100
	// +kubebuilder:validation:items:Pattern="^[a-zA-Z0-9]([-a-zA-Z0-9]*[a-zA-Z0-9])?(\\.[a-zA-Z0-9]([-a-zA-Z0-9]*[a-zA-Z0-9])?)*$"
	// +kubebuilder:validation:items:MaxLength=253
	// +listType=set
	// +kubebuilder:default={}
	// +optional
	// +kubebuilder:validation:XValidation:rule="self.all(h, !h.contains('..'))",message="domains must be exact host names"
	BlockedDomains []string `json:"blockedDomains,omitempty"`
}

// HostedPackages contains non-secret package specifiers.
type HostedPackages struct {
	// +kubebuilder:validation:MaxItems=64
	// +kubebuilder:validation:items:MaxLength=256
	// +optional
	// +listType=atomic
	// +kubebuilder:validation:items:MinLength=1
	// +kubebuilder:validation:items:MaxLength=256
	Python []string `json:"python,omitempty"`
	// +kubebuilder:validation:MaxItems=64
	// +kubebuilder:validation:items:MaxLength=256
	// +optional
	// +listType=atomic
	// +kubebuilder:validation:items:MinLength=1
	// +kubebuilder:validation:items:MaxLength=256
	System []string `json:"system,omitempty"`
	// +kubebuilder:validation:MaxItems=64
	// +kubebuilder:validation:items:MaxLength=256
	// +optional
	// +listType=atomic
	// +kubebuilder:validation:items:MinLength=1
	// +kubebuilder:validation:items:MaxLength=256
	NPM []string `json:"npm,omitempty"`
}

// +kubebuilder:validation:XValidation:rule="self.name != 'PATH' && self.name != 'OPENAI_API_KEY' && !self.name.startsWith('CODEX_')",message="Runtime-reserved environment names are restricted"
// HostedVariable reads values from the platform's codex-environment Secret.
type HostedVariable struct {
	// +kubebuilder:validation:Pattern="^[A-Za-z_][A-Za-z0-9_]{0,127}$"
	// +required
	Name string `json:"name"`
	// +kubebuilder:validation:Pattern="^[a-zA-Z0-9._-]{1,253}$"
	// +required
	SecretKey string `json:"secretKey"`
}

// HostedFileType selects a protected source and is translated to the provider file type.
type HostedFileType string

const (
	HostedFileTypeFileID HostedFileType = "FileID"
	HostedFileTypeInline HostedFileType = "Inline"
)

// HostedSkillType selects a pinned provider reference or a protected archive.
type HostedSkillType string

const (
	HostedSkillTypeSkillReference HostedSkillType = "SkillReference"
	HostedSkillTypeInline         HostedSkillType = "Inline"
)

// HostedFile never stores inline bytes in the API.
// +kubebuilder:validation:XValidation:rule="(self.type == 'FileID') == has(self.fileID)",message="fileID branch must match type"
// +kubebuilder:validation:XValidation:rule="(self.type == 'Inline') == has(self.secretKey)",message="inline branch must match type"
type HostedFile struct {
	// +kubebuilder:validation:Enum=FileID;Inline
	// +required
	Type HostedFileType `json:"type"`
	// +kubebuilder:validation:Pattern="^/workspace/[^/]+(/[^/]+)*$"
	// +kubebuilder:validation:XValidation:rule="self.split('/').all(p, p != '.' && p != '..')",message="path must be clean"
	// +kubebuilder:validation:MaxLength=4096
	// +required
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
	// +required
	SecretKey string `json:"secretKey"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=128
	// +required
	SecretUID string `json:"secretUID"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=128
	// +required
	SecretResourceVersion string `json:"secretResourceVersion"`
}

// HostedInlineCapability carries safe metadata and a protected archive reference.
type HostedInlineCapability struct {
	// +kubebuilder:validation:Pattern="^[a-z][a-z0-9-]{0,63}$"
	// +required
	Name string `json:"name"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=2048
	// +required
	Description string `json:"description"`
	// +required
	Source HostedArchive `json:"source"`
}

// HostedSkill selects an exact API version or a pinned protected archive.
// +kubebuilder:validation:XValidation:rule="(self.type == 'SkillReference') == (has(self.skillID) && has(self.version))",message="skillReference requires ID and concrete version"
// +kubebuilder:validation:XValidation:rule="(self.type == 'Inline') == has(self.inline)",message="inline branch must match type"
// +kubebuilder:validation:XValidation:rule="self.type == 'SkillReference' || (!has(self.skillID) && !has(self.version))",message="inline skill cannot carry reference fields"
type HostedSkill struct {
	// +kubebuilder:validation:Enum=SkillReference;Inline
	// +required
	Type HostedSkillType `json:"type"`
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
	// +required
	SecretKey string `json:"secretKey"`
	// +kubebuilder:default="/workspace"
	// +kubebuilder:validation:Pattern="^/workspace(/[^/]+)*$"
	// +kubebuilder:validation:XValidation:rule="self.split('/').all(p, p != '.' && p != '..')",message="working directory must be clean"
	// +kubebuilder:validation:MaxLength=4096
	// +optional
	WorkingDirectory string `json:"workingDirectory,omitempty"`
}

// EnvironmentSourceVersion identifies protected material without hashing values.
type EnvironmentSourceVersion struct {
	// +required
	UID string `json:"uid"`
	// +required
	ResourceVersion string `json:"resourceVersion"`
}

// EnvironmentStatus records identity, acknowledged intent and audit candidates.
type EnvironmentStatus struct {
	// +optional
	SandboxPool         *SandboxPoolSnapshot `json:"sandboxPool,omitempty"`
	CreationAttempted   bool                 `json:"creationAttempted,omitempty"`
	OwnerScope          string               `json:"ownerScope,omitempty"`
	CreationName        string               `json:"creationName,omitempty"`
	OpenAITemplateID    string               `json:"openaiTemplateID,omitempty"`
	ConfigurationDigest string               `json:"configurationDigest,omitempty"`
	// TemplateConfigurationDigest excludes container size and local deletion policy.
	TemplateConfigurationDigest string `json:"templateConfigurationDigest,omitempty"`
	// +optional
	SourceVersion *EnvironmentSourceVersion `json:"sourceVersion,omitempty"`
	// Audit candidates are never adopted automatically.
	// +kubebuilder:validation:MaxItems=1000
	// +optional
	// +listType=set
	// +kubebuilder:validation:items:MinLength=1
	// +kubebuilder:validation:items:MaxLength=256
	AuditCandidateIDs []string `json:"auditCandidateIDs,omitempty"`
	// +listType=map
	// +listMapKey=type
	// +optional
	// +kubebuilder:validation:MaxItems=16
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
