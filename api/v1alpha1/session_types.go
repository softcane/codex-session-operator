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

// SessionSpec defines the desired state of Session.
type SessionSpec struct {
	// SpendControl explicitly selects a per-Session OpenAI spend limit or unlimited spending.
	SpendControl *SessionSpendControl `json:"spendControl"`
	// VaultRefs attach same-namespace Vaults at creation; later attachment changes require a new Session.
	// +optional
	// +kubebuilder:validation:MaxItems=16
	// +listType=map
	// +listMapKey=name
	VaultRefs []AgentReference `json:"vaultRefs,omitempty"`
	// Agent overrides saved Agent settings for this Session.
	// +optional
	Agent *SessionAgentOverrides `json:"agent,omitempty"`
	// +optional
	// +kubebuilder:validation:MaxProperties=13
	// +kubebuilder:validation:XValidation:rule="self.all(k, size(k) >= 1 && size(k) <= 64 && size(self[k]) <= 512 && !(k in ['kubernetes_uid', 'operator', 'owner_scope']))",message="metadata exceeds limits or uses ownership keys"
	Metadata *map[string]string `json:"metadata,omitempty"`

	// EnvironmentRef selects a same-namespace Environment.
	EnvironmentRef *AgentReference `json:"environmentRef"`
	// AgentRef selects a saved Agent in this namespace.
	AgentRef *AgentReference `json:"agentRef"`
}

// SessionSpendControl models the OpenAI Session spend control. A limited
// Session must set a positive limit in US cents; unlimited has no limit value.
// +kubebuilder:validation:XValidation:rule="(self.mode == 'limited') == has(self.limitCents)",message="limited spending requires limitCents; unlimited spending forbids it"
type SessionSpendControl struct {
	// +kubebuilder:validation:Enum=limited;unlimited
	Mode string `json:"mode"`
	// LimitCents is a positive US-cent amount when mode is limited.
	// +optional
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=4503599627370495
	LimitCents *int64 `json:"limitCents,omitempty"`
}

// SessionAgentOverrides distinguishes omission from explicit empty, false and resets.
// +kubebuilder:validation:XValidation:rule="!(has(self.reasoning) && has(self.resetReasoning) && self.resetReasoning)",message="reasoning and resetReasoning conflict"
// +kubebuilder:validation:XValidation:rule="!(has(self.tools) && has(self.clearTools) && self.clearTools)",message="tools and clearTools conflict"
// +kubebuilder:validation:XValidation:rule="!(has(self.serviceTier) && has(self.resetServiceTier) && self.resetServiceTier)",message="tier and resetServiceTier conflict"
type SessionAgentOverrides struct {
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=128
	Model *string `json:"model,omitempty"`
	// +optional
	// +kubebuilder:validation:MaxLength=65536
	Instructions *string `json:"instructions,omitempty"`
	// +optional
	Reasoning *AgentReasoning `json:"reasoning,omitempty"`
	// +optional
	ResetReasoning bool `json:"resetReasoning,omitempty"`
	// +optional
	// +kubebuilder:validation:Enum=auto;default;flex;priority;fast
	ServiceTier *string `json:"serviceTier,omitempty"`
	// +optional
	ResetServiceTier bool `json:"resetServiceTier,omitempty"`
	// +optional
	Text *AgentText `json:"text,omitempty"`
	// +optional
	MultiAgent *AgentMultiAgent `json:"multiAgent,omitempty"`
	// +optional
	// +kubebuilder:validation:MaxItems=64
	// +listType=atomic
	Tools *[]AgentTool `json:"tools,omitempty"`
	// ClearTools sends an explicit null at creation.
	// +optional
	ClearTools bool `json:"clearTools,omitempty"`
}

// SessionCredentialSnapshot freezes public credential identity and acknowledged source versions.
type SessionCredentialSnapshot struct {
	Name               string `json:"name"`
	UID                string `json:"uid"`
	Generation         int64  `json:"generation"`
	OpenAICredentialID string `json:"openaiCredentialID"`
	AuthType           string `json:"authType"`
	// +optional
	SourceVersions []CredentialSourceVersion `json:"sourceVersions,omitempty"`
}

// SessionVaultSnapshot freezes each attached Vault and its selected credentials.
type SessionVaultSnapshot struct {
	Name          string `json:"name"`
	UID           string `json:"uid"`
	Generation    int64  `json:"generation"`
	OpenAIVaultID string `json:"openaiVaultID"`
	// +optional
	// +kubebuilder:validation:MaxItems=1000
	Credentials []SessionCredentialSnapshot `json:"credentials,omitempty"`
}

// AgentReference cannot address another namespace.
type AgentReference struct {
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern="^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$"
	Name string `json:"name"`
}

// SessionAgentSnapshot freezes the dependency selected by the creation claim.
type SessionAgentSnapshot struct {
	Name                string `json:"name"`
	UID                 string `json:"uid"`
	Generation          int64  `json:"generation"`
	OpenAIAgentID       string `json:"openaiAgentID"`
	Model               string `json:"model"`
	ConfigurationDigest string `json:"configurationDigest"`
}

// SessionStatus defines the observed state of Session.
// SessionEnvironmentSnapshot freezes only safe configuration and protected source versions.
type SessionEnvironmentSnapshot struct {
	Name                string          `json:"name"`
	UID                 string          `json:"uid"`
	Generation          int64           `json:"generation"`
	OpenAITemplateID    string          `json:"openaiTemplateID,omitempty"`
	ConfigurationDigest string          `json:"configurationDigest"`
	ObservableDigest    string          `json:"observableDigest,omitempty"`
	Profile             EnvironmentSpec `json:"profile"`
	// +optional
	SourceVersion *EnvironmentSourceVersion `json:"sourceVersion,omitempty"`
}

// SessionStatus defines observed state, never protected content.
type SessionStatus struct {
	// WorkspacePreparation records preparation evidence for one observed Pod.
	// Missing or replaced Pods invalidate this evidence before execution resumes.
	// +optional
	WorkspacePreparation *SessionWorkspacePreparationStatus `json:"workspacePreparation,omitempty"`
	// WorkspaceClaimCreationAttempted is a durable one-time claim for an
	// unowned Retain PVC. Unknown outcomes require administrative audit.
	WorkspaceClaimCreationAttempted bool `json:"workspaceClaimCreationAttempted,omitempty"`
	// WorkspaceClaimUID pins the Session-owned PVC identity across Pod replacement.
	// Empty means an ephemeral workspace or an unobserved claim.
	WorkspaceClaimUID string `json:"workspaceClaimUID,omitempty"`
	// +optional
	// +kubebuilder:validation:MaxItems=16
	VaultSnapshots []SessionVaultSnapshot `json:"vaultSnapshots,omitempty"`
	// CreationAgentDigest covers effective creation settings, never their payloads.
	CreationAgentDigest  string `json:"creationAgentDigest,omitempty"`
	CreationOnlyDigest   string `json:"creationOnlyDigest,omitempty"`
	AppliedMutableDigest string `json:"appliedMutableDigest,omitempty"`
	// SpendControlAttemptedDigest records a claimed remote spend-control create
	// or update whose outcome must be observed before another write is attempted.
	SpendControlAttemptedDigest string `json:"spendControlAttemptedDigest,omitempty"`
	// +optional
	// +kubebuilder:validation:MaxItems=26
	// +listType=set
	ManagedMetadataKeys []string `json:"managedMetadataKeys,omitempty"`
	OwnerScope          string   `json:"ownerScope,omitempty"`

	// +optional
	EnvironmentSnapshot *SessionEnvironmentSnapshot `json:"environmentSnapshot,omitempty"`
	// ResolvedCapabilityDirectories freezes public provider installation paths after creation.
	// +kubebuilder:validation:MaxItems=64
	// +listType=set
	// +optional
	ResolvedCapabilityDirectories []string `json:"resolvedCapabilityDirectories,omitempty"`
	// AgentSnapshot is frozen before creation, including ambiguous outcomes.
	// +optional
	AgentSnapshot *SessionAgentSnapshot `json:"agentSnapshot,omitempty"`
	// CreationAttempted is persisted before the only allowed remote create call.
	// Never reset it to retry an ambiguous create; discover by Kubernetes UID.
	CreationAttempted bool `json:"creationAttempted,omitempty"`

	// Phase is the current high-level lifecycle state.
	// Examples: Pending, Creating, Connecting, Ready, Failed.
	Phase string `json:"phase,omitempty"`

	// OpenAISessionID is the session ID returned by OpenAI.
	OpenAISessionID string `json:"openaiSessionID,omitempty"`

	// EnvironmentID identifies the OpenAI self-hosted environment
	// associated with this session.
	EnvironmentID string `json:"environmentID,omitempty"`

	// RemoteURL is the connection URL used by codex exec-server.
	RemoteURL string `json:"remoteURL,omitempty"`

	// PodName is the Kubernetes Pod running codex exec-server.
	PodName string `json:"podName,omitempty"`

	// ExecutorImage freezes the operator-selected image digest for this Session.
	ExecutorImage string `json:"executorImage,omitempty"`

	// Conditions represent detailed Kubernetes-style status information.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// SessionWorkspacePreparationStatus binds safe preparation evidence to compute and storage.
type SessionWorkspacePreparationStatus struct {
	PodUID string `json:"podUID"`
	// ContainerID and StartedAt identify the executor process generation inside the Pod.
	ContainerID  string      `json:"containerID"`
	RestartCount int32       `json:"restartCount"`
	StartedAt    metav1.Time `json:"startedAt"`
	// +optional
	ClaimUID     string `json:"claimUID,omitempty"`
	ConfigDigest string `json:"configDigest"`
	// +optional
	RepositoryDigest string `json:"repositoryDigest,omitempty"`
	// +optional
	InitialCommit string `json:"initialCommit,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:validation:XValidation:rule="has(self.spec.agentRef) && has(self.spec.environmentRef) && has(self.spec.spendControl)",message="Sessions require agentRef, environmentRef and spendControl"

// Session is the Schema for the sessions API
type Session struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of Session
	// +required
	Spec SessionSpec `json:"spec"`

	// status defines the observed state of Session
	// +optional
	Status SessionStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// SessionList contains a list of Session
type SessionList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []Session `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &Session{}, &SessionList{})
		return nil
	})
}
