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
	// +required
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
	// +required
	EnvironmentRef *AgentReference `json:"environmentRef"`
	// AgentRef selects a saved Agent in this namespace.
	// +required
	AgentRef *AgentReference `json:"agentRef"`
}

// SessionSpendMode requires an explicit monetary policy independently of token limits.
type SessionSpendMode string

const (
	SessionSpendModeLimited   SessionSpendMode = "Limited"
	SessionSpendModeUnlimited SessionSpendMode = "Unlimited"
)

// SessionSpendControl models the OpenAI Session spend control. A limited
// Session must set a positive limit in US cents; unlimited has no limit value.
// +kubebuilder:validation:XValidation:rule="(self.mode == 'Limited') == has(self.limitCents)",message="limited spending requires limitCents; unlimited spending forbids it"
type SessionSpendControl struct {
	// +kubebuilder:validation:Enum=Limited;Unlimited
	// +required
	Mode SessionSpendMode `json:"mode"`
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
	// +required
	Name string `json:"name"`
	// +required
	UID string `json:"uid"`
	// +required
	Generation int64 `json:"generation"`
	// +required
	OpenAICredentialID string `json:"openaiCredentialID"`
	// +required
	AuthType string `json:"authType"`
	// +optional
	// +listType=map
	// +listMapKey=name
	// +kubebuilder:validation:MaxItems=16
	SourceVersions []CredentialSourceVersion `json:"sourceVersions,omitempty"`
}

// SessionVaultSnapshot freezes each attached Vault and its selected credentials.
type SessionVaultSnapshot struct {
	// +required
	Name string `json:"name"`
	// +required
	UID string `json:"uid"`
	// +required
	Generation int64 `json:"generation"`
	// +required
	OpenAIVaultID string `json:"openaiVaultID"`
	// +optional
	// +kubebuilder:validation:MaxItems=1000
	// +listType=map
	// +listMapKey=name
	Credentials []SessionCredentialSnapshot `json:"credentials,omitempty"`
}

// AgentReference cannot address another namespace.
type AgentReference struct {
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern="^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$"
	// +required
	Name string `json:"name"`
}

// SessionAgentSnapshot freezes the dependency selected by the creation claim.
type SessionAgentSnapshot struct {
	// +required
	Name string `json:"name"`
	// +required
	UID string `json:"uid"`
	// +required
	Generation int64 `json:"generation"`
	// +required
	OpenAIAgentID string `json:"openaiAgentID"`
	// +required
	Model string `json:"model"`
	// +required
	ConfigurationDigest string `json:"configurationDigest"`
}

// SessionEnvironmentSnapshot freezes only safe configuration and protected source versions.
type SessionEnvironmentSnapshot struct {
	// +required
	Name string `json:"name"`
	// +required
	UID string `json:"uid"`
	// +required
	Generation       int64  `json:"generation"`
	OpenAITemplateID string `json:"openaiTemplateID,omitempty"`
	// +required
	ConfigurationDigest string `json:"configurationDigest"`
	ObservableDigest    string `json:"observableDigest,omitempty"`
	// +required
	Profile EnvironmentSpec `json:"profile"`
	// +optional
	SandboxPool *SandboxPoolSnapshot `json:"sandboxPool,omitempty"`
	// +optional
	SourceVersion *EnvironmentSourceVersion `json:"sourceVersion,omitempty"`
}

// SandboxSecretSource freezes Secret identity without recording protected values.
type SandboxSecretSource struct {
	// +required
	Name string `json:"name"`
	// +required
	UID string `json:"uid"`
}

// SessionStatus defines observed state, never protected content.
type SessionStatus struct {
	// SandboxSecretSources pins the protected sources selected by the template.
	// +optional
	// +listType=map
	// +listMapKey=name
	// +kubebuilder:validation:MaxItems=64
	SandboxSecretSources []SandboxSecretSource `json:"sandboxSecretSources,omitempty"`
	// ExecutorConfigDigest binds current protected source versions and connection epoch.
	ExecutorConfigDigest string `json:"executorConfigDigest,omitempty"`
	// ExecutorConnectionSequence freezes the observed SSE sequence for ExecutorPodUID.
	ExecutorConnectionSequence int64 `json:"executorConnectionSequence,omitempty"`
	// WorkspacePreparation records preparation evidence for one observed Pod.
	// Missing or replaced Pods invalidate this evidence before execution resumes.
	// +optional
	WorkspacePreparation *SessionWorkspacePreparationStatus `json:"workspacePreparation,omitempty"`
	// WorkspaceClaimUID pins the native Sandbox workspace identity across Pod replacement.
	// Empty means the native claim has not been observed.
	WorkspaceClaimUID string `json:"workspaceClaimUID,omitempty"`
	// +optional
	// +kubebuilder:validation:MaxItems=16
	// +listType=map
	// +listMapKey=name
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
	// +kubebuilder:validation:items:MinLength=1
	// +kubebuilder:validation:items:MaxLength=256
	ManagedMetadataKeys []string `json:"managedMetadataKeys,omitempty"`
	OwnerScope          string   `json:"ownerScope,omitempty"`

	// +optional
	EnvironmentSnapshot *SessionEnvironmentSnapshot `json:"environmentSnapshot,omitempty"`
	// ResolvedCapabilityDirectories freezes public provider installation paths after creation.
	// +kubebuilder:validation:MaxItems=64
	// +listType=set
	// +optional
	// +kubebuilder:validation:items:MinLength=1
	// +kubebuilder:validation:items:MaxLength=4096
	ResolvedCapabilityDirectories []string `json:"resolvedCapabilityDirectories,omitempty"`
	// AgentSnapshot is frozen before creation, including ambiguous outcomes.
	// +optional
	AgentSnapshot *SessionAgentSnapshot `json:"agentSnapshot,omitempty"`
	// CreationAttempted is persisted before the only allowed remote create call.
	// Never reset it to retry an ambiguous create; discover by Kubernetes UID.
	CreationAttempted bool `json:"creationAttempted,omitempty"`

	// OpenAISessionID is the session ID returned by OpenAI.
	OpenAISessionID string `json:"openaiSessionID,omitempty"`

	// EnvironmentID identifies the OpenAI self-hosted environment
	// associated with this session.
	EnvironmentID string `json:"environmentID,omitempty"`

	// RemoteURL is the connection URL used by codex exec-server.
	RemoteURL string `json:"remoteURL,omitempty"`

	// SandboxClaimName identifies the Session-owned native claim.
	SandboxClaimName string `json:"sandboxClaimName,omitempty"`
	// SandboxClaimUID freezes claim identity before compute can be trusted.
	SandboxClaimUID string `json:"sandboxClaimUID,omitempty"`

	// SandboxName identifies the claim-owned Agent Sandbox resource.
	SandboxName string `json:"sandboxName,omitempty"`
	// SandboxUID pins the compute owner before executor preparation is accepted.
	SandboxUID string `json:"sandboxUID,omitempty"`

	// ExecutorPodUID identifies the last executor observed under this Sandbox.
	ExecutorPodUID string `json:"executorPodUID,omitempty"`

	// PodName is the Sandbox-owned Pod running codex exec-server.
	PodName string `json:"podName,omitempty"`

	// Conditions represent detailed Kubernetes-style status information.
	// +listType=map
	// +listMapKey=type
	// +optional
	// +kubebuilder:validation:MaxItems=16
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// SessionWorkspacePreparationStatus binds safe preparation evidence to compute and storage.
type SessionWorkspacePreparationStatus struct {
	// +required
	SandboxClaimUID string `json:"sandboxClaimUID"`
	// +required
	SandboxUID string `json:"sandboxUID"`
	// +required
	PodUID string `json:"podUID"`
	// ContainerID and StartTime identify the executor process generation inside the Pod.
	// +required
	ContainerID string `json:"containerID"`
	// +required
	RestartCount int32 `json:"restartCount"`
	// +required
	StartTime metav1.Time `json:"startTime"`
	// +optional
	ClaimUID string `json:"claimUID,omitempty"`
	// +required
	ConfigDigest string `json:"configDigest"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:validation:XValidation:rule="has(self.spec.agentRef) && has(self.spec.environmentRef) && has(self.spec.spendControl)",message="Sessions require agentRef, environmentRef and spendControl"
// +kubebuilder:validation:XValidation:rule="!(has(oldSelf.status) && has(oldSelf.status.creationAttempted) && oldSelf.status.creationAttempted) || (self.spec.agentRef == oldSelf.spec.agentRef)",message="agentRef is immutable after the Session creation claim"
// +kubebuilder:validation:XValidation:rule="!(has(oldSelf.status) && has(oldSelf.status.creationAttempted) && oldSelf.status.creationAttempted) || (self.spec.environmentRef == oldSelf.spec.environmentRef)",message="environmentRef is immutable after the Session creation claim"
// +kubebuilder:validation:XValidation:rule="!(has(oldSelf.status) && has(oldSelf.status.creationAttempted) && oldSelf.status.creationAttempted) || ((has(self.spec.vaultRefs) ? self.spec.vaultRefs : []) == (has(oldSelf.spec.vaultRefs) ? oldSelf.spec.vaultRefs : []))",message="vaultRefs is immutable after the Session creation claim"
// +kubebuilder:validation:XValidation:rule="!(has(oldSelf.status) && has(oldSelf.status.creationAttempted) && oldSelf.status.creationAttempted) || ((has(self.spec.agent) && has(self.spec.agent.instructions)) == (has(oldSelf.spec.agent) && has(oldSelf.spec.agent.instructions)) && (!(has(self.spec.agent) && has(self.spec.agent.instructions)) || self.spec.agent.instructions == oldSelf.spec.agent.instructions))",message="agent.instructions is immutable after the Session creation claim"
// +kubebuilder:validation:XValidation:rule="!(has(oldSelf.status) && has(oldSelf.status.creationAttempted) && oldSelf.status.creationAttempted) || ((has(self.spec.agent) && has(self.spec.agent.tools)) == (has(oldSelf.spec.agent) && has(oldSelf.spec.agent.tools)) && (!(has(self.spec.agent) && has(self.spec.agent.tools)) || self.spec.agent.tools == oldSelf.spec.agent.tools))",message="agent.tools is immutable after the Session creation claim"
// +kubebuilder:validation:XValidation:rule="!(has(oldSelf.status) && has(oldSelf.status.creationAttempted) && oldSelf.status.creationAttempted) || ((has(self.spec.agent) && has(self.spec.agent.text)) == (has(oldSelf.spec.agent) && has(oldSelf.spec.agent.text)) && (!(has(self.spec.agent) && has(self.spec.agent.text)) || self.spec.agent.text == oldSelf.spec.agent.text))",message="agent.text is immutable after the Session creation claim"
// +kubebuilder:validation:XValidation:rule="!(has(oldSelf.status) && has(oldSelf.status.creationAttempted) && oldSelf.status.creationAttempted) || ((has(self.spec.agent) && has(self.spec.agent.multiAgent)) == (has(oldSelf.spec.agent) && has(oldSelf.spec.agent.multiAgent)) && (!(has(self.spec.agent) && has(self.spec.agent.multiAgent)) || self.spec.agent.multiAgent == oldSelf.spec.agent.multiAgent))",message="agent.multiAgent is immutable after the Session creation claim"
// +kubebuilder:validation:XValidation:rule="!(has(oldSelf.status) && has(oldSelf.status.creationAttempted) && oldSelf.status.creationAttempted) || (((has(self.spec.agent) && has(self.spec.agent.clearTools)) ? self.spec.agent.clearTools : false) == ((has(oldSelf.spec.agent) && has(oldSelf.spec.agent.clearTools)) ? oldSelf.spec.agent.clearTools : false))",message="agent.clearTools is immutable after the Session creation claim"
// +kubebuilder:validation:XValidation:rule="!(has(oldSelf.status) && has(oldSelf.status.creationAttempted) && oldSelf.status.creationAttempted) || ((has(self.spec.agent) && has(self.spec.agent.reasoning) && has(self.spec.agent.reasoning.summary)) == (has(oldSelf.spec.agent) && has(oldSelf.spec.agent.reasoning) && has(oldSelf.spec.agent.reasoning.summary)) && (!(has(self.spec.agent) && has(self.spec.agent.reasoning) && has(self.spec.agent.reasoning.summary)) || self.spec.agent.reasoning.summary == oldSelf.spec.agent.reasoning.summary))",message="agent.reasoning.summary is immutable after the Session creation claim"

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
