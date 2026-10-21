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
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// AgentSpec declares credential-free saved configuration. Omitted optional fields
// relinquish management and preserve remote values; explicit empties clear them.
// +kubebuilder:validation:XValidation:rule="!(has(self.name) && has(self.clearName) && self.clearName)",message="name and clearName conflict"
// +kubebuilder:validation:XValidation:rule="!(has(self.reasoning) && has(self.resetReasoning) && self.resetReasoning)",message="reasoning and resetReasoning conflict"
type AgentSpec struct {
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=128
	// +required
	Model string `json:"model"`
	// +optional
	// +kubebuilder:validation:MaxLength=128
	Name *string `json:"name,omitempty"`
	// +optional
	ClearName bool `json:"clearName,omitempty"`
	// +optional
	// +kubebuilder:validation:MaxLength=65536
	Instructions *string `json:"instructions,omitempty"`
	// Metadata permits 13 user entries, reserving three ownership entries.
	// +optional
	// +kubebuilder:validation:MaxProperties=13
	// +kubebuilder:validation:XValidation:rule="self.all(k, size(k) >= 1 && size(k) <= 64 && size(self[k]) <= 512 && !(k in ['kubernetes_uid', 'operator', 'owner_scope']))",message="metadata exceeds limits or uses reserved ownership keys"
	Metadata *map[string]string `json:"metadata,omitempty"`
	// +optional
	Reasoning *AgentReasoning `json:"reasoning,omitempty"`
	// +optional
	ResetReasoning bool `json:"resetReasoning,omitempty"`
	// +optional
	// +kubebuilder:validation:Enum=auto;default;flex;priority;fast
	ServiceTier *string `json:"serviceTier,omitempty"`
	// +optional
	Text *AgentText `json:"text,omitempty"`
	// +optional
	MultiAgent *AgentMultiAgent `json:"multiAgent,omitempty"`
	// Tools replaces the entire remote list when supplied, including an empty list.
	// +optional
	// +kubebuilder:validation:MaxItems=64
	// +listType=atomic
	Tools *[]AgentTool `json:"tools,omitempty"`
	// +kubebuilder:validation:Enum=Delete;Retain
	// +required
	DeletionPolicy DeletionPolicy `json:"deletionPolicy"`
}

// AgentReasoning replaces the complete remote reasoning object.
type AgentReasoning struct {
	// +optional
	// +kubebuilder:validation:Enum=none;minimal;low;medium;high;xhigh;max
	Effort *string `json:"effort,omitempty"`
	// +optional
	// +kubebuilder:validation:Enum=concise;detailed;auto
	Summary *string `json:"summary,omitempty"`
}

// AgentText replaces the complete remote output configuration.
type AgentText struct {
	// +optional
	// +kubebuilder:validation:Enum=low;medium;high
	Verbosity *string `json:"verbosity,omitempty"`
	// +optional
	Format *AgentTextFormat `json:"format,omitempty"`
}

// AgentTextFormat is a discriminated text/JSON schema union.
// +kubebuilder:validation:XValidation:rule="(self.type == 'json_schema') == has(self.schema)",message="json_schema requires schema; text forbids schema"
type AgentTextFormat struct {
	// +kubebuilder:validation:Enum=text;json_schema
	// +required
	Type string `json:"type"`
	// Schema is an object containing a JSON Schema, not arbitrary Agent configuration.
	// +optional
	// +kubebuilder:validation:Type=object
	// +kubebuilder:pruning:PreserveUnknownFields
	Schema *apiextensionsv1.JSON `json:"schema,omitempty"`
}

// AgentMultiAgent controls subagent concurrency. Function tools cannot be used by subagents.
type AgentMultiAgent struct {
	// +required
	Enabled bool `json:"enabled"`
	// +optional
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=32
	MaxConcurrentSubagents *int64 `json:"maxConcurrentSubagents,omitempty"`
}

// AgentTool is a discriminated union; fields from other variants are forbidden.
// +kubebuilder:validation:XValidation:rule="(self.type == 'function') == has(self.function)",message="function branch must match type"
// +kubebuilder:validation:XValidation:rule="(self.type == 'programmatic_tool_calling') == has(self.programmaticToolCalling)",message="programmatic branch must match type"
// +kubebuilder:validation:XValidation:rule="(self.type == 'mcp') == has(self.mcp)",message="mcp branch must match type"
// +kubebuilder:validation:XValidation:rule="(self.type == 'web_search') == has(self.webSearch)",message="webSearch branch must match type"
// +kubebuilder:validation:XValidation:rule="(self.type == 'computer_use') == has(self.computerUse)",message="computerUse branch must match type"
type AgentTool struct {
	// +kubebuilder:validation:Enum=function;tool_search;programmatic_tool_calling;mcp;web_search;computer_use
	// +required
	Type string `json:"type"`
	// +optional
	Function *AgentFunction `json:"function,omitempty"`
	// +optional
	ProgrammaticToolCalling *AgentProgrammaticToolCalling `json:"programmaticToolCalling,omitempty"`
	// +optional
	MCP *AgentMCP `json:"mcp,omitempty"`
	// +optional
	WebSearch *AgentWebSearch `json:"webSearch,omitempty"`
	// +optional
	ComputerUse *AgentComputerUse `json:"computerUse,omitempty"`
}

// AgentFunction declares an application tool; execution belongs to the gateway.
type AgentFunction struct {
	// +kubebuilder:validation:Pattern="^[A-Za-z0-9_-]{1,64}$"
	// +required
	Name string `json:"name"`
	// +kubebuilder:validation:MaxLength=8192
	// +required
	Description string `json:"description"`
	// +kubebuilder:validation:Type=object
	// +kubebuilder:pruning:PreserveUnknownFields
	// +required
	Parameters apiextensionsv1.JSON `json:"parameters"`
	// +optional
	DeferLoading *bool `json:"deferLoading,omitempty"`
}

// AgentProgrammaticToolCalling preserves explicit disabled configuration.
type AgentProgrammaticToolCalling struct {
	// +optional
	Enabled *bool `json:"enabled,omitempty"`
}

// AgentComputerUse requires hosted desktops and gateway approval handling.
type AgentComputerUse struct {
	// +optional
	IncludeScreenshots *bool `json:"includeScreenshots,omitempty"`
}

// AgentMCP declares a credential-free MCP server.
// +kubebuilder:validation:XValidation:rule="self.transport.type != 'stdio' || !has(self.connectionOrigin)",message="stdio MCP must omit connectionOrigin"
// +kubebuilder:validation:XValidation:rule="!has(self.credentialID) || (self.transport.type == 'http' && (!has(self.connectionOrigin) || self.connectionOrigin == 'service'))",message="Vault credentials require service-origin HTTP MCP"
type AgentMCP struct {
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=64
	// +required
	ServerLabel string `json:"serverLabel"`
	// +required
	Transport AgentMCPTransport `json:"transport"`
	// CredentialID references a Vault credential; it never contains a token.
	// +optional
	// +kubebuilder:validation:Pattern="^[A-Za-z0-9_-]{1,128}$"
	CredentialID *string `json:"credentialID,omitempty"`
	// +optional
	Required *bool `json:"required,omitempty"`
	// +optional
	// +kubebuilder:validation:MaxItems=128
	// +listType=atomic
	// +kubebuilder:validation:items:MinLength=1
	// +kubebuilder:validation:items:MaxLength=256
	AllowedTools *[]string `json:"allowedTools,omitempty"`
	// +optional
	// +kubebuilder:validation:Enum=service;environment
	ConnectionOrigin *string `json:"connectionOrigin,omitempty"`
	// RequestMetadata is limited to non-secret tracing fields by platform policy.
	// +optional
	// +kubebuilder:validation:MaxProperties=2
	// +kubebuilder:validation:XValidation:rule="self.all(k, k in ['traceparent', 'tracestate'] && size(self[k]) <= 512)",message="only non-secret tracing metadata is permitted"
	RequestMetadata *map[string]string `json:"requestMetadata,omitempty"`
}

// AgentMCPTransport permits exactly one HTTP or stdio branch.
// +kubebuilder:validation:XValidation:rule="(self.type == 'http') == has(self.http)",message="http branch must match type"
// +kubebuilder:validation:XValidation:rule="(self.type == 'stdio') == has(self.stdio)",message="stdio branch must match type"
type AgentMCPTransport struct {
	// +kubebuilder:validation:Enum=http;stdio
	// +required
	Type string `json:"type"`
	// +optional
	HTTP *AgentMCPHTTP `json:"http,omitempty"`
	// +optional
	Stdio *AgentMCPStdio `json:"stdio,omitempty"`
}

// AgentMCPHTTP excludes authorization headers and credentials in the URL.
type AgentMCPHTTP struct {
	// +kubebuilder:validation:MaxLength=2048
	// +kubebuilder:validation:Pattern="^https://[^/@?#]+(/[^?#]*)?$"
	// +required
	ServerURL string `json:"serverURL"`
	// +optional
	// +kubebuilder:validation:MaxProperties=2
	// +kubebuilder:validation:XValidation:rule="self.all(k, k in ['Accept', 'Content-Type'] && self[k] in ['application/json', 'text/event-stream'])",message="only credential-free protocol headers are permitted"
	Headers *map[string]string `json:"headers,omitempty"`
}

// AgentMCPStdio inherits named variables; it does not persist environment values.
type AgentMCPStdio struct {
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=256
	// +required
	Command string `json:"command"`
	// +kubebuilder:validation:Pattern="^/"
	// +kubebuilder:validation:MaxLength=4096
	// +required
	WorkingDirectory string `json:"workingDirectory"`
	// Args must contain non-secret arguments only.
	// +optional
	// +kubebuilder:validation:MaxItems=64
	// +listType=atomic
	// +kubebuilder:validation:items:MaxLength=4096
	Args *[]string `json:"args,omitempty"`
	// +optional
	// +kubebuilder:validation:MaxItems=64
	// +listType=atomic
	// +kubebuilder:validation:items:MinLength=1
	// +kubebuilder:validation:items:MaxLength=256
	// +kubebuilder:validation:items:Pattern="^[A-Za-z_][A-Za-z0-9_]{0,127}$"
	EnvVars *[]string `json:"envVars,omitempty"`
}

// AgentWebSearch declares web search configuration.
type AgentWebSearch struct {
	// +optional
	// +kubebuilder:validation:MaxItems=128
	// +listType=atomic
	// +kubebuilder:validation:items:MinLength=1
	// +kubebuilder:validation:items:MaxLength=256
	AllowedDomains *[]string `json:"allowedDomains,omitempty"`
	// +optional
	// +kubebuilder:validation:Enum=low;medium;high
	ContextSize *string `json:"contextSize,omitempty"`
	// +optional
	Location *AgentWebLocation `json:"location,omitempty"`
	// +optional
	// +kubebuilder:validation:Enum=disabled;cached;live
	Mode *string `json:"mode,omitempty"`
}

// AgentWebLocation is an approximate location, not a credential.
type AgentWebLocation struct {
	// +optional
	// +kubebuilder:validation:MaxLength=128
	City *string `json:"city,omitempty"`
	// +optional
	// +kubebuilder:validation:Pattern="^[A-Z]{2}$"
	Country *string `json:"country,omitempty"`
	// +optional
	// +kubebuilder:validation:MaxLength=128
	Region *string `json:"region,omitempty"`
	// +optional
	// +kubebuilder:validation:MaxLength=128
	Timezone *string `json:"timezone,omitempty"`
}

// AgentStatus stores identity and observations, never instructions or tool payloads.
type AgentStatus struct {
	// ReasoningConfigurationDigest acknowledges whole-object replacements and resets.
	ReasoningConfigurationDigest string `json:"reasoningConfigurationDigest,omitempty"`
	// ManagedMetadataKeys tracks ownership of keys, never their values.
	// +optional
	// +kubebuilder:validation:MaxItems=26
	// +listType=set
	// +kubebuilder:validation:items:MinLength=1
	// +kubebuilder:validation:items:MaxLength=256
	ManagedMetadataKeys []string `json:"managedMetadataKeys,omitempty"`
	CreationAttempted   bool     `json:"creationAttempted,omitempty"`
	// OwnerScope freezes the operator installation/project identity before creation.
	OwnerScope         string `json:"ownerScope,omitempty"`
	OpenAIAgentID      string `json:"openaiAgentID,omitempty"`
	ObservedGeneration int64  `json:"observedGeneration,omitempty"`
	// +listType=map
	// +listMapKey=type
	// +optional
	// +kubebuilder:validation:MaxItems=16
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:validation:XValidation:rule="has(self.spec.deletionPolicy)",message="Agents require deletionPolicy"

// Agent is the Schema for the agents API
type Agent struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of Agent
	// +required
	Spec AgentSpec `json:"spec"`

	// status defines the observed state of Agent
	// +optional
	Status AgentStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// AgentList contains a list of Agent
type AgentList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []Agent `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &Agent{}, &AgentList{})
		return nil
	})
}
