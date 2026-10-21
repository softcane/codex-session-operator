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

// VaultSpec owns remote name and metadata. Omission retains the remote value.
// +kubebuilder:validation:XValidation:rule="!(has(self.name) && has(self.clearName) && self.clearName)",message="name and clearName conflict"
type VaultSpec struct {
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=256
	Name *string `json:"name,omitempty"`
	// ClearName requests an explicit null name on update.
	// +optional
	ClearName bool `json:"clearName,omitempty"`
	// +optional
	// +kubebuilder:validation:MaxProperties=13
	// +kubebuilder:validation:XValidation:rule="self.all(k, size(k) >= 1 && size(k) <= 64 && size(self[k]) <= 512 && !(k in ['kubernetes_uid', 'operator', 'owner_scope']))",message="metadata exceeds limits or uses ownership keys"
	Metadata *map[string]string `json:"metadata,omitempty"`
	// +kubebuilder:validation:Enum=Delete;Retain
	// +required
	DeletionPolicy DeletionPolicy `json:"deletionPolicy"`
}

// VaultStatus records only remote identity and public observations.
type VaultStatus struct {
	CreationAttempted  bool   `json:"creationAttempted,omitempty"`
	OwnerScope         string `json:"ownerScope,omitempty"`
	OpenAIVaultID      string `json:"openaiVaultID,omitempty"`
	ObservedGeneration int64  `json:"observedGeneration,omitempty"`
	// ManagedMetadataKeys remembers old and new user keys while a whole-map
	// update is pending, without claiming unrelated remote metadata.
	// +optional
	// +kubebuilder:validation:MaxItems=26
	// +listType=set
	// +kubebuilder:validation:items:MinLength=1
	// +kubebuilder:validation:items:MaxLength=256
	ManagedMetadataKeys []string `json:"managedMetadataKeys,omitempty"`
	// +listType=map
	// +listMapKey=type
	// +optional
	// +kubebuilder:validation:MaxItems=16
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:validation:XValidation:rule="has(self.spec.deletionPolicy)",message="Vaults require deletionPolicy"

// Vault is the Schema for the vaults API
type Vault struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of Vault
	// +required
	Spec VaultSpec `json:"spec"`

	// status defines the observed state of Vault
	// +optional
	Status VaultStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// VaultList contains a list of Vault
type VaultList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []Vault `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &Vault{}, &VaultList{})
		return nil
	})
}
