package v1alpha1

import (
	"crypto/sha256"
	"encoding/hex"
	"slices"
	"strconv"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// ProfileSpec binds a namespaced recipe for future conversations. It contains
// references and spending policy, never input, instructions or credentials.
type ProfileSpec struct {
	AgentRef       AgentReference      `json:"agentRef"`
	EnvironmentRef AgentReference      `json:"environmentRef"`
	SpendControl   SessionSpendControl `json:"spendControl"`
	// +optional
	// +kubebuilder:validation:MaxItems=16
	// +listType=map
	// +listMapKey=name
	VaultRefs []AgentReference `json:"vaultRefs,omitempty"`
}

// ProfileDependencyStatus identifies one observed same-namespace dependency.
type ProfileDependencyStatus struct {
	Name       string `json:"name"`
	UID        string `json:"uid"`
	Generation int64  `json:"generation"`
}

// ProfileStatus records safe policy provenance and current dependency readiness.
// Readiness does not establish that an executor or model task has run.
type ProfileStatus struct {
	ObservedGeneration int64  `json:"observedGeneration,omitempty"`
	PolicyDigest       string `json:"policyDigest,omitempty"`
	// +optional
	Agent *ProfileDependencyStatus `json:"agent,omitempty"`
	// +optional
	Environment *ProfileDependencyStatus `json:"environment,omitempty"`
	// +optional
	// +kubebuilder:validation:MaxItems=16
	// +listType=map
	// +listMapKey=name
	Vaults []ProfileDependencyStatus `json:"vaults,omitempty"`
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// Profile publishes an author-managed recipe without owning remote resources,
// dependencies or the Sessions accepted from it.
type Profile struct {
	metav1.TypeMeta `json:",inline"`

	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`
	Spec              ProfileSpec `json:"spec"`
	// +optional
	Status ProfileStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// ProfileList contains a list of Profile.
type ProfileList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []Profile `json:"items"`
}

// ProfilePolicyDigest covers the complete safe policy. Vault ordering does not
// alter selection; length prefixes keep arbitrary field values unambiguous.
// This helper does not modify the caller's spec or hash protected values.
func ProfilePolicyDigest(spec ProfileSpec) string {
	limit := "none"
	if spec.SpendControl.LimitCents != nil {
		limit = strconv.FormatInt(*spec.SpendControl.LimitCents, 10)
	}
	parts := make([]string, 0, 5+len(spec.VaultRefs))
	parts = append(parts, "profile-policy-v1", spec.AgentRef.Name, spec.EnvironmentRef.Name, spec.SpendControl.Mode, limit)
	vaults := make([]string, 0, len(spec.VaultRefs))
	for _, ref := range spec.VaultRefs {
		vaults = append(vaults, ref.Name)
	}
	slices.Sort(vaults)
	parts = append(parts, vaults...)
	var canonical strings.Builder
	for _, part := range parts {
		canonical.WriteString(strconv.Itoa(len(part)))
		canonical.WriteByte(':')
		canonical.WriteString(part)
	}
	digest := sha256.Sum256([]byte(canonical.String()))
	return hex.EncodeToString(digest[:])
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &Profile{}, &ProfileList{})
		return nil
	})
}
