package v1alpha1

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation"
)

// WorkflowCoordinator configures the Session responsible for choosing assignments.
type WorkflowCoordinator struct {
	// +required
	ProfileRef AgentReference `json:"profileRef"`
	// +optional
	Instructions string `json:"instructions,omitempty"`
}

// WorkflowRole declares an available assignment; roles are not automatically run.
type WorkflowRole struct {
	// +required
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
	// +required
	ProfileRef AgentReference `json:"profileRef"`
	// +optional
	Instructions string `json:"instructions,omitempty"`
	// +optional
	// +listType=set
	DependsOn []string `json:"dependsOn,omitempty"`
	// +optional
	// +listType=set
	Outputs []string `json:"outputs,omitempty"`
}

// WorkflowTemplateSpec binds coordinator and optional role recipes in its namespace.
type WorkflowTemplateSpec struct {
	// +required
	Coordinator WorkflowCoordinator `json:"coordinator"`
	// +optional
	// +listType=map
	// +listMapKey=name
	Roles []WorkflowRole `json:"roles,omitempty"`
}

// WorkflowTemplateStatus records current safe configuration and Profile readiness.
type WorkflowTemplateStatus struct {
	ObservedGeneration int64  `json:"observedGeneration,omitempty"`
	SpecDigest         string `json:"specDigest,omitempty"`
	// +optional
	// +listType=map
	// +listMapKey=name
	Profiles []ProfileDependencyStatus `json:"profiles,omitempty"`
	// +optional
	// +listType=map
	// +listMapKey=type
	// +kubebuilder:validation:MaxItems=16
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// WorkflowTemplate publishes choices for future tasks without creating Sessions.
type WorkflowTemplate struct {
	metav1.TypeMeta `json:",inline"`
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`
	// +required
	Spec WorkflowTemplateSpec `json:"spec"`
	// +optional
	Status WorkflowTemplateStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// WorkflowTemplateList contains WorkflowTemplate objects.
type WorkflowTemplateList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []WorkflowTemplate `json:"items"`
}

// ValidateWorkflowTemplateSpec checks local references and the role dependency graph.
// Profile references have no namespace field and always resolve in the template namespace.
func ValidateWorkflowTemplateSpec(spec WorkflowTemplateSpec) error {
	validRef := func(ref AgentReference) bool { return len(validation.IsDNS1123Subdomain(ref.Name)) == 0 }
	if !validRef(spec.Coordinator.ProfileRef) {
		return errors.New("coordinator requires a valid same-namespace Profile reference")
	}
	roles := make(map[string]WorkflowRole, len(spec.Roles))
	for _, role := range spec.Roles {
		if strings.TrimSpace(role.Name) == "" {
			return errors.New("role name must not be empty")
		}
		if _, exists := roles[role.Name]; exists {
			return fmt.Errorf("duplicate role %q", role.Name)
		}
		if !validRef(role.ProfileRef) {
			return fmt.Errorf("role %q requires a valid same-namespace Profile reference", role.Name)
		}
		for _, labels := range [][]string{role.Outputs, role.DependsOn} {
			seen := make(map[string]bool, len(labels))
			for _, label := range labels {
				if strings.TrimSpace(label) == "" || seen[label] {
					return fmt.Errorf("role %q has an empty or duplicate output/dependency label", role.Name)
				}
				seen[label] = true
			}
		}
		roles[role.Name] = role
	}
	// Kahn's algorithm avoids recursion depth depending on administrator input.
	pending := make(map[string]int, len(roles))
	dependents := make(map[string][]string, len(roles))
	queue := make([]string, 0, len(roles))
	for name, role := range roles {
		pending[name] = len(role.DependsOn)
		if len(role.DependsOn) == 0 {
			queue = append(queue, name)
		}
		for _, dep := range role.DependsOn {
			if _, exists := roles[dep]; !exists {
				return fmt.Errorf("role %q depends on missing role %q", name, dep)
			}
			dependents[dep] = append(dependents[dep], name)
		}
	}
	visited := 0
	for len(queue) > 0 {
		name := queue[0]
		queue = queue[1:]
		visited++
		for _, dependent := range dependents[name] {
			pending[dependent]--
			if pending[dependent] == 0 {
				queue = append(queue, dependent)
			}
		}
	}
	if visited != len(roles) {
		return errors.New("role dependencies contain a cycle")
	}
	return nil
}

// WorkflowTemplateSpecDigest hashes all template content without modifying callers.
// Role, output and dependency ordering do not change their declared meaning.
func WorkflowTemplateSpecDigest(spec WorkflowTemplateSpec) string {
	spec.Roles = slices.Clone(spec.Roles)
	for i := range spec.Roles {
		spec.Roles[i].DependsOn = slices.Clone(spec.Roles[i].DependsOn)
		slices.Sort(spec.Roles[i].DependsOn)
		spec.Roles[i].Outputs = slices.Clone(spec.Roles[i].Outputs)
		slices.Sort(spec.Roles[i].Outputs)
	}
	slices.SortFunc(spec.Roles, func(a, b WorkflowRole) int { return strings.Compare(a.Name, b.Name) })
	data, _ := json.Marshal(spec) // This concrete type contains only JSON-supported fields.
	digest := sha256.Sum256(append([]byte("workflow-template-v1:"), data...))
	return hex.EncodeToString(digest[:])
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &WorkflowTemplate{}, &WorkflowTemplateList{})
		return nil
	})
}
