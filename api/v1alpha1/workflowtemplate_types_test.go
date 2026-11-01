package v1alpha1

import "testing"

const _workflowDiagnosisRole = "diagnose"

const _workflowProjectName = "project"

func TestValidateWorkflowTemplateSpec(t *testing.T) {
	good := WorkflowTemplateSpec{Coordinator: WorkflowCoordinator{ProfileRef: AgentReference{Name: _workflowProjectName}}, Roles: []WorkflowRole{{Name: _workflowDiagnosisRole, ProfileRef: AgentReference{Name: _workflowProjectName}, Outputs: []string{"observations"}}, {Name: "implement", ProfileRef: AgentReference{Name: _workflowProjectName}, DependsOn: []string{_workflowDiagnosisRole}}}}
	if err := ValidateWorkflowTemplateSpec(good); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		change func(*WorkflowTemplateSpec)
	}{
		{"duplicate role", func(s *WorkflowTemplateSpec) { s.Roles[1].Name = _workflowDiagnosisRole }},
		{"missing dependency", func(s *WorkflowTemplateSpec) { s.Roles[1].DependsOn = []string{"absent"} }},
		{"cycle", func(s *WorkflowTemplateSpec) { s.Roles[0].DependsOn = []string{"implement"} }},
		{"cross namespace reference", func(s *WorkflowTemplateSpec) { s.Coordinator.ProfileRef.Name = "other/project" }},
		{"duplicate output", func(s *WorkflowTemplateSpec) { s.Roles[0].Outputs = []string{"result", "result"} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := good
			s.Roles = append([]WorkflowRole(nil), good.Roles...)
			tc.change(&s)
			if ValidateWorkflowTemplateSpec(s) == nil {
				t.Fatal("accepted invalid spec")
			}
		})
	}
}

func TestWorkflowTemplateSpecDigest(t *testing.T) {
	spec := WorkflowTemplateSpec{Coordinator: WorkflowCoordinator{ProfileRef: AgentReference{Name: _workflowProjectName}}, Roles: []WorkflowRole{{Name: "b", Outputs: []string{"z", "a"}}, {Name: "a"}}}
	digest := WorkflowTemplateSpecDigest(spec)
	if spec.Roles[0].Outputs[0] != "z" {
		t.Fatal("digest mutated caller")
	}
	reordered := WorkflowTemplateSpec{Coordinator: spec.Coordinator, Roles: []WorkflowRole{{Name: "a"}, {Name: "b", Outputs: []string{"a", "z"}}}}
	if WorkflowTemplateSpecDigest(reordered) != digest {
		t.Fatal("ordering changed meaning")
	}
	reordered.Coordinator.Instructions = "new instructions"
	if WorkflowTemplateSpecDigest(reordered) == digest {
		t.Fatal("instructions missing from digest")
	}
}
