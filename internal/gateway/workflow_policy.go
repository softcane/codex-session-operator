package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func (g *Gateway) workflowMemberPolicy(ctx context.Context, namespace string, member *TaskMember, instructions string, spec agentsv1alpha1.WorkflowTemplateSpec) error {
	var agent agentsv1alpha1.Agent
	if err := g.reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: member.Route.Agent}, &agent); err != nil {
		return err
	}
	if member.Profile == nil || string(agent.UID) != member.Profile.Agent.UID {
		return errors.New("workflow Agent identity changed")
	}
	tools := []agentsv1alpha1.AgentTool{}
	if agent.Spec.Tools != nil {
		for _, tool := range *agent.Spec.Tools {
			if tool.Function != nil && strings.HasPrefix(tool.Function.Name, "workflow_") {
				return errors.New("workflow tool names are reserved")
			}
			tools = append(tools, *tool.DeepCopy())
		}
	}
	tools = append(tools, workflowTools(member.IsCoordinator)...)
	if len(tools) > 64 {
		return errors.New("workflow tools exceed Session tool capacity")
	}
	base := ""
	if agent.Spec.Instructions != nil {
		base = *agent.Spec.Instructions
	}
	text := base + "\n\n" + instructions + "\n\nConfigured workflow role: " + member.Role + ". Save explicit terminal evidence with workflow_result; normal turn completion is not a saved result. Evidence is reported by this Session unless independently checked. Keep each complete result JSON within 256 KiB."
	if !member.IsCoordinator {
		for _, role := range spec.Roles {
			if role.Name == member.Role {
				raw, _ := json.Marshal(role.Outputs)
				text += "\nRequired named outputs for success: " + string(raw)
				break
			}
		}
	}
	if member.IsCoordinator {
		raw, _ := json.Marshal(spec.Roles)
		text += "\nAvailable configured roles: " + string(raw) + "\nUse workflow_delegate for separate assignments; dependencies must name exact successful saved result IDs. Use workflow_read_result for saved evidence. Task remains open until the caller explicitly stops it."
	}
	if len(text) > 65536 {
		return errors.New("workflow instructions exceed Session capacity")
	}
	member.Policy.Agent = &agentsv1alpha1.SessionAgentOverrides{Instructions: &text, Tools: &tools}
	return nil
}

func workflowTools(coordinator bool) []agentsv1alpha1.AgentTool {
	function := func(name, description, schema string) agentsv1alpha1.AgentTool {
		return agentsv1alpha1.AgentTool{Type: "function", Function: &agentsv1alpha1.AgentFunction{Name: name, Description: description, Parameters: apiextensionsv1.JSON{Raw: []byte(schema)}}}
	}
	tools := []agentsv1alpha1.AgentTool{
		function("workflow_read_result", "Read exact saved evidence from this task by result ID.", `{"type":"object","properties":{"result_id":{"type":"string"}},"required":["result_id"],"additionalProperties":false}`),
		function("workflow_result", "Commit the first final result for this assignment. Provide required named outputs on success. Checks reference saved history item IDs; patch outputs require a full 40- or 64-character Git object ID as base revision, SHA256, checks and limitations. Evidence is reported, not independently verified.", `{"type":"object","properties":{"outcome":{"type":"string","enum":["success","failed","cancelled","incomplete"]},"outputs":{"type":"array","items":{"type":"object","properties":{"name":{"type":"string"},"content":{"type":"string"},"kind":{"type":"string"},"base_revision":{"type":"string"},"sha256":{"type":"string"},"checks":{"type":"array","items":{"type":"object","properties":{"command":{"type":"string"},"output":{"type":"string"},"exit_code":{"type":"integer"},"source_item_id":{"type":"string"}},"required":["command","output","exit_code","source_item_id"],"additionalProperties":false}},"limitations":{"type":"string"}},"required":["name","content"],"additionalProperties":false}},"references":{"type":"array","items":{"type":"string"}},"limitations":{"type":"string"}},"required":["outcome","outputs"],"additionalProperties":false}`),
	}
	if coordinator {
		tools = append(tools, function("workflow_delegate", "Assign immutable work to an available configured role; prerequisite dependencies identify exact saved successful result IDs.", `{"type":"object","properties":{"role":{"type":"string"},"text":{"type":"string"},"dependencies":{"type":"array","items":{"type":"string"}}},"required":["role","text"],"additionalProperties":false}`))
	}
	return tools
}
