package gateway

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"testing"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const _testWorkflowResource = "workflows"

func TestWorkflowDiscoveryRechecksLiveProfile(t *testing.T) {
	g, writer, profile := profileGateway(t, testStore(t))
	workflow := agentsv1alpha1.WorkflowTemplate{ObjectMeta: metav1.ObjectMeta{Name: "flow", Namespace: _testNamespace, UID: "flow-uid", Generation: 1}, Spec: agentsv1alpha1.WorkflowTemplateSpec{Coordinator: agentsv1alpha1.WorkflowCoordinator{ProfileRef: agentsv1alpha1.AgentReference{Name: profile.Name}}}}
	workflow.Status = agentsv1alpha1.WorkflowTemplateStatus{ObservedGeneration: 1, SpecDigest: agentsv1alpha1.WorkflowTemplateSpecDigest(workflow.Spec), Conditions: profileReadyConditions(1)}
	if !g.summarizeWorkflow(t.Context(), workflow).Ready {
		t.Fatal("current workflow and Profile not Ready")
	}
	profile.Generation++
	if err := writer.Update(t.Context(), profile); err != nil {
		t.Fatal(err)
	}
	summary := g.summarizeWorkflow(t.Context(), workflow)
	if summary.Ready || len(summary.Profiles) != 1 || summary.Profiles[0].Ready {
		t.Fatal("stale Profile advertised Ready")
	}
	workflow.Generation++
	if g.summarizeWorkflow(t.Context(), workflow).Ready {
		t.Fatal("stale template observation advertised Ready")
	}
	// Optional missing roles are observable, but do not invalidate structural admission.
	workflow.Spec.Roles = []agentsv1alpha1.WorkflowRole{{Name: "arbitrary role", ProfileRef: agentsv1alpha1.AgentReference{Name: "absent"}}}
	if err := writer.Create(t.Context(), &workflow); err != nil {
		t.Fatal(err)
	}
	if _, err := g.workflow(t.Context(), _testNamespace, workflow.Name); err != nil {
		t.Fatalf("optional missing role blocked structural lookup: %v", err)
	}
}

func TestWorkflowListPageValidationAndUnavailableVisibility(t *testing.T) {
	g, writer, _ := profileGateway(t, testStore(t))
	workflow := &agentsv1alpha1.WorkflowTemplate{ObjectMeta: metav1.ObjectMeta{Name: "flow", Namespace: _testNamespace, UID: "flow-uid", Generation: 1}, Spec: agentsv1alpha1.WorkflowTemplateSpec{Coordinator: agentsv1alpha1.WorkflowCoordinator{ProfileRef: agentsv1alpha1.AgentReference{Name: "absent"}}}}
	if err := writer.Create(t.Context(), workflow); err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{"?limit=101", "?limit=0", "?limit=2&limit=3", "?after="} {
		response := httptest.NewRecorder()
		g.serveWorkflows(response, httptest.NewRequest("GET", "/"+query, nil), []string{"v1", _namespacesSegment, _testNamespace, _testWorkflowResource}, requestScope{Namespace: _testNamespace})
		if response.Code != 400 {
			t.Fatalf("%s: %d", query, response.Code)
		}
	}
	response := httptest.NewRecorder()
	g.serveWorkflows(response, httptest.NewRequest("GET", "/?limit=1", nil), []string{"v1", _namespacesSegment, _testNamespace, _testWorkflowResource}, requestScope{Namespace: _testNamespace})
	var page struct {
		Workflows []WorkflowSummary `json:"workflows"`
		More      bool              `json:"more"`
		Cursor    string            `json:"next_cursor"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	if len(page.Workflows) != 1 || page.Workflows[0].Ready {
		t.Fatalf("unavailable workflow hidden or advertised Ready: %s", response.Body.String())
	}
	var sessions agentsv1alpha1.SessionList
	if err := writer.List(t.Context(), &sessions, client.InNamespace(_testNamespace)); err != nil || len(sessions.Items) != 0 {
		t.Fatal("discovery created execution", err)
	}
}

type workflowPageReader struct {
	client.Reader
	namespace string
	limit     int64
	after     string
}

func (r *workflowPageReader) List(_ context.Context, list client.ObjectList, options ...client.ListOption) error {
	opts := &client.ListOptions{}
	for _, option := range options {
		option.ApplyToList(opts)
	}
	r.namespace, r.limit, r.after = opts.Namespace, opts.Limit, opts.Continue
	page := list.(*agentsv1alpha1.WorkflowTemplateList)
	page.Continue = "next+/="
	page.Items = nil
	return nil
}

func TestWorkflowPaginationPreservesOpaqueCursorAndNamespace(t *testing.T) {
	g, _, _ := profileGateway(t, testStore(t))
	reader := &workflowPageReader{Reader: g.reader}
	g.reader = reader
	response := httptest.NewRecorder()
	g.serveWorkflows(response, httptest.NewRequest("GET", "/?limit=7&after=opaque%2B%2F%3D", nil), []string{"v1", _namespacesSegment, _testNamespace, _testWorkflowResource}, requestScope{Namespace: _testNamespace})
	if reader.namespace != _testNamespace || reader.limit != 7 || reader.after != "opaque+/=" {
		t.Fatalf("page selector: %#v", reader)
	}
	var page map[string]json.RawMessage
	if err := json.Unmarshal(response.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	if string(page["more"]) != "true" || string(page["next_cursor"]) != `"next+/="` {
		t.Fatalf("cursor lost: %s", response.Body.String())
	}
}
