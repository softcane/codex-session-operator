package controller

import (
	"testing"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

const (
	_workflowProfileName   = "project"
	_workflowTestNamespace = "workflow-test"
)

func TestWorkflowTemplateReadinessTracksCurrentProfile(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := agentsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	workflow := &agentsv1alpha1.WorkflowTemplate{ObjectMeta: metav1.ObjectMeta{Name: "workflow", Namespace: _workflowTestNamespace, UID: "workflow-uid", Generation: 1}, Spec: agentsv1alpha1.WorkflowTemplateSpec{Coordinator: agentsv1alpha1.WorkflowCoordinator{ProfileRef: agentsv1alpha1.AgentReference{Name: _workflowProfileName}}}}
	profile := &agentsv1alpha1.Profile{ObjectMeta: metav1.ObjectMeta{Name: _workflowProfileName, Namespace: _workflowTestNamespace, UID: "profile-uid", Generation: 1}}
	profile.Status.ObservedGeneration = 1
	profile.Status.PolicyDigest = agentsv1alpha1.ProfilePolicyDigest(profile.Spec)
	meta.SetStatusCondition(&profile.Status.Conditions, metav1.Condition{Type: readyCondition, Status: metav1.ConditionTrue, Reason: "ProfileReady", ObservedGeneration: 1})
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(workflow, profile).WithObjects(workflow, profile).Build()
	r := &WorkflowTemplateReconciler{Client: c}
	reconcile := func(want metav1.ConditionStatus) {
		t.Helper()
		if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(workflow)}); err != nil {
			t.Fatal(err)
		}
		if err := c.Get(t.Context(), client.ObjectKeyFromObject(workflow), workflow); err != nil {
			t.Fatal(err)
		}
		ready := meta.FindStatusCondition(workflow.Status.Conditions, readyCondition)
		if ready == nil || ready.Status != want {
			t.Fatalf("Ready = %#v; want %s", ready, want)
		}
	}
	reconcile(metav1.ConditionTrue)
	profile.Generation = 2
	if err := c.Update(t.Context(), profile); err != nil {
		t.Fatal(err)
	}
	reconcile(metav1.ConditionFalse)
	profile.Status.ObservedGeneration = 2
	profile.Status.Conditions[0].ObservedGeneration = 2
	if err := c.Status().Update(t.Context(), profile); err != nil {
		t.Fatal(err)
	}
	reconcile(metav1.ConditionTrue)
	var sessions agentsv1alpha1.SessionList
	if err := c.List(t.Context(), &sessions); err != nil {
		t.Fatal(err)
	}
	if len(sessions.Items) != 0 {
		t.Fatal("template created execution")
	}
}

func TestWorkflowTemplateProfileWatchIsNamespaced(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := agentsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	makeWorkflow := func(namespace, name, profile string) *agentsv1alpha1.WorkflowTemplate {
		return &agentsv1alpha1.WorkflowTemplate{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace}, Spec: agentsv1alpha1.WorkflowTemplateSpec{Coordinator: agentsv1alpha1.WorkflowCoordinator{ProfileRef: agentsv1alpha1.AgentReference{Name: profile}}, Roles: []agentsv1alpha1.WorkflowRole{{Name: "role", ProfileRef: agentsv1alpha1.AgentReference{Name: profile}}}}}
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithIndex(&agentsv1alpha1.WorkflowTemplate{}, _workflowProfileReferenceIndex, workflowProfileReferences).WithObjects(makeWorkflow(_workflowTestNamespace, "match", _workflowProfileName), makeWorkflow("other", "wrong-namespace", _workflowProfileName), makeWorkflow(_workflowTestNamespace, "wrong-profile", "another")).Build()
	r := &WorkflowTemplateReconciler{Client: c}
	requests := r.profileWorkflowRequests(t.Context(), &agentsv1alpha1.Profile{ObjectMeta: metav1.ObjectMeta{Name: _workflowProfileName, Namespace: _workflowTestNamespace}})
	if len(requests) != 1 || requests[0].Name != "match" || requests[0].Namespace != _workflowTestNamespace {
		t.Fatalf("watch requests = %#v", requests)
	}
}
