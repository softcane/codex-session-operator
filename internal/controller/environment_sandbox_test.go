package controller

import (
	"strings"
	"testing"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	"github.com/softcane/codex-session-operator/internal/testutil"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	extensions "sigs.k8s.io/agent-sandbox/extensions/api/v1beta1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestSandboxEnvironmentFreezesPoolAndRejectsSourceDrift(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := agentsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := extensions.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	pool, template := testutil.NewSandboxPool("agents")
	environment := &agentsv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "go-workspace", Namespace: "agents", UID: "env-uid", Generation: 1, Finalizers: []string{environmentFinalizer}}, Spec: agentsv1alpha1.EnvironmentSpec{Mode: "SelfHosted", DeletionPolicy: deletePolicy, SelfHosted: &agentsv1alpha1.SelfHostedEnvironment{SandboxPoolRef: agentsv1alpha1.AgentReference{Name: pool.Name}, Workspace: agentsv1alpha1.SelfHostedWorkspace{Directory: defaultWorkspace, DeletionPolicy: retainPolicy}}}}
	reader := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(environment).WithObjects(environment, pool, template).Build()
	reconciler := &EnvironmentReconciler{Client: reader, APIReader: reader, OwnerScope: environmentTestScope, SandboxPools: []string{pool.Name}}
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(environment)}
	if _, err := reconciler.Reconcile(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	if err := reader.Get(t.Context(), request.NamespacedName, environment); err != nil {
		t.Fatal(err)
	}
	if !meta.IsStatusConditionTrue(environment.Status.Conditions, readyCondition) || environment.Status.SandboxPool == nil {
		t.Fatalf("pool not resolved: %+v", environment.Status)
	}
	session := &agentsv1alpha1.Session{ObjectMeta: metav1.ObjectMeta{Namespace: environment.Namespace}, Spec: agentsv1alpha1.SessionSpec{EnvironmentRef: &agentsv1alpha1.AgentReference{Name: environment.Name}}}
	sessions := &SessionReconciler{Client: reader, APIReader: reader, Environments: &environmentProviderStub{}, AgentOwnerScope: environmentTestScope, SandboxPools: []string{pool.Name}}
	frozen, fields, reason, _, err := sessions.resolveEnvironment(t.Context(), session)
	if err != nil || reason != "" || frozen == nil || frozen.SandboxPool.UID != string(pool.UID) || fields["workspace_directory"] != defaultWorkspace {
		t.Fatalf("resolve failed: snapshot=%+v reason=%q err=%v", frozen, reason, err)
	}
	original := frozen.DeepCopy()
	if err := reader.Get(t.Context(), client.ObjectKeyFromObject(template), template); err != nil {
		t.Fatal(err)
	}
	template.Spec.PodTemplate.Spec.Containers[0].Image = "executor@sha256:" + strings.Repeat("b", 64)
	template.Generation++
	if err := reader.Update(t.Context(), template); err != nil {
		t.Fatal(err)
	}
	_, _, reason, _, err = sessions.resolveEnvironment(t.Context(), session)
	if err != nil || reason != environmentNotReadyReason {
		t.Fatalf("stale template allowed: reason=%q err=%v", reason, err)
	}
	if _, err := reconciler.Reconcile(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	current, _, reason, _, err := sessions.resolveEnvironment(t.Context(), session)
	if err != nil || reason != "" || current.SandboxPool.ConfigurationDigest == original.SandboxPool.ConfigurationDigest {
		t.Fatalf("new template did not require new source snapshot: reason=%q err=%v", reason, err)
	}
	if frozen.SandboxPool.ConfigurationDigest != original.SandboxPool.ConfigurationDigest {
		t.Fatal("existing snapshot mutated")
	}
	if err := reader.Delete(t.Context(), template); err != nil {
		t.Fatal(err)
	}
	template.UID = "replacement-template-uid"
	template.ResourceVersion = ""
	if err := reader.Create(t.Context(), template); err != nil {
		t.Fatal(err)
	}
	_, _, reason, _, err = sessions.resolveEnvironment(t.Context(), session)
	if err != nil || reason != environmentNotReadyReason {
		t.Fatalf("same-name template replacement passed frozen identity: reason=%q err=%v", reason, err)
	}
	reconciler.SandboxPools = nil
	if _, err := reconciler.Reconcile(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	if err := reader.Get(t.Context(), request.NamespacedName, environment); err != nil {
		t.Fatal(err)
	}
	if meta.IsStatusConditionTrue(environment.Status.Conditions, readyCondition) {
		t.Fatal("unapproved pool accepted")
	}
}
