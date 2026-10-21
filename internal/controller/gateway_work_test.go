package controller

import (
	"context"
	"testing"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	"github.com/softcane/codex-session-operator/internal/openaiapi"
	"github.com/softcane/codex-session-operator/internal/sandbox"
	"github.com/softcane/codex-session-operator/internal/testutil"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	extensions "sigs.k8s.io/agent-sandbox/extensions/api/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func TestWorkSessionCannotBypassMissingGatewayPolicy(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := agentsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := extensions.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	pool, template := testutil.NewSandboxPool("work-policy")
	environment := &agentsv1alpha1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "work-policy-environment", Namespace: "work-policy", UID: "env-uid", Generation: 1},
		Spec: agentsv1alpha1.EnvironmentSpec{
			Mode: testSelfHostedMode, DeletionPolicy: deletePolicy,
			SelfHosted: &agentsv1alpha1.SelfHostedEnvironment{
				SandboxPoolRef: agentsv1alpha1.AgentReference{Name: pool.Name}, Workspace: agentsv1alpha1.SelfHostedWorkspace{Directory: defaultWorkspace, DeletionPolicy: deletePolicy},
			},
		},
	}
	environment.Status.OwnerScope = testAgentScope
	meta.SetStatusCondition(&environment.Status.Conditions, metav1.Condition{
		Type: readyCondition, Status: metav1.ConditionTrue, Reason: "ProfileApproved", ObservedGeneration: 1,
	})
	session := &agentsv1alpha1.Session{
		ObjectMeta: metav1.ObjectMeta{Name: "work-policy-session", Namespace: environment.Namespace, UID: "work-uid",
			Annotations: map[string]string{"agents.softcane.io/work-source": "work-claim"}},
		Spec: agentsv1alpha1.SessionSpec{
			AgentRef:       &agentsv1alpha1.AgentReference{Name: "approved-agent-v1"},
			EnvironmentRef: &agentsv1alpha1.AgentReference{Name: environment.Name},
			SpendControl:   &agentsv1alpha1.SessionSpendControl{Mode: spendModeUnlimited},
		},
	}
	k8s := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&agentsv1alpha1.Session{}).
		WithObjects(environment, session, pool, template).Build()
	resolved, err := sandbox.ResolvePool(t.Context(), k8s, environment.Namespace, environment.Spec.SelfHosted)
	if err != nil {
		t.Fatal(err)
	}
	if err := k8s.Get(t.Context(), client.ObjectKeyFromObject(environment), environment); err != nil {
		t.Fatal(err)
	}
	environment.Status.SandboxPool = resolved
	environment.Status.ConfigurationDigest = openaiapi.SandboxEnvironmentDigest(environment.Spec, resolved)
	if err := k8s.Update(t.Context(), environment); err != nil {
		t.Fatal(err)
	}
	provider := &sessionProviderStub{}
	r := &SessionReconciler{Client: k8s, APIReader: k8s, Scheme: scheme, OpenAI: provider,
		Environments: &environmentProviderStub{}, SandboxPools: []string{pool.Name}, AgentOwnerScope: testAgentScope}
	request := reconcile.Request{NamespacedName: client.ObjectKeyFromObject(session)}
	for range 2 {
		if _, err := r.Reconcile(context.Background(), request); err != nil {
			t.Fatal(err)
		}
	}
	var observed agentsv1alpha1.Session
	if err := k8s.Get(context.Background(), request.NamespacedName, &observed); err != nil {
		t.Fatal(err)
	}
	condition := meta.FindStatusCondition(observed.Status.Conditions, readyCondition)
	if condition == nil || condition.Reason != "SessionPolicyRequired" ||
		observed.Status.CreationAttempted || provider.creates != 0 {
		t.Fatalf("gateway work created remotely without its policy gate: %+v", observed.Status)
	}
}
