package gateway

import (
	"strings"
	"testing"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	"github.com/softcane/codex-session-operator/internal/openaiapi"
	"github.com/softcane/codex-session-operator/internal/sandbox"
	"github.com/softcane/codex-session-operator/internal/testutil"
	"k8s.io/apimachinery/pkg/runtime"
	extensions "sigs.k8s.io/agent-sandbox/extensions/api/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestProfileAdmissionRejectsSandboxTemplateDriftBeforeEnvironmentReconcile(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := agentsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := extensions.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	profile, objects := readyProfileObjects(_testNamespace)
	pool, template := testutil.NewSandboxPool(_testNamespace)
	environment := objects[1].(*agentsv1alpha1.Environment)
	environment.Spec = agentsv1alpha1.EnvironmentSpec{Mode: _selfHostedMode, DeletionPolicy: _testDeletePolicy, SelfHosted: &agentsv1alpha1.SelfHostedEnvironment{SandboxPoolRef: agentsv1alpha1.AgentReference{Name: pool.Name}, Workspace: agentsv1alpha1.SelfHostedWorkspace{Directory: "/workspace", DeletionPolicy: _testDeletePolicy}}}
	objects = append(objects, pool, template)
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
	snapshot, err := sandbox.ResolvePool(t.Context(), reader, _testNamespace, environment.Spec.SelfHosted)
	if err != nil {
		t.Fatal(err)
	}
	if err := reader.Get(t.Context(), client.ObjectKeyFromObject(environment), environment); err != nil {
		t.Fatal(err)
	}
	environment.Status.SandboxPool = snapshot
	environment.Status.ConfigurationDigest = openaiapi.SandboxEnvironmentDigest(environment.Spec, snapshot)
	if err := reader.Update(t.Context(), environment); err != nil {
		t.Fatal(err)
	}
	resolver := &ProfileResolver{reader: reader}
	if _, err := resolver.selection(t.Context(), profile); err != nil {
		t.Fatalf("valid pool rejected: %v", err)
	}
	if err := reader.Get(t.Context(), client.ObjectKeyFromObject(template), template); err != nil {
		t.Fatal(err)
	}
	template.Spec.PodTemplate.Spec.Containers[0].Image = "executor@sha256:" + strings.Repeat("b", 64)
	template.Generation++
	if err := reader.Update(t.Context(), template); err != nil {
		t.Fatal(err)
	}
	if _, err := resolver.selection(t.Context(), profile); err == nil {
		t.Fatal("accepted stale sandbox approval before Environment reconciliation")
	}
}
