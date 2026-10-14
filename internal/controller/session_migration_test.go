package controller

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

// TestSessionSchemaMigration starts with the previous CRD, stores a legacy
// Session, then upgrades that same API server to the shipped CRD. A fresh
// API-server process with only the new schema cannot prove grandfathering.
func TestSessionSchemaMigration(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	path := filepath.Join("..", "..", "config", "crd", "bases", "agents.softcane.io_sessions.yaml")
	options := envtest.CRDInstallOptions{Paths: []string{path}, ErrorIfPathMissing: true}
	if err := envtest.ReadCRDFiles(&options); err != nil {
		t.Fatalf("read shipped Session CRD: %v", err)
	}
	if len(options.CRDs) != 1 {
		t.Fatalf("expected one Session CRD, got %d", len(options.CRDs))
	}
	shipped := options.CRDs[0]
	legacy := shipped.DeepCopy()
	rootSchema := legacy.Spec.Versions[0].Schema.OpenAPIV3Schema
	if len(rootSchema.XValidations) != 1 || rootSchema.XValidations[0].OptionalOldSelf == nil || !*rootSchema.XValidations[0].OptionalOldSelf {
		t.Fatal("shipped Session CRD lacks the expected migration rule")
	}
	rootSchema.XValidations = nil // Model the installed schema before this migration.

	server := &envtest.Environment{CRDs: []*apiextensionsv1.CustomResourceDefinition{legacy},
		BinaryAssetsDirectory: getFirstFoundEnvTestBinaryDir()}
	config, err := server.Start()
	if err != nil {
		t.Fatalf("start legacy API server: %v", err)
	}
	t.Cleanup(func() {
		if err := server.Stop(); err != nil {
			t.Errorf("stop API server: %v", err)
		}
	})
	scheme := runtime.NewScheme()
	if err := agentsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("register Session types: %v", err)
	}
	objects, err := client.New(config, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatalf("create Session client: %v", err)
	}
	definitions, err := apiextensionsclient.NewForConfig(config)
	if err != nil {
		t.Fatalf("create CRD client: %v", err)
	}

	old := &agentsv1alpha1.Session{ObjectMeta: metav1.ObjectMeta{Name: "preupgrade", Namespace: testNamespace}}
	if err := objects.Create(ctx, old); err != nil {
		t.Fatalf("create reference-free Session under legacy CRD: %v", err)
	}
	current, err := definitions.ApiextensionsV1().CustomResourceDefinitions().Get(ctx, shipped.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("read installed legacy CRD: %v", err)
	}
	current.Spec.Versions[0].Schema = shipped.Spec.Versions[0].Schema.DeepCopy()
	if _, err := definitions.ApiextensionsV1().CustomResourceDefinitions().Update(ctx, current, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("upgrade Session CRD: %v", err)
	}
	// Admission schema propagation is asynchronous; wait until a new legacy
	// CREATE is rejected before asserting migration behavior.
	err = wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 10*time.Second, true, func(ctx context.Context) (bool, error) {
		candidate := &agentsv1alpha1.Session{ObjectMeta: metav1.ObjectMeta{Name: "new-incomplete", Namespace: testNamespace}}
		createErr := objects.Create(ctx, candidate)
		if createErr == nil {
			if deleteErr := objects.Delete(ctx, candidate); deleteErr != nil {
				return false, deleteErr
			}
			return false, nil
		}
		return strings.Contains(createErr.Error(), "new Sessions require agentRef, environmentRef and spendControl"), nil
	})
	if err != nil {
		t.Fatalf("new CREATE was not rejected by shipped schema: %v", err)
	}

	verifyNewSessionAdmission(t, ctx, objects)

	key := client.ObjectKeyFromObject(old)
	if err := objects.Get(ctx, key, old); err != nil {
		t.Fatalf("read stored legacy Session after CRD upgrade: %v", err)
	}
	old.Labels = map[string]string{testMigrationLabel: testPreservedValue}
	if err := objects.Update(ctx, old); err != nil {
		t.Fatalf("update stored legacy Session without references: %v", err)
	}
	old.Status.Phase = "Pending"
	if err := objects.Status().Update(ctx, old); err != nil {
		t.Fatalf("update stored legacy Session status after CRD upgrade: %v", err)
	}
	old.Spec.AgentRef = &agentsv1alpha1.AgentReference{Name: testMigrationAgentName}
	if err := objects.Update(ctx, old); err != nil {
		t.Fatalf("partially migrate stored legacy Session: %v", err)
	}
	old.Spec.AgentRef = nil
	if err := objects.Update(ctx, old); err == nil || !strings.Contains(err.Error(), "existing choices cannot be removed") {
		t.Fatalf("legacy reference removal did not fail expected admission rule: %v", err)
	}
	if err := objects.Get(ctx, key, old); err != nil {
		t.Fatalf("re-read legacy Session before deletion: %v", err)
	}
	if err := objects.Delete(ctx, old); err != nil {
		t.Fatalf("delete stored legacy Session: %v", err)
	}
	t.Log("legacy Session updated and deleted after shipped CRD upgrade; new incomplete Session rejected")
}

func verifyNewSessionAdmission(t *testing.T, ctx context.Context, objects client.Client) {
	t.Helper()
	for _, spec := range []agentsv1alpha1.SessionSpec{
		{AgentRef: &agentsv1alpha1.AgentReference{Name: testMigrationAgentName}},
		{EnvironmentRef: &agentsv1alpha1.AgentReference{Name: testMigrationEnvironment}},
		{AgentRef: &agentsv1alpha1.AgentReference{Name: testMigrationAgentName},
			EnvironmentRef: &agentsv1alpha1.AgentReference{Name: testMigrationEnvironment}},
	} {
		candidate := &agentsv1alpha1.Session{ObjectMeta: metav1.ObjectMeta{GenerateName: "partial-", Namespace: testNamespace}, Spec: spec}
		if err := objects.Create(ctx, candidate); err == nil || !strings.Contains(err.Error(), "new Sessions require agentRef, environmentRef and spendControl") {
			t.Fatalf("incomplete new Session did not fail expected admission rule: %v", err)
		}
	}
	limit := int64(100)
	complete := &agentsv1alpha1.Session{ObjectMeta: metav1.ObjectMeta{Name: "new-complete", Namespace: testNamespace},
		Spec: agentsv1alpha1.SessionSpec{AgentRef: &agentsv1alpha1.AgentReference{Name: testMigrationAgentName},
			EnvironmentRef: &agentsv1alpha1.AgentReference{Name: testMigrationEnvironment},
			SpendControl:   &agentsv1alpha1.SessionSpendControl{Mode: spendModeLimited, LimitCents: &limit}}}
	if err := objects.Create(ctx, complete); err != nil {
		t.Fatalf("create fully referenced Session: %v", err)
	}
	complete.Spec.AgentRef = nil
	if err := objects.Update(ctx, complete); err == nil || !strings.Contains(err.Error(), "existing choices cannot be removed") {
		t.Fatalf("new Session removal did not fail expected admission rule: %v", err)
	}
	complete.Spec.AgentRef = &agentsv1alpha1.AgentReference{Name: testMigrationAgentName}
	complete.Spec.SpendControl = nil
	if err := objects.Update(ctx, complete); err == nil || !strings.Contains(err.Error(), "existing choices cannot be removed") {
		t.Fatalf("spend choice removal did not fail expected admission rule: %v", err)
	}
	for _, spend := range []agentsv1alpha1.SessionSpendControl{
		{Mode: spendModeLimited},
		{Mode: spendModeLimited, LimitCents: new(int64)},
		{Mode: spendModeUnlimited, LimitCents: &limit},
	} {
		candidate := &agentsv1alpha1.Session{ObjectMeta: metav1.ObjectMeta{GenerateName: "invalid-spend-", Namespace: testNamespace},
			Spec: agentsv1alpha1.SessionSpec{AgentRef: &agentsv1alpha1.AgentReference{Name: testMigrationAgentName},
				EnvironmentRef: &agentsv1alpha1.AgentReference{Name: testMigrationEnvironment}, SpendControl: &spend}}
		if err := objects.Create(ctx, candidate); err == nil {
			t.Fatalf("invalid spend control was accepted: %+v", spend)
		}
	}
	unlimited := &agentsv1alpha1.Session{ObjectMeta: metav1.ObjectMeta{Name: "explicit-unlimited", Namespace: testNamespace},
		Spec: agentsv1alpha1.SessionSpec{AgentRef: &agentsv1alpha1.AgentReference{Name: testMigrationAgentName},
			EnvironmentRef: &agentsv1alpha1.AgentReference{Name: testMigrationEnvironment},
			SpendControl:   &agentsv1alpha1.SessionSpendControl{Mode: spendModeUnlimited}}}
	if err := objects.Create(ctx, unlimited); err != nil {
		t.Fatalf("create explicitly unlimited Session: %v", err)
	}

	if err := objects.Delete(ctx, &agentsv1alpha1.Session{ObjectMeta: metav1.ObjectMeta{Name: complete.Name, Namespace: complete.Namespace}}); err != nil {
		t.Fatalf("delete new Session: %v", err)
	}
	if err := objects.Delete(ctx, unlimited); err != nil {
		t.Fatalf("delete explicitly unlimited Session: %v", err)
	}
}
