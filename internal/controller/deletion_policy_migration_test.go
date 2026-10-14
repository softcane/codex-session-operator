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

const (
	testMigrationAgentName   = "agent"
	testMigrationEnvironment = "environment"
	testMigrationLabel       = "migration"
	testMigrationModel       = "gpt-6-luna"
	testStaticBearerAuthType = "static_bearer"
)

// TestDeletionPolicySchemaMigration proves old Agent and VaultCredential objects
// remain writable after their CRDs are upgraded in the same API server.
func TestDeletionPolicySchemaMigration(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	base := filepath.Join("..", "..", "config", "crd", "bases")
	options := envtest.CRDInstallOptions{Paths: []string{
		filepath.Join(base, "agents.softcane.io_agents.yaml"),
		filepath.Join(base, "agents.softcane.io_vaultcredentials.yaml"),
	}, ErrorIfPathMissing: true}
	if err := envtest.ReadCRDFiles(&options); err != nil {
		t.Fatalf("read shipped CRDs: %v", err)
	}
	if len(options.CRDs) != 2 {
		t.Fatalf("expected two CRDs, got %d", len(options.CRDs))
	}
	shipped := make(map[string]*apiextensionsv1.CustomResourceDefinition, 2)
	legacy := make([]*apiextensionsv1.CustomResourceDefinition, 0, 2)
	for _, crd := range options.CRDs {
		shipped[crd.Spec.Names.Kind] = crd
		copy := crd.DeepCopy()
		rules := copy.Spec.Versions[0].Schema.OpenAPIV3Schema.XValidations
		if len(rules) != 1 || rules[0].OptionalOldSelf == nil || !*rules[0].OptionalOldSelf || !strings.Contains(rules[0].Message, "deletionPolicy") {
			t.Fatalf("%s CRD lacks expected deletion-policy migration rule", crd.Spec.Names.Kind)
		}
		copy.Spec.Versions[0].Schema.OpenAPIV3Schema.XValidations = nil
		legacy = append(legacy, copy)
	}
	if shipped["Agent"] == nil || shipped["VaultCredential"] == nil {
		t.Fatal("Agent or VaultCredential CRD missing")
	}
	server := &envtest.Environment{CRDs: legacy, BinaryAssetsDirectory: getFirstFoundEnvTestBinaryDir()}
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
		t.Fatalf("register custom resources: %v", err)
	}
	objects, err := client.New(config, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatalf("create object client: %v", err)
	}
	definitions, err := apiextensionsclient.NewForConfig(config)
	if err != nil {
		t.Fatalf("create CRD client: %v", err)
	}

	oldAgent := &agentsv1alpha1.Agent{ObjectMeta: metav1.ObjectMeta{Name: "preupgrade-agent", Namespace: testNamespace},
		Spec: agentsv1alpha1.AgentSpec{Model: testMigrationModel}}
	oldCredential := &agentsv1alpha1.VaultCredential{ObjectMeta: metav1.ObjectMeta{Name: "preupgrade-credential", Namespace: testNamespace},
		Spec: migrationCredentialSpec("")}
	for _, object := range []client.Object{oldAgent, oldCredential} {
		if err := objects.Create(ctx, object); err != nil {
			t.Fatalf("create %T under legacy CRD: %v", object, err)
		}
	}
	for _, kind := range []string{"Agent", "VaultCredential"} {
		crd := shipped[kind]
		current, getErr := definitions.ApiextensionsV1().CustomResourceDefinitions().Get(ctx, crd.Name, metav1.GetOptions{})
		if getErr != nil {
			t.Fatalf("read installed %s CRD: %v", kind, getErr)
		}
		current.Spec.Versions[0].Schema = crd.Spec.Versions[0].Schema.DeepCopy()
		if _, updateErr := definitions.ApiextensionsV1().CustomResourceDefinitions().Update(ctx, current, metav1.UpdateOptions{}); updateErr != nil {
			t.Fatalf("upgrade %s CRD: %v", kind, updateErr)
		}
	}

	newAgent, newCredential := verifyNewDeletionPolicyAdmission(t, ctx, objects)
	verifyLegacyDeletionPolicyUpdates(t, ctx, objects, oldAgent, oldCredential)

	for _, object := range []client.Object{newAgent, newCredential} {
		if err := objects.Delete(ctx, object); err != nil {
			t.Fatalf("delete %T: %v", object, err)
		}
	}
	t.Log("Agent and VaultCredential in-place CRD upgrades preserved old objects and required explicit new deletion policy")
}

func migrationCredentialSpec(policy string) agentsv1alpha1.VaultCredentialSpec {
	return agentsv1alpha1.VaultCredentialSpec{Name: "fixture", VaultRef: agentsv1alpha1.AgentReference{Name: "vault"},
		Auth: agentsv1alpha1.VaultCredentialAuth{Type: testStaticBearerAuthType, StaticBearer: &agentsv1alpha1.VaultStaticBearer{
			ServerURL: "https://example.invalid/mcp", TokenSecretRef: agentsv1alpha1.CredentialSecretReference{Name: "codex-vault", Key: "token"}}},
		DeletionPolicy: policy}
}

func verifyNewDeletionPolicyAdmission(t *testing.T, ctx context.Context, objects client.Client) (*agentsv1alpha1.Agent, *agentsv1alpha1.VaultCredential) {
	t.Helper()
	waitForNewCreateRejection := func(name string, newCandidate func() client.Object, want string) {
		t.Helper()
		err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 10*time.Second, true, func(ctx context.Context) (bool, error) {
			candidate := newCandidate()
			createErr := objects.Create(ctx, candidate)
			if createErr == nil {
				if deleteErr := objects.Delete(ctx, candidate); deleteErr != nil {
					return false, deleteErr
				}
				return false, nil
			}
			return strings.Contains(createErr.Error(), want), nil
		})
		if err != nil {
			t.Fatalf("new %s CREATE was not rejected by upgraded schema: %v", name, err)
		}
	}
	waitForNewCreateRejection("Agent", func() client.Object {
		return &agentsv1alpha1.Agent{ObjectMeta: metav1.ObjectMeta{Name: "new-agent-incomplete", Namespace: testNamespace},
			Spec: agentsv1alpha1.AgentSpec{Model: testMigrationModel}}
	}, "new Agents require deletionPolicy")
	waitForNewCreateRejection("VaultCredential", func() client.Object {
		return &agentsv1alpha1.VaultCredential{ObjectMeta: metav1.ObjectMeta{Name: "new-credential-incomplete", Namespace: testNamespace},
			Spec: migrationCredentialSpec("")}
	}, "new VaultCredentials require deletionPolicy")

	newAgent := &agentsv1alpha1.Agent{ObjectMeta: metav1.ObjectMeta{Name: "new-agent-complete", Namespace: testNamespace},
		Spec: agentsv1alpha1.AgentSpec{Model: testMigrationModel, DeletionPolicy: retainPolicy}}
	newCredential := &agentsv1alpha1.VaultCredential{ObjectMeta: metav1.ObjectMeta{Name: "new-credential-complete", Namespace: testNamespace},
		Spec: migrationCredentialSpec(deletePolicy)}
	for _, object := range []client.Object{newAgent, newCredential} {
		if err := objects.Create(ctx, object); err != nil {
			t.Fatalf("create %T with explicit deletion policy: %v", object, err)
		}
	}
	newAgent.Spec.DeletionPolicy = ""
	if err := objects.Update(ctx, newAgent); err == nil || !strings.Contains(err.Error(), "existing policy cannot be removed") {
		t.Fatalf("Agent deletion policy removal did not fail expected rule: %v", err)
	}
	newCredential.Spec.DeletionPolicy = ""
	if err := objects.Update(ctx, newCredential); err == nil || !strings.Contains(err.Error(), "existing policy cannot be removed") {
		t.Fatalf("VaultCredential deletion policy removal did not fail expected rule: %v", err)
	}

	return newAgent, newCredential
}

func verifyLegacyDeletionPolicyUpdates(t *testing.T, ctx context.Context, objects client.Client, oldAgent *agentsv1alpha1.Agent, oldCredential *agentsv1alpha1.VaultCredential) {
	t.Helper()
	if err := objects.Get(ctx, client.ObjectKeyFromObject(oldAgent), oldAgent); err != nil {
		t.Fatalf("read old Agent after CRD upgrade: %v", err)
	}
	oldAgent.Labels = map[string]string{testMigrationLabel: testPreservedValue}
	if err := objects.Update(ctx, oldAgent); err != nil {
		t.Fatalf("update old Agent without deletion policy: %v", err)
	}
	oldAgent.Status.CreationAttempted = true
	if err := objects.Status().Update(ctx, oldAgent); err != nil {
		t.Fatalf("update old Agent status: %v", err)
	}
	oldAgent.Spec.DeletionPolicy = retainPolicy
	if err := objects.Update(ctx, oldAgent); err != nil {
		t.Fatalf("adopt explicit policy on old Agent: %v", err)
	}
	oldAgent.Spec.DeletionPolicy = ""
	if err := objects.Update(ctx, oldAgent); err == nil || !strings.Contains(err.Error(), "existing policy cannot be removed") {
		t.Fatalf("old Agent policy removal did not fail expected rule: %v", err)
	}
	if err := objects.Get(ctx, client.ObjectKeyFromObject(oldAgent), oldAgent); err != nil {
		t.Fatalf("re-read old Agent before deletion: %v", err)
	}
	if err := objects.Delete(ctx, oldAgent); err != nil {
		t.Fatalf("delete old Agent: %v", err)
	}

	if err := objects.Get(ctx, client.ObjectKeyFromObject(oldCredential), oldCredential); err != nil {
		t.Fatalf("read old VaultCredential after CRD upgrade: %v", err)
	}
	oldCredential.Labels = map[string]string{testMigrationLabel: testPreservedValue}
	if err := objects.Update(ctx, oldCredential); err != nil {
		t.Fatalf("update old VaultCredential without deletion policy: %v", err)
	}
	oldCredential.Status.CreationAttempted = true
	if err := objects.Status().Update(ctx, oldCredential); err != nil {
		t.Fatalf("update old VaultCredential status: %v", err)
	}
	oldCredential.Spec.DeletionPolicy = deletePolicy
	if err := objects.Update(ctx, oldCredential); err != nil {
		t.Fatalf("adopt explicit policy on old VaultCredential: %v", err)
	}
	oldCredential.Spec.DeletionPolicy = ""
	if err := objects.Update(ctx, oldCredential); err == nil || !strings.Contains(err.Error(), "existing policy cannot be removed") {
		t.Fatalf("old VaultCredential policy removal did not fail expected rule: %v", err)
	}
	if err := objects.Get(ctx, client.ObjectKeyFromObject(oldCredential), oldCredential); err != nil {
		t.Fatalf("re-read old VaultCredential before deletion: %v", err)
	}
	if err := objects.Delete(ctx, oldCredential); err != nil {
		t.Fatalf("delete old VaultCredential: %v", err)
	}
}
