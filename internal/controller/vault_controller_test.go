package controller

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	"github.com/softcane/codex-session-operator/internal/openaiapi"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	testVaultTokenKey      = "token"
	testStaticBearerType   = "static_bearer"
	testMetadataPreserved  = "preserved"
	testMetadataOne        = "one"
	testUnlimitedSpendMode = agentsv1alpha1.SessionSpendModeUnlimited
	testLimitedMode        = "limited"
	testEnvironmentOrigin  = "environment"
)

// The stub never retains secret data and injects failures after committing writes.
type vaultProviderStub struct {
	vault                                                                                    openaiapi.Vault
	credential                                                                               openaiapi.Credential
	creates, updates, credentialCreates, rotations, deletes                                  int
	loseCreate, loseUpdate, loseCredentialCreate, loseRotation, hideDiscovery, deleteFailure bool
}

var _ VaultProvider = (*vaultProviderStub)(nil)
var _ CredentialProvider = (*vaultProviderStub)(nil)

func (p *vaultProviderStub) CreateVault(_ context.Context, uid, scope string, spec agentsv1alpha1.VaultSpec) (openaiapi.Vault, error) {
	p.creates++
	metadata, err := openaiapi.VaultMetadata(spec.Metadata, nil, nil, uid, scope)
	if err != nil {
		return openaiapi.Vault{}, err
	}
	name := ""
	if spec.Name != nil {
		name = strings.TrimSpace(*spec.Name)
	}
	p.vault = openaiapi.Vault{ID: "vault_test", OwnerUID: uid, OwnerScope: scope, Name: name, Metadata: metadata}
	if p.loseCreate {
		return openaiapi.Vault{}, errors.New("lost response after commit")
	}
	return p.vault, nil
}
func (p *vaultProviderStub) GetVault(_ context.Context, id string) (openaiapi.Vault, error) {
	if p.vault.ID != id || id == "" {
		return openaiapi.Vault{}, openaiapi.ErrNotFound
	}
	return p.vault, nil
}
func (p *vaultProviderStub) UpdateVault(_ context.Context, id string, name *string, clearName bool, metadata *map[string]string) (openaiapi.Vault, error) {
	if p.vault.ID != id {
		return openaiapi.Vault{}, openaiapi.ErrNotFound
	}
	p.updates++
	if name != nil {
		p.vault.Name = strings.TrimSpace(*name)
	} else if clearName {
		p.vault.Name = ""
	}
	if metadata != nil {
		p.vault.Metadata = maps.Clone(*metadata)
	}
	if p.loseUpdate {
		return openaiapi.Vault{}, errors.New("lost Vault update response")
	}
	return p.vault, nil
}
func (p *vaultProviderStub) FindVault(_ context.Context, uid, scope string) (openaiapi.Vault, error) {
	if p.hideDiscovery || p.vault.OwnerUID != uid || p.vault.OwnerScope != scope {
		return openaiapi.Vault{}, nil
	}
	return p.vault, nil
}
func (p *vaultProviderStub) DeleteVault(_ context.Context, _ string) error {
	if p.deleteFailure {
		return errors.New("delete unavailable")
	}
	p.deletes++
	p.vault = openaiapi.Vault{}
	return nil
}
func (p *vaultProviderStub) ListCredentials(_ context.Context, vaultID string) ([]openaiapi.Credential, error) {
	if p.credential.ID == "" || p.credential.VaultID != vaultID {
		return nil, nil
	}
	return []openaiapi.Credential{p.credential}, nil
}
func (p *vaultProviderStub) CreateCredential(_ context.Context, vaultID, name string, metadata map[string]string, auth map[string]any) (openaiapi.Credential, error) {
	p.credentialCreates++
	public := copyConfiguration(auth)
	delete(public, testVaultTokenKey)
	delete(public, "secret_value")
	delete(public, "access_token")
	p.credential = openaiapi.Credential{ID: "cred_test", VaultID: vaultID, Name: strings.TrimSpace(name), OwnerUID: metadata["kubernetes_uid"], OwnerScope: metadata["owner_scope"], Metadata: maps.Clone(metadata), Auth: public}
	if p.loseCredentialCreate {
		return openaiapi.Credential{}, errors.New("lost credential create response")
	}
	return p.credential, nil
}
func (p *vaultProviderStub) GetCredential(_ context.Context, vaultID, id string) (openaiapi.Credential, error) {
	if p.credential.ID != id || p.credential.VaultID != vaultID || id == "" {
		return openaiapi.Credential{}, openaiapi.ErrNotFound
	}
	return p.credential, nil
}
func (p *vaultProviderStub) FindCredential(_ context.Context, vaultID, uid, scope string) (openaiapi.Credential, error) {
	if p.hideDiscovery || p.credential.VaultID != vaultID || p.credential.OwnerUID != uid || p.credential.OwnerScope != scope {
		return openaiapi.Credential{}, nil
	}
	return p.credential, nil
}
func (p *vaultProviderStub) UpdateCredential(_ context.Context, _, _ string, fields map[string]any) (openaiapi.Credential, error) {
	p.rotations++
	if metadata, ok := fields["metadata"].(map[string]string); ok {
		p.credential.Metadata = maps.Clone(metadata)
	}
	if p.loseRotation {
		return openaiapi.Credential{}, errors.New("lost rotation response")
	}
	return p.credential, nil
}
func (p *vaultProviderStub) DeleteCredential(_ context.Context, _, _ string) error {
	if p.deleteFailure {
		return errors.New("delete unavailable")
	}
	p.deletes++
	p.credential = openaiapi.Credential{}
	return nil
}

func testCredentialToken() string {
	var data [32]byte
	_, err := rand.Read(data[:])
	Expect(err).NotTo(HaveOccurred())
	return hex.EncodeToString(data[:])
}

var _ = Describe("Vault and protected credential lifecycle", func() {
	var v *agentsv1alpha1.Vault
	var c *agentsv1alpha1.VaultCredential
	var secret *corev1.Secret
	var p *vaultProviderStub
	var vr *VaultReconciler
	var cr *VaultCredentialReconciler
	stepVault := func() error {
		_, err := vr.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(v)})
		return err
	}
	stepCredential := func() error {
		_, err := cr.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(c)})
		return err
	}
	readVault := func() *agentsv1alpha1.Vault {
		var result agentsv1alpha1.Vault
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(v), &result)).To(Succeed())
		return &result
	}
	readCredential := func() *agentsv1alpha1.VaultCredential {
		var result agentsv1alpha1.VaultCredential
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(c), &result)).To(Succeed())
		return &result
	}
	BeforeEach(func() {
		v = &agentsv1alpha1.Vault{ObjectMeta: metav1.ObjectMeta{GenerateName: "vault-", Namespace: testNamespace}, Spec: agentsv1alpha1.VaultSpec{DeletionPolicy: deletePolicy}}
		Expect(k8sClient.Create(ctx, v)).To(Succeed())
		secret = &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: openaiapi.VaultSecretName, Namespace: testNamespace}, Data: map[string][]byte{testVaultTokenKey: []byte(testCredentialToken())}}
		Expect(k8sClient.Create(ctx, secret)).To(Succeed())
		c = &agentsv1alpha1.VaultCredential{ObjectMeta: metav1.ObjectMeta{GenerateName: "credential-", Namespace: testNamespace, Finalizers: []string{otherTestFinalizer}}, Spec: agentsv1alpha1.VaultCredentialSpec{VaultRef: agentsv1alpha1.AgentReference{Name: v.Name}, Name: "controlled credential", DeletionPolicy: deletePolicy, Auth: agentsv1alpha1.VaultCredentialAuth{Type: testStaticBearerType, StaticBearer: &agentsv1alpha1.VaultStaticBearer{ServerURL: "https://mcp.example.com/mcp", TokenSecretRef: agentsv1alpha1.CredentialSecretReference{Name: openaiapi.VaultSecretName, Key: testVaultTokenKey}}}}}
		Expect(k8sClient.Create(ctx, c)).To(Succeed())
		p = &vaultProviderStub{}
		vr = &VaultReconciler{Client: k8sClient, APIReader: k8sClient, OpenAI: p, OwnerScope: testAgentScope}
		cr = &VaultCredentialReconciler{Client: k8sClient, APIReader: k8sClient, OpenAI: p, OwnerScope: testAgentScope}
	})
	AfterEach(func() {
		for _, obj := range []client.Object{readCredential(), readVault()} {
			obj.SetFinalizers(nil)
			Expect(k8sClient.Update(ctx, obj)).To(Succeed())
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, obj))).To(Succeed())
		}
		Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, secret))).To(Succeed())
	})
	provision := func() {
		for range 3 {
			Expect(stepVault()).To(Succeed())
		}
		for range 3 {
			Expect(stepCredential()).To(Succeed())
		}
	}
	It("claims once, becomes Ready on public readback and performs no redundant writes", func() {
		provision()
		Expect(stepCredential()).To(Succeed())
		Expect(stepVault()).To(Succeed())
		Expect(p.creates).To(Equal(1))
		Expect(p.credentialCreates).To(Equal(1))
		Expect(p.rotations).To(BeZero())
		status := readCredential().Status
		Expect(meta.FindStatusCondition(status.Conditions, readyCondition).Status).To(Equal(metav1.ConditionTrue))
		Expect(meta.FindStatusCondition(status.Conditions, "SecretMaterialVerified").Status).To(Equal(metav1.ConditionUnknown))
		Expect(status.SourceVersions[0].UID).To(Equal(string(secret.UID)))
	})
	It("updates name and user metadata, preserves external keys, and clears managed keys", func() {
		provision()
		p.vault.Metadata["external"] = testMetadataPreserved
		current := readVault()
		name := "renamed"
		metadata := map[string]string{"team": testMetadataOne}
		current.Spec.Name, current.Spec.Metadata = &name, &metadata
		Expect(k8sClient.Update(ctx, current)).To(Succeed())
		Expect(stepVault()).To(Succeed())
		Expect(p.updates).To(Equal(1))
		Expect(p.vault.Name).To(Equal(name))
		Expect(p.vault.Metadata).To(HaveKeyWithValue("external", testMetadataPreserved))
		Expect(stepVault()).To(Succeed())
		Expect(readVault().Status.ManagedMetadataKeys).To(Equal([]string{"team"}))
		current = readVault()
		cleared := map[string]string{}
		current.Spec.Name, current.Spec.ClearName, current.Spec.Metadata = nil, true, &cleared
		Expect(k8sClient.Update(ctx, current)).To(Succeed())
		Expect(stepVault()).To(Succeed())
		Expect(stepVault()).To(Succeed())
		Expect(p.updates).To(Equal(2))
		Expect(p.vault.Name).To(BeEmpty())
		Expect(p.vault.Metadata).NotTo(HaveKey("team"))
		Expect(p.vault.Metadata).To(HaveKeyWithValue("external", testMetadataPreserved))
		Expect(stepVault()).To(Succeed())
		Expect(p.updates).To(Equal(2))
	})
	It("observes a lost Vault update response before retrying", func() {
		provision()
		current := readVault()
		name := "after-loss"
		current.Spec.Name = &name
		Expect(k8sClient.Update(ctx, current)).To(Succeed())
		p.loseUpdate = true
		Expect(stepVault()).NotTo(Succeed())
		p.loseUpdate = false
		Expect(stepVault()).To(Succeed())
		Expect(p.updates).To(Equal(1))
		Expect(readVault().Status.ObservedGeneration).To(Equal(readVault().Generation))
	})
	It("keeps metadata ownership after a lost Vault create response and a changed spec", func() {
		current := readVault()
		metadata := map[string]string{testTeamMetadataKey: testMetadataOne}
		current.Spec.Metadata = &metadata
		Expect(k8sClient.Update(ctx, current)).To(Succeed())
		p.loseCreate = true
		Expect(stepVault()).To(Succeed())
		Expect(stepVault()).NotTo(Succeed())
		Expect(readVault().Status.ManagedMetadataKeys).To(ContainElement(testTeamMetadataKey))
		p.vault.Metadata[testExternalMetadataKey] = testExternalMetadataValue
		current = readVault()
		empty := make(map[string]string)
		current.Spec.Metadata = &empty
		Expect(k8sClient.Update(ctx, current)).To(Succeed())
		for range 3 {
			Expect(stepVault()).To(Succeed())
		}
		Expect(p.vault.Metadata).NotTo(HaveKey(testTeamMetadataKey))
		Expect(p.vault.Metadata).To(HaveKeyWithValue(testExternalMetadataKey, testExternalMetadataValue))
		Expect(p.creates).To(Equal(1))
	})
	It("keeps metadata ownership after a lost Vault update response and a changed spec", func() {
		provision()
		p.vault.Metadata[testExternalMetadataKey] = testExternalMetadataValue
		current := readVault()
		metadata := map[string]string{testTeamMetadataKey: testMetadataOne}
		current.Spec.Metadata = &metadata
		Expect(k8sClient.Update(ctx, current)).To(Succeed())
		p.loseUpdate = true
		Expect(stepVault()).NotTo(Succeed())
		Expect(readVault().Status.ManagedMetadataKeys).To(ContainElement(testTeamMetadataKey))
		p.loseUpdate = false
		current = readVault()
		empty := make(map[string]string)
		current.Spec.Metadata = &empty
		Expect(k8sClient.Update(ctx, current)).To(Succeed())
		Expect(stepVault()).To(Succeed())
		Expect(stepVault()).To(Succeed())
		Expect(p.vault.Metadata).NotTo(HaveKey(testTeamMetadataKey))
		Expect(p.vault.Metadata).To(HaveKeyWithValue(testExternalMetadataKey, testExternalMetadataValue))
		Expect(p.updates).To(Equal(2))
	})
	It("does not update a Vault after a conflicting metadata ownership claim", func() {
		provision()
		current := readVault()
		metadata := map[string]string{testTeamMetadataKey: testMetadataOne}
		current.Spec.Metadata = &metadata
		Expect(k8sClient.Update(ctx, current)).To(Succeed())
		vr.Client = &interceptClient{Client: k8sClient, beforeStatusPatch: func(obj client.Object) error {
			return apierrors.NewConflict(schema.GroupResource{Group: agentsv1alpha1.SchemeGroupVersion.Group, Resource: "vaults"}, obj.GetName(), errors.New("concurrent metadata claim"))
		}}
		Expect(apierrors.IsConflict(stepVault())).To(BeTrue())
		Expect(p.updates).To(BeZero())
	})
	It("tracks both full metadata maps during a replacement with a lost response", func() {
		previous := make(map[string]string, 13)
		replacement := make(map[string]string, 13)
		for i := range 13 {
			previous[fmt.Sprintf("old-%02d", i)] = testMetadataOne
			replacement[fmt.Sprintf("new-%02d", i)] = testMetadataOne
		}
		current := readVault()
		current.Spec.Metadata = &previous
		Expect(k8sClient.Update(ctx, current)).To(Succeed())
		provision()
		current = readVault()
		current.Spec.Metadata = &replacement
		Expect(k8sClient.Update(ctx, current)).To(Succeed())
		p.loseUpdate = true
		Expect(stepVault()).NotTo(Succeed())
		Expect(p.updates).To(Equal(1))
		Expect(readVault().Status.ManagedMetadataKeys).To(HaveLen(26))
		p.loseUpdate = false
		current = readVault()
		empty := make(map[string]string)
		current.Spec.Metadata = &empty
		Expect(k8sClient.Update(ctx, current)).To(Succeed())
		Expect(stepVault()).To(Succeed())
		Expect(stepVault()).To(Succeed())
		Expect(p.updates).To(Equal(2))
		Expect(p.vault.Metadata).To(HaveLen(3)) // Only reserved ownership keys remain.
		Expect(readVault().Status.ManagedMetadataKeys).To(BeEmpty())
	})
	It("retains credential metadata ownership when metadata is omitted before an explicit clear", func() {
		current := readCredential()
		metadata := map[string]string{testTeamMetadataKey: testMetadataOne}
		current.Spec.Metadata = &metadata
		Expect(k8sClient.Update(ctx, current)).To(Succeed())
		provision()
		p.credential.Metadata[testExternalMetadataKey] = testExternalMetadataValue
		current = readCredential()
		current.Spec.Metadata = nil
		Expect(k8sClient.Update(ctx, current)).To(Succeed())
		Expect(stepCredential()).To(Succeed())
		Expect(stepCredential()).To(Succeed())
		Expect(readCredential().Status.ManagedMetadataKeys).To(ContainElement(testTeamMetadataKey))
		current = readCredential()
		empty := make(map[string]string)
		current.Spec.Metadata = &empty
		Expect(k8sClient.Update(ctx, current)).To(Succeed())
		Expect(stepCredential()).To(Succeed())
		Expect(p.credential.Metadata).NotTo(HaveKey(testTeamMetadataKey))
		Expect(p.credential.Metadata).To(HaveKeyWithValue(testExternalMetadataKey, testExternalMetadataValue))
	})
	It("reports a missing credential Secret as an unavailable protected source", func() {
		provision()
		Expect(k8sClient.Delete(ctx, secret)).To(Succeed())
		Expect(stepCredential()).To(Succeed())
		Expect(meta.FindStatusCondition(readCredential().Status.Conditions, readyCondition).Reason).To(Equal("ProtectedSourceUnavailable"))
		Expect(p.rotations).To(BeZero())
	})
	It("recovers lost Vault and credential creates without another POST and rotates unacknowledged material", func() {
		p.loseCreate = true
		Expect(stepVault()).To(Succeed())
		Expect(stepVault()).NotTo(Succeed())
		Expect(stepVault()).To(Succeed())
		Expect(stepVault()).To(Succeed())
		p.loseCredentialCreate = true
		Expect(stepCredential()).To(Succeed())
		Expect(stepCredential()).NotTo(Succeed())
		Expect(stepCredential()).To(Succeed())
		Expect(readCredential().Status.SourceVersions).To(BeEmpty())
		Expect(stepCredential()).To(Succeed())
		Expect(stepCredential()).To(Succeed())
		Expect(p.creates).To(Equal(1))
		Expect(p.credentialCreates).To(Equal(1))
		Expect(p.rotations).To(Equal(1))
	})
	It("keeps ambiguous missing discovery blocked and retains cleanup obligations", func() {
		p.loseCreate, p.hideDiscovery = true, true
		Expect(stepVault()).To(Succeed())
		Expect(stepVault()).NotTo(Succeed())
		for range 3 {
			Expect(stepVault()).To(Succeed())
		}
		Expect(p.creates).To(Equal(1))
		Expect(readVault().Status.OpenAIVaultID).To(BeEmpty())
		Expect(readVault().Finalizers).To(ContainElement(vaultFinalizer))
		Expect(meta.FindStatusCondition(readVault().Status.Conditions, readyCondition).Reason).To(Equal(creationOutcomeUnknownReason))
	})
	It("rotates after a source update and does not acknowledge an ambiguous write", func() {
		provision()
		before := readCredential().Status.SourceVersions
		secret.Data[testVaultTokenKey] = []byte(testCredentialToken())
		Expect(k8sClient.Update(ctx, secret)).To(Succeed())
		p.loseRotation = true
		Expect(stepCredential()).NotTo(Succeed())
		Expect(readCredential().Status.SourceVersions).To(Equal(before))
		p.loseRotation = false
		Expect(stepCredential()).To(Succeed())
		Expect(stepCredential()).To(Succeed())
		Expect(reflect.DeepEqual(readCredential().Status.SourceVersions, before)).To(BeFalse())
		Expect(p.rotations).To(Equal(2))
	})
	It("protects frozen Session attachments and retries cleanup until remote absence", func() {
		provision()
		s := &agentsv1alpha1.Session{ObjectMeta: metav1.ObjectMeta{GenerateName: "vault-reference-", Namespace: testNamespace},
			Spec: agentsv1alpha1.SessionSpec{AgentRef: &agentsv1alpha1.AgentReference{Name: "unused-agent"},
				EnvironmentRef: &agentsv1alpha1.AgentReference{Name: "unused-environment"},
				SpendControl:   &agentsv1alpha1.SessionSpendControl{Mode: testUnlimitedSpendMode}}}
		Expect(k8sClient.Create(ctx, s)).To(Succeed())
		s.Status.VaultSnapshots = []agentsv1alpha1.SessionVaultSnapshot{{Name: v.Name, UID: string(v.UID), Generation: v.Generation, OpenAIVaultID: p.vault.ID}}
		Expect(k8sClient.Status().Update(ctx, s)).To(Succeed())
		Expect(k8sClient.Delete(ctx, c)).To(Succeed())
		Expect(stepCredential()).To(Succeed())
		Expect(p.deletes).To(BeZero())
		Expect(meta.FindStatusCondition(readCredential().Status.Conditions, readyCondition).Reason).To(Equal("CredentialInUse"))
		Expect(k8sClient.Delete(ctx, s)).To(Succeed())
		p.deleteFailure = true
		Expect(stepCredential()).NotTo(Succeed())
		Expect(readCredential().Finalizers).To(ContainElement(credentialFinalizer))
		p.deleteFailure = false
		Expect(stepCredential()).To(Succeed())
		Expect(p.deletes).To(Equal(1))
		Expect(stepCredential()).To(Succeed())
		Expect(readCredential().Finalizers).NotTo(ContainElement(credentialFinalizer))
	})
	It("does not POST after a conflicting Vault creation claim", func() {
		Expect(stepVault()).To(Succeed())
		vr.Client = &interceptClient{Client: k8sClient, beforeStatusPatch: func(obj client.Object) error {
			return apierrors.NewConflict(schema.GroupResource{Group: "agents.softcane.io", Resource: "vaults"}, obj.GetName(), errors.New("concurrent claim"))
		}}
		Expect(apierrors.IsConflict(stepVault())).To(BeTrue())
		Expect(p.creates).To(BeZero())
		vr.Client = k8sClient
		Expect(stepVault()).To(Succeed())
		Expect(p.creates).To(Equal(1))
	})
	It("recovers credential creation when remote success cannot be saved in status", func() {
		for range 3 {
			Expect(stepVault()).To(Succeed())
		}
		Expect(stepCredential()).To(Succeed())
		cr.Client = &interceptClient{Client: k8sClient, beforeStatusPatch: func(obj client.Object) error {
			credential, ok := obj.(*agentsv1alpha1.VaultCredential)
			if ok && credential.Status.OpenAICredentialID != "" {
				return errors.New("status storage unavailable")
			}
			return nil
		}}
		Expect(stepCredential()).NotTo(Succeed())
		Expect(p.credentialCreates).To(Equal(1))
		Expect(readCredential().Status.OpenAICredentialID).To(BeEmpty())
		cr.Client = k8sClient
		Expect(stepCredential()).To(Succeed())
		Expect(stepCredential()).To(Succeed())
		Expect(stepCredential()).To(Succeed())
		Expect(p.credentialCreates).To(Equal(1))
		Expect(p.rotations).To(Equal(1))
	})
	It("blocks cleanup of another owner's remote identity", func() {
		provision()
		p.credential.OwnerUID = "different-owner"
		Expect(k8sClient.Delete(ctx, c)).To(Succeed())
		Expect(stepCredential()).To(Succeed())
		Expect(p.deletes).To(BeZero())
		Expect(readCredential().Finalizers).To(ContainElement(credentialFinalizer))
		Expect(meta.FindStatusCondition(readCredential().Status.Conditions, readyCondition).Reason).To(Equal("OwnershipMismatch"))
	})
	It("protects deleting Vaults until their credential resources are removed", func() {
		provision()
		Expect(k8sClient.Delete(ctx, v)).To(Succeed())
		Expect(stepVault()).To(Succeed())
		Expect(p.deletes).To(BeZero())
		Expect(meta.FindStatusCondition(readVault().Status.Conditions, readyCondition).Reason).To(Equal("CredentialsInUse"))
	})

	It("does not transfer a credential claim to a recreated Vault with the same name", func() {
		provision()
		previousUID := string(v.UID)
		parent := readVault()
		parent.Spec.DeletionPolicy = retainPolicy
		Expect(k8sClient.Update(ctx, parent)).To(Succeed())
		Expect(k8sClient.Delete(ctx, parent)).To(Succeed())
		Expect(stepVault()).To(Succeed())
		v = &agentsv1alpha1.Vault{ObjectMeta: metav1.ObjectMeta{Name: parent.Name, Namespace: parent.Namespace}, Spec: agentsv1alpha1.VaultSpec{DeletionPolicy: deletePolicy}}
		Expect(k8sClient.Create(ctx, v)).To(Succeed())
		for range 3 {
			Expect(stepVault()).To(Succeed())
		}
		Expect(string(v.UID)).NotTo(Equal(previousUID))
		Expect(stepCredential()).To(Succeed())
		Expect(readCredential().Status.VaultUID).To(Equal(previousUID))
		Expect(p.credentialCreates).To(Equal(1))
		Expect(p.rotations).To(BeZero())
		Expect(meta.FindStatusCondition(readCredential().Status.Conditions, readyCondition).Reason).To(Equal("VaultNotReady"))
	})

	It("rejects unrelated and cross-namespace protected references through admission", func() {
		raw, err := json.Marshal(c.Spec)
		Expect(err).NotTo(HaveOccurred())
		var spec map[string]any
		Expect(json.Unmarshal(raw, &spec)).To(Succeed())
		auth, ok := spec["auth"].(map[string]any)
		Expect(ok).To(BeTrue())
		bearer, ok := auth["staticBearer"].(map[string]any)
		Expect(ok).To(BeTrue())
		ref, ok := bearer["tokenSecretRef"].(map[string]any)
		Expect(ok).To(BeTrue())
		obj := &unstructured.Unstructured{Object: map[string]any{testAPIVersionField: testAPIVersion, testKindField: "VaultCredential", agentMetadataField: map[string]any{testGenerateNameField: "invalid-credential-", testNamespaceField: testNamespace}, testSpecField: spec}}
		ref["name"] = "unrelated"
		Expect(apierrors.IsInvalid(k8sClient.Create(ctx, obj))).To(BeTrue())
		ref["name"] = openaiapi.VaultSecretName
		ref["namespace"] = "other-namespace"
		Expect(k8sClient.Create(ctx, obj, &client.CreateOptions{FieldValidation: metav1.FieldValidationStrict})).NotTo(Succeed())
		delete(ref, "namespace")
		bearer[testVaultTokenKey] = testForbiddenValue
		Expect(k8sClient.Create(ctx, obj, &client.CreateOptions{FieldValidation: metav1.FieldValidationStrict})).NotTo(Succeed())
	})
	It("requires deletion policy and isolates credential status from ordinary writes", func() {
		fresh := c.DeepCopy()
		fresh.ObjectMeta = metav1.ObjectMeta{GenerateName: "status-credential-", Namespace: testNamespace}
		fresh.Spec.DeletionPolicy = ""
		fresh.Status.OpenAICredentialID = "not-adopted"
		Expect(apierrors.IsInvalid(k8sClient.Create(ctx, fresh))).To(BeTrue())
		fresh.Spec.DeletionPolicy = deletePolicy
		Expect(k8sClient.Create(ctx, fresh)).To(Succeed())
		var stored agentsv1alpha1.VaultCredential
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(fresh), &stored)).To(Succeed())
		Expect(stored.Spec.DeletionPolicy).To(Equal(agentsv1alpha1.DeletionPolicyDelete))
		Expect(stored.Status.OpenAICredentialID).To(BeEmpty())
		stored.Status.OpenAICredentialID = "status-writer"
		Expect(k8sClient.Status().Update(ctx, &stored)).To(Succeed())
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(fresh), &stored)).To(Succeed())
		Expect(stored.Status.OpenAICredentialID).To(Equal("status-writer"))
		Expect(k8sClient.Delete(ctx, &stored)).To(Succeed())
	})

	It("allows Vault name updates, rejects conflicting clear, and keeps authentication type immutable", func() {
		missingPolicy := &agentsv1alpha1.Vault{ObjectMeta: metav1.ObjectMeta{GenerateName: "missing-policy-", Namespace: testNamespace}}
		Expect(apierrors.IsInvalid(k8sClient.Create(ctx, missingPolicy))).To(BeTrue())
		name := "original"
		current := readVault()
		current.Spec.DeletionPolicy = ""
		Expect(apierrors.IsInvalid(k8sClient.Update(ctx, current))).To(BeTrue())
		current = readVault()
		current.Spec.Name = &name
		Expect(k8sClient.Update(ctx, current)).To(Succeed())
		current.Spec.ClearName = true
		Expect(k8sClient.Update(ctx, current)).NotTo(Succeed())
		credential := readCredential()
		credential.Spec.Auth.Type = "environment_variable"
		credential.Spec.Auth.StaticBearer = nil
		credential.Spec.Auth.EnvironmentVariable = &agentsv1alpha1.VaultEnvironmentVariable{VariableName: "TEST_TOKEN", SecretValueSecretRef: agentsv1alpha1.CredentialSecretReference{Name: openaiapi.VaultSecretName, Key: testVaultTokenKey}, Networking: agentsv1alpha1.VaultCredentialNetworking{Type: "unrestricted"}}
		Expect(k8sClient.Update(ctx, credential)).NotTo(Succeed())
	})
})
