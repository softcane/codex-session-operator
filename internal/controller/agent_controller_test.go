package controller

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"strings"
	"sync"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	"github.com/softcane/codex-session-operator/internal/openaiapi"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	testNamespaceField        = "namespace"
	testSpecField             = "spec"
	testExternalMetadataKey   = "external"
	testExternalMetadataValue = "keep"
	testTeamMetadataKey       = "team"
	testReasoningField        = "reasoning"
	testAgentInUseReason      = "AgentInUse"
	testInitialMetadataValue  = "one"
)

const (
	testKindField         = "kind"
	testGenerateNameField = "generateName"
	testAPIVersionField   = "apiVersion"
	testMCPType           = "mcp"
	testAPIVersion        = "agents.softcane.io/v1alpha1"
	testServerURLField    = "serverURL"
	testHTTPType          = "http"
	testAgentKind         = "Agent"
	otherTestFinalizer    = "example.com/other"
	updatedTestModel      = "gpt-6-astra"
	agentTypeField        = "type"
	testNamespace         = "default"
	agentModelField       = "model"
	agentToolsField       = "tools"
	testAgentID           = "agent_test"
	agentRefField         = "agentRef"
	testAgentScope        = "test-installation/test-project"
)

var _ AgentProvider = (*agentProviderStub)(nil)
var _ SessionProvider = (*sessionProviderStub)(nil)
var _ AgentProvider = (*openaiapi.Client)(nil)
var _ SessionProvider = (*openaiapi.Client)(nil)

type agentProviderStub struct {
	mu                                                   sync.Mutex
	remote                                               openaiapi.Agent
	creates, updates, deletes                            int
	loseCreate, loseUpdate, hideDiscovery, deleteFailure bool
	discoveryErr                                         error
	updateFailure                                        error
}

func copyConfiguration(fields map[string]any) map[string]any {
	data, err := json.Marshal(fields)
	Expect(err).NotTo(HaveOccurred())
	var result map[string]any
	Expect(json.Unmarshal(data, &result)).To(Succeed())
	return result
}

func (p *agentProviderStub) CreateAgent(_ context.Context, uid, scope string, fields map[string]any) (openaiapi.Agent, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.creates++
	fields = copyConfiguration(fields)
	metadata, ok := fields[agentMetadataField].(map[string]any)
	if !ok {
		metadata = make(map[string]any)
	}
	metadata["kubernetes_uid"], metadata["operator"], metadata["owner_scope"] = uid, "codex-session-operator", scope
	fields[agentMetadataField] = metadata
	p.remote = openaiapi.Agent{ID: testAgentID, OwnerUID: uid, OwnerScope: scope, Model: agentStubModel(fields), Config: fields}
	if p.loseCreate {
		return openaiapi.Agent{}, errors.New("Create response lost after commit")
	}
	return p.remote, nil
}
func (p *agentProviderStub) GetAgent(_ context.Context, id string) (openaiapi.Agent, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.remote.ID != id || id == "" {
		return openaiapi.Agent{}, openaiapi.ErrNotFound
	}
	remote := p.remote
	remote.Config = copyConfiguration(p.remote.Config)
	return remote, nil
}
func (p *agentProviderStub) FindAgent(_ context.Context, uid, scope string) (openaiapi.Agent, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.discoveryErr != nil {
		return openaiapi.Agent{}, p.discoveryErr
	}
	if p.hideDiscovery || p.remote.OwnerUID != uid || p.remote.OwnerScope != scope {
		return openaiapi.Agent{}, nil
	}
	return p.remote, nil
}
func (p *agentProviderStub) UpdateAgent(_ context.Context, _ string, fields map[string]any) (openaiapi.Agent, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.updates++
	if p.updateFailure != nil {
		return openaiapi.Agent{}, p.updateFailure
	}
	maps.Copy(p.remote.Config, copyConfiguration(fields))
	p.remote.Model = agentStubModel(p.remote.Config)
	if p.loseUpdate {
		return openaiapi.Agent{}, errors.New("Update response lost after commit")
	}
	return p.remote, nil
}
func (p *agentProviderStub) DeleteAgent(_ context.Context, _ string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.deletes++
	if p.deleteFailure {
		return errors.New("Delete temporarily unavailable")
	}
	p.remote = openaiapi.Agent{}
	return nil
}

var _ = Describe("Saved Agent lifecycle (real Kubernetes API)", func() {
	var resource *agentsv1alpha1.Agent
	var provider *agentProviderStub
	var reconciler *AgentReconciler
	var request reconcile.Request
	read := func() *agentsv1alpha1.Agent {
		var agent agentsv1alpha1.Agent
		Expect(k8sClient.Get(ctx, request.NamespacedName, &agent)).To(Succeed())
		return &agent
	}
	step := func() error { _, err := reconciler.Reconcile(ctx, request); return err }
	provision := func() { Expect(step()).To(Succeed()); Expect(step()).To(Succeed()); Expect(step()).To(Succeed()) }
	BeforeEach(func() {
		resource = &agentsv1alpha1.Agent{ObjectMeta: metav1.ObjectMeta{GenerateName: "agent-", Namespace: testNamespace, Finalizers: []string{otherTestFinalizer}}, Spec: agentsv1alpha1.AgentSpec{Model: openaiapi.SessionModel, DeletionPolicy: deletePolicy}}
		Expect(k8sClient.Create(ctx, resource)).To(Succeed())
		request = reconcile.Request{NamespacedName: client.ObjectKeyFromObject(resource)}
		provider = &agentProviderStub{}
		reconciler = &AgentReconciler{Client: k8sClient, APIReader: k8sClient, OpenAI: provider, OwnerScope: testAgentScope}
	})
	AfterEach(func() {
		a := read()
		a.Finalizers = nil
		Expect(k8sClient.Update(ctx, a)).To(Succeed())
		if a.DeletionTimestamp.IsZero() {
			Expect(k8sClient.Delete(ctx, a)).To(Succeed())
		}
	})
	It("persists claim/finalizer, observes Ready and performs no stable writes", func() {
		provision()
		agent := read()
		Expect(agent.Status.CreationAttempted).To(BeTrue())
		Expect(agent.Status.OwnerScope).To(Equal(testAgentScope))
		Expect(agent.Finalizers).To(ContainElement(otherTestFinalizer))
		Expect(meta.IsStatusConditionTrue(agent.Status.Conditions, readyCondition)).To(BeTrue())
		version := agent.ResourceVersion
		Expect(step()).To(Succeed())
		Expect(read().ResourceVersion).To(Equal(version))
		Expect(provider.creates).To(Equal(1))
		Expect(provider.updates).To(BeZero())
	})
	It("recovers committed creation after a lost response without another POST", func() {
		provider.loseCreate = true
		Expect(step()).To(Succeed())
		Expect(step()).NotTo(Succeed())
		Expect(read().Status.OpenAIAgentID).To(BeEmpty())
		Expect(step()).To(Succeed())
		Expect(step()).To(Succeed())
		Expect(read().Status.OpenAIAgentID).To(Equal(testAgentID))
		Expect(provider.creates).To(Equal(1))
	})
	It("blocks missing and duplicate discovery without resetting the claim", func() {
		provider.loseCreate = true
		provider.hideDiscovery = true
		Expect(step()).To(Succeed())
		Expect(step()).NotTo(Succeed())
		Expect(step()).To(Succeed())
		Expect(step()).To(Succeed())
		Expect(read().Status.Conditions[0].Reason).To(Equal(creationOutcomeUnknownReason))
		provider.discoveryErr = errors.New("Multiple owned Agents require audit")
		Expect(step()).NotTo(Succeed())
		Expect(provider.creates).To(Equal(1))
	})
	It("does not POST if the creation claim conflicts", func() {
		Expect(step()).To(Succeed())
		reconciler.Client = &interceptClient{Client: k8sClient, beforeStatusPatch: func(obj client.Object) error {
			return apierrors.NewConflict(schema.GroupResource{Group: "agents.softcane.io", Resource: "agents"}, obj.GetName(), errors.New("Concurrent write"))
		}}
		Expect(apierrors.IsConflict(step())).To(BeTrue())
		Expect(provider.creates).To(BeZero())
	})
	It("recovers remote success followed by a failed identity status write", func() {
		Expect(step()).To(Succeed())
		reconciler.Client = &interceptClient{Client: k8sClient, beforeStatusPatch: func(obj client.Object) error {
			a, ok := obj.(*agentsv1alpha1.Agent)
			if ok && a.Status.OpenAIAgentID != "" {
				return errors.New("Status storage unavailable")
			}
			return nil
		}}
		Expect(step()).NotTo(Succeed())
		Expect(provider.creates).To(Equal(1))
		reconciler.Client = k8sClient
		Expect(step()).To(Succeed())
		Expect(provider.creates).To(Equal(1))
	})

	It("marks rejected model settings unready without modifying saved configuration", func() {
		provision()
		previous := copyConfiguration(provider.remote.Config)
		agent := read()
		agent.Spec.Model = updatedTestModel
		Expect(k8sClient.Update(ctx, agent)).To(Succeed())
		provider.updateFailure = &openaiapi.AgentRequestError{Operation: "update", StatusCode: 400}
		Expect(step()).NotTo(Succeed())
		Expect(read().Status.Conditions[0].Reason).To(Equal("UpdateRejected"))
		Expect(read().Status.Conditions[0].Status).To(Equal(metav1.ConditionFalse))
		Expect(provider.remote.Config).To(Equal(previous))
		Expect(provider.creates).To(Equal(1))
		Expect(provider.updates).To(Equal(1))
	})
	It("reobserves committed updates and preserves unmanaged metadata", func() {
		provision()
		existing, ok := provider.remote.Config[agentMetadataField].(map[string]any)
		Expect(ok).To(BeTrue())
		existing[testExternalMetadataKey] = testExternalMetadataValue
		agent := read()
		instructions := "Updated instructions"
		metadata := map[string]string{testTeamMetadataKey: testInitialMetadataValue}
		agent.Spec.Instructions = &instructions
		agent.Spec.Metadata = &metadata
		Expect(k8sClient.Update(ctx, agent)).To(Succeed())
		provider.loseUpdate = true
		Expect(step()).NotTo(Succeed())
		Expect(step()).To(Succeed())
		Expect(provider.updates).To(Equal(1))
		Expect(provider.creates).To(Equal(1))
		existing, ok = provider.remote.Config[agentMetadataField].(map[string]any)
		Expect(ok).To(BeTrue())
		Expect(existing).To(HaveKeyWithValue(testExternalMetadataKey, testExternalMetadataValue))
		Expect(read().Status.ObservedGeneration).To(Equal(read().Generation))
	})

	It("replaces reasoning even when omitted effort cannot be distinguished from defaults", func() {
		effort, summary := "high", "auto"
		agent := read()
		agent.Spec.Reasoning = &agentsv1alpha1.AgentReasoning{Effort: &effort, Summary: &summary}
		Expect(k8sClient.Update(ctx, agent)).To(Succeed())
		provision()
		agent = read()
		agent.Spec.Reasoning = &agentsv1alpha1.AgentReasoning{Summary: &summary}
		Expect(k8sClient.Update(ctx, agent)).To(Succeed())
		Expect(step()).To(Succeed())
		Expect(step()).To(Succeed())
		Expect(provider.updates).To(Equal(1))
		reasoning, ok := provider.remote.Config[testReasoningField].(map[string]any)
		Expect(ok).To(BeTrue())
		Expect(reasoning).NotTo(HaveKey("effort"))
		agent = read()
		agent.Spec.Reasoning = nil
		agent.Spec.ResetReasoning = true
		Expect(k8sClient.Update(ctx, agent)).To(Succeed())
		Expect(step()).To(Succeed())
		provider.remote.Config[testReasoningField] = map[string]any{"effort": "medium"}
		Expect(step()).To(Succeed())
		Expect(step()).To(Succeed())
		Expect(provider.updates).To(Equal(2))
		Expect(read().Status.ReasoningConfigurationDigest).NotTo(BeEmpty())
		previousDigest := read().Status.ReasoningConfigurationDigest
		agent = read()
		agent.Spec.Model = updatedTestModel
		Expect(k8sClient.Update(ctx, agent)).To(Succeed())
		Expect(step()).To(Succeed())
		Expect(step()).To(Succeed())
		Expect(provider.updates).To(Equal(3))
		Expect(read().Status.ReasoningConfigurationDigest).NotTo(Equal(previousDigest))

	})
	It("removes formerly managed metadata keys after an explicitly empty replacement", func() {
		agent := read()
		metadata := map[string]string{testTeamMetadataKey: testInitialMetadataValue}
		agent.Spec.Metadata = &metadata
		Expect(k8sClient.Update(ctx, agent)).To(Succeed())
		provision()
		existing, ok := provider.remote.Config[agentMetadataField].(map[string]any)
		Expect(ok).To(BeTrue())
		existing[testExternalMetadataKey] = testExternalMetadataValue
		agent = read()
		empty := map[string]string{}
		agent.Spec.Metadata = &empty
		Expect(k8sClient.Update(ctx, agent)).To(Succeed())
		provider.loseUpdate = true
		Expect(step()).NotTo(Succeed())
		Expect(step()).To(Succeed())
		Expect(provider.updates).To(Equal(1))
		existing, ok = provider.remote.Config[agentMetadataField].(map[string]any)
		Expect(ok).To(BeTrue())
		Expect(existing).NotTo(HaveKey(testTeamMetadataKey))
		Expect(existing).To(HaveKeyWithValue(testExternalMetadataKey, testExternalMetadataValue))
		Expect(read().Status.ManagedMetadataKeys).To(BeEmpty())
	})
	It("retains metadata ownership when metadata is omitted before an explicit clear", func() {
		agent := read()
		metadata := map[string]string{testTeamMetadataKey: testInitialMetadataValue}
		agent.Spec.Metadata = &metadata
		Expect(k8sClient.Update(ctx, agent)).To(Succeed())
		provision()
		agent = read()
		agent.Spec.Metadata = nil
		Expect(k8sClient.Update(ctx, agent)).To(Succeed())
		Expect(step()).To(Succeed())
		Expect(read().Status.ManagedMetadataKeys).To(ContainElement(testTeamMetadataKey))
		agent = read()
		empty := make(map[string]string)
		agent.Spec.Metadata = &empty
		Expect(k8sClient.Update(ctx, agent)).To(Succeed())
		Expect(step()).To(Succeed())
		existing, ok := provider.remote.Config[agentMetadataField].(map[string]any)
		Expect(ok).To(BeTrue())
		Expect(existing).NotTo(HaveKey(testTeamMetadataKey))
	})
	It("retains cleanup after a failed finalizer patch and safely retries already-absent remote state", func() {
		provision()
		Expect(k8sClient.Delete(ctx, read())).To(Succeed())
		Expect(step()).To(Succeed())
		reconciler.Client = &interceptClient{Client: k8sClient, beforePatch: func(client.Object) error { return errors.New("Finalizer persistence unavailable") }}
		Expect(step()).NotTo(Succeed())
		Expect(read().Finalizers).To(ContainElement(agentFinalizer))
		reconciler.Client = k8sClient
		Expect(step()).To(Succeed())
		Expect(read().Finalizers).NotTo(ContainElement(agentFinalizer))
		Expect(provider.deletes).To(Equal(1))
	})
	It("uses the direct reader despite a stale cache and separates a recreated UID", func() {
		provision()
		oldUID := string(resource.UID)
		old := read()
		old.Finalizers = nil
		Expect(k8sClient.Update(ctx, old)).To(Succeed())
		Expect(k8sClient.Delete(ctx, old)).To(Succeed())
		replacement := &agentsv1alpha1.Agent{ObjectMeta: metav1.ObjectMeta{Name: old.Name, Namespace: old.Namespace}, Spec: old.Spec}
		Expect(k8sClient.Create(ctx, replacement)).To(Succeed())
		resource = replacement
		Expect(string(resource.UID)).NotTo(Equal(oldUID))
		provision()
		Expect(provider.creates).To(Equal(2))
		Expect(provider.remote.OwnerUID).To(Equal(string(resource.UID)))
		stale := resource.DeepCopy()
		reconciler.Client = &staleAgentClient{Client: k8sClient, stale: stale}
		Expect(step()).To(Succeed())
		Expect(provider.creates).To(Equal(2))
	})
	It("refuses foreign ownership, a changed scope and missing remote identity", func() {
		provision()
		provider.remote.OwnerUID = "another-uid"
		agent := read()
		agent.Spec.Model = updatedTestModel
		Expect(k8sClient.Update(ctx, agent)).To(Succeed())
		Expect(step()).To(Succeed())
		Expect(read().Status.Conditions[0].Reason).To(Equal("OwnershipMismatch"))
		Expect(provider.updates).To(BeZero())
		reconciler.OwnerScope = "other-installation"
		Expect(step()).To(Succeed())
		Expect(read().Status.Conditions[0].Reason).To(Equal("OwnershipScopeChanged"))
		reconciler.OwnerScope = testAgentScope
		provider.remote = openaiapi.Agent{}
		Expect(step()).NotTo(Succeed())
		Expect(provider.creates).To(Equal(1))
	})
	It("blocks deletion while Sessions reference it then retries cleanup and observes absence", func() {
		provision()
		session := &agentsv1alpha1.Session{ObjectMeta: metav1.ObjectMeta{GenerateName: "reference-", Namespace: testNamespace}, Spec: agentsv1alpha1.SessionSpec{
			AgentRef: &agentsv1alpha1.AgentReference{Name: resource.Name}, EnvironmentRef: &agentsv1alpha1.AgentReference{Name: "unused-profile"},
			SpendControl: &agentsv1alpha1.SessionSpendControl{Mode: spendModeUnlimited}}}
		Expect(k8sClient.Create(ctx, session)).To(Succeed())
		Expect(k8sClient.Delete(ctx, read())).To(Succeed())
		Expect(step()).To(Succeed())
		Expect(read().Status.Conditions[0].Reason).To(Equal(testAgentInUseReason))
		Expect(provider.deletes).To(BeZero())
		Expect(k8sClient.Delete(ctx, session)).To(Succeed())
		provider.deleteFailure = true
		Expect(step()).NotTo(Succeed())
		Expect(read().Finalizers).To(ContainElement(agentFinalizer))
		provider.deleteFailure = false
		Expect(step()).To(Succeed())
		Expect(read().Finalizers).To(ContainElement(agentFinalizer))
		Expect(step()).To(Succeed())
		Expect(read().Finalizers).NotTo(ContainElement(agentFinalizer))
	})
	It("retains a remote Agent and releases only its own finalizer", func() {
		provision()
		agent := read()
		agent.Spec.DeletionPolicy = retainPolicy
		Expect(k8sClient.Update(ctx, agent)).To(Succeed())
		Expect(k8sClient.Delete(ctx, agent)).To(Succeed())
		Expect(step()).To(Succeed())
		Expect(provider.deletes).To(BeZero())
		Expect(read().Finalizers).To(Equal([]string{otherTestFinalizer}))
	})
	It("keeps an unresolved deletion claimed for audit", func() {
		provider.loseCreate = true
		provider.hideDiscovery = true
		Expect(step()).To(Succeed())
		Expect(step()).NotTo(Succeed())
		Expect(k8sClient.Delete(ctx, read())).To(Succeed())
		Expect(step()).To(Succeed())
		Expect(read().Finalizers).To(ContainElement(agentFinalizer))
		Expect(provider.creates).To(Equal(1))
	})
})

var _ = Describe("Agent CRD admission", func() {
	It("roundtrips explicit false, empty collections and schema keywords without aliasing", func() {
		spec := map[string]any{
			agentModelField:  openaiapi.SessionModel,
			"deletionPolicy": deletePolicy,
			"multiAgent":     map[string]any{"enabled": false},
			"text":           map[string]any{"format": map[string]any{agentTypeField: "json_schema", "schema": map[string]any{agentTypeField: "object", "properties": map[string]any{testServerURLField: map[string]any{agentTypeField: "string"}}}}},
			agentToolsField:  []any{map[string]any{agentTypeField: testMCPType, testMCPType: map[string]any{"serverLabel": "test", "required": false, "allowedTools": []any{}, "transport": map[string]any{agentTypeField: testHTTPType, testHTTPType: map[string]any{testServerURLField: "https://example.com/mcp"}}}}},
		}
		obj := &unstructured.Unstructured{Object: map[string]any{testAPIVersionField: testAPIVersion, testKindField: testAgentKind, agentMetadataField: map[string]any{testGenerateNameField: "roundtrip-", testNamespaceField: testNamespace}, testSpecField: spec}}
		Expect(k8sClient.Create(ctx, obj)).To(Succeed())
		var stored agentsv1alpha1.Agent
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(obj), &stored)).To(Succeed())
		Expect(stored.Spec.MultiAgent.Enabled).To(BeFalse())
		Expect(stored.Spec.Tools).NotTo(BeNil())
		tool := (*stored.Spec.Tools)[0]
		Expect(tool.MCP.Required).To(HaveValue(BeFalse()))
		Expect(tool.MCP.AllowedTools).NotTo(BeNil())
		Expect(*tool.MCP.AllowedTools).To(BeEmpty())
		Expect(string(stored.Spec.Text.Format.Schema.Raw)).To(ContainSubstring(testServerURLField))
		copied := stored.DeepCopy()
		copied.Spec.Text.Format.Schema.Raw[0] = 'x'
		*(*copied.Spec.Tools)[0].MCP.Required = true
		Expect(stored.Spec.Text.Format.Schema.Raw[0]).To(Equal(byte('{')))
		Expect(*tool.MCP.Required).To(BeFalse())
		Expect(k8sClient.Delete(ctx, &stored)).To(Succeed())
	})

	It("requires a model and explicit deletion policy, isolates status and rejects union/secret fields", func() {
		resource := &agentsv1alpha1.Agent{ObjectMeta: metav1.ObjectMeta{GenerateName: "schema-", Namespace: testNamespace}}
		Expect(apierrors.IsInvalid(k8sClient.Create(ctx, resource))).To(BeTrue())
		resource.Spec.Model = openaiapi.SessionModel
		Expect(apierrors.IsInvalid(k8sClient.Create(ctx, resource))).To(BeTrue())
		resource.Spec.DeletionPolicy = deletePolicy
		resource.Status.OpenAIAgentID = "cannot-set-through-spec"
		Expect(k8sClient.Create(ctx, resource)).To(Succeed())
		var stored agentsv1alpha1.Agent
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(resource), &stored)).To(Succeed())
		Expect(stored.Spec.DeletionPolicy).To(Equal(agentsv1alpha1.DeletionPolicyDelete))
		Expect(stored.Status.OpenAIAgentID).To(BeEmpty())
		stored.Status.OpenAIAgentID = "saved"
		Expect(k8sClient.Status().Update(ctx, &stored)).To(Succeed())
		Expect(k8sClient.Delete(ctx, &stored)).To(Succeed())
		invalid := []map[string]any{
			{agentModelField: openaiapi.SessionModel, "serviceTier": "ultrafast"},
			{agentModelField: openaiapi.SessionModel, "name": strings.Repeat("n", 129)},
			{agentModelField: openaiapi.SessionModel, testNameField: testNameField, "clearName": true},
			{agentModelField: openaiapi.SessionModel, testReasoningField: map[string]any{}, "resetReasoning": true},
			{agentModelField: openaiapi.SessionModel, agentMetadataField: map[string]any{"owner_scope": "spoof"}},
			{agentModelField: openaiapi.SessionModel, agentToolsField: []any{map[string]any{agentTypeField: _testGatewayFunction}}},
			{agentModelField: openaiapi.SessionModel, agentToolsField: []any{map[string]any{agentTypeField: "tool_search", "computerUse": map[string]any{}}}},
			{agentModelField: openaiapi.SessionModel, "text": map[string]any{"format": map[string]any{agentTypeField: "json_schema"}}},
			{agentModelField: openaiapi.SessionModel, "multiAgent": map[string]any{"enabled": true, "maxConcurrentSubagents": 0}},
			{agentModelField: openaiapi.SessionModel, agentToolsField: []any{map[string]any{agentTypeField: testMCPType, testMCPType: map[string]any{"serverLabel": "test", "transport": map[string]any{agentTypeField: testHTTPType, testHTTPType: map[string]any{testServerURLField: "https://example.com/mcp", "headers": map[string]any{"Authorization": testForbiddenValue}}}}}}},
		}
		for _, spec := range invalid {
			spec["deletionPolicy"] = deletePolicy
			obj := &unstructured.Unstructured{Object: map[string]any{testAPIVersionField: testAPIVersion, testKindField: testAgentKind, agentMetadataField: map[string]any{testGenerateNameField: "invalid-", testNamespaceField: testNamespace}, testSpecField: spec}}
			Expect(apierrors.IsInvalid(k8sClient.Create(ctx, obj))).To(BeTrue())
		}
		obj := &unstructured.Unstructured{Object: map[string]any{testAPIVersionField: testAPIVersion, testKindField: testAgentKind, agentMetadataField: map[string]any{testGenerateNameField: "unknown-", testNamespaceField: testNamespace}, testSpecField: map[string]any{agentModelField: openaiapi.SessionModel, "deletionPolicy": deletePolicy, "authorization": testForbiddenValue}}}
		Expect(k8sClient.Create(ctx, obj, &client.CreateOptions{FieldValidation: metav1.FieldValidationStrict})).NotTo(Succeed())
	})
})

func agentStubModel(fields map[string]any) string {
	model, ok := fields[agentModelField].(string)
	Expect(ok).To(BeTrue())
	return model
}

type staleAgentClient struct {
	client.Client
	stale *agentsv1alpha1.Agent
}

func (c *staleAgentClient) Get(_ context.Context, _ client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
	agent, ok := obj.(*agentsv1alpha1.Agent)
	if !ok {
		return errors.New("Unexpected cached object type")
	}
	c.stale.DeepCopyInto(agent)
	return nil
}
