package controller

import (
	"context"
	"errors"
	"maps"
	"reflect"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	"github.com/softcane/codex-session-operator/internal/openaiapi"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const testSessionUpdatedEffort = "medium"

type configuredSessionStub struct {
	lifecycle         *sessionProviderStub
	updates           int
	loseUpdate        bool
	omitSpend         bool
	rejectSpend       bool
	rejectSpendUpdate bool
}

var _ SessionConfigurationProvider = (*configuredSessionStub)(nil)

func (p *configuredSessionStub) CreateConfiguredSession(ctx context.Context, uid, scope, agentID string, environment, agent map[string]any, vaultIDs []string, metadata *map[string]string, spend *agentsv1alpha1.SessionSpendControl) (openaiapi.Session, error) {
	if p.rejectSpend && spend != nil && spend.Mode == spendModeLimited {
		return openaiapi.Session{}, openaiapi.ErrSpendControlUnavailable
	}
	workspace, _ := environment["workspace_directory"].(string)
	_, err := p.lifecycle.CreateSelfHostedSession(ctx, uid, workspace)
	p.lifecycle.mu.Lock()
	defer p.lifecycle.mu.Unlock()
	remote := &p.lifecycle.remote
	remote.AgentID = agentID
	remote.OwnerScope = scope
	remote.VaultIDs = append([]string(nil), vaultIDs...)
	remote.AgentConfiguration = openaiapi.EffectiveSessionAgent(map[string]any{"model": openaiapi.SessionModel}, agent)
	remote.Metadata, _ = openaiapi.VaultMetadata(metadata, nil, nil, uid, scope)
	remote.Model, _ = remote.AgentConfiguration["model"].(string)
	remote.AgentConfigurationDigest = openaiapi.AgentConfigurationDigest(remote.AgentConfiguration)
	if spend != nil {
		remote.SpendLimitCents = spend.LimitCents
		remote.SpendControlObserved = spend.Mode == spendModeLimited
		if p.omitSpend {
			remote.SpendLimitCents = nil
			remote.SpendControlObserved = false
		}
	}
	return *remote, err
}
func (p *configuredSessionStub) UpdateSession(_ context.Context, _ string, fields map[string]any) (openaiapi.Session, error) {
	p.lifecycle.mu.Lock()
	defer p.lifecycle.mu.Unlock()
	p.updates++
	if _, present := fields["spend_control"]; present && p.rejectSpendUpdate {
		return openaiapi.Session{}, openaiapi.ErrSpendControlUnavailable
	}
	if metadata, ok := fields["metadata"].(map[string]string); ok {
		p.lifecycle.remote.Metadata = maps.Clone(metadata)
	}
	if value, ok := fields["spend_control"]; ok {
		p.lifecycle.remote.SpendLimitCents = nil
		p.lifecycle.remote.SpendControlObserved = false
		if limit, limited := value.(map[string]any); limited {
			cents, _ := limit["limit"].(int64)
			p.lifecycle.remote.SpendLimitCents = &cents
			p.lifecycle.remote.SpendControlObserved = true
		}
	}
	if agent, ok := fields["agent"].(map[string]any); ok {
		for key, value := range agent {
			if key == "reasoning" {
				existing, _ := p.lifecycle.remote.AgentConfiguration[key].(map[string]any)
				existing = maps.Clone(existing)
				if existing == nil {
					existing = make(map[string]any)
				}
				if effort, ok := value.(map[string]any); ok {
					maps.Copy(existing, effort)
				}
				p.lifecycle.remote.AgentConfiguration[key] = existing
				continue
			}
			p.lifecycle.remote.AgentConfiguration[key] = value
		}
	}
	p.lifecycle.remote.Model, _ = p.lifecycle.remote.AgentConfiguration["model"].(string)
	if p.loseUpdate {
		return openaiapi.Session{}, errors.New("update response lost after commit")
	}
	return p.lifecycle.remote, nil
}

var _ = Describe("Legacy Session overrides and later-turn updates", func() {
	useLegacySessionSchema()
	var s *agentsv1alpha1.Session
	var lifecycle *sessionProviderStub
	var configured *configuredSessionStub
	var r *SessionReconciler
	read := func() *agentsv1alpha1.Session {
		var result agentsv1alpha1.Session
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(s), &result)).To(Succeed())
		return &result
	}
	step := func() error {
		_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(s)})
		return err
	}
	BeforeEach(func() {
		instructions, effort := "creation-only instructions", "low"
		s = &agentsv1alpha1.Session{ObjectMeta: metav1.ObjectMeta{GenerateName: "configured-session-", Namespace: testNamespace}, Spec: agentsv1alpha1.SessionSpec{Agent: &agentsv1alpha1.SessionAgentOverrides{Instructions: &instructions, Reasoning: &agentsv1alpha1.AgentReasoning{Effort: &effort}}}}
		Expect(k8sClient.Create(ctx, s)).To(Succeed())
		lifecycle = &sessionProviderStub{}
		configured = &configuredSessionStub{lifecycle: lifecycle}
		r = &SessionReconciler{Client: k8sClient, APIReader: k8sClient, OpenAI: lifecycle, Configuration: configured, AgentOwnerScope: testAgentScope}
	})
	AfterEach(func() {
		current := read()
		current.Finalizers = nil
		Expect(k8sClient.Update(ctx, current)).To(Succeed())
		Expect(k8sClient.Delete(ctx, current)).To(Succeed())
	})
	It("applies overrides at creation, updates effort without replacing Session and preserves unrelated metadata", func() {
		Expect(step()).To(Succeed())
		Expect(step()).To(Succeed())
		created := read()
		sid := created.Status.OpenAISessionID
		Expect(created.Status.CreationOnlyDigest).NotTo(BeEmpty())
		Expect(lifecycle.remote.AgentConfiguration["instructions"]).To(Equal(*s.Spec.Agent.Instructions))
		lifecycle.remote.Metadata[testExternalMetadataKey] = testExternalMetadataValue
		effort := testSessionUpdatedEffort
		current := read()
		current.Spec.Agent.Reasoning.Effort = &effort
		metadata := map[string]string{testTeamMetadataKey: "phase4"}
		current.Spec.Metadata = &metadata
		Expect(k8sClient.Update(ctx, current)).To(Succeed())
		Expect(step()).To(Succeed())
		Expect(step()).To(Succeed())
		Expect(configured.updates).To(Equal(1))
		Expect(lifecycle.creates).To(Equal(1))
		Expect(read().Status.OpenAISessionID).To(Equal(sid))
		Expect(lifecycle.remote.Metadata[testExternalMetadataKey]).To(Equal(testExternalMetadataValue))
		Expect(lifecycle.remote.Metadata["owner_scope"]).To(Equal(testAgentScope))
		Expect(meta.FindStatusCondition(read().Status.Conditions, "ConfigurationCurrent").Status).To(Equal(metav1.ConditionTrue))
	})
	It("observes committed mutable updates after lost response before retrying", func() {
		Expect(step()).To(Succeed())
		Expect(step()).To(Succeed())
		effort := testSessionUpdatedEffort
		current := read()
		current.Spec.Agent.Reasoning.Effort = &effort
		Expect(k8sClient.Update(ctx, current)).To(Succeed())
		configured.loseUpdate = true
		Expect(step()).NotTo(Succeed())
		Expect(step()).To(Succeed())
		Expect(configured.updates).To(Equal(1))
		Expect(lifecycle.creates).To(Equal(1))
	})
	It("retains metadata ownership during unrelated updates with metadata omitted", func() {
		current := read()
		metadata := map[string]string{testTeamMetadataKey: testInitialMetadataValue}
		current.Spec.Metadata = &metadata
		Expect(k8sClient.Update(ctx, current)).To(Succeed())
		Expect(step()).To(Succeed())
		Expect(step()).To(Succeed())
		lifecycle.remote.Metadata[testExternalMetadataKey] = testExternalMetadataValue
		current = read()
		current.Spec.Metadata = nil
		effort := testSessionUpdatedEffort
		current.Spec.Agent.Reasoning.Effort = &effort
		Expect(k8sClient.Update(ctx, current)).To(Succeed())
		Expect(step()).To(Succeed())
		Expect(read().Status.ManagedMetadataKeys).To(ContainElement(testTeamMetadataKey))
		current = read()
		empty := make(map[string]string)
		current.Spec.Metadata = &empty
		Expect(k8sClient.Update(ctx, current)).To(Succeed())
		Expect(step()).To(Succeed())
		Expect(lifecycle.remote.Metadata).NotTo(HaveKey(testTeamMetadataKey))
		Expect(lifecycle.remote.Metadata).To(HaveKeyWithValue(testExternalMetadataKey, testExternalMetadataValue))
		Expect(lifecycle.creates).To(Equal(1))
	})
	It("sends and verifies an explicit spend limit at creation and on a later cap change", func() {
		initial := int64(900)
		current := read()
		current.Spec.SpendControl = &agentsv1alpha1.SessionSpendControl{Mode: spendModeLimited, LimitCents: &initial}
		Expect(k8sClient.Update(ctx, current)).To(Succeed())
		Expect(step()).To(Succeed())
		Expect(step()).To(Succeed())
		created := read()
		Expect(created.Status.SpendControlAttemptedDigest).To(BeEmpty())
		Expect(lifecycle.remote.SpendLimitCents).NotTo(BeNil())
		Expect(*lifecycle.remote.SpendLimitCents).To(Equal(initial))
		sid := created.Status.OpenAISessionID
		next := int64(1200)
		current = read()
		current.Spec.SpendControl.LimitCents = &next
		Expect(k8sClient.Update(ctx, current)).To(Succeed())
		Expect(step()).To(Succeed())
		Expect(configured.updates).To(Equal(1))
		Expect(lifecycle.remote.SpendLimitCents).NotTo(BeNil())
		Expect(*lifecycle.remote.SpendLimitCents).To(Equal(next))
		Expect(read().Status.SpendControlAttemptedDigest).To(BeEmpty())
		Expect(read().Status.OpenAISessionID).To(Equal(sid))
	})
	It("observes a committed spend cap after a lost update response without another update", func() {
		initial := int64(900)
		current := read()
		current.Spec.SpendControl = &agentsv1alpha1.SessionSpendControl{Mode: spendModeLimited, LimitCents: &initial}
		Expect(k8sClient.Update(ctx, current)).To(Succeed())
		Expect(step()).To(Succeed())
		Expect(step()).To(Succeed())
		next := int64(1200)
		current = read()
		current.Spec.SpendControl.LimitCents = &next
		Expect(k8sClient.Update(ctx, current)).To(Succeed())
		configured.loseUpdate = true
		Expect(step()).NotTo(Succeed())
		Expect(read().Status.SpendControlAttemptedDigest).NotTo(BeEmpty())
		Expect(step()).To(Succeed())
		Expect(configured.updates).To(Equal(1))
		Expect(read().Status.SpendControlAttemptedDigest).To(BeEmpty())
	})
	It("blocks a limited Session when creation readback cannot confirm its cap", func() {
		initial := int64(900)
		current := read()
		current.Spec.SpendControl = &agentsv1alpha1.SessionSpendControl{Mode: spendModeLimited, LimitCents: &initial}
		Expect(k8sClient.Update(ctx, current)).To(Succeed())
		configured.omitSpend = true
		Expect(step()).To(Succeed())
		Expect(step()).To(Succeed())
		Expect(read().Status.OpenAISessionID).NotTo(BeEmpty())
		Expect(meta.FindStatusCondition(read().Status.Conditions, readyCondition).Reason).To(Equal("SpendControlUnverified"))
		Expect(step()).To(Succeed())
		Expect(configured.updates).To(BeZero())
		Expect(meta.FindStatusCondition(read().Status.Conditions, readyCondition).Reason).To(Equal("SpendControlOutcomeUnknown"))
	})
	It("does not replay an unobserved spend update after a persisted claim", func() {
		Expect(step()).To(Succeed())
		Expect(step()).To(Succeed())
		limit := int64(700)
		current := read()
		current.Spec.SpendControl = &agentsv1alpha1.SessionSpendControl{Mode: spendModeLimited, LimitCents: &limit}
		Expect(k8sClient.Update(ctx, current)).To(Succeed())
		current = read()
		current.Status.SpendControlAttemptedDigest = spendControlDigest(current.Spec.SpendControl)
		Expect(k8sClient.Status().Update(ctx, current)).To(Succeed())
		Expect(step()).To(Succeed())
		Expect(configured.updates).To(BeZero())
		Expect(meta.FindStatusCondition(read().Status.Conditions, readyCondition).Reason).To(Equal("SpendControlOutcomeUnknown"))
	})
	It("keeps a rejected project budget create claimed without another POST", func() {
		limit := int64(500)
		current := read()
		current.Spec.SpendControl = &agentsv1alpha1.SessionSpendControl{Mode: spendModeLimited, LimitCents: &limit}
		Expect(k8sClient.Update(ctx, current)).To(Succeed())
		configured.rejectSpend = true
		Expect(step()).To(Succeed())
		Expect(step()).NotTo(Succeed())
		Expect(read().Status.CreationAttempted).To(BeTrue())
		Expect(meta.FindStatusCondition(read().Status.Conditions, readyCondition).Reason).To(Equal(spendUnavailableReason))
		Expect(step()).To(Succeed())
		Expect(lifecycle.creates).To(BeZero())
		Expect(meta.FindStatusCondition(read().Status.Conditions, readyCondition).Reason).To(Equal(spendUnavailableReason))
	})
	It("keeps a rejected project budget update claimed without replaying it", func() {
		Expect(step()).To(Succeed())
		Expect(step()).To(Succeed())
		limit := int64(500)
		current := read()
		current.Spec.SpendControl = &agentsv1alpha1.SessionSpendControl{Mode: spendModeLimited, LimitCents: &limit}
		Expect(k8sClient.Update(ctx, current)).To(Succeed())
		configured.rejectSpendUpdate = true
		Expect(step()).NotTo(Succeed())
		Expect(configured.updates).To(Equal(1))
		Expect(meta.FindStatusCondition(read().Status.Conditions, readyCondition).Reason).To(Equal(spendUnavailableReason))
		Expect(step()).To(Succeed())
		Expect(configured.updates).To(Equal(1))
		Expect(meta.FindStatusCondition(read().Status.Conditions, readyCondition).Reason).To(Equal(spendUnavailableReason))
	})
	It("reports creation-only edits explicitly and never replaces or updates those fields", func() {
		Expect(step()).To(Succeed())
		Expect(step()).To(Succeed())
		frozen := read().Status.CreationAgentDigest
		instructions := "changed instructions"
		current := read()
		current.Spec.Agent.Instructions = &instructions
		Expect(k8sClient.Update(ctx, current)).To(Succeed())
		Expect(step()).To(Succeed())
		Expect(meta.FindStatusCondition(read().Status.Conditions, "ConfigurationCurrent").Reason).To(Equal("RequiresNewSession"))
		Expect(read().Status.CreationAgentDigest).To(Equal(frozen))
		Expect(configured.updates).To(BeZero())
		Expect(lifecycle.creates).To(Equal(1))
	})
	It("allows legacy Sessions to gain supported mutable settings without replacing them", func() {
		current := read()
		current.Spec.Agent = nil
		Expect(k8sClient.Update(ctx, current)).To(Succeed())
		Expect(step()).To(Succeed())
		Expect(step()).To(Succeed())
		sid := read().Status.OpenAISessionID
		lifecycle.remote.AgentConfiguration = map[string]any{"model": openaiapi.SessionModel}
		model := updatedTestModel
		current = read()
		current.Spec.Agent = &agentsv1alpha1.SessionAgentOverrides{Model: &model}
		Expect(k8sClient.Update(ctx, current)).To(Succeed())
		Expect(step()).To(Succeed())
		Expect(step()).To(Succeed())
		Expect(step()).To(Succeed())
		Expect(read().Status.OpenAISessionID).To(Equal(sid))
		Expect(lifecycle.creates).To(Equal(1))
		Expect(configured.updates).To(Equal(1))
		Expect(meta.FindStatusCondition(read().Status.Conditions, "ConfigurationCurrent").Status).To(Equal(metav1.ConditionTrue))
	})

	It("does not claim creation when a Vault reference is absent", func() {
		current := read()
		current.Spec.VaultRefs = []agentsv1alpha1.AgentReference{{Name: "absent-vault"}}
		Expect(k8sClient.Update(ctx, current)).To(Succeed())
		r.Vaults = &vaultProviderStub{}
		Expect(step()).To(Succeed())
		Expect(step()).To(Succeed())
		Expect(read().Status.CreationAttempted).To(BeFalse())
		Expect(lifecycle.creates).To(BeZero())
		Expect(meta.FindStatusCondition(read().Status.Conditions, readyCondition).Reason).To(Equal("VaultNotReady"))
	})
	It("freezes resolved Ready Vault identities and source versions before POST", func() {
		v := &agentsv1alpha1.Vault{ObjectMeta: metav1.ObjectMeta{GenerateName: "session-vault-", Namespace: testNamespace}, Spec: agentsv1alpha1.VaultSpec{DeletionPolicy: "Delete"}}
		Expect(k8sClient.Create(ctx, v)).To(Succeed())
		p := &vaultProviderStub{}
		vr := &VaultReconciler{Client: k8sClient, APIReader: k8sClient, OpenAI: p, OwnerScope: testAgentScope}
		for range 3 {
			_, err := vr.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(v)})
			Expect(err).NotTo(HaveOccurred())
		}
		current := read()
		current.Spec.VaultRefs = []agentsv1alpha1.AgentReference{{Name: v.Name}}
		Expect(k8sClient.Update(ctx, current)).To(Succeed())
		r.Vaults = p
		Expect(step()).To(Succeed())
		Expect(step()).To(Succeed())
		Expect(lifecycle.remote.VaultIDs).To(Equal([]string{p.vault.ID}))
		Expect(read().Status.VaultSnapshots[0].UID).To(Equal(string(v.UID)))
		frozen := read().Status.VaultSnapshots
		current = read()
		current.Spec.VaultRefs = nil
		Expect(k8sClient.Update(ctx, current)).To(Succeed())
		Expect(step()).To(Succeed())
		Expect(reflect.DeepEqual(frozen, read().Status.VaultSnapshots)).To(BeTrue())
		Expect(lifecycle.creates).To(Equal(1))
		v.Finalizers = nil
		Expect(k8sClient.Update(ctx, v)).NotTo(Succeed()) // Stale resourceVersion must not overwrite the finalizer claim.
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(v), v)).To(Succeed())
		v.Finalizers = nil
		Expect(k8sClient.Update(ctx, v)).To(Succeed())
		Expect(k8sClient.Delete(ctx, v)).To(Succeed())
	})
})
