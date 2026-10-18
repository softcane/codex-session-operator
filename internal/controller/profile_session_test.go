package controller

import (
	"context"
	"encoding/json"
	"errors"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const _profileWorkSourceAnnotation = "agents.softcane.io/work-source"

type resolvedProfilePolicyGate struct {
	gatewayStub

	calls            int
	expectedAgent    string
	resolvedSession  *agentsv1alpha1.Session
	beforeResolution func()
}

func (g *resolvedProfilePolicyGate) ValidateSessionPolicy(_ context.Context, session *agentsv1alpha1.Session, environment *agentsv1alpha1.SessionEnvironmentSnapshot) error {
	g.calls++
	if session.Status.AgentSnapshot == nil {
		if g.beforeResolution != nil {
			g.beforeResolution()
		}
		return nil // The first gate precedes Agent and Vault resolution.
	}
	g.resolvedSession = session.DeepCopy()
	if environment == nil || session.Status.AgentSnapshot.UID != g.expectedAgent {
		return errors.New("accepted Profile dependency identity changed")
	}
	return nil
}

var _ = Describe("Accepted Profile resolved dependency gate with shipped CRDs", func() {
	DescribeTable("checks resolved safe snapshots before consuming the remote creation claim",
		func(mismatch bool) {
			fixture := newDependencyScenarioFixture()
			fixture.ready()
			session := fixture.newSession(fixture.namespace)
			session.Annotations = map[string]string{_profileWorkSourceAnnotation: "accepted-profile-test"}
			session.Spec.VaultRefs = []agentsv1alpha1.AgentReference{{Name: fixture.vault.Name}}
			Expect(fixture.client.Update(ctx, session)).To(Succeed())
			gate := &resolvedProfilePolicyGate{expectedAgent: string(fixture.agent.UID)}
			if mismatch {
				gate.expectedAgent = "prior-accepted-agent-uid"
			}
			fixture.sessionController.Gateway = gate
			fixture.sessionStep(session) // Persist the lifecycle finalizer first.
			_, err := fixture.sessionController.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(session)})
			fixture.read(session)
			Expect(gate.calls).To(Equal(2))
			resolved := gate.resolvedSession
			Expect(resolved).NotTo(BeNil())
			Expect(resolved.Status.AgentSnapshot.UID).To(Equal(string(fixture.agent.UID)))
			Expect(resolved.Status.EnvironmentSnapshot.UID).To(Equal(string(fixture.environment.UID)))
			Expect(resolved.Status.VaultSnapshots).To(HaveLen(1))
			Expect(resolved.Status.VaultSnapshots[0].UID).To(Equal(string(fixture.vault.UID)))
			Expect(resolved.Status.VaultSnapshots[0].Credentials).To(HaveLen(1))
			Expect(resolved.Status.VaultSnapshots[0].Credentials[0].UID).To(Equal(string(fixture.credential.UID)))
			raw, encodeErr := json.Marshal(resolved.Status)
			Expect(encodeErr).NotTo(HaveOccurred())
			Expect(string(raw)).NotTo(ContainSubstring(string(fixture.secret.Data[testVaultTokenKey])))
			Expect(string(raw)).NotTo(ContainSubstring(*fixture.agent.Spec.Instructions))
			Expect(resolved.Status.CreationAttempted).To(BeFalse())
			if mismatch {
				Expect(err).To(HaveOccurred())
				Expect(session.Status.CreationAttempted).To(BeFalse())
				Expect(session.Status.OpenAISessionID).To(BeEmpty())
				Expect(session.Status.AgentSnapshot).To(BeNil())
				Expect(session.Status.EnvironmentSnapshot).To(BeNil())
				Expect(session.Status.VaultSnapshots).To(BeEmpty())
				condition := meta.FindStatusCondition(session.Status.Conditions, readyCondition)
				Expect(condition.Status).To(Equal(metav1.ConditionFalse))
				Expect(condition.Reason).To(Equal("SessionPolicyBlocked"))
				Expect(fixture.sessions.creates).To(BeZero())
			} else {
				Expect(err).NotTo(HaveOccurred())
				Expect(session.Status.CreationAttempted).To(BeTrue())
				Expect(session.Status.AgentSnapshot.UID).To(Equal(string(fixture.agent.UID)))
				Expect(session.Status.EnvironmentSnapshot.UID).To(Equal(string(fixture.environment.UID)))
				Expect(fixture.sessions.creates).To(Equal(1))
			}
		}, Entry("accepted dependency matches", false), Entry("dependency UID changed during resolution", true))

	It("rejects an Agent recreated between the early gate and reference resolution", func() {
		fixture := newDependencyScenarioFixture()
		fixture.ready()
		session := fixture.newSession(fixture.namespace)
		session.Annotations = map[string]string{_profileWorkSourceAnnotation: "accepted-profile-replacement-test"}
		Expect(fixture.client.Update(ctx, session)).To(Succeed())
		oldUID := string(fixture.agent.UID)
		gate := &resolvedProfilePolicyGate{expectedAgent: oldUID}
		gate.beforeResolution = func() {
			fixture.read(fixture.agent)
			previous := fixture.agent
			// Model forced replacement only within this isolated envtest API server.
			previous.Finalizers = nil
			Expect(fixture.client.Update(ctx, previous)).To(Succeed())
			Expect(fixture.client.Delete(ctx, previous)).To(Succeed())
			fixture.agent = &agentsv1alpha1.Agent{ObjectMeta: metav1.ObjectMeta{Name: previous.Name, Namespace: previous.Namespace}, Spec: *previous.Spec.DeepCopy()}
			Expect(fixture.client.Create(ctx, fixture.agent)).To(Succeed())
			for range 3 {
				_, err := fixture.agentController.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(fixture.agent)})
				Expect(err).NotTo(HaveOccurred())
			}
		}
		fixture.sessionController.Gateway = gate
		fixture.sessionStep(session)
		_, err := fixture.sessionController.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(session)})
		Expect(err).To(HaveOccurred())
		fixture.read(session)
		Expect(gate.calls).To(Equal(2))
		Expect(gate.resolvedSession.Status.AgentSnapshot.UID).To(Equal(string(fixture.agent.UID)))
		Expect(gate.resolvedSession.Status.AgentSnapshot.UID).NotTo(Equal(oldUID))
		Expect(session.Status.CreationAttempted).To(BeFalse())
		Expect(session.Status.AgentSnapshot).To(BeNil())
		Expect(meta.FindStatusCondition(session.Status.Conditions, readyCondition).Reason).To(Equal("SessionPolicyBlocked"))
		Expect(fixture.sessions.creates).To(BeZero())
	})
})
