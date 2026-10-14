package controller

import (
	"context"
	"encoding/json"
	"errors"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	"github.com/softcane/codex-session-operator/internal/openaiapi"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	_testFirstKey        = "first-key"
	_testGatewayFunction = "function"
)

type gatewayStub struct {
	text, key   string
	stopped     bool
	unavailable bool
	policyErr   error
}

func (g *gatewayStub) InitialInput(context.Context, string) (string, string, error) {
	if g.unavailable {
		return "", "", errors.New("store unavailable")
	}
	return g.text, g.key, nil
}
func (g *gatewayStub) ValidateInvestigation(context.Context, *agentsv1alpha1.Session, *agentsv1alpha1.SessionEnvironmentSnapshot) error {
	return g.policyErr
}
func (g *gatewayStub) ValidateTools(context.Context, map[string]any) error { return nil }
func (g *gatewayStub) Quiesce(context.Context, string, string) error {
	if g.unavailable {
		return errors.New("store unavailable")
	}
	g.stopped = true
	return nil
}

type initialSessionStub struct {
	configured                *configuredSessionStub
	receivedText, receivedKey string
	beforePost                func()
}

func (p *initialSessionStub) CreateInitialSession(ctx context.Context, uid, scope, agentID string, environment, agent map[string]any, vaultIDs []string, metadata *map[string]string, spend *agentsv1alpha1.SessionSpendControl, text, key string) (openaiapi.Session, error) {
	if p.beforePost != nil {
		p.beforePost()
	}
	p.receivedText, p.receivedKey = text, key
	_, err := p.configured.CreateConfiguredSession(ctx, uid, scope, agentID, environment, agent, vaultIDs, metadata, spend)
	p.configured.lifecycle.mu.Lock()
	defer p.configured.lifecycle.mu.Unlock()
	remote := &p.configured.lifecycle.remote
	remote.EnvironmentType = _noneMode
	remote.EnvironmentID = ""
	remote.SelfHosted = false
	return *remote, err
}

var _ = Describe("Legacy gateway coordinated none creation (pre-migration API server)", func() {
	useLegacySessionSchema()
	var environment *agentsv1alpha1.Environment
	var session *agentsv1alpha1.Session
	var r *SessionReconciler
	var gateway *gatewayStub
	var lifecycle *sessionProviderStub
	var initial *initialSessionStub
	read := func() *agentsv1alpha1.Session {
		var current agentsv1alpha1.Session
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(session), &current)).To(Succeed())
		return &current
	}
	step := func() error {
		_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(session)})
		return err
	}
	BeforeEach(func() {
		environment = &agentsv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{GenerateName: "gateway-none-", Namespace: testNamespace}, Spec: agentsv1alpha1.EnvironmentSpec{Mode: _noneMode, DeletionPolicy: "Delete"}}
		Expect(k8sClient.Create(ctx, environment)).To(Succeed())
		er := &EnvironmentReconciler{Client: k8sClient, APIReader: k8sClient, OwnerScope: testAgentScope, GatewayEnabled: true}
		for range 2 {
			_, err := er.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(environment)})
			Expect(err).NotTo(HaveOccurred())
		}
		session = &agentsv1alpha1.Session{ObjectMeta: metav1.ObjectMeta{GenerateName: "gateway-session-", Namespace: testNamespace}, Spec: agentsv1alpha1.SessionSpec{EnvironmentRef: &agentsv1alpha1.AgentReference{Name: environment.Name}}}
		Expect(k8sClient.Create(ctx, session)).To(Succeed())
		lifecycle = &sessionProviderStub{}
		configured := &configuredSessionStub{lifecycle: lifecycle}
		initial = &initialSessionStub{configured: configured}
		gateway = &gatewayStub{}
		r = &SessionReconciler{Client: k8sClient, APIReader: k8sClient, OpenAI: lifecycle, Configuration: configured, Environments: &environmentProviderStub{}, AgentOwnerScope: testAgentScope, Gateway: gateway, InitialSessions: initial}
	})
	AfterEach(func() {
		current := read()
		current.Finalizers = nil
		Expect(k8sClient.Update(ctx, current)).To(Succeed())
		Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, current))).To(Succeed())
		var currentEnvironment agentsv1alpha1.Environment
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(environment), &currentEnvironment)).To(Succeed())
		currentEnvironment.Finalizers = nil
		Expect(k8sClient.Update(ctx, &currentEnvironment)).To(Succeed())
		Expect(k8sClient.Delete(ctx, &currentEnvironment)).To(Succeed())
	})
	It("consults the source policy gate when the public issue marker is missing", func() {
		gateway.text, gateway.key = "issue evidence", _testFirstKey
		gateway.policyErr = errors.New("source identified by gateway store")
		Expect(step()).To(Succeed())
		Expect(step()).NotTo(Succeed())
		Expect(read().Annotations).To(BeEmpty())
		Expect(read().Status.CreationAttempted).To(BeFalse())
		Expect(lifecycle.creates).To(BeZero())
		Expect(meta.FindStatusCondition(read().Status.Conditions, readyCondition).Reason).To(Equal("InvestigationPolicyBlocked"))
	})
	It("blocks an issue policy failure before claiming remote creation", func() {
		current := read()
		current.Annotations = map[string]string{"agents.softcane.io/issue-source": "fixture-source"}
		Expect(k8sClient.Update(ctx, current)).To(Succeed())
		gateway.text, gateway.key = "issue evidence", _testFirstKey
		gateway.policyErr = errors.New("policy denied")
		Expect(step()).To(Succeed())
		Expect(step()).NotTo(Succeed())
		Expect(read().Status.CreationAttempted).To(BeFalse())
		Expect(lifecycle.creates).To(BeZero())
		Expect(meta.FindStatusCondition(read().Status.Conditions, readyCondition).Reason).To(Equal("InvestigationPolicyBlocked"))
	})
	It("blocks issue creation when gateway configuration is disabled", func() {
		current := read()
		current.Annotations = map[string]string{"agents.softcane.io/issue-source": "fixture-source"}
		Expect(k8sClient.Update(ctx, current)).To(Succeed())
		r.Gateway = nil
		Expect(step()).To(Succeed())
		Expect(step()).To(Succeed())
		Expect(read().Status.CreationAttempted).To(BeFalse())
		Expect(lifecycle.creates).To(BeZero())
		Expect(meta.FindStatusCondition(read().Status.Conditions, readyCondition).Reason).To(Equal("GatewayRequired"))
	})
	It("waits for durable initial input then freezes claim before a single create", func() {
		Expect(step()).To(Succeed())
		Expect(step()).To(Succeed())
		Expect(read().Status.CreationAttempted).To(BeFalse())
		Expect(meta.FindStatusCondition(read().Status.Conditions, readyCondition).Reason).To(Equal("GatewayInputRequired"))
		gateway.text, gateway.key = "first prompt in application store", "stable-first-key"
		initial.beforePost = func() {
			current := read()
			Expect(current.Status.CreationAttempted).To(BeTrue())
			Expect(current.Status.EnvironmentSnapshot.Profile.Mode).To(Equal(_noneMode))
			Expect(current.Finalizers).To(ContainElement(sessionFinalizer))
		}
		Expect(step()).To(Succeed())
		Expect(step()).To(Succeed())
		Expect(lifecycle.creates).To(Equal(1))
		Expect(initial.receivedText).To(Equal(gateway.text))
		Expect(initial.receivedKey).To(Equal(gateway.key))
		Expect(meta.FindStatusCondition(read().Status.Conditions, readyCondition).Status).To(Equal(metav1.ConditionTrue))
		Expect(read().Status.PodName).To(BeEmpty())
		raw, err := json.Marshal(read())
		Expect(err).NotTo(HaveOccurred())
		Expect(string(raw)).NotTo(ContainSubstring(gateway.text))
		Expect(string(raw)).NotTo(ContainSubstring(gateway.key))
	})
	It("discovers the same combined create after lost response and restart without reposting input", func() {
		gateway.text, gateway.key = "recover me", _testFirstKey
		lifecycle.loseResponse = true
		Expect(step()).To(Succeed())
		Expect(step()).NotTo(Succeed())
		Expect(read().Status.OpenAISessionID).To(BeEmpty())
		Expect(read().Status.CreationAttempted).To(BeTrue())
		gateway.text = "changed store text must not be read on recovery"
		Expect(step()).To(Succeed())
		Expect(step()).To(Succeed())
		Expect(lifecycle.creates).To(Equal(1))
		Expect(initial.receivedText).To(Equal("recover me"))
		Expect(read().Status.OpenAISessionID).To(Equal("sess_test"))
	})
	It("retains unknown-outcome cleanup and never creates a second Session", func() {
		gateway.text, gateway.key = "recover me", _testFirstKey
		lifecycle.loseResponse = true
		lifecycle.hideDiscovery = true
		Expect(step()).To(Succeed())
		Expect(step()).NotTo(Succeed())
		Expect(step()).To(Succeed())
		Expect(lifecycle.creates).To(Equal(1))
		Expect(read().Finalizers).To(ContainElement(sessionFinalizer))
		Expect(meta.FindStatusCondition(read().Status.Conditions, readyCondition).Reason).To(Equal(creationOutcomeUnknownReason))
	})
	It("holds cleanup when gateway cannot quiesce and observes remote absence after recovery", func() {
		gateway.text, gateway.key = "first", _testFirstKey
		Expect(step()).To(Succeed())
		Expect(step()).To(Succeed())
		current := read()
		current.Finalizers = append(current.Finalizers, "tests.softcane.io/hold")
		Expect(k8sClient.Update(ctx, current)).To(Succeed())
		Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, current))).To(Succeed())
		gateway.unavailable = true
		Expect(step()).NotTo(Succeed())
		Expect(lifecycle.deletes).To(BeZero())
		Expect(read().Finalizers).To(ContainElement(sessionFinalizer))
		gateway.unavailable = false
		Expect(step()).To(Succeed())
		Expect(gateway.stopped).To(BeTrue())
		Expect(lifecycle.deletes).To(Equal(1))
		// A test-only finalizer keeps the primary readable for AfterEach.
	})
	It("accepts function tools only with a registered gateway and rejects computer use without desktop", func() {
		fields := map[string]any{"tools": []any{map[string]any{_wireType: _testGatewayFunction, "name": "gateway_echo"}}}
		snapshot := &agentsv1alpha1.SessionEnvironmentSnapshot{Profile: agentsv1alpha1.EnvironmentSpec{Mode: _noneMode}}
		reason, _ := r.gatewayAgentBlock(ctx, fields, snapshot)
		Expect(reason).To(BeEmpty())
		fields["tools"] = []any{map[string]any{_wireType: "computer_use"}}
		reason, _ = r.gatewayAgentBlock(ctx, fields, snapshot)
		Expect(reason).To(Equal("HostedDesktopRequired"))
		// Typed function schemas retain the existing CRD validation path.
		current := read()
		current.Spec.Agent = &agentsv1alpha1.SessionAgentOverrides{Tools: &[]agentsv1alpha1.AgentTool{{Type: _testGatewayFunction, Function: &agentsv1alpha1.AgentFunction{Name: "gateway_echo", Description: "Return arguments", Parameters: apiextensionsv1.JSON{Raw: []byte(`{"type":"object"}`)}}}}}
		Expect(k8sClient.Update(ctx, current)).To(Succeed())
	})
})
