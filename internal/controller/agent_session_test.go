package controller

import (
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	"github.com/softcane/codex-session-operator/internal/openaiapi"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

var _ = Describe("Agent-backed Session creation", func() {
	var agent *agentsv1alpha1.Agent
	var sessions []*agentsv1alpha1.Session
	var agents *agentProviderStub
	var environment *agentsv1alpha1.Environment
	var environments *environmentProviderStub
	var provider *sessionProviderStub
	var agentController *AgentReconciler
	var sessionController *SessionReconciler
	agentStep := func() error {
		_, err := agentController.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(agent)})
		return err
	}
	sessionStep := func(session *agentsv1alpha1.Session) error {
		_, err := sessionController.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(session)})
		return err
	}
	read := func(session *agentsv1alpha1.Session) *agentsv1alpha1.Session {
		var current agentsv1alpha1.Session
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(session), &current)).To(Succeed())
		return &current
	}
	newSession := func(name string) *agentsv1alpha1.Session {
		s := &agentsv1alpha1.Session{ObjectMeta: metav1.ObjectMeta{GenerateName: "saved-session-", Namespace: testNamespace}, Spec: agentsv1alpha1.SessionSpec{
			AgentRef: &agentsv1alpha1.AgentReference{Name: name}, EnvironmentRef: &agentsv1alpha1.AgentReference{Name: environment.Name},
			SpendControl: &agentsv1alpha1.SessionSpendControl{Mode: spendModeUnlimited}}}
		Expect(k8sClient.Create(ctx, s)).To(Succeed())
		sessions = append(sessions, s)
		return s
	}
	readyAgent := func() {
		Expect(agentStep()).To(Succeed())
		Expect(agentStep()).To(Succeed())
		Expect(agentStep()).To(Succeed())
	}
	BeforeEach(func() {
		sessions = nil
		agent = &agentsv1alpha1.Agent{ObjectMeta: metav1.ObjectMeta{GenerateName: "saved-agent-", Namespace: testNamespace}, Spec: agentsv1alpha1.AgentSpec{Model: openaiapi.SessionModel, DeletionPolicy: deletePolicy}}
		Expect(k8sClient.Create(ctx, agent)).To(Succeed())
		agents = &agentProviderStub{}
		provider = &sessionProviderStub{}
		environment, environments = readyHostedSessionEnvironment(testNamespace)
		agentController = &AgentReconciler{Client: k8sClient, APIReader: k8sClient, OpenAI: agents, OwnerScope: testAgentScope}
		configuration := &dependencyScenarioSessionProvider{configuredSessionStub: configuredSessionStub{lifecycle: provider}, agents: agents}
		sessionController = &SessionReconciler{Client: k8sClient, APIReader: k8sClient, OpenAI: provider, Agents: agents,
			Environments: environments, Configuration: configuration, AgentOwnerScope: testAgentScope}
	})
	AfterEach(func() {
		for _, s := range sessions {
			current := read(s)
			current.Finalizers = nil
			Expect(k8sClient.Update(ctx, current)).To(Succeed())
			if current.DeletionTimestamp.IsZero() {
				Expect(k8sClient.Delete(ctx, current)).To(Succeed())
			}
		}
		var current agentsv1alpha1.Agent
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(agent), &current)).To(Succeed())
		current.Finalizers = nil
		Expect(k8sClient.Update(ctx, &current)).To(Succeed())
		if current.DeletionTimestamp.IsZero() {
			Expect(k8sClient.Delete(ctx, &current)).To(Succeed())
		}
	})
	It("waits for a ready same-namespace Agent before making the creation claim", func() {
		s := newSession("missing-agent")
		Expect(sessionStep(s)).To(Succeed())
		Expect(sessionStep(s)).To(Succeed())
		Expect(read(s).Status.CreationAttempted).To(BeFalse())
		Expect(provider.creates).To(BeZero())
		current := read(s)
		current.Spec.AgentRef.Name = agent.Name
		Expect(k8sClient.Update(ctx, current)).To(Succeed())
		Expect(sessionStep(s)).To(Succeed())
		Expect(provider.creates).To(BeZero())
		readyAgent()
		Expect(sessionStep(s)).To(Succeed())
		snapshot := read(s).Status.AgentSnapshot
		Expect(snapshot.UID).To(Equal(string(agent.UID)))
		Expect(snapshot.OpenAIAgentID).To(Equal(testAgentID))
		Expect(snapshot.ConfigurationDigest).NotTo(BeEmpty())
		Expect(provider.remote.AgentID).To(Equal(testAgentID))
		Expect(provider.creates).To(Equal(1))
	})
	It("keeps an existing Session configuration while a later Session copies the changed Agent", func() {
		readyAgent()
		first := newSession(agent.Name)
		Expect(sessionStep(first)).To(Succeed())
		Expect(sessionStep(first)).To(Succeed())
		oldRemote := provider.remote
		oldSnapshot := *read(first).Status.AgentSnapshot
		var updated agentsv1alpha1.Agent
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(agent), &updated)).To(Succeed())
		updated.Spec.Model = updatedTestModel
		Expect(k8sClient.Update(ctx, &updated)).To(Succeed())
		Expect(agentStep()).To(Succeed())
		Expect(agentStep()).To(Succeed())
		Expect(sessionStep(first)).To(Succeed())
		Expect(provider.remote).To(Equal(oldRemote))
		Expect(*read(first).Status.AgentSnapshot).To(Equal(oldSnapshot))
		second := newSession(agent.Name)
		Expect(sessionStep(second)).To(Succeed())
		Expect(sessionStep(second)).To(Succeed())
		Expect(read(second).Status.AgentSnapshot.Model).To(Equal(updatedTestModel))
		Expect(provider.creates).To(Equal(2))
	})
	It("freezes a reference through a lost Session create response and later Agent changes", func() {
		readyAgent()
		s := newSession(agent.Name)
		provider.loseResponse = true
		Expect(sessionStep(s)).To(Succeed())
		Expect(sessionStep(s)).NotTo(Succeed())
		frozen := *read(s).Status.AgentSnapshot
		var updated agentsv1alpha1.Agent
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(agent), &updated)).To(Succeed())
		updated.Spec.Model = updatedTestModel
		Expect(k8sClient.Update(ctx, &updated)).To(Succeed())
		Expect(agentStep()).To(Succeed())
		Expect(agentStep()).To(Succeed())
		Expect(sessionStep(s)).To(Succeed())
		Expect(*read(s).Status.AgentSnapshot).To(Equal(frozen))
		Expect(provider.creates).To(Equal(1))
	})
	It("reports reference edits as RequiresNewSession and retains the original remote Session", func() {
		readyAgent()
		s := newSession(agent.Name)
		Expect(sessionStep(s)).To(Succeed())
		Expect(sessionStep(s)).To(Succeed())
		current := read(s)
		current.Spec.AgentRef.Name = "another-agent"
		Expect(k8sClient.Update(ctx, current)).To(Succeed())
		Expect(sessionStep(s)).To(Succeed())
		condition := meta.FindStatusCondition(read(s).Status.Conditions, "ConfigurationCurrent")
		Expect(condition.Reason).To(Equal("RequiresNewSession"))
		Expect(provider.creates).To(Equal(1))
		Expect(read(s).Status.AgentSnapshot.UID).To(Equal(string(agent.UID)))
		Expect(k8sClient.Delete(ctx, agent)).To(Succeed())
		Expect(agentStep()).To(Succeed())
		var deleting agentsv1alpha1.Agent
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(agent), &deleting)).To(Succeed())
		Expect(deleting.Status.Conditions[0].Reason).To(Equal(testAgentInUseReason))
	})
	It("blocks function tools before claiming a Session", func() {
		function := agentsv1alpha1.AgentTool{Type: _testGatewayFunction, Function: &agentsv1alpha1.AgentFunction{Name: "lookup", Description: "Lookup", Parameters: apiextensionsv1.JSON{Raw: []byte(`{"type":"object"}`)}}}
		agent.Spec.Tools = &[]agentsv1alpha1.AgentTool{function}
		Expect(k8sClient.Update(ctx, agent)).To(Succeed())
		readyAgent()
		s := newSession(agent.Name)
		Expect(sessionStep(s)).To(Succeed())
		Expect(sessionStep(s)).To(Succeed())
		Expect(read(s).Status.CreationAttempted).To(BeFalse())
		Expect(provider.creates).To(BeZero())
		Expect(meta.FindStatusCondition(read(s).Status.Conditions, readyCondition).Reason).To(Equal("GatewayRequired"))
	})
	It("stops execution if the saved Agent changes between observation and Session POST", func() {
		readyAgent()
		s := newSession(agent.Name)
		provider.onCreate = func() {
			agents.mu.Lock()
			defer agents.mu.Unlock()
			agents.remote.Config["instructions"] = "changed after Session resolution"
		}
		Expect(sessionStep(s)).To(Succeed())
		Expect(sessionStep(s)).To(Succeed())
		Expect(read(s).Status.OpenAISessionID).NotTo(BeEmpty())
		Expect(read(s).Status.PodName).To(BeEmpty())
		Expect(meta.FindStatusCondition(read(s).Status.Conditions, readyCondition).Reason).To(Equal("ConfigurationMismatch"))
		Expect(provider.creates).To(Equal(1))
	})
})

// verifyAgentDependencyWatches uses the suite's single registered manager.
func verifyAgentDependencyWatches(namespace string) {
	environment := &agentsv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{GenerateName: "watch-agent-environment-", Namespace: namespace},
		Spec: agentsv1alpha1.EnvironmentSpec{Mode: openaiHostedMode, DeletionPolicy: deletePolicy,
			OpenAIHosted: &agentsv1alpha1.HostedEnvironment{Network: agentsv1alpha1.HostedNetwork{Access: testDisabledNetwork}}}}
	Expect(k8sClient.Create(ctx, environment)).To(Succeed())
	Eventually(func() bool {
		if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(environment), environment); err != nil {
			return false
		}
		return meta.IsStatusConditionTrue(environment.Status.Conditions, readyCondition)
	}, 10*time.Second, 20*time.Millisecond).Should(BeTrue())
	agent := &agentsv1alpha1.Agent{ObjectMeta: metav1.ObjectMeta{GenerateName: "watch-agent-", Namespace: namespace}, Spec: agentsv1alpha1.AgentSpec{Model: openaiapi.SessionModel, DeletionPolicy: deletePolicy}}
	Expect(k8sClient.Create(ctx, agent)).To(Succeed())
	session := &agentsv1alpha1.Session{ObjectMeta: metav1.ObjectMeta{GenerateName: "watch-session-", Namespace: namespace}, Spec: agentsv1alpha1.SessionSpec{
		AgentRef: &agentsv1alpha1.AgentReference{Name: agent.Name}, EnvironmentRef: &agentsv1alpha1.AgentReference{Name: environment.Name},
		SpendControl: &agentsv1alpha1.SessionSpendControl{Mode: spendModeUnlimited}}}
	Expect(k8sClient.Create(ctx, session)).To(Succeed())
	Eventually(func() string {
		var current agentsv1alpha1.Session
		if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(session), &current); err != nil {
			return ""
		}
		return current.Status.OpenAISessionID
	}, 10*time.Second, 20*time.Millisecond).Should(Equal(testSessionID))
	Expect(k8sClient.Delete(ctx, agent)).To(Succeed())
	Eventually(func() string {
		var current agentsv1alpha1.Agent
		if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(agent), &current); err != nil {
			return ""
		}
		c := meta.FindStatusCondition(current.Status.Conditions, readyCondition)
		if c == nil {
			return ""
		}
		return c.Reason
	}, 5*time.Second, 20*time.Millisecond).Should(Equal(testAgentInUseReason))
	Expect(k8sClient.Delete(ctx, session)).To(Succeed())
	Eventually(func() bool {
		var current agentsv1alpha1.Agent
		return client.IgnoreNotFound(k8sClient.Get(ctx, client.ObjectKeyFromObject(agent), &current)) == nil && current.Name == ""
	}, 5*time.Second, 20*time.Millisecond).Should(BeTrue())
	Expect(k8sClient.Delete(ctx, environment)).To(Succeed())
	Eventually(func() bool {
		return apierrors.IsNotFound(k8sClient.Get(ctx, client.ObjectKeyFromObject(environment), &agentsv1alpha1.Environment{}))
	}, 5*time.Second, 20*time.Millisecond).Should(BeTrue())
}

var _ = Describe("Agent competing creation claims", func() {
	It("allows only the winning optimistic status writer to POST", func() {
		agent := &agentsv1alpha1.Agent{ObjectMeta: metav1.ObjectMeta{GenerateName: "claim-agent-", Namespace: testNamespace, Finalizers: []string{agentFinalizer}}, Spec: agentsv1alpha1.AgentSpec{Model: openaiapi.SessionModel, DeletionPolicy: deletePolicy}}
		Expect(k8sClient.Create(ctx, agent)).To(Succeed())
		provider := &agentProviderStub{}
		reader := &barrierReader{Reader: k8sClient, arrived: make(chan struct{}, 2), release: make(chan struct{})}
		reconciler := &AgentReconciler{Client: k8sClient, APIReader: reader, OpenAI: provider, OwnerScope: testAgentScope}
		var wg sync.WaitGroup
		errs := make(chan error, 2)
		for range 2 {
			wg.Go(func() {
				defer GinkgoRecover()
				_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(agent)})
				errs <- err
			})
		}
		var release sync.Once
		unlock := func() { release.Do(func() { close(reader.release) }) }
		defer func() { unlock(); wg.Wait() }()
		Eventually(reader.arrived, 5*time.Second).Should(Receive())
		Eventually(reader.arrived, 5*time.Second).Should(Receive())
		unlock()
		wg.Wait()
		close(errs)
		conflicts := 0
		for err := range errs {
			if err != nil {
				conflicts++
			}
		}
		Expect(conflicts).To(Equal(1))
		Expect(provider.creates).To(Equal(1))
		var current agentsv1alpha1.Agent
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(agent), &current)).To(Succeed())
		current.Finalizers = nil
		Expect(k8sClient.Update(ctx, &current)).To(Succeed())
		Expect(k8sClient.Delete(ctx, &current)).To(Succeed())
	})
})
