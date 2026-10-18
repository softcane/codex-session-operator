package controller

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	"github.com/softcane/codex-session-operator/internal/openaiapi"
	"github.com/softcane/codex-session-operator/internal/sandbox"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	testSessionID      = "sess_test"
	testPreservedValue = "preserved"
)

// A provider simulator commits creation before optionally losing the response.
// Its mutex also supports the competing-claim test.
type sessionProviderStub struct {
	mu               sync.Mutex
	remote           openaiapi.Session
	creates, deletes int
	loseResponse     bool
	hideDiscovery    bool
	deleteFailure    bool
	onCreate         func()
}

func (p *sessionProviderStub) createSession(uid, workspace string) (openaiapi.Session, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.onCreate != nil {
		p.onCreate()
	}
	p.creates++
	p.remote = openaiapi.Session{ID: testSessionID, EnvironmentID: "env_test", RemoteURL: "https://api.openai.com/v1/agents/api", WorkspaceDirectory: workspace, Model: openaiapi.SessionModel, OwnerUID: uid, SelfHosted: true, Status: "idle"}
	if p.loseResponse {
		return openaiapi.Session{}, errors.New("Create response lost after remote commit")
	}
	return p.remote, nil
}

func (p *sessionProviderStub) GetSession(_ context.Context, id string) (openaiapi.Session, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.remote.ID != id || id == "" {
		return openaiapi.Session{}, openaiapi.ErrNotFound
	}
	return p.remote, nil
}
func (p *sessionProviderStub) FindSession(_ context.Context, uid string) (openaiapi.Session, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.hideDiscovery || p.remote.OwnerUID != uid {
		return openaiapi.Session{}, nil
	}
	return p.remote, nil
}
func (p *sessionProviderStub) DeleteSession(_ context.Context, _ string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.deletes++
	if p.deleteFailure {
		return errors.New("Cleanup temporarily unavailable")
	}
	p.remote = openaiapi.Session{}
	return nil
}

// Intercepts only selected Kubernetes writes; all semantics remain real envtest.
type interceptClient struct {
	client.Client
	beforeStatusPatch func(client.Object) error
	beforePatch       func(client.Object) error
}

func (c *interceptClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	if c.beforePatch != nil {
		if err := c.beforePatch(obj); err != nil {
			return err
		}
	}
	return c.Client.Patch(ctx, obj, patch, opts...)
}
func (c *interceptClient) Status() client.SubResourceWriter {
	return interceptStatus{SubResourceWriter: c.Client.Status(), before: c.beforeStatusPatch}
}

type interceptStatus struct {
	client.SubResourceWriter
	before func(client.Object) error
}

type barrierReader struct {
	client.Reader
	arrived chan struct{}
	release chan struct{}
	calls   atomic.Int32
}

func (r *barrierReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if err := r.Reader.Get(ctx, key, obj, opts...); err != nil {
		return err
	}
	if r.calls.Add(1) <= 2 {
		r.arrived <- struct{}{}
		select {
		case <-r.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

type staleClient struct {
	client.Client
	stale *agentsv1alpha1.Session
}

type currentSessionDependencies struct {
	agent        *agentsv1alpha1.Agent
	environment  *agentsv1alpha1.Environment
	agents       *agentProviderStub
	environments *environmentProviderStub
}

func readySelfHostedSessionDependencies(namespace string) *currentSessionDependencies {
	agent := &agentsv1alpha1.Agent{ObjectMeta: metav1.ObjectMeta{GenerateName: "session-agent-", Namespace: namespace},
		Spec: agentsv1alpha1.AgentSpec{Model: openaiapi.SessionModel, DeletionPolicy: deletePolicy}}
	environment := &agentsv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{GenerateName: "session-environment-", Namespace: namespace},
		Spec: agentsv1alpha1.EnvironmentSpec{Mode: testSelfHostedMode, DeletionPolicy: deletePolicy,
			SelfHosted: &agentsv1alpha1.SelfHostedEnvironment{Image: testExecutorImage, Architecture: "amd64",
				WorkspaceDirectory: defaultWorkspace, WorkspaceSize: "1Gi",
				WorkspaceStorage: &agentsv1alpha1.WorkspaceStorage{Type: sandbox.WorkspaceStorageEphemeral}}}}
	Expect(k8sClient.Create(ctx, agent)).To(Succeed())
	Expect(k8sClient.Create(ctx, environment)).To(Succeed())
	for _, object := range []client.Object{environment, agent} {
		DeferCleanup(func() {
			current := object.DeepCopyObject().(client.Object)
			if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(object), current); apierrors.IsNotFound(err) {
				return
			} else {
				Expect(err).NotTo(HaveOccurred())
			}
			current.SetFinalizers(nil)
			Expect(k8sClient.Update(ctx, current)).To(Succeed())
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, current))).To(Succeed())
		})
	}
	agents := &agentProviderStub{}
	environments := &environmentProviderStub{}
	agentController := &AgentReconciler{Client: k8sClient, APIReader: k8sClient, OpenAI: agents, OwnerScope: testAgentScope}
	environmentController := &EnvironmentReconciler{Client: k8sClient, APIReader: k8sClient, OpenAI: environments,
		OwnerScope: testAgentScope, ExecutorImages: []string{testExecutorImage}}
	for _, value := range []struct {
		reconciler reconcile.Reconciler
		object     client.Object
	}{{agentController, agent}, {environmentController, environment}} {
		for range 3 {
			_, err := value.reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(value.object)})
			Expect(err).NotTo(HaveOccurred())
		}
	}
	Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(agent), agent)).To(Succeed())
	Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(environment), environment)).To(Succeed())
	return &currentSessionDependencies{agent: agent, environment: environment, agents: agents, environments: environments}
}

func currentSessionSpec(dependencies *currentSessionDependencies) agentsv1alpha1.SessionSpec {
	return agentsv1alpha1.SessionSpec{
		AgentRef:       &agentsv1alpha1.AgentReference{Name: dependencies.agent.Name},
		EnvironmentRef: &agentsv1alpha1.AgentReference{Name: dependencies.environment.Name},
		SpendControl:   &agentsv1alpha1.SessionSpendControl{Mode: spendModeUnlimited},
	}
}

func readyHostedSessionEnvironment(namespace string) (*agentsv1alpha1.Environment, *environmentProviderStub) {
	environment := &agentsv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{GenerateName: "session-environment-", Namespace: namespace},
		Spec: agentsv1alpha1.EnvironmentSpec{Mode: openaiHostedMode, DeletionPolicy: deletePolicy,
			OpenAIHosted: &agentsv1alpha1.HostedEnvironment{Network: agentsv1alpha1.HostedNetwork{Access: testDisabledNetwork}}}}
	Expect(k8sClient.Create(ctx, environment)).To(Succeed())
	provider := &environmentProviderStub{runtimeStatus: testConnectedRuntime}
	reconciler := &EnvironmentReconciler{Client: k8sClient, APIReader: k8sClient, OpenAI: provider, OwnerScope: testAgentScope}
	for range 3 {
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(environment)})
		Expect(err).NotTo(HaveOccurred())
	}
	Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(environment), environment)).To(Succeed())
	DeferCleanup(func() {
		var current agentsv1alpha1.Environment
		if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(environment), &current); apierrors.IsNotFound(err) {
			return
		} else {
			Expect(err).NotTo(HaveOccurred())
		}
		current.Finalizers = nil
		Expect(k8sClient.Update(ctx, &current)).To(Succeed())
		Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, &current))).To(Succeed())
	})
	return environment, provider
}

func (c *staleClient) Get(_ context.Context, _ client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
	session, ok := obj.(*agentsv1alpha1.Session)
	if !ok {
		return errors.New("stale client requires a Session")
	}
	c.stale.DeepCopyInto(session)
	return nil
}

func (s interceptStatus) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	if s.before != nil {
		if err := s.before(obj); err != nil {
			return err
		}
	}
	return s.SubResourceWriter.Patch(ctx, obj, patch, opts...)
}

var _ = Describe("Remote Session lifecycle", func() {
	var resource *agentsv1alpha1.Session
	var provider *sessionProviderStub
	var reconciler *SessionReconciler
	var request reconcile.Request
	read := func() *agentsv1alpha1.Session {
		s := &agentsv1alpha1.Session{}
		Expect(k8sClient.Get(ctx, request.NamespacedName, s)).To(Succeed())
		return s
	}
	step := func() error { _, err := reconciler.Reconcile(ctx, request); return err }
	provision := func() { Expect(step()).To(Succeed()); Expect(step()).To(Succeed()) }
	BeforeEach(func() {
		fixture := newDependencyScenarioFixture()
		fixture.ready()
		resource = fixture.newSession(fixture.namespace)
		resource.Finalizers = append(resource.Finalizers, otherTestFinalizer)
		Expect(k8sClient.Update(ctx, resource)).To(Succeed())
		request = reconcile.Request{NamespacedName: client.ObjectKeyFromObject(resource)}
		provider = fixture.sessions
		reconciler = fixture.sessionController
	})
	AfterEach(func() {
		// Tests that intentionally block cleanup must release their simulated external
		// obligations before discarding the isolated API-server object.
		s := read()
		s.Finalizers = nil
		Expect(k8sClient.Update(ctx, s)).To(Succeed())
		Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, s))).To(Succeed())
	})
	It("persists finalizer and claim before POST; repeated reconciliation is a status no-op", func() {
		provider.onCreate = func() {
			s := read()
			Expect(s.Finalizers).To(ContainElements(sessionFinalizer, otherTestFinalizer))
			Expect(s.Status.CreationAttempted).To(BeTrue())
			Expect(s.Status.AgentSnapshot).NotTo(BeNil())
			Expect(s.Status.EnvironmentSnapshot).NotTo(BeNil())
		}
		provision()
		first := read()
		Expect(first.Status.OpenAISessionID).To(Equal(provider.remote.ID))
		Expect(first.Status.EnvironmentID).To(Equal("env_test"))
		Expect(first.Status.Phase).To(Equal("Ready"))
		Expect(meta.FindStatusCondition(first.Status.Conditions, "Ready").Status).To(Equal(metav1.ConditionTrue))
		for range 5 {
			Expect(step()).To(Succeed())
		}
		Expect(provider.creates).To(Equal(1))
		Expect(read().ResourceVersion).To(Equal(first.ResourceVersion))
	})
	It("recovers a lost create response after restart without another POST", func() {
		provider.loseResponse = true
		Expect(step()).To(Succeed())
		Expect(step()).NotTo(Succeed())
		s := read()
		Expect(s.Status.OpenAISessionID).To(BeEmpty())
		Expect(meta.FindStatusCondition(s.Status.Conditions, "RemoteSessionCreated").Reason).To(Equal(creationOutcomeUnknownReason))
		restarted := *reconciler
		restarted.Client = k8sClient
		restarted.APIReader = k8sClient
		reconciler = &restarted
		for range 4 {
			Expect(step()).To(Succeed())
		}
		Expect(read().Status.OpenAISessionID).To(Equal(provider.remote.ID))
		Expect(provider.creates).To(Equal(1))
	})
	It("blocks new creation and finalizer removal when an attempted create cannot be discovered", func() {
		provider.loseResponse = true
		provider.hideDiscovery = true
		Expect(step()).To(Succeed())
		Expect(step()).NotTo(Succeed())
		for range 3 {
			Expect(step()).To(Succeed())
		}
		Expect(k8sClient.Delete(ctx, read())).To(Succeed())
		for range 3 {
			Expect(step()).To(Succeed())
		}
		Expect(read().Finalizers).To(ContainElement(sessionFinalizer))
		Expect(read().Status.Phase).To(Equal("Terminating"))
		Expect(provider.creates).To(Equal(1))
		Expect(provider.deletes).To(BeZero())
		provider.hideDiscovery = false
		Expect(step()).To(Succeed())
		Expect(step()).To(Succeed())
		Expect(read().Finalizers).NotTo(ContainElement(sessionFinalizer))
		Expect(provider.deletes).To(Equal(1))
	})
	It("recovers remote success when saving the response to status fails", func() {
		failOnce := true
		reconciler.Client = &interceptClient{Client: k8sClient, beforeStatusPatch: func(obj client.Object) error {
			session, ok := obj.(*agentsv1alpha1.Session)
			if !ok {
				return errors.New("status patch did not receive a Session")
			}
			if session.Status.OpenAISessionID != "" && failOnce {
				failOnce = false
				return errors.New("Injected status persistence failure")
			}
			return nil
		}}
		Expect(step()).To(Succeed())
		Expect(step()).NotTo(Succeed())
		Expect(read().Status.CreationAttempted).To(BeTrue())
		Expect(read().Status.OpenAISessionID).To(BeEmpty())
		Expect(step()).To(Succeed())
		Expect(provider.creates).To(Equal(1))
		Expect(read().Status.OpenAISessionID).To(Equal(provider.remote.ID))
	})
	It("does not create when the optimistic claim conflicts; retry preserves unrelated metadata", func() {
		failOnce := true
		reconciler.Client = &interceptClient{Client: k8sClient, beforeStatusPatch: func(_ client.Object) error {
			if failOnce {
				failOnce = false
				s := read()
				s.Labels = map[string]string{"other": testPreservedValue}
				Expect(k8sClient.Update(ctx, s)).To(Succeed())
			}
			return nil
		}}
		Expect(step()).To(Succeed())
		Expect(apierrors.IsConflict(step())).To(BeTrue())
		Expect(provider.creates).To(BeZero())
		Expect(step()).To(Succeed())
		Expect(provider.creates).To(Equal(1))
		Expect(read().Labels).To(HaveKeyWithValue("other", testPreservedValue))
	})
	It("retains the finalizer on cleanup failure; retry deletes once and preserves another finalizer", func() {
		provision()
		provider.deleteFailure = true
		Expect(k8sClient.Delete(ctx, read())).To(Succeed())
		Expect(step()).NotTo(Succeed())
		Expect(read().Finalizers).To(ContainElement(sessionFinalizer))
		provider.deleteFailure = false
		Expect(step()).To(Succeed())
		Expect(step()).To(Succeed())
		Expect(read().Finalizers).To(ConsistOf(otherTestFinalizer))
		Expect(provider.creates).To(Equal(1))
		Expect(provider.deletes).To(Equal(2))
	})
	It("finishes cleanup after remote deletion succeeds but finalizer persistence fails", func() {
		provision()
		Expect(k8sClient.Delete(ctx, read())).To(Succeed())
		failOnce := true
		reconciler.Client = &interceptClient{Client: k8sClient, beforePatch: func(obj client.Object) error {
			if failOnce && len(obj.GetFinalizers()) == 1 {
				failOnce = false
				return errors.New("Injected finalizer persistence failure")
			}
			return nil
		}}
		Expect(step()).NotTo(Succeed())
		Expect(read().Finalizers).To(ContainElement(sessionFinalizer))
		Expect(step()).To(Succeed())
		Expect(provider.deletes).To(Equal(1))
		Expect(read().Finalizers).To(ConsistOf(otherTestFinalizer))
	})
	It("cleans up deletion before provisioning without calling create", func() {
		Expect(step()).To(Succeed())
		Expect(k8sClient.Delete(ctx, read())).To(Succeed())
		Expect(step()).To(Succeed())
		Expect(provider.creates).To(BeZero())
		Expect(provider.deletes).To(BeZero())
	})
	It("never replaces a missing remote Session", func() {
		provision()
		provider.remote = openaiapi.Session{}
		Expect(step()).NotTo(Succeed())
		Expect(meta.FindStatusCondition(read().Status.Conditions, "RemoteSessionCreated").Reason).To(Equal("RemoteSessionMissing"))
		Expect(provider.creates).To(Equal(1))
	})
	It("does not delete a remote Session owned by a different UID", func() {
		provision()
		provider.remote.OwnerUID = "different-uid"
		Expect(k8sClient.Delete(ctx, read())).To(Succeed())
		Expect(step()).To(Succeed())
		Expect(read().Finalizers).To(ContainElement(sessionFinalizer))
		Expect(provider.deletes).To(BeZero())
	})
	It("allows only one of two concurrent reconcilers to send create", func() {
		Expect(step()).To(Succeed())
		barrier := &barrierReader{Reader: k8sClient, arrived: make(chan struct{}, 2), release: make(chan struct{})}
		reconciler.APIReader = barrier
		bounded, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		results := make(chan error, 2)
		for range 2 {
			go func() {
				_, err := reconciler.Reconcile(bounded, request)
				results <- err
			}()
		}
		for range 2 {
			select {
			case <-barrier.arrived:
			case <-bounded.Done():
				Fail("Timed out waiting for concurrent reads")
			}
		}
		close(barrier.release)
		successes, conflicts := 0, 0
		for range 2 {
			select {
			case err := <-results:
				if err == nil {
					successes++
				} else if apierrors.IsConflict(err) {
					conflicts++
				} else {
					Expect(err).NotTo(HaveOccurred())
				}
			case <-bounded.Done():
				Fail("Concurrent reconciliation did not finish")
			}
		}
		Expect(successes).To(Equal(1))
		Expect(conflicts).To(Equal(1))
		Expect(provider.creates).To(Equal(1))
	})
	It("uses the authoritative reader despite stale cached status", func() {
		stale := read()
		provision()
		reconciler.Client = &staleClient{Client: k8sClient, stale: stale}
		for range 3 {
			Expect(step()).To(Succeed())
		}
		Expect(provider.creates).To(Equal(1))
	})
	It("does not adopt an older Session with the same name and a different UID", func() {
		oldUID := resource.UID
		provider.remote = openaiapi.Session{ID: "old-session", OwnerUID: string(oldUID)}
		old := read()
		old.Finalizers = nil
		Expect(k8sClient.Update(ctx, old)).To(Succeed())
		Expect(k8sClient.Delete(ctx, old)).To(Succeed())
		replacement := &agentsv1alpha1.Session{ObjectMeta: metav1.ObjectMeta{
			Name: request.Name, Namespace: request.Namespace, Finalizers: []string{otherTestFinalizer}},
			Spec: *resource.Spec.DeepCopy()}
		Expect(k8sClient.Create(ctx, replacement)).To(Succeed())
		Expect(replacement.UID).NotTo(Equal(oldUID))
		s := read()
		s.Status.CreationAttempted = true
		Expect(k8sClient.Status().Update(ctx, s)).To(Succeed())
		Expect(step()).To(Succeed())
		Expect(step()).To(Succeed())
		Expect(read().Status.OpenAISessionID).To(BeEmpty())
		Expect(provider.creates).To(BeZero())
		Expect(provider.deletes).To(BeZero())
	})
	It("preserves unrelated Conditions", func() {
		s := read()
		meta.SetStatusCondition(&s.Status.Conditions, metav1.Condition{
			Type: "OtherController", Status: metav1.ConditionTrue, Reason: "Unrelated", Message: "Preserve this condition"})
		Expect(k8sClient.Status().Update(ctx, s)).To(Succeed())
		provision()
		Expect(meta.FindStatusCondition(read().Status.Conditions, "OtherController").Reason).To(Equal("Unrelated"))
	})
})
