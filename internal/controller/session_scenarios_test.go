package controller

import (
	"context"
	"errors"
	"maps"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	"github.com/softcane/codex-session-operator/internal/openaiapi"
	"github.com/softcane/codex-session-operator/internal/sandbox"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	scenarioForeignVaultID       = "vault_foreign"
	scenarioForeignEnvironmentID = "env_foreign"
	scenarioMutableMetadataKey   = "mutable-edit"
	scenarioForeignPath          = "/foreign"
	scenarioForeignAgentID       = "agent_foreign"
)

// Faults cross the same provider boundary as real HTTP failures, while every
// Kubernetes claim, owner reference, status write, and finalizer uses envtest.
type scenarioSessionProvider struct {
	*sessionProviderStub

	configuration *configuredSessionStub
	getErr        error
	findErr       error
	returnedID    string
	deleteMissing bool
	deletePending bool
}

var _ SessionProvider = (*scenarioSessionProvider)(nil)
var _ SessionConfigurationProvider = (*scenarioSessionProvider)(nil)

func (p *scenarioSessionProvider) GetSession(ctx context.Context, id string) (openaiapi.Session, error) {
	if p.getErr != nil {
		return openaiapi.Session{}, p.getErr
	}
	remote, err := p.sessionProviderStub.GetSession(ctx, id)
	if err == nil && p.returnedID != "" {
		remote.ID = p.returnedID
	}
	return remote, err
}

func (p *scenarioSessionProvider) FindSession(ctx context.Context, uid string) (openaiapi.Session, error) {
	if p.findErr != nil {
		return openaiapi.Session{}, p.findErr
	}
	return p.sessionProviderStub.FindSession(ctx, uid)
}

func (p *scenarioSessionProvider) DeleteSession(ctx context.Context, id string) error {
	if p.deletePending {
		p.mu.Lock()
		defer p.mu.Unlock()
		p.deletes++
		return nil
	}
	err := p.sessionProviderStub.DeleteSession(ctx, id)
	if err == nil && p.deleteMissing {
		return openaiapi.ErrNotFound
	}
	return err
}

func (p *scenarioSessionProvider) CreateConfiguredSession(ctx context.Context, uid, scope, agentID string, environment, agent map[string]any, vaultIDs []string, metadata *map[string]string, spend *agentsv1alpha1.SessionSpendControl) (openaiapi.Session, error) {
	_, err := p.configuration.CreateConfiguredSession(ctx, uid, scope, agentID, environment, agent, vaultIDs, metadata, spend)
	p.mu.Lock()
	defer p.mu.Unlock()
	p.remote.AgentConfiguration = openaiapi.EffectiveSessionAgent(p.agents.remote.Config, agent)
	p.remote.AgentConfigurationDigest = openaiapi.AgentConfigurationDigest(p.remote.AgentConfiguration)
	p.remote.EnvironmentType = "self_hosted"
	return p.remote, err
}

func (p *scenarioSessionProvider) UpdateSession(ctx context.Context, id string, fields map[string]any) (openaiapi.Session, error) {
	return p.configuration.UpdateSession(ctx, id, fields)
}

type scenarioConnections struct {
	connectionStub

	forgotten []string
}

func (c *scenarioConnections) Forget(id string) { c.forgotten = append(c.forgotten, id) }

var _ = Describe("Session lifecycle scenarios with the shipped CRD", func() {
	var session *agentsv1alpha1.Session
	var agent *agentsv1alpha1.Agent
	var environment *agentsv1alpha1.Environment
	var secret *corev1.Secret
	var provider *scenarioSessionProvider
	var connections *scenarioConnections
	var r *SessionReconciler

	read := func() *agentsv1alpha1.Session {
		var current agentsv1alpha1.Session
		Expect(shippedK8sClient.Get(ctx, client.ObjectKeyFromObject(session), &current)).To(Succeed())
		return &current
	}
	step := func() error {
		_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(session)})
		return err
	}
	readPod := func() *corev1.Pod {
		var pod corev1.Pod
		Expect(shippedK8sClient.Get(ctx, client.ObjectKey{Namespace: session.Namespace, Name: sandbox.PodName(session)}, &pod)).To(Succeed())
		return &pod
	}
	expectComputeStopped := func() {
		var pod corev1.Pod
		err := shippedK8sClient.Get(ctx, client.ObjectKey{Namespace: session.Namespace, Name: sandbox.PodName(session)}, &pod)
		if !apierrors.IsNotFound(err) {
			Expect(err).NotTo(HaveOccurred())
			Expect(pod.DeletionTimestamp.IsZero()).To(BeFalse(), "unsafe executor must have a deletion request")
		}
	}
	provision := func() {
		for range 5 {
			Expect(step()).To(Succeed())
		}
		pod := readPod()
		pod.Status.Phase = corev1.PodRunning
		pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
		Expect(shippedK8sClient.Status().Update(ctx, pod)).To(Succeed())
		connections.state.Connected = true
		connections.state.Sequence++
		Expect(step()).To(Succeed())
		Expect(meta.IsStatusConditionTrue(read().Status.Conditions, readyCondition)).To(BeTrue())
	}
	removeObject := func(obj client.Object) {
		current, ok := obj.DeepCopyObject().(client.Object)
		Expect(ok).To(BeTrue())
		err := shippedK8sClient.Get(ctx, client.ObjectKeyFromObject(obj), current)
		if apierrors.IsNotFound(err) {
			return
		}
		Expect(err).NotTo(HaveOccurred())
		current.SetFinalizers(nil)
		Expect(shippedK8sClient.Update(ctx, current)).To(Succeed())
		Expect(client.IgnoreNotFound(shippedK8sClient.Delete(ctx, current, client.GracePeriodSeconds(0)))).To(Succeed())
	}
	removePod := func() {
		removeObject(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: session.Namespace, Name: sandbox.PodName(session)}})
	}
	BeforeEach(func() {
		namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "session-scenario-"}}
		Expect(shippedK8sClient.Create(ctx, namespace)).To(Succeed())
		agent = &agentsv1alpha1.Agent{ObjectMeta: metav1.ObjectMeta{Name: "agent", Namespace: namespace.Name},
			Spec: agentsv1alpha1.AgentSpec{Model: openaiapi.SessionModel, DeletionPolicy: deletePolicy}}
		Expect(shippedK8sClient.Create(ctx, agent)).To(Succeed())
		agents := &agentProviderStub{}
		agentController := &AgentReconciler{Client: shippedK8sClient, APIReader: shippedK8sClient, OpenAI: agents, OwnerScope: testAgentScope}
		for range 3 {
			_, err := agentController.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(agent)})
			Expect(err).NotTo(HaveOccurred())
		}
		environment = &agentsv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "environment", Namespace: namespace.Name},
			Spec: agentsv1alpha1.EnvironmentSpec{Mode: testSelfHostedMode, DeletionPolicy: deletePolicy, SelfHosted: &agentsv1alpha1.SelfHostedEnvironment{
				Image: testExecutorImage, Architecture: workspaceTestArch, WorkspaceStorage: &agentsv1alpha1.WorkspaceStorage{Type: sandbox.WorkspaceStorageEphemeral}}}}
		Expect(shippedK8sClient.Create(ctx, environment)).To(Succeed())
		environmentProvider := &environmentProviderStub{}
		environmentController := &EnvironmentReconciler{Client: shippedK8sClient, APIReader: shippedK8sClient, OpenAI: environmentProvider,
			OwnerScope: testAgentScope, ExecutorImages: []string{testExecutorImage}}
		for range 3 {
			_, err := environmentController.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(environment)})
			Expect(err).NotTo(HaveOccurred())
		}
		secret = &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: sandbox.ExecutorSecretName, Namespace: namespace.Name}, Type: corev1.SecretTypeOpaque,
			Data: map[string][]byte{sandbox.ExecutorSecretKey: []byte("scenario-only-placeholder")}}
		Expect(shippedK8sClient.Create(ctx, secret)).To(Succeed())
		effort := "low"
		session = &agentsv1alpha1.Session{ObjectMeta: metav1.ObjectMeta{Name: "session", Namespace: namespace.Name, Finalizers: []string{otherTestFinalizer}},
			Spec: agentsv1alpha1.SessionSpec{AgentRef: &agentsv1alpha1.AgentReference{Name: agent.Name}, EnvironmentRef: &agentsv1alpha1.AgentReference{Name: environment.Name},
				Agent:        &agentsv1alpha1.SessionAgentOverrides{Reasoning: &agentsv1alpha1.AgentReasoning{Effort: &effort}},
				SpendControl: &agentsv1alpha1.SessionSpendControl{Mode: spendModeUnlimited}}}
		Expect(shippedK8sClient.Create(ctx, session)).To(Succeed())
		lifecycle := &sessionProviderStub{agents: agents}
		provider = &scenarioSessionProvider{sessionProviderStub: lifecycle, configuration: &configuredSessionStub{lifecycle: lifecycle}}
		connections = &scenarioConnections{connectionStub: connectionStub{state: openaiapi.Connection{Subscribed: true, Epoch: "scenario-stream"}}}
		r = &SessionReconciler{Client: shippedK8sClient, APIReader: shippedK8sClient, Scheme: shippedK8sClient.Scheme(), OpenAI: provider,
			Agents: agents, Environments: environmentProvider, Configuration: provider, AgentOwnerScope: testAgentScope,
			Connections: connections, ExecutorImage: testExecutorImage}
	})
	AfterEach(func() {
		var pods corev1.PodList
		Expect(shippedK8sClient.List(ctx, &pods, client.InNamespace(session.Namespace))).To(Succeed())
		for i := range pods.Items {
			removeObject(&pods.Items[i])
		}
		var claims corev1.PersistentVolumeClaimList
		Expect(shippedK8sClient.List(ctx, &claims, client.InNamespace(session.Namespace))).To(Succeed())
		for i := range claims.Items {
			removeObject(&claims.Items[i])
		}
		removeObject(session)
		removeObject(agent)
		removeObject(environment)
		removeObject(secret)
	})

	It("creates one remote Session and owned executor, then repeated observation is a status no-op", func() {
		provision()
		first := read()
		pod := readPod()
		Expect(first.Status.AgentSnapshot.UID).To(Equal(string(agent.UID)))
		Expect(first.Status.EnvironmentSnapshot.UID).To(Equal(string(environment.UID)))
		Expect(first.Status.OwnerScope).To(Equal(testAgentScope))
		Expect(metav1.IsControlledBy(pod, first)).To(BeTrue())
		for range 4 {
			Expect(step()).To(Succeed())
		}
		Expect(read().ResourceVersion).To(Equal(first.ResourceVersion))
		Expect(readPod().UID).To(Equal(pod.UID))
		Expect(provider.creates).To(Equal(1))
		Expect(provider.configuration.updates).To(BeZero())
	})

	It("stops compute when the console deleted the remote Session without recreating or deleting the CR", func() {
		provision()
		id := read().Status.OpenAISessionID
		provider.remote = openaiapi.Session{}
		for range 3 {
			Expect(step()).To(MatchError(openaiapi.ErrNotFound))
		}
		current := read()
		Expect(current.DeletionTimestamp.IsZero()).To(BeTrue())
		Expect(current.Status.OpenAISessionID).To(Equal(id))
		Expect(current.Finalizers).To(ContainElement(sessionFinalizer))
		Expect(meta.FindStatusCondition(current.Status.Conditions, readyCondition).Reason).To(Equal("RemoteSessionMissing"))
		Expect(meta.IsStatusConditionFalse(current.Status.Conditions, readyCondition)).To(BeTrue())
		Expect(meta.IsStatusConditionTrue(current.Status.Conditions, connectedCondition)).To(BeFalse())
		expectComputeStopped()
		Expect(connections.forgotten).To(ContainElement(id))
		Expect(provider.creates).To(Equal(1))
		Expect(provider.deletes).To(BeZero())
	})

	It("keeps compute during a temporary API outage, revokes readiness and recovers the same Pod", func() {
		provision()
		podUID := readPod().UID
		provider.getErr = errors.New("temporary service outage")
		Expect(step()).To(MatchError(provider.getErr))
		Expect(meta.FindStatusCondition(read().Status.Conditions, readyCondition).Reason).To(Equal("ObservationFailed"))
		Expect(meta.IsStatusConditionFalse(read().Status.Conditions, readyCondition)).To(BeTrue())
		Expect(readPod().DeletionTimestamp.IsZero()).To(BeTrue())
		Expect(connections.forgotten).To(BeEmpty())
		provider.getErr = nil
		Expect(step()).To(Succeed())
		Expect(meta.IsStatusConditionTrue(read().Status.Conditions, readyCondition)).To(BeTrue())
		Expect(readPod().UID).To(Equal(podUID))
		Expect(provider.creates).To(Equal(1))
	})

	DescribeTable("stops compute and blocks foreign remote mutation when ownership drifts",
		func(change func(*scenarioSessionProvider)) {
			provision()
			change(provider)
			current := read()
			effort := testSessionUpdatedEffort
			current.Spec.Agent.Reasoning.Effort = &effort
			Expect(shippedK8sClient.Update(ctx, current)).To(Succeed())
			Expect(step()).To(Succeed())
			Expect(meta.FindStatusCondition(read().Status.Conditions, readyCondition).Reason).To(Equal("OwnershipMismatch"))
			expectComputeStopped()
			Expect(connections.forgotten).To(ContainElement(testSessionID))
			Expect(provider.configuration.updates).To(BeZero())
			Expect(shippedK8sClient.Delete(ctx, read())).To(Succeed())
			Expect(step()).To(Succeed())
			Expect(read().Finalizers).To(ContainElement(sessionFinalizer))
			Expect(provider.deletes).To(BeZero())
			Expect(provider.creates).To(Equal(1))
		},
		Entry("remote Kubernetes UID changed", func(p *scenarioSessionProvider) { p.remote.OwnerUID = "foreign-uid" }),
		Entry("remote installation/project scope changed", func(p *scenarioSessionProvider) { p.remote.OwnerScope = "foreign/project" }),
		Entry("GET returned a different remote Session ID", func(p *scenarioSessionProvider) { p.returnedID = "sess_foreign" }),
	)

	DescribeTable("stops an already running executor after a definitive remote configuration failure",
		func(change func(*openaiapi.Session), reason string) {
			provision()
			change(&provider.remote)
			Expect(step()).To(Succeed())
			Expect(meta.FindStatusCondition(read().Status.Conditions, readyCondition).Reason).To(Equal(reason))
			expectComputeStopped()
			Expect(provider.creates).To(Equal(1))
			Expect(provider.deletes).To(BeZero())
		},
		Entry("connection URL becomes unsafe", func(s *openaiapi.Session) { s.RemoteURL = "http://unsafe.example.com" }, "InvalidRemoteSession"),
		Entry("environment ID disappears", func(s *openaiapi.Session) { s.EnvironmentID = "" }, "InvalidRemoteSession"),
		Entry("workspace differs from frozen profile", func(s *openaiapi.Session) { s.WorkspaceDirectory = scenarioForeignPath }, "ConfigurationMismatch"),
		Entry("Agent identity changes", func(s *openaiapi.Session) { s.AgentID = scenarioForeignAgentID }, "ConfigurationMismatch"),
		Entry("Agent immutable configuration changes", func(s *openaiapi.Session) {
			s.AgentConfiguration = maps.Clone(s.AgentConfiguration)
			s.AgentConfiguration["instructions"] = "unexpected instructions"
		}, "ConfigurationMismatch"),
		Entry("Vault attachments change", func(s *openaiapi.Session) { s.VaultIDs = []string{scenarioForeignVaultID} }, "ConfigurationMismatch"),
		Entry("capability paths differ from frozen profile", func(s *openaiapi.Session) { s.CapabilityDirectories = []string{scenarioForeignPath} }, "ConfigurationMismatch"),
		Entry("environment type changes to hosted", func(s *openaiapi.Session) { s.SelfHosted = false }, "InvalidRemoteSession"),
		Entry("provider reports a failed Session", func(s *openaiapi.Session) { s.Status = remoteFailedStatus }, "RemoteSessionFailed"),
	)

	DescribeTable("rejects definitive remote drift before pending mutable settings can bypass validation",
		func(change func(*openaiapi.Session), reason string) {
			var updated agentsv1alpha1.Environment
			Expect(shippedK8sClient.Get(ctx, client.ObjectKeyFromObject(environment), &updated)).To(Succeed())
			updated.Spec.SelfHosted.WorkspaceStorage = &agentsv1alpha1.WorkspaceStorage{
				Type: sandbox.WorkspaceStoragePersistent, StorageClassName: workspaceTestClass, ClaimDeletionPolicy: deletePolicy}
			Expect(shippedK8sClient.Update(ctx, &updated)).To(Succeed())
			environmentController := &EnvironmentReconciler{Client: shippedK8sClient, APIReader: shippedK8sClient, OpenAI: &environmentProviderStub{},
				OwnerScope: testAgentScope, ExecutorImages: []string{testExecutorImage}, WorkspaceStorageClasses: []string{workspaceTestClass}}
			for range 2 {
				_, err := environmentController.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(environment)})
				Expect(err).NotTo(HaveOccurred())
			}
			r.WorkspaceStorageClasses = []string{workspaceTestClass}
			provision()
			original := read()
			claimKey := client.ObjectKey{Namespace: session.Namespace, Name: sandbox.WorkspaceClaimNameForUID(string(session.UID))}
			var claim corev1.PersistentVolumeClaim
			Expect(shippedK8sClient.Get(ctx, claimKey, &claim)).To(Succeed())
			claimUID := claim.UID
			current := read()
			effort, cents := testSessionUpdatedEffort, int64(300)
			current.Spec.Agent.Reasoning.Effort = &effort
			metadata := map[string]string{scenarioMutableMetadataKey: "pending"}
			current.Spec.Metadata = &metadata
			current.Spec.SpendControl = &agentsv1alpha1.SessionSpendControl{Mode: spendModeLimited, LimitCents: &cents}
			Expect(shippedK8sClient.Update(ctx, current)).To(Succeed())
			change(&provider.remote)
			Expect(step()).To(Succeed())
			failed := read()
			Expect(meta.FindStatusCondition(failed.Status.Conditions, readyCondition).Reason).To(Equal(reason))
			Expect(failed.Status.Phase).To(Equal("Failed"))
			expectComputeStopped()
			Expect(connections.forgotten).To(ContainElement(original.Status.OpenAISessionID))
			Expect(provider.configuration.updates).To(BeZero())
			Expect(provider.creates).To(Equal(1))
			Expect(provider.deletes).To(BeZero())
			Expect(failed.Status.OpenAISessionID).To(Equal(original.Status.OpenAISessionID))
			Expect(failed.Status.EnvironmentID).To(Equal(original.Status.EnvironmentID))
			Expect(failed.Status.RemoteURL).To(Equal(original.Status.RemoteURL))
			Expect(failed.Status.AgentSnapshot).To(Equal(original.Status.AgentSnapshot))
			Expect(failed.Status.EnvironmentSnapshot).To(Equal(original.Status.EnvironmentSnapshot))
			Expect(failed.Status.AppliedMutableDigest).To(Equal(original.Status.AppliedMutableDigest))
			Expect(failed.Status.SpendControlAttemptedDigest).To(BeEmpty())
			Expect(shippedK8sClient.Get(ctx, claimKey, &claim)).To(Succeed())
			Expect(claim.UID).To(Equal(claimUID))
			Expect(claim.DeletionTimestamp.IsZero()).To(BeTrue())
		},
		Entry("unsafe connection URL", func(s *openaiapi.Session) { s.RemoteURL = "http://unsafe.example.com" }, "InvalidRemoteSession"),
		Entry("missing environment ID", func(s *openaiapi.Session) { s.EnvironmentID = "" }, "InvalidRemoteSession"),
		Entry("changed environment ID", func(s *openaiapi.Session) { s.EnvironmentID = scenarioForeignEnvironmentID }, "ConfigurationMismatch"),
		Entry("workspace drift", func(s *openaiapi.Session) { s.WorkspaceDirectory = scenarioForeignPath }, "ConfigurationMismatch"),
		Entry("Agent ID drift", func(s *openaiapi.Session) { s.AgentID = scenarioForeignAgentID }, "ConfigurationMismatch"),
		Entry("Agent immutable configuration drift", func(s *openaiapi.Session) {
			s.AgentConfiguration = maps.Clone(s.AgentConfiguration)
			s.AgentConfiguration["instructions"] = "unexpected instructions"
		}, "ConfigurationMismatch"),
		Entry("Vault attachment drift", func(s *openaiapi.Session) { s.VaultIDs = []string{scenarioForeignVaultID} }, "ConfigurationMismatch"),
		Entry("capability path drift", func(s *openaiapi.Session) { s.CapabilityDirectories = []string{scenarioForeignPath} }, "ConfigurationMismatch"),
		Entry("changed environment type", func(s *openaiapi.Session) { s.SelfHosted = false }, "InvalidRemoteSession"),
		Entry("remote failed status", func(s *openaiapi.Session) { s.Status = remoteFailedStatus }, "RemoteSessionFailed"),
	)

	It("applies legitimate mutable effort, metadata, and spend updates without stopping a valid executor", func() {
		provision()
		original := read()
		podUID := readPod().UID
		effort, cents := testSessionUpdatedEffort, int64(300)
		current := read()
		current.Spec.Agent.Reasoning.Effort = &effort
		metadata := map[string]string{scenarioMutableMetadataKey: "allowed"}
		current.Spec.Metadata = &metadata
		current.Spec.SpendControl = &agentsv1alpha1.SessionSpendControl{Mode: spendModeLimited, LimitCents: &cents}
		Expect(shippedK8sClient.Update(ctx, current)).To(Succeed())
		Expect(step()).To(Succeed())
		Expect(provider.configuration.updates).To(Equal(1))
		Expect(step()).To(Succeed())
		Expect(meta.IsStatusConditionTrue(read().Status.Conditions, readyCondition)).To(BeTrue())
		Expect(read().Status.OpenAISessionID).To(Equal(original.Status.OpenAISessionID))
		Expect(readPod().UID).To(Equal(podUID))
		Expect(readPod().DeletionTimestamp.IsZero()).To(BeTrue())
		Expect(connections.forgotten).To(BeEmpty())
		Expect(provider.remote.Metadata).To(HaveKeyWithValue(scenarioMutableMetadataKey, "allowed"))
		Expect(provider.remote.SpendLimitCents).To(Equal(&cents))
		Expect(provider.remote.AgentConfiguration[testReasoningField]).To(HaveKeyWithValue("effort", effort))
	})

	It("stops the executor on immutable drift even when its failure status cannot be saved", func() {
		provision()
		current := read()
		effort := testSessionUpdatedEffort
		current.Spec.Agent.Reasoning.Effort = &effort
		Expect(shippedK8sClient.Update(ctx, current)).To(Succeed())
		provider.remote.AgentID = scenarioForeignAgentID
		r.Client = &interceptClient{Client: shippedK8sClient, beforeStatusPatch: func(client.Object) error {
			return errors.New("status storage unavailable")
		}}
		Expect(step()).To(HaveOccurred())
		expectComputeStopped()
		Expect(connections.forgotten).To(ContainElement(testSessionID))
		Expect(provider.configuration.updates).To(BeZero())
	})

	It("rejects an environment identity change and preserves the original executor attachment for audit", func() {
		provision()
		originalEnvironment := read().Status.EnvironmentID
		provider.remote.EnvironmentID = scenarioForeignEnvironmentID
		Expect(step()).To(Succeed())
		Expect(meta.FindStatusCondition(read().Status.Conditions, readyCondition).Reason).To(Equal("ConfigurationMismatch"))
		Expect(read().Status.EnvironmentID).To(Equal(originalEnvironment))
		expectComputeStopped()
		Expect(provider.creates).To(Equal(1))
	})

	It("finishes explicit CR deletion after console deletion, stopping the executor and preserving unrelated finalizers", func() {
		provision()
		provider.remote = openaiapi.Session{}
		Expect(shippedK8sClient.Delete(ctx, read())).To(Succeed())
		Expect(step()).To(Succeed())
		expectComputeStopped()
		// envtest has no kubelet to complete graceful termination of a running Pod.
		var pod corev1.Pod
		if err := shippedK8sClient.Get(ctx, client.ObjectKey{Namespace: session.Namespace, Name: sandbox.PodName(session)}, &pod); err == nil {
			Expect(shippedK8sClient.Delete(ctx, &pod, client.GracePeriodSeconds(0))).To(Succeed())
		} else {
			Expect(apierrors.IsNotFound(err)).To(BeTrue())
		}
		Expect(step()).To(Succeed())
		Expect(read().Finalizers).To(ConsistOf(otherTestFinalizer))
		Expect(provider.deletes).To(BeZero())
		Expect(provider.creates).To(Equal(1))
	})

	It("holds the cleanup finalizer through an API outage while stopping local compute", func() {
		provision()
		provider.getErr = errors.New("cleanup observation unavailable")
		Expect(shippedK8sClient.Delete(ctx, read())).To(Succeed())
		Expect(step()).To(MatchError(provider.getErr))
		expectComputeStopped()
		Expect(read().Finalizers).To(ContainElement(sessionFinalizer))
		Expect(provider.deletes).To(BeZero())
		provider.getErr = nil
		removePod()
		Expect(step()).To(Succeed())
		Expect(read().Finalizers).To(ConsistOf(otherTestFinalizer))
		Expect(provider.deletes).To(Equal(1))
	})

	It("requires observed remote absence after DELETE is accepted", func() {
		provision()
		provider.deletePending = true
		Expect(shippedK8sClient.Delete(ctx, read())).To(Succeed())
		Expect(step()).To(Succeed())
		Expect(meta.FindStatusCondition(read().Status.Conditions, readyCondition).Reason).To(Equal("CleanupPending"))
		Expect(read().Finalizers).To(ContainElement(sessionFinalizer))
		provider.remote = openaiapi.Session{}
		removePod()
		Expect(step()).To(Succeed())
		Expect(read().Finalizers).To(ConsistOf(otherTestFinalizer))
		Expect(provider.deletes).To(Equal(1))
	})

	It("handles a console deletion racing between cleanup GET and DELETE", func() {
		provision()
		provider.deleteMissing = true
		Expect(shippedK8sClient.Delete(ctx, read())).To(Succeed())
		Expect(step()).To(Succeed())
		expectComputeStopped()
		removePod()
		Expect(step()).To(Succeed())
		Expect(read().Finalizers).To(ConsistOf(otherTestFinalizer))
		Expect(provider.deletes).To(Equal(1))
	})

	It("recovers committed creation after a lost response and manager restart using the frozen references", func() {
		provider.loseResponse = true
		Expect(step()).To(Succeed())
		Expect(step()).To(HaveOccurred())
		claimed := read()
		Expect(claimed.Status.CreationAttempted).To(BeTrue())
		Expect(claimed.Status.OpenAISessionID).To(BeEmpty())
		Expect(claimed.Status.AgentSnapshot).NotTo(BeNil())
		Expect(claimed.Status.EnvironmentSnapshot).NotTo(BeNil())
		restarted := *r
		r = &restarted
		for range 3 {
			Expect(step()).To(Succeed())
		}
		Expect(read().Status.OpenAISessionID).To(Equal(testSessionID))
		Expect(read().Status.AgentSnapshot).To(Equal(claimed.Status.AgentSnapshot))
		Expect(read().Status.EnvironmentSnapshot).To(Equal(claimed.Status.EnvironmentSnapshot))
		Expect(provider.creates).To(Equal(1))
	})

	It("does not replay creation or release cleanup while lost-create discovery is unavailable", func() {
		provider.loseResponse = true
		provider.findErr = errors.New("discovery unavailable")
		Expect(step()).To(Succeed())
		Expect(step()).To(HaveOccurred())
		for range 2 {
			Expect(step()).To(MatchError(provider.findErr))
		}
		Expect(shippedK8sClient.Delete(ctx, read())).To(Succeed())
		Expect(step()).To(MatchError(provider.findErr))
		Expect(read().Finalizers).To(ContainElement(sessionFinalizer))
		Expect(provider.creates).To(Equal(1))
		Expect(provider.deletes).To(BeZero())
		provider.findErr = nil
		Expect(step()).To(Succeed())
		Expect(step()).To(Succeed())
		Expect(read().Finalizers).To(ConsistOf(otherTestFinalizer))
		Expect(provider.deletes).To(Equal(1))
	})

	DescribeTable("preserves persistent workspace after external deletion and applies policy only on explicit CR cleanup",
		func(policy string) {
			var updated agentsv1alpha1.Environment
			Expect(shippedK8sClient.Get(ctx, client.ObjectKeyFromObject(environment), &updated)).To(Succeed())
			updated.Spec.SelfHosted.WorkspaceSize = workspaceTestSize
			updated.Spec.SelfHosted.WorkspaceStorage = &agentsv1alpha1.WorkspaceStorage{
				Type: sandbox.WorkspaceStoragePersistent, StorageClassName: workspaceTestClass, ClaimDeletionPolicy: policy}
			Expect(shippedK8sClient.Update(ctx, &updated)).To(Succeed())
			environmentController := &EnvironmentReconciler{Client: shippedK8sClient, APIReader: shippedK8sClient, OpenAI: &environmentProviderStub{},
				OwnerScope: testAgentScope, ExecutorImages: []string{testExecutorImage}, WorkspaceStorageClasses: []string{workspaceTestClass}}
			for range 2 {
				_, err := environmentController.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(environment)})
				Expect(err).NotTo(HaveOccurred())
			}
			r.WorkspaceStorageClasses = []string{workspaceTestClass}
			provision()
			claimKey := client.ObjectKey{Namespace: session.Namespace, Name: sandbox.WorkspaceClaimNameForUID(string(session.UID))}
			var claim corev1.PersistentVolumeClaim
			Expect(shippedK8sClient.Get(ctx, claimKey, &claim)).To(Succeed())
			claimUID := claim.UID
			Expect(read().Status.WorkspaceClaimUID).To(Equal(string(claimUID)))
			provider.remote = openaiapi.Session{}
			Expect(step()).To(MatchError(openaiapi.ErrNotFound))
			expectComputeStopped()
			Expect(shippedK8sClient.Get(ctx, claimKey, &claim)).To(Succeed())
			Expect(claim.UID).To(Equal(claimUID))
			Expect(claim.DeletionTimestamp.IsZero()).To(BeTrue())
			Expect(claim.Annotations[sandbox.WorkspaceRetainedAnnotation]).To(BeEmpty())
			Expect(shippedK8sClient.Delete(ctx, read())).To(Succeed())
			removePod()
			Expect(step()).To(Succeed())
			Expect(read().Finalizers).To(ContainElement(sessionFinalizer))
			Expect(shippedK8sClient.Get(ctx, claimKey, &claim)).To(Succeed())
			if policy == deletePolicy {
				Expect(claim.DeletionTimestamp.IsZero()).To(BeFalse())
				// envtest does not run PVC protection to release its finalizer.
				removeObject(&claim)
			} else {
				Expect(claim.DeletionTimestamp.IsZero()).To(BeTrue())
				Expect(claim.OwnerReferences).To(BeEmpty())
				Expect(claim.Annotations[sandbox.WorkspaceRetainedAnnotation]).To(Equal(string(session.UID)))
			}
			Expect(step()).To(Succeed())
			Expect(read().Finalizers).To(ConsistOf(otherTestFinalizer))
			if policy == retainPolicy {
				Expect(shippedK8sClient.Get(ctx, claimKey, &claim)).To(Succeed())
				Expect(claim.UID).To(Equal(claimUID))
			} else {
				Expect(apierrors.IsNotFound(shippedK8sClient.Get(ctx, claimKey, &claim))).To(BeTrue())
			}
			Expect(provider.creates).To(Equal(1))
			Expect(provider.deletes).To(BeZero())
		},
		Entry("Delete workspace policy", deletePolicy),
		Entry("Retain workspace policy", retainPolicy),
	)

	It("does not send create when its durable claim conflicts and preserves unrelated Kubernetes metadata on retry", func() {
		Expect(step()).To(Succeed())
		failOnce := true
		r.Client = &interceptClient{Client: shippedK8sClient, beforeStatusPatch: func(obj client.Object) error {
			claimed, ok := obj.(*agentsv1alpha1.Session)
			if !ok {
				return errors.New("expected Session claim")
			}
			if failOnce && claimed.Status.CreationAttempted {
				failOnce = false
				current := read()
				current.Labels = map[string]string{"external-controller": "preserved"}
				Expect(shippedK8sClient.Update(ctx, current)).To(Succeed())
			}
			return nil
		}}
		Expect(apierrors.IsConflict(step())).To(BeTrue())
		Expect(provider.creates).To(BeZero())
		Expect(read().Status.CreationAttempted).To(BeFalse())
		Expect(step()).To(Succeed())
		Expect(provider.creates).To(Equal(1))
		Expect(read().Labels["external-controller"]).To(Equal("preserved"))
	})

	It("recovers remote creation when saving the returned ID fails without a second POST", func() {
		Expect(step()).To(Succeed())
		failOnce := true
		r.Client = &interceptClient{Client: shippedK8sClient, beforeStatusPatch: func(obj client.Object) error {
			current, ok := obj.(*agentsv1alpha1.Session)
			if !ok {
				return errors.New("expected Session status")
			}
			if failOnce && current.Status.OpenAISessionID != "" {
				failOnce = false
				return errors.New("Session ID status persistence failed")
			}
			return nil
		}}
		Expect(step()).To(HaveOccurred())
		Expect(read().Status.CreationAttempted).To(BeTrue())
		Expect(read().Status.OpenAISessionID).To(BeEmpty())
		r.Client = shippedK8sClient
		for range 2 {
			Expect(step()).To(Succeed())
		}
		Expect(read().Status.OpenAISessionID).To(Equal(testSessionID))
		Expect(provider.creates).To(Equal(1))
	})

	It("stops compute even when recording external deletion status fails, then reports the failure on retry", func() {
		provision()
		provider.remote = openaiapi.Session{}
		r.Client = &interceptClient{Client: shippedK8sClient, beforeStatusPatch: func(client.Object) error {
			return errors.New("status storage unavailable")
		}}
		Expect(step()).To(HaveOccurred())
		expectComputeStopped()
		r.Client = shippedK8sClient
		Expect(step()).To(MatchError(openaiapi.ErrNotFound))
		Expect(meta.FindStatusCondition(read().Status.Conditions, readyCondition).Reason).To(Equal("RemoteSessionMissing"))
		Expect(provider.creates).To(Equal(1))
	})

	It("blocks a replacement after a missing remote is observed following manager restart", func() {
		provision()
		id := read().Status.OpenAISessionID
		provider.remote = openaiapi.Session{}
		restarted := *r
		r = &restarted
		for range 3 {
			Expect(step()).To(MatchError(openaiapi.ErrNotFound))
		}
		Expect(read().Status.OpenAISessionID).To(Equal(id))
		Expect(provider.creates).To(Equal(1))
		expectComputeStopped()
	})

	It("does not start compute if the CR is deleted while the remote creation request is in flight", func() {
		provider.onCreate = func() {
			Expect(shippedK8sClient.Delete(ctx, read())).To(Succeed())
		}
		Expect(step()).To(Succeed())
		createErr := step()
		Expect(read().DeletionTimestamp.IsZero()).To(BeFalse())
		var pods corev1.PodList
		Expect(shippedK8sClient.List(ctx, &pods, client.InNamespace(session.Namespace))).To(Succeed())
		Expect(pods.Items).To(BeEmpty(), "a deletion requested during CREATE must prevent starting an executor")
		Expect(createErr).NotTo(HaveOccurred())
		for range 3 {
			Expect(step()).To(Succeed())
		}
		Expect(read().Finalizers).To(ConsistOf(otherTestFinalizer))
		Expect(provider.creates).To(Equal(1))
		Expect(provider.deletes).To(Equal(1))
	})

	It("cleans up deletion before claiming remote creation without provider mutations", func() {
		Expect(step()).To(Succeed())
		Expect(shippedK8sClient.Delete(ctx, read())).To(Succeed())
		Expect(step()).To(Succeed())
		Expect(read().Finalizers).To(ConsistOf(otherTestFinalizer))
		Expect(provider.creates).To(BeZero())
		Expect(provider.deletes).To(BeZero())
	})

	It("does not attach an in-flight create result to a same-name CR recreated with a different UID", func() {
		oldUID := session.UID
		provider.onCreate = func() {
			// Simulate an administrator forcibly removing finalizers in this isolated API server.
			current := read()
			current.Finalizers = nil
			Expect(shippedK8sClient.Update(ctx, current)).To(Succeed())
			Expect(shippedK8sClient.Delete(ctx, current)).To(Succeed())
			replacement := &agentsv1alpha1.Session{ObjectMeta: metav1.ObjectMeta{Name: session.Name, Namespace: session.Namespace}, Spec: *session.Spec.DeepCopy()}
			Expect(shippedK8sClient.Create(ctx, replacement)).To(Succeed())
		}
		Expect(step()).To(Succeed())
		Expect(step()).To(MatchError("session identity changed during remote creation; audit the original Kubernetes UID"))
		current := read()
		Expect(current.UID).NotTo(Equal(oldUID))
		Expect(current.Status.CreationAttempted).To(BeFalse())
		Expect(current.Status.OpenAISessionID).To(BeEmpty())
		Expect(current.Finalizers).To(BeEmpty())
		var pods corev1.PodList
		Expect(shippedK8sClient.List(ctx, &pods, client.InNamespace(session.Namespace))).To(Succeed())
		Expect(pods.Items).To(BeEmpty())
		Expect(provider.remote.OwnerUID).To(Equal(string(oldUID)))
		Expect(provider.creates).To(Equal(1))
		Expect(provider.deletes).To(BeZero())
	})

	It("holds an ambiguous create for audit when discovery finds no remote and later cleans up the unique result", func() {
		provider.loseResponse = true
		provider.hideDiscovery = true
		Expect(step()).To(Succeed())
		Expect(step()).To(HaveOccurred())
		for range 3 {
			Expect(step()).To(Succeed())
		}
		Expect(read().Status.OpenAISessionID).To(BeEmpty())
		Expect(meta.FindStatusCondition(read().Status.Conditions, readyCondition).Reason).To(Equal(creationOutcomeUnknownReason))
		Expect(shippedK8sClient.Delete(ctx, read())).To(Succeed())
		Expect(step()).To(Succeed())
		Expect(read().Finalizers).To(ContainElement(sessionFinalizer))
		Expect(provider.creates).To(Equal(1))
		Expect(provider.deletes).To(BeZero())
		provider.hideDiscovery = false
		Expect(step()).To(Succeed())
		Expect(step()).To(Succeed())
		Expect(read().Finalizers).To(ConsistOf(otherTestFinalizer))
		Expect(provider.deletes).To(Equal(1))
	})
})

// None creation includes the first input and the same saved Agent readback used
// by hosted scenarios. This keeps both modes on the current schema and claim path.
type scenarioNoneProvider struct {
	configured    *dependencyScenarioSessionProvider
	returnEmptyID bool
}

var _ InitialSessionProvider = (*scenarioNoneProvider)(nil)

func (p *scenarioNoneProvider) CreateInitialSession(ctx context.Context, uid, scope, agentID string,
	environment, agent map[string]any, vaultIDs []string, metadata *map[string]string,
	spend *agentsv1alpha1.SessionSpendControl, _, _ string) (openaiapi.Session, error) {
	_, err := p.configured.CreateConfiguredSession(ctx, uid, scope, agentID, environment, agent, vaultIDs, metadata, spend)
	p.configured.lifecycle.mu.Lock()
	defer p.configured.lifecycle.mu.Unlock()
	remote := &p.configured.lifecycle.remote
	remote.EnvironmentID = ""
	response := *remote
	if p.returnEmptyID {
		response.ID = ""
	}
	return response, err
}

type scenarioMissingIDProvider struct {
	SessionConfigurationProvider
}

var _ SessionConfigurationProvider = (*scenarioMissingIDProvider)(nil)

func (p *scenarioMissingIDProvider) CreateConfiguredSession(ctx context.Context, uid, scope, agentID string,
	environment, agent map[string]any, vaultIDs []string, metadata *map[string]string,
	spend *agentsv1alpha1.SessionSpendControl) (openaiapi.Session, error) {
	remote, err := p.SessionConfigurationProvider.CreateConfiguredSession(ctx, uid, scope, agentID, environment, agent, vaultIDs, metadata, spend)
	remote.ID = ""
	return remote, err
}

func configureScenarioNone(f *dependencyScenarioFixture, s *agentsv1alpha1.Session, returnEmptyID bool) {
	environment := &agentsv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "none-environment", Namespace: f.namespace},
		Spec: agentsv1alpha1.EnvironmentSpec{Mode: _noneMode, DeletionPolicy: deletePolicy}}
	Expect(f.client.Create(ctx, environment)).To(Succeed())
	f.cleanup(environment)
	er := &EnvironmentReconciler{Client: f.client, APIReader: f.client, OwnerScope: testAgentScope, GatewayEnabled: true}
	for range 2 {
		_, err := er.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(environment)})
		Expect(err).NotTo(HaveOccurred())
	}
	s.Spec.EnvironmentRef.Name = environment.Name
	Expect(f.client.Update(ctx, s)).To(Succeed())
	f.sessionController.Gateway = &gatewayStub{text: "controlled first input", key: _testFirstKey}
	f.sessionController.InitialSessions = &scenarioNoneProvider{configured: f.configuration, returnEmptyID: returnEmptyID}
}

var _ = Describe("Hosted and none immutable validation with shipped CRDs", func() {
	DescribeTable("holds a malformed creation response without an ID and recovers its one-time claim through discovery",
		func(mode string) {
			f := newDependencyScenarioFixture()
			f.ready()
			s := f.newSession(f.namespace)
			if mode == _noneMode {
				configureScenarioNone(f, s, true /* returnEmptyID */)
			} else {
				f.sessionController.Configuration = &scenarioMissingIDProvider{SessionConfigurationProvider: f.configuration}
			}
			for range 2 {
				f.sessionStep(s)
			}
			Expect(meta.FindStatusCondition(s.Status.Conditions, readyCondition).Reason).To(Equal(ownershipMismatchReason))
			Expect(meta.IsStatusConditionFalse(s.Status.Conditions, readyCondition)).To(BeTrue())
			Expect(s.Status.OpenAISessionID).To(BeEmpty())
			Expect(s.Status.CreationAttempted).To(BeTrue())
			Expect(s.Finalizers).To(ContainElement(sessionFinalizer))
			Expect(f.sessions.creates).To(Equal(1))
			for range 2 {
				f.sessionStep(s)
			}
			Expect(s.Status.Phase).To(Equal("Ready"))
			Expect(s.Status.OpenAISessionID).NotTo(BeEmpty())
			Expect(f.sessions.creates).To(Equal(1))
		}, Entry("hosted creation", openaiHostedMode), Entry("none creation", _noneMode))

	DescribeTable("blocks pending mutable updates after definitive remote drift",
		func(mode string, change func(*openaiapi.Session), wantReason string) {
			f := newDependencyScenarioFixture()
			f.ready()
			connections := &scenarioConnections{}
			f.sessionController.Connections = connections
			s := f.newSession(f.namespace)
			if mode == _noneMode {
				configureScenarioNone(f, s, false /* returnEmptyID */)
			}
			for range 3 {
				f.sessionStep(s)
			}
			Expect(s.Status.Phase).To(Equal("Ready"))
			original := s.DeepCopy()
			effort, cents := testSessionUpdatedEffort, int64(300)
			s.Spec.Agent = &agentsv1alpha1.SessionAgentOverrides{Reasoning: &agentsv1alpha1.AgentReasoning{Effort: &effort}}
			metadata := map[string]string{scenarioMutableMetadataKey: "pending"}
			s.Spec.Metadata = &metadata
			s.Spec.SpendControl = &agentsv1alpha1.SessionSpendControl{Mode: spendModeLimited, LimitCents: &cents}
			Expect(f.client.Update(ctx, s)).To(Succeed())
			change(&f.sessions.remote)
			f.sessionStep(s)
			Expect(meta.FindStatusCondition(s.Status.Conditions, readyCondition).Reason).To(Equal(wantReason))
			Expect(s.Status.Phase).To(Equal("Failed"))
			Expect(f.configuration.updates).To(BeZero())
			Expect(f.sessions.creates).To(Equal(1))
			Expect(f.sessions.deletes).To(BeZero())
			Expect(connections.forgotten).To(ContainElement(original.Status.OpenAISessionID))
			Expect(s.Status.OpenAISessionID).To(Equal(original.Status.OpenAISessionID))
			Expect(s.Status.EnvironmentID).To(Equal(original.Status.EnvironmentID))
			Expect(s.Status.AgentSnapshot).To(Equal(original.Status.AgentSnapshot))
			Expect(s.Status.EnvironmentSnapshot).To(Equal(original.Status.EnvironmentSnapshot))
		},
		Entry("hosted runtime identity", openaiHostedMode, func(s *openaiapi.Session) { s.EnvironmentID = scenarioForeignEnvironmentID }, configurationMismatchReason),
		Entry("hosted observable recipe", openaiHostedMode, func(s *openaiapi.Session) { s.EnvironmentConfigurationDigest = "foreign" }, configurationMismatchReason),
		Entry("hosted capability installation", openaiHostedMode, func(s *openaiapi.Session) { s.CapabilityDirectories = []string{scenarioForeignPath} }, configurationMismatchReason),
		Entry("hosted environment type", openaiHostedMode, func(s *openaiapi.Session) { s.EnvironmentType = _noneMode }, configurationMismatchReason),
		Entry("hosted Agent identity", openaiHostedMode, func(s *openaiapi.Session) { s.AgentID = scenarioForeignAgentID }, configurationMismatchReason),
		Entry("hosted immutable Agent settings", openaiHostedMode, func(s *openaiapi.Session) { s.AgentConfiguration["instructions"] = "foreign" }, configurationMismatchReason),
		Entry("hosted Vault attachments", openaiHostedMode, func(s *openaiapi.Session) { s.VaultIDs = []string{scenarioForeignVaultID} }, configurationMismatchReason),
		Entry("hosted failed Session", openaiHostedMode, func(s *openaiapi.Session) { s.Status = remoteFailedStatus }, "HostedSetupFailed"),
		Entry("none environment type", _noneMode, func(s *openaiapi.Session) { s.EnvironmentType = "openai_hosted" }, configurationMismatchReason),
		Entry("none Agent identity", _noneMode, func(s *openaiapi.Session) { s.AgentID = scenarioForeignAgentID }, configurationMismatchReason),
		Entry("none Vault attachments", _noneMode, func(s *openaiapi.Session) { s.VaultIDs = []string{scenarioForeignVaultID} }, configurationMismatchReason),
		Entry("none failed Session", _noneMode, func(s *openaiapi.Session) { s.Status = remoteFailedStatus }, "RemoteSessionFailed"),
	)
})
