package controller

import (
	"context"
	"encoding/json"
	"errors"
	"slices"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	"github.com/softcane/codex-session-operator/internal/openaiapi"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	dependencyScenarioURL           = "https://mcp.example.com/mcp"
	dependencyScenarioEnvironment   = "Environment"
	dependencyScenarioVault         = "Vault"
	dependencyScenarioCredential    = "VaultCredential"
	dependencyScenarioForeignOwner  = "another-owner"
	dependencyScenarioSelfHosted    = "self_hosted"
	dependencyScenarioVaultName     = "vault"
	dependencyScenarioRemoteMissing = "remote missing"
)

// These scenarios always use the shipped schema, including mandatory Session
// references and spend choices. Provider stubs expose public readback only.
type dependencyScenarioFixture struct {
	client                client.Client
	namespace             string
	agent                 *agentsv1alpha1.Agent
	environment           *agentsv1alpha1.Environment
	vault                 *agentsv1alpha1.Vault
	credential            *agentsv1alpha1.VaultCredential
	secret                *corev1.Secret
	agents                *agentProviderStub
	environments          *environmentProviderStub
	vaults                *vaultProviderStub
	agentController       *AgentReconciler
	environmentController *EnvironmentReconciler
	vaultController       *VaultReconciler
	credentialController  *VaultCredentialReconciler
	sessions              *sessionProviderStub
	configuration         *dependencyScenarioSessionProvider
	sessionController     *SessionReconciler
}

// Use the effective saved Agent and selected hosted recipe in creation readback,
// so current-schema Sessions exercise the complete dependency resolution path.
type dependencyScenarioSessionProvider struct {
	configuredSessionStub
	agents *agentProviderStub
}

var _ SessionConfigurationProvider = (*dependencyScenarioSessionProvider)(nil)

func (p *dependencyScenarioSessionProvider) CreateConfiguredSession(ctx context.Context, uid, scope, agentID string,
	environment, overrides map[string]any, vaultIDs []string, metadata *map[string]string,
	spend *agentsv1alpha1.SessionSpendControl) (openaiapi.Session, error) {
	_, err := p.configuredSessionStub.CreateConfiguredSession(ctx, uid, scope, agentID, environment, overrides, vaultIDs, metadata, spend)
	p.lifecycle.mu.Lock()
	defer p.lifecycle.mu.Unlock()
	p.agents.mu.Lock()
	defer p.agents.mu.Unlock()
	remote := &p.lifecycle.remote
	remote.ID = "sess_dependency_" + uid
	remote.AgentConfiguration = openaiapi.EffectiveSessionAgent(copyConfiguration(p.agents.remote.Config), overrides)
	remote.AgentConfigurationDigest = openaiapi.AgentConfigurationDigest(remote.AgentConfiguration)
	remote.Model, _ = remote.AgentConfiguration[agentModelField].(string)
	remote.EnvironmentType, _ = environment[agentTypeField].(string)
	remote.SelfHosted = remote.EnvironmentType == dependencyScenarioSelfHosted
	remote.EnvironmentConfigurationDigest = openaiapi.ObservableEnvironmentDigest(environment)
	return *remote, err
}

func newDependencyScenarioFixture() *dependencyScenarioFixture {
	f := &dependencyScenarioFixture{client: shippedK8sClient}
	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "dependency-scenario-"}}
	Expect(f.client.Create(ctx, namespace)).To(Succeed())
	f.namespace = namespace.Name
	name := "manual-agent"
	instructions := "Keep the first configuration."
	f.agent = &agentsv1alpha1.Agent{ObjectMeta: metav1.ObjectMeta{Name: "agent", Namespace: f.namespace},
		Spec: agentsv1alpha1.AgentSpec{Model: openaiapi.SessionModel, Name: &name, Instructions: &instructions, DeletionPolicy: deletePolicy}}
	f.environment = &agentsv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "environment", Namespace: f.namespace},
		Spec: agentsv1alpha1.EnvironmentSpec{Mode: openaiHostedMode, DeletionPolicy: deletePolicy,
			OpenAIHosted: &agentsv1alpha1.HostedEnvironment{Name: "manual-environment", Network: agentsv1alpha1.HostedNetwork{Access: testDisabledNetwork}}}}
	f.vault = &agentsv1alpha1.Vault{ObjectMeta: metav1.ObjectMeta{Name: dependencyScenarioVaultName, Namespace: f.namespace},
		Spec: agentsv1alpha1.VaultSpec{Name: &name, DeletionPolicy: deletePolicy}}
	f.credential = &agentsv1alpha1.VaultCredential{ObjectMeta: metav1.ObjectMeta{Name: "credential", Namespace: f.namespace},
		Spec: agentsv1alpha1.VaultCredentialSpec{Name: "mcp-credential", VaultRef: agentsv1alpha1.AgentReference{Name: f.vault.Name},
			DeletionPolicy: deletePolicy, Auth: agentsv1alpha1.VaultCredentialAuth{Type: testStaticBearerType,
				StaticBearer: &agentsv1alpha1.VaultStaticBearer{ServerURL: dependencyScenarioURL,
					TokenSecretRef: agentsv1alpha1.CredentialSecretReference{Name: openaiapi.VaultSecretName, Key: testVaultTokenKey}}}}}
	f.secret = &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: openaiapi.VaultSecretName, Namespace: f.namespace},
		Type: corev1.SecretTypeOpaque, Data: map[string][]byte{testVaultTokenKey: []byte(testCredentialToken())}}
	for _, obj := range []client.Object{f.agent, f.environment, f.vault, f.credential, f.secret} {
		Expect(f.client.Create(ctx, obj)).To(Succeed())
		f.cleanup(obj)
	}
	f.agents = &agentProviderStub{}
	f.environments = &environmentProviderStub{runtimeStatus: testConnectedRuntime}
	f.vaults = &vaultProviderStub{}
	f.agentController = &AgentReconciler{Client: f.client, APIReader: f.client, OpenAI: f.agents, OwnerScope: testAgentScope}
	f.environmentController = &EnvironmentReconciler{Client: f.client, APIReader: f.client, OpenAI: f.environments, OwnerScope: testAgentScope}
	f.vaultController = &VaultReconciler{Client: f.client, APIReader: f.client, OpenAI: f.vaults, OwnerScope: testAgentScope}
	f.credentialController = &VaultCredentialReconciler{Client: f.client, APIReader: f.client, OpenAI: f.vaults, OwnerScope: testAgentScope}
	f.sessions = &sessionProviderStub{}
	f.configuration = &dependencyScenarioSessionProvider{configuredSessionStub: configuredSessionStub{lifecycle: f.sessions}, agents: f.agents}
	f.sessionController = &SessionReconciler{Client: f.client, APIReader: f.client, OpenAI: f.sessions, Agents: f.agents,
		Environments: f.environments, Configuration: f.configuration, Vaults: f.vaults, AgentOwnerScope: testAgentScope}
	return f
}

func (f *dependencyScenarioFixture) cleanup(obj client.Object) {
	DeferCleanup(func() {
		current := obj.DeepCopyObject().(client.Object)
		err := f.client.Get(ctx, client.ObjectKeyFromObject(obj), current)
		if apierrors.IsNotFound(err) {
			return
		}
		Expect(err).NotTo(HaveOccurred())
		current.SetFinalizers(nil)
		Expect(f.client.Update(ctx, current)).To(Succeed())
		if current.GetDeletionTimestamp().IsZero() {
			Expect(client.IgnoreNotFound(f.client.Delete(ctx, current))).To(Succeed())
		}
	})
}

func (f *dependencyScenarioFixture) ready() {
	for _, reconciler := range []reconcile.Reconciler{f.agentController, f.environmentController, f.vaultController, f.credentialController} {
		var object client.Object
		switch reconciler {
		case f.agentController:
			object = f.agent
		case f.environmentController:
			object = f.environment
		case f.vaultController:
			object = f.vault
		case f.credentialController:
			object = f.credential
		}
		for range 3 {
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(object)})
			Expect(err).NotTo(HaveOccurred())
		}
	}
}

func (f *dependencyScenarioFixture) read(obj client.Object) {
	Expect(f.client.Get(ctx, client.ObjectKeyFromObject(obj), obj)).To(Succeed())
}

func (f *dependencyScenarioFixture) newSession(namespace string) *agentsv1alpha1.Session {
	s := &agentsv1alpha1.Session{ObjectMeta: metav1.ObjectMeta{GenerateName: "session-", Namespace: namespace},
		Spec: agentsv1alpha1.SessionSpec{AgentRef: &agentsv1alpha1.AgentReference{Name: f.agent.Name},
			EnvironmentRef: &agentsv1alpha1.AgentReference{Name: f.environment.Name},
			SpendControl:   &agentsv1alpha1.SessionSpendControl{Mode: testUnlimitedSpendMode}}}
	Expect(f.client.Create(ctx, s)).To(Succeed())
	f.cleanup(s)
	return s
}

func (f *dependencyScenarioFixture) sessionStep(s *agentsv1alpha1.Session) {
	_, err := f.sessionController.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(s)})
	Expect(err).NotTo(HaveOccurred())
	f.read(s)
}

type dependencyScenarioResource struct {
	object     client.Object
	reconciler reconcile.Reconciler
	finalizer  string
	conditions func() []metav1.Condition
	identity   func() string
	remoteID   func() (string, error)
	disappear  func()
	wrongOwner func()
	retain     func()
	creates    func() int
	updates    func() int
	deletes    func() int
}

func (f *dependencyScenarioFixture) resource(kind string) dependencyScenarioResource {
	switch kind {
	case testAgentKind:
		return dependencyScenarioResource{object: f.agent, reconciler: f.agentController, finalizer: agentFinalizer,
			conditions: func() []metav1.Condition { return f.agent.Status.Conditions },
			identity:   func() string { return f.agent.Status.OpenAIAgentID },
			remoteID: func() (string, error) {
				remote, err := f.agents.GetAgent(ctx, f.agent.Status.OpenAIAgentID)
				return remote.ID, err
			},
			disappear:  func() { f.agents.remote = openaiapi.Agent{} },
			wrongOwner: func() { f.agents.remote.OwnerUID = dependencyScenarioForeignOwner },
			retain:     func() { f.agent.Spec.DeletionPolicy = retainPolicy },
			creates:    func() int { return f.agents.creates }, updates: func() int { return f.agents.updates }, deletes: func() int { return f.agents.deletes }}
	case dependencyScenarioEnvironment:
		return dependencyScenarioResource{object: f.environment, reconciler: f.environmentController, finalizer: environmentFinalizer,
			conditions: func() []metav1.Condition { return f.environment.Status.Conditions },
			identity:   func() string { return f.environment.Status.OpenAITemplateID },
			remoteID: func() (string, error) {
				remote, err := f.environments.GetTemplate(ctx, f.environment.Status.OpenAITemplateID)
				return remote.ID, err
			},
			disappear:  func() { f.environments.remote = openaiapi.Template{} },
			wrongOwner: func() { f.environments.remote.Name = "another-template" },
			retain:     func() { f.environment.Spec.DeletionPolicy = retainPolicy },
			creates:    func() int { return f.environments.creates }, updates: func() int { return f.environments.updates }, deletes: func() int { return f.environments.deletes }}
	case dependencyScenarioVault:
		return dependencyScenarioResource{object: f.vault, reconciler: f.vaultController, finalizer: vaultFinalizer,
			conditions: func() []metav1.Condition { return f.vault.Status.Conditions },
			identity:   func() string { return f.vault.Status.OpenAIVaultID },
			remoteID: func() (string, error) {
				remote, err := f.vaults.GetVault(ctx, f.vault.Status.OpenAIVaultID)
				return remote.ID, err
			},
			disappear:  func() { f.vaults.vault = openaiapi.Vault{} },
			wrongOwner: func() { f.vaults.vault.OwnerUID = dependencyScenarioForeignOwner },
			retain:     func() { f.vault.Spec.DeletionPolicy = retainPolicy },
			creates:    func() int { return f.vaults.creates }, updates: func() int { return f.vaults.updates }, deletes: func() int { return f.vaults.deletes }}
	case dependencyScenarioCredential:
		return dependencyScenarioResource{object: f.credential, reconciler: f.credentialController, finalizer: credentialFinalizer,
			conditions: func() []metav1.Condition { return f.credential.Status.Conditions },
			identity:   func() string { return f.credential.Status.OpenAICredentialID },
			remoteID: func() (string, error) {
				remote, err := f.vaults.GetCredential(ctx, f.credential.Status.OpenAIVaultID, f.credential.Status.OpenAICredentialID)
				return remote.ID, err
			},
			disappear:  func() { f.vaults.credential = openaiapi.Credential{} },
			wrongOwner: func() { f.vaults.credential.OwnerUID = dependencyScenarioForeignOwner },
			retain:     func() { f.credential.Spec.DeletionPolicy = retainPolicy },
			creates:    func() int { return f.vaults.credentialCreates }, updates: func() int { return f.vaults.rotations }, deletes: func() int { return f.vaults.deletes }}
	default:
		Fail("Unknown dependency kind: " + kind)
		return dependencyScenarioResource{}
	}
}

func (d dependencyScenarioResource) step(f *dependencyScenarioFixture) error {
	_, err := d.reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(d.object)})
	f.read(d.object)
	return err
}

var _ = Describe("Dependency scenarios with shipped CRDs", func() {
	var f *dependencyScenarioFixture
	BeforeEach(func() { f = newDependencyScenarioFixture() })

	DescribeTable("external console deletion keeps the known identity and never creates a replacement",
		func(kind string) {
			f.ready()
			d := f.resource(kind)
			f.read(d.object)
			identity := d.identity()
			uid := d.object.GetUID()
			Expect(identity).NotTo(BeEmpty())
			d.disappear()
			for range 2 {
				Expect(errors.Is(d.step(f), openaiapi.ErrNotFound)).To(BeTrue())
			}
			Expect(meta.FindStatusCondition(d.conditions(), readyCondition).Reason).To(Equal("ObservationFailed"))
			Expect(d.creates()).To(Equal(1))
			Expect(d.updates()).To(BeZero())
			Expect(controllerutil.ContainsFinalizer(d.object, d.finalizer)).To(BeTrue())
			Expect(d.identity()).To(Equal(identity))
			Expect(d.object.GetUID()).To(Equal(uid))
			Expect(d.object.GetDeletionTimestamp().IsZero()).To(BeTrue())
			Expect(meta.IsStatusConditionFalse(d.conditions(), readyCondition)).To(BeTrue())
		}, Entry("Agent", testAgentKind), Entry(dependencyScenarioEnvironment, dependencyScenarioEnvironment), Entry(dependencyScenarioVault, dependencyScenarioVault), Entry(dependencyScenarioCredential, dependencyScenarioCredential))

	DescribeTable("foreign ownership blocks both synchronization and cleanup",
		func(kind string) {
			f.ready()
			d := f.resource(kind)
			d.wrongOwner()
			Expect(d.step(f)).To(Succeed())
			Expect(meta.FindStatusCondition(d.conditions(), readyCondition).Reason).To(Equal("OwnershipMismatch"))
			Expect(f.client.Delete(ctx, d.object)).To(Succeed())
			if kind == dependencyScenarioVault {
				f.removeCredential()
			}
			Expect(d.step(f)).To(Succeed())
			Expect(meta.FindStatusCondition(d.conditions(), readyCondition).Reason).To(Equal("OwnershipMismatch"))
			Expect(d.creates()).To(Equal(1))
			Expect(d.updates()).To(BeZero())
			Expect(d.deletes()).To(BeZero())
			Expect(controllerutil.ContainsFinalizer(d.object, d.finalizer)).To(BeTrue())
		}, Entry("Agent", testAgentKind), Entry(dependencyScenarioEnvironment, dependencyScenarioEnvironment), Entry(dependencyScenarioVault, dependencyScenarioVault), Entry(dependencyScenarioCredential, dependencyScenarioCredential))

	DescribeTable("Retain releases the Kubernetes obligation without deleting a remote dependency",
		func(kind string) {
			f.ready()
			d := f.resource(kind)
			f.read(d.object)
			identity := d.identity()
			d.retain()
			Expect(f.client.Update(ctx, d.object)).To(Succeed())
			Expect(f.client.Delete(ctx, d.object)).To(Succeed())
			_, err := d.reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(d.object)})
			Expect(err).NotTo(HaveOccurred())
			Expect(apierrors.IsNotFound(f.client.Get(ctx, client.ObjectKeyFromObject(d.object), d.object))).To(BeTrue())
			Expect(d.deletes()).To(BeZero())
			remoteID, err := d.remoteID()
			Expect(err).NotTo(HaveOccurred())
			Expect(remoteID).To(Equal(identity))
		}, Entry("Agent", testAgentKind), Entry(dependencyScenarioEnvironment, dependencyScenarioEnvironment), Entry(dependencyScenarioVault, dependencyScenarioVault), Entry(dependencyScenarioCredential, dependencyScenarioCredential))

	DescribeTable("Delete releases a finalizer when console deletion already removed the remote dependency",
		func(kind string) {
			f.ready()
			d := f.resource(kind)
			if kind == dependencyScenarioVault {
				f.removeCredential()
			}
			f.read(d.object)
			d.disappear()
			Expect(f.client.Delete(ctx, d.object)).To(Succeed())
			_, err := d.reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(d.object)})
			Expect(err).NotTo(HaveOccurred())
			Expect(apierrors.IsNotFound(f.client.Get(ctx, client.ObjectKeyFromObject(d.object), d.object))).To(BeTrue())
			Expect(d.creates()).To(Equal(1))
			Expect(d.deletes()).To(BeZero())
		}, Entry("Agent", testAgentKind), Entry(dependencyScenarioEnvironment, dependencyScenarioEnvironment), Entry(dependencyScenarioVault, dependencyScenarioVault), Entry(dependencyScenarioCredential, dependencyScenarioCredential))

	DescribeTable("Delete waits for observed remote absence before removing the Kubernetes resource",
		func(kind string) {
			f.ready()
			d := f.resource(kind)
			if kind == dependencyScenarioVault {
				f.removeCredential()
			}
			f.read(d.object)
			Expect(f.client.Delete(ctx, d.object)).To(Succeed())
			Expect(d.step(f)).To(Succeed())
			Expect(meta.FindStatusCondition(d.conditions(), readyCondition).Reason).To(Equal("CleanupPending"))
			Expect(controllerutil.ContainsFinalizer(d.object, d.finalizer)).To(BeTrue())
			Expect(d.deletes()).To(Equal(1))
			_, err := d.reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(d.object)})
			Expect(err).NotTo(HaveOccurred())
			Expect(apierrors.IsNotFound(f.client.Get(ctx, client.ObjectKeyFromObject(d.object), d.object))).To(BeTrue())
			Expect(d.deletes()).To(Equal(1))
		}, Entry("Agent", testAgentKind), Entry(dependencyScenarioEnvironment, dependencyScenarioEnvironment),
		Entry(dependencyScenarioVault, dependencyScenarioVault), Entry(dependencyScenarioCredential, dependencyScenarioCredential))

	DescribeTable("installation scope changes stop mutations and resume safely after the original scope is restored",
		func(kind string) {
			f.ready()
			d := f.resource(kind)
			f.read(d.object)
			identity := d.identity()
			setScope := func(scope string) {
				switch kind {
				case testAgentKind:
					f.agentController.OwnerScope = scope
				case dependencyScenarioEnvironment:
					f.environmentController.OwnerScope = scope
				case dependencyScenarioVault:
					f.vaultController.OwnerScope = scope
				case dependencyScenarioCredential:
					f.credentialController.OwnerScope = scope
				}
			}
			setScope("different-installation/project")
			Expect(d.step(f)).To(Succeed())
			Expect(meta.FindStatusCondition(d.conditions(), readyCondition).Reason).To(Equal("OwnershipScopeChanged"))
			Expect(d.creates()).To(Equal(1))
			Expect(d.updates()).To(BeZero())
			Expect(d.deletes()).To(BeZero())
			setScope(testAgentScope)
			Expect(d.step(f)).To(Succeed())
			Expect(meta.IsStatusConditionTrue(d.conditions(), readyCondition)).To(BeTrue())
			Expect(d.identity()).To(Equal(identity))
		}, Entry("Agent", testAgentKind), Entry(dependencyScenarioEnvironment, dependencyScenarioEnvironment),
		Entry(dependencyScenarioVault, dependencyScenarioVault), Entry(dependencyScenarioCredential, dependencyScenarioCredential))

	DescribeTable("frozen Session references protect deletion after the Session spec chooses different dependencies",
		func(kind, wantReason string) {
			f.ready()
			s := f.newSession(f.namespace)
			s.Spec.AgentRef.Name, s.Spec.EnvironmentRef.Name = "other-agent", "other-environment"
			Expect(f.client.Update(ctx, s)).To(Succeed())
			s.Status.AgentSnapshot = &agentsv1alpha1.SessionAgentSnapshot{Name: f.agent.Name, UID: string(f.agent.UID), Generation: f.agent.Generation,
				OpenAIAgentID: testAgentID, Model: openaiapi.SessionModel, ConfigurationDigest: "frozen"}
			f.read(f.environment)
			s.Status.EnvironmentSnapshot = &agentsv1alpha1.SessionEnvironmentSnapshot{Name: f.environment.Name, UID: string(f.environment.UID), Generation: f.environment.Generation,
				OpenAITemplateID: testTemplateID, ConfigurationDigest: f.environment.Status.ConfigurationDigest, Profile: f.environment.Spec}
			s.Status.VaultSnapshots = []agentsv1alpha1.SessionVaultSnapshot{{Name: f.vault.Name, UID: string(f.vault.UID), Generation: f.vault.Generation, OpenAIVaultID: f.vaults.vault.ID}}
			Expect(f.client.Status().Update(ctx, s)).To(Succeed())
			d := f.resource(kind)
			f.read(d.object)
			Expect(f.client.Delete(ctx, d.object)).To(Succeed())
			Expect(d.step(f)).To(Succeed())
			Expect(meta.FindStatusCondition(d.conditions(), readyCondition).Reason).To(Equal(wantReason))
			Expect(d.deletes()).To(BeZero())
		}, Entry("Agent", testAgentKind, testAgentInUseReason), Entry(dependencyScenarioEnvironment, dependencyScenarioEnvironment, "EnvironmentInUse"),
		Entry(dependencyScenarioVault, dependencyScenarioVault, "VaultInUse"), Entry(dependencyScenarioCredential, dependencyScenarioCredential, "CredentialInUse"))

	It("repairs external Agent and Vault public edits, preserves foreign metadata, and waits for readback", func() {
		f.ready()
		f.agents.remote.Config[testNameField] = "console-edited"
		f.vaults.vault.Name = "console-edited"
		f.vaults.vault.Metadata[testExternalMetadataKey] = testExternalMetadataValue
		for _, kind := range []string{testAgentKind, dependencyScenarioVault} {
			d := f.resource(kind)
			Expect(d.step(f)).To(Succeed())
			Expect(meta.IsStatusConditionTrue(d.conditions(), readyCondition)).To(BeFalse())
			Expect(d.step(f)).To(Succeed())
			Expect(meta.IsStatusConditionTrue(d.conditions(), readyCondition)).To(BeTrue())
			Expect(d.updates()).To(Equal(1))
		}
		Expect(f.agents.remote.Config[testNameField]).To(Equal(*f.agent.Spec.Name))
		Expect(f.vaults.vault.Name).To(Equal(*f.vault.Spec.Name))
		Expect(f.vaults.vault.Metadata[testExternalMetadataKey]).To(Equal(testExternalMetadataValue))
	})

	It("repairs external template drift with a complete replacement and retains the same identity", func() {
		f.ready()
		f.environments.remote.Config[testNetworkField] = map[string]any{testAccessField: "unrestricted"}
		d := f.resource(dependencyScenarioEnvironment)
		Expect(d.step(f)).To(Succeed())
		Expect(meta.IsStatusConditionFalse(d.conditions(), readyCondition)).To(BeTrue())
		Expect(d.step(f)).To(Succeed())
		Expect(meta.IsStatusConditionTrue(d.conditions(), readyCondition)).To(BeTrue())
		Expect(f.environments.creates).To(Equal(1))
		Expect(f.environments.updates).To(Equal(1))
		Expect(f.environment.Status.OpenAITemplateID).To(Equal(testTemplateID))
	})

	It("keeps an immutable credential public edit blocked instead of rewriting its authentication identity", func() {
		f.ready()
		f.vaults.credential.Name = "console-renamed"
		d := f.resource(dependencyScenarioCredential)
		Expect(d.step(f)).To(Succeed())
		Expect(meta.FindStatusCondition(d.conditions(), readyCondition).Reason).To(Equal("OwnershipMismatch"))
		Expect(f.vaults.rotations).To(BeZero())
	})

	It("freezes existing Session snapshots while later Sessions copy changed Agent and Environment settings", func() {
		f.ready()
		first := f.newSession(f.namespace)
		for range 3 {
			f.sessionStep(first)
		}
		Expect(first.Status.Phase).To(Equal("Ready"))
		agentSnapshot := *first.Status.AgentSnapshot
		environmentSnapshot := first.Status.EnvironmentSnapshot.DeepCopy()
		f.read(f.agent)
		updatedInstructions := "Use the later configuration."
		f.agent.Spec.Instructions = &updatedInstructions
		Expect(f.client.Update(ctx, f.agent)).To(Succeed())
		f.read(f.environment)
		f.environment.Spec.OpenAIHosted.ContainerSize = "large"
		Expect(f.client.Update(ctx, f.environment)).To(Succeed())
		f.ready()
		f.sessionStep(first)
		Expect(*first.Status.AgentSnapshot).To(Equal(agentSnapshot))
		Expect(first.Status.EnvironmentSnapshot).To(Equal(environmentSnapshot))
		Expect(f.configuration.updates).To(BeZero())
		// Give each remote Session its own independent readback, so reconciling
		// the first after the second cannot accidentally observe the latter.
		later := *f
		later.sessions = &sessionProviderStub{}
		later.configuration = &dependencyScenarioSessionProvider{
			configuredSessionStub: configuredSessionStub{lifecycle: later.sessions}, agents: f.agents}
		laterController := *f.sessionController
		laterController.OpenAI, laterController.Configuration = later.sessions, later.configuration
		later.sessionController = &laterController
		second := later.newSession(f.namespace)
		for range 3 {
			later.sessionStep(second)
		}
		Expect(second.Status.Phase).To(Equal("Ready"))
		Expect(second.Status.OpenAISessionID).NotTo(Equal(first.Status.OpenAISessionID))
		Expect(second.Status.AgentSnapshot.Generation).To(BeNumerically(">", agentSnapshot.Generation))
		Expect(second.Status.EnvironmentSnapshot.Profile.OpenAIHosted.ContainerSize).To(Equal("large"))
		Expect(later.sessions.remote.AgentConfiguration["instructions"]).To(Equal(updatedInstructions))
		f.sessionStep(first)
		Expect(first.Status.Phase).To(Equal("Ready"))
		Expect(*first.Status.AgentSnapshot).To(Equal(agentSnapshot))
		Expect(first.Status.EnvironmentSnapshot).To(Equal(environmentSnapshot))
		Expect(f.sessions.remote.AgentConfiguration["instructions"]).NotTo(Equal(updatedInstructions))
		Expect(f.sessions.creates).To(Equal(1))
		Expect(later.sessions.creates).To(Equal(1))
	})

	DescribeTable("a new Session waits for dependencies without spending a remote creation claim",
		func(kind, state, wantReason string) {
			f.ready()
			d := f.resource(kind)
			f.read(d.object)
			switch state {
			case "missing":
				d.object.SetFinalizers(nil)
				Expect(f.client.Update(ctx, d.object)).To(Succeed())
				Expect(f.client.Delete(ctx, d.object)).To(Succeed())
			case "stale":
				switch kind {
				case testAgentKind:
					value := "updated name"
					f.agent.Spec.Name = &value
				case dependencyScenarioEnvironment:
					f.environment.Spec.OpenAIHosted.ContainerSize = "large"
				case dependencyScenarioVault:
					value := "updated name"
					f.vault.Spec.Name = &value
				}
				Expect(f.client.Update(ctx, d.object)).To(Succeed())
			case "deleting":
				Expect(f.client.Delete(ctx, d.object)).To(Succeed())
			case dependencyScenarioRemoteMissing:
				d.disappear()
			}
			s := f.newSession(f.namespace)
			if kind == dependencyScenarioVault {
				s.Spec.VaultRefs = []agentsv1alpha1.AgentReference{{Name: f.vault.Name}}
				Expect(f.client.Update(ctx, s)).To(Succeed())
			}
			f.sessionStep(s)
			_, err := f.sessionController.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(s)})
			if state == dependencyScenarioRemoteMissing {
				Expect(errors.Is(err, openaiapi.ErrNotFound)).To(BeTrue())
			} else {
				Expect(err).NotTo(HaveOccurred())
			}
			f.read(s)
			Expect(meta.FindStatusCondition(s.Status.Conditions, readyCondition).Reason).To(Equal(wantReason))
			Expect(s.Status.CreationAttempted).To(BeFalse())
			Expect(f.sessions.creates).To(BeZero())
		}, Entry("missing Agent", testAgentKind, "missing", agentNotReadyReason), Entry("stale Agent", testAgentKind, "stale", agentNotReadyReason),
		Entry("deleting Agent", testAgentKind, "deleting", agentNotReadyReason), Entry("console-deleted Agent", testAgentKind, dependencyScenarioRemoteMissing, agentNotReadyReason),
		Entry("missing Environment", dependencyScenarioEnvironment, "missing", environmentNotReadyReason), Entry("stale Environment", dependencyScenarioEnvironment, "stale", environmentNotReadyReason),
		Entry("deleting Environment", dependencyScenarioEnvironment, "deleting", environmentNotReadyReason), Entry("console-deleted Environment", dependencyScenarioEnvironment, dependencyScenarioRemoteMissing, environmentNotReadyReason),
		Entry("missing Vault", dependencyScenarioVault, "missing", "VaultNotReady"), Entry("stale Vault", dependencyScenarioVault, "stale", "VaultNotReady"),
		Entry("deleting Vault", dependencyScenarioVault, "deleting", "VaultNotReady"), Entry("console-deleted Vault", dependencyScenarioVault, dependencyScenarioRemoteMissing, "VaultNotReady"))

	It("never resolves an Agent, Environment, or Vault from a different namespace with the same resource name", func() {
		f.ready()
		namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "different-dependency-"}}
		Expect(f.client.Create(ctx, namespace)).To(Succeed())
		s := f.newSession(namespace.Name)
		_, _, reason, _, err := f.sessionController.resolveEnvironment(ctx, s)
		Expect(err).NotTo(HaveOccurred())
		Expect(reason).To(Equal(environmentNotReadyReason))
		_, reason, _, err = f.sessionController.resolveAgent(ctx, s, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(reason).To(Equal(agentNotReadyReason))
		s.Spec.VaultRefs = []agentsv1alpha1.AgentReference{{Name: f.vault.Name}}
		_, _, message, err := f.sessionController.resolveVaults(ctx, s, nil, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(message).To(Equal("Could not read referenced Vault"))
		Expect(f.sessions.creates).To(BeZero())
	})

	DescribeTable("credentials stop reconciling when their claimed Vault is no longer safe to use",
		func(state string) {
			f.ready()
			f.read(f.vault)
			f.read(f.credential)
			identity, vaultUID := f.credential.Status.OpenAICredentialID, f.credential.Status.VaultUID
			switch state {
			case "missing":
				f.vault.Finalizers = nil
				Expect(f.client.Update(ctx, f.vault)).To(Succeed())
				Expect(f.client.Delete(ctx, f.vault)).To(Succeed())
			case "stale":
				name := "new Vault display name"
				f.vault.Spec.Name = &name
				Expect(f.client.Update(ctx, f.vault)).To(Succeed())
			case "not ready":
				vaultCondition(f.vault, metav1.ConditionFalse, "ObservationPending", "Waiting for observation")
				Expect(f.client.Status().Update(ctx, f.vault)).To(Succeed())
			case "deleting":
				Expect(f.client.Delete(ctx, f.vault)).To(Succeed())
			case dependencyScenarioRemoteMissing:
				f.vaults.vault = openaiapi.Vault{}
			case "foreign owner":
				f.vaults.vault.OwnerUID = dependencyScenarioForeignOwner
			}
			d := f.resource(dependencyScenarioCredential)
			err := d.step(f)
			if state == dependencyScenarioRemoteMissing {
				Expect(errors.Is(err, openaiapi.ErrNotFound)).To(BeTrue())
			} else {
				Expect(err).NotTo(HaveOccurred())
			}
			Expect(meta.FindStatusCondition(d.conditions(), readyCondition).Reason).To(Equal("VaultNotReady"))
			Expect(f.credential.Status.OpenAICredentialID).To(Equal(identity))
			Expect(f.credential.Status.VaultUID).To(Equal(vaultUID))
			Expect(f.vaults.credentialCreates).To(Equal(1))
			Expect(f.vaults.rotations).To(BeZero())
		}, Entry("missing Vault", "missing"), Entry("stale Vault", "stale"), Entry("unready Vault", "not ready"),
		Entry("deleting Vault", "deleting"), Entry("console-deleted Vault", dependencyScenarioRemoteMissing), Entry("foreign Vault", "foreign owner"))

	DescribeTable("new Sessions refuse unsafe Vault credential attachments before their creation claim",
		func(state string) {
			f.ready()
			f.read(f.credential)
			switch state {
			case "pending rotation":
				f.secret.Data[testVaultTokenKey] = []byte(testCredentialToken())
				Expect(f.client.Update(ctx, f.secret)).To(Succeed())
			case "not ready":
				credentialCondition(f.credential, metav1.ConditionFalse, "ObservationPending", "Waiting for observation")
				Expect(f.client.Status().Update(ctx, f.credential)).To(Succeed())
			case dependencyScenarioRemoteMissing:
				f.vaults.credential = openaiapi.Credential{}
			case "foreign owner":
				f.vaults.credential.OwnerUID = dependencyScenarioForeignOwner
			case "unmanaged attachment":
				f.credential.Finalizers = nil
				Expect(f.client.Update(ctx, f.credential)).To(Succeed())
				Expect(f.client.Delete(ctx, f.credential)).To(Succeed())
			}
			s := f.newSession(f.namespace)
			s.Spec.VaultRefs = []agentsv1alpha1.AgentReference{{Name: f.vault.Name}}
			Expect(f.client.Update(ctx, s)).To(Succeed())
			f.sessionStep(s)
			_, err := f.sessionController.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(s)})
			if state == dependencyScenarioRemoteMissing {
				Expect(errors.Is(err, openaiapi.ErrNotFound)).To(BeTrue())
			} else {
				Expect(err).NotTo(HaveOccurred())
			}
			f.read(s)
			Expect(meta.FindStatusCondition(s.Status.Conditions, readyCondition).Reason).To(Equal("VaultNotReady"))
			Expect(s.Status.CreationAttempted).To(BeFalse())
			Expect(f.sessions.creates).To(BeZero())
		}, Entry("pending credential rotation", "pending rotation"), Entry("unready credential", "not ready"),
		Entry("console-deleted credential", dependencyScenarioRemoteMissing), Entry("foreign credential", "foreign owner"), Entry("unmanaged attachment", "unmanaged attachment"))

	It("never transfers an existing credential claim to a same-name recreated Vault", func() {
		f.ready()
		f.read(f.vault)
		oldUID := f.vault.UID
		f.vault.Finalizers = nil
		Expect(f.client.Update(ctx, f.vault)).To(Succeed())
		Expect(f.client.Delete(ctx, f.vault)).To(Succeed())
		f.vault = &agentsv1alpha1.Vault{ObjectMeta: metav1.ObjectMeta{Name: dependencyScenarioVaultName, Namespace: f.namespace},
			Spec: agentsv1alpha1.VaultSpec{DeletionPolicy: deletePolicy}}
		Expect(f.client.Create(ctx, f.vault)).To(Succeed())
		for range 3 {
			_, err := f.vaultController.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(f.vault)})
			Expect(err).NotTo(HaveOccurred())
		}
		Expect(f.vault.UID).NotTo(Equal(oldUID))
		d := f.resource(dependencyScenarioCredential)
		Expect(d.step(f)).To(Succeed())
		Expect(meta.FindStatusCondition(d.conditions(), readyCondition).Reason).To(Equal("VaultNotReady"))
		Expect(f.credential.Status.VaultUID).To(Equal(string(oldUID)))
		Expect(f.vaults.credentialCreates).To(Equal(1))
		Expect(f.vaults.rotations).To(BeZero())
	})

	It("revokes credential readiness after source deletion and acknowledges recreated Secret identity before reuse", func() {
		f.ready()
		f.read(f.credential)
		oldVersions := slices.Clone(f.credential.Status.SourceVersions)
		Expect(f.client.Delete(ctx, f.secret)).To(Succeed())
		d := f.resource(dependencyScenarioCredential)
		Expect(d.step(f)).To(Succeed())
		Expect(meta.FindStatusCondition(d.conditions(), readyCondition).Reason).To(Equal("ProtectedSourceUnavailable"))
		Expect(f.credential.Status.SourceVersions).To(Equal(oldVersions))
		f.secret = &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: openaiapi.VaultSecretName, Namespace: f.namespace},
			Type: corev1.SecretTypeOpaque, Data: map[string][]byte{testVaultTokenKey: []byte(testCredentialToken())}}
		Expect(f.client.Create(ctx, f.secret)).To(Succeed())
		Expect(d.step(f)).To(Succeed())
		Expect(meta.IsStatusConditionFalse(d.conditions(), readyCondition)).To(BeTrue())
		Expect(f.credential.Status.SourceVersions[0].UID).NotTo(Equal(oldVersions[0].UID))
		Expect(d.step(f)).To(Succeed())
		Expect(meta.IsStatusConditionTrue(d.conditions(), readyCondition)).To(BeTrue())
		Expect(f.vaults.rotations).To(Equal(1))
		Expect(f.vaults.credentialCreates).To(Equal(1))
	})

	It("withholds a rotated source acknowledgment after an ambiguous response, then recovers from redacted public readback", func() {
		f.ready()
		f.read(f.credential)
		oldVersions := slices.Clone(f.credential.Status.SourceVersions)
		f.secret.Data[testVaultTokenKey] = []byte(testCredentialToken())
		Expect(f.client.Update(ctx, f.secret)).To(Succeed())
		f.vaults.loseRotation = true
		d := f.resource(dependencyScenarioCredential)
		Expect(d.step(f)).NotTo(Succeed())
		Expect(f.credential.Status.SourceVersions).To(Equal(oldVersions))
		Expect(meta.FindStatusCondition(d.conditions(), readyCondition).Reason).To(Equal("RotationOutcomeUnknown"))
		f.vaults.loseRotation = false
		Expect(d.step(f)).To(Succeed())
		Expect(d.step(f)).To(Succeed())
		Expect(f.credential.Status.SourceVersions[0].ResourceVersion).To(Equal(f.secret.ResourceVersion))
		Expect(meta.IsStatusConditionTrue(d.conditions(), readyCondition)).To(BeTrue())
		Expect(meta.FindStatusCondition(d.conditions(), "SecretMaterialVerified").Status).To(Equal(metav1.ConditionUnknown))
		encoded, err := json.Marshal(f.credential)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(encoded)).NotTo(ContainSubstring(string(f.secret.Data[testVaultTokenKey])))
	})

	It("revokes hosted Environment readiness on protected source loss and restores it after a replacement write", func() {
		secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: openaiapi.EnvironmentSecretName, Namespace: f.namespace},
			Type: corev1.SecretTypeOpaque, Data: map[string][]byte{testProtectedKey: []byte("controlled-environment-value")}}
		Expect(f.client.Create(ctx, secret)).To(Succeed())
		f.cleanup(secret)
		f.environment.Spec.OpenAIHosted.Env = []agentsv1alpha1.HostedVariable{{Name: "CONTROLLED_VALUE", SecretKey: testProtectedKey}}
		Expect(f.client.Update(ctx, f.environment)).To(Succeed())
		f.ready()
		f.read(f.environment)
		oldSource := *f.environment.Status.SourceVersion
		Expect(f.client.Delete(ctx, secret)).To(Succeed())
		d := f.resource(dependencyScenarioEnvironment)
		Expect(d.step(f)).NotTo(Succeed())
		Expect(meta.FindStatusCondition(d.conditions(), readyCondition).Reason).To(Equal("ProtectedSourceUnavailable"))
		secret = &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: openaiapi.EnvironmentSecretName, Namespace: f.namespace},
			Type: corev1.SecretTypeOpaque, Data: map[string][]byte{testProtectedKey: []byte("controlled-replacement-value")}}
		Expect(f.client.Create(ctx, secret)).To(Succeed())
		Expect(d.step(f)).To(Succeed())
		Expect(f.environment.Status.SourceVersion.UID).NotTo(Equal(oldSource.UID))
		Expect(d.step(f)).To(Succeed())
		Expect(meta.IsStatusConditionTrue(d.conditions(), readyCondition)).To(BeTrue())
		Expect(f.environments.updates).To(Equal(1))
	})
})

func (f *dependencyScenarioFixture) removeCredential() {
	f.read(f.credential)
	f.credential.Finalizers = nil
	Expect(f.client.Update(ctx, f.credential)).To(Succeed())
	Expect(f.client.Delete(ctx, f.credential)).To(Succeed())
	f.vaults.credential = openaiapi.Credential{}
}
