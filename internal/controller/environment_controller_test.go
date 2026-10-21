package controller

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	"github.com/softcane/codex-session-operator/internal/openaiapi"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const testSmallHostedSize = "small"

const (
	testNetworkField = "network"
)

const (
	testDisabledNetwork = "disabled"
	testProtectedKey    = "value"
	testNoneMode        = "none"
	testHostedBranch    = "openaiHosted"
	testModeField       = "mode"
	testAccessField     = "access"
	testNameField       = "name"
	testForbiddenValue  = "FORBIDDEN_PLACEHOLDER"
)

const testConnectedRuntime = "connected"

const testTemplateID = "tmpl_test"
const environmentTestScope = "installation/project"

type environmentProviderStub struct {
	mu                                    sync.Mutex
	sessions                              *sessionProviderStub
	remote                                openaiapi.Template
	creates, updates, deletes             int
	loseCreate, loseUpdate, deleteFailure bool
	runtimeStatus                         string
}

var _ TemplateProvider = (*environmentProviderStub)(nil)
var _ EnvironmentSessionProvider = (*environmentProviderStub)(nil)

func templateFromFields(fields map[string]any) openaiapi.Template {
	data, _ := json.Marshal(fields)
	var copyFields map[string]any
	_ = json.Unmarshal(data, &copyFields)
	name, _ := copyFields[testNameField].(string)
	return openaiapi.Template{ID: testTemplateID, Name: name, Config: openaiapi.ObservableEnvironment(copyFields, false)}
}
func (p *environmentProviderStub) CreateTemplate(_ context.Context, fields map[string]any) (openaiapi.Template, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.creates++
	p.remote = templateFromFields(fields)
	if p.loseCreate {
		return openaiapi.Template{}, errors.New("create response lost")
	}
	return p.remote, nil
}
func (p *environmentProviderStub) GetTemplate(_ context.Context, id string) (openaiapi.Template, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.remote.ID != id {
		return openaiapi.Template{}, openaiapi.ErrNotFound
	}
	return p.remote, nil
}
func (p *environmentProviderStub) UpdateTemplate(_ context.Context, _ string, fields map[string]any) (openaiapi.Template, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.updates++
	p.remote = templateFromFields(fields)
	if p.loseUpdate {
		return openaiapi.Template{}, errors.New("update response lost")
	}
	return p.remote, nil
}
func (p *environmentProviderStub) FindTemplateCandidates(_ context.Context, name string) ([]string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.remote.Name == name {
		return []string{p.remote.ID}, nil
	}
	return nil, nil
}
func (p *environmentProviderStub) DeleteTemplate(_ context.Context, _ string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.deletes++
	if p.deleteFailure {
		return errors.New("delete temporarily unavailable")
	}
	p.remote = openaiapi.Template{}
	return nil
}
func (p *environmentProviderStub) GetHostedRuntime(_ context.Context, id string) (openaiapi.HostedRuntime, error) {
	return openaiapi.HostedRuntime{ID: id, Status: p.runtimeStatus}, nil
}

var _ = Describe("Environment recipes and frozen Sessions", func() {
	var environment *agentsv1alpha1.Environment
	var agent *agentsv1alpha1.Agent
	var agents *agentProviderStub
	var controller *EnvironmentReconciler
	var provider *environmentProviderStub
	var sessions []*agentsv1alpha1.Session
	var namespace string
	step := func() error {
		_, err := controller.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(environment)})
		return err
	}
	read := func() *agentsv1alpha1.Environment {
		var current agentsv1alpha1.Environment
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(environment), &current)).To(Succeed())
		return &current
	}
	ready := func() {
		Expect(step()).To(Succeed())
		Expect(step()).To(Succeed())
		Expect(step()).To(Succeed())
		Expect(meta.FindStatusCondition(read().Status.Conditions, readyCondition).Status).To(Equal(metav1.ConditionTrue))
	}
	newSession := func() *agentsv1alpha1.Session {
		s := &agentsv1alpha1.Session{ObjectMeta: metav1.ObjectMeta{GenerateName: "environment-session-", Namespace: namespace}, Spec: agentsv1alpha1.SessionSpec{
			AgentRef: &agentsv1alpha1.AgentReference{Name: agent.Name}, EnvironmentRef: &agentsv1alpha1.AgentReference{Name: environment.Name},
			SpendControl: &agentsv1alpha1.SessionSpendControl{Mode: spendModeUnlimited}}}
		Expect(k8sClient.Create(ctx, s)).To(Succeed())
		sessions = append(sessions, s)
		return s
	}
	BeforeEach(func() {
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "environment-test-"}}
		Expect(k8sClient.Create(ctx, ns)).To(Succeed())
		namespace = ns.Name
		agent = &agentsv1alpha1.Agent{ObjectMeta: metav1.ObjectMeta{GenerateName: "environment-agent-", Namespace: namespace},
			Spec: agentsv1alpha1.AgentSpec{Model: openaiapi.SessionModel, DeletionPolicy: deletePolicy}}
		Expect(k8sClient.Create(ctx, agent)).To(Succeed())
		agents = &agentProviderStub{}
		agentController := &AgentReconciler{Client: k8sClient, APIReader: k8sClient, OpenAI: agents, OwnerScope: environmentTestScope}
		for range 3 {
			_, err := agentController.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(agent)})
			Expect(err).NotTo(HaveOccurred())
		}
		environment = &agentsv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{GenerateName: "profile-", Namespace: namespace}, Spec: agentsv1alpha1.EnvironmentSpec{Mode: openaiHostedMode, DeletionPolicy: deletePolicy, OpenAIHosted: &agentsv1alpha1.HostedEnvironment{Network: agentsv1alpha1.HostedNetwork{Access: testDisabledNetwork}}}}
		Expect(k8sClient.Create(ctx, environment)).To(Succeed())
		provider = &environmentProviderStub{sessions: &sessionProviderStub{}, runtimeStatus: testConnectedRuntime}
		controller = &EnvironmentReconciler{Client: k8sClient, APIReader: k8sClient, OpenAI: provider, OwnerScope: environmentTestScope}
		sessions = nil
	})
	AfterEach(func() {
		for _, s := range sessions {
			var current agentsv1alpha1.Session
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(s), &current)).To(Succeed())
			current.Finalizers = nil
			Expect(k8sClient.Update(ctx, &current)).To(Succeed())
			if current.DeletionTimestamp.IsZero() {
				Expect(k8sClient.Delete(ctx, &current)).To(Succeed())
			}
		}
		current := read()
		current.Finalizers = nil
		Expect(k8sClient.Update(ctx, current)).To(Succeed())
		if current.DeletionTimestamp.IsZero() {
			Expect(k8sClient.Delete(ctx, current)).To(Succeed())
		}
		var currentAgent agentsv1alpha1.Agent
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(agent), &currentAgent)).To(Succeed())
		currentAgent.Finalizers = nil
		Expect(k8sClient.Update(ctx, &currentAgent)).To(Succeed())
		Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, &currentAgent))).To(Succeed())
	})
	It("keeps explicit policy, redacted observation, quiet reconciliation and status isolation", func() {
		Expect(environment.Spec.OpenAIHosted.ContainerSize).To(Equal("medium"))
		Expect(environment.Spec.DeletionPolicy).To(Equal(agentsv1alpha1.DeletionPolicyDelete))
		ready()
		version := read().ResourceVersion
		Expect(step()).To(Succeed())
		Expect(read().ResourceVersion).To(Equal(version))
		Expect(provider.creates).To(Equal(1))
		Expect(provider.updates).To(BeZero())
		Expect(meta.FindStatusCondition(read().Status.Conditions, "ConfidentialContentVerified").Status).To(Equal(metav1.ConditionUnknown))
		current := read()
		current.Status.OpenAITemplateID = "forged"
		Expect(k8sClient.Update(ctx, current)).To(Succeed())
		Expect(read().Status.OpenAITemplateID).To(Equal(testTemplateID))
	})
	It("updates Session-only size without rewriting the hosted template", func() {
		ready()
		current := read()
		current.Spec.OpenAIHosted.ContainerSize = testSmallHostedSize
		Expect(k8sClient.Update(ctx, current)).To(Succeed())
		Expect(step()).To(Succeed())
		Expect(provider.updates).To(BeZero())
		Expect(meta.FindStatusCondition(read().Status.Conditions, readyCondition).ObservedGeneration).To(Equal(current.Generation))
	})
	It("never adopts a single name candidate after committed creation loses its response", func() {
		provider.loseCreate = true
		Expect(step()).To(Succeed())
		Expect(step()).NotTo(Succeed())
		Expect(step()).To(Succeed())
		Expect(step()).To(Succeed())
		Expect(provider.creates).To(Equal(1))
		Expect(read().Status.AuditCandidateIDs).To(Equal([]string{testTemplateID}))
		Expect(read().Status.OpenAITemplateID).To(BeEmpty())
		Expect(k8sClient.Delete(ctx, read())).To(Succeed())
		Expect(step()).To(Succeed())
		Expect(read().Finalizers).To(ContainElement(environmentFinalizer))
		Expect(provider.deletes).To(BeZero())
	})
	It("reapplies protected values after source rotation without putting values in status", func() {
		secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: openaiapi.EnvironmentSecretName, Namespace: namespace}, Type: corev1.SecretTypeOpaque, Data: map[string][]byte{testProtectedKey: []byte("protected-placeholder-one")}}
		Expect(k8sClient.Create(ctx, secret)).To(Succeed())
		environment.Spec.OpenAIHosted.Env = []agentsv1alpha1.HostedVariable{{Name: "TEST_VALUE", SecretKey: testProtectedKey}}
		Expect(k8sClient.Update(ctx, environment)).To(Succeed())
		ready()
		before := read().Status.ConfigurationDigest
		secret.Data[testProtectedKey] = []byte("protected-placeholder-two")
		Expect(k8sClient.Update(ctx, secret)).To(Succeed())
		Expect(step()).To(Succeed())
		Expect(step()).To(Succeed())
		current := read()
		Expect(current.Status.ConfigurationDigest).NotTo(Equal(before))
		Expect(current.Status.SourceVersion.ResourceVersion).To(Equal(secret.ResourceVersion))
		Expect(provider.updates).To(Equal(1))
		data, err := json.Marshal(current)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(data)).NotTo(ContainSubstring("protected-placeholder"))
	})
	It("blocks hosted updates while the protected source Secret is deleting", func() {
		secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: openaiapi.EnvironmentSecretName, Namespace: namespace,
			Finalizers: []string{otherTestFinalizer}}, Type: corev1.SecretTypeOpaque,
			Data: map[string][]byte{testProtectedKey: []byte("protected-placeholder")}}
		Expect(k8sClient.Create(ctx, secret)).To(Succeed())
		DeferCleanup(func() {
			var current corev1.Secret
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(secret), &current)).To(Succeed())
			current.Finalizers = nil
			Expect(k8sClient.Update(ctx, &current)).To(Succeed())
		})
		environment.Spec.OpenAIHosted.Env = []agentsv1alpha1.HostedVariable{{Name: "TEST_VALUE", SecretKey: testProtectedKey}}
		Expect(k8sClient.Update(ctx, environment)).To(Succeed())
		ready()
		Expect(k8sClient.Delete(ctx, secret)).To(Succeed())
		Expect(step()).NotTo(Succeed())
		Expect(meta.FindStatusCondition(read().Status.Conditions, readyCondition).Reason).To(Equal("ProtectedSourceUnavailable"))
		Expect(provider.updates).To(BeZero())
		Expect(provider.creates).To(Equal(1))
	})
	It("reobserves a lost update and preserves full replacement semantics", func() {
		ready()
		current := read()
		current.Spec.OpenAIHosted.Desktop = true
		Expect(k8sClient.Update(ctx, current)).To(Succeed())
		provider.loseUpdate = true
		Expect(step()).NotTo(Succeed())
		provider.loseUpdate = false
		Expect(step()).To(Succeed())
		Expect(step()).To(Succeed())
		Expect(provider.creates).To(Equal(1))
		Expect(provider.updates).To(Equal(2))
		Expect(meta.FindStatusCondition(read().Status.Conditions, readyCondition).Status).To(Equal(metav1.ConditionTrue))
	})
	It("rejects mismatched known identity and retries deletion without releasing obligations", func() {
		ready()
		provider.remote.Name = "unowned"
		Expect(step()).To(Succeed())
		Expect(meta.FindStatusCondition(read().Status.Conditions, readyCondition).Reason).To(Equal("OwnershipMismatch"))
		Expect(provider.updates).To(BeZero())
		provider.remote.Name = read().Status.CreationName
		Expect(k8sClient.Delete(ctx, read())).To(Succeed())
		provider.deleteFailure = true
		Expect(step()).NotTo(Succeed())
		Expect(read().Finalizers).To(ContainElement(environmentFinalizer))
		provider.deleteFailure = false
		Expect(step()).To(Succeed())
		Expect(step()).To(Succeed())
		// The finalizer is removed only after a separate absence observation.
		environment = &agentsv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{GenerateName: "cleanup-placeholder-", Namespace: namespace}, Spec: agentsv1alpha1.EnvironmentSpec{Mode: agentsv1alpha1.EnvironmentModeNone, DeletionPolicy: deletePolicy}}
		Expect(k8sClient.Create(ctx, environment)).To(Succeed())
	})
	It("freezes hosted settings and waits for runtime connected without creating a Pod", func() {
		ready()
		s := newSession()
		configuration := &dependencyScenarioSessionProvider{configuredSessionStub: configuredSessionStub{lifecycle: provider.sessions}, agents: agents}
		r := &SessionReconciler{Client: k8sClient, APIReader: k8sClient, Scheme: k8sClient.Scheme(), OpenAI: provider.sessions,
			Agents: agents, Environments: provider, Configuration: configuration, AgentOwnerScope: environmentTestScope}
		sessionStep := func() {
			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(s)})
			Expect(err).NotTo(HaveOccurred())
		}
		provider.runtimeStatus = "provisioning"
		sessionStep()
		sessionStep()
		var first agentsv1alpha1.Session
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(s), &first)).To(Succeed())
		Expect(meta.IsStatusConditionTrue(first.Status.Conditions, readyCondition)).To(BeFalse())
		snapshot := first.Status.EnvironmentSnapshot.DeepCopy()
		provider.runtimeStatus = testConnectedRuntime
		sessionStep()
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(s), &first)).To(Succeed())
		Expect(meta.IsStatusConditionTrue(first.Status.Conditions, readyCondition)).To(BeTrue())
		Expect(first.Status.PodName).To(BeEmpty())
		current := read()
		current.Spec.OpenAIHosted.ContainerSize = testSmallHostedSize
		Expect(k8sClient.Update(ctx, current)).To(Succeed())
		Expect(step()).To(Succeed())
		Expect(step()).To(Succeed())
		sessionStep()
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(s), &first)).To(Succeed())
		Expect(first.Status.EnvironmentSnapshot).To(Equal(snapshot))
		Expect(provider.sessions.creates).To(Equal(1))
		Expect(k8sClient.Delete(ctx, current)).To(Succeed())
		Expect(step()).To(Succeed())
		Expect(meta.FindStatusCondition(read().Status.Conditions, readyCondition).Reason).To(Equal("EnvironmentInUse"))
	})
	It("does not consume a Session creation claim on stale protected sources", func() {
		secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: openaiapi.EnvironmentSecretName, Namespace: namespace}, Type: corev1.SecretTypeOpaque, Data: map[string][]byte{testProtectedKey: []byte("placeholder")}}
		Expect(k8sClient.Create(ctx, secret)).To(Succeed())
		environment.Spec.OpenAIHosted.Env = []agentsv1alpha1.HostedVariable{{Name: "TEST", SecretKey: testProtectedKey}}
		Expect(k8sClient.Update(ctx, environment)).To(Succeed())
		ready()
		secret.Data[testProtectedKey] = []byte("rotated-placeholder")
		Expect(k8sClient.Update(ctx, secret)).To(Succeed())
		s := newSession()
		configuration := &dependencyScenarioSessionProvider{configuredSessionStub: configuredSessionStub{lifecycle: provider.sessions}, agents: agents}
		r := &SessionReconciler{Client: k8sClient, APIReader: k8sClient, OpenAI: provider.sessions,
			Agents: agents, Environments: provider, Configuration: configuration, AgentOwnerScope: environmentTestScope}
		for range 2 {
			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(s)})
			Expect(err).NotTo(HaveOccurred())
		}
		var current agentsv1alpha1.Session
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(s), &current)).To(Succeed())
		Expect(current.Status.CreationAttempted).To(BeFalse())
		Expect(provider.sessions.creates).To(BeZero())
	})
	It("persists a frozen Environment through ambiguous Session creation and reference mutation", func() {
		ready()
		s := newSession()
		provider.sessions.loseResponse = true
		configuration := &dependencyScenarioSessionProvider{configuredSessionStub: configuredSessionStub{lifecycle: provider.sessions}, agents: agents}
		r := &SessionReconciler{Client: k8sClient, APIReader: k8sClient, OpenAI: provider.sessions,
			Agents: agents, Environments: provider, Configuration: configuration, AgentOwnerScope: environmentTestScope}
		sessionStep := func() error {
			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(s)})
			return err
		}
		Expect(sessionStep()).To(Succeed())
		Expect(sessionStep()).NotTo(Succeed())
		var first agentsv1alpha1.Session
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(s), &first)).To(Succeed())
		snapshot := first.Status.EnvironmentSnapshot.DeepCopy()
		current := read()
		current.Spec.OpenAIHosted.ContainerSize = testSmallHostedSize
		Expect(k8sClient.Update(ctx, current)).To(Succeed())
		Expect(step()).To(Succeed())
		Expect(step()).To(Succeed())
		Expect(sessionStep()).To(Succeed())
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(s), &first)).To(Succeed())
		Expect(first.Status.EnvironmentSnapshot).To(Equal(snapshot))
		Expect(meta.IsStatusConditionTrue(first.Status.Conditions, readyCondition)).To(BeTrue())
		Expect(provider.sessions.creates).To(Equal(1))
		first.Spec.EnvironmentRef.Name = "another-environment"
		Expect(apierrors.IsInvalid(k8sClient.Update(ctx, &first))).To(BeTrue())
		Expect(sessionStep()).To(Succeed())
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(s), &first)).To(Succeed())
		Expect(meta.IsStatusConditionTrue(first.Status.Conditions, "ConfigurationCurrent")).To(BeTrue())
		Expect(first.Status.EnvironmentSnapshot).To(Equal(snapshot))
	})
	It("allows only the winning optimistic creation claim to POST", func() {
		Expect(step()).To(Succeed())
		barrier := &barrierReader{Reader: k8sClient, arrived: make(chan struct{}, 2), release: make(chan struct{})}
		controller.APIReader = barrier
		done := make(chan error, 2)
		for range 2 {
			go func() { defer GinkgoRecover(); done <- step() }()
		}
		<-barrier.arrived
		<-barrier.arrived
		close(barrier.release)
		succeeded := 0
		for range 2 {
			if <-done == nil {
				succeeded++
			}
		}
		controller.APIReader = k8sClient
		Expect(succeeded).To(Equal(1))
		Expect(provider.creates).To(Equal(1))
		Expect(read().Status.CreationAttempted).To(BeTrue())
	})
	It("keeps an unresolved claim after identity persistence fails", func() {
		Expect(step()).To(Succeed())
		controller.Client = &interceptClient{Client: k8sClient, beforeStatusPatch: func(obj client.Object) error {
			environment, ok := obj.(*agentsv1alpha1.Environment)
			if ok && environment.Status.OpenAITemplateID != "" {
				return errors.New("identity write unavailable")
			}
			return nil
		}}
		Expect(step()).NotTo(Succeed())
		controller.Client = k8sClient
		Expect(step()).To(Succeed())
		Expect(provider.creates).To(Equal(1))
		Expect(read().Status.OpenAITemplateID).To(BeEmpty())
		Expect(read().Status.AuditCandidateIDs).To(Equal([]string{testTemplateID}))
	})
	It("blocks none without the durable gateway", func() {
		Expect(k8sClient.Delete(ctx, environment)).To(Succeed())
		environment = &agentsv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{GenerateName: "none-profile-", Namespace: namespace}, Spec: agentsv1alpha1.EnvironmentSpec{Mode: agentsv1alpha1.EnvironmentModeNone, DeletionPolicy: deletePolicy}}
		Expect(k8sClient.Create(ctx, environment)).To(Succeed())
		Expect(step()).To(Succeed())
		Expect(meta.FindStatusCondition(read().Status.Conditions, readyCondition).Reason).To(Equal("GatewayRequired"))
		Expect(provider.creates).To(BeZero())
	})
})

var _ = Describe("Environment admission", func() {
	It("requires explicit deletion policy and sandbox pool configuration", func() {
		missingPolicy := &agentsv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{GenerateName: "missing-policy-", Namespace: testNamespace},
			Spec: agentsv1alpha1.EnvironmentSpec{Mode: agentsv1alpha1.EnvironmentModeNone}}
		Expect(k8sClient.Create(ctx, missingPolicy)).NotTo(Succeed())
		valid := &agentsv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{GenerateName: "explicit-policy-", Namespace: testNamespace},
			Spec: agentsv1alpha1.EnvironmentSpec{Mode: agentsv1alpha1.EnvironmentModeNone, DeletionPolicy: retainPolicy}}
		Expect(k8sClient.Create(ctx, valid)).To(Succeed())
		valid.Spec.DeletionPolicy = ""
		Expect(k8sClient.Update(ctx, valid)).NotTo(Succeed())
		Expect(k8sClient.Delete(ctx, valid)).To(Succeed())
		omittedStorage := &agentsv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{GenerateName: "omitted-storage-", Namespace: testNamespace},
			Spec: agentsv1alpha1.EnvironmentSpec{Mode: testSelfHostedMode, DeletionPolicy: deletePolicy, SelfHosted: &agentsv1alpha1.SelfHostedEnvironment{
				SandboxPoolRef: agentsv1alpha1.AgentReference{Name: "codex-go-v1"}, Workspace: agentsv1alpha1.SelfHostedWorkspace{DeletionPolicy: deletePolicy}}}}
		Expect(k8sClient.Create(ctx, omittedStorage)).To(Succeed())
		Expect(k8sClient.Delete(ctx, omittedStorage)).To(Succeed())
	})
	It("rejects branch conflicts, floating versions, credential values and illegal defaults", func() {
		invalid := []map[string]any{
			{testModeField: testSelfHostedMode},
			{testModeField: testNoneMode, testHostedBranch: map[string]any{testNetworkField: map[string]any{testAccessField: testDisabledNetwork}}},
			{testModeField: openaiHostedMode, testHostedBranch: map[string]any{testNetworkField: map[string]any{testAccessField: "restricted"}}},
			{testModeField: openaiHostedMode, testHostedBranch: map[string]any{"containerSize": "huge", testNetworkField: map[string]any{testAccessField: testDisabledNetwork}}},
			{testModeField: openaiHostedMode, testHostedBranch: map[string]any{testNetworkField: map[string]any{testAccessField: testDisabledNetwork}, "env": []any{map[string]any{testNameField: "OPENAI_API_KEY", "secretKey": "key"}}}},
			{testModeField: openaiHostedMode, testHostedBranch: map[string]any{testNetworkField: map[string]any{testAccessField: testDisabledNetwork}, "skills": []any{map[string]any{_wireType: string(agentsv1alpha1.HostedSkillTypeSkillReference), "skillID": "skill", "version": "latest"}}}},
			{testModeField: openaiHostedMode, testHostedBranch: map[string]any{testNetworkField: map[string]any{testAccessField: testDisabledNetwork}, "files": []any{map[string]any{"type": string(agentsv1alpha1.HostedFileTypeInline), "path": "/workspace/file", "fileID": "file"}}}},
		}
		for _, spec := range invalid {
			obj := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "agents.softcane.io/v1alpha1", "kind": "Environment", "metadata": map[string]any{"generateName": "invalid-profile-", "namespace": testNamespace}, "spec": spec}}
			Expect(k8sClient.Create(ctx, obj)).NotTo(Succeed())
		}
		obj := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "agents.softcane.io/v1alpha1", "kind": "Environment", "metadata": map[string]any{"generateName": "secret-profile-", "namespace": testNamespace}, "spec": map[string]any{testModeField: openaiHostedMode, testHostedBranch: map[string]any{testNetworkField: map[string]any{testAccessField: testDisabledNetwork}, "env": map[string]any{"TOKEN": testForbiddenValue}}}}}
		Expect(k8sClient.Create(ctx, obj, &client.CreateOptions{FieldValidation: metav1.FieldValidationStrict})).NotTo(Succeed())
	})
	It("keeps mode immutable", func() {
		environment := &agentsv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{GenerateName: "immutable-profile-", Namespace: testNamespace}, Spec: agentsv1alpha1.EnvironmentSpec{Mode: agentsv1alpha1.EnvironmentModeNone, DeletionPolicy: deletePolicy}}
		Expect(k8sClient.Create(ctx, environment)).To(Succeed())
		environment.Spec.Mode = openaiHostedMode
		environment.Spec.OpenAIHosted = &agentsv1alpha1.HostedEnvironment{Network: agentsv1alpha1.HostedNetwork{Access: testDisabledNetwork}}
		Expect(k8sClient.Update(ctx, environment)).NotTo(Succeed())
		environment.Spec.Mode = testNoneMode
		environment.Spec.OpenAIHosted = nil
		Expect(k8sClient.Delete(ctx, environment)).To(Succeed())
	})
	It("rejects self-hosted settings without an explicit workspace policy", func() {
		environment := &agentsv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{GenerateName: "missing-workspace-policy-", Namespace: testNamespace},
			Spec: agentsv1alpha1.EnvironmentSpec{Mode: testSelfHostedMode, DeletionPolicy: deletePolicy,
				SelfHosted: &agentsv1alpha1.SelfHostedEnvironment{SandboxPoolRef: agentsv1alpha1.AgentReference{Name: "codex-go-v1"}}}}
		Expect(k8sClient.Create(ctx, environment)).NotTo(Succeed())
	})
})

// verifyEnvironmentDependencyWatches runs against the suite's registered manager.
func verifyEnvironmentDependencyWatches(namespace string) {
	agent := &agentsv1alpha1.Agent{ObjectMeta: metav1.ObjectMeta{GenerateName: "watch-environment-agent-", Namespace: namespace},
		Spec: agentsv1alpha1.AgentSpec{Model: openaiapi.SessionModel, DeletionPolicy: deletePolicy}}
	environment := &agentsv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{GenerateName: "watch-environment-", Namespace: namespace}, Spec: agentsv1alpha1.EnvironmentSpec{Mode: openaiHostedMode, DeletionPolicy: deletePolicy, OpenAIHosted: &agentsv1alpha1.HostedEnvironment{Network: agentsv1alpha1.HostedNetwork{Access: testDisabledNetwork}}}}
	session := &agentsv1alpha1.Session{ObjectMeta: metav1.ObjectMeta{GenerateName: "watch-environment-session-", Namespace: namespace}, Spec: agentsv1alpha1.SessionSpec{
		AgentRef: &agentsv1alpha1.AgentReference{Name: agent.Name}, EnvironmentRef: &agentsv1alpha1.AgentReference{Name: environment.Name},
		SpendControl: &agentsv1alpha1.SessionSpendControl{Mode: spendModeUnlimited}}}
	Expect(k8sClient.Create(ctx, agent)).To(Succeed())
	Expect(k8sClient.Create(ctx, environment)).To(Succeed())
	Eventually(func() bool {
		if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(agent), agent); err != nil {
			return false
		}
		return meta.IsStatusConditionTrue(agent.Status.Conditions, readyCondition)
	}, 10*time.Second, 20*time.Millisecond).Should(BeTrue())
	session.Spec.AgentRef.Name = agent.Name
	session.Spec.EnvironmentRef.Name = environment.Name
	Expect(k8sClient.Create(ctx, session)).To(Succeed())
	Eventually(func() bool {
		var current agentsv1alpha1.Session
		return k8sClient.Get(ctx, client.ObjectKeyFromObject(session), &current) == nil && current.Status.EnvironmentSnapshot != nil
	}, 10*time.Second, 20*time.Millisecond).Should(BeTrue())
	Expect(k8sClient.Delete(ctx, environment)).To(Succeed())
	Eventually(func() string {
		var current agentsv1alpha1.Environment
		if k8sClient.Get(ctx, client.ObjectKeyFromObject(environment), &current) != nil {
			return ""
		}
		ready := meta.FindStatusCondition(current.Status.Conditions, readyCondition)
		if ready == nil {
			return ""
		}
		return ready.Reason
	}, 10*time.Second, 20*time.Millisecond).Should(Equal("EnvironmentInUse"))
	Expect(k8sClient.Delete(ctx, session)).To(Succeed())
	Eventually(func() bool {
		var current agentsv1alpha1.Environment
		return client.IgnoreNotFound(k8sClient.Get(ctx, client.ObjectKeyFromObject(environment), &current)) == nil && current.DeletionTimestamp.IsZero()
	}, 10*time.Second, 20*time.Millisecond).Should(BeTrue())
	Expect(k8sClient.Delete(ctx, agent)).To(Succeed())
}

func verifyProtectedEnvironmentWatch(namespace string) {
	environment := &agentsv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{GenerateName: "watch-protected-profile-", Namespace: namespace}, Spec: agentsv1alpha1.EnvironmentSpec{Mode: openaiHostedMode, DeletionPolicy: deletePolicy, OpenAIHosted: &agentsv1alpha1.HostedEnvironment{Network: agentsv1alpha1.HostedNetwork{Access: testDisabledNetwork}, Env: []agentsv1alpha1.HostedVariable{{Name: "TEST", SecretKey: testProtectedKey}}}}}
	Expect(k8sClient.Create(ctx, environment)).To(Succeed())
	Eventually(func() string {
		var current agentsv1alpha1.Environment
		if k8sClient.Get(ctx, client.ObjectKeyFromObject(environment), &current) != nil {
			return ""
		}
		ready := meta.FindStatusCondition(current.Status.Conditions, readyCondition)
		if ready == nil {
			return ""
		}
		return ready.Reason
	}, 10*time.Second, 20*time.Millisecond).Should(Equal("ProtectedSourceUnavailable"))
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: openaiapi.EnvironmentSecretName, Namespace: namespace}, Type: corev1.SecretTypeOpaque, Data: map[string][]byte{testProtectedKey: []byte("placeholder")}}
	Expect(k8sClient.Create(ctx, secret)).To(Succeed())
	Eventually(func() bool {
		var current agentsv1alpha1.Environment
		if k8sClient.Get(ctx, client.ObjectKeyFromObject(environment), &current) != nil {
			return false
		}
		return meta.IsStatusConditionTrue(current.Status.Conditions, readyCondition)
	}, 10*time.Second, 20*time.Millisecond).Should(BeTrue())
	secret.Data[testProtectedKey] = []byte("rotated-placeholder")
	Expect(k8sClient.Update(ctx, secret)).To(Succeed())
	Eventually(func() string {
		var current agentsv1alpha1.Environment
		if k8sClient.Get(ctx, client.ObjectKeyFromObject(environment), &current) != nil || current.Status.SourceVersion == nil {
			return ""
		}
		return current.Status.SourceVersion.ResourceVersion
	}, 10*time.Second, 20*time.Millisecond).Should(Equal(secret.ResourceVersion))
	Expect(k8sClient.Delete(ctx, environment)).To(Succeed())
	Eventually(func() bool {
		var current agentsv1alpha1.Environment
		return client.IgnoreNotFound(k8sClient.Get(ctx, client.ObjectKeyFromObject(environment), &current)) == nil && current.DeletionTimestamp.IsZero()
	}, 10*time.Second, 20*time.Millisecond).Should(BeTrue())
	Expect(k8sClient.Delete(ctx, secret)).To(Succeed())
}
