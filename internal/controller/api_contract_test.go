package controller

import (
	"encoding/json"
	"reflect"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	_contractFunctionSchema            = "function parameters"
	_contractJSONSchemaType            = "json_schema"
	_contractEnvironmentCredentialType = "environment_variable"
)

var _ = Describe("Generated API contract admission", func() {
	var namespace string
	BeforeEach(func() {
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "api-contract-"}}
		Expect(shippedK8sClient.Create(ctx, ns)).To(Succeed())
		namespace = ns.Name
	})
	newSession := func() *agentsv1alpha1.Session {
		return &agentsv1alpha1.Session{ObjectMeta: metav1.ObjectMeta{GenerateName: "contract-", Namespace: namespace}, Spec: agentsv1alpha1.SessionSpec{
			AgentRef: &agentsv1alpha1.AgentReference{Name: "contract-agent"}, EnvironmentRef: &agentsv1alpha1.AgentReference{Name: "contract-environment"}, SpendControl: &agentsv1alpha1.SessionSpendControl{Mode: agentsv1alpha1.SessionSpendModeUnlimited},
		}}
	}
	It("rejects invalid DNS references and unsafe or unclean workspace paths", func() {
		for _, name := range []string{"a..b", "-agent", "agent-", "agent_.name", "agent."} {
			p := &agentsv1alpha1.Profile{ObjectMeta: metav1.ObjectMeta{GenerateName: "invalid-ref-", Namespace: namespace}, Spec: agentsv1alpha1.ProfileSpec{AgentRef: agentsv1alpha1.AgentReference{Name: name}, EnvironmentRef: agentsv1alpha1.AgentReference{Name: "contract-environment"}, SpendControl: agentsv1alpha1.SessionSpendControl{Mode: agentsv1alpha1.SessionSpendModeUnlimited}}}
			Expect(apierrors.IsInvalid(shippedK8sClient.Create(ctx, p))).To(BeTrue(), name)
		}
		for _, directory := range []string{"/", "/etc", "/etc/project", "/workspace/../etc", "/workspace//project", "/workspace/.", "/workspace/", "relative"} {
			e := &agentsv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{GenerateName: "invalid-path-", Namespace: namespace}, Spec: agentsv1alpha1.EnvironmentSpec{Mode: agentsv1alpha1.EnvironmentModeSelfHosted, DeletionPolicy: agentsv1alpha1.DeletionPolicyDelete, SelfHosted: &agentsv1alpha1.SelfHostedEnvironment{SandboxPoolRef: agentsv1alpha1.AgentReference{Name: "pool"}, Workspace: agentsv1alpha1.SelfHostedWorkspace{Directory: directory, DeletionPolicy: agentsv1alpha1.DeletionPolicyRetain}}}}
			Expect(apierrors.IsInvalid(shippedK8sClient.Create(ctx, e))).To(BeTrue(), directory)
		}
		e := &agentsv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{GenerateName: "default-path-", Namespace: namespace}, Spec: agentsv1alpha1.EnvironmentSpec{Mode: agentsv1alpha1.EnvironmentModeSelfHosted, DeletionPolicy: agentsv1alpha1.DeletionPolicyDelete, SelfHosted: &agentsv1alpha1.SelfHostedEnvironment{SandboxPoolRef: agentsv1alpha1.AgentReference{Name: "pool.name"}, Workspace: agentsv1alpha1.SelfHostedWorkspace{DeletionPolicy: agentsv1alpha1.DeletionPolicyRetain}}}}
		Expect(shippedK8sClient.Create(ctx, e)).To(Succeed())
		Expect(e.Spec.SelfHosted.Workspace.Directory).To(Equal(defaultWorkspace))
	})
	It("rejects reserved credential names and malformed host items but accepts host set reordering", func() {
		credential := func() *agentsv1alpha1.VaultCredential {
			return &agentsv1alpha1.VaultCredential{ObjectMeta: metav1.ObjectMeta{GenerateName: "credential-", Namespace: namespace}, Spec: agentsv1alpha1.VaultCredentialSpec{VaultRef: agentsv1alpha1.AgentReference{Name: "vault"}, Name: "token", DeletionPolicy: agentsv1alpha1.DeletionPolicyDelete, Auth: agentsv1alpha1.VaultCredentialAuth{Type: _contractEnvironmentCredentialType, EnvironmentVariable: &agentsv1alpha1.VaultEnvironmentVariable{VariableName: "SERVICE_TOKEN", SecretValueSecretRef: agentsv1alpha1.CredentialSecretReference{Name: "codex-vault", Key: "token"}, Networking: agentsv1alpha1.VaultCredentialNetworking{Type: "limited", AllowedHosts: []string{"b.example.com", "a.example.com"}}}}}}
		}
		for _, name := range []string{"OPENAI_API_KEY", "http_proxy", "CODEX_REMOTE_URL", "SSL_CERT_FILE"} {
			c := credential()
			c.Spec.Auth.EnvironmentVariable.VariableName = name
			Expect(apierrors.IsInvalid(shippedK8sClient.Create(ctx, c))).To(BeTrue(), name)
		}
		for _, host := range []string{"A.example.com", "a..example.com", "a.-example.com", "a.example-.com", "https://a.example.com", "*.example.com", strings.Repeat("x", 257)} {
			c := credential()
			c.Spec.Auth.EnvironmentVariable.Networking.AllowedHosts = []string{host}
			Expect(apierrors.IsInvalid(shippedK8sClient.Create(ctx, c))).To(BeTrue(), host)
		}
		c := credential()
		Expect(shippedK8sClient.Create(ctx, c)).To(Succeed())
		c.Spec.Auth.EnvironmentVariable.Networking.AllowedHosts = []string{"a.example.com", "b.example.com"}
		Expect(shippedK8sClient.Update(ctx, c)).To(Succeed())
	})
	It("permits preclaim edits and mutable settings after claiming while freezing creation-only fields", func() {
		s := newSession()
		instructions := "before"
		s.Spec.Agent = &agentsv1alpha1.SessionAgentOverrides{Instructions: &instructions}
		Expect(shippedK8sClient.Create(ctx, s)).To(Succeed())
		s.Spec.AgentRef.Name = "contract-other-agent"
		Expect(shippedK8sClient.Update(ctx, s)).To(Succeed())
		s.Status.CreationAttempted = true
		Expect(shippedK8sClient.Status().Update(ctx, s)).To(Succeed())
		changed := s.DeepCopy()
		changed.Spec.AgentRef.Name = "third-agent"
		Expect(apierrors.IsInvalid(shippedK8sClient.Update(ctx, changed))).To(BeTrue())
		changed = s.DeepCopy()
		after := "after"
		changed.Spec.Agent.Instructions = &after
		Expect(apierrors.IsInvalid(shippedK8sClient.Update(ctx, changed))).To(BeTrue())
		changed = s.DeepCopy()
		model, effort, tier := "gpt-6-luna", "high", "priority"
		changed.Spec.Agent.Model = &model
		changed.Spec.Agent.Reasoning = &agentsv1alpha1.AgentReasoning{Effort: &effort}
		changed.Spec.Agent.ServiceTier = &tier
		metadata := map[string]string{"purpose": "updated"}
		changed.Spec.Metadata = &metadata
		changed.Spec.SpendControl = &agentsv1alpha1.SessionSpendControl{Mode: agentsv1alpha1.SessionSpendModeLimited, LimitCents: new(int64(200))}
		Expect(shippedK8sClient.Update(ctx, changed)).To(Succeed())
		changed.Spec.Agent.Reasoning = nil
		changed.Spec.Agent.ResetReasoning = true
		changed.Spec.Agent.ServiceTier = nil
		changed.Spec.Agent.ResetServiceTier = true
		Expect(shippedK8sClient.Update(ctx, changed)).To(Succeed())
	})
	It("protects frozen remote configuration when CEL admits opaque JSON key replacements", func() {
		for _, field := range []string{_contractFunctionSchema, "text schema"} {
			f := newDependencyScenarioFixture()
			f.ready()
			f.sessionController.Gateway = &gatewayStub{}
			s := f.newSession(f.namespace)
			before := apiextensionsv1.JSON{Raw: []byte(`{"type":"object","title":"Before"}`)}
			s.Spec.Agent = &agentsv1alpha1.SessionAgentOverrides{}
			if field == _contractFunctionSchema {
				tools := []agentsv1alpha1.AgentTool{{Type: "function", Function: &agentsv1alpha1.AgentFunction{Name: "contract_lookup", Description: "Contract lookup", Parameters: before}}}
				s.Spec.Agent.Tools = &tools
			} else {
				s.Spec.Agent.Text = &agentsv1alpha1.AgentText{Format: &agentsv1alpha1.AgentTextFormat{Type: _contractJSONSchemaType, Schema: &before}}
			}
			Expect(shippedK8sClient.Update(ctx, s)).To(Succeed())
			for range 3 {
				f.sessionStep(s)
			}
			originalSpec := s.Spec.DeepCopy()
			remoteID := s.Status.OpenAISessionID
			beforeRemote, err := json.Marshal(f.sessions.remote.AgentConfiguration)
			Expect(err).NotTo(HaveOccurred())
			Expect(remoteID).NotTo(BeEmpty())
			after := apiextensionsv1.JSON{Raw: []byte(`{"type":"object","description":"After"}`)}
			if field == _contractFunctionSchema {
				(*s.Spec.Agent.Tools)[0].Function.Parameters = after
			} else {
				s.Spec.Agent.Text.Format.Schema = &after
			}
			// Kubernetes CEL equality currently admits some same-sized unknown-key
			// replacements in preserved JSON objects. The controller remains authoritative.
			Expect(shippedK8sClient.Update(ctx, s)).To(Succeed(), field)
			f.sessionStep(s)
			Expect(meta.FindStatusCondition(s.Status.Conditions, "ConfigurationCurrent").Reason).To(Equal("RequiresNewSession"))
			Expect(s.Status.OpenAISessionID).To(Equal(remoteID))
			Expect(f.sessions.creates).To(Equal(1))
			Expect(f.configuration.updates).To(BeZero())
			afterRemote, err := json.Marshal(f.sessions.remote.AgentConfiguration)
			Expect(err).NotTo(HaveOccurred())
			Expect(reflect.DeepEqual(beforeRemote, afterRemote)).To(BeTrue())
			s.Spec = *originalSpec
			Expect(shippedK8sClient.Update(ctx, s)).To(Succeed())
			f.sessionStep(s)
			Expect(meta.IsStatusConditionTrue(s.Status.Conditions, "ConfigurationCurrent")).To(BeTrue())
		}
	})
})
