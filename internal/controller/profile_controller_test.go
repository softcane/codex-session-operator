package controller

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

const (
	_profileTestName      = "published"
	_profileTestNamespace = "profile-unit-team"
	_profileMissingState  = "missing dependency"
	_profileStaleState    = "stale dependency"
	_profileDeletingState = "deleting dependency"
	_profileForeignState  = "foreign dependency"
	_profileForeignScope  = "profile-other/project"
	_profileAgentName     = "agent"
	_profileEnvironment   = "environment"
	_profileModel         = "gpt-6-luna"
)

func testProfilePolicy() agentsv1alpha1.ProfileSpec {
	return agentsv1alpha1.ProfileSpec{
		AgentRef:       agentsv1alpha1.AgentReference{Name: _profileAgentName},
		EnvironmentRef: agentsv1alpha1.AgentReference{Name: _profileEnvironment},
		VaultRefs:      []agentsv1alpha1.AgentReference{{Name: dependencyScenarioVaultName}},
		SpendControl:   agentsv1alpha1.SessionSpendControl{Mode: spendModeUnlimited},
	}
}

func setProfileDependencyReady(object client.Object, ready bool) {
	status := metav1.ConditionFalse
	if ready {
		status = metav1.ConditionTrue
	}
	condition := metav1.Condition{Type: readyCondition, Status: status, Reason: "TestObservation", ObservedGeneration: object.GetGeneration()}
	switch value := object.(type) {
	case *agentsv1alpha1.Agent:
		value.Status.OwnerScope, value.Status.OpenAIAgentID = testAgentScope, "profile-test-agent"
		meta.SetStatusCondition(&value.Status.Conditions, condition)
	case *agentsv1alpha1.Environment:
		value.Status.OwnerScope, value.Status.ConfigurationDigest = testAgentScope, "profile-test-environment"
		meta.SetStatusCondition(&value.Status.Conditions, condition)
	case *agentsv1alpha1.Vault:
		value.Status.OwnerScope, value.Status.OpenAIVaultID = testAgentScope, "profile-test-vault"
		meta.SetStatusCondition(&value.Status.Conditions, condition)
	}
}

var _ = Describe("Published Profile readiness with shipped CRDs", func() {
	var profile *agentsv1alpha1.Profile
	var agent *agentsv1alpha1.Agent
	var environment *agentsv1alpha1.Environment
	var vault *agentsv1alpha1.Vault
	var reconciler *ProfileReconciler
	read := func() *agentsv1alpha1.Profile {
		var current agentsv1alpha1.Profile
		Expect(shippedK8sClient.Get(ctx, client.ObjectKeyFromObject(profile), &current)).To(Succeed())
		return &current
	}
	step := func() {
		_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(profile)})
		Expect(err).NotTo(HaveOccurred())
	}
	cleanup := func(object client.Object) {
		DeferCleanup(func() {
			current := object.DeepCopyObject().(client.Object)
			err := shippedK8sClient.Get(ctx, client.ObjectKeyFromObject(object), current)
			if apierrors.IsNotFound(err) {
				return
			}
			Expect(err).NotTo(HaveOccurred())
			current.SetFinalizers(nil)
			Expect(shippedK8sClient.Update(ctx, current)).To(Succeed())
			Expect(client.IgnoreNotFound(shippedK8sClient.Delete(ctx, current))).To(Succeed())
		})
	}
	BeforeEach(func() {
		namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "profile-test-"}}
		Expect(shippedK8sClient.Create(ctx, namespace)).To(Succeed())
		agent = &agentsv1alpha1.Agent{ObjectMeta: metav1.ObjectMeta{Name: _profileAgentName, Namespace: namespace.Name},
			Spec: agentsv1alpha1.AgentSpec{Model: _profileModel, DeletionPolicy: deletePolicy}}
		environment = &agentsv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: _profileEnvironment, Namespace: namespace.Name},
			Spec: agentsv1alpha1.EnvironmentSpec{Mode: agentsv1alpha1.EnvironmentModeNone, DeletionPolicy: deletePolicy}}
		vault = &agentsv1alpha1.Vault{ObjectMeta: metav1.ObjectMeta{Name: dependencyScenarioVaultName, Namespace: namespace.Name},
			Spec: agentsv1alpha1.VaultSpec{DeletionPolicy: deletePolicy}}
		for _, object := range []client.Object{agent, environment, vault} {
			Expect(shippedK8sClient.Create(ctx, object)).To(Succeed())
			cleanup(object)
			setProfileDependencyReady(object, true)
			Expect(shippedK8sClient.Status().Update(ctx, object)).To(Succeed())
		}
		profile = &agentsv1alpha1.Profile{ObjectMeta: metav1.ObjectMeta{Name: _profileTestName, Namespace: namespace.Name}, Spec: testProfilePolicy()}
		Expect(shippedK8sClient.Create(ctx, profile)).To(Succeed())
		cleanup(profile)
		reconciler = &ProfileReconciler{Client: shippedK8sClient, APIReader: shippedK8sClient, OwnerScope: testAgentScope}
	})

	It("records safe dependency identities without owning or creating execution resources and stays quiet", func() {
		step()
		current := read()
		Expect(current.Status.ObservedGeneration).To(Equal(current.Generation))
		Expect(current.Status.PolicyDigest).To(Equal(agentsv1alpha1.ProfilePolicyDigest(current.Spec)))
		Expect(current.Status.Agent).To(Equal(&agentsv1alpha1.ProfileDependencyStatus{Name: agent.Name, UID: string(agent.UID), Generation: agent.Generation}))
		Expect(current.Status.Environment.UID).To(Equal(string(environment.UID)))
		Expect(current.Status.Vaults).To(Equal([]agentsv1alpha1.ProfileDependencyStatus{{Name: vault.Name, UID: string(vault.UID), Generation: vault.Generation}}))
		Expect(meta.IsStatusConditionTrue(current.Status.Conditions, readyCondition)).To(BeTrue())
		Expect(current.Finalizers).To(BeEmpty())
		Expect(current.OwnerReferences).To(BeEmpty())
		var sessions agentsv1alpha1.SessionList
		Expect(shippedK8sClient.List(ctx, &sessions, client.InNamespace(profile.Namespace))).To(Succeed())
		Expect(sessions.Items).To(BeEmpty())
		step()
		Expect(read().ResourceVersion).To(Equal(current.ResourceVersion))
	})

	DescribeTable("withholds readiness for missing, stale, deleting and foreign dependencies",
		func(kind, state string) {
			var object client.Object
			switch kind {
			case testAgentKind:
				object = agent
			case dependencyScenarioEnvironment:
				object = environment
			case dependencyScenarioVault:
				object = vault
			}
			step()
			switch state {
			case _profileMissingState:
				Expect(shippedK8sClient.Delete(ctx, object)).To(Succeed())
			case _profileStaleState:
				setProfileDependencyReady(object, true)
				switch value := object.(type) {
				case *agentsv1alpha1.Agent:
					value.Status.Conditions[0].ObservedGeneration--
				case *agentsv1alpha1.Environment:
					value.Status.Conditions[0].ObservedGeneration--
				case *agentsv1alpha1.Vault:
					value.Status.Conditions[0].ObservedGeneration--
				}
				Expect(shippedK8sClient.Status().Update(ctx, object)).To(Succeed())
			case _profileDeletingState:
				object.SetFinalizers([]string{otherTestFinalizer})
				Expect(shippedK8sClient.Update(ctx, object)).To(Succeed())
				Expect(shippedK8sClient.Delete(ctx, object)).To(Succeed())
			case _profileForeignState:
				switch value := object.(type) {
				case *agentsv1alpha1.Agent:
					value.Status.OwnerScope = _profileForeignScope
				case *agentsv1alpha1.Environment:
					value.Status.OwnerScope = _profileForeignScope
				case *agentsv1alpha1.Vault:
					value.Status.OwnerScope = _profileForeignScope
				}
				Expect(shippedK8sClient.Status().Update(ctx, object)).To(Succeed())
			}
			step()
			condition := meta.FindStatusCondition(read().Status.Conditions, readyCondition)
			Expect(condition.Status).To(Equal(metav1.ConditionFalse))
			want := kind + "NotReady"
			if state == _profileForeignState {
				want = "OwnershipMismatch"
			}
			Expect(condition.Reason).To(Equal(want))
		},
		Entry("missing Agent", testAgentKind, _profileMissingState), Entry("stale Agent", testAgentKind, _profileStaleState), Entry("deleting Agent", testAgentKind, _profileDeletingState), Entry("foreign Agent", testAgentKind, _profileForeignState),
		Entry("missing Environment", dependencyScenarioEnvironment, _profileMissingState), Entry("stale Environment", dependencyScenarioEnvironment, _profileStaleState), Entry("deleting Environment", dependencyScenarioEnvironment, _profileDeletingState), Entry("foreign Environment", dependencyScenarioEnvironment, _profileForeignState),
		Entry("missing Vault", dependencyScenarioVault, _profileMissingState), Entry("stale Vault", dependencyScenarioVault, _profileStaleState), Entry("deleting Vault", dependencyScenarioVault, _profileDeletingState), Entry("foreign Vault", dependencyScenarioVault, _profileForeignState))

	It("updates observed generations after same-UID edits without changing policy identity", func() {
		step()
		original := read()
		agent.Spec.Model = updatedTestModel
		Expect(shippedK8sClient.Update(ctx, agent)).To(Succeed())
		step()
		Expect(meta.IsStatusConditionFalse(read().Status.Conditions, readyCondition)).To(BeTrue())
		setProfileDependencyReady(agent, true)
		Expect(shippedK8sClient.Status().Update(ctx, agent)).To(Succeed())
		step()
		current := read()
		Expect(current.Status.PolicyDigest).To(Equal(original.Status.PolicyDigest))
		Expect(current.Status.Agent.UID).To(Equal(original.Status.Agent.UID))
		Expect(current.Status.Agent.Generation).To(BeNumerically(">", original.Status.Agent.Generation))
		Expect(meta.IsStatusConditionTrue(current.Status.Conditions, readyCondition)).To(BeTrue())
	})

	It("does not borrow dependencies from another namespace and keeps same-name Profiles separate", func() {
		namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "profile-other-"}}
		Expect(shippedK8sClient.Create(ctx, namespace)).To(Succeed())
		other := &agentsv1alpha1.Profile{ObjectMeta: metav1.ObjectMeta{Name: profile.Name, Namespace: namespace.Name}, Spec: testProfilePolicy()}
		Expect(shippedK8sClient.Create(ctx, other)).To(Succeed())
		cleanup(other)
		_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(other)})
		Expect(err).NotTo(HaveOccurred())
		Expect(shippedK8sClient.Get(ctx, client.ObjectKeyFromObject(other), other)).To(Succeed())
		Expect(meta.IsStatusConditionFalse(other.Status.Conditions, readyCondition)).To(BeTrue())
		Expect(other.Status.Agent).To(BeNil())
		step()
		Expect(meta.IsStatusConditionTrue(read().Status.Conditions, readyCondition)).To(BeTrue())
	})

	It("reconciles status-only readiness changes and same-name UID replacement through dependency watches", func() {
		manager, err := ctrl.NewManager(cfg, ctrl.Options{Scheme: shippedK8sClient.Scheme(), Metrics: metricsserver.Options{BindAddress: "0"},
			Cache: cache.Options{DefaultNamespaces: map[string]cache.Config{profile.Namespace: {}}}})
		Expect(err).NotTo(HaveOccurred())
		reconciler.Client, reconciler.APIReader = manager.GetClient(), manager.GetAPIReader()
		Expect(reconciler.SetupWithManager(manager)).To(Succeed())
		managerCtx, cancelManager := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- manager.Start(managerCtx) }()
		DeferCleanup(func() {
			cancelManager()
			Eventually(done, 10*time.Second).Should(Receive(BeNil()))
		})
		Eventually(func() bool { return meta.IsStatusConditionTrue(read().Status.Conditions, readyCondition) }, 10*time.Second).Should(BeTrue())
		for _, object := range []client.Object{agent, environment, vault} {
			setProfileDependencyReady(object, false)
			Expect(shippedK8sClient.Status().Update(ctx, object)).To(Succeed())
			Eventually(func() bool { return meta.IsStatusConditionFalse(read().Status.Conditions, readyCondition) }, 5*time.Second).Should(BeTrue())
			setProfileDependencyReady(object, true)
			Expect(shippedK8sClient.Status().Update(ctx, object)).To(Succeed())
			Eventually(func() bool { return meta.IsStatusConditionTrue(read().Status.Conditions, readyCondition) }, 5*time.Second).Should(BeTrue())
		}
		oldUID := agent.UID
		Expect(shippedK8sClient.Delete(ctx, agent)).To(Succeed())
		agent = &agentsv1alpha1.Agent{ObjectMeta: metav1.ObjectMeta{Name: _profileAgentName, Namespace: profile.Namespace},
			Spec: agentsv1alpha1.AgentSpec{Model: _profileModel, DeletionPolicy: deletePolicy}}
		Expect(shippedK8sClient.Create(ctx, agent)).To(Succeed())
		setProfileDependencyReady(agent, true)
		Expect(shippedK8sClient.Status().Update(ctx, agent)).To(Succeed())
		Eventually(func() bool {
			current := read()
			return current.Status.Agent != nil && current.Status.Agent.UID == string(agent.UID) && meta.IsStatusConditionTrue(current.Status.Conditions, readyCondition)
		}, 5*time.Second).Should(BeTrue())
		Expect(agent.UID).NotTo(Equal(oldUID))
	})

	It("deletes without cascading dependencies or an existing Session", func() {
		step()
		session := &agentsv1alpha1.Session{ObjectMeta: metav1.ObjectMeta{Name: "profile-session", Namespace: profile.Namespace},
			Spec: agentsv1alpha1.SessionSpec{AgentRef: &profile.Spec.AgentRef, EnvironmentRef: &profile.Spec.EnvironmentRef,
				SpendControl: &profile.Spec.SpendControl}}
		Expect(shippedK8sClient.Create(ctx, session)).To(Succeed())
		cleanup(session)
		Expect(shippedK8sClient.Delete(ctx, read())).To(Succeed())
		for _, object := range []client.Object{agent, environment, vault, session} {
			Expect(shippedK8sClient.Get(ctx, client.ObjectKeyFromObject(object), object)).To(Succeed())
			Expect(object.GetDeletionTimestamp().IsZero()).To(BeTrue())
		}
	})

	It("admits complete policy and rejects missing refs, invalid spending, duplicate Vaults and unsupported fields", func() {
		valid := profile.DeepCopy()
		valid.ObjectMeta = metav1.ObjectMeta{GenerateName: "profile-valid-", Namespace: profile.Namespace}
		valid.Spec.SpendControl = agentsv1alpha1.SessionSpendControl{Mode: spendModeLimited, LimitCents: new(int64(100))}
		valid.Status.PolicyDigest = "cannot-set-through-spec"
		Expect(shippedK8sClient.Create(ctx, valid)).To(Succeed())
		cleanup(valid)
		Expect(shippedK8sClient.Get(ctx, client.ObjectKeyFromObject(valid), valid)).To(Succeed())
		Expect(valid.Status.PolicyDigest).To(BeEmpty())
		invalid := []agentsv1alpha1.ProfileSpec{
			{}, {AgentRef: profile.Spec.AgentRef, EnvironmentRef: profile.Spec.EnvironmentRef},
			{AgentRef: profile.Spec.AgentRef, EnvironmentRef: profile.Spec.EnvironmentRef, SpendControl: agentsv1alpha1.SessionSpendControl{Mode: spendModeLimited}},
			{AgentRef: profile.Spec.AgentRef, EnvironmentRef: profile.Spec.EnvironmentRef, SpendControl: agentsv1alpha1.SessionSpendControl{Mode: spendModeUnlimited, LimitCents: new(int64(1))}},
			{AgentRef: profile.Spec.AgentRef, EnvironmentRef: profile.Spec.EnvironmentRef, SpendControl: profile.Spec.SpendControl,
				VaultRefs: []agentsv1alpha1.AgentReference{{Name: dependencyScenarioVaultName}, {Name: dependencyScenarioVaultName}}},
		}
		for _, spec := range invalid {
			candidate := &agentsv1alpha1.Profile{ObjectMeta: metav1.ObjectMeta{GenerateName: "profile-invalid-", Namespace: profile.Namespace}, Spec: spec}
			Expect(apierrors.IsInvalid(shippedK8sClient.Create(ctx, candidate))).To(BeTrue())
		}
		for _, field := range []string{testNamespaceField, "instructions"} {
			raw, err := json.Marshal(profile.Spec)
			Expect(err).NotTo(HaveOccurred())
			var spec map[string]any
			Expect(json.Unmarshal(raw, &spec)).To(Succeed())
			if field == testNamespaceField {
				ref, ok := spec["agentRef"].(map[string]any)
				Expect(ok).To(BeTrue())
				ref[field] = "other"
			} else {
				spec[field] = "private input is not Profile policy"
			}
			candidate := &unstructured.Unstructured{Object: map[string]any{testAPIVersionField: testAPIVersion, testKindField: "Profile",
				agentMetadataField: map[string]any{testGenerateNameField: "profile-unknown-", testNamespaceField: profile.Namespace}, testSpecField: spec}}
			Expect(shippedK8sClient.Create(ctx, candidate, &client.CreateOptions{FieldValidation: metav1.FieldValidationStrict})).NotTo(Succeed())
		}
	})
})

func TestProfilePolicyDigestIsCanonicalAndCopiesAreIndependent(t *testing.T) {
	spec := testProfilePolicy()
	spec.VaultRefs = []agentsv1alpha1.AgentReference{{Name: "z"}, {Name: "a"}}
	spec.SpendControl = agentsv1alpha1.SessionSpendControl{Mode: spendModeLimited, LimitCents: new(int64(100))}
	profile := &agentsv1alpha1.Profile{Spec: spec, Status: agentsv1alpha1.ProfileStatus{
		Agent:  &agentsv1alpha1.ProfileDependencyStatus{Name: _profileAgentName, UID: "safe-uid", Generation: 1},
		Vaults: []agentsv1alpha1.ProfileDependencyStatus{{Name: "a", UID: "safe-vault", Generation: 1}}}}
	if got := agentsv1alpha1.ProfilePolicyDigest(profile.Spec); got != "c1888ab67d0e857458a74b1a736e39d4f36c013da9f64ee5b433d1324a84bfe4" {
		t.Fatalf("durable policy digest encoding changed: %s", got)
	}
	copy := profile.DeepCopy()
	slices.Reverse(copy.Spec.VaultRefs)
	if agentsv1alpha1.ProfilePolicyDigest(copy.Spec) != agentsv1alpha1.ProfilePolicyDigest(profile.Spec) {
		t.Fatal("Vault order changed the safe policy digest")
	}
	if profile.Spec.VaultRefs[0].Name != "z" {
		t.Fatal("canonicalization changed caller-owned policy")
	}
	*copy.Spec.SpendControl.LimitCents = 200
	copy.Status.Agent.UID = _changedIdentityUID
	copy.Status.Vaults[0].UID = _changedIdentityUID
	if *profile.Spec.SpendControl.LimitCents != 100 || profile.Status.Agent.UID != "safe-uid" || profile.Status.Vaults[0].UID != "safe-vault" {
		t.Fatal("Profile DeepCopy aliased mutable policy or status")
	}
	if agentsv1alpha1.ProfilePolicyDigest(copy.Spec) == agentsv1alpha1.ProfilePolicyDigest(profile.Spec) {
		t.Fatal("changed spending policy retained the same digest")
	}
}

type failingProfileReader struct {
	client.Reader
	err error
}

func (r failingProfileReader) Get(ctx context.Context, key client.ObjectKey, object client.Object, options ...client.GetOption) error {
	if _, ok := object.(*agentsv1alpha1.Agent); ok {
		return r.err
	}
	return r.Reader.Get(ctx, key, object, options...)
}

func TestProfileDependencyObservationFailureRevokesReadiness(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := agentsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	profile := &agentsv1alpha1.Profile{ObjectMeta: metav1.ObjectMeta{Name: _profileTestName, Namespace: _profileTestNamespace, UID: "profile-uid", Generation: 1}, Spec: testProfilePolicy()}
	meta.SetStatusCondition(&profile.Status.Conditions, metav1.Condition{Type: readyCondition, Status: metav1.ConditionTrue, Reason: "OldReady", ObservedGeneration: 1})
	k8s := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&agentsv1alpha1.Profile{}).WithObjects(profile).Build()
	cause := errors.New("dependency observation unavailable")
	r := &ProfileReconciler{Client: k8s, APIReader: failingProfileReader{Reader: k8s, err: cause}, OwnerScope: testAgentScope}
	if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(profile)}); !errors.Is(err, cause) {
		t.Fatalf("observation cause was not returned: %v", err)
	}
	if err := k8s.Get(t.Context(), client.ObjectKeyFromObject(profile), profile); err != nil {
		t.Fatal(err)
	}
	condition := meta.FindStatusCondition(profile.Status.Conditions, readyCondition)
	if condition == nil || condition.Status != metav1.ConditionFalse || condition.Reason != "DependencyObservationFailed" {
		t.Fatalf("failed observation left usable readiness: %+v", profile.Status)
	}
}

func TestProfileDependencyEventsMatchNamespaceAndReference(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	if err := agentsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	selected := &agentsv1alpha1.Profile{ObjectMeta: metav1.ObjectMeta{Name: _profileTestName, Namespace: _profileTestNamespace}, Spec: testProfilePolicy()}
	otherNamespace := selected.DeepCopy()
	otherNamespace.Namespace = "profile-other-team"
	unrelated := selected.DeepCopy()
	unrelated.Name = "unrelated"
	unrelated.Spec.AgentRef.Name, unrelated.Spec.EnvironmentRef.Name = "other-agent", "other-environment"
	unrelated.Spec.VaultRefs = []agentsv1alpha1.AgentReference{{Name: "other-vault"}}
	builder := fake.NewClientBuilder().WithScheme(scheme).WithObjects(selected, otherNamespace, unrelated)
	for _, field := range []string{agentReferenceIndex, environmentReferenceIndex, _profileVaultReferenceIndex} {
		builder = builder.WithIndex(&agentsv1alpha1.Profile{}, field, func(object client.Object) []string {
			return profileReferences(object, field)
		})
	}
	r := &ProfileReconciler{Client: builder.Build()}
	tests := []struct {
		name   string
		object client.Object
	}{
		{name: "Agent status event", object: &agentsv1alpha1.Agent{ObjectMeta: metav1.ObjectMeta{Name: _profileAgentName, Namespace: selected.Namespace}}},
		{name: "Environment status event", object: &agentsv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: _profileEnvironment, Namespace: selected.Namespace}}},
		{name: "Vault status event", object: &agentsv1alpha1.Vault{ObjectMeta: metav1.ObjectMeta{Name: dependencyScenarioVaultName, Namespace: selected.Namespace}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			requests := r.dependencyProfileRequests(t.Context(), tt.object)
			if len(requests) != 1 || requests[0].NamespacedName != client.ObjectKeyFromObject(selected) {
				t.Fatalf("dependency event crossed namespace or recipe selection: %v", requests)
			}
		})
	}
}
