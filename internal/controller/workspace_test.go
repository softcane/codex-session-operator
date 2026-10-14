package controller

import (
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	"github.com/softcane/codex-session-operator/internal/openaiapi"
	"github.com/softcane/codex-session-operator/internal/sandbox"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	workspaceTestSize      = "2Gi"
	workspaceTestSessionID = "sess_test"
	workspaceTestArch      = "amd64"
	workspaceTestClass     = "approved"
)

var _ = Describe("Persistent Session workspaces (real Kubernetes API)", func() {
	var session *agentsv1alpha1.Session
	var reconciler *SessionReconciler
	var provider *sessionProviderStub
	var namespace string

	readSession := func() *agentsv1alpha1.Session {
		current := &agentsv1alpha1.Session{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(session), current)).To(Succeed())
		return current
	}
	readClaim := func() *corev1.PersistentVolumeClaim {
		claim := &corev1.PersistentVolumeClaim{}
		Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: namespace, Name: sandbox.WorkspaceClaimNameForUID(string(session.UID))}, claim)).To(Succeed())
		return claim
	}
	makeSession := func(policy string) {
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "workspace-test-"}}
		Expect(k8sClient.Create(ctx, ns)).To(Succeed())
		namespace = ns.Name
		session = &agentsv1alpha1.Session{ObjectMeta: metav1.ObjectMeta{Name: "workspace", Namespace: namespace, Finalizers: []string{sessionFinalizer}},
			Spec: agentsv1alpha1.SessionSpec{AgentRef: &agentsv1alpha1.AgentReference{Name: "saved-agent"}, EnvironmentRef: &agentsv1alpha1.AgentReference{Name: "saved-environment"},
				SpendControl: &agentsv1alpha1.SessionSpendControl{Mode: spendModeUnlimited}}}
		Expect(k8sClient.Create(ctx, session)).To(Succeed())
		session.Status.EnvironmentSnapshot = &agentsv1alpha1.SessionEnvironmentSnapshot{Profile: agentsv1alpha1.EnvironmentSpec{Mode: testSelfHostedMode, SelfHosted: &agentsv1alpha1.SelfHostedEnvironment{
			Image: testExecutorImage, Architecture: workspaceTestArch, WorkspaceDirectory: defaultWorkspace, WorkspaceSize: workspaceTestSize,
			WorkspaceStorage: &agentsv1alpha1.WorkspaceStorage{Type: sandbox.WorkspaceStoragePersistent, StorageClassName: workspaceTestClass, ClaimDeletionPolicy: policy}}}}
		session.Status.OpenAISessionID = workspaceTestSessionID
		Expect(k8sClient.Status().Update(ctx, session)).To(Succeed())
		session = readSession()
		provider = &sessionProviderStub{remote: openaiapi.Session{ID: workspaceTestSessionID, OwnerUID: string(session.UID)}}
		reconciler = &SessionReconciler{Client: k8sClient, APIReader: k8sClient, Scheme: k8sClient.Scheme(), OpenAI: provider, WorkspaceStorageClasses: []string{workspaceTestClass}}
	}
	BeforeEach(func() { makeSession(retainPolicy) })
	AfterEach(func() {
		claim := &corev1.PersistentVolumeClaim{}
		if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: namespace, Name: sandbox.WorkspaceClaimNameForUID(string(session.UID))}, claim); err == nil {
			claim.Finalizers = nil
			Expect(k8sClient.Update(ctx, claim)).To(Succeed())
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, claim))).To(Succeed())
		} else {
			Expect(apierrors.IsNotFound(err)).To(BeTrue())
		}
		current := &agentsv1alpha1.Session{}
		if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(session), current); err == nil {
			current.Finalizers = nil
			Expect(k8sClient.Update(ctx, current)).To(Succeed())
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, current))).To(Succeed())
		} else {
			Expect(apierrors.IsNotFound(err)).To(BeTrue())
		}
	})

	It("freezes a claim UID before Pod creation and blocks silent workspace replacement", func() {
		ready, committed, err := reconciler.reconcileWorkspaceClaim(ctx, session)
		Expect(err).NotTo(HaveOccurred())
		Expect(ready).To(BeFalse())
		Expect(committed).To(BeTrue())
		claim := readClaim()
		Expect(claim.OwnerReferences).To(BeEmpty())
		Expect(claim.Spec.StorageClassName).To(HaveValue(Equal(workspaceTestClass)))
		Expect(claim.Spec.Resources.Requests.Storage().String()).To(Equal(workspaceTestSize))
		Expect(session.Status.WorkspaceClaimUID).To(Equal(string(claim.UID)))
		session = readSession()
		ready, committed, err = reconciler.reconcileWorkspaceClaim(ctx, session)
		Expect(err).NotTo(HaveOccurred())
		Expect(ready).To(BeTrue()) // WaitForFirstConsumer classes bind after Pod scheduling.
		Expect(committed).To(BeFalse())
		// envtest does not run the PVC protection controller that would clear this finalizer.
		claim.Finalizers = nil
		Expect(k8sClient.Update(ctx, claim)).To(Succeed())
		Expect(k8sClient.Delete(ctx, claim)).To(Succeed())
		ready, committed, err = reconciler.reconcileWorkspaceClaim(ctx, session)
		Expect(err).NotTo(HaveOccurred())
		Expect(ready).To(BeFalse())
		Expect(committed).To(BeFalse())
		Expect(meta.FindStatusCondition(session.Status.Conditions, readyCondition).Reason).To(Equal("WorkspaceClaimMissing"))
		Expect(apierrors.IsNotFound(k8sClient.Get(ctx, client.ObjectKeyFromObject(claim), &corev1.PersistentVolumeClaim{}))).To(BeTrue())
	})

	It("admits only complete persistent and explicit ephemeral workspace settings", func() {
		settings := []struct {
			name    string
			storage *agentsv1alpha1.WorkspaceStorage
			valid   bool
		}{
			{name: "new-omission-rejected"},
			{name: "explicit-ephemeral", storage: &agentsv1alpha1.WorkspaceStorage{Type: sandbox.WorkspaceStorageEphemeral}, valid: true},
			{name: "persistent", storage: &agentsv1alpha1.WorkspaceStorage{Type: sandbox.WorkspaceStoragePersistent, StorageClassName: workspaceTestClass, ClaimDeletionPolicy: retainPolicy}, valid: true},
			{name: "missing-policy", storage: &agentsv1alpha1.WorkspaceStorage{Type: sandbox.WorkspaceStoragePersistent, StorageClassName: workspaceTestClass}},
			{name: "ephemeral-with-class", storage: &agentsv1alpha1.WorkspaceStorage{Type: sandbox.WorkspaceStorageEphemeral, StorageClassName: workspaceTestClass}},
		}
		for _, tt := range settings {
			environment := &agentsv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: tt.name, Namespace: namespace},
				Spec: agentsv1alpha1.EnvironmentSpec{Mode: testSelfHostedMode, DeletionPolicy: deletePolicy, SelfHosted: &agentsv1alpha1.SelfHostedEnvironment{
					Image: testExecutorImage, Architecture: workspaceTestArch, WorkspaceStorage: tt.storage}}}
			err := k8sClient.Create(ctx, environment)
			Expect(err == nil).To(Equal(tt.valid), tt.name)
		}
	})

	It("accepts a legacy Environment profile frozen into Session status", func() {
		legacy := &agentsv1alpha1.Session{ObjectMeta: metav1.ObjectMeta{Name: "legacy-snapshot", Namespace: namespace}, Spec: agentsv1alpha1.SessionSpec{
			AgentRef: &agentsv1alpha1.AgentReference{Name: "saved-agent"}, EnvironmentRef: &agentsv1alpha1.AgentReference{Name: "saved-environment"},
			SpendControl: &agentsv1alpha1.SessionSpendControl{Mode: spendModeUnlimited}}}
		Expect(k8sClient.Create(ctx, legacy)).To(Succeed())
		legacy.Status.EnvironmentSnapshot = &agentsv1alpha1.SessionEnvironmentSnapshot{Profile: agentsv1alpha1.EnvironmentSpec{
			Mode: testSelfHostedMode, SelfHosted: &agentsv1alpha1.SelfHostedEnvironment{Image: testExecutorImage, Architecture: workspaceTestArch, WorkspaceDirectory: defaultWorkspace}}}
		Expect(k8sClient.Status().Update(ctx, legacy)).To(Succeed())
		Expect(k8sClient.Delete(ctx, legacy)).To(Succeed())
	})

	It("marks an unowned Retain PVC only after remote cleanup", func() {
		claim, err := sandbox.WorkspaceClaim(session)
		Expect(err).NotTo(HaveOccurred())
		Expect(k8sClient.Create(ctx, claim)).To(Succeed())
		claim = readClaim()
		session.Status.WorkspaceClaimCreationAttempted = true
		session.Status.WorkspaceClaimUID = string(claim.UID)
		Expect(k8sClient.Status().Update(ctx, session)).To(Succeed())
		Expect(k8sClient.Delete(ctx, readSession())).To(Succeed())
		provider.deleteFailure = true
		_, err = reconciler.finalize(ctx, readSession())
		Expect(err).To(HaveOccurred())
		Expect(readClaim().OwnerReferences).To(BeEmpty())
		provider.deleteFailure = false
		_, err = reconciler.finalize(ctx, readSession())
		Expect(err).NotTo(HaveOccurred())
		claim = readClaim()
		Expect(claim.OwnerReferences).To(BeEmpty())
		Expect(claim.Annotations[sandbox.WorkspaceRetainedAnnotation]).To(Equal(string(session.UID)))
		_, err = reconciler.finalize(ctx, readSession())
		Expect(err).NotTo(HaveOccurred())
		Expect(apierrors.IsNotFound(k8sClient.Get(ctx, client.ObjectKeyFromObject(session), &agentsv1alpha1.Session{}))).To(BeTrue())
		Expect(readClaim()).NotTo(BeNil())
	})

	It("blocks Retain creation outcomes lost before and after PVC creation", func() {
		session.Status.WorkspaceClaimCreationAttempted = true
		Expect(k8sClient.Status().Update(ctx, session)).To(Succeed())
		session = readSession()
		ready, committed, err := reconciler.reconcileWorkspaceClaim(ctx, session)
		Expect(err).NotTo(HaveOccurred())
		Expect(ready).To(BeFalse())
		Expect(committed).To(BeFalse())
		Expect(meta.FindStatusCondition(session.Status.Conditions, readyCondition).Reason).To(Equal("WorkspaceClaimOutcomeUnknown"))
		claim, err := sandbox.WorkspaceClaim(session)
		Expect(err).NotTo(HaveOccurred())
		Expect(k8sClient.Create(ctx, claim)).To(Succeed())
		ready, committed, err = reconciler.reconcileWorkspaceClaim(ctx, session)
		Expect(err).NotTo(HaveOccurred())
		Expect(ready).To(BeFalse())
		Expect(committed).To(BeFalse())
		Expect(session.Status.WorkspaceClaimUID).To(BeEmpty())
		Expect(meta.FindStatusCondition(session.Status.Conditions, readyCondition).Reason).To(Equal("WorkspaceClaimOutcomeUnknown"))
	})

	It("does not adopt an unclaimed same-name Retain PVC", func() {
		claim, err := sandbox.WorkspaceClaim(session)
		Expect(err).NotTo(HaveOccurred())
		Expect(k8sClient.Create(ctx, claim)).To(Succeed())
		ready, committed, err := reconciler.reconcileWorkspaceClaim(ctx, session)
		Expect(err).NotTo(HaveOccurred())
		Expect(ready).To(BeFalse())
		Expect(committed).To(BeFalse())
		Expect(session.Status.WorkspaceClaimUID).To(BeEmpty())
		Expect(meta.FindStatusCondition(session.Status.Conditions, readyCondition).Reason).To(Equal("WorkspaceOwnershipMismatch"))
		session.Status.WorkspaceClaimUID = string(readClaim().UID)
		ready, committed, err = reconciler.reconcileWorkspaceClaim(ctx, session)
		Expect(err).NotTo(HaveOccurred())
		Expect(ready).To(BeFalse())
		Expect(committed).To(BeFalse())
		Expect(meta.FindStatusCondition(session.Status.Conditions, readyCondition).Reason).To(Equal("WorkspaceOwnershipMismatch"))
	})

	It("does not delete a different claim UID during Session cleanup", func() {
		claim, err := sandbox.WorkspaceClaim(session)
		Expect(err).NotTo(HaveOccurred())
		Expect(k8sClient.Create(ctx, claim)).To(Succeed())
		session.Status.WorkspaceClaimCreationAttempted = true
		session.Status.WorkspaceClaimUID = "different-uid"
		Expect(k8sClient.Status().Update(ctx, session)).To(Succeed())
		provider.remote = openaiapi.Session{}
		_, err = reconciler.finalize(ctx, readSession())
		Expect(err).To(HaveOccurred())
		Expect(strings.Contains(err.Error(), "workspace claim UID")).To(BeTrue())
		Expect(readClaim().OwnerReferences).To(BeEmpty())
	})

	It("deletes the owned claim only after remote cleanup", func() {
		session.Status.EnvironmentSnapshot.Profile.SelfHosted.WorkspaceStorage.ClaimDeletionPolicy = deletePolicy
		Expect(k8sClient.Status().Update(ctx, session)).To(Succeed())
		session = readSession()
		claim, err := sandbox.WorkspaceClaim(session)
		Expect(err).NotTo(HaveOccurred())
		Expect(controllerutil.SetControllerReference(session, claim, k8sClient.Scheme())).To(Succeed())
		Expect(k8sClient.Create(ctx, claim)).To(Succeed())
		claim = readClaim()
		session.Status.WorkspaceClaimUID = string(claim.UID)
		Expect(k8sClient.Status().Update(ctx, session)).To(Succeed())
		Expect(k8sClient.Delete(ctx, readSession())).To(Succeed())
		provider.deleteFailure = true
		_, err = reconciler.finalize(ctx, readSession())
		Expect(err).To(HaveOccurred())
		Expect(readClaim()).NotTo(BeNil())
		claim = readClaim()
		claim.Finalizers = nil // envtest has no PVC protection controller.
		Expect(k8sClient.Update(ctx, claim)).To(Succeed())
		provider.deleteFailure = false
		_, err = reconciler.finalize(ctx, readSession())
		Expect(err).NotTo(HaveOccurred())
		_, err = reconciler.finalize(ctx, readSession())
		Expect(err).NotTo(HaveOccurred())
		Expect(apierrors.IsNotFound(k8sClient.Get(ctx, client.ObjectKeyFromObject(claim), &corev1.PersistentVolumeClaim{}))).To(BeTrue())
		Expect(apierrors.IsNotFound(k8sClient.Get(ctx, client.ObjectKeyFromObject(session), &agentsv1alpha1.Session{}))).To(BeTrue())
	})
})
