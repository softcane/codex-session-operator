package controller

import (
	"context"
	"crypto/sha256"
	"errors"
	"strings"
	"time"

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
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

var testExecutorImage = "example.com/executor@sha256:" + strings.Repeat("a", 64)

type connectionStub struct {
	state openaiapi.Connection
	err   error
}

func (c *connectionStub) Observe(_, _ string) (openaiapi.Connection, error) { return c.state, c.err }
func (*connectionStub) Forget(string)                                       {}

var _ = Describe("Legacy executor lifecycle (pre-migration Kubernetes API)", func() {
	useLegacySessionSchema()
	var s *agentsv1alpha1.Session
	var r *SessionReconciler
	var provider *sessionProviderStub
	var observer *connectionStub
	var secret *corev1.Secret
	step := func() {
		_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(s)})
		Expect(err).NotTo(HaveOccurred())
	}
	read := func() *agentsv1alpha1.Session {
		out := &agentsv1alpha1.Session{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(s), out)).To(Succeed())
		return out
	}
	readPod := func() *corev1.Pod {
		out := &corev1.Pod{}
		Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: s.Namespace, Name: sandbox.PodName(s)}, out)).To(Succeed())
		return out
	}
	BeforeEach(func() {
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "executor-test-"}}
		Expect(k8sClient.Create(ctx, ns)).To(Succeed())
		s = &agentsv1alpha1.Session{ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: ns.Name}, Spec: agentsv1alpha1.SessionSpec{}}
		Expect(k8sClient.Create(ctx, s)).To(Succeed())
		secret = &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: sandbox.ExecutorSecretName, Namespace: ns.Name}, Type: corev1.SecretTypeOpaque,
			Data: map[string][]byte{sandbox.ExecutorSecretKey: []byte("test-only-placeholder")}}
		Expect(k8sClient.Create(ctx, secret)).To(Succeed())
		provider = &sessionProviderStub{}
		observer = &connectionStub{state: openaiapi.Connection{Subscribed: true, Epoch: "stream-1"}}
		r = &SessionReconciler{Client: k8sClient, APIReader: k8sClient, Scheme: k8sClient.Scheme(), OpenAI: provider, Connections: observer, ExecutorImage: testExecutorImage}
		step()
		step()
		step()
	})
	AfterEach(func() {
		pods := &corev1.PodList{}
		Expect(k8sClient.List(ctx, pods, client.InNamespace(s.Namespace))).To(Succeed())
		for i := range pods.Items {
			p := &pods.Items[i]
			p.Finalizers = nil
			Expect(k8sClient.Update(ctx, p)).To(Succeed())
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, p, client.GracePeriodSeconds(0)))).To(Succeed())
		}
		current := &agentsv1alpha1.Session{}
		if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(s), current); err == nil {
			current.Finalizers = nil
			Expect(k8sClient.Update(ctx, current)).To(Succeed())
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, current))).To(Succeed())
		} else {
			Expect(apierrors.IsNotFound(err)).To(BeTrue())
		}
		Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, secret))).To(Succeed())
	})
	It("creates one owned Pod with a frozen image, writable volumes and only the executor Secret", func() {
		pod := readPod()
		Expect(metav1.IsControlledBy(pod, s)).To(BeTrue())
		Expect(pod.Spec.AutomountServiceAccountToken).To(HaveValue(BeFalse()))
		Expect(pod.Spec.Containers).To(HaveLen(1))
		container := pod.Spec.Containers[0]
		Expect(container.Env).To(HaveLen(1))
		Expect(container.Env[0].Name).To(Equal(sandbox.ExecutorSecretKey))
		Expect(container.Env[0].Value).To(BeEmpty())
		Expect(container.Env[0].ValueFrom.SecretKeyRef.Name).To(Equal(sandbox.ExecutorSecretName))
		Expect(container.WorkingDir).To(Equal(defaultWorkspace))
		Expect(pod.Spec.Volumes[0].EmptyDir).NotTo(BeNil())
		Expect(container.SecurityContext.ReadOnlyRootFilesystem).To(HaveValue(BeTrue()))
		Expect(executorMatches(pod, pod)).To(BeTrue())
		step()
		first := read()
		for range 3 {
			step()
		}
		Expect(read().ResourceVersion).To(Equal(first.ResourceVersion))
		Expect(readPod().UID).To(Equal(pod.UID))
		Expect(provider.creates).To(Equal(1))
		r.ExecutorImage = "changed-platform-image"
		step()
		Expect(readPod().Spec.Containers[0].Image).To(Equal(testExecutorImage))
	})
	It("requires a live connected event AND Pod health; disconnect and observation failure revoke readiness", func() {
		pod := readPod()
		pod.Status.Phase = corev1.PodRunning
		pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
		Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
		step()
		Expect(meta.IsStatusConditionFalse(read().Status.Conditions, "Ready")).To(BeTrue())
		observer.state.Connected = true
		observer.state.Sequence++
		step()
		Expect(meta.IsStatusConditionTrue(read().Status.Conditions, "Ready")).To(BeTrue())
		version := read().ResourceVersion
		step()
		Expect(read().ResourceVersion).To(Equal(version))
		observer.state.Connected = false
		step()
		Expect(meta.IsStatusConditionFalse(read().Status.Conditions, "Ready")).To(BeTrue())
		observer.err = errors.New("Observation unavailable")
		_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(s)})
		Expect(err).To(HaveOccurred())
		Expect(meta.IsStatusConditionFalse(read().Status.Conditions, "Ready")).To(BeTrue())
	})
	It("replaces a removed or terminal Pod without creating a new remote Session", func() {
		old := readPod()
		Expect(k8sClient.Delete(ctx, old, client.GracePeriodSeconds(0))).To(Succeed())
		step()
		replacement := readPod()
		Expect(replacement.UID).NotTo(Equal(old.UID))
		Expect(read().Status.OpenAISessionID).To(Equal("sess_test"))
		replacement.Status.Phase = corev1.PodFailed
		Expect(k8sClient.Status().Update(ctx, replacement)).To(Succeed())
		step()
		step()
		Expect(readPod().UID).NotTo(Equal(replacement.UID))
		Expect(provider.creates).To(Equal(1))
	})
	It("does not reuse the old Pod's connected report for its replacement", func() {
		healthy := func() {
			pod := readPod()
			pod.Status.Phase = corev1.PodRunning
			pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
			Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
		}
		healthy()
		observer.state.Connected = true
		observer.state.Sequence = 1
		step()
		Expect(meta.IsStatusConditionTrue(read().Status.Conditions, readyCondition)).To(BeTrue())
		Expect(k8sClient.Delete(ctx, readPod(), client.GracePeriodSeconds(0))).To(Succeed())
		step()
		healthy()
		step()
		Expect(meta.IsStatusConditionFalse(read().Status.Conditions, readyCondition)).To(BeTrue())
		observer.state.Sequence++
		step()
		Expect(meta.IsStatusConditionTrue(read().Status.Conditions, readyCondition)).To(BeTrue())
	})
	It("rejects the manager key even if it is stored under CODEX_API_KEY and stops a Pod when the Secret disappears", func() {
		r.ApplicationKeyHash = sha256.Sum256(secret.Data[sandbox.ExecutorSecretKey])
		step()
		Expect(meta.FindStatusCondition(read().Status.Conditions, readyCondition).Reason).To(Equal("ExecutorCredentialInvalid"))
		Expect(apierrors.IsNotFound(k8sClient.Get(ctx, client.ObjectKey{Namespace: s.Namespace, Name: sandbox.PodName(s)}, &corev1.Pod{}))).To(BeTrue())
		r.ApplicationKeyHash = [32]byte{}
		step()
		Expect(readPod()).NotTo(BeNil())
		Expect(k8sClient.Delete(ctx, secret)).To(Succeed())
		provider.remote = openaiapi.Session{} // Provider outage cannot delay key removal.
		step()
		Expect(meta.FindStatusCondition(read().Status.Conditions, readyCondition).Reason).To(Equal("ExecutorCredentialMissing"))
		Expect(apierrors.IsNotFound(k8sClient.Get(ctx, client.ObjectKey{Namespace: s.Namespace, Name: sandbox.PodName(s)}, &corev1.Pod{}))).To(BeTrue())
	})
	It("stops compute even when the durable gateway cannot quiesce", func() {
		r.Gateway = &gatewayStub{unavailable: true}
		Expect(k8sClient.Delete(ctx, read())).To(Succeed())
		_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(s)})
		Expect(err).To(HaveOccurred())
		Expect(apierrors.IsNotFound(k8sClient.Get(ctx, client.ObjectKey{Namespace: s.Namespace, Name: sandbox.PodName(s)}, &corev1.Pod{}))).To(BeTrue())
		Expect(read().Finalizers).To(ContainElement(sessionFinalizer))
		Expect(provider.deletes).To(BeZero())
		r.Gateway = nil
		step()
	})
	It("stops compute when the executor Secret is deleting even before its finalizers are removed", func() {
		secret.Finalizers = []string{otherTestFinalizer}
		Expect(k8sClient.Update(ctx, secret)).To(Succeed())
		DeferCleanup(func() {
			var current corev1.Secret
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(secret), &current)).To(Succeed())
			current.Finalizers = nil
			Expect(k8sClient.Update(ctx, &current)).To(Succeed())
		})
		Expect(k8sClient.Delete(ctx, secret)).To(Succeed())
		step()
		Expect(apierrors.IsNotFound(k8sClient.Get(ctx, client.ObjectKey{Namespace: s.Namespace, Name: sandbox.PodName(s)}, &corev1.Pod{}))).To(BeTrue())
		Expect(meta.FindStatusCondition(read().Status.Conditions, readyCondition).Reason).To(Equal("ExecutorCredentialInvalid"))
	})
	It("uses a fresh Pod read before releasing the Session finalizer when the cache misses the executor", func() {
		pod := readPod()
		pod.Finalizers = []string{otherTestFinalizer}
		Expect(k8sClient.Update(ctx, pod)).To(Succeed())
		r.Client = &missingPodClient{Client: k8sClient}
		Expect(k8sClient.Delete(ctx, read())).To(Succeed())
		step()
		Expect(readPod().DeletionTimestamp.IsZero()).To(BeFalse())
		Expect(read().Finalizers).To(ContainElement(sessionFinalizer))
		step()
		Expect(read().Finalizers).To(ContainElement(sessionFinalizer))
	})

	It("waits for terminating Pods before replacement and finalization; remote cleanup failure retains obligations", func() {
		pod := readPod()
		pod.Finalizers = []string{"test.example/block"}
		Expect(k8sClient.Update(ctx, pod)).To(Succeed())
		Expect(k8sClient.Delete(ctx, pod)).To(Succeed())
		step()
		step()
		Expect(readPod().UID).To(Equal(pod.UID))
		provider.deleteFailure = true
		Expect(k8sClient.Delete(ctx, read())).To(Succeed())
		_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(s)})
		Expect(err).To(HaveOccurred())
		Expect(read().Finalizers).To(ContainElement(sessionFinalizer))
		provider.deleteFailure = false
		step()
		Expect(read().Finalizers).To(ContainElement(sessionFinalizer))
		pod = readPod()
		pod.Finalizers = nil
		Expect(k8sClient.Update(ctx, pod)).To(Succeed())
		step()
		Expect(apierrors.IsNotFound(k8sClient.Get(ctx, client.ObjectKeyFromObject(s), &agentsv1alpha1.Session{}))).To(BeTrue())
		Expect(provider.creates).To(Equal(1))
	})
	It("replaces compute after key rotation or stream resubscription and rejects application credentials", func() {
		old := readPod()
		secret.Data[sandbox.ExecutorSecretKey] = []byte("rotated-test-placeholder")
		Expect(k8sClient.Update(ctx, secret)).To(Succeed())
		step()
		step()
		Expect(readPod().UID).NotTo(Equal(old.UID))
		old = readPod()
		observer.state.Epoch = "stream-2"
		step()
		step()
		Expect(readPod().UID).NotTo(Equal(old.UID))
		secret.Data["OPENAI_API_KEY"] = []byte("application-placeholder")
		Expect(k8sClient.Update(ctx, secret)).To(Succeed())
		step()
		Expect(apierrors.IsNotFound(k8sClient.Get(ctx, client.ObjectKeyFromObject(old), &corev1.Pod{}))).To(BeTrue())
		Expect(meta.IsStatusConditionFalse(read().Status.Conditions, "Ready")).To(BeTrue())
	})
	It("does not adopt or delete a Pod owned by a different Session UID", func() {
		pod := readPod()
		Expect(k8sClient.Delete(ctx, pod, client.GracePeriodSeconds(0))).To(Succeed())
		pod.ResourceVersion = ""
		pod.UID = ""
		pod.OwnerReferences[0].UID = "another-session"
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		step()
		Expect(meta.FindStatusCondition(read().Status.Conditions, "Ready").Reason).To(Equal("ExecutorOwnershipMismatch"))
		Expect(readPod().UID).To(Equal(pod.UID))
		Expect(k8sClient.Delete(ctx, read())).To(Succeed())
		step()
		Expect(readPod().UID).To(Equal(pod.UID))
	})
	It("reacts to Secret creation and owned Pod deletion through registered watches", func() {
		Expect(k8sClient.Delete(ctx, readPod(), client.GracePeriodSeconds(0))).To(Succeed())
		Expect(k8sClient.Delete(ctx, secret)).To(Succeed())
		mgr, err := ctrl.NewManager(cfg, ctrl.Options{Scheme: k8sClient.Scheme(), Metrics: metricsserver.Options{BindAddress: "0"},
			Cache: cache.Options{DefaultNamespaces: map[string]cache.Config{s.Namespace: {}}}})
		Expect(err).NotTo(HaveOccurred())
		r.Client = mgr.GetClient()
		r.APIReader = mgr.GetAPIReader()
		agents := &agentProviderStub{}
		r.Agents, r.AgentOwnerScope = agents, testAgentScope
		provider.agents = agents
		agentController := &AgentReconciler{Client: mgr.GetClient(), APIReader: mgr.GetAPIReader(), OpenAI: agents, OwnerScope: testAgentScope}
		Expect(agentController.SetupWithManager(mgr)).To(Succeed())
		environments := &environmentProviderStub{sessions: provider, runtimeStatus: testConnectedRuntime}
		r.Environments = environments
		environmentController := &EnvironmentReconciler{Client: mgr.GetClient(), APIReader: mgr.GetAPIReader(), OpenAI: environments, OwnerScope: testAgentScope}
		Expect(environmentController.SetupWithManager(mgr)).To(Succeed())
		Expect(r.SetupWithManager(mgr)).To(Succeed())
		managerCtx, cancel := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- mgr.Start(managerCtx) }()
		defer func() { cancel(); Expect(<-done).To(Succeed()) }()
		Eventually(func() string { return meta.FindStatusCondition(read().Status.Conditions, "Ready").Reason }, 10*time.Second).Should(Equal("ExecutorCredentialMissing"))
		secret.ResourceVersion = ""
		secret.UID = ""
		Expect(k8sClient.Create(ctx, secret)).To(Succeed())
		key := client.ObjectKey{Namespace: s.Namespace, Name: sandbox.PodName(s)}
		Eventually(func() error { return k8sClient.Get(ctx, key, &corev1.Pod{}) }, 5*time.Second).Should(Succeed())
		old := readPod()
		Expect(k8sClient.Delete(ctx, old, client.GracePeriodSeconds(0))).To(Succeed())
		Eventually(func() bool { pod := &corev1.Pod{}; return k8sClient.Get(ctx, key, pod) == nil && pod.UID != old.UID }, 5*time.Second).Should(BeTrue())
		verifyAgentDependencyWatches(s.Namespace)
		verifyEnvironmentDependencyWatches(s.Namespace)
		verifyProtectedEnvironmentWatch(s.Namespace)
	})
})

// missingPodClient models informer lag independently of the authoritative reader.
type missingPodClient struct {
	client.Client
}

func (c *missingPodClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*corev1.Pod); ok {
		return apierrors.NewNotFound(corev1.Resource("pods"), key.Name)
	}
	return c.Client.Get(ctx, key, obj, opts...)
}
