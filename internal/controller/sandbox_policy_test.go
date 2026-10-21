package controller

import (
	"os"
	"path/filepath"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/softcane/codex-session-operator/internal/testutil"
	admissionv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var _ = Describe("Managed Sandbox recipe admission", func() {
	It("freezes complete template and pool specs while permitting metadata updates", func() {
		manifest, err := os.Open(filepath.Join("..", "..", "config", "sandbox-policy", "immutable-templates.yaml"))
		Expect(err).NotTo(HaveOccurred())
		defer func() { Expect(manifest.Close()).To(Succeed()) }()
		decoder := yaml.NewYAMLOrJSONDecoder(manifest, 4096)
		policy := &admissionv1.ValidatingAdmissionPolicy{}
		binding := &admissionv1.ValidatingAdmissionPolicyBinding{}
		Expect(decoder.Decode(policy)).To(Succeed())
		Expect(decoder.Decode(binding)).To(Succeed())
		Expect(k8sClient.Create(ctx, policy)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, policy)).To(Succeed()) })
		Expect(k8sClient.Create(ctx, binding)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, binding)).To(Succeed()) })
		namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "sandbox-policy-"}}
		Expect(k8sClient.Create(ctx, namespace)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, namespace)).To(Succeed()) })

		// Admission watches update asynchronously. Probe a disposable pool until
		// the shipped policy rejects a replica change, without mutating a fixture.
		Eventually(func() bool {
			probe, _ := testutil.NewSandboxPool(namespace.Name)
			probe.Name, probe.GenerateName, probe.UID = "", "policy-probe-", ""
			Expect(k8sClient.Create(ctx, probe)).To(Succeed())
			probe.Spec.Replicas = new(int32(1))
			updateErr := k8sClient.Update(ctx, probe)
			Expect(k8sClient.Delete(ctx, probe)).To(Succeed())
			return apierrors.IsInvalid(updateErr)
		}, 15*time.Second, 100*time.Millisecond).Should(BeTrue())

		pool, template := testutil.NewSandboxPool(namespace.Name)
		pool.UID, template.UID = "", ""
		Expect(k8sClient.Create(ctx, template)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, template)).To(Succeed()) })
		Expect(k8sClient.Create(ctx, pool)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, pool)).To(Succeed()) })

		changedTemplate := template.DeepCopy()
		changedTemplate.Spec.PodTemplate.Spec.Containers[0].Image = "executor@sha256:" + strings.Repeat("b", 64)
		Expect(apierrors.IsInvalid(k8sClient.Update(ctx, changedTemplate))).To(BeTrue())
		changedPool := pool.DeepCopy()
		changedPool.Spec.Replicas = new(int32(1))
		Expect(apierrors.IsInvalid(k8sClient.Update(ctx, changedPool))).To(BeTrue())
		changedPool = pool.DeepCopy()
		changedPool.Spec.TemplateRef.Name = "another-version"
		Expect(apierrors.IsInvalid(k8sClient.Update(ctx, changedPool))).To(BeTrue())
		changedPool = pool.DeepCopy()
		delete(changedPool.Labels, "agents.softcane.io/managed")
		Expect(apierrors.IsInvalid(k8sClient.Update(ctx, changedPool))).To(BeTrue())
		changedTemplate = template.DeepCopy()
		delete(changedTemplate.Labels, "agents.softcane.io/managed")
		Expect(apierrors.IsInvalid(k8sClient.Update(ctx, changedTemplate))).To(BeTrue())

		pool.Annotations = map[string]string{"example.com/note": "metadata is editable"}
		Expect(k8sClient.Update(ctx, pool)).To(Succeed())
		template.Annotations = map[string]string{"example.com/note": "metadata is editable"}
		Expect(k8sClient.Update(ctx, template)).To(Succeed())
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(policy), policy)).To(Succeed())
		if policy.Status.TypeChecking != nil {
			Expect(policy.Status.TypeChecking.ExpressionWarnings).To(BeEmpty())
		}
	})
})
