package controller

import (
	"context"
	"reflect"

	"github.com/softcane/codex-session-operator/internal/sandbox"
	"github.com/softcane/codex-session-operator/internal/testutil"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	sandboxv1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	extensions "sigs.k8s.io/agent-sandbox/extensions/api/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// createTestSandboxPool installs administrator configuration in an isolated namespace.
func createTestSandboxPool(ctx context.Context, writer client.Client, namespace string) (*extensions.SandboxWarmPool, error) {
	pool, template := testutil.NewSandboxPool(namespace)
	pool.UID, template.UID = "", ""
	pool.Generation, template.Generation = 0, 0
	for _, object := range []client.Object{template, pool} {
		if err := writer.Create(ctx, object); err != nil {
			return nil, err
		}
	}
	return pool, nil
}

// reconcileTestSandboxInfrastructure supplies the external Sandbox operator,
// kubelet termination and garbage collection absent from envtest. It never
// changes Session status or turns an unready Pod into a ready executor.
// Deployed Kind tests exercise the actual upstream controllers.
func reconcileTestSandboxInfrastructure(ctx context.Context, writer client.Client, namespace string) error {
	var claims extensions.SandboxClaimList
	if err := writer.List(ctx, &claims, client.InNamespace(namespace)); err != nil {
		return err
	}
	for i := range claims.Items {
		if err := materializeTestSandboxClaim(ctx, writer, &claims.Items[i]); err != nil {
			return err
		}
	}
	var runtimes sandboxv1.SandboxList
	if err := writer.List(ctx, &runtimes, client.InNamespace(namespace)); err != nil {
		return err
	}
	for i := range runtimes.Items {
		if err := reconcileTestSandboxRuntime(ctx, writer, &runtimes.Items[i]); err != nil {
			return err
		}
	}
	return collectTestSandboxClaims(ctx, writer, claims.Items)
}

func collectTestSandboxClaims(ctx context.Context, writer client.Client, claims []extensions.SandboxClaim) error {
	for i := range claims {
		claim := &claims[i]
		if claim.DeletionTimestamp.IsZero() {
			continue
		}
		var runtimes sandboxv1.SandboxList
		if err := writer.List(ctx, &runtimes, client.InNamespace(claim.Namespace)); err != nil {
			return err
		}
		owned := false
		for i := range runtimes.Items {
			if metav1.IsControlledBy(&runtimes.Items[i], claim) {
				owned = true
			}
		}
		if !owned {
			if err := forceDeleteTestResource(ctx, writer, claim); err != nil {
				return err
			}
		}
	}
	return nil
}

func reconcileTestSandboxRuntime(ctx context.Context, writer client.Client, runtime *sandboxv1.Sandbox) error {
	if removed, err := collectTestSandbox(ctx, writer, runtime); removed || err != nil {
		return err
	}

	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: runtime.Name, Namespace: runtime.Namespace}}
	if runtime.Spec.OperatingMode == sandboxv1.SandboxOperatingModeSuspended {
		if err := deleteOwnedTestResource(ctx, writer, pod, runtime); err != nil {
			return err
		}
		return nil
	}
	for _, volume := range runtime.Spec.VolumeClaimTemplates {
		pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: volume.Name + "-" + runtime.Name, Namespace: runtime.Namespace, OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(runtime, sandboxv1.GroupVersion.WithKind("Sandbox"))}}, Spec: *volume.Spec.DeepCopy()}
		if err := writer.Get(ctx, client.ObjectKeyFromObject(pvc), &corev1.PersistentVolumeClaim{}); apierrors.IsNotFound(err) {
			if err := writer.Create(ctx, pvc); err != nil {
				return err
			}
		} else if err != nil {
			return err
		}
	}
	if err := writer.Get(ctx, client.ObjectKeyFromObject(pod), pod); apierrors.IsNotFound(err) {
		expected, err := sandbox.ExpectedPod(runtime)
		if err != nil {
			return err
		}
		expected.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(runtime, sandboxv1.GroupVersion.WithKind("Sandbox"))}
		if err := writer.Create(ctx, expected); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	return nil
}

func collectTestSandbox(ctx context.Context, writer client.Client, runtime *sandboxv1.Sandbox) (bool, error) {

	owner := metav1.GetControllerOf(runtime)
	if owner != nil && owner.Kind == "SandboxClaim" {
		var claim extensions.SandboxClaim
		err := writer.Get(ctx, client.ObjectKey{Namespace: runtime.Namespace, Name: owner.Name}, &claim)
		if apierrors.IsNotFound(err) || err == nil && (!claim.DeletionTimestamp.IsZero() || claim.UID != owner.UID) {
			if err := deleteTestSandboxResources(ctx, writer, runtime); err != nil {
				return false, err
			}
			if err == nil && !claim.DeletionTimestamp.IsZero() {
				if err := forceDeleteTestResource(ctx, writer, &claim); err != nil {
					return false, err
				}
			}
			return true, nil
		}
		if err != nil {
			return false, err
		}
	}
	return false, nil
}

func materializeTestSandboxClaim(ctx context.Context, writer client.Client, claim *extensions.SandboxClaim) error {
	if !claim.DeletionTimestamp.IsZero() {
		return nil
	}

	var pool extensions.SandboxWarmPool
	if err := writer.Get(ctx, client.ObjectKey{Namespace: claim.Namespace, Name: claim.Spec.WarmPoolRef.Name}, &pool); err != nil {
		return client.IgnoreNotFound(err)
	}
	var template extensions.SandboxTemplate
	if err := writer.Get(ctx, client.ObjectKey{Namespace: claim.Namespace, Name: pool.Spec.TemplateRef.Name}, &template); err != nil {
		return client.IgnoreNotFound(err)
	}
	var runtime sandboxv1.Sandbox
	key := client.ObjectKey{Namespace: claim.Namespace, Name: claim.Name}
	err := writer.Get(ctx, key, &runtime)
	if apierrors.IsNotFound(err) {
		runtime = sandboxv1.Sandbox{ObjectMeta: metav1.ObjectMeta{Name: claim.Name, Namespace: claim.Namespace, OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(claim, extensions.GroupVersion.WithKind("SandboxClaim"))}}, Spec: sandboxv1.SandboxSpec{OperatingMode: sandboxv1.SandboxOperatingModeRunning, SandboxBlueprint: *sandbox.ExpectedBlueprint(&template, claim)}}
		if err := writer.Create(ctx, &runtime); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	if !metav1.IsControlledBy(&runtime, claim) {
		return nil
	}
	expected := sandbox.ExpectedBlueprint(&template, claim)
	if !reflect.DeepEqual(runtime.Spec.PodTemplate.ObjectMeta, expected.PodTemplate.ObjectMeta) {
		base := runtime.DeepCopy()
		runtime.Spec.PodTemplate.ObjectMeta = expected.PodTemplate.ObjectMeta
		if err := writer.Patch(ctx, &runtime, client.MergeFrom(base)); err != nil {
			return err
		}
	}
	if claim.Status.SandboxStatus.Name != runtime.Name {
		base := claim.DeepCopy()
		claim.Status.SandboxStatus.Name = runtime.Name
		if err := writer.Status().Patch(ctx, claim, client.MergeFrom(base)); err != nil {
			return err
		}
	}
	return nil
}

func deleteTestSandboxResources(ctx context.Context, writer client.Client, runtime *sandboxv1.Sandbox) error {
	if err := deleteOwnedTestResource(ctx, writer, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: runtime.Name, Namespace: runtime.Namespace}}, runtime); err != nil {
		return err
	}
	for _, volume := range runtime.Spec.VolumeClaimTemplates {
		if err := deleteOwnedTestResource(ctx, writer, &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: volume.Name + "-" + runtime.Name, Namespace: runtime.Namespace}}, runtime); err != nil {
			return err
		}
	}
	return forceDeleteTestResource(ctx, writer, runtime)
}

func deleteOwnedTestResource(ctx context.Context, writer client.Client, object client.Object, owner metav1.Object) error {
	if err := writer.Get(ctx, client.ObjectKeyFromObject(object), object); err != nil {
		return client.IgnoreNotFound(err)
	}
	if !metav1.IsControlledBy(object, owner) {
		return nil
	}
	return forceDeleteTestResource(ctx, writer, object)
}

func forceDeleteTestResource(ctx context.Context, writer client.Client, object client.Object) error {
	if err := writer.Get(ctx, client.ObjectKeyFromObject(object), object); err != nil {
		return client.IgnoreNotFound(err)
	}
	if len(object.GetFinalizers()) > 0 {
		object.SetFinalizers(nil)
		if err := writer.Update(ctx, object); err != nil {
			return err
		}
	}
	return client.IgnoreNotFound(writer.Delete(ctx, object, client.GracePeriodSeconds(0)))
}
