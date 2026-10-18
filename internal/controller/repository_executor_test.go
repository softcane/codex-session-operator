package controller

import (
	"encoding/json"
	"strings"
	"testing"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	"github.com/softcane/codex-session-operator/internal/sandbox"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func repositoryExecutorFixture(t *testing.T) (*SessionReconciler, *agentsv1alpha1.Session, *corev1.Secret) {
	t.Helper()
	r, session, _ := preparationFixture(t)
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
		Name: sandbox.RepositorySecretName, Namespace: session.Namespace, UID: _preparationSourceUID,
	}, Type: corev1.SecretTypeOpaque, Data: map[string][]byte{testVaultTokenKey: []byte("protected-repository-token")}}
	if err := r.Create(t.Context(), secret); err != nil {
		t.Fatal(err)
	}
	repository := frozenRepository(session)
	repository.MountCredentialsInExecutor = true
	repository.CredentialSecretRef = &agentsv1alpha1.RepositoryCredentialSecretReference{
		Name: sandbox.RepositorySecretName, Key: testVaultTokenKey,
	}
	session.Status.EnvironmentSnapshot.SourceVersion = &agentsv1alpha1.EnvironmentSourceVersion{
		UID: string(secret.UID), ResourceVersion: secret.ResourceVersion,
	}
	if err := r.Status().Update(t.Context(), session); err != nil {
		t.Fatal(err)
	}
	return r, session, secret
}

func TestRepositoryExecutorRotationReplacesPodWithoutExposingCredential(t *testing.T) {
	r, session, secret := repositoryExecutorFixture(t)
	preparationStep(t, r, session)
	before := preparationPod(t, r, session)
	healthyPreparationPod(t, r, before, "before-rotation", 0)
	if !r.revalidateExecutorCredentials(t.Context(), session, before, "stream-1") {
		t.Fatal("unchanged runtime credentials failed revalidation")
	}
	secret.Data[testVaultTokenKey] = []byte("rotated-repository-token")
	if err := r.Update(t.Context(), secret); err != nil {
		t.Fatal(err)
	}
	if r.revalidateExecutorCredentials(t.Context(), session, before, "stream-1") {
		t.Fatal("runtime revalidation accepted credentials rotated during a provider read")
	}
	preparationStep(t, r, session)
	var current corev1.Pod
	if err := r.Get(t.Context(), client.ObjectKeyFromObject(before), &current); !apierrors.IsNotFound(err) {
		t.Fatal("rotation did not remove the old credential-bearing executor", err)
	}
	preparationStep(t, r, session)
	after := preparationPod(t, r, session)
	if after.Annotations[sandbox.ConfigAnnotation] == before.Annotations[sandbox.ConfigAnnotation] {
		t.Fatal("repository credential rotation did not change public executor identity")
	}
	encoded, err := json.Marshal([]any{after, session.Status})
	if err != nil || strings.Contains(string(encoded), "repository-token") {
		t.Fatal("credential contents entered executor configuration or status", err)
	}
}

func TestRepositoryExecutorCredentialRevocationPrecedesProviderObservation(t *testing.T) {
	for _, name := range []string{"deleted", "invalid token value", "recreated"} {
		t.Run(name, func(t *testing.T) {
			r, session, secret := repositoryExecutorFixture(t)
			session.Finalizers = []string{sessionFinalizer}
			if err := r.Update(t.Context(), session); err != nil {
				t.Fatal(err)
			}
			preparationStep(t, r, session)
			pod := preparationPod(t, r, session)
			switch name {
			case "deleted":
				if err := r.Delete(t.Context(), secret); err != nil {
					t.Fatal(err)
				}
			case "invalid token value":
				secret.Data[testVaultTokenKey] = []byte("invalid\ncredential")
				if err := r.Update(t.Context(), secret); err != nil {
					t.Fatal(err)
				}
			case "recreated":
				if err := r.Delete(t.Context(), secret); err != nil {
					t.Fatal(err)
				}
				secret.UID, secret.ResourceVersion = "replacement-source-uid", ""
				if err := r.Create(t.Context(), secret); err != nil {
					t.Fatal(err)
				}
			}
			r.OpenAI = &preparationOutageProvider{unavailable: true}
			if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(session)}); err != nil {
				t.Fatal("credential revocation depended on an unavailable provider", err)
			}
			if err := r.Get(t.Context(), client.ObjectKeyFromObject(pod), &corev1.Pod{}); !apierrors.IsNotFound(err) {
				t.Fatal("unavailable protected source left a credential-bearing executor running", err)
			}
			if err := r.Get(t.Context(), client.ObjectKeyFromObject(session), session); err != nil {
				t.Fatal(err)
			}
			condition := meta.FindStatusCondition(session.Status.Conditions, readyCondition)
			if condition == nil || condition.Reason != "RepositoryCredentialUnavailable" || condition.Status != metav1.ConditionFalse {
				t.Fatal("unavailable protected source did not hold Session readiness")
			}
		})
	}
}

func TestRepositorySecretEventsOnlySelectOptedInSessions(t *testing.T) {
	r, session, secret := repositoryExecutorFixture(t)
	if requests := r.repositorySecretRequests(t.Context(), secret); len(requests) != 1 || requests[0].Name != session.Name {
		t.Fatal("protected source event did not select its opted-in Session")
	}
	frozenRepository(session).MountCredentialsInExecutor = false
	if err := r.Status().Update(t.Context(), session); err != nil {
		t.Fatal(err)
	}
	if len(r.repositorySecretRequests(t.Context(), secret)) != 0 {
		t.Fatal("preparation-only Session received runtime credential events")
	}
}
