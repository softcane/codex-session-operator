package controller

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	"github.com/softcane/codex-session-operator/internal/openaiapi"
	"github.com/softcane/codex-session-operator/internal/sandbox"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	_sandboxCredentialTokenKey = "token"
	_sandboxCredentialEpoch    = "credential-stream"
	_sandboxCredentialOriginal = "fixture-original-token"
	_sandboxCredentialRotated  = "fixture-rotated-token"
)

func sandboxCredentialFixture(t *testing.T) (*workspaceLifecycleFixture, *corev1.Secret) {
	t.Helper()
	fixture := newWorkspaceLifecycleFixture(t, sandbox.WorkspaceClaimRetain)
	fixture.template.Spec.PodTemplate.Spec.Containers[0].Env = append(fixture.template.Spec.PodTemplate.Spec.Containers[0].Env,
		corev1.EnvVar{Name: "REPOSITORY_TOKEN", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: sandbox.RepositorySecretName}, Key: _sandboxCredentialTokenKey}}})
	if err := fixture.writer.Update(t.Context(), fixture.template); err != nil {
		t.Fatal(err)
	}
	resolved, err := sandbox.ResolvePool(t.Context(), fixture.writer, fixture.session.Namespace, fixture.session.Status.EnvironmentSnapshot.Profile.SelfHosted)
	if err != nil {
		t.Fatal(err)
	}
	fixture.session.Status.EnvironmentSnapshot.SandboxPool = resolved
	executor := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: sandbox.ExecutorSecretName, Namespace: fixture.session.Namespace, UID: "executor-source"}, Type: corev1.SecretTypeOpaque, Data: map[string][]byte{sandbox.ExecutorSecretKey: []byte("fixture-executor-key")}}
	repository := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: sandbox.RepositorySecretName, Namespace: fixture.session.Namespace, UID: "repository-source"}, Type: corev1.SecretTypeOpaque, Data: map[string][]byte{_sandboxCredentialTokenKey: []byte(_sandboxCredentialOriginal)}}
	for _, secret := range []*corev1.Secret{executor, repository} {
		if err := fixture.writer.Create(t.Context(), secret); err != nil {
			t.Fatal(err)
		}
	}
	claimUID := fixture.session.Status.SandboxClaimUID
	fixture.session.Status.SandboxClaimUID = ""
	versions, err := fixture.reconciler.sandboxCredentials(t.Context(), fixture.session, fixture.template)
	if err != nil {
		t.Fatal(err)
	}
	fixture.session.Status.SandboxClaimUID = claimUID
	config, err := claimConfigurationDigest(resolved, versions, _sandboxCredentialEpoch)
	if err != nil {
		t.Fatal(err)
	}
	fixture.session.Status.ExecutorConfigDigest = config
	fixture.claim.Annotations[sandbox.ConfigAnnotation] = config
	fixture.claim.Spec.AdditionalPodMetadata.Annotations[sandbox.ConfigAnnotation] = config
	fixture.sandbox.Spec.SandboxBlueprint = *sandbox.ExpectedBlueprint(fixture.template, fixture.claim)
	pod, err := sandbox.ExpectedPod(fixture.sandbox)
	if err != nil {
		t.Fatal(err)
	}
	fixture.pod.Spec, fixture.pod.Annotations = pod.Spec, pod.Annotations
	for _, object := range []client.Object{fixture.claim, fixture.sandbox, fixture.pod} {
		if err := fixture.writer.Update(t.Context(), object); err != nil {
			t.Fatal(err)
		}
	}
	if err := fixture.writer.Status().Update(t.Context(), fixture.session); err != nil {
		t.Fatal(err)
	}
	fixture.reconciler.SandboxPools = []string{resolved.Name}
	fixture.reconciler.Connections = &connectionStub{state: openaiapi.Connection{Subscribed: true, Epoch: _sandboxCredentialEpoch}}
	return fixture, repository
}

func TestSandboxCredentialRotationSuspendsBeforeReplacingSourceBaseline(t *testing.T) {
	fixture, secret := sandboxCredentialFixture(t)
	before := fixture.session.Status.ExecutorConfigDigest
	identities := append([]agentsv1alpha1.SandboxSecretSource(nil), fixture.session.Status.SandboxSecretSources...)
	if !fixture.reconciler.revalidateExecutorCredentials(t.Context(), fixture.session, fixture.pod, _sandboxCredentialEpoch) {
		t.Fatal("unchanged credential source failed revalidation")
	}
	secret.Data[_sandboxCredentialTokenKey] = []byte(_sandboxCredentialRotated)
	if err := fixture.writer.Update(t.Context(), secret); err != nil {
		t.Fatal(err)
	}
	if fixture.reconciler.revalidateExecutorCredentials(t.Context(), fixture.session, fixture.pod, _sandboxCredentialEpoch) {
		t.Fatal("rotated credential source retained a stale execution capability")
	}
	fixture.session.Status.WorkspacePreparation = &agentsv1alpha1.SessionWorkspacePreparationStatus{PodUID: string(fixture.pod.UID)}
	for range 2 {
		_, err := fixture.reconciler.reconcileExecutor(t.Context(), fixture.session)
		if err != nil {
			t.Fatal(err)
		}
		if err := fixture.writer.Get(t.Context(), client.ObjectKeyFromObject(fixture.sandbox), fixture.sandbox); err != nil {
			t.Fatal(err)
		}
		if fixture.sandbox.Spec.OperatingMode != sandboxv1beta1.SandboxOperatingModeSuspended {
			t.Fatal("rotation left credential-bearing compute running")
		}
		if err := fixture.writer.Get(t.Context(), client.ObjectKeyFromObject(fixture.pod), fixture.pod); err != nil {
			t.Fatal("operator deleted the upstream-owned Pod directly", err)
		}
		if err := fixture.writer.Get(t.Context(), client.ObjectKeyFromObject(fixture.claim), fixture.claim); err != nil {
			t.Fatal(err)
		}
		if fixture.claim.Spec.AdditionalPodMetadata.Annotations[sandbox.ConfigAnnotation] != before {
			t.Fatal("credential baseline advanced before old compute was absent")
		}
	}
	if fixture.session.Status.WorkspacePreparation != nil || fixture.session.Status.ExecutorConfigDigest == before {
		t.Fatal("rotation did not revoke preparation and update desired source version")
	}
	for range 3 {
		if err := reconcileTestSandboxInfrastructure(t.Context(), fixture.writer.Client, fixture.session.Namespace); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.reconciler.reconcileExecutor(t.Context(), fixture.session); err != nil {
			t.Fatal(err)
		}
	}
	if err := fixture.writer.Get(t.Context(), client.ObjectKeyFromObject(fixture.sandbox), fixture.sandbox); err != nil {
		t.Fatal(err)
	}
	if fixture.sandbox.Spec.OperatingMode != sandboxv1beta1.SandboxOperatingModeRunning || fixture.sandbox.Spec.PodTemplate.ObjectMeta.Annotations[sandbox.ConfigAnnotation] != fixture.session.Status.ExecutorConfigDigest {
		t.Fatal("native runtime did not resume with the new credential baseline")
	}
	if !reflect.DeepEqual(identities, fixture.session.Status.SandboxSecretSources) {
		t.Fatal("rotation changed frozen Secret identities")
	}
	encoded, err := json.Marshal([]any{fixture.session.Status, fixture.claim, fixture.sandbox})
	if err != nil || strings.Contains(string(encoded), _sandboxCredentialOriginal) || strings.Contains(string(encoded), _sandboxCredentialRotated) {
		t.Fatal("protected credential values entered native configuration or status", err)
	}
}

func TestSandboxCredentialRevocationStopsComputeBeforeConnectionObservation(t *testing.T) {
	for _, name := range []string{"deleted", "missing key", "recreated UID"} {
		t.Run(name, func(t *testing.T) {
			fixture, secret := sandboxCredentialFixture(t)
			switch name {
			case "deleted":
				if err := fixture.writer.Client.Delete(t.Context(), secret); err != nil {
					t.Fatal(err)
				}
			case "missing key":
				secret.Data = nil
				if err := fixture.writer.Update(t.Context(), secret); err != nil {
					t.Fatal(err)
				}
			case "recreated UID":
				if err := fixture.writer.Client.Delete(t.Context(), secret); err != nil {
					t.Fatal(err)
				}
				secret.UID, secret.ResourceVersion = "replacement-source", ""
				if err := fixture.writer.Create(t.Context(), secret); err != nil {
					t.Fatal(err)
				}
			}
			unavailable := errors.New("connection observer must not run")
			fixture.reconciler.Connections = &connectionStub{err: unavailable}
			_, err := fixture.reconciler.reconcileExecutor(t.Context(), fixture.session)
			if err == nil || errors.Is(err, unavailable) {
				t.Fatal("credential revocation depended on the connection provider", err)
			}
			if err := fixture.writer.Get(t.Context(), client.ObjectKeyFromObject(fixture.sandbox), fixture.sandbox); err != nil {
				t.Fatal(err)
			}
			if fixture.sandbox.Spec.OperatingMode != sandboxv1beta1.SandboxOperatingModeSuspended {
				t.Fatal("unavailable protected source left compute running")
			}
			condition := meta.FindStatusCondition(fixture.session.Status.Conditions, readyCondition)
			if condition == nil || condition.Reason != "SandboxCredentialUnavailable" || condition.Status != metav1.ConditionFalse {
				t.Fatal("credential revocation did not hold readiness")
			}
			if err := fixture.writer.Get(t.Context(), client.ObjectKeyFromObject(fixture.workspace), fixture.workspace); err != nil {
				t.Fatal("credential revocation removed the workspace", err)
			}
		})
	}
}

func TestSandboxCredentialIdentityMustBeFrozenBeforeClaimAllocation(t *testing.T) {
	fixture, secret := sandboxCredentialFixture(t)
	fixture.session.Status.SandboxSecretSources = nil
	if _, err := fixture.reconciler.sandboxCredentials(t.Context(), fixture.session, fixture.template); err == nil {
		t.Fatal("already allocated claim retroactively trusted credentials")
	}
	fixture.session.Status.SandboxClaimUID = ""
	if _, err := fixture.reconciler.sandboxCredentials(t.Context(), fixture.session, fixture.template); err != nil {
		t.Fatal(err)
	}
	if len(fixture.session.Status.SandboxSecretSources) != 2 {
		t.Fatal("not all template Secret identities were frozen")
	}
	if err := fixture.writer.Client.Delete(t.Context(), secret); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.reconciler.sandboxCredentials(t.Context(), fixture.session, fixture.template); !apierrors.IsNotFound(err) {
		t.Fatal("missing frozen source was silently accepted", err)
	}
}
