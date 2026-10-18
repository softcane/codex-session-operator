package controller

import (
	"context"
	"errors"
	"testing"
	"time"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	"github.com/softcane/codex-session-operator/internal/openaiapi"
	"github.com/softcane/codex-session-operator/internal/sandbox"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const _runtimePendingState = "pending"
const _runtimeSessionIdentityChange = "Session identity"

type executorRuntimeProvider struct {
	SessionProvider
	state   openaiapi.HistoryEnvironment
	err     error
	onRead  func()
	reads   int
	bounded bool
}

func (p *executorRuntimeProvider) ConnectionState(ctx context.Context, _ string) (openaiapi.HistoryEnvironment, error) {
	p.reads++
	deadline, ok := ctx.Deadline()
	p.bounded = ok && time.Until(deadline) > 0 && time.Until(deadline) <= 20*time.Second
	if p.onRead != nil {
		p.onRead()
	}
	return p.state, p.err
}

func runtimeExecutorFixture(t *testing.T) (*SessionReconciler, *agentsv1alpha1.Session, *connectionStub, *executorRuntimeProvider, func() *agentsv1alpha1.Session) {
	t.Helper()
	return runtimeExecutorStorageFixture(t, false)
}

func runtimeExecutorStorageFixture(t *testing.T, persistent bool) (*SessionReconciler, *agentsv1alpha1.Session, *connectionStub, *executorRuntimeProvider, func() *agentsv1alpha1.Session) {
	t.Helper()
	policy := ""
	if persistent {
		policy = sandbox.WorkspaceClaimDelete
	}
	return runtimeExecutorPolicyFixture(t, policy)
}

func runtimeExecutorPolicyFixture(t *testing.T, claimPolicy string) (*SessionReconciler, *agentsv1alpha1.Session, *connectionStub, *executorRuntimeProvider, func() *agentsv1alpha1.Session) {
	t.Helper()
	r, session, observer := preparationFixture(t)
	if claimPolicy != "" {
		session.Status.EnvironmentSnapshot.Profile.SelfHosted.WorkspaceStorage = &agentsv1alpha1.WorkspaceStorage{
			Type: sandbox.WorkspaceStoragePersistent, StorageClassName: workspaceTestClass, ClaimDeletionPolicy: claimPolicy}
		r.WorkspaceStorageClasses = []string{workspaceTestClass}
		claim, err := sandbox.WorkspaceClaim(session)
		if err != nil {
			t.Fatal(err)
		}
		claim.UID = "runtime-claim-uid"
		if claimPolicy == sandbox.WorkspaceClaimDelete {
			if err := controllerutil.SetControllerReference(session, claim, r.Scheme); err != nil {
				t.Fatal(err)
			}
		} else {
			session.Status.WorkspaceClaimCreationAttempted = true
		}
		if err := r.Create(t.Context(), claim); err != nil {
			t.Fatal(err)
		}
		session.Status.WorkspaceClaimUID = string(claim.UID)
		if err := r.Status().Update(t.Context(), session); err != nil {
			t.Fatal(err)
		}
	}
	lifecycle := &sessionProviderStub{remote: preparationRemote(session)}
	provider := &executorRuntimeProvider{SessionProvider: lifecycle,
		state: openaiapi.HistoryEnvironment{ID: session.Status.EnvironmentID, Type: executorRuntimeSelfHostedType, Status: "connected"}}
	r.OpenAI = provider
	r.Configuration = &configuredSessionStub{lifecycle: lifecycle}
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(session)}
	reconcile := func() *agentsv1alpha1.Session {
		t.Helper()
		if _, err := r.Reconcile(t.Context(), request); err != nil {
			t.Fatal(err)
		}
		var current agentsv1alpha1.Session
		if err := r.APIReader.Get(t.Context(), request.NamespacedName, &current); err != nil {
			t.Fatal(err)
		}
		return &current
	}
	reconcile() // Finalizer.
	reconcile() // First Pod with sequence baseline zero.
	pod := preparationPod(t, r, session)
	healthyPreparationPod(t, r, pod, "runtime-initial-pod", 0)
	observer.state.Connected, observer.state.Sequence = true, 1
	if !meta.IsStatusConditionTrue(reconcile().Status.Conditions, readyCondition) || provider.reads != 0 {
		t.Fatal("fresh connected SSE did not establish readiness independently")
	}
	if err := r.Delete(t.Context(), pod, client.GracePeriodSeconds(0)); err != nil {
		t.Fatal(err)
	}
	reconcile()
	pod = preparationPod(t, r, session)
	if pod.Annotations["agents.softcane.io/connection-sequence"] != "1" {
		t.Fatal("replacement did not freeze the previous stream sequence")
	}
	healthyPreparationPod(t, r, pod, "runtime-replacement-pod", 0)
	return r, session, observer, provider, reconcile
}

type executorRuntimeReader struct {
	client.Reader
	afterPodRead func()
}

func (r *executorRuntimeReader) Get(ctx context.Context, key client.ObjectKey, object client.Object, opts ...client.GetOption) error {
	err := r.Reader.Get(ctx, key, object, opts...)
	if _, pod := object.(*corev1.Pod); err == nil && pod && r.afterPodRead != nil {
		r.afterPodRead()
	}
	return err
}

func TestReconcileRecoversIdleReplacementWithoutNewConnectedEvent(t *testing.T) {
	for _, connected := range []bool{false, true} {
		t.Run(map[bool]string{false: "older negative notification", true: "older connected notification"}[connected], func(t *testing.T) {
			r, session, observer, provider, reconcile := runtimeExecutorFixture(t)
			observer.state.Connected = connected
			before := observer.state
			current := reconcile()
			if !meta.IsStatusConditionTrue(current.Status.Conditions, readyCondition) ||
				meta.FindStatusCondition(current.Status.Conditions, connectedCondition).Reason != "EnvironmentRuntimeObserved" ||
				current.Status.WorkspacePreparation == nil || current.Status.WorkspacePreparation.PodUID != "runtime-replacement-pod" {
				t.Fatal("current runtime connection did not recover the qualified idle replacement")
			}
			if provider.reads != 1 || !provider.bounded || observer.state != before {
				t.Fatal("recovery used an unbounded read or fabricated stream connection evidence")
			}
			if preparationPod(t, r, session).UID != "runtime-replacement-pod" || current.Status.OpenAISessionID != session.Status.OpenAISessionID {
				t.Fatal("runtime observation changed compute or remote identity")
			}
		})
	}
}

func TestExecutorRuntimeRecoveryFailsClosedOnUnverifiedState(t *testing.T) {
	for _, name := range []string{_runtimePendingState, "disconnected", "failed", "malformed", "foreign environment", "foreign type", "missing runtime", "timeout"} {
		t.Run(name, func(t *testing.T) {
			_, _, _, provider, reconcile := runtimeExecutorFixture(t)
			switch name {
			case "malformed":
				provider.state = openaiapi.HistoryEnvironment{}
			case "foreign environment":
				provider.state.ID = "foreign-environment"
			case "foreign type":
				provider.state.Type = "openai_hosted"
			case "missing runtime":
				provider.err = openaiapi.ErrNotFound
			case "timeout":
				provider.err = context.DeadlineExceeded
			default:
				provider.state.Status = name
			}
			current := reconcile()
			if meta.IsStatusConditionTrue(current.Status.Conditions, readyCondition) || meta.IsStatusConditionTrue(current.Status.Conditions, connectedCondition) {
				t.Fatal("unverified current runtime state authorized execution")
			}
			if provider.reads != 1 || !provider.bounded {
				t.Fatal("runtime observation was not a single bounded read")
			}
		})
	}
}

func TestExecutorRuntimeRecoveryVetoesInterveningWatchTransitions(t *testing.T) {
	for _, name := range []string{"negative transition", "stream loss", "new epoch", "observation error"} {
		t.Run(name, func(t *testing.T) {
			_, _, observer, provider, reconcile := runtimeExecutorFixture(t)
			provider.onRead = func() {
				switch name {
				case "negative transition":
					observer.state.Connected = false
					observer.state.Sequence++
				case "stream loss":
					observer.state = openaiapi.Connection{}
				case "new epoch":
					observer.state.Epoch = "replacement-stream"
				case "observation error":
					observer.err = errors.New("stream unavailable")
				}
			}
			current := reconcile()
			if meta.IsStatusConditionTrue(current.Status.Conditions, readyCondition) || meta.IsStatusConditionTrue(current.Status.Conditions, connectedCondition) {
				t.Fatal("older runtime response overrode a newer watch transition or loss")
			}
		})
	}
}

func TestExecutorRuntimeRecoveryVetoesTransitionDuringFinalPodRead(t *testing.T) {
	r, _, observer, provider, reconcile := runtimeExecutorFixture(t)
	provider.onRead = func() {
		r.APIReader = &executorRuntimeReader{Reader: r.APIReader, afterPodRead: func() {
			observer.state.Connected = false
			observer.state.Sequence++
		}}
	}
	current := reconcile()
	if meta.IsStatusConditionTrue(current.Status.Conditions, readyCondition) || meta.IsStatusConditionTrue(current.Status.Conditions, connectedCondition) {
		t.Fatal("watch transition during the final compute reread was ignored")
	}
}

func TestExecutorRuntimeRecoveryVetoesPersistentClaimReplacement(t *testing.T) {
	r, session, _, provider, reconcile := runtimeExecutorStorageFixture(t, true)
	provider.onRead = func() {
		var claim corev1.PersistentVolumeClaim
		key := client.ObjectKey{Namespace: session.Namespace, Name: sandbox.WorkspaceClaimNameForUID(string(session.UID))}
		if err := r.APIReader.Get(t.Context(), key, &claim); err != nil {
			t.Fatal(err)
		}
		claim.UID = "raced-claim-uid"
		if err := r.Update(t.Context(), &claim); err != nil {
			t.Fatal(err)
		}
	}
	current := reconcile()
	if meta.IsStatusConditionTrue(current.Status.Conditions, readyCondition) || current.Status.WorkspacePreparation != nil {
		t.Fatal("runtime response authorized a replaced persistent workspace claim")
	}
}

func TestExecutorRuntimeRecoveryVetoesInterveningComputeChanges(t *testing.T) {
	for _, name := range []string{"Pod UID", "config digest", "container ID", "start time", "restart count", "init failure", "owner", "sequence baseline", _runtimeSessionIdentityChange} {
		t.Run(name, func(t *testing.T) {
			r, session, _, provider, reconcile := runtimeExecutorFixture(t)
			provider.onRead = func() {
				pod := preparationPod(t, r, session)
				switch name {
				case "Pod UID":
					pod.UID = "raced-replacement"
				case "config digest":
					pod.Annotations[sandbox.ConfigAnnotation] = "raced-config"
				case "owner":
					pod.OwnerReferences[0].UID = "foreign-session"
				case "sequence baseline":
					pod.Annotations["agents.softcane.io/connection-sequence"] = "raced-sequence-baseline"
				case _runtimeSessionIdentityChange:
					var current agentsv1alpha1.Session
					if err := r.APIReader.Get(t.Context(), client.ObjectKeyFromObject(session), &current); err != nil {
						t.Fatal(err)
					}
					current.Status.EnvironmentID = "foreign-runtime"
					if err := r.Status().Update(t.Context(), &current); err != nil {
						t.Fatal(err)
					}
					return
				default:
					switch name {
					case "container ID":
						pod.Status.ContainerStatuses[0].ContainerID = "containerd://raced-instance"
					case "start time":
						pod.Status.ContainerStatuses[0].State.Running.StartedAt = metav1.NewTime(time.Unix(1700000001, 0))
					case "restart count":
						pod.Status.ContainerStatuses[0].RestartCount++
					case "init failure":
						pod.Status.InitContainerStatuses[0].State.Terminated.ExitCode = 1
					}
					if err := r.Status().Update(t.Context(), pod); err != nil {
						t.Fatal(err)
					}
					return
				}
				if err := r.Update(t.Context(), pod); err != nil {
					t.Fatal(err)
				}
			}
			if name == _runtimeSessionIdentityChange {
				// Concurrent Session status mutation must also defeat the caller's
				// optimistic status publication, so invoke and inspect its rejection.
				_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(session)})
				if err == nil {
					t.Fatal("concurrent Session identity edit was overwritten")
				}
				return
			}
			current := reconcile()
			if meta.IsStatusConditionTrue(current.Status.Conditions, readyCondition) || meta.IsStatusConditionTrue(current.Status.Conditions, connectedCondition) ||
				current.Status.WorkspacePreparation != nil {
				t.Fatal("runtime response authorized changed compute or preparation")
			}
		})
	}
}

func TestExecutorRuntimeRecoveryVetoesSameUIDClaimOwnershipAndSpecDrift(t *testing.T) {
	for _, policy := range []string{sandbox.WorkspaceClaimDelete, sandbox.WorkspaceClaimRetain} {
		for _, mutation := range []string{"owner policy", "requested size"} {
			t.Run(policy+"/"+mutation, func(t *testing.T) {
				r, session, _, provider, reconcile := runtimeExecutorPolicyFixture(t, policy)
				provider.onRead = func() {
					var claim corev1.PersistentVolumeClaim
					key := client.ObjectKey{Namespace: session.Namespace, Name: sandbox.WorkspaceClaimNameForUID(string(session.UID))}
					if err := r.APIReader.Get(t.Context(), key, &claim); err != nil {
						t.Fatal(err)
					}
					if mutation == "requested size" {
						claim.Spec.Resources.Requests[corev1.ResourceStorage] = resource.MustParse("2Gi")
					} else {
						claim.OwnerReferences = []metav1.OwnerReference{{APIVersion: agentsv1alpha1.SchemeGroupVersion.String(), Kind: "Session", Name: "raced-claim-owner", UID: "foreign-owner", Controller: new(true)}}
					}
					if err := r.Update(t.Context(), &claim); err != nil {
						t.Fatal(err)
					}
				}
				current := reconcile()
				if meta.IsStatusConditionTrue(current.Status.Conditions, readyCondition) || current.Status.WorkspacePreparation != nil {
					t.Fatal("fresh runtime connection overrode same-UID claim ownership or spec drift")
				}
			})
		}
	}
}

func TestExecutorRuntimeRecoveryRejectsMalformedCreationBaseline(t *testing.T) {
	r, session, _, provider, reconcile := runtimeExecutorFixture(t)
	pod := preparationPod(t, r, session)
	pod.Annotations["agents.softcane.io/connection-sequence"] = "invalid"
	if err := r.Update(t.Context(), pod); err != nil {
		t.Fatal(err)
	}
	current := reconcile()
	if provider.reads != 0 || meta.IsStatusConditionTrue(current.Status.Conditions, readyCondition) {
		t.Fatal("runtime fallback bypassed an invalid Pod connection baseline")
	}
}

func TestExecutorRuntimeRecoveryRequiresCurrentStreamSubscription(t *testing.T) {
	for _, name := range []string{"unsubscribed", "missing epoch", "negative sequence"} {
		t.Run(name, func(t *testing.T) {
			_, _, observer, provider, reconcile := runtimeExecutorFixture(t)
			switch name {
			case "unsubscribed":
				observer.state.Subscribed = false
			case "missing epoch":
				observer.state.Epoch = ""
			case "negative sequence":
				observer.state.Sequence = -1
			}
			current := reconcile()
			if provider.reads != 0 || meta.IsStatusConditionTrue(current.Status.Conditions, readyCondition) {
				t.Fatal("runtime fallback bypassed missing or invalid stream subscription evidence")
			}
		})
	}
}
