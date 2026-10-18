package gateway

import (
	"errors"
	"net/http"
	"testing"
	"time"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestSelfHostedPodChangeDuringSendClaimHoldsOriginalInputUntilFreshProof(t *testing.T) {
	for _, tt := range []struct {
		name        string
		terminating bool
	}{
		{name: "terminating owned Pod", terminating: true},
		{name: "replacement owned Pod"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			session, pod := executionSessionAndPod(t)
			if tt.terminating {
				pod.Finalizers = []string{"test/executor-cleanup"}
			}
			store := &executionClaimFaultStore{memoryStore: testStore(t)}
			provider := simulatorForTest()
			g, writer := testGateway(t, store, provider, session, pod)
			const key = "pod-claim-change"
			if response := request(g, http.MethodPost, _testMessagePath, _token, key, `{"text":"Keep the original operation"}`); response.Code != http.StatusAccepted {
				t.Fatal(response.Code)
			}
			replacement := replacementExecutionPod(pod)
			store.afterClaim = func() {
				if err := writer.Delete(t.Context(), pod); err != nil {
					t.Fatal(err)
				}
				if !tt.terminating {
					if err := writer.Create(t.Context(), replacement); err != nil {
						t.Fatal(err)
					}
				}
			}
			if err := g.sync(t.Context(), _testUID); err != nil || provider.posts != 0 {
				t.Fatal("post-claim Pod change permitted stale execution", err)
			}
			accepted := mustRecord(t, store).Submissions[0]
			if accepted.Attempts != 1 || accepted.Accepted || accepted.RemoteKey != wireKey(_testUID, key) {
				t.Fatal("execution hold changed the pending input identity")
			}
			if tt.terminating {
				var current corev1.Pod
				if err := writer.Get(t.Context(), client.ObjectKeyFromObject(pod), &current); err != nil {
					t.Fatal(err)
				}
				if current.DeletionTimestamp.IsZero() {
					t.Fatal("fault did not leave the old Pod in its termination grace period")
				}
				current.Finalizers = nil
				if err := writer.Update(t.Context(), &current); err != nil {
					t.Fatal(err)
				}
				if err := writer.Create(t.Context(), replacement); err != nil {
					t.Fatal(err)
				}
			}
			// Pod identity alone is insufficient; keep the old persisted proof until
			// the controller has independently confirmed the replacement instance.
			if err := g.sync(t.Context(), _testUID); err != nil || provider.posts != 0 {
				t.Fatal("replacement ran under the previous Pod preparation proof", err)
			}
			var currentSession agentsv1alpha1.Session
			if err := writer.Get(t.Context(), client.ObjectKeyFromObject(session), &currentSession); err != nil {
				t.Fatal(err)
			}
			proof := currentSession.Status.WorkspacePreparation
			proof.PodUID = string(replacement.UID)
			proof.ContainerID = replacement.Status.ContainerStatuses[0].ContainerID
			proof.StartedAt = replacement.Status.ContainerStatuses[0].State.Running.StartedAt
			if err := writer.Update(t.Context(), &currentSession); err != nil {
				t.Fatal(err)
			}
			if err := g.sync(t.Context(), _testUID); err != nil || provider.commits != 1 ||
				mustRecord(t, store).Submissions[0].RemoteKey != accepted.RemoteKey {
				t.Fatal("fresh replacement proof failed to release the original operation once", err)
			}
		})
	}
}

func replacementExecutionPod(original *corev1.Pod) *corev1.Pod {
	replacement := original.DeepCopy()
	replacement.ResourceVersion = ""
	replacement.Finalizers = nil
	replacement.UID = "post-claim-replacement"
	replacement.Status.ContainerStatuses[0].ContainerID = "containerd://post-claim-instance"
	replacement.Status.ContainerStatuses[0].State.Running.StartedAt.Time =
		replacement.Status.ContainerStatuses[0].State.Running.StartedAt.Add(time.Minute)
	return replacement
}

func TestSessionUIDChangeDuringSendClaimNeverDeliversOrAdoptsReplacement(t *testing.T) {
	session, pod := executionSessionAndPod(t)
	store := &executionClaimFaultStore{memoryStore: testStore(t)}
	provider := simulatorForTest()
	g, writer := testGateway(t, store, provider, session, pod)
	const key = "session-claim-change"
	if response := request(g, http.MethodPost, _testMessagePath, _token, key, `{"text":"Bound to the original Session"}`); response.Code != http.StatusAccepted {
		t.Fatal(response.Code)
	}
	store.afterClaim = func() {
		if err := writer.Delete(t.Context(), session); err != nil {
			t.Fatal(err)
		}
		foreign := session.DeepCopy()
		foreign.UID, foreign.ResourceVersion = "post-claim-foreign-session", ""
		if err := writer.Create(t.Context(), foreign); err != nil {
			t.Fatal(err)
		}
	}
	if err := g.sync(t.Context(), _testUID); !errors.Is(err, errClosed) || provider.posts != 0 {
		t.Fatal("post-claim Session UID replacement permitted stale delivery", err)
	}
	if err := g.sync(t.Context(), _testUID); err != nil {
		t.Fatal(err)
	}
	original := mustRecord(t, store)
	if !original.Closed || original.Submissions[0].Accepted || original.Submissions[0].RemoteKey != wireKey(_testUID, key) || provider.posts != 0 {
		t.Fatal("foreign Session revived the original input or erased its identity")
	}
}
