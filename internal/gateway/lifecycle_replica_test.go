package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestConversationComputeReadinessDoesNotDependOnHTTPReplicaStreamOwnership(t *testing.T) {
	store := testStore(t)
	leader, writer, _ := profileGateway(t, store)
	receipt := readConversationReceipt(t, conversationRequest(leader, http.MethodPost, _conversationPath, "replica-runtime", _profileTestBody))
	id := conversationRecordID(_testNamespace, _testConversationSubject, "replica-runtime")
	if err := leader.syncWork(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	record := readWorkRecord(t, store, id)
	current := workerWorkSession(t, writer, record)
	prepared, _ := executionSessionAndPod(t)
	prepared.ObjectMeta = *current.ObjectMeta.DeepCopy()
	prepared.Generation = 1
	prepared.Spec = current.Spec
	prepared.Status.EnvironmentSnapshot.Name = _profileTestEnvironment
	prepared.Status.EnvironmentSnapshot.UID = _profileTestEnvironmentUID
	prepared, pod := prepareExecutionSession(t, prepared)
	if err := writer.Update(t.Context(), prepared); err != nil {
		t.Fatal(err)
	}
	for _, object := range executionObjects(t, prepared, pod)[1:] {
		if err := writer.Client.Create(t.Context(), object); err != nil {
			t.Fatal(err)
		}
	}
	provider, ok := leader.provider.(*simulator)
	if !ok {
		t.Fatal("unexpected simulated provider")
	}
	provider.history.Metadata["kubernetes_uid"] = string(prepared.UID)
	if err := leader.sync(t.Context(), string(prepared.UID)); err != nil {
		t.Fatal(err)
	}
	follower, err := New(store, writer, provider, "writer", nil)
	if err != nil || follower.EnableProfiles(writer) != nil {
		t.Fatal(err)
	}
	// Readiness can be served by a port-forward pinned to the HTTP follower.
	// This must not manufacture a producer stream on that replica.
	detail := readLifecycleDetail(t, follower, receipt.ConversationURL)
	if !detail.Lifecycle.ExecutionReady || detail.Lifecycle.DeliveryHeld ||
		detail.Lifecycle.DeliveryObservation != _deliveryRuntimeOnly || len(follower.watches) != 0 {
		t.Fatal("follower confused local dispatch ownership with current compute readiness", detail.Lifecycle)
	}
	if ready, _ := follower.executionReady(context.Background(), prepared); ready {
		t.Fatal("read-only follower acquired an active worker execution capability")
	}
	leader.mu.Lock()
	w := leader.watches[string(prepared.UID)]
	leader.mu.Unlock()
	leader.observeExecutionEvent(w, json.RawMessage(`{"type":"agent.session.environment.disconnected","environment":{"id":"env-execution","type":"self_hosted","status":"disconnected"}}`))
	leaderDetail := readLifecycleDetail(t, leader, receipt.ConversationURL)
	if !leaderDetail.Lifecycle.ExecutionReady || !leaderDetail.Lifecycle.DeliveryHeld ||
		leaderDetail.Lifecycle.DeliveryObservation != _deliveryLocalWorkerHeld || !leaderDetail.Lifecycle.Actions.Continue {
		t.Fatal("leader omitted its inspected local stream hold", leaderDetail.Lifecycle)
	}
	if leader.executionWatchAvailable(prepared) {
		t.Fatal("read-only readiness transferred a fresh producer capability")
	}
	if detail := readLifecycleDetail(t, follower, receipt.ConversationURL); !detail.Lifecycle.ExecutionReady ||
		detail.Lifecycle.DeliveryObservation != _deliveryRuntimeOnly {
		t.Fatal("follower claimed observation of another process's stream state", detail.Lifecycle)
	}
	if ready, _ := leader.executionReady(t.Context(), prepared); !ready {
		t.Fatal("producer's own fresh runtime read did not supersede the older negative observation")
	}
}

func currentOwner(session client.Object) []metav1.OwnerReference {
	return []metav1.OwnerReference{{APIVersion: agentsv1alpha1.SchemeGroupVersion.String(), Kind: "Session",
		Name: session.GetName(), UID: session.GetUID(), Controller: new(true)}}
}
