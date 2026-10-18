package gateway

import (
	"context"
	"net/http"
	"testing"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestLimitedSpendPausesMessagesAndQueuedDeliveryUntilCurrentReady(t *testing.T) {
	store := testStore(t)
	p := simulatorForTest()
	session := testSession()
	session.Generation = 2
	session.Spec.SpendControl = &agentsv1alpha1.SessionSpendControl{Mode: _scenarioLimited, LimitCents: func() *int64 { n := int64(100); return &n }()}
	g, k8s := testGateway(t, store, p, session)
	path := _testMessagePath
	if got := request(g, http.MethodPost, path, _token, "first", `{"text":"hello"}`).Code; got != http.StatusServiceUnavailable {
		t.Fatalf("unverified cap accepted message: %d", got)
	}
	if err := g.enqueue(t.Context(), session, "queued-before-observation", "queued message", "", nil); err != nil {
		t.Fatal(err)
	}
	if err := g.sync(context.Background(), string(session.UID)); err != nil {
		t.Fatal(err)
	}
	if p.commits != 0 || mustRecord(t, store).Submissions[0].Attempts != 0 {
		t.Fatal("queued message reached provider before cap verification")
	}
	var saved agentsv1alpha1.Session
	if err := k8s.Get(t.Context(), client.ObjectKeyFromObject(session), &saved); err != nil {
		t.Fatal(err)
	}
	saved.Status.Conditions = []metav1.Condition{{Type: "Ready", Status: metav1.ConditionTrue, Reason: _testObservedReason, Message: "Ready", LastTransitionTime: metav1.Now(), ObservedGeneration: 1}}
	if err := k8s.Update(t.Context(), &saved); err != nil {
		t.Fatal(err)
	}
	if got := request(g, http.MethodPost, path, _token, "second", `{"text":"hello"}`).Code; got != http.StatusServiceUnavailable {
		t.Fatalf("stale Ready accepted message: %d", got)
	}
	saved.Status.Conditions[0].ObservedGeneration = saved.Generation
	if err := k8s.Update(t.Context(), &saved); err != nil {
		t.Fatal(err)
	}
	if err := g.sync(context.Background(), string(session.UID)); err != nil {
		t.Fatal(err)
	}
	if p.commits != 1 {
		t.Fatalf("verified cap did not release queued work: %d", p.commits)
	}
	if got := request(g, http.MethodPost, path, _token, "second", `{"text":"hello"}`).Code; got != http.StatusAccepted {
		t.Fatalf("verified cap rejected new message: %d", got)
	}
}
