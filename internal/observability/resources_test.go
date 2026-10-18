package observability

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

const _metricsNamespace = "team"

func resourceReader(t *testing.T, objects ...client.Object) client.Reader {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := agentsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
}

func readyCondition(status metav1.ConditionStatus, generation int64) []metav1.Condition {
	return []metav1.Condition{{Type: "Ready", Status: status, ObservedGeneration: generation, Reason: "test"}}
}

func TestResourcesCurrentGenerationAndPrivacy(t *testing.T) {
	objects := []*agentsv1alpha1.Session{
		{ObjectMeta: metav1.ObjectMeta{Name: "private-object-a", Namespace: _metricsNamespace, Generation: 2},
			Status: agentsv1alpha1.SessionStatus{OpenAISessionID: "private-remote-a", Conditions: readyCondition(metav1.ConditionTrue, 2)}},
		{ObjectMeta: metav1.ObjectMeta{Name: "private-object-b", Namespace: _metricsNamespace, Generation: 2},
			Status: agentsv1alpha1.SessionStatus{Conditions: readyCondition(metav1.ConditionFalse, 2)}},
		{ObjectMeta: metav1.ObjectMeta{Name: "private-object-c", Namespace: _metricsNamespace, Generation: 2},
			Status: agentsv1alpha1.SessionStatus{CreationAttempted: true, Conditions: readyCondition(metav1.ConditionTrue, 1)}},
	}
	reader := resourceReader(t, objects[0], objects[1], objects[2])
	collector := NewResources(reader)
	if err := testutil.CollectAndCompare(collector, strings.NewReader(`
# HELP codex_operator_resources Current CR counts by readiness for the current generation; duplicated across manager replicas.
# TYPE codex_operator_resources gauge
codex_operator_resources{kind="Session",namespace="team",state="ready"} 1
codex_operator_resources{kind="Session",namespace="team",state="not_ready"} 1
codex_operator_resources{kind="Session",namespace="team",state="unknown"} 1
codex_operator_resources{kind="Session",namespace="team",state="terminating"} 0
# HELP codex_operator_creation_claims_unresolved CRs with a one-time remote creation claim but no recorded remote ID; includes creation in progress.
# TYPE codex_operator_creation_claims_unresolved gauge
codex_operator_creation_claims_unresolved{kind="Session",namespace="team"} 1
`), "codex_operator_resources", "codex_operator_creation_claims_unresolved"); err != nil {
		t.Fatal(err)
	}
	registry := prometheus.NewPedanticRegistry()
	registry.MustRegister(collector)
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		for _, metric := range family.Metric {
			for _, label := range metric.Label {
				if strings.Contains(label.GetValue(), "private-") {
					t.Fatal("object or remote identity leaked into metrics")
				}
			}
		}
	}
}

func TestResourcesDeletionAgeAndRecreatedIdentity(t *testing.T) {
	now := time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)
	old := metav1.NewTime(now.Add(-20 * time.Minute))
	newer := metav1.NewTime(now.Add(-time.Minute))
	reader := resourceReader(t,
		&agentsv1alpha1.Agent{ObjectMeta: metav1.ObjectMeta{Name: "old", Namespace: _metricsNamespace, Generation: 1,
			DeletionTimestamp: &old, Finalizers: []string{"hold"}}, Status: agentsv1alpha1.AgentStatus{
			CreationAttempted: true, OpenAIAgentID: "known", Conditions: readyCondition(metav1.ConditionTrue, 1)}},
		&agentsv1alpha1.Agent{ObjectMeta: metav1.ObjectMeta{Name: "newer", Namespace: _metricsNamespace, DeletionTimestamp: &newer,
			Finalizers: []string{"hold"}}})
	collector := NewResources(reader)
	collector.now = func() time.Time { return now }
	if err := testutil.CollectAndCompare(collector, strings.NewReader(`
# HELP codex_operator_cleanup_oldest_seconds Age of the oldest deleting CR still present in each namespace and kind.
# TYPE codex_operator_cleanup_oldest_seconds gauge
codex_operator_cleanup_oldest_seconds{kind="Agent",namespace="team"} 1200
# HELP codex_operator_creation_claims_unresolved CRs with a one-time remote creation claim but no recorded remote ID; includes creation in progress.
# TYPE codex_operator_creation_claims_unresolved gauge
codex_operator_creation_claims_unresolved{kind="Agent",namespace="team"} 0
`), "codex_operator_cleanup_oldest_seconds", "codex_operator_creation_claims_unresolved"); err != nil {
		t.Fatal(err)
	}
}

type failingReader struct {
	client.Reader
	profile bool
}

func (r failingReader) List(ctx context.Context, objects client.ObjectList, options ...client.ListOption) error {
	_, sessionList := objects.(*agentsv1alpha1.SessionList)
	_, profileList := objects.(*agentsv1alpha1.ProfileList)
	if sessionList && !r.profile || profileList && r.profile {
		return errors.New("private-provider-error")
	}
	return r.Reader.List(ctx, objects, options...)
}

func TestResourcesListFailureIsNotHealthyZero(t *testing.T) {
	collector := NewResources(failingReader{Reader: resourceReader(t)})
	registry := prometheus.NewPedanticRegistry()
	registry.MustRegister(collector)
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != "codex_operator_resource_collection_success" {
			t.Fatal("failed/empty list emitted fabricated state", family.GetName())
		}
		if len(family.Metric) != 6 {
			t.Fatal("missing per-kind collection results")
		}
		for _, metric := range family.Metric {
			kind := metric.Label[0].GetValue()
			want := 1.0
			if kind == "Session" {
				want = 0
			}
			if metric.GetGauge().GetValue() != want {
				t.Fatal("wrong collection result", kind)
			}
		}
	}
}

func TestResourcesAllKindsAndResolvedClaims(t *testing.T) {
	const remoteID = "remote"
	metadata := func(name string) metav1.ObjectMeta {
		return metav1.ObjectMeta{Name: name, Namespace: _metricsNamespace, Generation: 1}
	}
	reader := resourceReader(t,
		&agentsv1alpha1.Agent{ObjectMeta: metadata("agent"), Status: agentsv1alpha1.AgentStatus{
			CreationAttempted: true, OpenAIAgentID: remoteID, Conditions: readyCondition(metav1.ConditionTrue, 1)}},
		&agentsv1alpha1.Environment{ObjectMeta: metadata("environment"), Status: agentsv1alpha1.EnvironmentStatus{
			Conditions: readyCondition(metav1.ConditionTrue, 1)}},
		&agentsv1alpha1.Vault{ObjectMeta: metadata("vault"), Status: agentsv1alpha1.VaultStatus{
			CreationAttempted: true, OpenAIVaultID: remoteID, Conditions: readyCondition(metav1.ConditionTrue, 1)}},
		&agentsv1alpha1.VaultCredential{ObjectMeta: metadata("credential"), Status: agentsv1alpha1.VaultCredentialStatus{
			CreationAttempted: true, OpenAICredentialID: remoteID, Conditions: readyCondition(metav1.ConditionTrue, 1)}},
		&agentsv1alpha1.Session{ObjectMeta: metadata("session"), Status: agentsv1alpha1.SessionStatus{
			CreationAttempted: true, OpenAISessionID: remoteID, Conditions: readyCondition(metav1.ConditionTrue, 1)}},
		&agentsv1alpha1.Profile{ObjectMeta: metadata("profile"), Status: agentsv1alpha1.ProfileStatus{
			Conditions: readyCondition(metav1.ConditionTrue, 1)}})
	registry := prometheus.NewPedanticRegistry()
	registry.MustRegister(NewResources(reader))
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	ready := 0
	for _, family := range families {
		for _, metric := range family.Metric {
			if family.GetName() == "codex_operator_creation_claims_unresolved" && metric.Gauge.GetValue() != 0 {
				t.Fatal("resolved identity reported as unresolved")
			}
			if family.GetName() == "codex_operator_resources" {
				for _, label := range metric.Label {
					if label.GetName() == "state" && label.GetValue() == "ready" && metric.Gauge.GetValue() == 1 {
						ready++
					}
				}
			}
		}
	}
	if ready != 6 {
		t.Fatal("missing ready resource kind", ready)
	}
}

func TestResourcesProfileReadinessAndPrivacy(t *testing.T) {
	t.Parallel()
	reader := resourceReader(t,
		&agentsv1alpha1.Profile{ObjectMeta: metav1.ObjectMeta{Name: "private-profile", Namespace: _metricsNamespace, UID: "private-profile-uid", Generation: 2},
			Status: agentsv1alpha1.ProfileStatus{ObservedGeneration: 2, PolicyDigest: "private-policy-digest",
				Agent:      &agentsv1alpha1.ProfileDependencyStatus{Name: "private-agent", UID: "private-agent-uid", Generation: 4},
				Conditions: readyCondition(metav1.ConditionTrue, 2)}},
		&agentsv1alpha1.Profile{ObjectMeta: metav1.ObjectMeta{Name: "private-profile", Namespace: "other-team", Generation: 2},
			Status: agentsv1alpha1.ProfileStatus{Conditions: readyCondition(metav1.ConditionTrue, 1)}})
	collector := NewResources(reader)
	if err := testutil.CollectAndCompare(collector, strings.NewReader(`
# HELP codex_operator_resources Current CR counts by readiness for the current generation; duplicated across manager replicas.
# TYPE codex_operator_resources gauge
codex_operator_resources{kind="Profile",namespace="team",state="ready"} 1
codex_operator_resources{kind="Profile",namespace="team",state="not_ready"} 0
codex_operator_resources{kind="Profile",namespace="team",state="unknown"} 0
codex_operator_resources{kind="Profile",namespace="team",state="terminating"} 0
codex_operator_resources{kind="Profile",namespace="other-team",state="ready"} 0
codex_operator_resources{kind="Profile",namespace="other-team",state="not_ready"} 0
codex_operator_resources{kind="Profile",namespace="other-team",state="unknown"} 1
codex_operator_resources{kind="Profile",namespace="other-team",state="terminating"} 0
# HELP codex_operator_creation_claims_unresolved CRs with a one-time remote creation claim but no recorded remote ID; includes creation in progress.
# TYPE codex_operator_creation_claims_unresolved gauge
codex_operator_creation_claims_unresolved{kind="Profile",namespace="team"} 0
codex_operator_creation_claims_unresolved{kind="Profile",namespace="other-team"} 0
`), "codex_operator_resources", "codex_operator_creation_claims_unresolved"); err != nil {
		t.Fatal(err)
	}
	registry := prometheus.NewPedanticRegistry()
	registry.MustRegister(collector)
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		for _, metric := range family.Metric {
			for _, label := range metric.Label {
				if strings.Contains(label.GetValue(), "private-") ||
					label.GetName() != "kind" && label.GetName() != "namespace" && label.GetName() != "state" {
					t.Fatalf("Profile identity or unbounded dimension leaked into metrics: %s", label)
				}
			}
		}
	}
}

func TestResourcesProfileListFailureIsNotHealthyZero(t *testing.T) {
	t.Parallel()
	reader := resourceReader(t, &agentsv1alpha1.Profile{ObjectMeta: metav1.ObjectMeta{Name: "hidden", Namespace: _metricsNamespace, Generation: 1},
		Status: agentsv1alpha1.ProfileStatus{Conditions: readyCondition(metav1.ConditionTrue, 1)}})
	registry := prometheus.NewPedanticRegistry()
	registry.MustRegister(NewResources(failingReader{Reader: reader, profile: true}))
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, family := range families {
		if family.GetName() != "codex_operator_resource_collection_success" {
			t.Fatalf("Profile list failure emitted fabricated state: %s", family.GetName())
		}
		for _, metric := range family.Metric {
			if metric.Label[0].GetValue() == "Profile" {
				found = true
				if metric.GetGauge().GetValue() != 0 {
					t.Fatal("Profile list failure was reported as successful")
				}
			}
		}
	}
	if !found {
		t.Fatal("Profile collection failure was not reported")
	}
}
