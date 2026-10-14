// Package observability exports aggregate lifecycle state without object identities.
package observability

import (
	"context"
	"errors"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	_collectionTimeout = 5 * time.Second
	_kindLabel         = "kind"
)

// Resources reads the manager's cache on demand. A failed list emits collection
// failure, not a healthy zero; it never queries OpenAI or reads Secret material.
type Resources struct {
	reader     client.Reader
	now        func() time.Time
	resources  *prometheus.Desc
	claims     *prometheus.Desc
	cleanupAge *prometheus.Desc
	success    *prometheus.Desc
}

var _ prometheus.Collector = (*Resources)(nil)

type sample struct {
	namespace  string
	state      string
	unresolved bool
	deleting   *metav1.Time
}

// NewResources requires a cache-backed reader. Namespace is the only naming
// dimension; resource names, reasons, UIDs, labels and remote IDs are excluded.
func NewResources(reader client.Reader) *Resources {
	labels := []string{_kindLabel, "namespace"}
	return &Resources{reader: reader, now: time.Now,
		resources: prometheus.NewDesc("codex_operator_resources",
			"Current CR counts by readiness for the current generation; duplicated across manager replicas.",
			[]string{_kindLabel, "namespace", "state"}, nil),
		claims: prometheus.NewDesc("codex_operator_creation_claims_unresolved",
			"CRs with a one-time remote creation claim but no recorded remote ID; includes creation in progress.", labels, nil),
		cleanupAge: prometheus.NewDesc("codex_operator_cleanup_oldest_seconds",
			"Age of the oldest deleting CR still present in each namespace and kind.", labels, nil),
		success: prometheus.NewDesc("codex_operator_resource_collection_success",
			"Whether the latest cached list for a CR kind succeeded; missing state samples are not healthy zeros.", []string{_kindLabel}, nil)}
}

func (c *Resources) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.resources
	ch <- c.claims
	ch <- c.cleanupAge
	ch <- c.success
}

func (c *Resources) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), _collectionTimeout)
	defer cancel()
	for _, kind := range []string{"Agent", "Environment", "Vault", "VaultCredential", "Session"} {
		values, err := c.list(ctx, kind)
		if err != nil {
			ch <- prometheus.MustNewConstMetric(c.success, prometheus.GaugeValue, 0, kind)
			continue
		}
		ch <- prometheus.MustNewConstMetric(c.success, prometheus.GaugeValue, 1, kind)
		c.collectKind(ch, kind, values)
	}
}

func (c *Resources) collectKind(ch chan<- prometheus.Metric, kind string, values []sample) {
	type counts struct {
		states map[string]float64
		claims float64
		oldest float64
	}
	byNamespace := make(map[string]*counts)
	now := c.now()
	for _, value := range values {
		ns := byNamespace[value.namespace]
		if ns == nil {
			ns = &counts{states: make(map[string]float64, 4)}
			byNamespace[value.namespace] = ns
		}
		ns.states[value.state]++
		if value.unresolved {
			ns.claims++
		}
		if value.deleting != nil {
			ns.oldest = max(ns.oldest, now.Sub(value.deleting.Time).Seconds())
		}
	}
	for namespace, ns := range byNamespace {
		for _, state := range []string{"ready", "not_ready", "unknown", "terminating"} {
			ch <- prometheus.MustNewConstMetric(c.resources, prometheus.GaugeValue, ns.states[state], kind, namespace, state)
		}
		ch <- prometheus.MustNewConstMetric(c.claims, prometheus.GaugeValue, ns.claims, kind, namespace)
		ch <- prometheus.MustNewConstMetric(c.cleanupAge, prometheus.GaugeValue, ns.oldest, kind, namespace)
	}
}

func lifecycleSample(obj metav1.Object, conditions []metav1.Condition, attempted bool, remoteID string) sample {
	value := sample{namespace: obj.GetNamespace(), state: "unknown", unresolved: attempted && remoteID == ""}
	if deleting := obj.GetDeletionTimestamp(); deleting != nil && !deleting.IsZero() {
		value.state, value.deleting = "terminating", deleting
		return value
	}
	condition := meta.FindStatusCondition(conditions, "Ready")
	if condition == nil || condition.ObservedGeneration != obj.GetGeneration() {
		return value
	}
	switch condition.Status {
	case metav1.ConditionTrue:
		value.state = "ready"
	case metav1.ConditionFalse:
		value.state = "not_ready"
	}
	return value
}

func (c *Resources) list(ctx context.Context, kind string) ([]sample, error) {
	if c.reader == nil {
		return nil, errors.New("lifecycle metrics reader is not configured")
	}
	var values []sample
	switch kind {
	case "Agent":
		var objects agentsv1alpha1.AgentList
		if err := c.reader.List(ctx, &objects); err != nil {
			return nil, err
		}
		for i := range objects.Items {
			o := &objects.Items[i]
			values = append(values, lifecycleSample(o, o.Status.Conditions, o.Status.CreationAttempted, o.Status.OpenAIAgentID))
		}
	case "Environment":
		var objects agentsv1alpha1.EnvironmentList
		if err := c.reader.List(ctx, &objects); err != nil {
			return nil, err
		}
		for i := range objects.Items {
			o := &objects.Items[i]
			values = append(values, lifecycleSample(o, o.Status.Conditions, o.Status.CreationAttempted, o.Status.OpenAITemplateID))
		}
	case "Vault":
		var objects agentsv1alpha1.VaultList
		if err := c.reader.List(ctx, &objects); err != nil {
			return nil, err
		}
		for i := range objects.Items {
			o := &objects.Items[i]
			values = append(values, lifecycleSample(o, o.Status.Conditions, o.Status.CreationAttempted, o.Status.OpenAIVaultID))
		}
	case "VaultCredential":
		var objects agentsv1alpha1.VaultCredentialList
		if err := c.reader.List(ctx, &objects); err != nil {
			return nil, err
		}
		for i := range objects.Items {
			o := &objects.Items[i]
			values = append(values, lifecycleSample(o, o.Status.Conditions, o.Status.CreationAttempted, o.Status.OpenAICredentialID))
		}
	case "Session":
		var objects agentsv1alpha1.SessionList
		if err := c.reader.List(ctx, &objects); err != nil {
			return nil, err
		}
		for i := range objects.Items {
			o := &objects.Items[i]
			values = append(values, lifecycleSample(o, o.Status.Conditions, o.Status.CreationAttempted, o.Status.OpenAISessionID))
		}
	}
	return values, nil
}
