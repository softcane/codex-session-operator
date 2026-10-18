package gateway

import (
	"context"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/felixge/httpsnoop"
	"github.com/prometheus/client_golang/prometheus"
)

const _metricsStorageTimeout = time.Second

const (
	_metricsRouteLabel  = "route"
	_metricsCodeLabel   = "code"
	_metricsMethodLabel = "method"
	_metricsOtherCode   = "other"
	_metricsMessages    = "session_messages"
	_metricsUnmatched   = "unmatched"
	_metricsCatalog     = "catalog"
	_metricsActions     = "actions"
	_metricsCancel      = "cancel"
)

// StorageStatistics contains operational counts and limits, never application
// records, connection addresses, owner identities, or protected values.
type StorageStatistics struct {
	DeliveryEnabled bool
	Acquired        int32
	Idle            int32
	Total           int32
	Max             int32
	ActiveLimit     int
	RetainedLimit   int
}

// StatisticsStorage provides one bounded read-only snapshot per scrape. The
// PostgreSQL implementation reads its pool and singleton metadata row only.
type StatisticsStorage interface {
	Statistics(context.Context) (StorageStatistics, error)
}

type gatewayMetrics struct {
	store         Storage
	requests      *prometheus.CounterVec
	duration      *prometheus.HistogramVec
	sseDuration   *prometheus.HistogramVec
	sseActive     *prometheus.GaugeVec
	workerLeader  atomic.Int64
	workerContext atomic.Pointer[workerMetricContext]
	workerEpoch   atomic.Int64
	messageStream atomic.Int64
	leaderDesc    *prometheus.Desc
	epochDesc     *prometheus.Desc
	streamDesc    *prometheus.Desc
	databaseDesc  *prometheus.Desc
	deliveryDesc  *prometheus.Desc
	poolDesc      *prometheus.Desc
	activeDesc    *prometheus.Desc
	retainedDesc  *prometheus.Desc
}

type workerMetricContext struct {
	ctx context.Context
}

var _ prometheus.Collector = (*gatewayMetrics)(nil)

// Metrics returns this gateway's collector. Callers register it with their own
// registry; construction never mutates Prometheus's default registry.
func (g *Gateway) Metrics() prometheus.Collector { return g.metricState() }

func (g *Gateway) metricState() *gatewayMetrics {
	g.metricsOnce.Do(func() {
		if g.metrics == nil {
			g.metrics = newGatewayMetrics(g.store)
		}
	})
	return g.metrics
}

func newGatewayMetrics(store Storage) *gatewayMetrics {
	return &gatewayMetrics{
		store: store,
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "codex_gateway_http_requests_total",
			Help: "Completed public gateway requests, including authentication and capacity rejections."}, []string{_metricsRouteLabel, _metricsMethodLabel, _metricsCodeLabel}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "codex_gateway_http_request_duration_seconds",
			Help: "Public HTTP request duration; established SSE connections are excluded.", Buckets: prometheus.DefBuckets}, []string{_metricsRouteLabel, _metricsMethodLabel}),
		sseDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "codex_gateway_sse_connection_duration_seconds",
			Help:    "Lifetime of completed established SSE connections, not ordinary HTTP request latency.",
			Buckets: []float64{1, 10, 30, 60, 300, 900, 3600}}, []string{_metricsRouteLabel}),
		sseActive: prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "codex_gateway_sse_connections",
			Help: "Currently established public SSE connections."}, []string{_metricsRouteLabel}),
		leaderDesc:   prometheus.NewDesc("codex_gateway_worker_leader", "One while the local delivery worker is active; zero after cancellation or observed renewal failure. This is not an instantaneous lease validity check.", nil, nil),
		epochDesc:    prometheus.NewDesc("codex_gateway_worker_epoch", "Last durable ownership epoch claimed by this process; zero before a claim.", nil, nil),
		streamDesc:   prometheus.NewDesc("codex_gateway_message_streams", "Currently running outgoing provider message streams.", nil, nil),
		databaseDesc: prometheus.NewDesc("codex_gateway_database_up", "One when a bounded read of gateway database metadata succeeds.", nil, nil),
		deliveryDesc: prometheus.NewDesc("codex_gateway_database_delivery_enabled", "One when database delivery is enabled; zero during restore holds. Omitted when database metadata is unavailable.", nil, nil),
		poolDesc:     prometheus.NewDesc("codex_gateway_database_pool_connections", "PostgreSQL pool connections by bounded state.", []string{"state"}, nil),
		activeDesc:   prometheus.NewDesc("codex_gateway_database_active_limit", "Configured database active execution reservation limit.", nil, nil),
		retainedDesc: prometheus.NewDesc("codex_gateway_database_retained_limit", "Configured database retained identity limit.", nil, nil),
	}
}

func (m *gatewayMetrics) Describe(ch chan<- *prometheus.Desc) {
	m.requests.Describe(ch)
	m.duration.Describe(ch)
	m.sseDuration.Describe(ch)
	m.sseActive.Describe(ch)
	for _, desc := range []*prometheus.Desc{m.leaderDesc, m.epochDesc, m.streamDesc,
		m.databaseDesc, m.deliveryDesc, m.poolDesc, m.activeDesc, m.retainedDesc} {
		ch <- desc
	}
}

func (m *gatewayMetrics) Collect(ch chan<- prometheus.Metric) {
	m.requests.Collect(ch)
	m.duration.Collect(ch)
	m.sseDuration.Collect(ch)
	m.sseActive.Collect(ch)
	leader := m.workerLeader.Load()
	if worker := m.workerContext.Load(); worker != nil && worker.ctx.Err() != nil {
		leader = 0
	}
	ch <- prometheus.MustNewConstMetric(m.leaderDesc, prometheus.GaugeValue, float64(leader))
	ch <- prometheus.MustNewConstMetric(m.epochDesc, prometheus.GaugeValue, float64(m.workerEpoch.Load()))
	ch <- prometheus.MustNewConstMetric(m.streamDesc, prometheus.GaugeValue, float64(m.messageStream.Load()))
	store, ok := m.store.(StatisticsStorage)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), _metricsStorageTimeout)
	defer cancel()
	statistics, err := store.Statistics(ctx)
	up := 0.0
	if err == nil {
		up = 1
		delivery := 0.0
		if statistics.DeliveryEnabled {
			delivery = 1
		}
		ch <- prometheus.MustNewConstMetric(m.activeDesc, prometheus.GaugeValue, float64(statistics.ActiveLimit))
		ch <- prometheus.MustNewConstMetric(m.retainedDesc, prometheus.GaugeValue, float64(statistics.RetainedLimit))
		ch <- prometheus.MustNewConstMetric(m.deliveryDesc, prometheus.GaugeValue, delivery)
	}
	ch <- prometheus.MustNewConstMetric(m.databaseDesc, prometheus.GaugeValue, up)
	for state, count := range map[string]int32{"acquired": statistics.Acquired, "idle": statistics.Idle,
		"total": statistics.Total, "max": statistics.Max} {
		ch <- prometheus.MustNewConstMetric(m.poolDesc, prometheus.GaugeValue, float64(count), state)
	}
}

// InstrumentedHandler preserves the ResponseWriter's optional interfaces and
// Unwrap chain, including SSE flushing and ResponseController write deadlines.
func (g *Gateway) InstrumentedHandler() http.Handler {
	metrics := g.metricState()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		route, method := metricsRoute(r.URL.Path), metricsMethod(r.Method)
		code := http.StatusOK
		final, stream := false, false
		mark := func(status int) {
			if final || status < http.StatusOK {
				return
			}
			code, final = status, true
			if status == http.StatusOK && strings.Split(w.Header().Get("Content-Type"), ";")[0] == "text/event-stream" {
				stream = true
				metrics.sseActive.WithLabelValues(route).Inc()
			}
		}
		wrapped := httpsnoop.Wrap(w, httpsnoop.Hooks{
			WriteHeader: func(next httpsnoop.WriteHeaderFunc) httpsnoop.WriteHeaderFunc {
				return func(status int) { mark(status); next(status) }
			},
			Write: func(next httpsnoop.WriteFunc) httpsnoop.WriteFunc {
				return func(body []byte) (int, error) { mark(http.StatusOK); return next(body) }
			},
			Flush: func(next httpsnoop.FlushFunc) httpsnoop.FlushFunc {
				return func() { mark(http.StatusOK); next() }
			},
			ReadFrom: func(next httpsnoop.ReadFromFunc) httpsnoop.ReadFromFunc {
				return func(reader io.Reader) (int64, error) { mark(http.StatusOK); return next(reader) }
			},
		})
		defer func() {
			metrics.requests.WithLabelValues(route, method, metricsCode(code)).Inc()
			seconds := time.Since(started).Seconds()
			if stream {
				metrics.sseActive.WithLabelValues(route).Dec()
				metrics.sseDuration.WithLabelValues(route).Observe(seconds)
				return
			}
			metrics.duration.WithLabelValues(route, method).Observe(seconds)
		}()
		g.ServeHTTP(wrapped, r)
	})
}

func metricsMethod(method string) string {
	switch method {
	case http.MethodGet, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete,
		http.MethodHead, http.MethodOptions, http.MethodConnect, http.MethodTrace:
		return method
	default:
		return "OTHER"
	}
}

func metricsCode(code int) string {
	if code < 100 || code > 599 {
		return _metricsOtherCode
	}
	return strconv.Itoa(code)
}

func metricsRoute(path string) string {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) == 1 && parts[0] == "readyz" {
		return "ready"
	}
	if len(parts) < 4 || parts[0] != "v1" || parts[1] != _namespacesSegment {
		return _metricsUnmatched
	}
	if len(parts) == 4 {
		return metricsCollectionRoute(parts[3])
	}
	if len(parts) == 5 {
		return metricsDetailRoute(parts[3])
	}
	if parts[3] == _sessionsSegment && len(parts) == 7 && parts[5] == _metricsActions {
		return "session_action"
	}
	if parts[3] != _sessionsSegment || len(parts) != 6 {
		return _metricsUnmatched
	}
	switch parts[5] {
	case "messages":
		return _metricsMessages
	case _metricsCancel:
		return "session_cancel"
	case _historyField:
		return "session_history"
	case _metricsActions:
		return "session_actions"
	case _eventsSegment:
		return "session_events"
	default:
		return _metricsUnmatched
	}
}

func metricsCollectionRoute(kind string) string {
	switch kind {
	case _metricsCatalog:
		return _metricsCatalog
	case _identitySegment:
		return _identitySegment
	case _profilesSegment:
		return _profilesSegment
	case _sessionsSegment:
		return _sessionsSegment
	case _recordsSegment:
		return _recordsSegment
	default:
		return _metricsUnmatched
	}
}

func metricsDetailRoute(kind string) string {
	switch kind {
	case _sessionsSegment:
		return "session"
	case _recordsSegment:
		return "record"
	case _profilesSegment:
		return _profileSourceName
	default:
		return _metricsUnmatched
	}
}

var _ StatisticsStorage = (*PostgreSQLStore)(nil)

func (s *PostgreSQLStore) Statistics(ctx context.Context) (StorageStatistics, error) {
	ctx, cancel := context.WithTimeout(ctx, _metricsStorageTimeout)
	defer cancel()
	stat := s.pool.Stat()
	result := StorageStatistics{Acquired: stat.AcquiredConns(), Idle: stat.IdleConns(), Total: stat.TotalConns(), Max: stat.MaxConns()}
	var disabled bool
	if err := s.pool.QueryRow(ctx, "SELECT delivery_disabled,max_active,max_records FROM gateway_meta WHERE id=1").Scan(
		&disabled, &result.ActiveLimit, &result.RetainedLimit); err != nil {
		return result, errUnavailable
	}
	result.DeliveryEnabled = !disabled
	return result, nil
}
