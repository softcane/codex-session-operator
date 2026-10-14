package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

const (
	_testGatewayRequestsMetric = "codex_gateway_http_requests_total"
	_testGatewayLatencyMetric  = "codex_gateway_http_request_duration_seconds"
	_testGatewaySSEMetric      = "codex_gateway_sse_connection_duration_seconds"
	_testGatewaySSEActive      = "codex_gateway_sse_connections"
	_testGatewayDatabaseMetric = "codex_gateway_database_up"
	_testGatewayDeliveryMetric = "codex_gateway_database_delivery_enabled"
	_testGatewayLeaderMetric   = "codex_gateway_worker_leader"
	_testGatewayStreamsMetric  = "codex_gateway_message_streams"
	_testGatewayEventsRoute    = "session_events"
)

type metricsPlainWriter struct {
	header http.Header
	code   int
}

var _ http.ResponseWriter = (*metricsPlainWriter)(nil)

func (w *metricsPlainWriter) Header() http.Header { return w.header }
func (w *metricsPlainWriter) WriteHeader(code int) {
	if w.code == 0 {
		w.code = code
	}
}
func (w *metricsPlainWriter) Write(body []byte) (int, error) {
	w.WriteHeader(http.StatusOK)
	return len(body), nil
}

type metricsDeadlineWriter struct {
	*httptest.ResponseRecorder

	deadlines atomic.Int32
}

func (w *metricsDeadlineWriter) SetWriteDeadline(time.Time) error {
	w.deadlines.Add(1)
	return nil
}

type metricsStatisticsStore struct {
	Storage

	reads      atomic.Int32
	appReads   atomic.Int32
	mutations  atomic.Int32
	statistics StorageStatistics
	err        error
}

var _ StatisticsStorage = (*metricsStatisticsStore)(nil)

func (s *metricsStatisticsStore) Statistics(ctx context.Context) (StorageStatistics, error) {
	s.reads.Add(1)
	if _, ok := ctx.Deadline(); !ok {
		return StorageStatistics{}, errors.New("scrape statistics lacked a deadline")
	}
	return s.statistics, s.err
}

func (s *metricsStatisticsStore) Get(ctx context.Context, uid string) (Record, error) {
	s.appReads.Add(1)
	return s.Storage.Get(ctx, uid)
}

func (s *metricsStatisticsStore) Records(ctx context.Context) ([]Record, error) {
	s.appReads.Add(1)
	return s.Storage.Records(ctx)
}

func (s *metricsStatisticsStore) Update(ctx context.Context, uid string, update func(*Record) error) error {
	s.mutations.Add(1)
	return s.Storage.Update(ctx, uid, update)
}

type metricsBlockingStore struct {
	Storage

	started chan struct{}
	release chan struct{}
}

func (s *metricsBlockingStore) Records(ctx context.Context) ([]Record, error) {
	close(s.started)
	<-s.release
	return nil, nil
}

func gatewayMetricsRegistry(t *testing.T, g *Gateway) *prometheus.Registry {
	t.Helper()
	registry := prometheus.NewPedanticRegistry()
	if err := registry.Register(g.Metrics()); err != nil {
		t.Fatal(err)
	}
	return registry
}

func gatewayMetric(t *testing.T, registry *prometheus.Registry, name string, labels map[string]string) *dto.Metric {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			if len(metric.GetLabel()) != len(labels) {
				continue
			}
			matches := true
			for _, label := range metric.GetLabel() {
				if labels[label.GetName()] != label.GetValue() {
					matches = false
				}
			}
			if matches {
				return metric
			}
		}
	}
	return nil
}

func metricsRequest(handler http.Handler, method, path, token, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer "+token)
	r.Header.Set("Idempotency-Key", "metrics-input")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	return w
}

func TestGatewayMetricsCountRejectedAndSuccessfulRequests(t *testing.T) {
	g, _ := testGateway(t, testStore(t), simulatorForTest(), testSession())
	registry := gatewayMetricsRegistry(t, g)
	handler := g.InstrumentedHandler()
	tests := []struct {
		method string
		path   string
		token  string
		body   string
		route  string
		code   int
	}{
		{method: http.MethodPost, path: _testMessagePath, token: _token, body: _testFirstMessageBody, route: _metricsMessages, code: http.StatusAccepted},
		{method: http.MethodPost, path: _testMessagePath, body: _testFirstMessageBody, route: _metricsMessages, code: http.StatusUnauthorized},
		{method: http.MethodPut, path: _testMessagePath, token: _token, route: _metricsMessages, code: http.StatusMethodNotAllowed},
		{method: http.MethodGet, path: "/readyz", route: "ready", code: http.StatusOK},
	}
	for _, tt := range tests {
		if got := metricsRequest(handler, tt.method, tt.path, tt.token, tt.body).Code; got != tt.code {
			t.Fatal(got, tt.code)
		}
		metric := gatewayMetric(t, registry, _testGatewayRequestsMetric,
			map[string]string{_metricsRouteLabel: tt.route, _metricsMethodLabel: tt.method, _metricsCodeLabel: strconv.Itoa(tt.code)})
		if metric == nil || metric.GetCounter().GetValue() != 1 {
			t.Fatal("HTTP request status was not counted", tt.route, tt.code)
		}
	}
	for range cap(g.requestSlots) {
		g.requestSlots <- struct{}{}
	}
	response := metricsRequest(handler, http.MethodPost, _testMessagePath, _token, _testFirstMessageBody)
	for range cap(g.requestSlots) {
		<-g.requestSlots
	}
	if response.Code != http.StatusServiceUnavailable {
		t.Fatal(response.Code)
	}
	metric := gatewayMetric(t, registry, _testGatewayRequestsMetric,
		map[string]string{_metricsRouteLabel: _metricsMessages, _metricsMethodLabel: http.MethodPost, _metricsCodeLabel: "503"})
	if metric == nil || metric.GetCounter().GetValue() != 1 {
		t.Fatal("capacity rejection was not counted")
	}
	if metric := gatewayMetric(t, registry, _testGatewayLatencyMetric,
		map[string]string{_metricsRouteLabel: _metricsMessages, _metricsMethodLabel: http.MethodPost}); metric == nil || metric.GetHistogram().GetSampleCount() != 3 {
		t.Fatal("ordinary HTTP latency did not include successful/auth/capacity requests")
	}
}

func TestGatewayMetricsLabelsRemainBoundedAndPrivate(t *testing.T) {
	g, _ := testGateway(t, testStore(t), simulatorForTest())
	registry := gatewayMetricsRegistry(t, g)
	for i := range 50 {
		canary := "private-resource-" + strconv.Itoa(i)
		metricsRequest(g.InstrumentedHandler(), "PRIVATE"+strconv.Itoa(i),
			"/v1/namespaces/"+canary+"/sessions/"+canary+"/messages?token="+canary, canary, "")
		metricsRequest(g.InstrumentedHandler(), http.MethodGet, "/"+canary, canary, "")
	}
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(families)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "private-resource-") || strings.Contains(string(encoded), "PRIVATE0") || strings.Contains(string(encoded), _token) {
		t.Fatal("operational metrics leaked request/resource/authentication values")
	}
	for _, family := range families {
		if family.GetName() == _testGatewayRequestsMetric && len(family.GetMetric()) != 2 {
			t.Fatal("attacker-controlled routes/methods created unbounded series", len(family.GetMetric()))
		}
	}
	if metricsCode(99999) != _metricsOtherCode || metricsMethod("secret-method") != "OTHER" {
		t.Fatal("unknown status/method labels were not bounded")
	}
}

func TestGatewayMetricsSSEPreservesFlushingAndSeparateDuration(t *testing.T) {
	store := testStore(t)
	g, _ := testGateway(t, store, simulatorForTest(), testSession())
	registry := gatewayMetricsRegistry(t, g)
	server := httptest.NewServer(g.InstrumentedHandler())
	t.Cleanup(server.Close)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	t.Cleanup(cancel)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+_scenarioSessionPath+"/events", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+_token)
	response, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = response.Body.Close() })
	if response.StatusCode != http.StatusOK || response.Header.Get("Content-Type") != "text/event-stream" {
		t.Fatal("instrumentation changed SSE response", response.StatusCode)
	}
	if metric := gatewayMetric(t, registry, _testGatewaySSEActive, map[string]string{_metricsRouteLabel: _testGatewayEventsRoute}); metric == nil || metric.GetGauge().GetValue() != 1 {
		t.Fatal("established SSE connection was not counted live")
	}
	if metric := gatewayMetric(t, registry, _testGatewayLatencyMetric,
		map[string]string{_metricsRouteLabel: _testGatewayEventsRoute, _metricsMethodLabel: http.MethodGet}); metric != nil {
		t.Fatal("live SSE lifetime was reported as ordinary HTTP latency")
	}
	if err := store.Update(ctx, _testUID, func(r *Record) error { r.Closed = true; return nil }); err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(io.Discard, response.Body); err != nil {
		t.Fatal(err)
	}
	if metric := gatewayMetric(t, registry, _testGatewaySSEMetric, map[string]string{_metricsRouteLabel: _testGatewayEventsRoute}); metric == nil || metric.GetHistogram().GetSampleCount() != 1 {
		t.Fatal("completed SSE lifetime was not recorded separately")
	}
	if metric := gatewayMetric(t, registry, _testGatewaySSEActive, map[string]string{_metricsRouteLabel: _testGatewayEventsRoute}); metric == nil || metric.GetGauge().GetValue() != 0 {
		t.Fatal("completed SSE left a live-connection metric")
	}
	metricsRequest(g.InstrumentedHandler(), http.MethodGet, _scenarioSessionPath+"/events", "wrong-token", "")
	if metric := gatewayMetric(t, registry, _testGatewayLatencyMetric,
		map[string]string{_metricsRouteLabel: _testGatewayEventsRoute, _metricsMethodLabel: http.MethodGet}); metric == nil || metric.GetHistogram().GetSampleCount() != 1 {
		t.Fatal("rejected SSE request was omitted from ordinary HTTP latency")
	}
}

func TestGatewayMetricsPreserveResponseControllerAndOptionalInterfaces(t *testing.T) {
	store := testStore(t)
	g, _ := testGateway(t, store, simulatorForTest(), testSession())
	if _, _, err := g.lookup(t.Context(), _testNamespace, _testSessionName); err != nil {
		t.Fatal(err)
	}
	if err := store.Update(t.Context(), _testUID, func(r *Record) error { r.Closed = true; return nil }); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, _scenarioSessionPath+"/events", nil)
	req.Header.Set("Authorization", "Bearer "+_token)
	writer := &metricsDeadlineWriter{ResponseRecorder: httptest.NewRecorder()}
	g.InstrumentedHandler().ServeHTTP(writer, req)
	if !writer.Flushed || writer.deadlines.Load() == 0 || writer.Code != http.StatusOK {
		t.Fatal("wrapper lost Flusher or ResponseController Unwrap/write-deadline forwarding")
	}
	plain := &metricsPlainWriter{header: make(http.Header)}
	g.InstrumentedHandler().ServeHTTP(plain, req)
	if plain.code != http.StatusServiceUnavailable {
		t.Fatal("wrapper advertised unsupported Flusher", plain.code)
	}
}

func TestGatewayMetricsRegistryIsolationAndConcurrentConstruction(t *testing.T) {
	g, _ := testGateway(t, testStore(t), simulatorForTest())
	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			if g.Metrics() != g.Metrics() {
				t.Error("gateway metrics collector changed identity")
			}
		})
	}
	wg.Wait()
	first := gatewayMetricsRegistry(t, g)
	other, _ := testGateway(t, testStore(t), simulatorForTest())
	second := gatewayMetricsRegistry(t, other)
	metricsRequest(g.InstrumentedHandler(), http.MethodGet, "/unknown", "", "")
	if gatewayMetric(t, first, _testGatewayRequestsMetric,
		map[string]string{_metricsRouteLabel: _metricsUnmatched, _metricsMethodLabel: http.MethodGet, _metricsCodeLabel: "401"}) == nil {
		t.Fatal("first registry did not receive its request")
	}
	if gatewayMetric(t, second, _testGatewayRequestsMetric,
		map[string]string{_metricsRouteLabel: _metricsUnmatched, _metricsMethodLabel: http.MethodGet, _metricsCodeLabel: "401"}) != nil {
		t.Fatal("gateway instances shared counters/default-registry state")
	}
	if gatewayMetric(t, first, _testGatewayDatabaseMetric, nil) != nil {
		t.Fatal("non-PostgreSQL test store pretended to expose database health")
	}
}

func TestGatewayMetricsReadOnlyStorageSnapshotsAndFailureSemantics(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		delivery bool
		wantUp   float64
	}{
		{name: "available", delivery: true, wantUp: 1},
		{name: "restore held", wantUp: 1},
		{name: "database unavailable", err: errUnavailable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &metricsStatisticsStore{Storage: testStore(t), statistics: StorageStatistics{
				DeliveryEnabled: tt.delivery, Acquired: 1, Idle: 2, Total: 3, Max: 4, ActiveLimit: 32, RetainedLimit: 100000}, err: tt.err}
			g, _ := testGateway(t, store, simulatorForTest())
			registry := gatewayMetricsRegistry(t, g)
			metric := gatewayMetric(t, registry, _testGatewayDatabaseMetric, nil)
			if metric == nil || metric.GetGauge().GetValue() != tt.wantUp || store.reads.Load() != 1 ||
				store.appReads.Load() != 0 || store.mutations.Load() != 0 {
				t.Fatal("storage health required more than one bounded snapshot or gave wrong availability")
			}
			if tt.err != nil {
				if gatewayMetric(t, registry, _testGatewayDeliveryMetric, nil) != nil {
					t.Fatal("database failure was misreported as a restore hold")
				}
				return
			}
			want := 0.0
			if tt.delivery {
				want = 1
			}
			if metric := gatewayMetric(t, registry, _testGatewayDeliveryMetric, nil); metric == nil || metric.GetGauge().GetValue() != want {
				t.Fatal("delivery enablement was not distinguished from database availability")
			}
		})
	}
}

func TestGatewayMetricsOutgoingStreamsAndWorkerCancellation(t *testing.T) {
	g, _ := testGateway(t, testStore(t), simulatorForTest())
	registry := gatewayMetricsRegistry(t, g)
	stream := &closeOnlyStream{started: make(chan struct{}), closed: make(chan struct{})}
	t.Cleanup(stream.release)
	g.provider = &closeOnlyProvider{Provider: g.provider, stream: stream}
	if err := g.ensureWatch(_testUID, _testRemoteID); err != nil {
		t.Fatal(err)
	}
	select {
	case <-stream.started:
	case <-time.After(2 * time.Second):
		t.Fatal("outgoing stream did not start")
	}
	if metric := gatewayMetric(t, registry, _testGatewayStreamsMetric, nil); metric.GetGauge().GetValue() != 1 {
		t.Fatal("active outgoing stream was not counted")
	}
	g.stopWatch(_testUID)
	if metric := gatewayMetric(t, registry, _testGatewayStreamsMetric, nil); metric.GetGauge().GetValue() != 0 {
		t.Fatal("joined outgoing stream remained active")
	}
	blocking := &metricsBlockingStore{Storage: testStore(t), started: make(chan struct{}), release: make(chan struct{})}
	worker, _ := testGateway(t, blocking, simulatorForTest())
	workerRegistry := gatewayMetricsRegistry(t, worker)
	ctx, cancel := context.WithCancel(t.Context())
	joined := make(chan error, 1)
	var once sync.Once
	release := func() { once.Do(func() { close(blocking.release) }) }
	go func() { joined <- worker.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		release()
		select {
		case <-joined:
		case <-time.After(2 * time.Second):
			t.Error("metrics worker did not join shutdown")
		}
	})
	select {
	case <-blocking.started:
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not begin its bounded active-record scan")
	}
	if metric := gatewayMetric(t, workerRegistry, _testGatewayLeaderMetric, nil); metric.GetGauge().GetValue() != 1 {
		t.Fatal("active local worker was not reported")
	}
	cancel()
	if metric := gatewayMetric(t, workerRegistry, _testGatewayLeaderMetric, nil); metric.GetGauge().GetValue() != 0 {
		t.Fatal("canceled worker still appeared active")
	}
}

func TestPostgreSQLGatewayMetricsDoNotMutateClaimsOrReadTranscript(t *testing.T) {
	store, _ := postgresForTest(t, 32)
	if err := store.Update(t.Context(), "private-metrics-record", func(r *Record) error {
		r.Namespace = _testNamespace
		r.Notes["private"] = "transcript-must-not-be-scraped"
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	before, err := store.Get(context.Background(), "private-metrics-record")
	if err != nil {
		t.Fatal(err)
	}
	var epochBefore, epochAfter int64
	if err := store.pool.QueryRow(t.Context(), "SELECT epoch FROM gateway_meta WHERE id=1").Scan(&epochBefore); err != nil {
		t.Fatal(err)
	}
	g, _ := testGateway(t, store, simulatorForTest())
	registry := gatewayMetricsRegistry(t, g)
	if metric := gatewayMetric(t, registry, _testGatewayDatabaseMetric, nil); metric == nil || metric.GetGauge().GetValue() != 1 {
		t.Fatal("real PostgreSQL availability was not observed")
	}
	if err := store.pool.QueryRow(t.Context(), "SELECT epoch FROM gateway_meta WHERE id=1").Scan(&epochAfter); err != nil {
		t.Fatal(err)
	}
	after, err := store.Get(context.Background(), before.UID)
	if err != nil || epochBefore != epochAfter || string(mustJSON(t, before)) != string(mustJSON(t, after)) {
		t.Fatal("read-only scrape changed a record or ownership epoch", err)
	}
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(mustJSON(t, families)), before.Notes["private"]) || strings.Contains(string(mustJSON(t, families)), before.UID) {
		t.Fatal("database metrics exposed retained application data")
	}
}

func TestPostgreSQLGatewayMetricsShareOwnedWorkerEpoch(t *testing.T) {
	store, _ := postgresForTest(t, 32)
	g, _ := testGateway(t, store, simulatorForTest())
	registry := gatewayMetricsRegistry(t, g)
	ctx, cancel := context.WithCancel(t.Context())
	joined := make(chan error, 1)
	go func() { joined <- g.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-joined:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(3 * time.Second):
			t.Error("shared worker did not join shutdown")
		}
	})
	deadline := time.Now().Add(3 * time.Second)
	for {
		metric := gatewayMetric(t, registry, _testGatewayLeaderMetric, nil)
		if metric != nil && metric.GetGauge().GetValue() == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("claimed delivery worker was not reported by the HTTP gateway collector")
		}
		time.Sleep(5 * time.Millisecond)
	}
	var epoch int64
	if err := store.pool.QueryRow(t.Context(), "SELECT epoch FROM gateway_meta WHERE id=1").Scan(&epoch); err != nil {
		t.Fatal(err)
	}
	metric := gatewayMetric(t, registry, "codex_gateway_worker_epoch", nil)
	if epoch <= 0 || metric == nil || metric.GetGauge().GetValue() != float64(epoch) {
		t.Fatal("worker clone did not share its durable claimed epoch")
	}
	cancel()
	if metric := gatewayMetric(t, registry, _testGatewayLeaderMetric, nil); metric.GetGauge().GetValue() != 0 {
		t.Fatal("canceled owned worker remained active")
	}
}
