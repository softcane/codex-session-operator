package openaiapi

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	openai "github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/option"
	"github.com/openai/openai-go/v3/packages/ssestream"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

const (
	testRequestCounterName = "codex_openai_requests_total"
	testRequestDuration    = "codex_openai_request_duration_seconds"
	testMetricsSecret      = "metrics-test-private-marker"
	testMetricOperation    = "operation"
	testMetricMethod       = "method"
	testMetricCode         = "code"
)

func metricSeries(t *testing.T, registry *prometheus.Registry, name, operation, method, code string) *dto.Metric {
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
			labels := make(map[string]string, len(metric.GetLabel()))
			for _, pair := range metric.GetLabel() {
				labels[pair.GetName()] = pair.GetValue()
			}
			if len(labels) != 3 {
				t.Fatalf("unexpected metric label dimensions: %v", labels)
			}
			if labels[testMetricOperation] == operation && labels[testMetricMethod] == method && labels[testMetricCode] == code {
				return metric
			}
		}
	}
	return nil
}

func metricsClient(t *testing.T, registry prometheus.Registerer) *Client {
	t.Helper()
	t.Setenv("OPENAI_API_KEY", testMetricsSecret)
	c, err := NewClientWithMetrics(registry)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestRequestMetricsHTTPResultsAndTransportFailure(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Header.Get("Authorization") != "Bearer "+testMetricsSecret {
			t.Error("instrumentation changed request authentication")
		}
		w.Header().Set("Content-Type", "application/json")
		if strings.HasSuffix(r.URL.Path, "/missing") {
			w.WriteHeader(http.StatusNotFound)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
		_, _ = io.WriteString(w, `{"error":{"message":"private response body"}}`)
	}))
	defer server.Close()
	registry := prometheus.NewRegistry()
	c := metricsClient(t, registry)
	independent := prometheus.NewRegistry()
	_ = metricsClient(t, independent)
	for _, tt := range []struct {
		id   string
		code string
	}{
		{id: "missing", code: "404"},
		{id: "unavailable", code: "500"},
	} {
		t.Run(tt.code, func(t *testing.T) {
			_, err := c.SDK.Beta.Agents.Sessions.Get(t.Context(), tt.id, option.WithBaseURL(server.URL+"/v1/"))
			if err == nil {
				t.Fatal("HTTP failure was swallowed")
			}
			counter := metricSeries(t, registry, testRequestCounterName, "sessions", http.MethodGet, tt.code)
			histogram := metricSeries(t, registry, testRequestDuration, "sessions", http.MethodGet, tt.code)
			if counter.GetCounter().GetValue() != 1 || histogram.GetHistogram().GetSampleCount() != 1 {
				t.Fatal("HTTP result was not recorded once")
			}
		})
	}
	transportError := errors.New("private transport detail")
	_, err := c.SDK.Beta.Agents.Sessions.Get(t.Context(), testSessionID,
		option.WithBaseURL(server.URL+"/v1/"), option.WithHTTPClient(&http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return nil, transportError
		})}))
	if !errors.Is(err, transportError) {
		t.Fatal("instrumentation changed transport error propagation")
	}
	if metricSeries(t, registry, testRequestCounterName, "sessions", http.MethodGet, "network_error").GetCounter().GetValue() != 1 ||
		metricSeries(t, registry, testRequestDuration, "sessions", http.MethodGet, "network_error").GetHistogram().GetSampleCount() != 1 {
		t.Fatal("transport failure was not recorded")
	}
	if calls.Load() != 2 {
		t.Fatalf("instrumentation changed the no-retry policy: %d HTTP calls", calls.Load())
	}
	families, err := independent.Gather()
	if err != nil || len(families) != 0 {
		t.Fatal("request samples leaked into an independent registry", err)
	}
}

func TestRequestMetricsRecordsStreamHeadersBeforeBodyWithClientOverride(t *testing.T) {
	arrived := make(chan struct{})
	headers := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(arrived)
		select {
		case <-headers:
		case <-r.Context().Done():
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Error("response writer cannot flush stream headers")
			return
		}
		flusher.Flush()
		// There is no event or EOF until the caller closes the stream.
		<-r.Context().Done()
	}))
	defer server.Close()
	registry := prometheus.NewRegistry()
	c := metricsClient(t, registry)
	var overrideCalls atomic.Int32
	ctx, cancel := context.WithCancel(t.Context())
	streams := make(chan *ssestream.Stream[openai.AgentSessionEventUnion], 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		streams <- c.SDK.Beta.Agents.Sessions.Events.StreamStreaming(ctx, testSessionID,
			option.WithBaseURL(server.URL+"/v1/"), option.WithRequestTimeout(0),
			option.WithHTTPClient(&http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
				overrideCalls.Add(1)
				return http.DefaultTransport.RoundTrip(request)
			})}))
	}()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("stream constructor did not stop after cancellation")
		}
	}()
	select {
	case <-arrived:
	case <-time.After(3 * time.Second):
		t.Fatal("stream request did not reach the test server")
	}
	if metricSeries(t, registry, testRequestCounterName, "session_events", http.MethodGet, "200") != nil {
		t.Fatal("request was counted before its headers returned")
	}
	close(headers)
	select {
	case stream := <-streams:
		defer func() { _ = stream.Close() }()
		if stream.Err() != nil {
			t.Fatal(stream.Err())
		}
	case <-time.After(3 * time.Second):
		t.Fatal("SDK waited for the stream body rather than response headers")
	}
	counter := metricSeries(t, registry, testRequestCounterName, "session_events", http.MethodGet, "200")
	histogram := metricSeries(t, registry, testRequestDuration, "session_events", http.MethodGet, "200")
	if counter.GetCounter().GetValue() != 1 || histogram.GetHistogram().GetSampleCount() != 1 || overrideCalls.Load() != 1 {
		t.Fatal("open stream or per-call HTTP client bypassed request instrumentation")
	}
}

func TestRequestMetricOperationsAreFinite(t *testing.T) {
	for _, tt := range []struct {
		path string
		want string
	}{
		{path: "/v1/agents", want: "agents"},
		{path: "/v1/agents/private-agent", want: "agents"},
		{path: "/v1/agents/sessions", want: "sessions"},
		{path: "/v1/agents/sessions/private-session/events", want: "session_events"},
		{path: "/v1/agents/sessions/private-session/items/private-item", want: "session_items"},
		{path: "/v1/agents/sessions/private-session/turns/private-turn", want: "session_turns"},
		{path: "/v1/vaults/private-vault", want: "vaults"},
		{path: "/v1/vaults/private-vault/credentials/private-credential", want: "credentials"},
		{path: "/v1/agents/environments/templates/private-template", want: "environment_templates"},
		{path: "/v1/agents/environments/private-environment", want: "environments"},
		{path: "/v1/agents/sessions/private-session/unknown/private-detail", want: "other"},
		{path: "/private/path/" + strings.Repeat("secret/", 50), want: "other"},
	} {
		t.Run(tt.want+tt.path, func(t *testing.T) {
			if got := requestOperation(tt.path); got != tt.want {
				t.Fatalf("operation=%q want=%q", got, tt.want)
			}
		})
	}
}

func TestRequestMetricsUnknownPathAndMethodDoNotExposeRequestDetails(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{}`)
	}))
	defer server.Close()
	registry := prometheus.NewRegistry()
	c := metricsClient(t, registry)
	for _, suffix := range []string{"private-resource-a", "private-resource-b", "private-scope-c"} {
		var response map[string]any
		if err := c.SDK.Execute(t.Context(), "CUSTOM-"+suffix, "unknown/"+suffix+"?secret="+testMetricsSecret, nil, &response,
			option.WithBaseURL(server.URL+"/v1/")); err != nil {
			t.Fatal(err)
		}
	}
	if metricSeries(t, registry, testRequestCounterName, "other", "other", "200").GetCounter().GetValue() != 3 {
		t.Fatal("unknown route/method did not collapse to the bounded bucket")
	}
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if len(family.GetMetric()) != 1 {
			t.Fatal("unknown request details expanded metric series")
		}
		for _, secret := range []string{testMetricsSecret, "private-resource", "private-scope", server.URL} {
			if strings.Contains(family.String(), secret) {
				t.Fatal("request detail was included in a collector")
			}
		}
	}
}

type failHistogramRegisterer struct {
	prometheus.Registerer

	calls int
	err   error
}

func (r *failHistogramRegisterer) Register(collector prometheus.Collector) error {
	r.calls++
	if r.calls == 2 {
		return r.err
	}
	return r.Registerer.Register(collector)
}

func TestRequestMetricsRegistrationErrorsAndIsolatedRegistries(t *testing.T) {
	t.Setenv("OPENAI_API_KEY", testMetricsSecret)
	registry := prometheus.NewRegistry()
	if _, err := NewClientWithMetrics(registry); err != nil {
		t.Fatal(err)
	}
	if _, err := NewClientWithMetrics(registry); err == nil {
		t.Fatal("duplicate registration unexpectedly succeeded")
	} else {
		if _, ok := errors.AsType[prometheus.AlreadyRegisteredError](err); !ok {
			t.Fatalf("duplicate registration error was not preserved: %v", err)
		}
	}
	if _, err := NewClientWithMetrics(nil); err == nil {
		t.Fatal("nil registerer unexpectedly succeeded")
	}
	independent := prometheus.NewRegistry()
	if _, err := NewClientWithMetrics(independent); err != nil {
		t.Fatal("independent registry reused global collectors", err)
	}
	partial := prometheus.NewRegistry()
	registrationError := errors.New("histogram registration rejected")
	failing := &failHistogramRegisterer{Registerer: partial, err: registrationError}
	if _, err := NewClientWithMetrics(failing); !errors.Is(err, registrationError) {
		t.Fatal("registration error was not preserved", err)
	}
	if _, err := NewClientWithMetrics(partial); err != nil {
		t.Fatal("failed registration left a counter behind", err)
	}
	t.Setenv("OPENAI_API_KEY", "")
	if _, err := NewClient(); err == nil {
		t.Fatal("legacy constructor no longer requires API configuration")
	}
	empty := prometheus.NewRegistry()
	if _, err := NewClientWithMetrics(empty); err == nil {
		t.Fatal("instrumented constructor no longer requires API configuration")
	}
	if _, err := newRequestMetrics(empty); err != nil {
		t.Fatal("missing API key left metric registrations behind", err)
	}
}
