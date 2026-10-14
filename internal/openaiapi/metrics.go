package openaiapi

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/openai/openai-go/v3/option"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	metricOther        = "other"
	metricAgents       = "agents"
	metricEnvironments = "environments"
)

type requestMetrics struct {
	requests *prometheus.CounterVec
	duration *prometheus.HistogramVec
}

func newRequestMetrics(registerer prometheus.Registerer) (*requestMetrics, error) {
	if registerer == nil {
		return nil, errors.New("OpenAI metrics registerer is required")
	}
	labels := []string{"operation", "method", "code"}
	m := &requestMetrics{
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "codex_openai_requests_total",
			Help: "OpenAI SDK HTTP request attempts by bounded operation, method, and response code or network_error.",
		}, labels),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "codex_openai_request_duration_seconds",
			Help:    "OpenAI SDK HTTP request latency until response headers return or transport fails; excludes response body and SSE stream lifetime.",
			Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 20, 30, 60},
		}, labels),
	}
	if err := registerer.Register(m.requests); err != nil {
		return nil, fmt.Errorf("register OpenAI request counter: %w", err)
	}
	if err := registerer.Register(m.duration); err != nil {
		if !registerer.Unregister(m.requests) {
			return nil, errors.Join(fmt.Errorf("register OpenAI request histogram: %w", err),
				errors.New("OpenAI request counter registration rollback failed"))
		}
		return nil, fmt.Errorf("register OpenAI request histogram: %w", err)
	}
	return m, nil
}

// Middleware observes the HTTP attempt itself, before the SDK reads or decodes
// its body. Per-request HTTP-client overrides therefore keep these collectors,
// and an open event stream does not delay the sample until it ends.
func (m *requestMetrics) middleware(request *http.Request, next option.MiddlewareNext) (*http.Response, error) {
	started := time.Now()
	response, err := next(request)
	duration := time.Since(started).Seconds()
	code := "network_error"
	if err == nil && response != nil && response.StatusCode >= 100 && response.StatusCode <= 999 {
		code = strconv.Itoa(response.StatusCode)
	}
	operation := metricOther
	if request.URL != nil {
		operation = requestOperation(request.URL.Path)
	}
	method := requestMethod(request.Method)
	m.requests.WithLabelValues(operation, method, code).Inc()
	m.duration.WithLabelValues(operation, method, code).Observe(duration)
	return response, err
}

func requestMethod(method string) string {
	switch method {
	case http.MethodGet, http.MethodPost, http.MethodPatch, http.MethodPut,
		http.MethodDelete, http.MethodHead, http.MethodOptions:
		return method
	default:
		return metricOther
	}
}

// IDs are used only to locate finite route shapes and never become labels.
// SplitN also bounds allocations for an unknown path with many components.
func requestOperation(path string) string {
	parts := strings.SplitN(strings.Trim(path, "/"), "/", 7)
	if len(parts) < 2 || parts[0] != "v1" {
		return metricOther
	}
	if parts[1] == "vaults" {
		if len(parts) == 2 || len(parts) == 3 {
			return "vaults"
		}
		if (len(parts) == 4 || len(parts) == 5) && parts[3] == "credentials" {
			return "credentials"
		}
		return metricOther
	}
	if parts[1] != metricAgents {
		return metricOther
	}
	if len(parts) == 2 {
		return metricAgents
	}
	switch parts[2] {
	case "sessions":
		if len(parts) == 3 || len(parts) == 4 {
			return "sessions"
		}
		if len(parts) != 5 && len(parts) != 6 {
			return metricOther
		}
		switch parts[4] {
		case "events":
			if len(parts) == 5 {
				return "session_events"
			}
		case "items":
			return "session_items"
		case "turns":
			return "session_turns"
		}
	case metricEnvironments:
		if len(parts) == 3 {
			return metricEnvironments
		}
		if parts[3] == "templates" && (len(parts) == 4 || len(parts) == 5) {
			return "environment_templates"
		}
		if len(parts) == 4 {
			return metricEnvironments
		}
	default:
		if len(parts) == 3 {
			return metricAgents
		}
	}
	return metricOther
}
