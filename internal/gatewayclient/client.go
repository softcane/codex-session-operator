// Package gatewayclient implements a private, retryable conversation API client.
package gatewayclient

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

const _maxResponseBytes = 8 << 20

type client struct {
	origin    string
	namespace string
	token     string
	http      *http.Client
}

type clientConfig struct {
	origin, namespace, tokenFile, caFile string
	requestTimeout                       time.Duration
}

type apiError struct {
	status int
}

func (e *apiError) Error() string {
	reason := "gateway request rejected"
	switch e.status {
	case http.StatusBadRequest:
		reason = "invalid request"
	case http.StatusUnauthorized, http.StatusForbidden:
		reason = "authentication or namespace authorization failed"
	case http.StatusNotFound:
		reason = "conversation, profile or namespace unavailable"
	case http.StatusConflict:
		reason = "replay or lifecycle conflict"
	case http.StatusGone:
		reason = "attempt closed or superseded"
	case http.StatusServiceUnavailable:
		reason = "gateway unavailable or recovery hold; retain the saved operation"
	}
	return fmt.Sprintf("%s (HTTP %d)", reason, e.status)
}

func newClient(config clientConfig) (*client, error) {
	u, err := url.Parse(config.origin)
	if err != nil || u == nil {
		return nil, errors.New("origin must be an explicit HTTPS origin")
	}
	invalidAuthority := u.Scheme != "https" || u.Hostname() == "" || u.User != nil
	invalidSuffix := strings.ContainsAny(config.origin, "?#") || u.RawPath != "" || u.Opaque != "" ||
		(u.Path != "" && u.Path != "/")
	if invalidAuthority || invalidSuffix || strings.TrimSpace(config.origin) != config.origin {
		return nil, errors.New("origin must be an explicit HTTPS origin without credentials, path, query or fragment")
	}
	if !validNamespace(config.namespace) {
		return nil, errors.New("namespace must be an explicit Kubernetes namespace")
	}
	if config.requestTimeout <= 0 || config.requestTimeout > 5*time.Minute {
		return nil, errors.New("request timeout must be positive and at most 5m")
	}
	tokenBytes, err := readPrivateFile(config.tokenFile, 1024)
	if err != nil {
		return nil, errors.New("read restricted regular token file")
	}
	token := strings.TrimSpace(string(tokenBytes))
	if token == "" || len(token)+len("Bearer ") > 1024 || strings.ContainsAny(token, "\r\n\t ") {
		return nil, errors.New("invalid token file")
	}
	pool, err := x509.SystemCertPool()
	if err != nil {
		pool = x509.NewCertPool()
	}
	if config.caFile != "" {
		ca, readErr := os.ReadFile(config.caFile)
		if readErr != nil || !pool.AppendCertsFromPEM(ca) {
			return nil, errors.New("read trusted CA file")
		}
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: pool}
	return &client{
		origin: strings.TrimSuffix(config.origin, "/"), namespace: config.namespace, token: token,
		http: &http.Client{Transport: transport, Timeout: config.requestTimeout,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
	}, nil
}

func (c *client) close() { c.http.CloseIdleConnections() }

func (c *client) basePath() string { return "/v1/namespaces/" + c.namespace }

// Errors deliberately exclude response bodies and transport error strings, which
// may contain credentials or untrusted provider diagnostics.
func (c *client) request(ctx context.Context, method, path, key string, body []byte, result any) error {
	if !c.validPath(path) {
		return errors.New("request target is outside the configured namespace")
	}
	r, err := http.NewRequestWithContext(ctx, method, c.origin+path, bytes.NewReader(body))
	if err != nil {
		return errors.New("construct gateway request")
	}
	r.Header.Set("Authorization", "Bearer "+c.token)
	r.Header.Set("Accept", "application/json")
	if method == http.MethodPost {
		// Retry only through the persisted operation, rather than the transport's
		// implicit retry path for requests carrying Idempotency-Key.
		r.GetBody = nil
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("Idempotency-Key", key)
	}
	response, err := c.http.Do(r)
	if err != nil {
		return errors.New("gateway response unavailable; a sent mutation may have been accepted")
	}
	defer func() { _ = response.Body.Close() }()
	expected := http.StatusOK
	if method == http.MethodPost {
		expected = http.StatusAccepted
	}
	if response.StatusCode != expected {
		return &apiError{status: response.StatusCode}
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, _maxResponseBytes+1))
	if err != nil || len(raw) > _maxResponseBytes {
		return errors.New("gateway response incomplete or oversized")
	}
	if json.Unmarshal(raw, result) != nil {
		return errors.New("invalid gateway JSON response")
	}
	return nil
}

func (c *client) validPath(path string) bool {
	u, err := url.Parse(path)
	if err != nil || u.IsAbs() || u.Host != "" || u.Fragment != "" {
		return false
	}
	return strings.HasPrefix(u.Path, c.basePath()+"/") && !strings.Contains(u.Path, "..") &&
		!strings.Contains(u.Path, "//") && !strings.Contains(u.Path, "\\")
}

func (c *client) subject(ctx context.Context) (string, error) {
	var identity struct {
		Subject   string `json:"caller_subject"`
		Namespace string `json:"namespace"`
	}
	if err := c.request(ctx, http.MethodGet, c.basePath()+"/identity", "", nil, &identity); err != nil {
		return "", err
	}
	if strings.TrimSpace(identity.Subject) == "" || identity.Namespace != c.namespace {
		return "", errors.New("gateway did not provide authenticated subject and namespace")
	}
	return identity.Subject, nil
}

func validNamespace(value string) bool {
	if len(value) == 0 || len(value) > 63 || value[0] == '-' || value[len(value)-1] == '-' {
		return false
	}
	for _, char := range value {
		if (char < 'a' || char > 'z') && (char < '0' || char > '9') && char != '-' {
			return false
		}
	}
	return true
}
