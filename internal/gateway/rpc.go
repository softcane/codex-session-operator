package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
)

const (
	internalPath        = "/internal/v1/"
	internalMaxBytes    = 1 << 20
	internalCallTimeout = 30 * time.Second
)

type internalRequest struct {
	UID         string                                     `json:"uid,omitempty"`
	RemoteID    string                                     `json:"remote_id,omitempty"`
	Tools       map[string]any                             `json:"tools,omitempty"`
	Session     *agentsv1alpha1.Session                    `json:"session,omitempty"`
	Environment *agentsv1alpha1.SessionEnvironmentSnapshot `json:"environment,omitempty"`
}

type initialResponse struct {
	Text string `json:"text"`
	Key  string `json:"key"`
}

// InternalHandler exposes only the four lifecycle operations the controller
// needs. The token is independent of caller tokens and is never logged.
func (g *Gateway) InternalHandler(token string) (http.Handler, error) {
	if len(token) < 32 || len(token) > 512 {
		return nil, errors.New("invalid gateway internal token")
	}
	digest := sha256.Sum256([]byte(token))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == internalPath+"readyz" && r.Method == http.MethodGet {
			if err := g.Ready(r); err != nil {
				http.Error(w, "Gateway unavailable", http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if r.Method != http.MethodPost || !strings.HasPrefix(r.URL.Path, internalPath) {
			http.NotFound(w, r)
			return
		}
		auth := r.Header.Get("Authorization")
		if !strings.HasPrefix(auth, "Bearer ") || len(auth) > 1024 {
			http.Error(w, "Authentication required", http.StatusUnauthorized)
			return
		}
		got := sha256.Sum256([]byte(strings.TrimPrefix(auth, "Bearer ")))
		if subtle.ConstantTimeCompare(got[:], digest[:]) != 1 {
			http.Error(w, "Authentication required", http.StatusUnauthorized)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, internalMaxBytes)
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()
		var request internalRequest
		if decoder.Decode(&request) != nil || !errors.Is(decoder.Decode(new(any)), io.EOF) {
			http.Error(w, "Invalid internal request", http.StatusBadRequest)
			return
		}
		switch r.URL.Path {
		case internalPath + "validate-tools":
			if g.ValidateTools(r.Context(), request.Tools) != nil {
				http.Error(w, "Gateway tool policy rejected", http.StatusUnprocessableEntity)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		case internalPath + "initial-input":
			if request.UID == "" {
				http.Error(w, "Session UID required", http.StatusBadRequest)
				return
			}
			text, key, err := g.InitialInput(r.Context(), request.UID)
			if err != nil {
				http.Error(w, "Gateway initial input unavailable", http.StatusServiceUnavailable)
				return
			}
			writeJSON(w, http.StatusOK, initialResponse{Text: text, Key: key})
		case internalPath + "quiesce":
			if request.UID == "" || g.Quiesce(r.Context(), request.UID, request.RemoteID) != nil {
				http.Error(w, "Gateway cleanup pending", http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		case internalPath + "validate-investigation":
			if request.Session == nil || g.ValidateInvestigation(r.Context(), request.Session, request.Environment) != nil {
				http.Error(w, "Gateway policy rejected", http.StatusUnprocessableEntity)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}), nil
}

// InternalServer keeps controller RPC on a separate TLS port from public input.
type InternalServer struct {
	Handler  http.Handler
	Address  string
	CertFile string
	KeyFile  string
}

func (s *InternalServer) Start(ctx context.Context) error {
	if s.Handler == nil || s.CertFile == "" || s.KeyFile == "" {
		return errors.New("gateway internal TLS configuration required")
	}
	listener, err := net.Listen("tcp", s.Address)
	if err != nil {
		return err
	}
	defer func() { _ = listener.Close() }()
	server := gatewayHTTPServer(ctx, s.Handler)
	done := make(chan error, 1)
	go func() { done <- server.ServeTLS(listener, s.CertFile, s.KeyFile) }()
	select {
	case <-ctx.Done():
		err = shutdownHTTPServer(server, done, 5*time.Second)
	case err = <-done:
	}
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func shutdownHTTPServer(server *http.Server, done <-chan error, timeout time.Duration) error {
	shutdownCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	shutdownErr := server.Shutdown(shutdownCtx)
	if shutdownErr != nil {
		closeErr := server.Close()
		if closeErr != nil && !errors.Is(closeErr, http.ErrServerClosed) {
			shutdownErr = errors.Join(shutdownErr, closeErr)
		}
	}
	serveErr := <-done
	if errors.Is(serveErr, http.ErrServerClosed) {
		serveErr = nil
	}
	return errors.Join(shutdownErr, serveErr)
}

// RemoteClient is the manager's bounded lifecycle client. It never opens the
// gateway store and never sends the application key to the gateway RPC port.
type RemoteClient struct {
	endpoint string
	token    string
	http     *http.Client
}

func NewRemoteClient(endpoint, token string, caPEM []byte) (*RemoteClient, error) {
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil || parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "" ||
		len(token) < 32 || len(token) > 512 {
		return nil, errors.New("invalid gateway internal client configuration")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return nil, errors.New("invalid gateway internal CA")
	}
	return &RemoteClient{endpoint: parsed.String(), token: token,
		http: &http.Client{Timeout: internalCallTimeout, Transport: &http.Transport{
			TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12},
		}}}, nil
}

func (c *RemoteClient) call(ctx context.Context, operation string, input internalRequest, output any) error {
	ctx, cancel := context.WithTimeout(ctx, internalCallTimeout)
	defer cancel()
	body, err := json.Marshal(input)
	if err != nil || len(body) > internalMaxBytes {
		return errors.New("invalid gateway internal request")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint+internalPath+operation, bytes.NewReader(body))
	if err != nil {
		return errors.New("invalid gateway internal endpoint")
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/json")
	response, err := c.http.Do(req)
	if err != nil {
		return errors.New("gateway internal request unavailable")
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusNoContent {
		return errors.New("gateway internal policy or service unavailable")
	}
	if output != nil {
		if json.NewDecoder(io.LimitReader(response.Body, internalMaxBytes+1)).Decode(output) != nil {
			return errors.New("invalid gateway internal response")
		}
	}
	return nil
}

func (c *RemoteClient) InitialInput(ctx context.Context, uid string) (string, string, error) {
	var response initialResponse
	err := c.call(ctx, "initial-input", internalRequest{UID: uid}, &response)
	return response.Text, response.Key, err
}

func (c *RemoteClient) ValidateTools(ctx context.Context, fields map[string]any) error {
	return c.call(ctx, "validate-tools", internalRequest{Tools: fields}, nil)
}

func (c *RemoteClient) Quiesce(ctx context.Context, uid, remoteID string) error {
	return c.call(ctx, "quiesce", internalRequest{UID: uid, RemoteID: remoteID}, nil)
}

func (c *RemoteClient) ValidateInvestigation(ctx context.Context, session *agentsv1alpha1.Session, environment *agentsv1alpha1.SessionEnvironmentSnapshot) error {
	return c.call(ctx, "validate-investigation", internalRequest{Session: session, Environment: environment}, nil)
}
