package openaiapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	openai "github.com/openai/openai-go/v3"
)

const _connectionEnvironmentID = "connection-environment"

func TestConnectionStateUsesOneBoundedGetAndSafeProjection(t *testing.T) {
	for _, status := range []string{string(openai.EnvironmentInfoStatusPending), string(openai.EnvironmentInfoStatusConnected),
		string(openai.EnvironmentInfoStatusDisconnected), string(openai.EnvironmentInfoStatusExpired), string(openai.EnvironmentInfoStatusFailed)} {
		t.Run(status, func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				if r.Method != http.MethodGet || r.URL.Path != "/v1/agents/environments/"+_connectionEnvironmentID || r.Header.Get("OpenAI-Beta") != agentsBetaHeader {
					t.Error("connection gate used an unsupported endpoint or mutation")
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = fmt.Fprintf(w, `{"id":%q,"type":"self_hosted","status":%q,"remote_url":"private-url-must-not-escape","env":{"TOKEN":"protected-value"}}`, _connectionEnvironmentID, status)
			}))
			defer server.Close()
			state, err := clientForServer(server, nil).ConnectionState(t.Context(), _connectionEnvironmentID)
			if err != nil || state.ID != _connectionEnvironmentID || state.Type != selfHostedType || state.Status != status || calls != 1 {
				t.Fatal("connection readback did not preserve exact state", err)
			}
			raw, err := json.Marshal(state)
			if err != nil || strings.Contains(string(raw), "private-url") || strings.Contains(string(raw), "protected-value") {
				t.Fatal("connection readback retained protected provider fields")
			}
		})
	}
}

func TestConnectionStateRejectsIncompleteIdentityAndSanitizesFailure(t *testing.T) {
	for _, body := range []string{`null`, `{"id":"foreign","type":"self_hosted","status":"connected"}`,
		`{"id":"connection-environment","type":"self_hosted"}`, `{"id":"connection-environment","status":"connected"}`} {
		t.Run(body, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = fmt.Fprint(w, body)
			}))
			defer server.Close()
			if _, err := clientForServer(server, nil).ConnectionState(t.Context(), _connectionEnvironmentID); err == nil {
				t.Fatal("incomplete current environment observation was accepted")
			}
		})
	}
	const private = "connection-private-provider-detail"
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = fmt.Fprint(w, private)
	}))
	defer server.Close()
	client := clientForServer(server, nil)
	if _, err := client.ConnectionState(t.Context(), _connectionEnvironmentID); err == nil || strings.Contains(err.Error(), private) || calls != 1 {
		t.Fatal("connection read retried or leaked private provider details")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := client.ConnectionState(ctx, _connectionEnvironmentID); !errors.Is(err, context.Canceled) {
		t.Fatal("connection observation lost cancellation identity")
	}
}
