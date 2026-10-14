package gateway

import (
	"bufio"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type scenarioTLSBody struct {
	io.ReadCloser

	started chan struct{}
	once    sync.Once
}

var _ io.ReadCloser = (*scenarioTLSBody)(nil)

func (b *scenarioTLSBody) Read(p []byte) (int, error) {
	b.once.Do(func() { close(b.started) })
	return b.ReadCloser.Read(p)
}

// Exercise the actual TLS request/response boundary. ResponseRecorder cannot
// expose connection resets when a lifecycle fence rejects an unread POST body.
func TestScenarioGatewayTLSLifecycleFenceReturnsHTTPStatus(t *testing.T) {
	tests := []struct {
		name       string
		closed     bool
		wantStatus int
	}{
		{name: "controller remote missing", wantStatus: http.StatusServiceUnavailable},
		{name: "sealed conversation", closed: true, wantStatus: http.StatusGone},
	}
	mutations := []struct {
		name  string
		path  string
		body  string
		token string
	}{
		{name: _scenarioMessage, path: _testMessagePath, body: _testFirstMessageBody, token: _token},
		{name: _scenarioCancel, path: _scenarioSessionPath + "/cancel",
			body: string(mustJSON(t, map[string]string{"turn_id": _testTurnID})), token: _token},
		{name: _scenarioApproval, path: _scenarioSessionPath + "/actions/" + actionID(scenarioPendingAction()),
			body: string(mustJSON(t, map[string]string{"decision": _approve})), token: _token + "-approver"},
	}
	for _, tt := range tests {
		for _, mutation := range mutations {
			t.Run(tt.name+"/"+mutation.name, func(t *testing.T) {
				store := testStore(t)
				provider := simulatorForTest()
				provider.history.Actions = append(provider.history.Actions, scenarioPendingAction())
				session := testSession()
				session.Generation = 2
				session.Status.Conditions = []metav1.Condition{{Type: _readyCondition,
					Status: metav1.ConditionFalse, Reason: _scenarioRemoteMissing, ObservedGeneration: 2}}
				g, _ := testGateway(t, store, provider, session)
				scenarioRecover(t, g, store, provider)
				if tt.closed {
					if err := store.Update(t.Context(), _testUID, func(r *Record) error { r.Closed = true; return nil }); err != nil {
						t.Fatal(err)
					}
				}
				before := mustRecord(t, store)
				server := httptest.NewUnstartedServer(g)
				server.Config.ReadHeaderTimeout = 5 * time.Second
				server.Config.ReadTimeout = 30 * time.Second
				server.Config.IdleTimeout = 60 * time.Second
				server.Config.MaxHeaderBytes = 16 << 10
				server.StartTLS()
				t.Cleanup(server.Close)
				caller := server.Client()
				caller.Timeout = 10 * time.Second
				t.Cleanup(caller.CloseIdleConnections)
				request, err := http.NewRequestWithContext(t.Context(), http.MethodPost,
					server.URL+mutation.path, strings.NewReader(mutation.body))
				if err != nil {
					t.Fatal(err)
				}
				// urllib uses Connection: close and a known Content-Length for the
				// short JSON POST that exposed the deployed acceptance failure.
				request.Close = true
				request.Header.Set("Authorization", "Bearer "+mutation.token)
				request.Header.Set("Content-Type", "application/json")
				request.Header.Set("Idempotency-Key", "tls-fenced-"+mutation.name)
				response, err := caller.Do(request)
				if err != nil {
					t.Fatalf("lifecycle rejection reset the TLS request: %v", err)
				}
				body, readErr := io.ReadAll(response.Body)
				closeErr := response.Body.Close()
				if readErr != nil || closeErr != nil {
					t.Fatalf("lifecycle response did not complete: read=%v close=%v", readErr, closeErr)
				}
				if response.StatusCode != tt.wantStatus || len(body) == 0 {
					t.Fatalf("got HTTP %d %q, want HTTP %d with rejection body", response.StatusCode, body, tt.wantStatus)
				}
				after := mustRecord(t, store)
				if !reflect.DeepEqual(before.Submissions, after.Submissions) || !reflect.DeepEqual(before.Actions, after.Actions) ||
					!reflect.DeepEqual(before.Notes, after.Notes) || provider.posts != 0 {
					t.Fatal("TLS rejection accepted input, changed an approval, or performed a provider write")
				}
			})
		}
	}
}

// urllib transmits headers and body separately. Deliberately hold the body
// after the server receives headers, then require a complete rejection response
// after the short bounded body is sent. This makes early-close timing observable.
func TestScenarioGatewayTLSLifecycleFenceWaitsForShortBody(t *testing.T) {
	tests := []struct {
		name              string
		closed            bool
		missingDuringRead bool
		wantStatus        int
	}{
		{name: "controller remote missing", wantStatus: http.StatusServiceUnavailable},
		{name: "sealed conversation", closed: true, wantStatus: http.StatusGone},
		{name: "remote disappears during body read", missingDuringRead: true, wantStatus: http.StatusServiceUnavailable},
	}
	mutations := []struct {
		name  string
		path  string
		body  string
		token string
	}{
		{name: _scenarioMessage, path: _testMessagePath, body: _testFirstMessageBody, token: _token},
		{name: _scenarioCancel, path: _scenarioSessionPath + "/cancel",
			body: string(mustJSON(t, map[string]string{"turn_id": _testTurnID})), token: _token},
		{name: _scenarioApproval, path: _scenarioSessionPath + "/actions/" + actionID(scenarioPendingAction()),
			body: string(mustJSON(t, map[string]string{"decision": _approve})), token: _token + "-approver"},
	}
	for _, tt := range tests {
		for _, mutation := range mutations {
			t.Run(tt.name+"/"+mutation.name, func(t *testing.T) {
				store := testStore(t)
				provider := simulatorForTest()
				provider.history.Actions = append(provider.history.Actions, scenarioPendingAction())
				session := testSession()
				session.Generation = 2
				session.Status.Conditions = []metav1.Condition{{Type: _readyCondition,
					Status: metav1.ConditionFalse, Reason: _scenarioRemoteMissing, ObservedGeneration: 2}}
				if tt.missingDuringRead {
					session.Status.Conditions[0].Status = metav1.ConditionTrue
					session.Status.Conditions[0].Reason = "RemoteSessionCreated"
				}
				g, writer := testGateway(t, store, provider, session)
				scenarioRecover(t, g, store, provider)
				if tt.closed {
					if err := store.Update(t.Context(), _testUID, func(r *Record) error { r.Closed = true; return nil }); err != nil {
						t.Fatal(err)
					}
				}
				before := mustRecord(t, store)
				headersSeen := make(chan struct{})
				bodyReadStarted := make(chan struct{})
				server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					r.Body = &scenarioTLSBody{ReadCloser: r.Body, started: bodyReadStarted}
					close(headersSeen)
					g.ServeHTTP(w, r)
				}))
				server.Config.ReadHeaderTimeout = 5 * time.Second
				server.Config.ReadTimeout = 30 * time.Second
				server.Config.IdleTimeout = 60 * time.Second
				server.Config.MaxHeaderBytes = 16 << 10
				server.StartTLS()
				t.Cleanup(server.Close)
				roots := x509.NewCertPool()
				roots.AddCert(server.Certificate())
				connection, err := tls.DialWithDialer(&net.Dialer{Timeout: 5 * time.Second}, "tcp",
					strings.TrimPrefix(server.URL, "https://"), &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = connection.Close() })
				if err := connection.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
					t.Fatal(err)
				}
				_, err = fmt.Fprintf(connection, "POST %s HTTP/1.1\r\nHost: %s\r\nConnection: close\r\nContent-Type: application/json\r\nAuthorization: Bearer %s\r\nIdempotency-Key: tls-split-%s\r\nContent-Length: %d\r\n\r\n",
					mutation.path, connection.RemoteAddr(), mutation.token, mutation.name, len(mutation.body))
				if err != nil {
					t.Fatal(err)
				}
				select {
				case <-headersSeen:
				case <-time.After(2 * time.Second):
					t.Fatal("server did not receive split-request headers")
				}
				if tt.missingDuringRead {
					select {
					case <-bodyReadStarted:
					case <-time.After(2 * time.Second):
						t.Fatal("server did not start reading the held POST body")
					}
					session.Status.Conditions[0].Status = metav1.ConditionFalse
					session.Status.Conditions[0].Reason = _scenarioRemoteMissing
					if err := writer.Update(t.Context(), session); err != nil {
						t.Fatal(err)
					}
				}
				if err := connection.SetReadDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
					t.Fatal(err)
				}
				var firstByte [1]byte
				n, readErr := connection.Read(firstByte[:])
				var timeout net.Error
				if n != 0 || !errors.As(readErr, &timeout) || !timeout.Timeout() {
					t.Fatalf("server responded or closed before the short POST body: bytes=%d error=%v", n, readErr)
				}
				if err := connection.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
					t.Fatal(err)
				}
				if _, err := io.WriteString(connection, mutation.body); err != nil {
					t.Fatalf("lifecycle rejection reset body transmission: %v", err)
				}
				response, err := http.ReadResponse(bufio.NewReader(connection), &http.Request{Method: http.MethodPost})
				if err != nil {
					t.Fatal(err)
				}
				body, readErr := io.ReadAll(response.Body)
				closeErr := response.Body.Close()
				if readErr != nil || closeErr != nil || response.StatusCode != tt.wantStatus || len(body) == 0 {
					t.Fatalf("incomplete lifecycle response: status=%d read=%v close=%v body=%q", response.StatusCode, readErr, closeErr, body)
				}
				after := mustRecord(t, store)
				if !reflect.DeepEqual(before.Submissions, after.Submissions) || !reflect.DeepEqual(before.Actions, after.Actions) ||
					!reflect.DeepEqual(before.Notes, after.Notes) || provider.posts != 0 {
					t.Fatal("split TLS request changed input, approvals, notes, or provider state")
				}
			})
		}
	}
}
