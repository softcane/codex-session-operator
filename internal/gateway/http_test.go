package gateway

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// The probe signals only after the stream has flushed or an error has completed.
// Its output is discarded so waiting streams do not race on a shared recorder.
type streamProbe struct {
	header http.Header
	code   int
	ready  chan struct{}
	once   sync.Once
}

func TestGatewayListenersOnlyNegotiateHTTP1(t *testing.T) {
	fixture := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	certificate := fixture.TLS.Certificates[0]
	roots := x509.NewCertPool()
	roots.AddCert(fixture.Certificate())
	fixture.Close()
	key, err := x509.MarshalPKCS8PrivateKey(certificate.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	certFile, keyFile := filepath.Join(directory, "tls.crt"), filepath.Join(directory, "tls.key")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Certificate[0]}), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key}), 0600); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"public", "internal"} {
		t.Run(name, func(t *testing.T) {
			g, _ := testGateway(t, testStore(t), simulatorForTest())
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			address := listener.Addr().String()
			if err := listener.Close(); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			var start func(context.Context) error
			path, wantStatus := "/readyz", http.StatusOK
			if name == "public" {
				start = (&Server{Gateway: g, Address: address, CertFile: certFile, KeyFile: keyFile}).Start
			} else {
				handler, err := g.InternalHandler(_internalToken)
				if err != nil {
					t.Fatal(err)
				}
				start = (&InternalServer{Handler: handler, Address: address, CertFile: certFile, KeyFile: keyFile}).Start
				path, wantStatus = internalPath+"readyz", http.StatusNoContent
			}
			finished := make(chan error, 1)
			go func() { finished <- start(ctx) }()
			t.Cleanup(func() {
				cancel()
				select {
				case err := <-finished:
					if err != nil {
						t.Error(err)
					}
				case <-time.After(2 * time.Second):
					t.Error("TLS listener did not join shutdown")
				}
			})
			var connection *tls.Conn
			deadline := time.Now().Add(2 * time.Second)
			for time.Now().Before(deadline) {
				connection, err = tls.DialWithDialer(&net.Dialer{Timeout: 100 * time.Millisecond}, "tcp", address,
					&tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12, NextProtos: []string{"h2", "http/1.1"}})
				if err == nil {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = connection.Close() }()
			if got := connection.ConnectionState().NegotiatedProtocol; got != "http/1.1" {
				t.Fatalf("gateway negotiated %q", got)
			}
			if err := connection.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
				t.Fatal(err)
			}
			if _, err := fmt.Fprintf(connection, "GET %s HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n", path); err != nil {
				t.Fatal(err)
			}
			response, err := http.ReadResponse(bufio.NewReader(connection), &http.Request{Method: http.MethodGet})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = response.Body.Close() }()
			if response.StatusCode != wantStatus {
				t.Fatalf("HTTP/1 readiness returned %d", response.StatusCode)
			}
		})
	}
}

// Shorten a real configured write deadline so the regression does not wait for
// the production timeout. An unbounded server never calls this method.
type shortWriteConnection struct{ net.Conn }

func (c *shortWriteConnection) SetWriteDeadline(deadline time.Time) error {
	if !deadline.IsZero() {
		deadline = time.Now().Add(50 * time.Millisecond)
	}
	return c.Conn.SetWriteDeadline(deadline)
}

type oneConnectionListener struct {
	connection net.Conn
	accepted   bool
	closed     chan struct{}
	once       sync.Once
}

func (l *oneConnectionListener) Accept() (net.Conn, error) {
	if !l.accepted {
		l.accepted = true
		return l.connection, nil
	}
	<-l.closed
	return nil, net.ErrClosed
}
func (l *oneConnectionListener) Close() error {
	l.once.Do(func() { close(l.closed) })
	return nil
}
func (l *oneConnectionListener) Addr() net.Addr { return l.connection.LocalAddr() }

func TestOrdinaryResponseWriteDeadlineReleasesCapacity(t *testing.T) {
	store := testStore(t)
	g, _ := testGateway(t, store, simulatorForTest(), testSession())
	if err := store.Update(t.Context(), _testUID, func(r *Record) error {
		r.Namespace, r.Name = _testNamespace, _testSessionName
		r.Notes["large"] = strings.Repeat("x", 8<<20)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	serverSide, clientSide := net.Pipe()
	defer func() { _ = clientSide.Close() }()
	finished := make(chan struct{})
	server := gatewayHTTPServer(t.Context(), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(finished)
		g.ServeHTTP(w, r)
	}))
	listener := &oneConnectionListener{connection: &shortWriteConnection{Conn: serverSide}, closed: make(chan struct{})}
	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()
	t.Cleanup(func() {
		_ = server.Close()
		select {
		case <-served:
		case <-time.After(2 * time.Second):
			t.Error("response server did not join")
		}
	})
	if err := clientSide.SetWriteDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(clientSide, "GET /v1/namespaces/team/sessions/work HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer "+_token+"\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	// The client deliberately never reads its multi-megabyte response.
	select {
	case <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("slow response reader retained gateway capacity without a write deadline")
	}
	if len(g.requestSlots) != 0 {
		t.Fatal("timed-out response retained its ordinary request slot")
	}
}

func (w *streamProbe) Header() http.Header { return w.header }
func (w *streamProbe) WriteHeader(code int) {
	if w.code == 0 {
		w.code = code
	}
}
func (w *streamProbe) Write(data []byte) (int, error) {
	w.WriteHeader(http.StatusOK)
	return len(data), nil
}
func (w *streamProbe) Flush() { w.once.Do(func() { close(w.ready) }) }

func openTestEvents(t *testing.T, g *Gateway, token string) (int, func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	request := httptest.NewRequestWithContext(ctx, http.MethodGet,
		"/v1/namespaces/team/sessions/work/events", nil)
	request.Header.Set("Authorization", "Bearer "+token)
	w := &streamProbe{header: make(http.Header), ready: make(chan struct{})}
	done := make(chan struct{})
	go func() {
		defer close(done)
		g.ServeHTTP(w, request)
		w.Flush()
	}()
	stop := func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("event stream did not join after cancellation")
		}
	}
	t.Cleanup(stop)
	select {
	case <-w.ready:
		return w.code, stop
	case <-time.After(2 * time.Second):
		t.Fatal("event stream neither flushed nor returned a rejection")
		return 0, stop
	}
}

func TestEventStreamsLeaveMessageCapacity(t *testing.T) {
	g, _ := testGateway(t, testStore(t), simulatorForTest(), testSession())
	accepted := 0
	for range 64 {
		code, _ := openTestEvents(t, g, _token)
		switch code {
		case http.StatusOK:
			accepted++
		case http.StatusServiceUnavailable:
			// A bounded stream quota may reject excess observers.
		default:
			t.Fatalf("unexpected stream response: %d", code)
		}
	}
	if accepted == 0 {
		t.Fatal("no event stream was admitted")
	}
	if response := request(g, http.MethodPost, _testMessagePath, _token,
		"streams-still-allow-messages", _testFirstMessageBody); response.Code != http.StatusAccepted {
		t.Fatalf("event streams prevented a normal message: HTTP %d", response.Code)
	}
	if response := request(g, http.MethodGet, "/ui/", "", "", ""); response.Code != http.StatusOK {
		t.Fatalf("event streams prevented opening the UI: HTTP %d", response.Code)
	}
}

func TestEventStreamQuotaSharesRotatedTokensAndReleasesOnCancel(t *testing.T) {
	g, _ := testGateway(t, testStore(t), simulatorForTest(), testSession())
	rotatedToken := _token + "-rotated"
	rotated, err := copyPrincipals([]Principal{{Token: rotatedToken, Subject: "writer", Namespace: _testNamespace}})
	if err != nil {
		t.Fatal(err)
	}
	g.principals = append(g.principals, rotated...)
	var stopFirst func()
	for i := range 4 {
		code, stop := openTestEvents(t, g, _token)
		if code != http.StatusOK {
			t.Fatalf("stream %d rejected before its quota: HTTP %d", i, code)
		}
		if i == 0 {
			stopFirst = stop
		}
	}
	if code, _ := openTestEvents(t, g, rotatedToken); code != http.StatusServiceUnavailable {
		t.Fatalf("rotated token bypassed its principal quota: HTTP %d", code)
	}
	if code, _ := openTestEvents(t, g, _token+"-approver"); code != http.StatusOK {
		t.Fatalf("one principal exhausted another principal's streams: HTTP %d", code)
	}
	stopFirst()
	if code, _ := openTestEvents(t, g, rotatedToken); code != http.StatusOK {
		t.Fatalf("canceled stream retained its quota: HTTP %d", code)
	}
}
