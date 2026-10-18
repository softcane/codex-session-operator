package gateway

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
)

const _internalToken = "gateway-controller-token-0123456789abcdef"
const _certificatePEMType = "CERTIFICATE"
const (
	_rpcFirstKey     = "first"
	_rpcFirstMessage = "first message"
	_rpcToolsField   = "tools"
)

func TestSeparateGatewayLifecycleRPC(t *testing.T) {
	store := testStore(t)
	g, _ := testGateway(t, store, simulatorForTest(), testSession())
	if err := store.Update(t.Context(), "uid-1", func(r *Record) error {
		r.Namespace = _testNamespace
		r.Name = _testSessionName
		r.RemoteID = _testRemoteID
		r.InitialKey = _rpcFirstKey
		r.Submissions = []Submission{{Key: _rpcFirstKey, RemoteKey: wireKey("uid-1", _rpcFirstKey), Text: _rpcFirstMessage, Initial: true}}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	handler, err := g.InternalHandler(_internalToken)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewTLSServer(handler)
	defer server.Close()
	certificate, err := x509.ParseCertificate(server.TLS.Certificates[0].Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	ca := pem.EncodeToMemory(&pem.Block{Type: _certificatePEMType, Bytes: certificate.Raw})
	remote, err := NewRemoteClient(server.URL, _internalToken, ca)
	if err != nil {
		t.Fatal(err)
	}
	text, key, err := remote.InitialInput(context.Background(), "uid-1")
	if err != nil || text != _rpcFirstMessage || key != wireKey("uid-1", _rpcFirstKey) {
		t.Fatalf("initial input %q %q: %v", text, key, err)
	}
	if err := remote.ValidateTools(context.Background(), map[string]any{_rpcToolsField: []any{map[string]any{"type": "function", "name": _echoFunction}}}); err != nil {
		t.Fatal(err)
	}
	if err := remote.ValidateTools(context.Background(), map[string]any{_rpcToolsField: []any{map[string]any{"type": "function", "name": "unregistered"}}}); err == nil {
		t.Fatal("unregistered handler allowed")
	}
	if err := remote.ValidateTools(context.Background(), map[string]any{_rpcToolsField: "malformed"}); err == nil {
		t.Fatal("malformed tool list allowed")
	}
	if err := remote.Quiesce(context.Background(), "uid-1", _testRemoteID); err != nil {
		t.Fatal(err)
	}
	if !mustRecord(t, store).Closed {
		t.Fatal("gateway did not persist the deletion fence")
	}
	if _, _, err := remote.InitialInput(context.Background(), "uid-1"); err == nil {
		t.Fatal("closed Session returned initial input")
	}
	req := httptest.NewRequest(http.MethodPost, internalPath+"initial-input", nil)
	req.Header.Set("Authorization", "Bearer wrong-token")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("internal RPC without token returned %d", w.Code)
	}
}

func TestRemoteClientValidatesCurrentProfileSessionPolicy(t *testing.T) {
	store := testStore(t)
	g, writer, _ := profileGateway(t, store)
	receipt := readConversationReceipt(t, conversationRequest(g, http.MethodPost, _conversationPath,
		"policy-rpc", _profileTestBody))
	recordID := conversationRecordID(_testNamespace, _testConversationSubject, "policy-rpc")
	if err := g.syncWork(t.Context(), recordID); err != nil {
		t.Fatal(err)
	}
	record := readWorkRecord(t, store, recordID)
	session := workerWorkSession(t, writer, record)
	environment := &agentsv1alpha1.SessionEnvironmentSnapshot{
		Name: _profileTestEnvironment,
		UID:  _profileTestEnvironmentUID,
		Profile: agentsv1alpha1.EnvironmentSpec{
			Mode: _noneMode,
		},
	}

	handler, err := g.InternalHandler(_internalToken)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewTLSServer(handler)
	defer server.Close()
	certificate, err := x509.ParseCertificate(server.TLS.Certificates[0].Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	ca := pem.EncodeToMemory(&pem.Block{Type: _certificatePEMType, Bytes: certificate.Raw})
	remote, err := NewRemoteClient(server.URL, _internalToken, ca)
	if err != nil {
		t.Fatal(err)
	}
	if err := remote.ValidateSessionPolicy(t.Context(), session, environment); err != nil {
		t.Fatal("current Profile Session policy was rejected", err)
	}
	replacement := environment.DeepCopy()
	replacement.UID = "replacement-environment"
	if err := remote.ValidateSessionPolicy(t.Context(), session, replacement); err == nil {
		t.Fatal("replacement dependency identity passed the gateway policy gate")
	}
	if receipt.ConversationID == "" {
		t.Fatal("policy validation fixture did not create a conversation")
	}
}

func TestInternalClientRequiresTLSAndCA(t *testing.T) {
	if _, err := NewRemoteClient("http://gateway:8083", _internalToken, []byte("bad")); err == nil {
		t.Fatal("plaintext gateway accepted")
	}
	if _, err := NewRemoteClient("https://gateway:8083", _internalToken, []byte("bad")); err == nil {
		t.Fatal("invalid CA accepted")
	}
	if _, err := (&Gateway{}).InternalHandler("short"); err == nil {
		t.Fatal("short token accepted")
	}
}

func TestShutdownHTTPServerForcesCloseAfterDeadline(t *testing.T) {
	requestStarted := make(chan struct{})
	releaseHandler := make(chan struct{})
	server := &http.Server{Handler: http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		close(requestStarted)
		<-releaseHandler
	})}
	t.Cleanup(func() {
		select {
		case <-releaseHandler:
		default:
			close(releaseHandler)
		}
		_ = server.Close()
	})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	responseDone := make(chan struct{})
	go func() {
		response, requestErr := http.Get("http://" + listener.Addr().String()) //nolint:noctx
		if requestErr == nil {
			_ = response.Body.Close()
		}
		close(responseDone)
	}()
	select {
	case <-requestStarted:
	case <-time.After(time.Second):
		t.Fatal("Request did not reach server")
	}
	if err := shutdownHTTPServer(server, done, 10*time.Millisecond); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Forced shutdown returned %v", err)
	}
	close(releaseHandler)
	select {
	case <-responseDone:
	case <-time.After(time.Second):
		t.Fatal("Forced shutdown did not release client")
	}
}
