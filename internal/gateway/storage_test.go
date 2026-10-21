package gateway

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestStorageCanceledRequestDoesNotAcceptConversation(t *testing.T) {
	store := testStore(t)
	g, _, _ := profileConversationGateway(t, store)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	request := httptest.NewRequest(http.MethodPost, _conversationPath, strings.NewReader(_profileTestBody)).WithContext(ctx)
	request.Header.Set("Idempotency-Key", "canceled-http")
	response := httptest.NewRecorder()
	g.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("canceled request returned %d", response.Code)
	}
	records, err := store.Records(t.Context())
	if err != nil || len(records) != 0 {
		t.Fatalf("canceled request persisted a conversation: %d %v", len(records), err)
	}
}

func TestStorageCanceledLifecycleRead(t *testing.T) {
	store := testStore(t)
	g, _ := testGateway(t, store, simulatorForTest(), testSession())
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, _, err := g.InitialInput(ctx, _testUID); !errors.Is(err, context.Canceled) {
		t.Fatalf("lifecycle read lost caller cancellation: %v", err)
	}
}
