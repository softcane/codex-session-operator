package gateway

import (
	"context"
	"encoding/json"
	"testing"
)

const (
	_testWorkRouteID   = _profileTestName
	_testWorkUnlimited = "unlimited"
	_testWorkOther     = "other"
	_testWorkPhase     = "Ready"
)

func profileConversationGateway(t *testing.T, store Storage) (*Gateway, *sessionWriter, *simulator) {
	t.Helper()
	_, objects := readyProfileObjects(_testNamespace)
	provider := simulatorForTest()
	gateway, k8s := testGateway(t, store, provider, objects...)
	writer := &sessionWriter{Client: k8s}
	if err := gateway.EnableProfiles(writer); err != nil {
		t.Fatal(err)
	}
	return gateway, writer, provider
}

func readWorkRecord(t *testing.T, store Storage, id string) Record {
	t.Helper()
	record, err := store.Get(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return record
}

func mustJSON(t testing.TB, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
