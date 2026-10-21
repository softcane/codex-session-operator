package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

const _identityTestPath = "/v1/namespaces/team/identity"

type unavailableMetadataReader struct{ calls atomic.Int64 }

func (r *unavailableMetadataReader) Get(context.Context, client.ObjectKey, client.Object, ...client.GetOption) error {
	r.calls.Add(1)
	return errors.New("private metadata failure")
}

func (r *unavailableMetadataReader) List(context.Context, client.ObjectList, ...client.ListOption) error {
	r.calls.Add(1)
	return errors.New("private metadata failure")
}

func TestIdentityRemainsAvailableWithoutMetadataOrStorage(t *testing.T) {
	store := testStore(t)
	g, writer := testGateway(t, store, simulatorForTest())
	if err := g.EnableProfiles(writer); err != nil {
		t.Fatal(err)
	}
	unavailable := &unavailableMetadataReader{}
	g.reader, g.profiles.reader = unavailable, unavailable
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	response := request(g, http.MethodGet, _identityTestPath, "", "")
	var identity map[string]string
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &identity) != nil ||
		identity[_callerSubjectField] != _testConversationSubject || identity["namespace"] != _testNamespace ||
		len(identity) != 2 || unavailable.calls.Load() != 0 {
		t.Fatal("identity discovery depended on Kubernetes or durable storage", response.Code, response.Body.String())
	}
	for _, tt := range []struct {
		path, method string
		want         int
	}{
		{path: _identityTestPath, method: http.MethodGet, want: http.StatusOK},
		{path: "/v1/namespaces/other/identity", method: http.MethodGet, want: http.StatusOK},
		{path: _identityTestPath, method: http.MethodPost, want: http.StatusMethodNotAllowed},
	} {
		if response := request(g, tt.method, tt.path, "", ""); response.Code != tt.want {
			t.Fatal("identity method/URL-namespace contract changed", response.Code, tt.want)
		}
	}
}
