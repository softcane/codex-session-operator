package job

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestSubmitJob(t *testing.T) {
	store := NewStore()
	request := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(`{"request_id":"first"}`))
	response := httptest.NewRecorder()
	Handler(store).ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body %s", response.Code, response.Body.String())
	}
	var created Job
	if err := json.Unmarshal(response.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	if created.ID != 1 || created.RequestID != "first" || store.Count() != 1 {
		t.Fatalf("submission = %+v, count %d", created, store.Count())
	}
}
