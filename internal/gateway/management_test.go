package gateway

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	_testDeletePolicy       = "Delete"
	_catalogEnvironmentName = "catalog-environment"
	_catalogVaultName       = "catalog-vault"
	_catalogRouteID         = "approved"
)

func TestSessionManagementRequiresExplicitChoicesAndNamespace(t *testing.T) {
	store := testStore(t)
	g, writer := testGateway(t, store, simulatorForTest())
	if err := g.EnableSessionManagement(writer); err != nil {
		t.Fatal(err)
	}
	endpoint := "/v1/namespaces/team/sessions"
	for _, body := range []string{
		`{"name":"investigation","spec":{"agentRef":{"name":"agent"},"environmentRef":{"name":"env"}}}`,
		`{"name":"investigation","spec":{"environmentRef":{"name":"env"},"spendControl":{"mode":"Unlimited"}}}`,
		`{"name":"investigation","spec":{"agentRef":{"name":"agent"},"spendControl":{"mode":"Unlimited"}}}`,
		`{"name":"investigation","spec":{"agentRef":{"name":"agent"},"environmentRef":{"name":"env"},"spendControl":{"mode":"Limited","limitCents":0}}}`,
	} {
		if got := request(g, http.MethodPost, endpoint, "", body).Code; got != http.StatusBadRequest {
			t.Fatalf("invalid create returned %d for %s", got, body)
		}
	}
	body := `{"name":"investigation","spec":{"agentRef":{"name":"agent"},"environmentRef":{"name":"env"},"spendControl":{"mode":"Limited","limitCents":150}}}`
	response := request(g, http.MethodPost, endpoint, "", body)
	if response.Code != http.StatusCreated || response.Header().Get("Location") != endpoint+"/investigation" {
		t.Fatalf("create: %d %s", response.Code, response.Body.String())
	}
	var saved agentsv1alpha1.Session
	if err := writer.Get(t.Context(), client.ObjectKey{Namespace: _testNamespace, Name: "investigation"}, &saved); err != nil {
		t.Fatal(err)
	}
	if saved.Spec.AgentRef.Name != _auditAgentName || saved.Spec.EnvironmentRef.Name != "env" || *saved.Spec.SpendControl.LimitCents != 150 {
		t.Fatalf("unexpected saved Session: %+v", saved.Spec)
	}
	if got := request(g, http.MethodPost, endpoint, "", body).Code; got != http.StatusConflict {
		t.Fatalf("duplicate create: %d", got)
	}
	response = request(g, http.MethodGet, endpoint, "", "")
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"name":"investigation"`) || strings.Contains(response.Body.String(), "openaiSessionID") {
		t.Fatalf("unsafe Session list: %d %s", response.Code, response.Body.String())
	}
}

func TestCatalogExposesOnlyNamesReadinessAndMode(t *testing.T) {
	store := testStore(t)
	agent := &agentsv1alpha1.Agent{ObjectMeta: metav1.ObjectMeta{Name: "private-agent", Namespace: _testNamespace}, Spec: agentsv1alpha1.AgentSpec{Model: "gpt-6-luna", DeletionPolicy: _testDeletePolicy}}
	instructions := "confidential instructions"
	agent.Spec.Instructions = &instructions
	environment := &agentsv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "compute", Namespace: _testNamespace}, Spec: agentsv1alpha1.EnvironmentSpec{Mode: agentsv1alpha1.EnvironmentModeNone, DeletionPolicy: _testDeletePolicy}}
	vault := &agentsv1alpha1.Vault{ObjectMeta: metav1.ObjectMeta{Name: "secrets", Namespace: _testNamespace}, Spec: agentsv1alpha1.VaultSpec{DeletionPolicy: _testDeletePolicy}}
	g, _ := testGateway(t, store, simulatorForTest(), agent, environment, vault)
	response := request(g, http.MethodGet, "/v1/namespaces/team/catalog", "", "")
	if response.Code != http.StatusOK || strings.Contains(response.Body.String(), instructions) || strings.Contains(response.Body.String(), "spec") {
		t.Fatalf("unsafe catalog: %d %s", response.Code, response.Body.String())
	}
	var catalog map[string]json.RawMessage
	if err := json.Unmarshal(response.Body.Bytes(), &catalog); err != nil {
		t.Fatal(err)
	}
	if len(catalog["agents"]) == 0 || len(catalog["environments"]) == 0 || len(catalog["vaults"]) == 0 {
		t.Fatalf("missing catalog items: %s", response.Body.String())
	}
}

func TestReadinessAndSessionListWithoutAuthentication(t *testing.T) {
	store := testStore(t)
	g, writer := testGateway(t, store, simulatorForTest())
	if err := g.EnableSessionManagement(writer); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/ui", "/ui/", "/ui/app.js", "/ui/style.css"} {
		if got := request(g, http.MethodGet, path, "", "").Code; got != http.StatusNotFound {
			t.Fatalf("removed interface path %s returned HTTP %d", path, got)
		}
	}
	if got := request(g, http.MethodGet, "/readyz", "", "").Code; got != http.StatusOK {
		t.Fatalf("worker readiness: %d", got)
	}
	if got := request(g, http.MethodGet, "/v1/namespaces/team/sessions", "", "").Code; got != http.StatusOK {
		t.Fatalf("Session list without credentials: %d", got)
	}
}

func TestUnsupportedMessageContentIsRejectedBeforePersistence(t *testing.T) {
	store := testStore(t)
	p := simulatorForTest()
	g, _ := testGateway(t, store, p, testSession())
	response := request(g, http.MethodPost, _testMessagePath, "image-1", `{"content":[{"type":"input_image","image_url":"https://example.com/image.png"}]}`)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("unsupported input accepted: %d", response.Code)
	}
	record, err := store.Get(t.Context(), "uid-1")
	if err != nil || len(record.Submissions) != 0 || p.commits != 0 {
		t.Fatal("unsupported input was queued or submitted")
	}
}
