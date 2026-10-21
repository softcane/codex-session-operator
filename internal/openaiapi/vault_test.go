package openaiapi

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const testVaultExampleHost = "example.com"
const testVaultLimitedNetworking = "limited"
const testVaultClientIDField = "client_id"
const testVaultTokenEndpointField = "token_endpoint"
const testVaultCredentialID = "cred_test"
const testVaultID = "vault_test"
const testVaultSecretNameField = "secret_name"

const testVaultServerURLField = "mcp_server_url"
const testVaultNetworkingField = "networking"
const testVaultTokenField = "token"
const testVaultMCPURL = "https://mcp.example.com/mcp"
const testVaultVariable = "TEST_TOKEN"
const testVaultUID = "uid"
const testVaultCredentialName = "test"
const testVaultDeletedField = "deleted"
const testVaultScope = "scope"

func TestVaultUpdateWirePreservesOmissionAndExplicitClear(t *testing.T) {
	owner := map[string]string{ownerKey: testVaultUID, operatorKey: operatorValue, agentScopeKey: testVaultScope, "external": "kept"}
	posts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		posts++
		if r.Method != http.MethodPost || r.URL.Path != "/v1/vaults/"+testVaultID || r.Header.Get("OpenAI-Beta") != agentsBetaHeader || r.Header.Get("Authorization") == "" {
			t.Errorf("unexpected update request: %s %s", r.Method, r.URL.Path)
		}
		var body map[string]json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		switch posts {
		case 1:
			if _, ok := body[nameField]; !ok || body[metadataField] != nil {
				t.Error("name-only update changed metadata")
			}
		case 2:
			if string(body[nameField]) != "null" || body[metadataField] == nil {
				t.Error("explicit clear or metadata replacement was omitted")
			}
		default:
			t.Error("unexpected Vault retry")
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"id": testVaultID, "name": "", "metadata": owner, "created_at": 1, "object": "vault"})
	}))
	defer server.Close()
	c := clientForServer(server, nil)
	name := " Renamed "
	if _, err := c.UpdateVault(t.Context(), testVaultID, &name, false, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := c.UpdateVault(t.Context(), testVaultID, nil, true, &owner); err != nil {
		t.Fatal(err)
	}
	if posts != 2 {
		t.Fatalf("updates=%d", posts)
	}
}

func controlledCredential(t *testing.T) string {
	t.Helper()
	var value [24]byte
	if _, err := rand.Read(value[:]); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(value[:])
}
func credentialRef() agentsv1alpha1.CredentialSecretReference {
	return agentsv1alpha1.CredentialSecretReference{Name: VaultSecretName, Key: testVaultTokenField}
}
func TestCredentialAuthWireVariants(t *testing.T) {
	token := controlledCredential(t)
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: VaultSecretName}, Data: map[string][]byte{testVaultTokenField: []byte(token)}}
	type authTest struct {
		name string
		auth agentsv1alpha1.VaultCredentialAuth
		want map[string]any
	}
	tests := make([]authTest, 0, 6)
	tests = append(tests,
		authTest{name: "bearer", auth: agentsv1alpha1.VaultCredentialAuth{Type: staticBearerType, StaticBearer: &agentsv1alpha1.VaultStaticBearer{ServerURL: testVaultMCPURL, TokenSecretRef: credentialRef()}}, want: map[string]any{typeField: staticBearerType, testVaultServerURLField: testVaultMCPURL, testVaultTokenField: token}},
		authTest{name: "hosted limited", auth: agentsv1alpha1.VaultCredentialAuth{Type: environmentCredentialType, EnvironmentVariable: &agentsv1alpha1.VaultEnvironmentVariable{VariableName: testVaultVariable, SecretValueSecretRef: credentialRef(), Networking: agentsv1alpha1.VaultCredentialNetworking{Type: testVaultLimitedNetworking, AllowedHosts: []string{"api.example.com"}}}}, want: map[string]any{typeField: environmentCredentialType, testVaultSecretNameField: testVaultVariable, "secret_value": token, testVaultNetworkingField: map[string]any{typeField: testVaultLimitedNetworking, "allowed_hosts": []string{"api.example.com"}}}},
		authTest{name: "hosted unrestricted", auth: agentsv1alpha1.VaultCredentialAuth{Type: environmentCredentialType, EnvironmentVariable: &agentsv1alpha1.VaultEnvironmentVariable{VariableName: testVaultVariable, SecretValueSecretRef: credentialRef(), Networking: agentsv1alpha1.VaultCredentialNetworking{Type: "unrestricted"}}}, want: map[string]any{typeField: environmentCredentialType, testVaultSecretNameField: testVaultVariable, "secret_value": token, testVaultNetworkingField: map[string]any{typeField: "unrestricted"}}},
	)
	for _, method := range []string{_noneValue, "client_secret_basic", "client_secret_post"} {
		b := &agentsv1alpha1.VaultMCPOAuth{ServerURL: testVaultMCPURL, AccessTokenSecretRef: credentialRef(), ExpirationTime: &metav1.Time{Time: time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)}, Refresh: &agentsv1alpha1.VaultOAuthRefresh{TokenEndpoint: "https://auth.example.com/token", ClientID: "test-client", RefreshTokenSecretRef: credentialRef(), TokenEndpointAuth: agentsv1alpha1.VaultTokenEndpointAuth{Type: method}}}
		auth := map[string]any{typeField: method}
		if method != _noneValue {
			ref := credentialRef()
			b.Refresh.TokenEndpointAuth.ClientSecretRef = &ref
			auth["client_secret"] = token
		}
		tests = append(tests, authTest{name: "oauth " + method, auth: agentsv1alpha1.VaultCredentialAuth{Type: oauthCredentialType, MCPOAuth: b}, want: map[string]any{typeField: oauthCredentialType, testVaultServerURLField: b.ServerURL, "access_token": token, "expires_at": "2030-01-01T00:00:00Z", "refresh": map[string]any{testVaultTokenEndpointField: b.Refresh.TokenEndpoint, testVaultClientIDField: b.Refresh.ClientID, "refresh_token": token, "token_endpoint_auth": auth}}})
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			give, err := CredentialAuth(tt.auth, secret, false)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(give, tt.want) {
				t.Fatal("credential create wire differs from independent fixture; values withheld")
			}
			rotated, err := CredentialAuth(tt.auth, secret, true)
			if err != nil {
				t.Fatal(err)
			}
			for _, key := range []string{testVaultServerURLField, testVaultSecretNameField, testVaultNetworkingField} {
				if _, ok := rotated[key]; ok {
					t.Fatal("rotation sends immutable identity")
				}
			}
			if refresh, ok := rotated["refresh"].(map[string]any); ok {
				for _, key := range []string{testVaultTokenEndpointField, testVaultClientIDField, "resource"} {
					if _, ok := refresh[key]; ok {
						t.Fatal("rotation sends immutable OAuth identity")
					}
				}
			}
		})
	}
}
func TestCredentialRejectsUnsafeReferencesAndValues(t *testing.T) {
	auth := agentsv1alpha1.VaultCredentialAuth{Type: staticBearerType, StaticBearer: &agentsv1alpha1.VaultStaticBearer{ServerURL: testVaultMCPURL, TokenSecretRef: credentialRef()}}
	tests := []struct {
		name   string
		secret *corev1.Secret
	}{
		{name: "missing protected source"}, {name: "unrelated", secret: &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "unrelated"}, Data: map[string][]byte{testVaultTokenField: []byte(controlledCredential(t))}}},
		{name: "empty protected source", secret: &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: VaultSecretName}}},
		{name: "newline", secret: &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: VaultSecretName}, Data: map[string][]byte{testVaultTokenField: []byte("\n")}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := CredentialAuth(auth, tt.secret, false); err == nil {
				t.Fatal("unsafe credential accepted")
			}
		})
	}
}
func TestCredentialSDKLifecycleAndReadbackRedaction(t *testing.T) {
	token := controlledCredential(t)
	posts := 0
	metadata := map[string]string{ownerKey: testVaultUID, operatorKey: operatorValue, agentScopeKey: testVaultScope}
	public := map[string]any{typeField: staticBearerType, testVaultServerURLField: testVaultMCPURL, testVaultTokenField: token}
	body := map[string]any{"id": testVaultCredentialID, "vault_id": testVaultID, nameField: testVaultCredentialName, metadataField: metadata, "auth": public}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Header.Get("OpenAI-Beta") != agentsBetaHeader {
			t.Error("missing beta contract")
		}
		switch r.Method {
		case http.MethodPost:
			posts++
			var request map[string]any
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Error(err)
			}
			auth, _ := request["auth"].(map[string]any)
			if auth[testVaultTokenField] != token || auth[typeField] != staticBearerType {
				t.Error("protected write missing; values withheld")
			}
			if posts == 1 && request[nameField] != testVaultCredentialName {
				t.Error("credential name missing")
			}
			_ = json.NewEncoder(w).Encode(body)
		case http.MethodGet:
			_ = json.NewEncoder(w).Encode(body)
		case http.MethodDelete:
			_ = json.NewEncoder(w).Encode(map[string]any{"id": testVaultCredentialID, testVaultDeletedField: true})
		}
	}))
	defer server.Close()
	c := clientForServer(server, nil)
	remote, err := c.CreateCredential(context.Background(), testVaultID, testVaultCredentialName, metadata, public)
	if err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(remote)
	if strings.Contains(string(encoded), token) {
		t.Fatal("readback exposes protected credential")
	}
	if remote.OwnerUID != testVaultUID || remote.OwnerScope != testVaultScope {
		t.Fatal("ownership missing")
	}
	if _, err := c.UpdateCredential(context.Background(), testVaultID, remote.ID, map[string]any{"auth": map[string]any{typeField: staticBearerType, testVaultTokenField: token}}); err != nil {
		t.Fatal(err)
	}
	if err := c.DeleteCredential(context.Background(), testVaultID, remote.ID); err != nil {
		t.Fatal(err)
	}
	if posts != 2 {
		t.Fatal("unexpected SDK retry")
	}
}
func TestVaultDiscoveryRequiresCompleteUniqueScopeScan(t *testing.T) {
	tests := []struct {
		name      string
		body      string
		wantError bool
		wantID    string
	}{
		{name: "owned", body: `{"data":[{"id":"vault_test","metadata":{"kubernetes_uid":"uid","operator":"codex-session-operator","owner_scope":"scope"}}],"has_more":false}`, wantID: "vault_test"},
		{name: "absent", body: `{"data":[],"has_more":false}`},
		{name: "incomplete", body: `{"data":[]}`, wantError: true},
		{name: "duplicate", body: `{"data":[{"id":"one","metadata":{"kubernetes_uid":"uid","operator":"codex-session-operator","owner_scope":"scope"}},{"id":"two","metadata":{"kubernetes_uid":"uid","operator":"codex-session-operator","owner_scope":"scope"}}],"has_more":false}`, wantError: true},
		{name: "wrong scope", body: `{"data":[{"id":"foreign","metadata":{"kubernetes_uid":"uid","operator":"codex-session-operator","owner_scope":"other"}}],"has_more":false}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(tt.body))
			}))
			defer server.Close()
			result, err := clientForServer(server, nil).FindVault(context.Background(), testVaultUID, testVaultScope)
			if (err != nil) != tt.wantError || result.ID != tt.wantID {
				t.Fatal("discovery did not enforce unique complete ownership scan")
			}
		})
	}
}
func TestCredentialProviderErrorRedactionAndCancellation(t *testing.T) {
	secret := controlledCredential(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(500); _, _ = w.Write([]byte(secret)) }))
	defer server.Close()
	c := clientForServer(server, nil)
	_, err := c.CreateCredential(context.Background(), "vault", testVaultCredentialName, nil, map[string]any{})
	if err == nil || strings.Contains(err.Error(), secret) {
		t.Fatal("provider failure not safely classified")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = c.GetCredential(ctx, "vault", "cred")
	if !errors.Is(err, context.Canceled) {
		t.Fatal("cancellation lost")
	}
}

func TestCredentialDiscoveryRejectsPartialDuplicateAndForeignIdentity(t *testing.T) {
	tests := []struct {
		name      string
		data      string
		wantError bool
		wantID    string
	}{
		{name: "owned", data: `{"data":[{"id":"cred_test","vault_id":"vault_test","metadata":{"kubernetes_uid":"uid","operator":"codex-session-operator","owner_scope":"scope"}}],"has_more":false}`, wantID: "cred_test"},
		{name: "duplicate", data: `{"data":[{"id":"one","vault_id":"vault_test","metadata":{"kubernetes_uid":"uid","operator":"codex-session-operator","owner_scope":"scope"}},{"id":"two","vault_id":"vault_test","metadata":{"kubernetes_uid":"uid","operator":"codex-session-operator","owner_scope":"scope"}}],"has_more":false}`, wantError: true},
		{name: "partial", data: `{"data":[]}`, wantError: true},
		{name: "foreign vault", data: `{"data":[{"id":"foreign","vault_id":"other-vault","metadata":{"kubernetes_uid":"uid","operator":"codex-session-operator","owner_scope":"scope"}}],"has_more":false}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(tt.data))
			}))
			defer server.Close()
			remote, err := clientForServer(server, nil).FindCredential(context.Background(), testVaultID, testVaultUID, testVaultScope)
			if (err != nil) != tt.wantError || remote.ID != tt.wantID {
				t.Fatal("credential discovery accepted incomplete or nonunique ownership")
			}
		})
	}
}

func TestOAuthOptionalRefreshAndNullRotationFields(t *testing.T) {
	token := controlledCredential(t)
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: VaultSecretName}, Data: map[string][]byte{testVaultTokenField: []byte(token)}}
	scope, resource := "read test", "https://api.example.com/controlled"
	ref := credentialRef()
	spec := agentsv1alpha1.VaultCredentialAuth{Type: oauthCredentialType, MCPOAuth: &agentsv1alpha1.VaultMCPOAuth{ServerURL: testVaultMCPURL, AccessTokenSecretRef: ref, Refresh: &agentsv1alpha1.VaultOAuthRefresh{ClientID: "controlled-client", TokenEndpoint: "https://auth.example.com/token", RefreshTokenSecretRef: ref, Resource: &resource, Scope: &scope, TokenEndpointAuth: agentsv1alpha1.VaultTokenEndpointAuth{Type: "client_secret_basic", ClientSecretRef: &ref}}}}
	created, err := CredentialAuth(spec, secret, false)
	if err != nil {
		t.Fatal(err)
	}
	refresh, ok := created["refresh"].(map[string]any)
	if !ok || refresh["resource"] != resource || refresh[scopeField] != scope {
		t.Fatal("optional grant fields were omitted")
	}
	if _, present := created["expires_at"]; present {
		t.Fatal("absent create expiry must remain omitted")
	}
	spec.MCPOAuth.Refresh.Scope = nil
	spec.MCPOAuth.Refresh.ClearScope = true
	rotated, err := CredentialAuth(spec, secret, true)
	if err != nil {
		t.Fatal(err)
	}
	refresh, ok = rotated["refresh"].(map[string]any)
	if !ok {
		t.Fatal("refresh rotation is missing")
	}
	if value, present := refresh[scopeField]; !present || value != nil {
		t.Fatal("scope reset must send null")
	}
	if value, present := rotated["expires_at"]; !present || value != nil {
		t.Fatal("expiry reset with replacement token must send null")
	}
	for _, key := range []string{"resource", testVaultClientIDField, testVaultTokenEndpointField} {
		if _, present := refresh[key]; present {
			t.Fatal("rotation changed immutable grant identity")
		}
	}
}

func TestCredentialNetworkingRejectsUnsafeHostForms(t *testing.T) {
	tests := []struct {
		name  string
		hosts []string
	}{
		{name: "wildcard", hosts: []string{"*.example.com"}}, {name: "scheme", hosts: []string{"https://example.com"}},
		{name: "port", hosts: []string{"example.com:443"}}, {name: "path", hosts: []string{"example.com/path"}},
		{name: "IPv6", hosts: []string{"::1"}}, {name: "uppercase", hosts: []string{"EXAMPLE.COM"}},
		{name: duplicateScenario, hosts: []string{testVaultExampleHost, testVaultExampleHost}}, {name: "missing protected source"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if validateCredentialNetworking(agentsv1alpha1.VaultCredentialNetworking{Type: testVaultLimitedNetworking, AllowedHosts: tt.hosts}) == nil {
				t.Fatal("unsafe host configuration accepted")
			}
		})
	}
}
