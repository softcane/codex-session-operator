package openaiapi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"maps"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	openai "github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/option"
	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/validation"
)

const httpsScheme = "https"

const VaultSecretName = "codex-vault"
const staticBearerType = "static_bearer"
const oauthCredentialType = "mcp_oauth"
const environmentCredentialType = "environment_variable"
const limitedNetworkingType = "limited"
const scopeField = "scope"

// Vault contains public remote fields only.
type Vault struct {
	ID, OwnerUID, OwnerScope, Name string
	Metadata                       map[string]string
}

// Credential intentionally has no token fields and never retains raw SDK resources.
type Credential struct {
	ID, VaultID, OwnerUID, OwnerScope, Name string
	Metadata                                map[string]string
	Auth                                    map[string]any
}

// VaultMetadata protects ownership while preserving unrelated remote keys.
func VaultMetadata(user *map[string]string, current map[string]string, managed []string, uid, scope string) (map[string]string, error) {
	if err := validateAgentMetadata(user); err != nil {
		return nil, err
	}
	result := maps.Clone(current)
	if result == nil {
		result = make(map[string]string)
	}
	if user != nil {
		for _, key := range managed {
			delete(result, key)
		}
		maps.Copy(result, *user)
	}
	result[ownerKey], result[operatorKey], result[agentScopeKey] = uid, operatorValue, scope
	if len(result) > 16 {
		return nil, errors.New("metadata plus retained ownership keys exceeds 16 entries")
	}
	return result, nil
}

// ValidateVaultName checks the remote byte limit after trimming.
func ValidateVaultName(name string) error {
	name = strings.TrimSpace(name)
	if len(name) == 0 || len(name) > 256 || !utf8.ValidString(name) {
		return errors.New("name must contain 1 to 256 UTF-8 bytes after trimming")
	}
	return nil
}

// CredentialAuth builds validated create or rotation payloads from protected data.
func CredentialAuth(spec agentsv1alpha1.VaultCredentialAuth, secret *corev1.Secret, rotate bool) (map[string]any, error) {
	branches := 0
	for _, present := range []bool{spec.StaticBearer != nil, spec.MCPOAuth != nil, spec.EnvironmentVariable != nil} {
		if present {
			branches++
		}
	}
	if branches != 1 {
		return nil, errors.New("credential requires exactly one authentication branch")
	}
	auth := map[string]any{typeField: spec.Type}
	switch spec.Type {
	case staticBearerType:
		if spec.StaticBearer == nil {
			return nil, errors.New("static bearer branch is required")
		}
		b := spec.StaticBearer
		if err := validateCredentialURL(b.ServerURL); err != nil {
			return nil, err
		}
		token, err := credentialValue(secret, b.TokenSecretRef)
		if err != nil {
			return nil, err
		}
		auth["token"] = token
		if !rotate {
			auth["mcp_server_url"] = b.ServerURL
		}
	case oauthCredentialType:
		if spec.MCPOAuth == nil {
			return nil, errors.New("OAuth branch is required")
		}
		b := spec.MCPOAuth
		if err := validateCredentialURL(b.ServerURL); err != nil {
			return nil, err
		}
		token, err := credentialValue(secret, b.AccessTokenSecretRef)
		if err != nil {
			return nil, err
		}
		auth["access_token"] = token
		if !rotate {
			auth["mcp_server_url"] = b.ServerURL
		}
		if b.ExpirationTime != nil {
			auth["expires_at"] = b.ExpirationTime.UTC().Format(time.RFC3339)
		} else if rotate {
			auth["expires_at"] = nil
		}
		if b.Refresh != nil {
			refresh, err := credentialRefresh(b.Refresh, secret, rotate)
			if err != nil {
				return nil, err
			}
			auth["refresh"] = refresh
		}
	case environmentCredentialType:
		if spec.EnvironmentVariable == nil {
			return nil, errors.New("environment credential branch is required")
		}
		b := spec.EnvironmentVariable
		if !regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,127}$`).MatchString(b.VariableName) || reservedCredentialVariable(b.VariableName) {
			return nil, errors.New("environment credential name is invalid or reserved")
		}
		if err := validateCredentialNetworking(b.Networking); err != nil {
			return nil, err
		}
		value, err := credentialValue(secret, b.SecretValueSecretRef)
		if err != nil {
			return nil, err
		}
		auth["secret_value"] = value
		if !rotate {
			auth["secret_name"] = b.VariableName
			network := map[string]any{typeField: b.Networking.Type}
			if b.Networking.Type == limitedNetworkingType {
				network["allowed_hosts"] = append([]string(nil), b.Networking.AllowedHosts...)
			}
			auth["networking"] = network
		}
	default:
		return nil, errors.New("unsupported credential authentication type")
	}
	return auth, nil
}

func credentialRefresh(b *agentsv1alpha1.VaultOAuthRefresh, secret *corev1.Secret, rotate bool) (map[string]any, error) {
	if err := validateCredentialURL(b.TokenEndpoint); err != nil {
		return nil, err
	}
	if b.ClientID == "" || len(b.ClientID) > 512 || (b.Scope != nil && b.ClearScope) {
		return nil, errors.New("OAuth refresh configuration is invalid")
	}
	token, err := credentialValue(secret, b.RefreshTokenSecretRef)
	if err != nil {
		return nil, err
	}
	result := map[string]any{"refresh_token": token}
	if !rotate {
		result["token_endpoint"], result["client_id"] = b.TokenEndpoint, b.ClientID
		if b.Resource != nil {
			result["resource"] = *b.Resource
		}
	}
	if b.Scope != nil {
		result[scopeField] = *b.Scope
	} else if b.ClearScope {
		result[scopeField] = nil
	}
	method := b.TokenEndpointAuth
	if method.Type == _noneValue {
		if method.ClientSecretRef != nil {
			return nil, errors.New("none authentication forbids client secret")
		}
		if !rotate {
			result["token_endpoint_auth"] = map[string]any{typeField: _noneValue}
		}
		return result, nil
	}
	if (method.Type != "client_secret_basic" && method.Type != "client_secret_post") || method.ClientSecretRef == nil {
		return nil, errors.New("OAuth client authentication requires a supported method and protected client secret")
	}
	value, err := credentialValue(secret, *method.ClientSecretRef)
	if err != nil {
		return nil, err
	}
	result["token_endpoint_auth"] = map[string]any{typeField: method.Type, "client_secret": value}
	return result, nil
}

func credentialValue(secret *corev1.Secret, ref agentsv1alpha1.CredentialSecretReference) (string, error) {
	if ref.Name != VaultSecretName || secret == nil || secret.Name != ref.Name || !secret.DeletionTimestamp.IsZero() {
		return "", errors.New("same-namespace protected credential source is unavailable")
	}
	value := string(secret.Data[ref.Key])
	if len(value) == 0 || len(value) > 65536 || !utf8.ValidString(value) || strings.ContainsAny(value, "\r\n\x00") {
		return "", errors.New("protected credential value is absent or invalid")
	}
	return value, nil
}

func validateCredentialURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || len(raw) > 2048 || u.Scheme != httpsScheme || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return errors.New("credential endpoint must be HTTPS without userinfo, query or fragment")
	}
	return nil
}

func validateCredentialNetworking(n agentsv1alpha1.VaultCredentialNetworking) error {
	if n.Type == "unrestricted" && len(n.AllowedHosts) == 0 {
		return nil
	}
	if n.Type != limitedNetworkingType || len(n.AllowedHosts) < 1 || len(n.AllowedHosts) > 16 {
		return errors.New("credential networking branch is invalid")
	}
	seen := make(map[string]bool)
	for _, host := range n.AllowedHosts {
		if host != strings.ToLower(host) || len(validation.IsDNS1123Subdomain(host)) != 0 || seen[host] {
			return errors.New("credential hosts must be unique exact lowercase hostnames or IPv4 addresses")
		}
		seen[host] = true
	}
	return nil
}

func reservedCredentialVariable(name string) bool {
	if strings.HasPrefix(name, "CODEX_") {
		return true
	}
	switch strings.ToUpper(name) {
	case "OPENAI_API_KEY", "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY", "SSL_CERT_FILE", "SSL_CERT_DIR", "REQUESTS_CA_BUNDLE", "CURL_CA_BUNDLE", "NODE_EXTRA_CA_CERTS":
		return true
	}
	return false
}

// CredentialPublicMatches compares immutable identity, excluding secrets and refresh expiry.
func CredentialPublicMatches(spec agentsv1alpha1.VaultCredentialAuth, remote Credential) bool {
	if remote.Auth[typeField] != spec.Type {
		return false
	}
	switch spec.Type {
	case staticBearerType:
		return spec.StaticBearer != nil && remote.Auth["mcp_server_url"] == spec.StaticBearer.ServerURL
	case oauthCredentialType:
		if spec.MCPOAuth == nil || remote.Auth["mcp_server_url"] != spec.MCPOAuth.ServerURL {
			return false
		}
		refresh, ok := remote.Auth["refresh"].(map[string]any)
		if spec.MCPOAuth.Refresh == nil {
			return !ok || len(refresh) == 0
		}
		b := spec.MCPOAuth.Refresh
		method, _ := refresh["token_endpoint_auth"].(map[string]any)
		return refresh["token_endpoint"] == b.TokenEndpoint && refresh["client_id"] == b.ClientID && method[typeField] == b.TokenEndpointAuth.Type && (b.Resource == nil || refresh["resource"] == *b.Resource)
	case environmentCredentialType:
		if spec.EnvironmentVariable == nil {
			return false
		}
		b := spec.EnvironmentVariable
		network, _ := remote.Auth["networking"].(map[string]any)
		if network[typeField] != b.Networking.Type || remote.Auth["secret_name"] != b.VariableName {
			return false
		}
		if b.Networking.Type == "unrestricted" {
			return true
		}
		raw, _ := json.Marshal(network["allowed_hosts"])
		var hosts []string
		if json.Unmarshal(raw, &hosts) != nil {
			return false
		}
		return slices.Equal(sortedStrings(hosts), sortedStrings(b.Networking.AllowedHosts))
	}
	return false
}

// CredentialDigest hashes public configuration and source versions, never protected values.
func CredentialDigest(spec agentsv1alpha1.VaultCredentialSpec, sources []agentsv1alpha1.CredentialSourceVersion) string {
	spec = *spec.DeepCopy()
	spec.DeletionPolicy = ""
	if env := spec.Auth.EnvironmentVariable; env != nil {
		slices.Sort(env.Networking.AllowedHosts)
	}
	sources = slices.Clone(sources)
	slices.SortFunc(sources, func(a, b agentsv1alpha1.CredentialSourceVersion) int { return strings.Compare(a.Name, b.Name) })
	raw, _ := json.Marshal(struct {
		Spec    agentsv1alpha1.VaultCredentialSpec       `json:"Spec"`
		Sources []agentsv1alpha1.CredentialSourceVersion `json:"Sources"`
	}{Spec: spec, Sources: sources})
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func (c *Client) CreateVault(ctx context.Context, uid, scope string, spec agentsv1alpha1.VaultSpec) (Vault, error) {
	if spec.Name != nil && spec.ClearName {
		return Vault{}, errors.New("Vault name and clearName conflict")
	}
	metadata, err := VaultMetadata(spec.Metadata, nil, nil, uid, scope)
	if err != nil {
		return Vault{}, err
	}
	body := openai.BetaAgentVaultNewParams{Metadata: metadata}
	if spec.Name != nil {
		if err := ValidateVaultName(*spec.Name); err != nil {
			return Vault{}, err
		}
		body.Name = openai.String(strings.TrimSpace(*spec.Name))
	}
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	remote, err := c.SDK.Beta.Agents.Vaults.New(ctx, body, option.WithMaxRetries(0))
	if err != nil {
		return Vault{}, safeError("create Vault", err)
	}
	return summarizeVault(remote), nil
}

// UpdateVault uses the documented POST /vaults/{id} contract. The pinned SDK
// predates its typed method; Client.Post retains the configured base URL and auth.
// Only changed fields are supplied. A nil name is an explicit clear request only
// when clearName is true; nil metadata leaves remote metadata unchanged.
func (c *Client) UpdateVault(ctx context.Context, id string, name *string, clearName bool, metadata *map[string]string) (Vault, error) {
	if id == "" || name != nil && clearName {
		return Vault{}, errors.New("invalid Vault update")
	}
	body := make(map[string]any)
	if name != nil {
		if err := ValidateVaultName(*name); err != nil {
			return Vault{}, err
		}
		body[nameField] = strings.TrimSpace(*name)
	} else if clearName {
		body[nameField] = nil
	}
	if metadata != nil {
		if len(*metadata) > 16 || (*metadata)[ownerKey] == "" || (*metadata)[operatorKey] != operatorValue || (*metadata)[agentScopeKey] == "" {
			return Vault{}, errors.New("Vault metadata update lacks protected ownership")
		}
		body[metadataField] = *metadata
	}
	if len(body) == 0 {
		return Vault{}, errors.New("empty Vault update")
	}
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	var remote openai.Vault
	err := c.SDK.Post(ctx, "vaults/"+url.PathEscape(id), body, &remote,
		option.WithHeader("OpenAI-Beta", agentsBetaHeader), option.WithMaxRetries(0))
	if err != nil {
		return Vault{}, safeError("update Vault", err)
	}
	if remote.ID != id {
		return Vault{}, errors.New("Vault update returned unexpected identity")
	}
	return summarizeVault(&remote), nil
}
func (c *Client) GetVault(ctx context.Context, id string) (Vault, error) {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	remote, err := c.SDK.Beta.Agents.Vaults.Get(ctx, id, option.WithMaxRetries(0))
	if err != nil {
		return Vault{}, safeError("retrieve Vault", err)
	}
	return summarizeVault(remote), nil
}
func (c *Client) FindVault(ctx context.Context, uid, scope string) (Vault, error) {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	page, err := c.SDK.Beta.Agents.Vaults.List(ctx, openai.BetaAgentVaultListParams{Limit: openai.Int(100)}, option.WithMaxRetries(0))
	var found Vault
	for n := 0; page != nil && n < maxDiscoveryPages; n++ {
		if err != nil {
			return Vault{}, safeError("discover Vault", err)
		}
		for i := range page.Data {
			v := summarizeVault(&page.Data[i])
			if v.OwnerUID != uid || v.OwnerScope != scope {
				continue
			}
			if found.ID != "" {
				return Vault{}, errors.New("multiple owned Vaults; audit required")
			}
			found = v
		}
		if !page.JSON.HasMore.Valid() {
			return Vault{}, errors.New("Vault pagination is incomplete")
		}
		if !page.HasMore {
			return found, nil
		}
		if len(page.Data) == 0 || n+1 == maxDiscoveryPages {
			return Vault{}, errors.New("Vault discovery limit reached")
		}
		page, err = page.GetNextPage()
	}
	return Vault{}, safeError("discover Vault", err)
}
func (c *Client) DeleteVault(ctx context.Context, id string) error {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	result, err := c.SDK.Beta.Agents.Vaults.Delete(ctx, id, option.WithMaxRetries(0))
	if err != nil {
		if clean := safeError("delete Vault", err); !errors.Is(clean, ErrNotFound) {
			return clean
		}
		return nil
	}
	if result == nil || result.ID != id || !result.Deleted {
		return errors.New("Vault deletion was not confirmed")
	}
	return nil
}
func summarizeVault(v *openai.Vault) Vault {
	if v == nil {
		return Vault{}
	}
	uid, scope := "", ""
	if v.Metadata[operatorKey] == operatorValue {
		uid, scope = v.Metadata[ownerKey], v.Metadata[agentScopeKey]
	}
	return Vault{ID: v.ID, OwnerUID: uid, OwnerScope: scope, Name: v.Name, Metadata: maps.Clone(v.Metadata)}
}
func (c *Client) CreateCredential(ctx context.Context, vaultID, name string, metadata map[string]string, auth map[string]any) (Credential, error) {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	result, err := c.SDK.Beta.Agents.Vaults.Credentials.New(ctx, vaultID, openai.BetaAgentVaultCredentialNewParams{Name: strings.TrimSpace(name), Metadata: metadata}, option.WithMaxRetries(0), option.WithJSONSet("auth", auth))
	if err != nil {
		return Credential{}, safeError("create credential", err)
	}
	return summarizeCredential(result), nil
}
func (c *Client) UpdateCredential(ctx context.Context, vaultID, id string, fields map[string]any) (Credential, error) {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	opts := make([]option.RequestOption, 0, 1+len(fields))
	opts = append(opts, option.WithMaxRetries(0))
	for key, value := range fields {
		opts = append(opts, option.WithJSONSet(key, value))
	}
	result, err := c.SDK.Beta.Agents.Vaults.Credentials.Update(ctx, vaultID, id, openai.BetaAgentVaultCredentialUpdateParams{}, opts...)
	if err != nil {
		return Credential{}, safeError("rotate credential", err)
	}
	return summarizeCredential(result), nil
}
func (c *Client) GetCredential(ctx context.Context, vaultID, id string) (Credential, error) {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	result, err := c.SDK.Beta.Agents.Vaults.Credentials.Get(ctx, vaultID, id, option.WithMaxRetries(0))
	if err != nil {
		return Credential{}, safeError("retrieve credential", err)
	}
	return summarizeCredential(result), nil
}
func (c *Client) FindCredential(ctx context.Context, vaultID, uid, scope string) (Credential, error) {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	page, err := c.SDK.Beta.Agents.Vaults.Credentials.List(ctx, vaultID, openai.BetaAgentVaultCredentialListParams{Limit: openai.Int(100)}, option.WithMaxRetries(0))
	var found Credential
	for n := 0; page != nil && n < maxDiscoveryPages; n++ {
		if err != nil {
			return Credential{}, safeError("discover credential", err)
		}
		for i := range page.Data {
			v := summarizeCredential(&page.Data[i])
			if v.OwnerUID != uid || v.OwnerScope != scope || v.VaultID != vaultID {
				continue
			}
			if found.ID != "" {
				return Credential{}, errors.New("multiple owned credentials; audit required")
			}
			found = v
		}
		if !page.JSON.HasMore.Valid() {
			return Credential{}, errors.New("credential pagination is incomplete")
		}
		if !page.HasMore {
			return found, nil
		}
		if len(page.Data) == 0 || n+1 == maxDiscoveryPages {
			return Credential{}, errors.New("credential discovery limit reached")
		}
		page, err = page.GetNextPage()
	}
	return Credential{}, safeError("discover credential", err)
}
func (c *Client) DeleteCredential(ctx context.Context, vaultID, id string) error {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	result, err := c.SDK.Beta.Agents.Vaults.Credentials.Delete(ctx, vaultID, id, option.WithMaxRetries(0))
	if err != nil {
		if clean := safeError("delete credential", err); !errors.Is(clean, ErrNotFound) {
			return clean
		}
		return nil
	}
	if result == nil || result.ID != id || !result.Deleted {
		return errors.New("credential deletion was not confirmed")
	}
	return nil
}
func summarizeCredential(v *openai.Credential) Credential {
	if v == nil {
		return Credential{}
	}
	uid, scope := "", ""
	if v.Metadata[operatorKey] == operatorValue {
		uid, scope = v.Metadata[ownerKey], v.Metadata[agentScopeKey]
	}
	// Copy only explicitly public fields. Unexpected response token fields are discarded.
	var auth map[string]any
	_ = json.Unmarshal([]byte(v.Auth.RawJSON()), &auth)
	delete(auth, "token")
	delete(auth, "access_token")
	delete(auth, "secret_value")
	if refresh, ok := auth["refresh"].(map[string]any); ok {
		delete(refresh, "refresh_token")
		if method, ok := refresh["token_endpoint_auth"].(map[string]any); ok {
			delete(method, "client_secret")
		}
	}
	return Credential{ID: v.ID, VaultID: v.VaultID, OwnerUID: uid, OwnerScope: scope, Name: v.Name, Metadata: maps.Clone(v.Metadata), Auth: auth}
}

// ListCredentials completes a bounded scan before returning attachment metadata.
func (c *Client) ListCredentials(ctx context.Context, vaultID string) ([]Credential, error) {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	page, err := c.SDK.Beta.Agents.Vaults.Credentials.List(ctx, vaultID, openai.BetaAgentVaultCredentialListParams{Limit: openai.Int(100)}, option.WithMaxRetries(0))
	var result []Credential
	for n := 0; page != nil && n < maxDiscoveryPages; n++ {
		if err != nil {
			return nil, safeError("list credentials", err)
		}
		for i := range page.Data {
			result = append(result, summarizeCredential(&page.Data[i]))
		}
		if !page.JSON.HasMore.Valid() {
			return nil, errors.New("credential list pagination is incomplete")
		}
		if !page.HasMore {
			return result, nil
		}
		if len(page.Data) == 0 || n+1 == maxDiscoveryPages {
			return nil, errors.New("credential list limit reached")
		}
		page, err = page.GetNextPage()
	}
	return nil, safeError("list credentials", err)
}
