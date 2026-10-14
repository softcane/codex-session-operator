package openaiapi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"path"
	"reflect"
	"strings"
	"unicode/utf8"

	openai "github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/option"
	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
)

const (
	updateOperation             = "update"
	serverURLField              = "server_url"
	httpTransportType           = "http"
	transportField              = "transport"
	stdioTransportType          = "stdio"
	credentialIDField           = "credential_id"
	allowedToolsField           = "allowed_tools"
	typeField                   = "type"
	functionToolType            = "function"
	mcpToolType                 = "mcp"
	computerUseToolType         = "computer_use"
	textField                   = "text"
	multiAgentField             = "multi_agent"
	agentField                  = "agent"
	agentsBetaHeader            = "agents=v1"
	dataField                   = "data"
	deleteOperation             = "delete"
	modelField                  = "model"
	metadataField               = "metadata"
	toolsField                  = "tools"
	reasoningField              = "reasoning"
	nameField                   = "name"
	serviceTierField            = "service_tier"
	hasMoreField                = "has_more"
	toolSearchType              = "tool_search"
	webSearchType               = "web_search"
	programmaticToolCallingType = "programmatic_tool_calling"
	enabledField                = "enabled"
	agentScopeKey               = "owner_scope"
)

// Agent contains credential-free provider observations. Config is never stored in CR status.
type Agent struct {
	ID, OwnerUID, OwnerScope, Model string
	Config                          map[string]any
}

// AgentRequestError preserves only the HTTP classification, never SDK headers or bodies.
type AgentRequestError struct {
	Operation  string
	StatusCode int
}

var _ error = (*AgentRequestError)(nil)

func (e *AgentRequestError) Error() string {
	return fmt.Sprintf("OpenAI Agent %s failed (HTTP %d)", e.Operation, e.StatusCode)
}

// AgentConfiguration translates only typed, validated fields into the pinned wire contract.
// Omission is preserved, while pointers retain explicit false, empty maps and lists.
func AgentConfiguration(spec agentsv1alpha1.AgentSpec) (map[string]any, error) {
	if spec.Model == "" {
		return nil, errors.New("Agent model is required")
	}
	if spec.Name != nil && utf8.RuneCountInString(*spec.Name) > 128 {
		return nil, errors.New("Agent name exceeds the OpenAI limit")
	}
	data, err := json.Marshal(spec)
	if err != nil {
		return nil, errors.New("Agent configuration could not be serialized")
	}
	var fields map[string]any
	if err := json.Unmarshal(data, &fields); err != nil {
		return nil, errors.New("Agent configuration is not an object")
	}
	delete(fields, "deletionPolicy")
	delete(fields, "clearName")
	delete(fields, "resetReasoning")
	if spec.ClearName {
		if spec.Name != nil {
			return nil, errors.New("Agent name and clearName conflict")
		}
		fields[nameField] = nil
	}
	if spec.ResetReasoning {
		if spec.Reasoning != nil {
			return nil, errors.New("Agent reasoning and resetReasoning conflict")
		}
		fields[reasoningField] = nil
	}
	if spec.Tools != nil {
		tools, err := agentToolsConfiguration(*spec.Tools)
		if err != nil {
			return nil, err
		}
		fields[toolsField] = tools
	}
	if err := validateAgentMetadata(spec.Metadata); err != nil {
		return nil, err
	}
	if spec.Text != nil && spec.Text.Format != nil {
		f := spec.Text.Format
		if (f.Type == "json_schema") != (f.Schema != nil) || (f.Type != textField && f.Type != "json_schema") {
			return nil, errors.New("Agent text format branch does not match its type")
		}
	}
	return agentWireNames(fields), nil
}

func agentToolsConfiguration(configured []agentsv1alpha1.AgentTool) ([]any, error) {
	tools := make([]any, 0, len(configured))
	names := make(map[string]bool)
	deferred, search := false, false
	for _, tool := range configured {
		branch, err := agentToolBranch(tool)
		if err != nil {
			return nil, err
		}
		if tool.Function != nil {
			if names[tool.Function.Name] {
				return nil, errors.New("Agent function names must be unique")
			}
			names[tool.Function.Name] = true
			if tool.Function.DeferLoading != nil && *tool.Function.DeferLoading {
				deferred = true
			}
		}
		if tool.Type == toolSearchType {
			search = true
		}
		wire := make(map[string]any)
		if branch != nil {
			raw, err := json.Marshal(branch)
			if err != nil {
				return nil, errors.New("Agent tool could not be serialized")
			}
			if err := json.Unmarshal(raw, &wire); err != nil {
				return nil, errors.New("Agent tool is not an object")
			}
		}
		wire[typeField] = tool.Type
		if tool.MCP != nil {
			transport, err := agentMCPTransportConfiguration(tool.MCP.Transport)
			if err != nil {
				return nil, err
			}
			wire[transportField] = transport
		}
		tools = append(tools, wire)
	}
	if deferred != search {
		return nil, errors.New("Agent deferred functions and tool_search must be configured together")
	}
	return tools, nil
}

func agentToolBranch(tool agentsv1alpha1.AgentTool) (any, error) {
	branchCount := 0
	for _, present := range []bool{tool.Function != nil, tool.MCP != nil, tool.WebSearch != nil, tool.ComputerUse != nil, tool.ProgrammaticToolCalling != nil} {
		if present {
			branchCount++
		}
	}
	if tool.Type == toolSearchType && branchCount == 0 {
		return nil, nil
	}
	if branchCount != 1 {
		return nil, errors.New("Agent tool branch does not match its type")
	}
	switch tool.Type {
	case functionToolType:
		if tool.Function != nil {
			var parameters map[string]any
			if err := json.Unmarshal(tool.Function.Parameters.Raw, &parameters); err != nil || parameters == nil {
				return nil, errors.New("Agent function parameters must be a JSON Schema object")
			}
			return tool.Function, nil
		}
	case mcpToolType:
		if tool.MCP != nil {
			return tool.MCP, validateAgentMCP(tool.MCP)
		}
	case webSearchType:
		if tool.WebSearch != nil {
			return tool.WebSearch, nil
		}
	case computerUseToolType:
		if tool.ComputerUse != nil {
			return tool.ComputerUse, nil
		}
	case programmaticToolCallingType:
		if tool.ProgrammaticToolCalling != nil {
			return tool.ProgrammaticToolCalling, nil
		}
	}
	return nil, errors.New("Agent tool branch does not match its type")
}

func validateAgentMCP(mcp *agentsv1alpha1.AgentMCP) error {
	t := mcp.Transport
	if t.Type == stdioTransportType && mcp.ConnectionOrigin != nil {
		return errors.New("stdio MCP must omit connectionOrigin")
	}
	if mcp.CredentialID != nil && (t.Type != httpTransportType || (mcp.ConnectionOrigin != nil && *mcp.ConnectionOrigin != "service")) {
		return errors.New("vault credentials require service-origin HTTP MCP")
	}
	switch t.Type {
	case httpTransportType:
		if t.HTTP == nil || t.Stdio != nil {
			return errors.New("Agent MCP transport branch does not match its type")
		}
		if err := validateAgentMCPHTTP(t.HTTP); err != nil {
			return err
		}
	case stdioTransportType:
		if t.Stdio == nil || t.HTTP != nil {
			return errors.New("Agent MCP transport branch does not match its type")
		}
	default:
		return errors.New("Agent MCP transport branch does not match its type")
	}
	if mcp.RequestMetadata != nil {
		for k := range *mcp.RequestMetadata {
			if k != "traceparent" && k != "tracestate" {
				return errors.New("Agent MCP request metadata permits only tracing fields")
			}
		}
	}
	return nil
}

func validateAgentMCPHTTP(transport *agentsv1alpha1.AgentMCPHTTP) error {
	u, err := url.Parse(transport.ServerURL)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return errors.New("Agent MCP URL must be HTTPS without credentials or query parameters")
	}
	if transport.Headers != nil {
		for k, v := range *transport.Headers {
			if (k != "Accept" && k != "Content-Type") || (v != "application/json" && v != "text/event-stream") {
				return errors.New("Agent MCP headers permit only credential-free protocol values")
			}
		}
	}
	return nil
}

func agentMCPTransportConfiguration(transport agentsv1alpha1.AgentMCPTransport) (map[string]any, error) {
	var branch any = transport.HTTP
	if transport.Stdio != nil {
		branch = transport.Stdio
	}
	raw, err := json.Marshal(branch)
	if err != nil {
		return nil, errors.New("Agent MCP transport could not be serialized")
	}
	var fields map[string]any
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, errors.New("Agent MCP transport is not an object")
	}
	fields[typeField] = transport.Type
	return fields, nil
}

func validateAgentMetadata(metadata *map[string]string) error {
	if metadata == nil {
		return nil
	}
	if len(*metadata) > 13 {
		return errors.New("Agent metadata exceeds the ownership budget")
	}
	for k, v := range *metadata {
		if k == ownerKey || k == operatorKey || k == agentScopeKey || utf8.RuneCountInString(k) > 64 || utf8.RuneCountInString(v) > 512 {
			return errors.New("Agent metadata exceeds bounds or uses reserved ownership keys")
		}
	}
	return nil
}

// SDK wire names are mapped explicitly. Opaque schema/map keys must never be renamed.
func agentWireNames(fields map[string]any) map[string]any {
	names := map[string]string{
		"serviceTier": serviceTierField, "multiAgent": multiAgentField, "maxConcurrentSubagents": "max_concurrent_subagents",
		"deferLoading": "defer_loading", "serverLabel": "server_label", "credentialID": credentialIDField,
		"allowedTools": allowedToolsField, "connectionOrigin": "connection_origin", "requestMetadata": "request_metadata",
		"serverURL": serverURLField, "envVars": "env_vars", "allowedDomains": allowedDomainsField, "contextSize": "context_size",
		"includeScreenshots": "include_screenshots",
	}
	result := make(map[string]any, len(fields))
	for k, v := range fields {
		opaque := k == "schema" || k == "parameters" || k == metadataField || k == "requestMetadata" || k == "headers"
		if !opaque {
			if obj, ok := v.(map[string]any); ok {
				v = agentWireNames(obj)
			}
			if items, ok := v.([]any); ok {
				for i, item := range items {
					if obj, ok := item.(map[string]any); ok {
						items[i] = agentWireNames(obj)
					}
				}
			}
		}
		key := k
		if name, ok := names[k]; ok {
			key = name
		}
		result[key] = v
	}
	return result
}

// AgentChanges computes intentional field replacements against a fresh observation.
// Unmanaged metadata survives; reserved identity is checked by the controller first.
func AgentChanges(desired map[string]any, remote Agent, managedMetadataKeys []string) (map[string]any, error) {
	changes := make(map[string]any)
	for k, v := range desired {
		current := remote.Config[k]
		if k == metadataField {
			merged := make(map[string]any)
			if existing, ok := current.(map[string]any); ok {
				maps.Copy(merged, existing)
			}
			if user, ok := v.(map[string]any); ok {
				for _, owned := range managedMetadataKeys {
					if _, present := user[owned]; !present {
						delete(merged, owned)
					}
				}
				maps.Copy(merged, user)
			}
			if len(merged) > 16 {
				return nil, errors.New("Agent metadata plus retained remote keys exceeds 16 entries")
			}
			v = merged
		}
		if !agentFieldEqual(k, v, current) {
			changes[k] = v
		}
	}
	return changes, nil
}

func agentFieldEqual(key string, desired, current any) bool {
	if key == nameField && desired == nil {
		return current == nil || current == ""
	}
	if key == reasoningField {
		if desired == nil {
			return false
		} // Reset needs an acknowledgment ledger (controller generation).
		d, ok := desired.(map[string]any)
		c, other := current.(map[string]any)
		if !ok || !other {
			return false
		}
		for k, v := range d {
			if !reflect.DeepEqual(v, c[k]) {
				return false
			}
		}
		if _, specified := d["summary"]; !specified {
			return c["summary"] == nil || c["summary"] == ""
		}
		return true
	}
	return reflect.DeepEqual(agentDefaults(key, desired), agentDefaults(key, current))
}

// Defaults remove readback-only representation differences while retaining replacement semantics.
func agentDefaults(key string, value any) any {
	raw, _ := json.Marshal(value)
	var normalized any
	_ = json.Unmarshal(raw, &normalized)
	if key == toolsField {
		items, ok := normalized.([]any)
		if !ok {
			return []any{}
		}
		for _, item := range items {
			if obj, ok := item.(map[string]any); ok {
				agentToolDefaults(obj)
			}
		}
	}
	if obj, ok := normalized.(map[string]any); ok {
		switch key {
		case textField:
			defaultField(obj, "verbosity", "medium")
			defaultField(obj, "format", map[string]any{typeField: textField})
			if format, ok := obj["format"].(map[string]any); ok && format[typeField] == textField && format["schema"] == nil {
				delete(format, "schema")
			}
		case multiAgentField:
			defaultField(obj, "max_concurrent_subagents", float64(6))
		}
	}
	return normalized
}

func agentToolDefaults(obj map[string]any) {
	switch obj[typeField] {
	case functionToolType:
		defaultField(obj, "defer_loading", false)
	case programmaticToolCallingType:
		defaultField(obj, enabledField, true)
	case computerUseToolType:
		defaultField(obj, "include_screenshots", false)
	case mcpToolType:
		agentMCPDefaults(obj)
	case webSearchType:
		defaultField(obj, "context_size", "medium")
		defaultField(obj, "mode", "live")
		if location, ok := obj["location"].(map[string]any); ok {
			for k, v := range location {
				if v == nil || v == "" {
					delete(location, k)
				}
			}
			if len(location) == 0 {
				delete(obj, "location")
			}
		}
		for _, k := range []string{allowedDomainsField, "location"} {
			if obj[k] == nil {
				delete(obj, k)
			}
		}
	}
}

func agentMCPDefaults(obj map[string]any) {
	defaultField(obj, "required", false)
	transport, ok := obj[transportField].(map[string]any)
	if ok && transport[typeField] == stdioTransportType {
		delete(obj, "connection_origin")
	} else {
		defaultField(obj, "connection_origin", "service")
	}
	for _, k := range []string{credentialIDField, allowedToolsField, "request_metadata"} {
		if obj[k] == nil || (k == credentialIDField && obj[k] == "") {
			delete(obj, k)
		}
	}
	if emptyAgentContainer(obj["request_metadata"]) {
		delete(obj, "request_metadata")
	}
	if !ok {
		return
	}
	for _, k := range []string{"headers", "args", "env_vars"} {
		if transport[k] == nil || emptyAgentContainer(transport[k]) {
			delete(transport, k)
		}
	}
}

func emptyAgentContainer(value any) bool {
	switch container := value.(type) {
	case map[string]any:
		return len(container) == 0
	case []any:
		return len(container) == 0
	}
	return false
}

func defaultField(obj map[string]any, key string, value any) {
	if obj[key] == nil {
		obj[key] = value
	}
}

func agentOptions(fields map[string]any) []option.RequestOption {
	opts := make([]option.RequestOption, 0, 1+len(fields))
	opts = append(opts, option.WithMaxRetries(0))
	for key, value := range fields {
		opts = append(opts, option.WithJSONSet(key, value))
	}
	return opts
}

// CreateAgent sends one POST. The caller owns the durable creation claim.
func (c *Client) CreateAgent(ctx context.Context, uid, scope string, fields map[string]any) (Agent, error) {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	body := maps.Clone(fields)
	// Create uses defaults; null reset controls are needed only for updates.
	for _, key := range []string{nameField, reasoningField} {
		if body[key] == nil {
			delete(body, key)
		}
	}
	metadata := make(map[string]any)
	if user, ok := fields[metadataField].(map[string]any); ok {
		maps.Copy(metadata, user)
	}
	if len(metadata) > 13 || scope == "" {
		return Agent{}, errors.New("Agent creation requires owner scope and metadata budget")
	}
	metadata[ownerKey], metadata[operatorKey], metadata[agentScopeKey] = uid, operatorValue, scope
	body[metadataField] = metadata
	remote, err := c.SDK.Beta.Agents.New(ctx, openai.BetaAgentNewParams{}, agentOptions(body)...)
	if err != nil {
		return Agent{}, safeAgentError(createOperation, err)
	}
	return summarizeAgent(remote)
}

// GetAgent retrieves current owned configuration; absence never permits replacement.
func (c *Client) GetAgent(ctx context.Context, id string) (Agent, error) {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	remote, err := c.SDK.Beta.Agents.Get(ctx, id, option.WithMaxRetries(0))
	if err != nil {
		return Agent{}, safeAgentError("retrieve", err)
	}
	return summarizeAgent(remote)
}

// UpdateAgent sends only changed fields, including explicit null/empty replacements.
func (c *Client) UpdateAgent(ctx context.Context, id string, fields map[string]any) (Agent, error) {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	remote, err := c.SDK.Beta.Agents.Update(ctx, id, openai.BetaAgentUpdateParams{}, agentOptions(fields)...)
	if err != nil {
		return Agent{}, safeAgentError(updateOperation, err)
	}
	return summarizeAgent(remote)
}

// FindAgent requires a completed, bounded scan and a unique scope/UID match.
func (c *Client) FindAgent(ctx context.Context, uid, scope string) (Agent, error) {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	page, err := c.SDK.Beta.Agents.List(ctx, openai.BetaAgentListParams{Limit: openai.Int(100)}, option.WithMaxRetries(0))
	var found Agent
	for n := 0; page != nil && n < maxDiscoveryPages; n++ {
		if err != nil {
			return Agent{}, safeAgentError("discover", err)
		}
		for i := range page.Data {
			remote := &page.Data[i]
			if remote.Metadata[ownerKey] != uid || remote.Metadata[agentScopeKey] != scope || remote.Metadata[operatorKey] != operatorValue {
				continue
			}
			if found.ID != "" {
				return Agent{}, errors.New("multiple owned OpenAI Agents found; audit required")
			}
			found, err = summarizeAgent(remote)
			if err != nil {
				return Agent{}, err
			}
		}
		if !page.JSON.HasMore.Valid() {
			return Agent{}, errors.New("OpenAI Agent discovery pagination is incomplete")
		}
		if !page.HasMore {
			return found, nil
		}
		if len(page.Data) == 0 || n+1 == maxDiscoveryPages {
			return Agent{}, errors.New("OpenAI Agent discovery limit reached; audit required")
		}
		page, err = page.GetNextPage()
	}
	if err != nil {
		return Agent{}, safeAgentError("discover", err)
	}
	return Agent{}, errors.New("OpenAI Agent discovery returned no page")
}

// DeleteAgent checks confirmation; the controller observes absence separately.
func (c *Client) DeleteAgent(ctx context.Context, id string) error {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	deleted, err := c.SDK.Beta.Agents.Delete(ctx, id, option.WithMaxRetries(0))
	if err != nil {
		clean := safeAgentError(deleteOperation, err)
		if errors.Is(clean, ErrNotFound) {
			return nil
		}
		return clean
	}
	if deleted == nil || !deleted.Deleted || deleted.ID != id {
		return errors.New("OpenAI Agent deletion was not confirmed")
	}
	return nil
}

func summarizeAgent(remote *openai.Agent) (Agent, error) {
	if remote == nil || remote.ID == "" {
		return Agent{}, errors.New("OpenAI Agent returned no identity")
	}
	var fields map[string]any
	if err := json.Unmarshal([]byte(remote.RawJSON()), &fields); err != nil {
		return Agent{}, errors.New("OpenAI Agent returned invalid configuration")
	}
	owner, scope := "", ""
	if remote.Metadata[operatorKey] == operatorValue {
		owner, scope = remote.Metadata[ownerKey], remote.Metadata[agentScopeKey]
	}
	return Agent{ID: remote.ID, OwnerUID: owner, OwnerScope: scope, Model: remote.Model, Config: fields}, nil
}

func safeAgentError(operation string, err error) error {
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	if apiErr, ok := errors.AsType[*openai.Error](err); ok {
		if apiErr.StatusCode == http.StatusNotFound && operation != createOperation {
			return ErrNotFound
		}
		return &AgentRequestError{Operation: operation, StatusCode: apiErr.StatusCode}
	}
	return fmt.Errorf("OpenAI Agent %s failed (transport or response error)", operation)
}

// AgentSessionBlock identifies capabilities not yet supplied by this phase.
func AgentSessionBlock(fields map[string]any) (string, string) {
	tools, ok := fields[toolsField].([]any)
	if !ok && fields[toolsField] != nil {
		return invalidConfigurationReason, invalidAgentToolsMessage
	}
	for _, item := range tools {
		tool, ok := item.(map[string]any)
		if !ok {
			return invalidConfigurationReason, invalidAgentToolsMessage
		}
		switch tool[typeField] {
		case functionToolType:
			return "GatewayRequired", "Function tools require gateway result handling"
		case computerUseToolType:
			return "HostedDesktopRequired", "Computer use requires a hosted desktop and gateway approvals"
		case toolSearchType, programmaticToolCallingType, webSearchType:
		case mcpToolType:
			if tool[credentialIDField] != nil && tool[credentialIDField] != "" {
				return "VaultRequired", "MCP credentials require Session Vault attachment support"
			}
			if transport, ok := tool[transportField].(map[string]any); ok && transport[typeField] == stdioTransportType {
				return environmentCapabilitiesReason, "Stdio MCP requires Environment capability delivery"
			}

		default:
			return "UnsupportedTool", "Saved Agent contains a tool outside the pinned configuration contract"
		}
	}
	return "", ""
}

// AgentConfigurationDigest records only effective, credential-free configuration.
func AgentConfigurationDigest(fields map[string]any) string {
	configuration := make(map[string]any)
	for _, key := range []string{modelField, nameField, "instructions", reasoningField, serviceTierField, textField, multiAgentField, toolsField} {
		if value, ok := fields[key]; ok {
			configuration[key] = agentDefaults(key, value)
		}
	}
	// Session readback redacts HTTP headers, including non-secret protocol headers.
	// They are managed on saved Agents but cannot participate in the Session comparison.
	if tools, ok := configuration[toolsField].([]any); ok {
		for _, item := range tools {
			if tool, ok := item.(map[string]any); ok {
				if transport, ok := tool[transportField].(map[string]any); ok && transport[typeField] == httpTransportType {
					delete(transport, "headers")
				}
			}
		}
	}
	data, _ := json.Marshal(configuration)
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

// AgentReasoningDigest acknowledges an intentional whole-object replacement or null reset.
func AgentReasoningDigest(fields map[string]any) string {
	value, present := fields[reasoningField]
	if !present {
		return ""
	}
	// A default reset belongs to the selected model; a model change must reset again.
	if value == nil {
		value = map[string]any{modelField: fields[modelField], reasoningField: nil}
	}
	data, _ := json.Marshal(value)
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

// AgentEnvironmentBlock allows local MCP only when a pinned capability owns its path.
func AgentEnvironmentBlock(fields map[string]any, profile *agentsv1alpha1.EnvironmentSpec) (string, string) {
	tools, ok := fields[toolsField].([]any)
	if !ok && fields[toolsField] != nil {
		return invalidConfigurationReason, invalidAgentToolsMessage
	}
	for _, item := range tools {
		tool, ok := item.(map[string]any)
		if !ok {
			return invalidConfigurationReason, invalidAgentToolsMessage
		}
		if tool[typeField] != mcpToolType {
			if reason, message := AgentSessionBlock(map[string]any{toolsField: []any{tool}}); reason != "" {
				return reason, message
			}
			continue
		}
		transport, ok := tool[transportField].(map[string]any)
		if !ok {
			return invalidConfigurationReason, "MCP transport is invalid"
		}
		if transport[typeField] == stdioTransportType {
			if profile == nil || profile.SelfHosted == nil {
				return environmentCapabilitiesReason, "Direct stdio MCP requires a pinned self-hosted capability path; hosted MCP uses pinned plugins"
			}
			cwd, _ := transport["cwd"].(string)
			command, _ := transport["command"].(string)
			matched := false
			for _, capability := range profile.SelfHosted.Capabilities {
				if (cwd == capability.Path || strings.HasPrefix(cwd, capability.Path+"/")) && strings.HasPrefix(command, capability.Path+"/") && path.Clean(command) == command && path.Clean(cwd) == cwd {
					matched = true
				}
			}
			if !matched {
				return environmentCapabilitiesReason, "Stdio MCP executable and cwd must belong to a pinned capability"
			}
		} else if tool["connection_origin"] == "environment" && profile != nil && profile.OpenAIHosted != nil && profile.OpenAIHosted.Network.Access == "disabled" {
			return "EnvironmentNetworkRequired", "Environment-origin MCP requires permitted hosted network access"
		}
	}
	return "", ""
}
