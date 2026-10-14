package openaiapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
)

const (
	testMCPURL = "https://example.com"
)

func TestAgentNameMatchesRemoteLimit(t *testing.T) {
	valid := strings.Repeat("n", 128)
	if _, err := AgentConfiguration(agentsv1alpha1.AgentSpec{Model: SessionModel, Name: &valid}); err != nil {
		t.Fatal(err)
	}
	tooLong := valid + "n"
	if _, err := AgentConfiguration(agentsv1alpha1.AgentSpec{Model: SessionModel, Name: &tooLong}); err == nil {
		t.Fatal("Agent name beyond remote limit accepted")
	}
}

const (
	testWorkspace             = "/workspace"
	objectField               = "object"
	effortField               = "effort"
	verbosityField            = "verbosity"
	lowSetting                = "low"
	missingPaginationScenario = "missing-pagination"
	testOwnedMetadataKey      = "owned"
	duplicateScenario         = "duplicate"
	limitScenario             = "limit"
	unownedScenario           = "unowned"
	deleteUnconfirmedScenario = "delete-unconfirmed"
	agentTestScope            = "installation/project"
	agentTestID               = "agent_test"
)

func agentResponse(uid, scope, id string) map[string]any {
	return map[string]any{"id": id, objectField: agentField, modelField: SessionModel, metadataField: map[string]string{ownerKey: uid, operatorKey: operatorValue, agentScopeKey: scope}}
}

func TestAgentFullConfigurationWireContract(t *testing.T) {
	// Independent CRD and wire fixtures catch spelling, flattening and omission errors.
	specJSON := `{"model":"gpt-6-astra","name":"Helper","instructions":"","metadata":{"team":"one"},"reasoning":{"effort":"high","summary":"auto"},"serviceTier":"priority","text":{"verbosity":"low","format":{"type":"json_schema","schema":{"type":"object","properties":{"serviceTier":{"type":"string"}}}}},"multiAgent":{"enabled":false,"maxConcurrentSubagents":3},"tools":[{"type":"function","function":{"name":"lookup","description":"Look up data","parameters":{"type":"object","properties":{"serverURL":{"type":"string"}}},"deferLoading":true}},{"type":"tool_search"},{"type":"programmatic_tool_calling","programmaticToolCalling":{"enabled":false}},{"type":"mcp","mcp":{"serverLabel":"remote","credentialID":"cred_reference","required":false,"allowedTools":[],"connectionOrigin":"service","requestMetadata":{"traceparent":"trace"},"transport":{"type":"http","http":{"serverURL":"https://example.com/mcp","headers":{"Accept":"application/json"}}}}},{"type":"mcp","mcp":{"serverLabel":"local","transport":{"type":"stdio","stdio":{"command":"mcp-server","cwd":"/workspace","args":[],"envVars":["MCP_ENDPOINT"]}}}},{"type":"web_search","webSearch":{"allowedDomains":["example.com"],"contextSize":"low","mode":"cached","location":{"city":"Stockholm","country":"SE","region":"Stockholm","timezone":"Europe/Stockholm"}}},{"type":"computer_use","computerUse":{"includeScreenshots":false}}]}`
	expectedJSON := `{"model":"gpt-6-astra","name":"Helper","instructions":"","metadata":{"team":"one","kubernetes_uid":"uid-test","operator":"codex-session-operator","owner_scope":"installation/project"},"reasoning":{"effort":"high","summary":"auto"},"service_tier":"priority","text":{"verbosity":"low","format":{"type":"json_schema","schema":{"type":"object","properties":{"serviceTier":{"type":"string"}}}}},"multi_agent":{"enabled":false,"max_concurrent_subagents":3},"tools":[{"type":"function","name":"lookup","description":"Look up data","parameters":{"type":"object","properties":{"serverURL":{"type":"string"}}},"defer_loading":true},{"type":"tool_search"},{"type":"programmatic_tool_calling","enabled":false},{"type":"mcp","server_label":"remote","credential_id":"cred_reference","required":false,"allowed_tools":[],"connection_origin":"service","request_metadata":{"traceparent":"trace"},"transport":{"type":"http","server_url":"https://example.com/mcp","headers":{"Accept":"application/json"}}},{"type":"mcp","server_label":"local","transport":{"type":"stdio","command":"mcp-server","cwd":"/workspace","args":[],"env_vars":["MCP_ENDPOINT"]}},{"type":"web_search","allowed_domains":["example.com"],"context_size":"low","mode":"cached","location":{"city":"Stockholm","country":"SE","region":"Stockholm","timezone":"Europe/Stockholm"}},{"type":"computer_use","include_screenshots":false}]}`
	var spec agentsv1alpha1.AgentSpec
	if err := json.Unmarshal([]byte(specJSON), &spec); err != nil {
		t.Fatal(err)
	}
	fields, err := AgentConfiguration(spec)
	if err != nil {
		t.Fatal(err)
	}
	var expected map[string]any
	if err := json.Unmarshal([]byte(expectedJSON), &expected); err != nil {
		t.Fatal(err)
	}
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Method != http.MethodPost || r.URL.Path != "/v1/agents" || r.Header.Get("OpenAI-Beta") != agentsBetaHeader {
			t.Error("Incorrect saved Agent endpoint or beta contract")
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if !reflect.DeepEqual(body, expected) {
			t.Errorf("Wire configuration differs: got %v", body)
		}
		w.Header().Set("Content-Type", "application/json")
		body["id"] = agentTestID
		body[objectField] = agentField
		_ = json.NewEncoder(w).Encode(body)
	}))
	defer server.Close()
	remote, err := clientForServer(server, nil).CreateAgent(context.Background(), "uid-test", agentTestScope, fields)
	if err != nil || remote.OwnerScope != agentTestScope || remote.OwnerUID != "uid-test" || calls != 1 {
		t.Fatalf("Create observation: calls=%d err=%v", calls, err)
	}
	changes, err := AgentChanges(fields, remote, []string{"team"})
	if err != nil || len(changes) != 0 {
		t.Fatalf("Stable readback would update: %v, %v", changes, err)
	}
}

func TestAgentPartialUpdateAndExplicitResets(t *testing.T) {
	spec := agentsv1alpha1.AgentSpec{Model: SessionModel, ClearName: true, ResetReasoning: true}
	emptyTools := []agentsv1alpha1.AgentTool{}
	spec.Tools = &emptyTools
	fields, err := AgentConfiguration(spec)
	if err != nil {
		t.Fatal(err)
	}
	remote := Agent{Config: map[string]any{modelField: SessionModel, nameField: "old", reasoningField: map[string]any{effortField: "high"}, toolsField: []any{map[string]any{typeField: webSearchType}}}}
	changes, err := AgentChanges(fields, remote, nil)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if r.URL.Path != "/v1/agents/agent_test" || r.Method != http.MethodPost || len(body) != 3 {
			t.Error("Update must contain only name, reasoning and tools")
		}
		for _, key := range []string{nameField, reasoningField} {
			value, ok := body[key]
			if !ok || value != nil {
				t.Error("Explicit null was lost")
			}
		}
		if !reflect.DeepEqual(body[toolsField], []any{}) {
			t.Error("Explicit empty tools was lost")
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(agentResponse("uid-test", agentTestScope, agentTestID))
	}))
	defer server.Close()
	if _, err := clientForServer(server, nil).UpdateAgent(context.Background(), agentTestID, changes); err != nil {
		t.Fatal(err)
	}
}

func TestAgentMetadataOwnershipAndReplacement(t *testing.T) {
	remote := Agent{Config: map[string]any{modelField: SessionModel, metadataField: map[string]any{"external": "keep", testOwnedMetadataKey: "remove", ownerKey: "uid", operatorKey: operatorValue, agentScopeKey: agentTestScope}}}
	changes, err := AgentChanges(map[string]any{metadataField: map[string]any{}}, remote, []string{testOwnedMetadataKey})
	if err != nil {
		t.Fatal(err)
	}
	metadata, ok := changes[metadataField].(map[string]any)
	if !ok || metadata["external"] != "keep" || metadata[ownerKey] != "uid" {
		t.Fatal("Unmanaged metadata or ownership was removed")
	}
	if _, ok := metadata[testOwnedMetadataKey]; ok {
		t.Fatal("Empty map did not clear managed key")
	}
	original, ok := remote.Config[metadataField].(map[string]any)
	if !ok || original[testOwnedMetadataKey] != "remove" {
		t.Fatal("Observation was mutated")
	}
	changes, err = AgentChanges(map[string]any{modelField: SessionModel}, remote, []string{testOwnedMetadataKey})
	if err != nil || len(changes) != 0 {
		t.Fatal("Omitted metadata must remain unchanged")
	}
}

func TestAgentDiscoveryLostResponseAndScope(t *testing.T) {
	creates := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPost {
			creates++
			_ = json.NewEncoder(w).Encode(agentResponse("uid-test", agentTestScope, agentTestID))
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{dataField: []any{agentResponse("uid-test", "other-project", "agent_foreign"), agentResponse("uid-test", agentTestScope, agentTestID)}, hasMoreField: false})
	}))
	defer server.Close()
	drop := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		res, err := http.DefaultTransport.RoundTrip(r)
		if err != nil {
			return nil, err
		}
		if r.Method == http.MethodPost {
			_, _ = io.Copy(io.Discard, res.Body)
			_ = res.Body.Close()
			return nil, errors.New("private response detail")
		}
		return res, nil
	})
	c := clientForServer(server, drop)
	_, err := c.CreateAgent(context.Background(), "uid-test", agentTestScope, map[string]any{modelField: SessionModel})
	if err == nil || strings.Contains(err.Error(), "private") {
		t.Fatal("Lost response must be sanitized")
	}
	found, err := c.FindAgent(context.Background(), "uid-test", agentTestScope)
	if err != nil || found.ID != agentTestID || creates != 1 {
		t.Fatalf("Recovery failed: calls=%d err=%v", creates, err)
	}
}

func TestAgentDiscoveryBoundaries(t *testing.T) {
	tests := []struct {
		name      string
		wantError bool
		wantCalls int
	}{
		{name: "paginated", wantCalls: 2}, {name: duplicateScenario, wantError: true, wantCalls: 1},
		{name: limitScenario, wantError: true, wantCalls: maxDiscoveryPages}, {name: missingPaginationScenario, wantError: true, wantCalls: 1},
		{name: "empty", wantCalls: 1}, {name: unownedScenario, wantCalls: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				w.Header().Set("Content-Type", "application/json")
				page := map[string]any{dataField: []any{agentResponse("other-uid", agentTestScope, "agent_other")}, hasMoreField: false}
				switch tt.name {
				case "paginated":
					if calls == 1 {
						page[hasMoreField] = true
					} else {
						if r.URL.Query().Get("after") != "agent_other" {
							t.Error("Cursor lost")
						}
						page[dataField] = []any{agentResponse("uid-test", agentTestScope, agentTestID)}
					}
				case duplicateScenario:
					page[dataField] = []any{agentResponse("uid-test", agentTestScope, agentTestID), agentResponse("uid-test", agentTestScope, "agent_duplicate")}
				case limitScenario:
					page[hasMoreField] = true
				case missingPaginationScenario:
					delete(page, hasMoreField)
				case "empty":
					page[dataField] = []any{}
				}
				_ = json.NewEncoder(w).Encode(page)
			}))
			defer server.Close()
			found, err := clientForServer(server, nil).FindAgent(context.Background(), "uid-test", agentTestScope)
			if (err != nil) != tt.wantError || calls != tt.wantCalls {
				t.Fatalf("Unsafe discovery: calls=%d err=%v", calls, err)
			}
			if tt.wantError && found.ID != "" {
				t.Fatal("Partial match escaped failed scan")
			}
		})
	}
}

func TestAgentProviderFailureClassification(t *testing.T) {
	tests := []struct {
		name        string
		method      string
		status      int
		response    string
		wantMissing bool
	}{
		{name: "create-no-retry", method: createOperation, status: 500},
		{name: "update-rejected", method: updateOperation, status: 400},
		{name: "get-missing", method: "get", status: 404, wantMissing: true},
		{name: "delete-missing", method: deleteOperation, status: 404},
		{name: deleteUnconfirmedScenario, method: deleteOperation, status: 200, response: `{"id":"agent_test","deleted":false}`},
		{name: "delete-wrong-id", method: deleteOperation, status: 200, response: `{"id":"agent_other","deleted":true}`},
		{name: "delete-confirmed", method: deleteOperation, status: 200, response: `{"id":"agent_test","deleted":true}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls++
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tt.status)
				body := tt.response
				if body == "" {
					body = `{"error":{"message":"private-provider-detail"}}`
				}
				_, _ = io.WriteString(w, body)
			}))
			defer server.Close()
			c := clientForServer(server, nil)
			var err error
			switch tt.method {
			case createOperation:
				_, err = c.CreateAgent(context.Background(), "uid", agentTestScope, map[string]any{modelField: SessionModel})
			case updateOperation:
				_, err = c.UpdateAgent(context.Background(), agentTestID, map[string]any{modelField: SessionModel})
			case "get":
				_, err = c.GetAgent(context.Background(), agentTestID)
			case deleteOperation:
				err = c.DeleteAgent(context.Background(), agentTestID)
			}
			if tt.method == updateOperation {
				failure, ok := errors.AsType[*AgentRequestError](err)
				if !ok || failure.StatusCode != http.StatusBadRequest {
					t.Fatal("Rejected update lost its safe HTTP classification")
				}
			}
			wantError := tt.name != "delete-missing" && tt.name != "delete-confirmed"
			if (err != nil) != wantError || calls != 1 || (err != nil && strings.Contains(err.Error(), "private-provider-detail")) || errors.Is(err, ErrNotFound) != tt.wantMissing {
				t.Fatalf("Failure contract: calls=%d err=%v", calls, err)
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) { t.Error("Canceled call reached server") }))
	defer server.Close()
	if _, err := clientForServer(server, nil).GetAgent(ctx, agentTestID); !errors.Is(err, context.Canceled) {
		t.Fatal("Cancellation identity lost")
	}
}

func TestSavedAgentSessionWireContract(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if body["agent_id"] != agentTestID {
			t.Error("Saved Agent ID missing")
		}
		if _, ok := body[agentField]; ok {
			t.Error("Inline Agent would override saved configuration")
		}
		for _, key := range []string{"input", "stream"} {
			if _, ok := body[key]; ok {
				t.Error("Gateway traffic escaped into lifecycle")
			}
		}
		response := remoteJSON("uid-test", testSessionID)
		response[agentField] = map[string]any{"id": agentTestID, modelField: "gpt-6-astra"}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()
	remote, err := clientForServer(server, nil).CreateAgentSession(context.Background(), "uid-test", testWorkspace, agentTestID)
	if err != nil || remote.AgentID != agentTestID || remote.Model != "gpt-6-astra" {
		t.Fatalf("Agent Session failed: %v", err)
	}
}

func TestAgentNestedDefaultsAndReplacement(t *testing.T) {
	tests := []struct {
		name             string
		field            string
		desired, current any
		wantEqual        bool
	}{
		{name: "MCP HTTP readback defaults", field: toolsField, desired: []any{map[string]any{typeField: mcpToolType, "server_label": "test", transportField: map[string]any{typeField: httpTransportType, serverURLField: testMCPURL}}}, current: []any{map[string]any{typeField: mcpToolType, "server_label": "test", "required": false, allowedToolsField: nil, credentialIDField: nil, "request_metadata": map[string]any{}, testConnectionOriginField: testServiceOrigin, transportField: map[string]any{typeField: httpTransportType, serverURLField: testMCPURL, "headers": map[string]any{}}}}, wantEqual: true},
		{name: "MCP stdio readback defaults", field: toolsField, desired: []any{map[string]any{typeField: mcpToolType, transportField: map[string]any{typeField: stdioTransportType, testCommandField: "server", testCwdField: testWorkspace}}}, current: []any{map[string]any{typeField: mcpToolType, testConnectionOriginField: testServiceOrigin, transportField: map[string]any{typeField: stdioTransportType, testCommandField: "server", testCwdField: testWorkspace, "args": []any{}, "env_vars": []any{}}}}, wantEqual: true},
		{name: "partial web location", field: toolsField, desired: []any{map[string]any{typeField: webSearchType, "location": map[string]any{"country": "SE"}}}, current: []any{map[string]any{typeField: webSearchType, "location": map[string]any{"country": "SE", "city": nil, "region": nil, "timezone": nil}}}, wantEqual: true},

		{name: "text defaults", field: textField, desired: map[string]any{}, current: map[string]any{verbosityField: "medium", "format": map[string]any{typeField: textField}}, wantEqual: true},
		{name: "text replacement clears schema", field: textField, desired: map[string]any{verbosityField: lowSetting}, current: map[string]any{verbosityField: lowSetting, "format": map[string]any{typeField: "json_schema", "schema": map[string]any{typeField: objectField}}}},
		{name: "subagent default", field: multiAgentField, desired: map[string]any{enabledField: false}, current: map[string]any{enabledField: false, "max_concurrent_subagents": float64(6)}, wantEqual: true},
		{name: "disabled programmatic calling", field: toolsField, desired: []any{map[string]any{typeField: programmaticToolCallingType, enabledField: false}}, current: []any{map[string]any{typeField: programmaticToolCallingType, enabledField: true}}},
		{name: "reasoning clears summary", field: reasoningField, desired: map[string]any{effortField: lowSetting}, current: map[string]any{effortField: lowSetting, "summary": "auto"}},
		{name: "empty MCP allowlist differs from all", field: toolsField, desired: []any{map[string]any{typeField: mcpToolType, allowedToolsField: []any{}}}, current: []any{map[string]any{typeField: mcpToolType, allowedToolsField: nil}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := agentFieldEqual(tt.field, tt.desired, tt.current); got != tt.wantEqual {
				t.Fatalf("Replacement semantics mismatch: equal=%v", got)
			}
		})
	}
}

func TestAgentInvalidConfigurationAndCapabilityGates(t *testing.T) {
	tests := []struct {
		name string
		spec string
	}{
		{name: "conflicting branches", spec: `{"model":"gpt-6-luna","tools":[{"type":"tool_search","computerUse":{}}]}`},
		{name: "deferred without search", spec: `{"model":"gpt-6-luna","tools":[{"type":"function","function":{"name":"lookup","description":"Look up","parameters":{},"deferLoading":true}}]}`},
		{name: "search without deferred", spec: `{"model":"gpt-6-luna","tools":[{"type":"tool_search"}]}`},
		{name: "secret header", spec: `{"model":"gpt-6-luna","tools":[{"type":"mcp","mcp":{"serverLabel":"test","transport":{"type":"http","http":{"serverURL":"https://example.com","headers":{"Authorization":"FORBIDDEN_PLACEHOLDER"}}}}}]}`},
		{name: "URL credentials", spec: `{"model":"gpt-6-luna","tools":[{"type":"mcp","mcp":{"serverLabel":"test","transport":{"type":"http","http":{"serverURL":"https://user:FORBIDDEN_PLACEHOLDER@example.com"}}}}]}`},
		{name: "arbitrary request metadata", spec: `{"model":"gpt-6-luna","tools":[{"type":"mcp","mcp":{"serverLabel":"test","requestMetadata":{"authorization":"FORBIDDEN_PLACEHOLDER"},"transport":{"type":"http","http":{"serverURL":"https://example.com"}}}}]}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var spec agentsv1alpha1.AgentSpec
			if err := json.Unmarshal([]byte(tt.spec), &spec); err != nil {
				t.Fatal(err)
			}
			if _, err := AgentConfiguration(spec); err == nil {
				t.Fatal("Unsafe configuration accepted")
			}
		})
	}
	gates := []struct {
		name       string
		tool       map[string]any
		wantReason string
	}{
		{name: "function", tool: map[string]any{typeField: functionToolType}, wantReason: "GatewayRequired"},
		{name: "computer", tool: map[string]any{typeField: computerUseToolType}, wantReason: "HostedDesktopRequired"},
		{name: "credential", tool: map[string]any{typeField: mcpToolType, credentialIDField: "cred_reference"}, wantReason: "VaultRequired"},
		{name: stdioTransportType, tool: map[string]any{typeField: mcpToolType, transportField: map[string]any{typeField: stdioTransportType}}, wantReason: environmentCapabilitiesReason},
		{name: "web", tool: map[string]any{typeField: webSearchType}},
	}
	for _, tt := range gates {
		t.Run(tt.name, func(t *testing.T) {
			reason, _ := AgentSessionBlock(map[string]any{toolsField: []any{tt.tool}})
			if reason != tt.wantReason {
				t.Fatalf("Capability gate=%s", reason)
			}
		})
	}
}

func TestAgentSessionDigestAccountsForRedactedProtocolHeaders(t *testing.T) {
	saved := map[string]any{modelField: SessionModel, toolsField: []any{map[string]any{typeField: mcpToolType, transportField: map[string]any{typeField: httpTransportType, serverURLField: testMCPURL, "headers": map[string]any{"Accept": "application/json"}}}}}
	copied := map[string]any{modelField: SessionModel, toolsField: []any{map[string]any{typeField: mcpToolType, transportField: map[string]any{typeField: httpTransportType, serverURLField: testMCPURL}}}}
	if AgentConfigurationDigest(saved) != AgentConfigurationDigest(copied) {
		t.Fatal("Redacted protocol headers made a valid copied Session mismatch")
	}
	copied["instructions"] = "Different configuration"
	if AgentConfigurationDigest(saved) == AgentConfigurationDigest(copied) {
		t.Fatal("Observable configuration change was hidden")
	}
}
