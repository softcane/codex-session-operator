package openaiapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
)

const testSessionFormatField = "format"
const testSessionMediumSetting = "medium"
const testConfiguredModel = "gpt-6-astra"

func TestConfiguredSessionWireAndPartialUpdates(t *testing.T) {
	model, effort, tier, instructions := SessionModel, lowSetting, "priority", "Session creation instructions"
	tools := []agentsv1alpha1.AgentTool{}
	spec := &agentsv1alpha1.SessionAgentOverrides{Model: &model, Instructions: &instructions, Reasoning: &agentsv1alpha1.AgentReasoning{Effort: &effort}, ServiceTier: &tier, Tools: &tools, MultiAgent: &agentsv1alpha1.AgentMultiAgent{Enabled: false}}
	fields, err := SessionAgentConfiguration(spec)
	if err != nil {
		t.Fatal(err)
	}
	const wantJSON = `{"model":"gpt-6-luna","instructions":"Session creation instructions","reasoning":{"effort":"low"},"service_tier":"priority","tools":[],"multi_agent":{"enabled":false}}`
	var want map[string]any
	if err := json.Unmarshal([]byte(wantJSON), &want); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(fields, want) {
		t.Fatal("Session override fields differ from independent fixture")
	}
	count := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request map[string]any
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
		}
		if count == 0 {
			if r.URL.Path != testSessionsPath || request["agent_id"] != agentTestID {
				t.Error("creation Agent reference missing")
			}
			if !reflect.DeepEqual(request[agentField], want) || !reflect.DeepEqual(request["vault_ids"], []any{testVaultID}) {
				t.Error("creation overrides or Vault attachment missing")
			}
			for _, key := range []string{"input", "stream"} {
				if _, present := request[key]; present {
					t.Error("gateway traffic appeared in lifecycle request")
				}
			}
		} else {
			expected := map[string]any{agentField: map[string]any{reasoningField: map[string]any{effortField: nil}, serviceTierField: nil}, metadataField: map[string]any{ownerKey: "uid-settings", operatorKey: operatorValue, agentScopeKey: agentTestScope}}
			if !reflect.DeepEqual(request, expected) {
				t.Error("partial Session update sent unsupported or omitted fields")
			}
		}
		count++
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(remoteJSON("uid-settings", testSessionID))
	}))
	defer server.Close()
	c := clientForServer(server, nil)
	spend := &agentsv1alpha1.SessionSpendControl{Mode: sessionSpendModeUnlimited}
	if _, err := c.CreateConfiguredSession(context.Background(), "uid-settings", agentTestScope, agentTestID, map[string]any{typeField: selfHostedType, workspaceDirectoryField: testWorkspace}, fields, []string{testVaultID}, nil, spend); err != nil {
		t.Fatal(err)
	}
	if _, err := c.UpdateSession(context.Background(), testSessionID, map[string]any{agentField: map[string]any{reasoningField: map[string]any{effortField: nil}, serviceTierField: nil}, metadataField: map[string]string{ownerKey: "uid-settings", operatorKey: operatorValue, agentScopeKey: agentTestScope}}); err != nil {
		t.Fatal(err)
	}
}
func TestSessionSettingsOmissionResetAndCreationDigest(t *testing.T) {
	model, effort, summary := SessionModel, lowSetting, "auto"
	spec := &agentsv1alpha1.SessionAgentOverrides{Model: &model, Reasoning: &agentsv1alpha1.AgentReasoning{Effort: &effort, Summary: &summary}}
	before := SessionCreationOnlyDigest(spec)
	effort = "high"
	model = testConfiguredModel
	if SessionCreationOnlyDigest(spec) != before {
		t.Fatal("mutable settings changed creation-only digest")
	}
	summary = "detailed"
	if SessionCreationOnlyDigest(spec) == before {
		t.Fatal("unsupported summary mutation was not detected")
	}
	remote := Session{AgentConfiguration: map[string]any{modelField: model, reasoningField: map[string]any{effortField: effort}}}
	if len(SessionSettingsChanges(nil, remote, "")) != 0 {
		t.Fatal("omission should preserve remote values")
	}
	reset := &agentsv1alpha1.SessionAgentOverrides{ResetReasoning: true, ResetServiceTier: true}
	if len(SessionSettingsChanges(reset, remote, "")) != 2 {
		t.Fatal("null resets were not sent")
	}
	if len(SessionSettingsChanges(reset, remote, SessionMutableDigest(reset))) != 0 {
		t.Fatal("acknowledged defaults triggered redundant updates")
	}
}

func TestSessionImmutableDigestNormalizesNullSummary(t *testing.T) {
	requested := map[string]any{modelField: SessionModel, reasoningField: map[string]any{effortField: lowSetting}}
	readback := map[string]any{modelField: SessionModel, reasoningField: map[string]any{effortField: testSessionMediumSetting, "summary": nil}}
	if SessionImmutableAgentDigest(requested) != SessionImmutableAgentDigest(readback) {
		t.Fatal("null default summary falsely changed immutable configuration")
	}
}

func TestInlineSessionDigestIncludesReadbackDefaults(t *testing.T) {
	request := map[string]any{modelField: SessionModel}
	readback := map[string]any{modelField: SessionModel, nameField: nil, "instructions": "", textField: map[string]any{testSessionFormatField: map[string]any{typeField: textField}, "verbosity": testSessionMediumSetting}, multiAgentField: map[string]any{enabledField: false, "max_concurrent_subagents": nil}, toolsField: []any{}}
	if SessionImmutableAgentDigest(request) != SessionImmutableAgentDigest(readback) {
		t.Fatal("inline Session default readback differs from creation configuration")
	}
}

func TestSessionSpendControlField(t *testing.T) {
	one, zero, over := int64(1), int64(0), maxSessionSpendLimitCents+1
	tests := []struct {
		name    string
		spec    *agentsv1alpha1.SessionSpendControl
		present bool
		want    any
		wantErr bool
	}{
		{name: "missing required choice", wantErr: true},
		{name: "minimum limit", spec: &agentsv1alpha1.SessionSpendControl{Mode: sessionSpendModeLimited, LimitCents: &one}, present: true, want: map[string]any{spendLimitField: one}},
		{name: "explicit unlimited", spec: &agentsv1alpha1.SessionSpendControl{Mode: sessionSpendModeUnlimited}, present: true},
		{name: "missing limit", spec: &agentsv1alpha1.SessionSpendControl{Mode: sessionSpendModeLimited}, wantErr: true},
		{name: "zero limit", spec: &agentsv1alpha1.SessionSpendControl{Mode: sessionSpendModeLimited, LimitCents: &zero}, wantErr: true},
		{name: "over maximum", spec: &agentsv1alpha1.SessionSpendControl{Mode: sessionSpendModeLimited, LimitCents: &over}, wantErr: true},
		{name: "unlimited with limit", spec: &agentsv1alpha1.SessionSpendControl{Mode: sessionSpendModeUnlimited, LimitCents: &one}, wantErr: true},
		{name: "unknown mode", spec: &agentsv1alpha1.SessionSpendControl{Mode: "unknown"}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, present, err := SessionSpendControlField(tt.spec)
			if (err != nil) != tt.wantErr || present != tt.present || !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("field = (%v, %v, %v), want (%v, %v, error=%v)", got, present, err, tt.want, tt.present, tt.wantErr)
			}
		})
	}
}

func TestSessionSpendControlWireAndReadback(t *testing.T) {
	limit := int64(125)
	updatedLimit := int64(200)
	tests := []struct {
		name             string
		spec             *agentsv1alpha1.SessionSpendControl
		wantPresent      bool
		wantValue        any
		wantUpdatedValue any
		remoteLimit      *int64
		remoteNull       bool
		updateFields     map[string]any
	}{
		{name: "limited", spec: &agentsv1alpha1.SessionSpendControl{Mode: sessionSpendModeLimited, LimitCents: &limit}, wantPresent: true, wantValue: map[string]any{spendLimitField: float64(limit)}, wantUpdatedValue: map[string]any{spendLimitField: float64(updatedLimit)}, remoteLimit: &limit,
			updateFields: map[string]any{spendControlField: map[string]any{spendLimitField: updatedLimit}}},
		{name: "explicit unlimited", spec: &agentsv1alpha1.SessionSpendControl{Mode: sessionSpendModeUnlimited},
			updateFields: map[string]any{spendControlField: nil}},
		{name: "explicit null readback", spec: &agentsv1alpha1.SessionSpendControl{Mode: sessionSpendModeUnlimited}, remoteNull: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			requests := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests++
				w.Header().Set("Content-Type", "application/json")
				if r.Method == http.MethodPost {
					var body map[string]any
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
						t.Error(err)
					}
					value, present := body[spendControlField]
					if r.URL.Path == testSessionsPath {
						if present != tt.wantPresent || !reflect.DeepEqual(value, tt.wantValue) {
							t.Errorf("create spend_control = (%v, %v), want (%v, %v)", value, present, tt.wantValue, tt.wantPresent)
						}
					} else if !reflect.DeepEqual(value, tt.wantUpdatedValue) || !present {
						t.Errorf("update spend_control = (%v, %v), want %v", value, present, tt.updateFields)
					}
				}
				response := remoteJSON("uid-spend", testSessionID)
				if tt.remoteLimit != nil {
					observedLimit := *tt.remoteLimit
					if requests == 3 {
						observedLimit = updatedLimit
					}
					response[spendControlField] = map[string]any{spendLimitField: observedLimit, spendConsumedField: 5}
				} else if tt.remoteNull {
					response[spendControlField] = nil
				}
				_ = json.NewEncoder(w).Encode(response)
			}))
			defer server.Close()
			c := clientForServer(server, nil)
			remote, err := c.CreateConfiguredSession(context.Background(), "uid-spend", agentTestScope, agentTestID,
				map[string]any{typeField: selfHostedType, workspaceDirectoryField: testWorkspace}, nil, nil, nil, tt.spec)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(remote.SpendLimitCents, tt.remoteLimit) || remote.SpendControlObserved != (tt.remoteLimit != nil || tt.remoteNull) ||
				(tt.remoteLimit != nil && (remote.SpendConsumedCents == nil || *remote.SpendConsumedCents != 5)) {
				t.Fatalf("create readback = (%v, %v), want limit %v consumed 5", remote.SpendLimitCents, remote.SpendConsumedCents, tt.remoteLimit)
			}
			remote, err = c.GetSession(context.Background(), testSessionID)
			if err != nil || !reflect.DeepEqual(remote.SpendLimitCents, tt.remoteLimit) {
				t.Fatalf("GET readback limit = %v, error = %v", remote.SpendLimitCents, err)
			}
			if tt.updateFields != nil {
				remote, err = c.UpdateSession(context.Background(), testSessionID, tt.updateFields)
				wantUpdatedLimit := tt.remoteLimit
				if tt.remoteLimit != nil {
					wantUpdatedLimit = &updatedLimit
				}
				if err != nil || !reflect.DeepEqual(remote.SpendLimitCents, wantUpdatedLimit) {
					t.Fatalf("update readback limit = %v, error = %v", remote.SpendLimitCents, err)
				}
			}
			wantRequests := 2
			if tt.updateFields != nil {
				wantRequests++
			}
			if requests != wantRequests {
				t.Fatalf("requests = %d, want %d", requests, wantRequests)
			}
		})
	}
}

func TestUnlimitedCreationDoesNotConfigureProviderBudget(t *testing.T) {
	for _, mode := range []string{selfHostedType, _noneValue} {
		t.Run(mode, func(t *testing.T) {
			requests := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests++
				w.Header().Set("Content-Type", "application/json")
				var body map[string]json.RawMessage
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Error(err)
				}
				if r.Method != http.MethodPost || r.URL.Path != testSessionsPath {
					t.Error("unlimited creation used an unexpected endpoint")
				}
				// Projects without the optional budget capability can reject even
				// explicit null. Omitting the field must avoid that capability gate.
				if _, present := body[spendControlField]; present {
					w.WriteHeader(http.StatusBadRequest)
					_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{
						"param": spendControlField, "message": "Session budget configuration is not enabled"}})
					return
				}
				response := remoteJSON("uid-unlimited", testSessionID)
				response[testEnvironmentField] = map[string]any{typeField: mode}
				_ = json.NewEncoder(w).Encode(response)
			}))
			defer server.Close()
			client := clientForServer(server, nil)
			spend := &agentsv1alpha1.SessionSpendControl{Mode: sessionSpendModeUnlimited}
			environment := map[string]any{typeField: mode}
			var remote Session
			var err error
			if mode == _noneValue {
				remote, err = client.CreateInitialSession(t.Context(), "uid-unlimited", agentTestScope, agentTestID,
					environment, nil, nil, nil, spend, "durable initial task", "durable-first-key")
			} else {
				remote, err = client.CreateConfiguredSession(t.Context(), "uid-unlimited", agentTestScope, agentTestID,
					environment, nil, nil, nil, spend)
			}
			if err != nil || remote.ID != testSessionID || remote.SpendControlObserved || remote.SpendLimitCents != nil || requests != 1 {
				t.Fatalf("unlimited creation configured a budget or did not create once: calls=%d error=%v", requests, err)
			}
		})
	}
}

func TestSessionMalformedSpendReadback(t *testing.T) {
	tests := []struct {
		name  string
		value any
	}{
		{name: "missing limit", value: map[string]any{spendConsumedField: 1}},
		{name: "invalid limit", value: map[string]any{spendLimitField: 0}},
		{name: "invalid consumed", value: map[string]any{spendLimitField: 10, spendConsumedField: -1}},
		{name: "wrong type", value: []any{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				response := remoteJSON("uid-spend", testSessionID)
				response[spendControlField] = tt.value
				_ = json.NewEncoder(w).Encode(response)
			}))
			defer server.Close()
			remote, err := clientForServer(server, nil).GetSession(context.Background(), testSessionID)
			if err != nil {
				t.Fatal(err)
			}
			if !remote.SpendControlObserved || !remote.SpendControlMalformed {
				t.Fatalf("malformed readback accepted: observed=%t malformed=%t", remote.SpendControlObserved, remote.SpendControlMalformed)
			}
		})
	}
}
