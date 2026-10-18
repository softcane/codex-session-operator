package openaiapi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"maps"
	"reflect"

	openai "github.com/openai/openai-go/v3"
	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
)

const (
	maxSessionSpendLimitCents int64 = 4503599627370495
	sessionSpendModeLimited         = "limited"
	sessionSpendModeUnlimited       = "unlimited"
	spendControlField               = "spend_control"
	spendLimitField                 = "limit"
	spendConsumedField              = "consumed"
	workspaceDirectoryField         = "workspace_directory"
)

// SessionSpendControlField validates an explicit unlimited choice or positive limit in USD cents.
func SessionSpendControlField(spec *agentsv1alpha1.SessionSpendControl) (value any, present bool, err error) {
	if spec == nil {
		return nil, false, errors.New("Session spend control is required")
	}
	switch spec.Mode {
	case sessionSpendModeLimited:
		if spec.LimitCents == nil || *spec.LimitCents < 1 || *spec.LimitCents > maxSessionSpendLimitCents {
			return nil, false, errors.New("Session spend limit must be within the OpenAI cent range")
		}
		return map[string]any{spendLimitField: *spec.LimitCents}, true, nil
	case sessionSpendModeUnlimited:
		if spec.LimitCents != nil {
			return nil, false, errors.New("unlimited Session cannot set a spend limit")
		}
		return nil, true, nil
	default:
		return nil, false, errors.New("unsupported Session spend mode")
	}
}

// SessionAgentConfiguration reuses the maintained typed Agent unions without saved-resource fields.
func SessionAgentConfiguration(spec *agentsv1alpha1.SessionAgentOverrides) (map[string]any, error) {
	if spec == nil {
		return nil, nil
	}
	model := SessionModel
	if spec.Model != nil {
		model = *spec.Model
	}
	fields, err := AgentConfiguration(agentsv1alpha1.AgentSpec{Model: model, Instructions: spec.Instructions, Reasoning: spec.Reasoning, ResetReasoning: spec.ResetReasoning, ServiceTier: spec.ServiceTier, Text: spec.Text, MultiAgent: spec.MultiAgent, Tools: spec.Tools})
	if err != nil {
		return nil, err
	}
	if spec.Model == nil {
		delete(fields, modelField)
	}
	if spec.ClearTools {
		if spec.Tools != nil {
			return nil, errors.New("tools and clearTools conflict")
		}
		fields[toolsField] = nil
	}
	if spec.ResetServiceTier {
		if spec.ServiceTier != nil {
			return nil, errors.New("serviceTier and resetServiceTier conflict")
		}
		fields[serviceTierField] = nil
	}
	return fields, nil
}

// EffectiveSessionAgent models field replacement when saved configuration is overridden.
func EffectiveSessionAgent(saved, overrides map[string]any) map[string]any {
	raw, _ := json.Marshal(saved)
	var result map[string]any
	_ = json.Unmarshal(raw, &result)
	if result == nil {
		result = make(map[string]any)
	}
	maps.Copy(result, overrides)
	return result
}

// SessionImmutableAgentDigest excludes the fields the Session update API can mutate.
func SessionImmutableAgentDigest(fields map[string]any) string {
	result := maps.Clone(fields)
	if result == nil {
		result = make(map[string]any)
	}
	// Inline Sessions have no saved-Agent GET from which to inherit these defaults.
	// Match the documented complete readback while retaining explicit replacements.
	defaultField(result, nameField, nil)
	defaultField(result, "instructions", "")
	defaultField(result, textField, map[string]any{"format": map[string]any{typeField: textField}, "verbosity": "medium"})
	defaultField(result, multiAgentField, map[string]any{enabledField: false})
	defaultField(result, toolsField, []any{})
	delete(result, modelField)
	delete(result, serviceTierField)
	if reasoning, ok := result[reasoningField].(map[string]any); ok {
		reasoning = maps.Clone(reasoning)
		delete(reasoning, "effort")
		if reasoning["summary"] == nil || reasoning["summary"] == "" {
			delete(reasoning, "summary")
		}
		if len(reasoning) == 0 {
			delete(result, reasoningField)
		} else {
			result[reasoningField] = reasoning
		}
	} else {
		delete(result, reasoningField)
	}
	return AgentConfigurationDigest(result)
}

// SessionCreationOnlyDigest detects user edits to unsupported existing-Session fields.
func SessionCreationOnlyDigest(spec *agentsv1alpha1.SessionAgentOverrides) string {
	fields, _ := SessionAgentConfiguration(spec)
	return SessionImmutableAgentDigest(fields)
}

// SessionMutableFields sends only documented existing-Session settings.
func SessionMutableFields(spec *agentsv1alpha1.SessionAgentOverrides) map[string]any {
	agent := make(map[string]any)
	if spec == nil {
		return agent
	}
	if spec.Model != nil {
		agent[modelField] = *spec.Model
	}
	if spec.ServiceTier != nil {
		agent[serviceTierField] = *spec.ServiceTier
	} else if spec.ResetServiceTier {
		agent[serviceTierField] = nil
	}
	if spec.ResetReasoning {
		agent[reasoningField] = map[string]any{"effort": nil}
	} else if spec.Reasoning != nil && spec.Reasoning.Effort != nil {
		agent[reasoningField] = map[string]any{"effort": *spec.Reasoning.Effort}
	}
	return agent
}

// SessionMutableDigest acknowledges null/default resets independently of redacted/default GET representations.
func SessionMutableDigest(spec *agentsv1alpha1.SessionAgentOverrides) string {
	raw, _ := json.Marshal(SessionMutableFields(spec))
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// SessionSettingsChanges preserves omissions and handles nested effort-only updates.
func SessionSettingsChanges(spec *agentsv1alpha1.SessionAgentOverrides, remote Session, acknowledged string) map[string]any {
	desired := SessionMutableFields(spec)
	changes := make(map[string]any)
	for key, value := range desired {
		if key == reasoningField {
			reasoning, ok := value.(map[string]any)
			if !ok {
				changes[key] = value
				continue
			}
			effort := reasoning["effort"]
			actual, _ := remote.AgentConfiguration[reasoningField].(map[string]any)
			if effort == nil {
				if acknowledged != SessionMutableDigest(spec) {
					changes[key] = value
				}
			} else if actual["effort"] != effort {
				changes[key] = value
			}
			continue
		}
		if value == nil {
			if acknowledged != SessionMutableDigest(spec) {
				changes[key] = nil
			}
			continue
		}
		if !reflect.DeepEqual(value, remote.AgentConfiguration[key]) {
			changes[key] = value
		}
	}
	return changes
}

// CreateConfiguredSession sends complete resolved creation settings in one request.
func (c *Client) CreateConfiguredSession(ctx context.Context, uid, scope, agentID string, environment, agent map[string]any, vaultIDs []string, metadata *map[string]string, spend *agentsv1alpha1.SessionSpendControl) (Session, error) {
	return c.createConfiguredSession(ctx, uid, scope, agentID, environment, agent, vaultIDs, metadata, spend, "")
}

// CreateInitialSession atomically creates a none-mode Session with gateway-owned first input.
// Its UID claim/discovery prevents repeating POST. No idempotency header is sent:
// Go transports may otherwise replay creation after a lost response.
func (c *Client) CreateInitialSession(ctx context.Context, uid, scope, agentID string, environment, agent map[string]any, vaultIDs []string, metadata *map[string]string, spend *agentsv1alpha1.SessionSpendControl, text, key string) (Session, error) {
	if text == "" || key == "" || environment["type"] != _noneValue {
		return Session{}, errors.New("none Session requires durable initial text and key")
	}
	return c.createConfiguredSession(ctx, uid, scope, agentID, environment, agent, vaultIDs, metadata, spend, text)
}

func (c *Client) createConfiguredSession(ctx context.Context, uid, scope, agentID string, environment, agent map[string]any, vaultIDs []string, metadata *map[string]string, spend *agentsv1alpha1.SessionSpendControl, text string) (Session, error) {
	owned, err := VaultMetadata(metadata, nil, nil, uid, scope)
	if err != nil {
		return Session{}, err
	}
	fields := map[string]any{"environment": environment, metadataField: owned, "vault_ids": append([]string{}, vaultIDs...)}
	spendValue, spendPresent, err := SessionSpendControlField(spend)
	if err != nil {
		return Session{}, err
	}
	// New unlimited Sessions use the provider default without configuring its
	// optional budget feature. Existing-Session updates still send null to clear a limit.
	if spendPresent && spendValue != nil {
		fields[spendControlField] = spendValue
	}
	if agentID != "" {
		fields["agent_id"] = agentID
	}
	if len(agent) > 0 {
		fields[agentField] = agent
	} else if agentID == "" {
		fields[agentField] = map[string]any{modelField: SessionModel}
	}
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	options := agentOptions(fields)
	if text != "" {
		fields["input"] = text
		fields["stream"] = false
		options = agentOptions(fields)
	}
	result, err := c.SDK.Beta.Agents.Sessions.New(ctx, openai.BetaAgentSessionNewParams{}, options...)
	if err != nil {
		return Session{}, safeError(createOperation, err)
	}
	return summarize(result), nil
}

// UpdateSession applies only metadata and the supported settings for subsequent turns.
func (c *Client) UpdateSession(ctx context.Context, id string, fields map[string]any) (Session, error) {
	for key := range fields {
		if key != agentField && key != metadataField && key != spendControlField {
			return Session{}, errors.New("unsupported Session update field")
		}
	}
	if spend, present := fields[spendControlField]; present && spend != nil {
		limit, ok := spend.(map[string]any)
		if !ok || len(limit) != 1 {
			return Session{}, errors.New("Session spend update must contain only a limit")
		}
		cents, ok := limit[spendLimitField].(int64)
		if !ok || cents < 1 || cents > maxSessionSpendLimitCents {
			return Session{}, errors.New("Session spend limit must be within the OpenAI cent range")
		}
	}
	if agent, ok := fields[agentField].(map[string]any); ok {
		for key, value := range agent {
			if key != modelField && key != serviceTierField && key != reasoningField {
				return Session{}, errors.New("unsupported Session Agent update field")
			}
			if key == reasoningField {
				reasoning, ok := value.(map[string]any)
				if !ok || len(reasoning) != 1 {
					return Session{}, errors.New("Session reasoning update must contain effort only")
				}
				if _, present := reasoning["effort"]; !present {
					return Session{}, errors.New("Session reasoning update must contain effort")
				}
			}
		}
	}
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	result, err := c.SDK.Beta.Agents.Sessions.Update(ctx, id, openai.BetaAgentSessionUpdateParams{}, agentOptions(fields)...)
	if err != nil {
		return Session{}, safeError(updateOperation, err)
	}
	return summarize(result), nil
}
