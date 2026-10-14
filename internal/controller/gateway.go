package controller

import (
	"context"
	"errors"
	"maps"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	"github.com/softcane/codex-session-operator/internal/openaiapi"
)

const (
	_noneMode = "none"
	_wireType = "type"
)

// SessionGateway owns messages and controlled application tools, never CRD transcripts.
type SessionGateway interface {
	InitialInput(context.Context, string) (string, string, error)
	ValidateTools(context.Context, map[string]any) error
	Quiesce(context.Context, string, string) error
}

type investigationPolicy interface {
	ValidateInvestigation(context.Context, *agentsv1alpha1.Session, *agentsv1alpha1.SessionEnvironmentSnapshot) error
}

// InitialSessionProvider combines none-mode creation and first input in one POST.
type InitialSessionProvider interface {
	CreateInitialSession(context.Context, string, string, string, map[string]any, map[string]any, []string, *map[string]string, *agentsv1alpha1.SessionSpendControl, string, string) (openaiapi.Session, error)
}

var _ InitialSessionProvider = (*openaiapi.Client)(nil)

func (r *SessionReconciler) gatewayAgentBlock(ctx context.Context, fields map[string]any, environment *agentsv1alpha1.SessionEnvironmentSnapshot) (string, string) {
	if r.Gateway == nil {
		return agentEnvironmentBlock(fields, environment)
	}
	if err := r.Gateway.ValidateTools(ctx, fields); err != nil {
		return "GatewayHandlerRequired", "Effective functions need registered authorized gateway handlers"
	}
	tools, _ := fields["tools"].([]any)
	filtered := make([]any, 0, len(tools))
	for _, raw := range tools {
		tool, ok := raw.(map[string]any)
		if !ok {
			return invalidConfigurationReason, "Invalid Agent tool"
		}
		switch tool[_wireType] {
		case "function":
			continue
		case "computer_use":
			if environment == nil || environment.Profile.OpenAIHosted == nil || !environment.Profile.OpenAIHosted.Desktop {
				return "HostedDesktopRequired", "Computer use requires an OpenAI-hosted desktop"
			}
			continue
		default:
			filtered = append(filtered, raw)
		}
	}
	copyFields := maps.Clone(fields)
	if copyFields == nil {
		copyFields = make(map[string]any)
	}
	copyFields["tools"] = filtered
	return agentEnvironmentBlock(copyFields, environment)
}

func (r *SessionReconciler) initialGatewayInput(ctx context.Context, s *agentsv1alpha1.Session, snapshot *agentsv1alpha1.SessionEnvironmentSnapshot) (string, string, error) {
	if snapshot == nil || snapshot.Profile.Mode != _noneMode {
		return "", "", nil
	}
	if r.Gateway == nil || r.InitialSessions == nil {
		return "", "", errors.New("none mode requires a durable gateway and initial Session provider")
	}
	return r.Gateway.InitialInput(ctx, string(s.UID))
}

func (r *SessionReconciler) sessionGatewayBlock(ctx context.Context, remote openaiapi.Session, environment *agentsv1alpha1.SessionEnvironmentSnapshot) (string, string) {
	if r.Gateway == nil {
		return sessionAgentBlock(remote, environment)
	}
	return r.gatewayAgentBlock(ctx, remote.AgentConfiguration, environment)
}
