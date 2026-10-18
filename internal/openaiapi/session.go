package openaiapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	openai "github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/option"
)

const (
	// SessionModel is fixed for the first remote-only slice.
	SessionModel      = "gpt-6-luna"
	selfHostedType    = "self_hosted"
	ownerKey          = "kubernetes_uid"
	operatorKey       = "operator"
	operatorValue     = "codex-session-operator"
	callTimeout       = 20 * time.Second
	maxDiscoveryPages = 10
)

// ErrNotFound indicates an absent remote resource, not a missing Kubernetes CR.
var ErrNotFound = errors.New("OpenAI resource not found")

// ErrSpendControlUnavailable is the known project capability rejection. The
// provider does not expose the raw API error body or request through this error.
var ErrSpendControlUnavailable = errors.New("OpenAI Session spending limits are not enabled for this project")

// Session contains only the fields needed for lifecycle reconciliation.
// No provider error text or credentials are copied into this value.
type Session struct {
	ID, EnvironmentID, RemoteURL, WorkspaceDirectory, Model, OwnerUID, Status string
	AgentID                                                                   string
	AgentConfigurationDigest                                                  string
	AgentBlockReason, AgentBlockMessage                                       string
	SelfHosted                                                                bool
	EnvironmentType                                                           string
	EnvironmentConfigurationDigest                                            string
	CapabilityDirectories                                                     []string
	AgentConfiguration                                                        map[string]any
	Metadata                                                                  map[string]string
	VaultIDs                                                                  []string
	OwnerScope                                                                string
	SpendLimitCents, SpendConsumedCents                                       *int64
	SpendControlObserved                                                      bool
	SpendControlMalformed                                                     bool
}

// GetSession re-observes remote state. It never provisions a replacement.
func (c *Client) GetSession(ctx context.Context, id string) (Session, error) {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	remote, err := c.SDK.Beta.Agents.Sessions.Get(ctx, id, option.WithMaxRetries(0))
	if err != nil {
		return Session{}, safeError("retrieve", err)
	}
	return summarize(remote), nil
}

// FindSession discovers by exact UID metadata. Absence is not permission to create.
// A bounded scan must complete before returning a unique match; duplicate matches
// or an incomplete scan require an operator audit rather than unsafe adoption.
func (c *Client) FindSession(ctx context.Context, uid string) (Session, error) {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	page, err := c.SDK.Beta.Agents.Sessions.List(ctx,
		openai.BetaAgentSessionListParams{Limit: openai.Int(100)}, option.WithMaxRetries(0))
	var found Session
	for n := 0; page != nil && n < maxDiscoveryPages; n++ {
		if err != nil {
			return Session{}, safeError("discover", err)
		}
		for i := range page.Data {
			remote := summarize(&page.Data[i])
			if remote.OwnerUID != uid {
				continue
			}
			if found.ID != "" {
				return Session{}, errors.New("multiple OpenAI Sessions match Kubernetes UID; audit required")
			}
			found = remote
		}
		if !page.JSON.HasMore.Valid() {
			return Session{}, errors.New("OpenAI Session discovery pagination is incomplete")
		}
		if !page.HasMore {
			return found, nil
		}
		if len(page.Data) == 0 || n+1 == maxDiscoveryPages {
			return Session{}, errors.New("OpenAI Session discovery limit reached; audit required")
		}
		page, err = page.GetNextPage()
	}
	if err != nil {
		return Session{}, safeError("discover", err)
	}
	return Session{}, errors.New("OpenAI Session discovery returned no page")
}

// DeleteSession accepts an already-absent Session and checks deletion confirmation.
// The provider may continue physical cleanup asynchronously after confirmation.
func (c *Client) DeleteSession(ctx context.Context, id string) error {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	deleted, err := c.SDK.Beta.Agents.Sessions.Delete(ctx, id, option.WithMaxRetries(0))
	if err != nil {
		clean := safeError(deleteOperation, err)
		if errors.Is(clean, ErrNotFound) {
			return nil
		}
		return clean
	}
	if deleted == nil || !deleted.Deleted || deleted.ID != id {
		return errors.New("OpenAI Session deletion was not confirmed")
	}
	return nil
}

// ValidateConnection prevents credentials in a returned URL from reaching status.
// The documented remote URL is a public endpoint, passed unchanged to the executor.
func (s Session) ValidateConnection() error {
	u, err := url.Parse(s.RemoteURL)
	if s.ID == "" || s.EnvironmentID == "" || !s.SelfHosted || err != nil ||
		u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return errors.New("OpenAI Session returned invalid connection details")
	}
	return nil
}

func summarize(remote *openai.AgentSession) Session {
	if remote == nil {
		return Session{}
	}
	owner := ""
	if remote.Metadata[operatorKey] == operatorValue {
		owner = remote.Metadata[ownerKey]
	}
	var configuration map[string]any
	_ = json.Unmarshal([]byte(remote.Agent.RawJSON()), &configuration)
	var environment map[string]any
	_ = json.Unmarshal([]byte(remote.Environment.RawJSON()), &environment)
	var rawSession map[string]json.RawMessage
	_ = json.Unmarshal([]byte(remote.RawJSON()), &rawSession)
	spendingRaw, spendingObserved := rawSession[spendControlField]
	var spending *struct {
		Limit    *int64 `json:"limit"`
		Consumed *int64 `json:"consumed"`
	}
	spendMalformed := false
	if spendingObserved && !bytes.Equal(bytes.TrimSpace(spendingRaw), []byte("null")) {
		if err := json.Unmarshal(spendingRaw, &spending); err != nil || spending == nil || spending.Limit == nil ||
			*spending.Limit < 1 || *spending.Limit > maxSessionSpendLimitCents ||
			(spending.Consumed != nil && *spending.Consumed < 0) {
			spendMalformed = true
		}
	}
	var spendLimit, spendConsumed *int64
	if spending != nil {
		spendLimit = spending.Limit
		spendConsumed = spending.Consumed
	}
	reason, message := AgentSessionBlock(configuration)
	return Session{AgentConfigurationDigest: AgentConfigurationDigest(configuration), AgentBlockReason: reason, AgentBlockMessage: message, ID: remote.ID, EnvironmentID: remote.Environment.ID,
		RemoteURL: remote.Environment.RemoteURL, WorkspaceDirectory: remote.Environment.WorkspaceDirectory,
		Model: remote.Agent.Model, AgentID: remote.Agent.ID, OwnerUID: owner, Status: string(remote.Status),
		SelfHosted: remote.Environment.Type == selfHostedType, EnvironmentType: remote.Environment.Type,
		EnvironmentConfigurationDigest: ObservableEnvironmentDigest(environment), CapabilityDirectories: append([]string(nil), remote.Environment.CapabilityDirectories...),
		AgentConfiguration: configuration, Metadata: maps.Clone(remote.Metadata), VaultIDs: slices.Clone(remote.VaultIDs), OwnerScope: remote.Metadata[agentScopeKey],
		SpendLimitCents: spendLimit, SpendConsumedCents: spendConsumed, SpendControlObserved: spendingObserved,
		SpendControlMalformed: spendMalformed}
}

// SDK errors can embed request headers and response bodies. Never return them to
// controller-runtime, which logs reconcile errors. Preserve only safe classification.
func safeError(operation string, err error) error {
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	if apiErr, ok := errors.AsType[*openai.Error](err); ok {
		if apiErr.StatusCode == http.StatusNotFound && !strings.HasPrefix(operation, createOperation) {
			return ErrNotFound
		}
		if apiErr.StatusCode == http.StatusBadRequest && apiErr.Param == spendControlField &&
			apiErr.Message == "Session budget configuration is not enabled" {
			return ErrSpendControlUnavailable
		}
		return fmt.Errorf("OpenAI Session %s failed (HTTP %d)", operation, apiErr.StatusCode)
	}
	return fmt.Errorf("OpenAI Session %s failed (transport or response error)", operation)
}
