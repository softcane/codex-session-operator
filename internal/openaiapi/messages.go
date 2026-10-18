package openaiapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	openai "github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/option"
)

const (
	_noneValue = "none"
)

// PendingAction is taken from current required_actions, never inferred from history.
type PendingAction struct {
	Type          string          `json:"type"`
	TurnID        string          `json:"turn_id,omitempty"`
	CallID        string          `json:"call_id,omitempty"`
	RequestID     string          `json:"request_id,omitempty"`
	Name          string          `json:"name,omitempty"`
	Arguments     json.RawMessage `json:"arguments,omitempty"`
	Request       json.RawMessage `json:"request,omitempty"`
	EnvironmentID string          `json:"environment_id,omitempty"`
}

// History is an authoritative saved snapshot. A live stream alone cannot recover gaps.
type History struct {
	ID       string            `json:"id"`
	Status   string            `json:"status"`
	Metadata map[string]string `json:"metadata"`
	Actions  []PendingAction   `json:"required_actions"`
	Items    []json.RawMessage `json:"items"`
	Turns    []json.RawMessage `json:"turns"`
}

// MessageStream owns its response body; Close must unblock Next.
type MessageStream interface {
	Next() bool
	Event() json.RawMessage
	Err() error
	Close() error
}

type sdkMessageStream struct {
	next    func() bool
	event   func() json.RawMessage
	err     func() error
	closeFn func() error
}

var _ MessageStream = (*sdkMessageStream)(nil)

func (s *sdkMessageStream) Next() bool             { return s.next() }
func (s *sdkMessageStream) Event() json.RawMessage { return s.event() }
func (s *sdkMessageStream) Err() error             { return s.err() }
func (s *sdkMessageStream) Close() error           { return s.closeFn() }

// SubmitInput reuses the durable key and exact event body, including after lost acceptance.
func (c *Client) SubmitInput(ctx context.Context, id, key string, event json.RawMessage) error {
	if id == "" || key == "" || !json.Valid(event) {
		return errors.New("Session, submission key and valid event are required")
	}
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	err := c.SDK.Beta.Agents.Sessions.Events.New(ctx, id,
		openai.BetaAgentSessionEventNewParams{IdempotencyKey: openai.String(key)},
		option.WithJSONSet("events", []json.RawMessage{event}), option.WithMaxRetries(0))
	if err != nil {
		return safeError("submit input", err)
	}
	return nil
}

// SavedHistory fails closed when the bounded pagination scan is incomplete.
func (c *Client) SavedHistory(ctx context.Context, id string) (History, error) {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	session, err := c.SDK.Beta.Agents.Sessions.Get(ctx, id, option.WithMaxRetries(0))
	if err != nil {
		return History{}, safeError("read history", err)
	}
	if session == nil {
		return History{}, errors.New("missing Session state")
	}
	var result History
	if err := json.Unmarshal([]byte(session.RawJSON()), &result); err != nil {
		return History{}, errors.New("invalid Session state")
	}
	items, err := c.SDK.Beta.Agents.Sessions.Items.List(ctx, id,
		openai.BetaAgentSessionItemListParams{Order: "asc", Limit: openai.Int(100)}, option.WithMaxRetries(0))
	for n := 0; ; n++ {
		if err != nil {
			return History{}, safeError("read items", err)
		}
		if items == nil || !items.JSON.HasMore.Valid() {
			return History{}, errors.New("incomplete items page")
		}
		for _, item := range items.Data {
			result.Items = append(result.Items, json.RawMessage(item.RawJSON()))
		}
		if !items.HasMore {
			break
		}
		if n+1 >= maxDiscoveryPages || len(items.Data) == 0 {
			return History{}, errors.New("history item limit reached")
		}
		items, err = items.GetNextPage()
	}
	turns, err := c.SDK.Beta.Agents.Sessions.Turns.List(ctx, id,
		openai.BetaAgentSessionTurnListParams{Order: "asc", Limit: openai.Int(100)}, option.WithMaxRetries(0))
	for n := 0; ; n++ {
		if err != nil {
			return History{}, safeError("read turns", err)
		}
		if turns == nil || !turns.JSON.HasMore.Valid() {
			return History{}, errors.New("incomplete turns page")
		}
		for _, turn := range turns.Data {
			result.Turns = append(result.Turns, json.RawMessage(turn.RawJSON()))
		}
		if !turns.HasMore {
			break
		}
		if n+1 >= maxDiscoveryPages || len(turns.Data) == 0 {
			return History{}, errors.New("history turn limit reached")
		}
		turns, err = turns.GetNextPage()
	}
	return result, nil
}

// OpenMessageStream establishes the subscription before callers submit work.
func (c *Client) OpenMessageStream(ctx context.Context, id string) (MessageStream, error) {
	base, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return nil, errors.New("default HTTP transport is unsupported")
	}
	transport := base.Clone()
	transport.ResponseHeaderTimeout = callTimeout
	s, cancel, err := openEventStream(
		ctx,
		c.SDK,
		id,
		eventStreamSettings{transport: transport, setupTimeout: callTimeout},
	)
	if err != nil {
		transport.CloseIdleConnections()
		return nil, safeError("subscribe", err)
	}
	return &sdkMessageStream{next: s.Next, event: func() json.RawMessage { return json.RawMessage(s.Current().RawJSON()) },
		err: func() error {
			if err := s.Err(); err != nil && !errors.Is(err, io.EOF) {
				return safeError("stream", err)
			}
			return nil
		},
		closeFn: func() error { cancel(); err := s.Close(); transport.CloseIdleConnections(); return err }}, nil
}
