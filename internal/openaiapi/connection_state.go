package openaiapi

import (
	"context"
	"errors"

	"github.com/openai/openai-go/v3/option"
)

// HistoryEnvironment contains current connection state without protected settings or URLs.
type HistoryEnvironment struct {
	ID     string `json:"id"`
	Type   string `json:"type"`
	Status string `json:"status"`
}

// ConnectionState reads the first-class runtime endpoint; Session GET does not
// include environment connection status in the pinned provider contract.
func (c *Client) ConnectionState(ctx context.Context, environmentID string) (HistoryEnvironment, error) {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	environment, err := c.SDK.Beta.Agents.Environments.Get(ctx, environmentID, option.WithMaxRetries(0))
	if err != nil {
		return HistoryEnvironment{}, safeEnvironmentError("read connection", err)
	}
	if environment == nil || environment.ID != environmentID || !environment.JSON.Status.Valid() || !environment.JSON.Type.Valid() {
		return HistoryEnvironment{}, errors.New("connection observation returned incomplete environment identity or state")
	}
	return HistoryEnvironment{ID: environment.ID, Type: string(environment.Type), Status: string(environment.Status)}, nil
}
