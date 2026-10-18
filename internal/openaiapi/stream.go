package openaiapi

import (
	"context"
	"net/http"
	"time"

	openai "github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/option"
	"github.com/openai/openai-go/v3/packages/ssestream"
)

type eventStreamSettings struct {
	transport    *http.Transport
	setupTimeout time.Duration
}

// openEventStream bounds the entire handshake, including an HTTP error body.
// Successful streams keep their lifetime context after the setup timer stops.
func openEventStream(
	ctx context.Context,
	sdk openai.Client,
	id string,
	settings eventStreamSettings,
) (*ssestream.Stream[openai.AgentSessionEventUnion], context.CancelFunc, error) {
	streamCtx, cancelCause := context.WithCancelCause(ctx)
	cancel := func() { cancelCause(context.Canceled) }
	timer := time.AfterFunc(settings.setupTimeout, func() { cancelCause(context.DeadlineExceeded) })
	stream := sdk.Beta.Agents.Sessions.Events.StreamStreaming(
		streamCtx,
		id,
		option.WithMaxRetries(0),
		option.WithRequestTimeout(0),
		option.WithHTTPClient(&http.Client{Transport: settings.transport}),
	)
	if !timer.Stop() {
		// The timer may be running concurrently with a successful response.
		cancelCause(context.DeadlineExceeded)
	}
	err := context.Cause(streamCtx)
	if err == nil {
		err = stream.Err()
	}
	if err != nil {
		cancel()
		_ = stream.Close()
		return nil, nil, err
	}
	return stream, cancel, nil
}
