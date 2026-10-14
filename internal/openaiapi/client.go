package openaiapi

import (
	"errors"
	"os"
	"time"

	openai "github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/option"
	"github.com/prometheus/client_golang/prometheus"
)

// Client wraps the OpenAI SDK.
type Client struct {
	SDK openai.Client
}

// NewClient requires the process application key and disables automatic SDK
// retries so an ambiguous mutation is resolved by its owner instead of replayed.
func NewClient() (*Client, error) {
	if err := configuredAPIKey(); err != nil {
		return nil, err
	}
	return newClient(), nil
}

// NewClientWithMetrics registers bounded HTTP-attempt metrics in the supplied
// registry. It preserves the SDK retry, deadline, and streaming configuration.
func NewClientWithMetrics(registerer prometheus.Registerer) (*Client, error) {
	if err := configuredAPIKey(); err != nil {
		return nil, err
	}
	metrics, err := newRequestMetrics(registerer)
	if err != nil {
		return nil, err
	}
	return newClient(option.WithMiddleware(metrics.middleware)), nil
}

func newClient(opts ...option.RequestOption) *Client {
	opts = append([]option.RequestOption{option.WithMaxRetries(0), option.WithRequestTimeout(20 * time.Second)}, opts...)
	return &Client{
		SDK: openai.NewClient(opts...),
	}
}

func configuredAPIKey() error {
	if os.Getenv("OPENAI_API_KEY") == "" {
		return errors.New("OPENAI_API_KEY is not set")
	}
	return nil
}
