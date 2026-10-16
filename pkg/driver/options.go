package driver

import (
	"net/http"
	"time"
)

// Option configures an AnchorDB Client.
type Option func(*clientConfig)

type clientConfig struct {
	token      string
	model      string
	timeout    time.Duration
	httpClient *http.Client
}

func defaultClientConfig() clientConfig {
	return clientConfig{
		model:   "default",
		timeout: 30 * time.Second,
	}
}

// WithToken sets the Bearer authorization token for remote engine requests.
func WithToken(token string) Option {
	return func(c *clientConfig) {
		c.token = token
	}
}

// WithModel sets the target model database name (default: "default").
func WithModel(model string) Option {
	return func(c *clientConfig) {
		if model != "" {
			c.model = model
		}
	}
}

// WithTimeout sets the default request timeout for remote engine calls.
func WithTimeout(d time.Duration) Option {
	return func(c *clientConfig) {
		if d > 0 {
			c.timeout = d
		}
	}
}

// WithHTTPClient overrides the default HTTP client used for remote engine calls.
func WithHTTPClient(client *http.Client) Option {
	return func(c *clientConfig) {
		if client != nil {
			c.httpClient = client
		}
	}
}
