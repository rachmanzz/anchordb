package driver

import (
	"net/http"
	"time"
)

// Option configures an AnchorDB Client.
type Option func(*clientConfig)

type clientConfig struct {
	token             string
	model             string
	timeout           time.Duration
	httpClient        *http.Client
	syncInterval      time.Duration
	workerConcurrency int
	enableScheduler   bool
	schema            Schema
}

func defaultClientConfig() clientConfig {
	return clientConfig{
		model:             "default",
		timeout:           30 * time.Second,
		workerConcurrency: 2,
		enableScheduler:   false,
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

// WithHTTPTransport sets a custom RoundTripper on the HTTP client used for remote engine calls.
func WithHTTPTransport(transport http.RoundTripper) Option {
	return func(c *clientConfig) {
		if transport != nil {
			if c.httpClient == nil {
				c.httpClient = &http.Client{
					Transport: transport,
					Timeout:   c.timeout,
				}
			} else {
				c.httpClient.Transport = transport
			}
		}
	}
}

// WithSyncInterval configures automatic periodic sync in embedded engine mode.
func WithSyncInterval(d time.Duration) Option {
	return func(c *clientConfig) {
		c.syncInterval = d
	}
}

// WithWorkerConcurrency sets the concurrency limit for the embedded worker pool.
func WithWorkerConcurrency(n int) Option {
	return func(c *clientConfig) {
		if n > 0 {
			c.workerConcurrency = n
		}
	}
}

// WithScheduler enables the background task scheduler in embedded engine mode.
func WithScheduler(enable bool) Option {
	return func(c *clientConfig) {
		c.enableScheduler = enable
	}
}

// WithAttachSchema specifies a schema that must be attached and verified against the model contract.
func WithAttachSchema(schema Schema) Option {
	return func(c *clientConfig) {
		c.schema = schema
	}
}

// WithSchema is an alias for WithAttachSchema.
func WithSchema(schema Schema) Option {
	return WithAttachSchema(schema)
}
