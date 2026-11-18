package driver

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
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
	adaptivePacing    bool
	targetRecords     uint32
	poolSize          int
	configErr         error
}

func (c *clientConfig) addErr(err error) {
	c.configErr = errors.Join(c.configErr, err)
}

func resolveClientConfig(opts []Option) (clientConfig, error) {
	cfg := defaultClientConfig()
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		opt(&cfg)
	}
	return cfg, cfg.configErr
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
		if strings.TrimSpace(model) == "" {
			c.addErr(errors.New("driver: model name cannot be empty"))
			return
		}
		c.model = model
	}
}

// WithTimeout sets the default request timeout for remote engine calls.
func WithTimeout(d time.Duration) Option {
	return func(c *clientConfig) {
		if d <= 0 {
			c.addErr(fmt.Errorf("driver: timeout %v must be > 0", d))
			return
		}
		c.timeout = d
	}
}

// WithHTTPClient overrides the default HTTP client used for remote engine calls.
func WithHTTPClient(client *http.Client) Option {
	return func(c *clientConfig) {
		if client == nil {
			c.addErr(errors.New("driver: HTTP client cannot be nil"))
			return
		}
		c.httpClient = client
	}
}

// WithHTTPTransport sets a custom RoundTripper on the HTTP client used for remote engine calls.
func WithHTTPTransport(transport http.RoundTripper) Option {
	return func(c *clientConfig) {
		if transport == nil {
			c.addErr(errors.New("driver: HTTP transport cannot be nil"))
			return
		}
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

// WithSyncInterval configures automatic periodic sync in embedded engine mode.
func WithSyncInterval(d time.Duration) Option {
	return func(c *clientConfig) {
		if d < 0 {
			c.addErr(fmt.Errorf("driver: sync interval %v cannot be negative", d))
			return
		}
		c.syncInterval = d
	}
}

// WithWorkerConcurrency is retained for API compatibility as a no-op; embedded
// mode executes operations synchronously in-process.
//
// Deprecated: Embedded mode no longer runs a background worker pool.
func WithWorkerConcurrency(n int) Option {
	return func(c *clientConfig) {
		if n <= 0 {
			c.addErr(fmt.Errorf("driver: worker concurrency %d must be > 0", n))
			return
		}
		c.workerConcurrency = n
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

// WithAdaptivePacing enables dynamic memory headroom negotiation and progressive batch release pacing.
func WithAdaptivePacing(targetRecords int) Option {
	return func(c *clientConfig) {
		if targetRecords <= 0 {
			c.addErr(fmt.Errorf("driver: adaptive pacing targetRecords %d must be > 0", targetRecords))
			return
		}
		c.adaptivePacing = true
		c.targetRecords = uint32(targetRecords)
	}
}

// WithPoolSize sets the number of concurrent AnchorWire connections in the client connection pool.
// When poolSize > 1, concurrent worker goroutines use parallel TCP connections rather than serializing
// over a single socket, scaling ingestion throughput linearly with worker concurrency.
func WithPoolSize(n int) Option {
	return func(c *clientConfig) {
		if n <= 0 {
			c.addErr(fmt.Errorf("driver: pool size %d must be > 0", n))
			return
		}
		c.poolSize = n
	}
}
