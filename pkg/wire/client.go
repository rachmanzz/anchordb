package wire

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rachmanzz/anchordb/pkg/storage"
)

// Client is a high-speed AnchorWire TCP client.
type Client struct {
	conn    net.Conn
	reader  *bufio.Reader
	writer  *bufio.Writer
	mu      sync.Mutex
	seq     atomic.Uint32
	model   string
	timeout time.Duration
}

// ClientConfig holds wire connection options.
type ClientConfig struct {
	Token   string
	Model   string
	Timeout time.Duration
}

// ClientOption configures a wire Client.
type ClientOption func(*ClientConfig)

// WithClientToken sets the authentication token.
func WithClientToken(token string) ClientOption {
	return func(c *ClientConfig) {
		c.Token = token
	}
}

// WithClientModel sets the initial active model.
func WithClientModel(model string) ClientOption {
	return func(c *ClientConfig) {
		c.Model = model
	}
}

// WithClientTimeout sets the network read/write deadline timeout.
func WithClientTimeout(d time.Duration) ClientOption {
	return func(c *ClientConfig) {
		c.Timeout = d
	}
}

// Connect opens a TCP connection to an AnchorWire daemon.
func Connect(addr string, opts ...ClientOption) (*Client, error) {
	cfg := ClientConfig{
		Model:   "default",
		Timeout: 30 * time.Second,
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	dialer := net.Dialer{Timeout: cfg.Timeout}
	conn, err := dialer.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to dial wire server at %s: %w", addr, err)
	}

	c := &Client{
		conn:    conn,
		reader:  bufio.NewReader(conn),
		writer:  bufio.NewWriter(conn),
		model:   cfg.Model,
		timeout: cfg.Timeout,
	}

	if cfg.Token != "" {
		if err := c.Auth(cfg.Token); err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("wire auth failed: %w", err)
		}
	}

	if cfg.Model != "" && cfg.Model != "default" {
		if err := c.SelectModel(cfg.Model); err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("wire select model failed: %w", err)
		}
	}

	return c, nil
}

func (c *Client) call(opCode byte, payload []byte) (Frame, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn == nil {
		return Frame{}, errors.New("wire client closed")
	}

	seq := c.seq.Add(1)
	req := Frame{
		Magic:   Magic,
		Version: Version,
		OpCode:  opCode,
		Seq:     seq,
		Payload: payload,
	}

	if c.timeout > 0 {
		_ = c.conn.SetDeadline(time.Now().Add(c.timeout))
	}

	if err := WriteFrame(c.writer, req); err != nil {
		return Frame{}, fmt.Errorf("failed to send frame: %w", err)
	}
	if err := c.writer.Flush(); err != nil {
		return Frame{}, fmt.Errorf("failed to flush frame: %w", err)
	}

	resp, err := ReadFrame(c.reader)
	if err != nil {
		return Frame{}, fmt.Errorf("failed to read response frame: %w", err)
	}

	if resp.OpCode == OpResponseError {
		code, msg, _ := DecodeErrorPayload(resp.Payload)
		return resp, fmt.Errorf("wire error (code %d): %s", code, msg)
	}

	return resp, nil
}

// Ping sends a heartbeat and waits for PONG response.
func (c *Client) Ping() error {
	_, err := c.call(OpPing, []byte("PING"))
	return err
}

// Auth authenticates this client connection with the given bearer token.
func (c *Client) Auth(token string) error {
	_, err := c.call(OpAuth, EncodeAuthPayload(token))
	return err
}

// ModelName returns the currently active model name.
func (c *Client) ModelName() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.model
}

// SelectModel changes the active model on this connection.
func (c *Client) SelectModel(model string) error {
	_, err := c.call(OpSelectModel, EncodeSelectModelPayload(model))
	if err == nil {
		c.model = model
	}
	return err
}

// Save stores an entity record and returns its effective name.
func (c *Client) Save(item storage.BatchRecordItem) (string, error) {
	resp, err := c.call(OpSave, EncodeSavePayload(item))
	if err != nil {
		return "", err
	}
	return DecodeOKPayload(resp.Payload)
}

// SaveBatch stores multiple records in a single high-throughput frame.
func (c *Client) SaveBatch(records []storage.BatchRecordItem) ([]string, error) {
	resp, err := c.call(OpSaveBatch, EncodeSaveBatchPayload(records))
	if err != nil {
		return nil, err
	}
	names := make([]string, len(records))
	for i, r := range records {
		names[i] = r.Name
	}
	_ = resp
	return names, nil
}

// Get retrieves an entity's properties and operation lifecycle state.
func (c *Client) Get(name string) (map[string]float32, uint8, error) {
	resp, err := c.call(OpGet, EncodeGetPayload(name))
	if err != nil {
		return nil, 0, err
	}
	op, props, err := DecodeGetResultPayload(resp.Payload)
	return props, op, err
}

// Edge adds or removes an explicit graph edge.
func (c *Client) Edge(action, source, relation, target string, weight float32) error {
	_, err := c.call(OpEdge, EncodeEdgePayload(action, source, relation, target, weight))
	return err
}

// Causal executes a causal step traversal over graph edges.
func (c *Client) Causal(seeds []string, relation string, limitK int) ([]storage.CausalResult, error) {
	resp, err := c.call(OpCausal, EncodeCausalPayload(seeds, relation, limitK))
	if err != nil {
		return nil, err
	}
	return DecodeCausalResultPayload(resp.Payload)
}

// Sync requests a disk flush on the active model.
func (c *Client) Sync() error {
	_, err := c.call(OpSync, nil)
	return err
}

// Close closes the underlying TCP connection.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		err := c.conn.Close()
		c.conn = nil
		return err
	}
	return nil
}
