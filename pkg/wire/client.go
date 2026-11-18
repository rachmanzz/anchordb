package wire

import (
	"bufio"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rachmanzz/anchordb/pkg/spatial"
	"github.com/rachmanzz/anchordb/pkg/storage"
)

// Client is an AnchorWire TCP client with optional adaptive memory pacing.
//
// A Client holds exactly one connection, and the server tracks one active
// model per connection. Sequential SelectModel calls are safe. Concurrent use
// of a single Client across different models is NOT supported: two goroutines
// selecting different models on the same Client will steer each other's
// requests to the wrong model. Use one Client (connection) per model for
// concurrent multi-model workloads; the driver pool in pkg/driver already
// follows this rule by giving each checkout exclusive ownership of its Client.
type Client struct {
	connMu             sync.Mutex
	conn               net.Conn
	reader             *bufio.Reader
	writer             *bufio.Writer
	mu                 sync.Mutex
	seq                atomic.Uint32
	modelMu            sync.RWMutex
	model              string
	timeout            time.Duration
	negoMu             sync.RWMutex
	negotiation        *MemoryNegotiationResponse
	negotiatedExplicit bool
	pacingEnabled      bool
}

// ClientConfig holds wire connection options.
type ClientConfig struct {
	Token         string
	Model         string
	Timeout       time.Duration
	TargetRecords uint32
	EnablePacing  bool
	TLSConfig     *tls.Config
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

// WithClientAdaptivePacing enables adaptive memory negotiation and pacing on the wire client.
func WithClientAdaptivePacing(targetRecords uint32) ClientOption {
	return func(c *ClientConfig) {
		c.EnablePacing = true
		c.TargetRecords = targetRecords
	}
}

// WithClientTLS configures TLS for the AnchorWire client connection.
func WithClientTLS(cfg *tls.Config) ClientOption {
	return func(c *ClientConfig) {
		if cfg != nil {
			c.TLSConfig = cfg.Clone()
		} else {
			c.TLSConfig = nil
		}
	}
}

// Connect opens a TCP (or TLS if configured) connection to an AnchorWire daemon.
func Connect(addr string, opts ...ClientOption) (*Client, error) {
	cfg := ClientConfig{
		Model:   "default",
		Timeout: 30 * time.Second,
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	dialer := net.Dialer{Timeout: cfg.Timeout}
	var conn net.Conn
	var err error
	if cfg.TLSConfig != nil {
		tlsDialer := tls.Dialer{
			NetDialer: &dialer,
			Config:    cfg.TLSConfig.Clone(),
		}
		conn, err = tlsDialer.Dial("tcp", addr)
	} else {
		conn, err = dialer.Dial("tcp", addr)
	}
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

	if cfg.EnablePacing {
		c.pacingEnabled = true
		if _, err := c.NegotiateMemory(cfg.TargetRecords, 512, 1000); err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("wire negotiate memory failed: %w", err)
		}
	}

	return c, nil
}

// WireError represents a protocol-level error response returned by the AnchorWire server.
// Unlike transport or framing errors, the underlying TCP stream remains synchronized.
type WireError struct {
	Code    uint16
	Message string
}

func (e *WireError) Error() string {
	return fmt.Sprintf("wire error (code %d): %s", e.Code, e.Message)
}

func (c *Client) call(opCode byte, payload []byte) (Frame, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.connMu.Lock()
	conn := c.conn
	c.connMu.Unlock()
	if conn == nil {
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
		_ = conn.SetDeadline(time.Now().Add(c.timeout))
	}

	if err := WriteFrame(c.writer, req); err != nil {
		_ = c.Close()
		return Frame{}, fmt.Errorf("failed to send frame: %w", err)
	}
	if err := c.writer.Flush(); err != nil {
		_ = c.Close()
		return Frame{}, fmt.Errorf("failed to flush frame: %w", err)
	}

	resp, err := ReadFrame(c.reader)
	if err != nil {
		_ = c.Close()
		return Frame{}, fmt.Errorf("failed to read response frame: %w", err)
	}
	if resp.Seq != seq {
		_ = c.Close()
		return Frame{}, fmt.Errorf("wire response sequence mismatch: got %d, want %d", resp.Seq, seq)
	}

	if resp.Flags&FlagMemoryPressure != 0 {
		c.negoMu.Lock()
		if c.negotiation == nil {
			c.negotiation = &MemoryNegotiationResponse{
				Status:             MemoryStatusHold,
				HostUsedPct:        85,
				AllocatedBatchSize: 500,
				ReleaseIntervalMs:  10,
			}
		} else if c.negotiation.Status == MemoryStatusGranted {
			// Escalate only Granted -> Hold; keep an explicit MemoryStatusThrottled budget intact.
			c.negotiation.Status = MemoryStatusHold
		}
		c.negoMu.Unlock()
	} else {
		c.negoMu.Lock()
		if c.negotiation != nil && c.negotiation.Status == MemoryStatusHold && !c.negotiatedExplicit {
			c.negotiation.Status = MemoryStatusGranted
			if c.negotiation.AllocatedBatchSize == 0 {
				c.negotiation.AllocatedBatchSize = 1000
			}
			c.negotiation.ReleaseIntervalMs = 0
		}
		c.negoMu.Unlock()
	}

	if resp.OpCode == OpResponseError {
		code, msg, decErr := DecodeErrorPayload(resp.Payload)
		if decErr != nil {
			return resp, &WireError{Code: 0, Message: fmt.Sprintf("malformed wire error payload: %v", decErr)}
		}
		return resp, &WireError{Code: code, Message: msg}
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
	payload, err := EncodeAuthPayload(token)
	if err != nil {
		return err
	}
	_, err = c.call(OpAuth, payload)
	return err
}

// AuthStructured performs a structured handshake authentication with the server,
// returning an issued session key or status message if one was provided in the response.
func (c *Client) AuthStructured(req AuthRequest) (string, error) {
	payload, err := EncodeStructuredAuth(req)
	if err != nil {
		return "", err
	}
	resp, err := c.call(OpAuth, payload)
	if err != nil {
		return "", err
	}
	if len(resp.Payload) > 0 {
		switch resp.Payload[0] {
		case StructuredRotationRespMagic:
			_, msg, dErr := DecodeRotationStagedResponse(resp.Payload)
			return msg, dErr
		case StructuredRespMagic:
			return DecodeAuthResponse(resp.Payload)
		}
	}
	return "", nil
}

// AuthPairing performs an initial pairing handshake with the server, returning a session key.
func (c *Client) AuthPairing(nodeID string) (string, error) {
	return c.AuthStructured(AuthRequest{
		Type:   AuthTypePairing,
		NodeID: nodeID,
	})
}

// AuthPairingWithCommitment performs initial pairing while registering an initial preimage commitment.
func (c *Client) AuthPairingWithCommitment(nodeID string, commitment [32]byte) (string, error) {
	return c.AuthStructured(AuthRequest{
		Type:              AuthTypePairing,
		NodeID:            nodeID,
		InitialCommitment: commitment,
		HasCommitment:     true,
	})
}

// AuthOTK performs an ephemeral One-Time Key join handshake with the server.
func (c *Client) AuthOTK(nodeID, otk string) (string, error) {
	return c.AuthStructured(AuthRequest{
		Type:       AuthTypeOTK,
		NodeID:     nodeID,
		Credential: otk,
	})
}

// AuthOTKWithCommitment performs OTK join while registering an initial preimage commitment.
func (c *Client) AuthOTKWithCommitment(nodeID, otk string, commitment [32]byte) (string, error) {
	return c.AuthStructured(AuthRequest{
		Type:              AuthTypeOTK,
		NodeID:            nodeID,
		Credential:        otk,
		InitialCommitment: commitment,
		HasCommitment:     true,
	})
}

// AuthSession authenticates using an established session key ticket.
func (c *Client) AuthSession(nodeID, sessionKey string) error {
	_, err := c.AuthStructured(AuthRequest{
		Type:       AuthTypeSession,
		NodeID:     nodeID,
		Credential: sessionKey,
	})
	return err
}

// AuthStagedReauth performs Staged Preimage Commitment Rotation re-handshake.
func (c *Client) AuthStagedReauth(nodeID string, currentValidator [32]byte, nextCommitment [32]byte) error {
	_, err := c.AuthStructured(AuthRequest{
		Type:           AuthTypeStagedReauth,
		NodeID:         nodeID,
		Validator:      currentValidator,
		NextCommitment: nextCommitment,
	})
	return err
}

// ModelName returns the currently active model name.
func (c *Client) ModelName() string {
	c.modelMu.RLock()
	defer c.modelMu.RUnlock()
	return c.model
}

// SelectModel changes the active model on this connection. It is safe for
// sequential use. Do not call it concurrently with other operations on the
// same Client for a different model; open a separate Client per model instead.
func (c *Client) SelectModel(model string) error {
	payload, err := EncodeSelectModelPayload(model)
	if err != nil {
		return err
	}
	_, err = c.call(OpSelectModel, payload)
	if err == nil {
		c.modelMu.Lock()
		c.model = model
		c.modelMu.Unlock()
	}
	return err
}

// NegotiateMemory negotiates memory budget and ingestion pacing parameters with the server.
func (c *Client) NegotiateMemory(targetRecords, estimatedRecordSizeBytes, preferredBatchSize uint32) (MemoryNegotiationResponse, error) {
	req := MemoryNegotiationRequest{
		TargetRecords:            targetRecords,
		EstimatedRecordSizeBytes: estimatedRecordSizeBytes,
		ClientBatchSize:          preferredBatchSize,
	}
	resp, err := c.call(OpNegotiateMemory, EncodeMemoryNegotiationRequest(req))
	if err != nil {
		return MemoryNegotiationResponse{}, err
	}
	nego, err := DecodeMemoryNegotiationResponse(resp.Payload)
	if err != nil {
		return MemoryNegotiationResponse{}, err
	}

	c.negoMu.Lock()
	c.negotiation = &nego
	c.negotiatedExplicit = true
	c.negoMu.Unlock()

	return nego, nil
}

// EnableAdaptivePacing turns on or off adaptive batch pacing.
func (c *Client) EnableAdaptivePacing(enabled bool) {
	c.negoMu.Lock()
	c.pacingEnabled = enabled
	c.negoMu.Unlock()
}

// GetNegotiation returns the current active memory negotiation response, if any.
func (c *Client) GetNegotiation() *MemoryNegotiationResponse {
	c.negoMu.RLock()
	defer c.negoMu.RUnlock()
	if c.negotiation == nil {
		return nil
	}
	negoCopy := *c.negotiation
	return &negoCopy
}

func isWireBackpressureError(err error) bool {
	var wErr *WireError
	return errors.As(err, &wErr) && wErr.Code == 507
}

// Save stores an entity record and returns its effective name.
func (c *Client) Save(item storage.BatchRecordItem) (string, error) {
	payload, err := EncodeSavePayload(item)
	if err != nil {
		return "", err
	}
	var resp Frame
	for attempt := 0; attempt < 5; attempt++ {
		resp, err = c.call(OpSave, payload)
		if err == nil {
			break
		}
		if isWireBackpressureError(err) {
			time.Sleep(time.Duration(15*(attempt+1)) * time.Millisecond)
			continue
		}
		return "", err
	}
	if err != nil {
		return "", err
	}
	return DecodeOKPayload(resp.Payload)
}

// SaveTyped stores an entity record with typed attributes.
func (c *Client) SaveTyped(name string, opStatus uint8, attrs []storage.AttrEntry) (string, error) {
	return c.Save(storage.BatchRecordItem{
		Name:      name,
		Operation: opStatus,
		Attrs:     attrs,
	})
}

// SaveBatch stores multiple records atomically in a single high-throughput frame, applying adaptive pre-send pacing if enabled.
func (c *Client) SaveBatch(records []storage.BatchRecordItem) ([]string, error) {
	if len(records) == 0 {
		return nil, nil
	}

	payload, err := EncodeSaveBatchPayload(records)
	if err != nil {
		return nil, err
	}

	c.negoMu.RLock()
	pacing := c.pacingEnabled
	var nego *MemoryNegotiationResponse
	if c.negotiation != nil {
		negoCopy := *c.negotiation
		nego = &negoCopy
	}
	c.negoMu.RUnlock()

	if pacing && nego != nil && nego.Status != MemoryStatusGranted && nego.ReleaseIntervalMs > 0 {
		time.Sleep(time.Duration(nego.ReleaseIntervalMs) * time.Millisecond)
	}

	var resp Frame
	for attempt := 0; attempt < 4; attempt++ {
		resp, err = c.call(OpSaveBatch, payload)
		if err == nil {
			break
		}
		if isWireBackpressureError(err) {
			time.Sleep(time.Duration(20*(attempt+1)) * time.Millisecond)
			continue
		}
		return nil, err
	}
	if err != nil {
		return nil, err
	}
	return DecodeSaveBatchResponse(resp.Payload)
}

// Get retrieves an entity's properties and operation lifecycle state.
func (c *Client) Get(name string) (map[string]float32, uint8, error) {
	return c.GetWithOptions(GetRequest{Name: name})
}

// GetWithOptions retrieves an entity's properties with optional projection and computations.
func (c *Client) GetWithOptions(req GetRequest) (map[string]float32, uint8, error) {
	payload, err := EncodeGetRequest(req)
	if err != nil {
		return nil, 0, err
	}
	resp, err := c.call(OpGet, payload)
	if err != nil {
		return nil, 0, err
	}
	op, props, err := DecodeGetResultPayload(resp.Payload)
	return props, op, err
}

// GetTyped retrieves an entity's typed attributes and operation lifecycle state.
func (c *Client) GetTyped(name string) (storage.CompactRecord, error) {
	return c.GetWithOptionsTyped(GetRequest{Name: name})
}

// GetWithOptionsTyped retrieves an entity's typed attributes with optional projection and computations.
func (c *Client) GetWithOptionsTyped(req GetRequest) (storage.CompactRecord, error) {
	payload, err := EncodeGetRequest(req)
	if err != nil {
		return storage.CompactRecord{}, err
	}
	resp, err := c.call(OpGet, payload)
	if err != nil {
		return storage.CompactRecord{}, err
	}
	op, attrs, err := DecodeGetResultTypedPayload(resp.Payload)
	if err != nil {
		return storage.CompactRecord{}, err
	}
	cr := storage.NewCompactRecord(op, attrs)
	return cr, nil
}

// Edge adds or removes an explicit graph edge.
func (c *Client) Edge(action, source, relation, target string, weight float32) error {
	payload, err := EncodeEdgePayload(action, source, relation, target, weight)
	if err != nil {
		return err
	}
	_, err = c.call(OpEdge, payload)
	return err
}

// SetRelation registers or replaces a query direction vector on the active model.
func (c *Client) SetRelation(name string, direction spatial.Point2D) error {
	payload, err := EncodeEdgePayloadWithDirection("set_relation", "", name, "", 0, &direction)
	if err != nil {
		return err
	}
	_, err = c.call(OpEdge, payload)
	return err
}

// Causal executes a causal step traversal over graph edges.
func (c *Client) Causal(seeds []string, relation string, limitK int) ([]storage.CausalResult, error) {
	payload, err := EncodeCausalPayload(seeds, relation, limitK)
	if err != nil {
		return nil, err
	}
	resp, err := c.call(OpCausal, payload)
	if err != nil {
		return nil, err
	}
	return DecodeCausalResultPayload(resp.Payload)
}

// CausalChain executes a multi-hop causal chain query in a single round-trip.
func (c *Client) CausalChain(seeds []string, relations []string, limitK int) ([]storage.CausalResult, error) {
	payload, err := EncodeCausalChainPayload(seeds, relations, limitK)
	if err != nil {
		return nil, err
	}
	resp, err := c.call(OpCausalChain, payload)
	if err != nil {
		return nil, err
	}
	return DecodeCausalResultPayload(resp.Payload)
}

// List executes a structured query returning records matching filters and computations.
func (c *Client) List(req ListRequest) (ListResponse, error) {
	payload, err := EncodeListRequest(req)
	if err != nil {
		return ListResponse{}, err
	}
	resp, err := c.call(OpList, payload)
	if err != nil {
		return ListResponse{}, err
	}
	return DecodeListResponse(resp.Payload)
}

// Update applies atomic mutations to records matching filter conditions.
func (c *Client) Update(req UpdateRequest) (int, error) {
	payload, err := EncodeUpdateRequest(req)
	if err != nil {
		return 0, err
	}
	resp, err := c.call(OpUpdate, payload)
	if err != nil {
		return 0, err
	}
	return DecodeUpdateResponse(resp.Payload)
}

// Graph executes a beam search step along spatial graph edges.
func (c *Client) Graph(req GraphRequest) (GraphResponsePayload, error) {
	payload, err := EncodeGraphRequest(req)
	if err != nil {
		return GraphResponsePayload{}, err
	}
	resp, err := c.call(OpGraph, payload)
	if err != nil {
		return GraphResponsePayload{}, err
	}
	return DecodeGraphResponsePayload(resp.Payload)
}

// GraphChain executes a multi-hop graph chain traversal across specified relations.
func (c *Client) GraphChain(req GraphChainRequest) ([]storage.GraphQueryResult, error) {
	reqPayload, err := EncodeGraphChainRequest(req)
	if err != nil {
		return nil, err
	}
	resp, err := c.call(OpGraphChain, reqPayload)
	if err != nil {
		return nil, err
	}
	payload, err := DecodeGraphResponsePayload(resp.Payload)
	if err != nil {
		return nil, err
	}
	return payload.Results, nil
}

// AttachSchema registers a fixed schema definition for the active model.
func (c *Client) AttachSchema(schemaJSON string) error {
	payload, err := EncodeAttachSchemaPayload(schemaJSON)
	if err != nil {
		return err
	}
	_, err = c.call(OpAttachSchema, payload)
	return err
}

// GetSchema returns the attached schema definition JSON for the active model.
func (c *Client) GetSchema() (string, error) {
	resp, err := c.call(OpGetSchema, nil)
	if err != nil {
		return "", err
	}
	return DecodeGetSchemaPayload(resp.Payload)
}

// ResetModel resets schema locks on the active model.
func (c *Client) ResetModel(confirm bool) error {
	_, err := c.call(OpResetModel, EncodeResetModelPayload(confirm))
	return err
}

// Math performs in-engine geometric or spatial math operations (distance, centroid, cosine_similarity, nearest).
func (c *Client) Math(req MathRequest) (MathResponse, error) {
	payload, err := EncodeMathRequest(req)
	if err != nil {
		return MathResponse{}, err
	}
	resp, err := c.call(OpMath, payload)
	if err != nil {
		return MathResponse{}, err
	}
	return DecodeMathResponse(resp.Payload)
}

// Sync requests a disk flush on the active model.
func (c *Client) Sync() error {
	_, err := c.call(OpSync, nil)
	return err
}

// PartitionCatalog retrieves registered partition archives for a model (or all models) over AnchorWire.
func (c *Client) PartitionCatalog(model string) ([]PartitionCatalogItemDTO, error) {
	resp, err := c.call(OpPartitionCatalog, EncodePartitionCatalogRequest(model))
	if err != nil {
		return nil, err
	}
	return DecodePartitionCatalogResponse(resp.Payload)
}

// PartitionInspect inspects an archived partition file over AnchorWire, returning specific record properties or a record list.
func (c *Client) PartitionInspect(req PartitionInspectWireRequest) (PartitionInspectWireResponse, error) {
	payload, err := EncodePartitionInspectRequest(req)
	if err != nil {
		return PartitionInspectWireResponse{}, err
	}
	resp, err := c.call(OpPartitionInspect, payload)
	if err != nil {
		return PartitionInspectWireResponse{}, err
	}
	return DecodePartitionInspectResponse(resp.Payload)
}

// Close closes the underlying TCP connection immediately so any in-flight
// ReadFrame unblocks without waiting for the request timeout.
func (c *Client) Close() error {
	c.connMu.Lock()
	conn := c.conn
	c.conn = nil
	c.connMu.Unlock()
	if conn != nil {
		return conn.Close()
	}
	return nil
}
