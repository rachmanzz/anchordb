package wire

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rachmanzz/anchordb/pkg/spatial"
	"github.com/rachmanzz/anchordb/pkg/storage"
)

// Client is an AnchorWire TCP client with optional adaptive memory pacing.
type Client struct {
	conn          net.Conn
	reader        *bufio.Reader
	writer        *bufio.Writer
	mu            sync.Mutex
	seq           atomic.Uint32
	model         string
	timeout       time.Duration
	negoMu        sync.RWMutex
	negotiation   *MemoryNegotiationResponse
	pacingEnabled bool
}

// ClientConfig holds wire connection options.
type ClientConfig struct {
	Token         string
	Model         string
	Timeout       time.Duration
	TargetRecords uint32
	EnablePacing  bool
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

	if cfg.EnablePacing {
		c.pacingEnabled = true
		_, _ = c.NegotiateMemory(cfg.TargetRecords, 512, 1000)
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

	if resp.Flags&FlagMemoryPressure != 0 {
		c.negoMu.Lock()
		if c.negotiation == nil {
			c.negotiation = &MemoryNegotiationResponse{
				Status:             MemoryStatusHold,
				HostUsedPct:        85,
				AllocatedBatchSize: 500,
				ReleaseIntervalMs:  10,
			}
		} else {
			c.negotiation.Status = MemoryStatusHold
		}
		c.negoMu.Unlock()
	} else if c.negotiation != nil && c.negotiation.Status == MemoryStatusHold {
		c.negoMu.Lock()
		c.negotiation.Status = MemoryStatusGranted
		c.negotiation.AllocatedBatchSize = 1000
		c.negotiation.ReleaseIntervalMs = 0
		c.negoMu.Unlock()
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

// AuthStructured performs a structured handshake authentication with the server,
// returning an issued session key or status message if one was provided in the response.
func (c *Client) AuthStructured(req AuthRequest) (string, error) {
	resp, err := c.call(OpAuth, EncodeStructuredAuth(req))
	if err != nil {
		return "", err
	}
	if len(resp.Payload) > 0 && resp.Payload[0] == StructuredRespMagic {
		if len(resp.Payload) > 1 && resp.Payload[1] == SubCodeRotationStaged {
			_, msg, dErr := DecodeRotationStagedResponse(resp.Payload)
			return msg, dErr
		}
		return DecodeAuthResponse(resp.Payload)
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
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.model
}

// SelectModel changes the active model on this connection.
func (c *Client) SelectModel(model string) error {
	_, err := c.call(OpSelectModel, EncodeSelectModelPayload(model))
	if err == nil {
		c.mu.Lock()
		c.model = model
		c.mu.Unlock()
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

// Save stores an entity record and returns its effective name.
func (c *Client) Save(item storage.BatchRecordItem) (string, error) {
	var resp Frame
	var err error
	for attempt := 0; attempt < 5; attempt++ {
		resp, err = c.call(OpSave, EncodeSavePayload(item))
		if err == nil {
			break
		}
		if strings.Contains(err.Error(), "507") || strings.Contains(err.Error(), "backpressure") {
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

// SaveBatch stores multiple records in a single high-throughput frame, automatically pacing if enabled.
func (c *Client) SaveBatch(records []storage.BatchRecordItem) ([]string, error) {
	if len(records) == 0 {
		return nil, nil
	}

	c.negoMu.RLock()
	pacing := c.pacingEnabled
	var nego *MemoryNegotiationResponse
	if c.negotiation != nil {
		negoCopy := *c.negotiation
		nego = &negoCopy
	}
	c.negoMu.RUnlock()

	// If pacing is enabled and negotiation indicates hold status
	if pacing && nego != nil && nego.Status == MemoryStatusHold {
		// Slicing into negotiated batch size
		batchSize := int(nego.AllocatedBatchSize)
		if batchSize <= 0 {
			batchSize = 500
		}

		if len(records) > batchSize {
			var allNames []string
			for i := 0; i < len(records); i += batchSize {
				end := i + batchSize
				if end > len(records) {
					end = len(records)
				}
				chunk := records[i:end]
				var err error
				for attempt := 0; attempt < 4; attempt++ {
					_, err = c.call(OpSaveBatch, EncodeSaveBatchPayload(chunk))
					if err == nil {
						break
					}
					if strings.Contains(err.Error(), "507") || strings.Contains(err.Error(), "backpressure") {
						time.Sleep(time.Duration(20*(attempt+1)) * time.Millisecond)
						continue
					}
					return allNames, err
				}
				if err != nil {
					return allNames, err
				}
				for _, r := range chunk {
					allNames = append(allNames, r.Name)
				}

				// Progressive release delay between slices
				if nego.ReleaseIntervalMs > 0 && end < len(records) {
					time.Sleep(time.Duration(nego.ReleaseIntervalMs) * time.Millisecond)
				}
			}
			return allNames, nil
		}
	}

	// Direct send (single chunk or unpaced)
	var err error
	for attempt := 0; attempt < 4; attempt++ {
		_, err = c.call(OpSaveBatch, EncodeSaveBatchPayload(records))
		if err == nil {
			break
		}
		if strings.Contains(err.Error(), "507") || strings.Contains(err.Error(), "backpressure") {
			time.Sleep(time.Duration(20*(attempt+1)) * time.Millisecond)
			continue
		}
		return nil, err
	}
	if err != nil {
		return nil, err
	}
	names := make([]string, len(records))
	for i, r := range records {
		names[i] = r.Name
	}
	return names, nil
}

// Get retrieves an entity's properties and operation lifecycle state.
func (c *Client) Get(name string) (map[string]float32, uint8, error) {
	return c.GetWithOptions(GetRequest{Name: name})
}

// GetWithOptions retrieves an entity's properties with optional projection and computations.
func (c *Client) GetWithOptions(req GetRequest) (map[string]float32, uint8, error) {
	resp, err := c.call(OpGet, EncodeGetRequest(req))
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

// SetRelation registers or replaces a query direction vector on the active model.
func (c *Client) SetRelation(name string, direction spatial.Point2D) error {
	_, err := c.call(OpEdge, EncodeEdgePayloadWithDirection("set_relation", "", name, "", 0, &direction))
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

// List executes a structured query returning records matching filters and computations.
func (c *Client) List(req ListRequest) (ListResponse, error) {
	resp, err := c.call(OpList, EncodeListRequest(req))
	if err != nil {
		return ListResponse{}, err
	}
	return DecodeListResponse(resp.Payload)
}

// Update applies atomic mutations to records matching filter conditions.
func (c *Client) Update(req UpdateRequest) (int, error) {
	resp, err := c.call(OpUpdate, EncodeUpdateRequest(req))
	if err != nil {
		return 0, err
	}
	return DecodeUpdateResponse(resp.Payload)
}

// Graph executes a beam search step along spatial graph edges.
func (c *Client) Graph(req GraphRequest) (GraphResponsePayload, error) {
	resp, err := c.call(OpGraph, EncodeGraphRequest(req))
	if err != nil {
		return GraphResponsePayload{}, err
	}
	return DecodeGraphResponsePayload(resp.Payload)
}

// GraphChain executes a multi-hop graph chain traversal across specified relations.
func (c *Client) GraphChain(req GraphChainRequest) ([]storage.GraphQueryResult, error) {
	resp, err := c.call(OpGraphChain, EncodeGraphChainRequest(req))
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
	_, err := c.call(OpAttachSchema, EncodeAttachSchemaPayload(schemaJSON))
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
	resp, err := c.call(OpMath, EncodeMathRequest(req))
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
