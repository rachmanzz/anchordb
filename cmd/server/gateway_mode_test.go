package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/rachmanzz/anchordb/pkg/storage"
	"github.com/rachmanzz/anchordb/pkg/wire"
	"github.com/rachmanzz/anchordb/pkg/worker"
)

func createGatewayTestServer(t *testing.T) (*Server, string) {
	t.Helper()
	tmpDir, err := os.MkdirTemp("", "anchordb-gateway-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	cfg := ServerConfig{
		DataDir:        tmpDir,
		Port:           "0",
		WirePort:       "0",
		GatewayMode:    true,
		PairingTimeout: 10 * time.Second,
		WorkerConfig: worker.Config{
			Enabled:         true,
			Concurrency:     2,
			QueueSize:       50,
			ShutdownTimeout: 500 * time.Millisecond,
		},
	}

	s, err := NewServer(cfg)
	if err != nil {
		_ = os.RemoveAll(tmpDir)
		t.Fatalf("NewServer failed in gateway mode: %v", err)
	}
	return s, tmpDir
}

func TestGatewayMode_HTTPDirectAccessBlocked(t *testing.T) {
	s, dir := createGatewayTestServer(t)
	defer cleanupServer(t, s, dir)

	handler := s.Routes()

	// 1. Direct hit to /v1/save must return 403 Forbidden with gateway_mode_enforced
	savePayload := `{"name":"node_a","props":{"val":1.0}}`
	req := httptest.NewRequest("POST", "/v1/save", bytes.NewBufferString(savePayload))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusForbidden {
		t.Fatalf("expected HTTP 403 Forbidden, got %d", w.Code)
	}

	var resp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse JSON response: %v", err)
	}
	if resp["error"] != "gateway_mode_enforced" {
		t.Fatalf("expected error 'gateway_mode_enforced', got %v", resp["error"])
	}

	// 2. Direct hit to /v1/get must return 403 Forbidden
	reqGet := httptest.NewRequest("GET", "/v1/get?name=node_a", nil)
	wGet := httptest.NewRecorder()
	handler.ServeHTTP(wGet, reqGet)
	if wGet.Code != http.StatusForbidden {
		t.Fatalf("expected HTTP 403 Forbidden on /v1/get, got %d", wGet.Code)
	}

	// 3. Direct hit to /health probe must return 200 OK (liveness), but NOT leak internal models/stats
	reqHealth := httptest.NewRequest("GET", "/health", nil)
	wHealth := httptest.NewRecorder()
	handler.ServeHTTP(wHealth, reqHealth)
	if wHealth.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200 OK on /health, got %d", wHealth.Code)
	}

	var healthResp map[string]interface{}
	if err := json.Unmarshal(wHealth.Body.Bytes(), &healthResp); err != nil {
		t.Fatalf("failed to parse health response: %v", err)
	}
	if healthResp["status"] != "ok" || healthResp["engine"] != "AnchorDB" {
		t.Fatalf("unexpected health response: %+v", healthResp)
	}
	if _, leaked := healthResp["database"]; leaked {
		t.Fatalf("internal database state leaked in unauthenticated /health")
	}

	// 4. Metrics endpoint /metrics should remain available for Prometheus
	reqMetrics := httptest.NewRequest("GET", "/metrics", nil)
	wMetrics := httptest.NewRecorder()
	handler.ServeHTTP(wMetrics, reqMetrics)
	if wMetrics.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200 on /metrics, got %d", wMetrics.Code)
	}
}

func TestGatewayMode_WirePairingAndSessionIsolation(t *testing.T) {
	s, dir := createGatewayTestServer(t)
	defer cleanupServer(t, s, dir)

	// Bind dynamic wire listener
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen on wire port: %v", err)
	}
	defer ln.Close()

	go func() {
		_ = s.WireServer().Serve(ln)
	}()

	addr := ln.Addr().String()

	// 1. Direct legacy auth with static token must be rejected in Gateway Mode
	conn1, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("failed to connect to wire server: %v", err)
	}
	defer conn1.Close()

	legacyAuthFrame := wire.Frame{
		Magic:   wire.Magic,
		Version: wire.Version,
		Seq:     1,
		OpCode:  wire.OpAuth,
		Payload: wire.EncodeAuthPayload("some-static-token"),
	}
	if err := wire.WriteFrame(conn1, legacyAuthFrame); err != nil {
		t.Fatalf("failed to write legacy auth frame: %v", err)
	}

	resp1, err := wire.ReadFrame(conn1)
	if err != nil {
		t.Fatalf("failed to read response to legacy auth: %v", err)
	}
	if resp1.OpCode != wire.OpResponseError {
		t.Fatalf("expected OpResponseError for static token in gateway mode, got OpCode 0x%02X", resp1.OpCode)
	}
	errCode, errMsg, _ := wire.DecodeErrorPayload(resp1.Payload)
	if errCode != wire.ErrCodeUnauthorized {
		t.Fatalf("expected ErrCodeUnauthorized (1005), got %d (%s)", errCode, errMsg)
	}
	_ = conn1.Close()

	// 2. Structured pairing handshake from anchor-connector
	conn2, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("failed to connect connector: %v", err)
	}
	defer conn2.Close()

	pairingPayload := wire.EncodeStructuredAuth(wire.AuthRequest{
		Type:       wire.AuthTypePairing,
		NodeID:     "connector-gateway-01",
		Credential: "",
	})
	pairingFrame := wire.Frame{
		Magic:   wire.Magic,
		Version: wire.Version,
		Seq:     2,
		OpCode:  wire.OpAuth,
		Payload: pairingPayload,
	}
	if err := wire.WriteFrame(conn2, pairingFrame); err != nil {
		t.Fatalf("failed to write pairing frame: %v", err)
	}

	resp2, err := wire.ReadFrame(conn2)
	if err != nil {
		t.Fatalf("failed to read pairing response: %v", err)
	}
	if resp2.OpCode != wire.OpResponseOK {
		t.Fatalf("expected OpResponseOK for pairing, got 0x%02X", resp2.OpCode)
	}

	sessionKey, err := wire.DecodeAuthResponse(resp2.Payload)
	if err != nil {
		t.Fatalf("failed to decode session key from pairing response: %v", err)
	}
	if !strings.HasPrefix(sessionKey, "SESS.") {
		t.Fatalf("expected session key with SESS. prefix, got %q", sessionKey)
	}

	// 3. Second pairing attempt must be rejected because window auto-locks
	conn3, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer conn3.Close()

	pairingFrame2 := wire.Frame{
		Magic:   wire.Magic,
		Version: wire.Version,
		Seq:     3,
		OpCode:  wire.OpAuth,
		Payload: pairingPayload,
	}
	if err := wire.WriteFrame(conn3, pairingFrame2); err != nil {
		t.Fatalf("failed to write 2nd pairing frame: %v", err)
	}
	resp3, err := wire.ReadFrame(conn3)
	if err != nil {
		t.Fatalf("failed to read 2nd pairing response: %v", err)
	}
	if resp3.OpCode != wire.OpResponseError {
		t.Fatalf("expected OpResponseError for 2nd pairing, got 0x%02X", resp3.OpCode)
	}
	errCode2, _, _ := wire.DecodeErrorPayload(resp3.Payload)
	if errCode2 != wire.ErrCodePairingClosed {
		t.Fatalf("expected ErrCodePairingClosed (1001), got %d", errCode2)
	}

	// 4. Session Key authentication on a new connection succeeds and executes data operations
	connSession, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer connSession.Close()

	sessionAuthPayload := wire.EncodeStructuredAuth(wire.AuthRequest{
		Type:       wire.AuthTypeSession,
		NodeID:     "connector-gateway-01",
		Credential: sessionKey,
	})
	sessionAuthFrame := wire.Frame{
		Magic:   wire.Magic,
		Version: wire.Version,
		Seq:     4,
		OpCode:  wire.OpAuth,
		Payload: sessionAuthPayload,
	}
	if err := wire.WriteFrame(connSession, sessionAuthFrame); err != nil {
		t.Fatalf("failed to write session auth frame: %v", err)
	}
	respSession, err := wire.ReadFrame(connSession)
	if err != nil {
		t.Fatalf("failed to read session auth response: %v", err)
	}
	if respSession.OpCode != wire.OpResponseOK {
		t.Fatalf("expected OpResponseOK for session auth, got 0x%02X", respSession.OpCode)
	}

	// Now send OpSave on authenticated connection
	item := storage.BatchRecordItem{
		Name: "node_x",
		Props: map[string]float32{
			"cpu": 45.0,
		},
	}
	savePayloadBinary := wire.EncodeSavePayload(item)
	saveFrame := wire.Frame{
		Magic:   wire.Magic,
		Version: wire.Version,
		Seq:     5,
		OpCode:  wire.OpSave,
		Payload: savePayloadBinary,
	}
	if err := wire.WriteFrame(connSession, saveFrame); err != nil {
		t.Fatalf("failed to write save frame: %v", err)
	}
	respSave, err := wire.ReadFrame(connSession)
	if err != nil {
		t.Fatalf("failed to read save response: %v", err)
	}
	if respSave.OpCode != wire.OpResponseOK {
		t.Fatalf("expected OpResponseOK for save, got 0x%02X", respSave.OpCode)
	}
}

func TestGatewayMode_WireClient_StructuredAuth(t *testing.T) {
	s, dir := createGatewayTestServer(t)
	defer cleanupServer(t, s, dir)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen on wire port: %v", err)
	}
	defer ln.Close()

	go func() {
		_ = s.WireServer().Serve(ln)
	}()

	wireAddr := ln.Addr().String()

	// 1. Client pairs with server using AuthPairing
	client1, err := wire.Connect(wireAddr)
	if err != nil {
		t.Fatalf("failed to connect wire client: %v", err)
	}
	defer client1.Close()

	sessKey, err := client1.AuthPairing("client-connector-1")
	if err != nil {
		t.Fatalf("client.AuthPairing failed: %v", err)
	}
	if !strings.HasPrefix(sessKey, "SESS.") {
		t.Fatalf("unexpected session key: %s", sessKey)
	}

	// 2. Subsequent client authenticates with AuthSession using the issued session ticket
	client2, err := wire.Connect(wireAddr)
	if err != nil {
		t.Fatalf("failed to connect second wire client: %v", err)
	}
	defer client2.Close()

	if err := client2.AuthSession("client-connector-1", sessKey); err != nil {
		t.Fatalf("client.AuthSession failed: %v", err)
	}

	// Ping succeeds on authenticated connection
	if err := client2.Ping(); err != nil {
		t.Fatalf("client.Ping failed after session auth: %v", err)
	}
}

func TestGatewayMode_StagedPreimageRotation(t *testing.T) {
	// 1. Setup Server 1
	s1, tmpDir := createGatewayTestServer(t)

	ln1, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen on wire port: %v", err)
	}

	go func() {
		_ = s1.WireServer().Serve(ln1)
	}()

	addr1 := ln1.Addr().String()

	// Initial Enrollment: Connector registers Initial Commitment E1
	v1 := [32]byte{0x10, 0x20, 0x30, 0x40, 0x50, 0x60}
	e1 := sha256.Sum256(v1[:])
	nodeID := "connector-node-rot"

	c1, err := wire.Connect(addr1)
	if err != nil {
		t.Fatalf("failed to connect to server 1: %v", err)
	}

	sessKey1, err := c1.AuthPairingWithCommitment(nodeID, e1)
	if err != nil {
		t.Fatalf("AuthPairingWithCommitment failed: %v", err)
	}
	if !strings.HasPrefix(sessKey1, "SESS.") {
		t.Fatalf("expected valid session key from server 1, got %q", sessKey1)
	}
	if err := c1.Ping(); err != nil {
		t.Fatalf("ping failed on server 1: %v", err)
	}
	_ = c1.Close()

	// Stop Server 1 cleanly without deleting tmpDir
	if s1.WireServer() != nil {
		_ = s1.WireServer().Close()
	}
	if s1.scheduler != nil {
		s1.scheduler.Stop()
	}
	if s1.worker != nil {
		_ = s1.worker.Shutdown()
	}
	s1.mu.Lock()
	for _, db := range s1.models {
		_ = db.Close()
	}
	s1.mu.Unlock()
	_ = ln1.Close()

	// Start Server 2 pointing to the SAME data dir, but with a different session secret
	// (simulating ephemeral session secret recreation where stateless HMAC session keys become invalid)
	cfg2 := ServerConfig{
		DataDir:        tmpDir,
		Port:           "0",
		WirePort:       "0",
		GatewayMode:    true,
		PairingTimeout: 10 * time.Second,
		SessionSecret:  "new-different-session-secret-key-after-restart!",
		WorkerConfig: worker.Config{
			Enabled:         true,
			Concurrency:     2,
			QueueSize:       50,
			ShutdownTimeout: 500 * time.Millisecond,
		},
	}
	s2, err := NewServer(cfg2)
	if err != nil {
		t.Fatalf("failed to start server 2 on same data dir: %v", err)
	}
	defer cleanupServer(t, s2, tmpDir)

	ln2, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen on wire port for server 2: %v", err)
	}
	defer ln2.Close()

	go func() {
		_ = s2.WireServer().Serve(ln2)
	}()

	addr2 := ln2.Addr().String()

	// 3. Connect to Server 2
	c2, err := wire.Connect(addr2)
	if err != nil {
		t.Fatalf("failed to connect to server 2: %v", err)
	}
	defer c2.Close()

	// Verify old session ticket is REJECTED by Server 2 (because session secret changed)
	errOldSess := c2.AuthSession(nodeID, sessKey1)
	if errOldSess == nil {
		t.Fatal("expected old session key to be rejected after server restart")
	}

	// 4. Staged Reauth: Connector uses V1 and registers E2
	v2 := [32]byte{0x77, 0x88, 0x99, 0xAA, 0xBB, 0xCC}
	e2 := sha256.Sum256(v2[:])

	// Reconnect for reauth
	c3, err := wire.Connect(addr2)
	if err != nil {
		t.Fatalf("failed to connect c3: %v", err)
	}
	defer c3.Close()

	if err := c3.AuthStagedReauth(nodeID, v1, e2); err != nil {
		t.Fatalf("AuthStagedReauth with V1 failed: %v", err)
	}

	// 5. Send first operational frame (OpPing) to trigger Piggyback Pruning!
	if err := c3.Ping(); err != nil {
		t.Fatalf("ping failed after staged reauth: %v", err)
	}

	// 6. Verification: Replay of V1 MUST BE REJECTED (burned & pruned)
	cReplay, err := wire.Connect(addr2)
	if err != nil {
		t.Fatalf("failed to connect replay client: %v", err)
	}
	defer cReplay.Close()

	vFake := [32]byte{0xFE, 0xED}
	eFake := sha256.Sum256(vFake[:])
	errReplay := cReplay.AuthStagedReauth(nodeID, v1, eFake)
	if errReplay == nil {
		t.Fatal("replay of V1 must be rejected after confirmation")
	}

	// 7. Verification: Next reauth with V2 SUCCEEDS
	cNext, err := wire.Connect(addr2)
	if err != nil {
		t.Fatalf("failed to connect next client: %v", err)
	}
	defer cNext.Close()

	v3 := [32]byte{0x12, 0x34, 0x56, 0x78}
	e3 := sha256.Sum256(v3[:])

	if err := cNext.AuthStagedReauth(nodeID, v2, e3); err != nil {
		t.Fatalf("reauth with V2 failed: %v", err)
	}
	if err := cNext.Ping(); err != nil {
		t.Fatalf("ping after V2 reauth failed: %v", err)
	}
}
