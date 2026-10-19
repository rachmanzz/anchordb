package wire

import (
	"strings"
	"testing"
	"time"
)

type mockAddr struct {
	ipStr string
}

func (m mockAddr) Network() string { return "tcp" }
func (m mockAddr) String() string  { return m.ipStr }

func TestJoinManager_PairingWindowAndLock(t *testing.T) {
	jm := NewJoinManager(JoinConfig{
		GatewayMode:    true,
		PairingTimeout: 2 * time.Second,
		ClusterID:      "test-cluster",
	})

	if !jm.IsPairingActive() {
		t.Fatal("expected pairing window to be active initially")
	}

	remoteAddr := mockAddr{ipStr: "192.168.1.50:54321"}
	req := AuthRequest{
		Type:       AuthTypePairing,
		NodeID:     "connector-1",
		Credential: "",
	}

	// 1. Initial pairing succeeds and returns valid SessionKey
	sessKey, errCode, err := jm.HandleStructuredAuth(req, remoteAddr)
	if err != nil {
		t.Fatalf("first pairing failed: %v", err)
	}
	if errCode != 0 {
		t.Fatalf("expected errCode 0, got %d", errCode)
	}
	if !strings.HasPrefix(sessKey, "SESS.test-cluster.connector-1.") {
		t.Fatalf("unexpected session key format: %s", sessKey)
	}

	// Pairing should be locked after the first successful handshake
	if jm.IsPairingActive() {
		t.Fatal("expected pairing window to be locked after successful handshake")
	}

	// 2. Second pairing attempt must fail with ErrCodePairingClosed (1001)
	_, errCode2, err2 := jm.HandleStructuredAuth(req, remoteAddr)
	if err2 == nil || errCode2 != ErrCodePairingClosed {
		t.Fatalf("expected ErrCodePairingClosed (%d), got errCode: %d, err: %v", ErrCodePairingClosed, errCode2, err2)
	}
}

func TestJoinManager_PairingAllowedCIDR(t *testing.T) {
	jm := NewJoinManager(JoinConfig{
		GatewayMode:        true,
		PairingAllowedCIDR: "10.0.0.0/8, 192.168.1.0/24",
		ClusterID:          "test-cluster",
	})

	req := AuthRequest{
		Type:   AuthTypePairing,
		NodeID: "connector-test",
	}

	// Rejected IP
	badAddr := mockAddr{ipStr: "172.16.0.5:1234"}
	_, errCode, err := jm.HandleStructuredAuth(req, badAddr)
	if err == nil || errCode != ErrCodeIPNotAllowed {
		t.Fatalf("expected ErrCodeIPNotAllowed (%d), got errCode: %d, err: %v", ErrCodeIPNotAllowed, errCode, err)
	}

	// Allowed IP
	goodAddr := mockAddr{ipStr: "192.168.1.25:1234"}
	sessKey, errCodeGood, errGood := jm.HandleStructuredAuth(req, goodAddr)
	if errGood != nil || errCodeGood != 0 {
		t.Fatalf("expected pairing to succeed for allowed CIDR, got: %v", errGood)
	}
	if !strings.HasPrefix(sessKey, "SESS.") {
		t.Fatalf("invalid session key format: %s", sessKey)
	}
}

func TestJoinManager_OTK_Lifecycle(t *testing.T) {
	jm := NewJoinManager(JoinConfig{
		GatewayMode: true,
		ClusterID:   "cluster-otk",
	})

	token, err := jm.GenerateOTK("CONNECTOR", 1, 5*time.Minute)
	if err != nil {
		t.Fatalf("failed to generate OTK: %v", err)
	}

	req := AuthRequest{
		Type:       AuthTypeOTK,
		NodeID:     "connector-otk-node",
		Credential: token,
	}

	// 1. Consume OTK
	sessKey, errCode, err := jm.HandleStructuredAuth(req, mockAddr{ipStr: "127.0.0.1:9999"})
	if err != nil || errCode != 0 {
		t.Fatalf("OTK auth failed: %v", err)
	}
	if !strings.HasPrefix(sessKey, "SESS.cluster-otk.connector-otk-node.") {
		t.Fatalf("unexpected session key: %s", sessKey)
	}

	// 2. Reusing burned OTK must fail
	_, errCode2, err2 := jm.HandleStructuredAuth(req, mockAddr{ipStr: "127.0.0.1:9999"})
	if err2 == nil || errCode2 != ErrCodeOTKExpiredOrBurned {
		t.Fatalf("expected ErrCodeOTKExpiredOrBurned, got code: %d, err: %v", errCode2, err2)
	}

	// 3. Forged OTK must fail
	forgedReq := AuthRequest{
		Type:       AuthTypeOTK,
		NodeID:     "forged-node",
		Credential: "ANCH.JOIN.CONNECTOR.cluster-otk.1.fake12345678.9999",
	}
	_, errCode3, err3 := jm.HandleStructuredAuth(forgedReq, mockAddr{ipStr: "127.0.0.1:9999"})
	if err3 == nil || errCode3 != ErrCodeOTKExpiredOrBurned {
		t.Fatalf("expected ErrCodeOTKExpiredOrBurned for fake token, got code: %d, err: %v", errCode3, err3)
	}
}

func TestJoinManager_SessionKeyValidation(t *testing.T) {
	jm := NewJoinManager(JoinConfig{
		GatewayMode:   true,
		ClusterID:     "sess-cluster",
		SessionSecret: "super-secure-session-secret-32-chars!",
	})

	sessKey := jm.GenerateSessionKey("node-sess-1")
	nodeID, err := jm.ValidateSessionKey(sessKey)
	if err != nil {
		t.Fatalf("failed to validate valid session key: %v", err)
	}
	if nodeID != "node-sess-1" {
		t.Fatalf("expected nodeID 'node-sess-1', got %q", nodeID)
	}

	// HandleStructuredAuth with AuthTypeSession
	req := AuthRequest{
		Type:       AuthTypeSession,
		NodeID:     "node-sess-1",
		Credential: sessKey,
	}
	retKey, code, err := jm.HandleStructuredAuth(req, nil)
	if err != nil || code != 0 {
		t.Fatalf("session auth failed: %v", err)
	}
	if retKey != sessKey {
		t.Fatalf("expected returned session key %s, got %s", sessKey, retKey)
	}

	// Tampered session key
	tampered := sessKey[:len(sessKey)-2] + "ff"
	_, tamperedCode, tamperedErr := jm.HandleStructuredAuth(AuthRequest{
		Type:       AuthTypeSession,
		Credential: tampered,
	}, nil)
	if tamperedErr == nil || tamperedCode != ErrCodeInvalidSession {
		t.Fatalf("expected ErrCodeInvalidSession (%d), got code: %d, err: %v", ErrCodeInvalidSession, tamperedCode, tamperedErr)
	}
}

func TestJoinManager_GatewayMode_RejectsStatic(t *testing.T) {
	// In gateway mode, direct static tokens are disallowed
	jm := NewJoinManager(JoinConfig{
		GatewayMode: true,
		AuthToken:   "my-static-token",
	})

	req := AuthRequest{
		Type:       AuthTypeStatic,
		Credential: "my-static-token",
	}

	_, errCode, err := jm.HandleStructuredAuth(req, nil)
	if err == nil || errCode != ErrCodeUnauthorized {
		t.Fatalf("expected ErrCodeUnauthorized in gateway mode, got code: %d, err: %v", errCode, err)
	}

	// In non-gateway mode, static tokens succeed if valid
	jmNormal := NewJoinManager(JoinConfig{
		GatewayMode: false,
		AuthToken:   "my-static-token",
	})

	_, errCodeNormal, errNormal := jmNormal.HandleStructuredAuth(req, nil)
	if errNormal != nil || errCodeNormal != 0 {
		t.Fatalf("expected static token to succeed in non-gateway mode, got: %v", errNormal)
	}
}

func TestJoinManager_PruneExpiredTokens(t *testing.T) {
	jm := NewJoinManager(JoinConfig{
		GatewayMode: true,
		ClusterID:   "cluster-prune",
	})

	// 1. Create a short-lived token
	token1, err := jm.GenerateOTK("CONNECTOR", 1, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("failed to generate token1: %v", err)
	}

	// 2. Create another token that we will burn
	token2, err := jm.GenerateOTK("CONNECTOR", 2, 10*time.Minute)
	if err != nil {
		t.Fatalf("failed to generate token2: %v", err)
	}
	_, _, _, err = jm.ValidateAndBurnOTK(token2)
	if err != nil {
		t.Fatalf("failed to burn token2: %v", err)
	}

	// 3. Create a valid long-lived token
	token3, err := jm.GenerateOTK("CONNECTOR", 3, 10*time.Minute)
	if err != nil {
		t.Fatalf("failed to generate token3: %v", err)
	}

	// Wait for token1 to expire
	time.Sleep(60 * time.Millisecond)

	// Prune now: token1 should be pruned (expired). token2 is burned but not yet > 10m old. token3 is valid.
	pruned := jm.PruneExpiredTokens(time.Now())
	if pruned != 1 {
		t.Errorf("expected 1 token pruned, got %d", pruned)
	}

	// Verify token1 is gone
	_, _, _, err1 := jm.ValidateAndBurnOTK(token1)
	if err1 != ErrOTKExpiredOrBurned {
		t.Errorf("expected ErrOTKExpiredOrBurned for pruned token1, got %v", err1)
	}

	// Verify token3 still exists and can be consumed
	_, _, _, err3 := jm.ValidateAndBurnOTK(token3)
	if err3 != nil {
		t.Errorf("expected token3 to still be valid, got %v", err3)
	}

	// Fast-forward simulated time to prune stale burned token2
	prunedStale := jm.PruneExpiredTokens(time.Now().Add(15 * time.Minute))
	if prunedStale < 1 {
		t.Errorf("expected at least 1 stale burned token pruned, got %d", prunedStale)
	}
}

func TestJoinManager_HMACSignatureLength(t *testing.T) {
	jm := NewJoinManager(JoinConfig{
		GatewayMode: true,
		ClusterID:   "cluster-hmac",
	})

	// OTK signature should be 32 hex chars (128-bit)
	token, err := jm.GenerateOTK("CONNECTOR", 1, 5*time.Minute)
	if err != nil {
		t.Fatalf("failed to generate OTK: %v", err)
	}
	parts := strings.Split(token, ".")
	if len(parts) != 7 {
		t.Fatalf("unexpected OTK parts count: %d", len(parts))
	}
	otkSig := parts[6]
	if len(otkSig) != 32 {
		t.Errorf("expected 32 hex chars (128-bit) for OTK HMAC sig, got %d (%q)", len(otkSig), otkSig)
	}

	// SessionKey signature should be 32 hex chars (128-bit)
	sessKey := jm.GenerateSessionKey("node-hmac")
	sessParts := strings.Split(sessKey, ".")
	if len(sessParts) != 5 {
		t.Fatalf("unexpected SessionKey parts count: %d", len(sessParts))
	}
	sessSig := sessParts[4]
	if len(sessSig) != 32 {
		t.Errorf("expected 32 hex chars (128-bit) for SessionKey HMAC sig, got %d (%q)", len(sessSig), sessSig)
	}
}
