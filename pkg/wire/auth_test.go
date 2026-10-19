package wire

import (
	"bytes"
	"testing"
)

func TestStructuredAuth_EncodeDecode(t *testing.T) {
	testCases := []struct {
		name string
		req  AuthRequest
	}{
		{
			name: "Pairing auth",
			req: AuthRequest{
				Type:       AuthTypePairing,
				NodeID:     "connector-node-01",
				Credential: "req-pairing-credential",
			},
		},
		{
			name: "OTK auth",
			req: AuthRequest{
				Type:       AuthTypeOTK,
				NodeID:     "connector-node-02",
				Credential: "ANCH.JOIN.CONNECTOR.cluster-01.1.12345678.abcd",
			},
		},
		{
			name: "Session auth",
			req: AuthRequest{
				Type:       AuthTypeSession,
				NodeID:     "connector-node-03",
				Credential: "SESS.cluster-01.connector-node-03.a1b2c3d4e5f6.9988776655443322",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			encoded := EncodeStructuredAuth(tc.req)
			if len(encoded) == 0 {
				t.Fatalf("encoded payload is empty")
			}
			if encoded[0] != StructuredAuthMagic {
				t.Fatalf("expected StructuredAuthMagic (0xAA), got 0x%02X", encoded[0])
			}

			decoded, err := DecodeStructuredAuth(encoded)
			if err != nil {
				t.Fatalf("failed to decode structured auth: %v", err)
			}

			if decoded.Type != tc.req.Type {
				t.Errorf("type mismatch: got %d, want %d", decoded.Type, tc.req.Type)
			}
			if decoded.NodeID != tc.req.NodeID {
				t.Errorf("nodeID mismatch: got %q, want %q", decoded.NodeID, tc.req.NodeID)
			}
			if decoded.Credential != tc.req.Credential {
				t.Errorf("credential mismatch: got %q, want %q", decoded.Credential, tc.req.Credential)
			}
		})
	}
}

func TestStructuredAuth_LegacyFallback(t *testing.T) {
	legacyPayload := EncodeAuthPayload("legacy-secret-token")
	decoded, err := DecodeStructuredAuth(legacyPayload)
	if err != nil {
		t.Fatalf("DecodeStructuredAuth failed on legacy payload: %v", err)
	}

	if decoded.Type != AuthTypeStatic {
		t.Errorf("expected AuthTypeStatic for legacy payload, got %d", decoded.Type)
	}
	if decoded.Credential != "legacy-secret-token" {
		t.Errorf("expected credential %q, got %q", "legacy-secret-token", decoded.Credential)
	}
}

func TestStructuredAuth_Malformed(t *testing.T) {
	// Empty payload
	if _, err := DecodeStructuredAuth([]byte{}); err == nil {
		t.Error("expected error for empty payload")
	}

	// Truncated structured magic
	if _, err := DecodeStructuredAuth([]byte{StructuredAuthMagic, AuthTypePairing}); err == nil {
		t.Error("expected error for truncated payload")
	}

	// Corrupted length
	badPayload := []byte{StructuredAuthMagic, AuthTypePairing, 0xFF, 0xFF}
	if _, err := DecodeStructuredAuth(badPayload); err == nil {
		t.Error("expected error for payload with invalid length header")
	}
}

func TestAuthResponse_EncodeDecode(t *testing.T) {
	sessionKey := "SESS.cluster-01.node-abc.1234.5678"
	encoded := EncodeAuthResponse(sessionKey)
	if len(encoded) == 0 || encoded[0] != StructuredRespMagic {
		t.Fatalf("expected StructuredRespMagic (0xAB), got 0x%02X", encoded[0])
	}

	decoded, err := DecodeAuthResponse(encoded)
	if err != nil {
		t.Fatalf("failed to decode auth response: %v", err)
	}
	if decoded != sessionKey {
		t.Errorf("decoded session key mismatch: got %q, want %q", decoded, sessionKey)
	}

	// Malformed response
	if _, err := DecodeAuthResponse([]byte{StructuredRespMagic, 0xFF}); err == nil {
		t.Error("expected error for truncated response")
	}
	if _, err := DecodeAuthResponse([]byte{0x00, 0x01, 0x02}); err == nil {
		t.Error("expected error for invalid magic response")
	}
}

func TestStructuredAuth_RoundtripBuffer(t *testing.T) {
	req := AuthRequest{
		Type:       AuthTypePairing,
		NodeID:     "gateway-test",
		Credential: "secret-handshake",
	}

	buf := EncodeStructuredAuth(req)
	reader := bytes.NewReader(buf)
	readBuf := make([]byte, reader.Len())
	_, _ = reader.Read(readBuf)

	got, err := DecodeStructuredAuth(readBuf)
	if err != nil {
		t.Fatalf("failed to read decoded struct: %v", err)
	}
	if got != req {
		t.Errorf("mismatch: got %+v, want %+v", got, req)
	}
}

func TestStructuredAuth_LargeFieldsClamping(t *testing.T) {
	hugeNodeID := string(make([]byte, 70000))
	hugeCred := string(make([]byte, 80000))
	req := AuthRequest{
		Type:       AuthTypeSession,
		NodeID:     hugeNodeID,
		Credential: hugeCred,
	}

	encoded := EncodeStructuredAuth(req)
	decoded, err := DecodeStructuredAuth(encoded)
	if err != nil {
		t.Fatalf("failed to decode clamped auth: %v", err)
	}

	// Should be clamped to 65535 without panics or corrupt buffer
	if len(decoded.NodeID) != 65535 {
		t.Errorf("expected nodeID clamped to 65535, got %d", len(decoded.NodeID))
	}
	if len(decoded.Credential) != 65535 {
		t.Errorf("expected credential clamped to 65535, got %d", len(decoded.Credential))
	}
}

func TestStructuredAuth_Helpers(t *testing.T) {
	pBuf := EncodeStructuredAuthPairing("node-p")
	pReq, err := DecodeStructuredAuth(pBuf)
	if err != nil || pReq.Type != AuthTypePairing || pReq.NodeID != "node-p" {
		t.Fatalf("unexpected pairing decoded: %+v, err: %v", pReq, err)
	}

	oBuf := EncodeStructuredAuthOTK("node-o", "otk-123")
	oReq, err := DecodeStructuredAuth(oBuf)
	if err != nil || oReq.Type != AuthTypeOTK || oReq.NodeID != "node-o" || oReq.Credential != "otk-123" {
		t.Fatalf("unexpected otk decoded: %+v, err: %v", oReq, err)
	}

	sBuf := EncodeStructuredAuthSession("node-s", "sess-key-xyz")
	sReq, err := DecodeStructuredAuth(sBuf)
	if err != nil || sReq.Type != AuthTypeSession || sReq.NodeID != "node-s" || sReq.Credential != "sess-key-xyz" {
		t.Fatalf("unexpected session decoded: %+v, err: %v", sReq, err)
	}
}

func TestStructuredAuth_StagedReauth(t *testing.T) {
	nodeID := "connector-node-staged"
	val := [32]byte{0xAA, 0xBB, 0xCC, 0xDD}
	nextComm := [32]byte{0x11, 0x22, 0x33, 0x44}

	buf := EncodeStructuredAuthStagedReauth(nodeID, val, nextComm)
	req, err := DecodeStructuredAuth(buf)
	if err != nil {
		t.Fatalf("failed to decode StagedReauth: %v", err)
	}

	if req.Type != AuthTypeStagedReauth {
		t.Errorf("expected AuthTypeStagedReauth (%d), got %d", AuthTypeStagedReauth, req.Type)
	}
	if req.NodeID != nodeID {
		t.Errorf("nodeID mismatch: got %q, want %q", req.NodeID, nodeID)
	}
	if req.Validator != val {
		t.Errorf("validator mismatch: got %x, want %x", req.Validator, val)
	}
	if req.NextCommitment != nextComm {
		t.Errorf("nextCommitment mismatch: got %x, want %x", req.NextCommitment, nextComm)
	}
}

func TestStructuredAuth_WithCommitment(t *testing.T) {
	comm := [32]byte{0xDE, 0xAD, 0xBE, 0xEF}

	// 1. Pairing with Commitment
	pBuf := EncodeStructuredAuthPairingWithCommitment("node-comm-p", comm)
	pReq, err := DecodeStructuredAuth(pBuf)
	if err != nil {
		t.Fatalf("failed to decode pairing with commitment: %v", err)
	}
	if !pReq.HasCommitment || pReq.InitialCommitment != comm {
		t.Errorf("pairing commitment mismatch: has=%v, comm=%x", pReq.HasCommitment, pReq.InitialCommitment)
	}

	// 2. OTK with Commitment
	oBuf := EncodeStructuredAuthOTKWithCommitment("node-comm-o", "otk-token-99", comm)
	oReq, err := DecodeStructuredAuth(oBuf)
	if err != nil {
		t.Fatalf("failed to decode OTK with commitment: %v", err)
	}
	if !oReq.HasCommitment || oReq.InitialCommitment != comm || oReq.Credential != "otk-token-99" {
		t.Errorf("OTK commitment mismatch: has=%v, comm=%x, cred=%s", oReq.HasCommitment, oReq.InitialCommitment, oReq.Credential)
	}
}

func TestRotationStagedResponse_EncodeDecode(t *testing.T) {
	msg := "Commitment staged successfully"
	buf := EncodeRotationStagedResponse(msg)

	subCode, gotMsg, err := DecodeRotationStagedResponse(buf)
	if err != nil {
		t.Fatalf("failed to decode rotation staged response: %v", err)
	}
	if subCode != SubCodeRotationStaged {
		t.Errorf("expected SubCodeRotationStaged (0x01), got 0x%02X", subCode)
	}
	if gotMsg != msg {
		t.Errorf("message mismatch: got %q, want %q", gotMsg, msg)
	}
}
