package wire

import (
	"encoding/binary"
	"errors"
	"math"
)

// Authentication types for structured OpAuth (anchor-connector handshake).
const (
	// AuthTypePairing initiates initial node pairing during an open pairing window.
	AuthTypePairing byte = 0x01
	// AuthTypeOTK uses an ephemeral, single-use One-Time Join Key.
	AuthTypeOTK byte = 0x02
	// AuthTypeSession authenticates with an HMAC-signed session ticket.
	AuthTypeSession byte = 0x03
	// AuthTypeStatic evaluates a legacy pre-shared static auth token.
	AuthTypeStatic byte = 0x04
	// AuthTypeStagedReauth authenticates using Staged Preimage Commitment Rotation.
	AuthTypeStagedReauth byte = 0x05

	// StructuredAuthMagic is the leading byte identifying structured handshake frames.
	StructuredAuthMagic byte = 0xAA
	// StructuredRespMagic is the leading byte identifying structured handshake response frames.
	StructuredRespMagic byte = 0xAB

	// SubCodeRotationStaged indicates that NextCommitment was successfully staged to disk.
	SubCodeRotationStaged byte = 0x01
)

// Error codes for OpResponseError during handshake.
const (
	ErrCodePairingClosed      uint16 = 1001
	ErrCodeIPNotAllowed       uint16 = 1002
	ErrCodeOTKExpiredOrBurned uint16 = 1003
	ErrCodeInvalidSession     uint16 = 1004
	ErrCodeUnauthorized       uint16 = 1005
	ErrCodeMalformedPayload   uint16 = 1006
)

var (
	// ErrMalformedAuthRequest is returned when structured auth payload cannot be decoded.
	ErrMalformedAuthRequest = errors.New("wire: malformed structured auth request payload")
	// ErrMalformedAuthResponse is returned when structured auth response cannot be decoded.
	ErrMalformedAuthResponse = errors.New("wire: malformed structured auth response payload")
)

// AuthRequest represents a structured handshake authentication payload.
type AuthRequest struct {
	Type              byte
	NodeID            string
	Credential        string
	Validator         [32]byte
	NextCommitment    [32]byte
	InitialCommitment [32]byte
	HasCommitment     bool
}

// EncodeStructuredAuth encodes an AuthRequest into wire binary format.
// Fields exceeding math.MaxUint16 bytes are defensively clamped to prevent numeric overflow.
func EncodeStructuredAuth(req AuthRequest) []byte {
	nodeIDBytes := []byte(req.NodeID)
	if len(nodeIDBytes) > math.MaxUint16 {
		nodeIDBytes = nodeIDBytes[:math.MaxUint16]
	}

	if req.Type == AuthTypeStagedReauth {
		totalLen := 1 + 1 + 2 + len(nodeIDBytes) + 2 + 32 + 2 + 32
		buf := make([]byte, totalLen)
		buf[0] = StructuredAuthMagic
		buf[1] = AuthTypeStagedReauth

		offset := 2
		binary.LittleEndian.PutUint16(buf[offset:offset+2], uint16(len(nodeIDBytes)))
		offset += 2
		copy(buf[offset:offset+len(nodeIDBytes)], nodeIDBytes)
		offset += len(nodeIDBytes)

		binary.LittleEndian.PutUint16(buf[offset:offset+2], 32)
		offset += 2
		copy(buf[offset:offset+32], req.Validator[:])
		offset += 32

		binary.LittleEndian.PutUint16(buf[offset:offset+2], 32)
		offset += 2
		copy(buf[offset:offset+32], req.NextCommitment[:])
		return buf
	}

	credBytes := []byte(req.Credential)
	if len(credBytes) > math.MaxUint16 {
		credBytes = credBytes[:math.MaxUint16]
	}

	totalLen := 1 + 1 + 2 + len(nodeIDBytes) + 2 + len(credBytes)
	if req.HasCommitment {
		totalLen += 2 + 32
	}
	buf := make([]byte, totalLen)

	buf[0] = StructuredAuthMagic
	buf[1] = req.Type

	offset := 2
	binary.LittleEndian.PutUint16(buf[offset:offset+2], uint16(len(nodeIDBytes)))
	offset += 2
	copy(buf[offset:offset+len(nodeIDBytes)], nodeIDBytes)
	offset += len(nodeIDBytes)

	binary.LittleEndian.PutUint16(buf[offset:offset+2], uint16(len(credBytes)))
	offset += 2
	copy(buf[offset:offset+len(credBytes)], credBytes)
	offset += len(credBytes)

	if req.HasCommitment {
		binary.LittleEndian.PutUint16(buf[offset:offset+2], 32)
		offset += 2
		copy(buf[offset:offset+32], req.InitialCommitment[:])
	}

	return buf
}

// DecodeStructuredAuth parses an AuthRequest from a binary payload,
// falling back to legacy string tokens if the structured magic marker is absent.
func DecodeStructuredAuth(payload []byte) (AuthRequest, error) {
	if len(payload) == 0 {
		return AuthRequest{}, ErrMalformedAuthRequest
	}

	if payload[0] != StructuredAuthMagic {
		token, err := DecodeAuthPayload(payload)
		if err != nil {
			return AuthRequest{}, err
		}
		return AuthRequest{
			Type:       AuthTypeStatic,
			Credential: token,
		}, nil
	}

	if len(payload) < 6 {
		return AuthRequest{}, ErrMalformedAuthRequest
	}

	authType := payload[1]
	offset := 2

	nodeIDLen := int(binary.LittleEndian.Uint16(payload[offset : offset+2]))
	offset += 2
	if len(payload) < offset+nodeIDLen {
		return AuthRequest{}, ErrMalformedAuthRequest
	}
	nodeID := string(payload[offset : offset+nodeIDLen])
	offset += nodeIDLen

	if authType == AuthTypeStagedReauth {
		if len(payload) < offset+4+64 {
			return AuthRequest{}, ErrMalformedAuthRequest
		}
		valLen := int(binary.LittleEndian.Uint16(payload[offset : offset+2]))
		offset += 2
		if valLen != 32 || len(payload) < offset+32+2+32 {
			return AuthRequest{}, ErrMalformedAuthRequest
		}
		var validator, nextCommitment [32]byte
		copy(validator[:], payload[offset:offset+32])
		offset += 32

		nextLen := int(binary.LittleEndian.Uint16(payload[offset : offset+2]))
		offset += 2
		if nextLen != 32 || len(payload) < offset+32 {
			return AuthRequest{}, ErrMalformedAuthRequest
		}
		copy(nextCommitment[:], payload[offset:offset+32])

		return AuthRequest{
			Type:           AuthTypeStagedReauth,
			NodeID:         nodeID,
			Validator:      validator,
			NextCommitment: nextCommitment,
		}, nil
	}

	if len(payload) < offset+2 {
		return AuthRequest{}, ErrMalformedAuthRequest
	}
	credLen := int(binary.LittleEndian.Uint16(payload[offset : offset+2]))
	offset += 2
	if len(payload) < offset+credLen {
		return AuthRequest{}, ErrMalformedAuthRequest
	}
	cred := string(payload[offset : offset+credLen])
	offset += credLen

	req := AuthRequest{
		Type:       authType,
		NodeID:     nodeID,
		Credential: cred,
	}

	// Check optional InitialCommitment for Pairing / OTK
	if len(payload) >= offset+2+32 {
		commLen := int(binary.LittleEndian.Uint16(payload[offset : offset+2]))
		if commLen == 32 && len(payload) >= offset+2+32 {
			offset += 2
			copy(req.InitialCommitment[:], payload[offset:offset+32])
			req.HasCommitment = true
		}
	}

	return req, nil
}

// EncodeAuthResponse encodes an issued SessionKey into an OpResponseOK payload.
// Keys exceeding math.MaxUint16 bytes are defensively clamped to prevent numeric overflow.
func EncodeAuthResponse(sessionKey string) []byte {
	keyBytes := []byte(sessionKey)
	if len(keyBytes) > math.MaxUint16 {
		keyBytes = keyBytes[:math.MaxUint16]
	}
	buf := make([]byte, 1+2+len(keyBytes))
	buf[0] = StructuredRespMagic
	binary.LittleEndian.PutUint16(buf[1:3], uint16(len(keyBytes)))
	copy(buf[3:], keyBytes)
	return buf
}

// DecodeAuthResponse extracts an issued SessionKey from an OpResponseOK payload.
func DecodeAuthResponse(payload []byte) (string, error) {
	if len(payload) < 3 {
		return "", ErrMalformedAuthResponse
	}
	if payload[0] != StructuredRespMagic {
		return "", errors.New("wire: auth response is not a structured session response")
	}
	keyLen := int(binary.LittleEndian.Uint16(payload[1:3]))
	if len(payload) < 3+keyLen {
		return "", ErrMalformedAuthResponse
	}
	return string(payload[3 : 3+keyLen]), nil
}

// EncodeRotationStagedResponse encodes an OpResponseOK payload carrying SubCodeRotationStaged.
func EncodeRotationStagedResponse(msg string) []byte {
	msgBytes := []byte(msg)
	if len(msgBytes) > math.MaxUint16 {
		msgBytes = msgBytes[:math.MaxUint16]
	}
	buf := make([]byte, 1+1+2+len(msgBytes))
	buf[0] = StructuredRespMagic
	buf[1] = SubCodeRotationStaged
	binary.LittleEndian.PutUint16(buf[2:4], uint16(len(msgBytes)))
	copy(buf[4:], msgBytes)
	return buf
}

// DecodeRotationStagedResponse decodes an OpResponseOK payload carrying SubCodeRotationStaged.
func DecodeRotationStagedResponse(payload []byte) (byte, string, error) {
	if len(payload) < 4 {
		return 0, "", ErrMalformedAuthResponse
	}
	if payload[0] != StructuredRespMagic {
		return 0, "", errors.New("wire: auth response is not a structured response")
	}
	subCode := payload[1]
	msgLen := int(binary.LittleEndian.Uint16(payload[2:4]))
	if len(payload) < 4+msgLen {
		return 0, "", ErrMalformedAuthResponse
	}
	return subCode, string(payload[4 : 4+msgLen]), nil
}

// EncodeStructuredAuthPairing encodes an AuthRequest for initial node pairing.
func EncodeStructuredAuthPairing(nodeID string) []byte {
	return EncodeStructuredAuth(AuthRequest{
		Type:   AuthTypePairing,
		NodeID: nodeID,
	})
}

// EncodeStructuredAuthPairingWithCommitment encodes a pairing request with an initial commitment.
func EncodeStructuredAuthPairingWithCommitment(nodeID string, commitment [32]byte) []byte {
	return EncodeStructuredAuth(AuthRequest{
		Type:              AuthTypePairing,
		NodeID:            nodeID,
		InitialCommitment: commitment,
		HasCommitment:     true,
	})
}

// EncodeStructuredAuthOTK encodes an AuthRequest for single-use OTK joining.
func EncodeStructuredAuthOTK(nodeID, otk string) []byte {
	return EncodeStructuredAuth(AuthRequest{
		Type:       AuthTypeOTK,
		NodeID:     nodeID,
		Credential: otk,
	})
}

// EncodeStructuredAuthOTKWithCommitment encodes an OTK request with an initial commitment.
func EncodeStructuredAuthOTKWithCommitment(nodeID, otk string, commitment [32]byte) []byte {
	return EncodeStructuredAuth(AuthRequest{
		Type:              AuthTypeOTK,
		NodeID:            nodeID,
		Credential:        otk,
		InitialCommitment: commitment,
		HasCommitment:     true,
	})
}

// EncodeStructuredAuthSession encodes an AuthRequest with an existing session ticket.
func EncodeStructuredAuthSession(nodeID, sessionKey string) []byte {
	return EncodeStructuredAuth(AuthRequest{
		Type:       AuthTypeSession,
		NodeID:     nodeID,
		Credential: sessionKey,
	})
}

// EncodeStructuredAuthStagedReauth encodes an AuthTypeStagedReauth request.
func EncodeStructuredAuthStagedReauth(nodeID string, validator [32]byte, nextCommitment [32]byte) []byte {
	return EncodeStructuredAuth(AuthRequest{
		Type:           AuthTypeStagedReauth,
		NodeID:         nodeID,
		Validator:      validator,
		NextCommitment: nextCommitment,
	})
}
