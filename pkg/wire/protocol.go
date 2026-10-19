// Package wire implements the AnchorWire binary protocol, connection pooling,
// and cryptographic authentication for AnchorDB.
package wire

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"time"

	"github.com/rachmanzz/anchordb/pkg/spatial"
	"github.com/rachmanzz/anchordb/pkg/storage"
)

const (
	// Magic byte for AnchorWire protocol identification
	Magic byte = 0xA5
	// Version 1 of AnchorWire
	Version byte = 0x01

	// HeaderSize is the fixed size of each packet header (16 bytes)
	HeaderSize = 16

	// MaxPayloadSize caps incoming frames at 64 MiB to guard against untrusted allocations
	MaxPayloadSize = 64 << 20
)

// OpCodes
const (
	// Core & Session
	OpPing            byte = 0x01
	OpPong            byte = 0x02
	OpAuth            byte = 0x03
	OpSelectModel     byte = 0x04
	OpNegotiateMemory byte = 0x05

	// Mutations & Queries
	OpSave         byte = 0x10
	OpSaveBatch    byte = 0x11
	OpGet          byte = 0x12
	OpEdge         byte = 0x13
	OpCausal       byte = 0x14
	OpSync         byte = 0x15
	OpList         byte = 0x16
	OpUpdate       byte = 0x17
	OpGraph        byte = 0x18
	OpGraphChain   byte = 0x19
	OpAttachSchema byte = 0x1A
	OpGetSchema    byte = 0x1B
	OpMath         byte = 0x1C
	OpResetModel   byte = 0x1D

	// Cold Storage & Tiered Partition Extensions
	OpPartitionCatalog byte = 0x20
	OpPartitionInspect byte = 0x21

	// Cluster Administration & Replication Extensions
	OpPromoteToPrimary  byte = 0x30
	OpDemoteToReplica   byte = 0x31
	OpReplicationStream byte = 0x32

	// Responses
	OpResponseOK    byte = 0x80
	OpResponseError byte = 0x81
)

// Flags
const (
	FlagNone           byte = 0x00
	FlagCompressed     byte = 0x01
	FlagOneway         byte = 0x02
	FlagMemoryPressure byte = 0x04
	FlagColdTier       byte = 0x08 // Carried when record was retrieved from Cold Storage Tier

	// Connector Routing Extensions
	FlagRoutePrimary byte = 0x10 // Forces routing to Primary (Read-Your-Own-Writes)
	FlagRouteReplica byte = 0x20 // Permits routing to Read Replicas
	FlagEpochFenced  byte = 0x40 // Carries Monotonic Epoch fencing token
)

// Memory Admission Status
const (
	MemoryStatusGranted   uint8 = 0 // RAM headroom is healthy (<60% used), normal ingestion
	MemoryStatusThrottled uint8 = 1 // Moderate memory pressure (60%-80% used), paced drip-feed
	MemoryStatusHold      uint8 = 2 // High memory pressure (>80% used), hold and pause ingestion
)

// Frame represents a single AnchorWire protocol packet.
type Frame struct {
	Magic   byte
	Version byte
	OpCode  byte
	Flags   byte
	Seq     uint32
	Payload []byte
}

// ReadFrame decodes a Frame from an io.Reader.
func ReadFrame(r io.Reader) (Frame, error) {
	var hdr [HeaderSize]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return Frame{}, err
	}

	if hdr[0] != Magic {
		return Frame{}, fmt.Errorf("invalid wire magic: 0x%02X (expected 0x%02X)", hdr[0], Magic)
	}
	if hdr[1] != Version {
		return Frame{}, fmt.Errorf("unsupported wire version: %d (expected %d)", hdr[1], Version)
	}

	opCode := hdr[2]
	flags := hdr[3]
	seq := binary.LittleEndian.Uint32(hdr[4:8])
	payloadLen := binary.LittleEndian.Uint32(hdr[8:12])
	expectedChecksum := binary.LittleEndian.Uint32(hdr[12:16])

	if payloadLen > MaxPayloadSize {
		return Frame{}, fmt.Errorf("payload length %d exceeds max allowed %d", payloadLen, MaxPayloadSize)
	}

	payload := make([]byte, payloadLen)
	if payloadLen > 0 {
		if _, err := io.ReadFull(r, payload); err != nil {
			return Frame{}, fmt.Errorf("failed to read payload: %w", err)
		}
		actualChecksum := crc32.ChecksumIEEE(payload)
		if actualChecksum != expectedChecksum {
			return Frame{}, fmt.Errorf("checksum mismatch: expected 0x%08X, got 0x%08X", expectedChecksum, actualChecksum)
		}
	} else if expectedChecksum != 0 {
		return Frame{}, fmt.Errorf("non-zero checksum for empty payload")
	}

	return Frame{
		Magic:   Magic,
		Version: Version,
		OpCode:  opCode,
		Flags:   flags,
		Seq:     seq,
		Payload: payload,
	}, nil
}

// WriteFrame encodes and writes a Frame to an io.Writer.
func WriteFrame(w io.Writer, f Frame) error {
	var hdr [HeaderSize]byte
	hdr[0] = Magic
	hdr[1] = Version
	hdr[2] = f.OpCode
	hdr[3] = f.Flags
	binary.LittleEndian.PutUint32(hdr[4:8], f.Seq)
	binary.LittleEndian.PutUint32(hdr[8:12], uint32(len(f.Payload)))

	var checksum uint32
	if len(f.Payload) > 0 {
		checksum = crc32.ChecksumIEEE(f.Payload)
	}
	binary.LittleEndian.PutUint32(hdr[12:16], checksum)

	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	if len(f.Payload) > 0 {
		if _, err := w.Write(f.Payload); err != nil {
			return err
		}
	}
	return nil
}

// --- PAYLOAD SERIALIZATION HELPERS -------------------------------------------

func appendString(buf []byte, s string) []byte {
	l := len(s)
	if l > math.MaxUint16 {
		l = math.MaxUint16
		s = s[:l]
	}
	var lenBytes [2]byte
	binary.LittleEndian.PutUint16(lenBytes[:], uint16(l))
	buf = append(buf, lenBytes[:]...)
	return append(buf, s...)
}

func readString(data []byte, offset int) (string, int, error) {
	if offset+2 > len(data) {
		return "", offset, errors.New("truncated string length prefix")
	}
	l := int(binary.LittleEndian.Uint16(data[offset : offset+2]))
	offset += 2
	if offset+l > len(data) {
		return "", offset, errors.New("truncated string bytes")
	}
	s := string(data[offset : offset+l])
	offset += l
	return s, offset, nil
}

// EncodeAuthPayload encodes auth token
func EncodeAuthPayload(token string) []byte {
	return appendString(nil, token)
}

// DecodeAuthPayload decodes auth token, transparently supporting structured auth requests.
func DecodeAuthPayload(data []byte) (string, error) {
	if len(data) > 0 && data[0] == StructuredAuthMagic {
		req, err := DecodeStructuredAuth(data)
		if err != nil {
			return "", err
		}
		return req.Credential, nil
	}
	token, _, err := readString(data, 0)
	return token, err
}

// EncodeSelectModelPayload encodes model name
func EncodeSelectModelPayload(model string) []byte {
	return appendString(nil, model)
}

// DecodeSelectModelPayload decodes model name
func DecodeSelectModelPayload(data []byte) (string, error) {
	model, _, err := readString(data, 0)
	return model, err
}

// EncodeSavePayload serializes an entity record for OpSave
func EncodeSavePayload(item storage.BatchRecordItem) []byte {
	buf := appendString(nil, item.Name)
	buf = append(buf, item.Operation)

	var countBytes [2]byte
	binary.LittleEndian.PutUint16(countBytes[:], uint16(len(item.Props)))
	buf = append(buf, countBytes[:]...)

	for k, v := range item.Props {
		buf = appendString(buf, k)
		var valBytes [4]byte
		binary.LittleEndian.PutUint32(valBytes[:], math.Float32bits(v))
		buf = append(buf, valBytes[:]...)
	}

	if item.Coord != nil || item.Roles != nil {
		buf = append(buf, 1) // has node
		coord := spatial.Point2D{}
		if item.Coord != nil {
			coord = *item.Coord
		}
		var roles [4]float32
		if item.Roles != nil {
			roles = *item.Roles
		}

		var geoBytes [24]byte
		binary.LittleEndian.PutUint32(geoBytes[0:4], math.Float32bits(coord.X))
		binary.LittleEndian.PutUint32(geoBytes[4:8], math.Float32bits(coord.Y))
		for r := 0; r < 4; r++ {
			binary.LittleEndian.PutUint32(geoBytes[8+r*4:12+r*4], math.Float32bits(roles[r]))
		}
		buf = append(buf, geoBytes[:]...)
	} else {
		buf = append(buf, 0) // no node
	}

	return buf
}

// DecodeSavePayload deserializes an entity record from OpSave
func DecodeSavePayload(data []byte) (storage.BatchRecordItem, error) {
	name, offset, err := readString(data, 0)
	if err != nil {
		return storage.BatchRecordItem{}, err
	}
	if offset+1 > len(data) {
		return storage.BatchRecordItem{}, errors.New("truncated operation byte")
	}
	opStatus := data[offset]
	offset++

	if offset+2 > len(data) {
		return storage.BatchRecordItem{}, errors.New("truncated attr count")
	}
	attrCount := int(binary.LittleEndian.Uint16(data[offset : offset+2]))
	offset += 2

	props := make(map[string]float32, attrCount)
	for i := 0; i < attrCount; i++ {
		key, nextOffset, err := readString(data, offset)
		if err != nil {
			return storage.BatchRecordItem{}, err
		}
		offset = nextOffset
		if offset+4 > len(data) {
			return storage.BatchRecordItem{}, errors.New("truncated attr value")
		}
		val := math.Float32frombits(binary.LittleEndian.Uint32(data[offset : offset+4]))
		offset += 4
		props[key] = val
	}

	item := storage.BatchRecordItem{
		Name:      name,
		Operation: opStatus,
		Props:     props,
	}

	if offset < len(data) && data[offset] == 1 {
		offset++
		if offset+24 > len(data) {
			return storage.BatchRecordItem{}, errors.New("truncated geometry bytes")
		}
		coordX := math.Float32frombits(binary.LittleEndian.Uint32(data[offset : offset+4]))
		coordY := math.Float32frombits(binary.LittleEndian.Uint32(data[offset+4 : offset+8]))
		var roles [4]float32
		for r := 0; r < 4; r++ {
			roles[r] = math.Float32frombits(binary.LittleEndian.Uint32(data[offset+8+r*4 : offset+12+r*4]))
		}
		item.Coord = &spatial.Point2D{X: coordX, Y: coordY}
		item.Roles = &roles
	}

	return item, nil
}

// EncodeSaveBatchPayload serializes multiple records for OpSaveBatch
func EncodeSaveBatchPayload(records []storage.BatchRecordItem) []byte {
	var countBytes [4]byte
	binary.LittleEndian.PutUint32(countBytes[:], uint32(len(records)))
	buf := append([]byte(nil), countBytes[:]...)

	for _, rec := range records {
		recBytes := EncodeSavePayload(rec)
		var lenBytes [4]byte
		binary.LittleEndian.PutUint32(lenBytes[:], uint32(len(recBytes)))
		buf = append(buf, lenBytes[:]...)
		buf = append(buf, recBytes...)
	}
	return buf
}

// DecodeSaveBatchPayload deserializes multiple records from OpSaveBatch
func DecodeSaveBatchPayload(data []byte) ([]storage.BatchRecordItem, error) {
	if len(data) < 4 {
		return nil, errors.New("truncated batch count")
	}
	count := int(binary.LittleEndian.Uint32(data[0:4]))
	offset := 4

	records := make([]storage.BatchRecordItem, 0, count)
	for i := 0; i < count; i++ {
		if offset+4 > len(data) {
			return nil, errors.New("truncated record length in batch")
		}
		recLen := int(binary.LittleEndian.Uint32(data[offset : offset+4]))
		offset += 4
		if offset+recLen > len(data) {
			return nil, errors.New("truncated record bytes in batch")
		}
		rec, err := DecodeSavePayload(data[offset : offset+recLen])
		if err != nil {
			return nil, fmt.Errorf("failed to decode batch record %d: %w", i, err)
		}
		offset += recLen
		records = append(records, rec)
	}
	return records, nil
}

// GetRequest specifies a get query with optional field projection and computations.
type GetRequest struct {
	Name         string
	Select       []string
	Computations map[string]storage.ComputeOp
}

// EncodeGetRequest serializes GetRequest for OpGet.
func EncodeGetRequest(req GetRequest) []byte {
	buf := appendString(nil, req.Name)
	if len(req.Select) == 0 && len(req.Computations) == 0 {
		return buf
	}
	var countBytes [2]byte
	binary.LittleEndian.PutUint16(countBytes[:], uint16(len(req.Select)))
	buf = append(buf, countBytes[:]...)
	for _, s := range req.Select {
		buf = appendString(buf, s)
	}

	binary.LittleEndian.PutUint16(countBytes[:], uint16(len(req.Computations)))
	buf = append(buf, countBytes[:]...)
	for k, comp := range req.Computations {
		buf = appendString(buf, k)
		buf = appendComputeOp(buf, comp)
	}
	return buf
}

// DecodeGetRequest deserializes GetRequest from OpGet.
func DecodeGetRequest(data []byte) (GetRequest, error) {
	name, offset, err := readString(data, 0)
	if err != nil {
		return GetRequest{}, err
	}
	req := GetRequest{Name: name}
	if offset+2 <= len(data) {
		selectCount := int(binary.LittleEndian.Uint16(data[offset : offset+2]))
		offset += 2
		for i := 0; i < selectCount; i++ {
			s, nextOffset, err := readString(data, offset)
			if err != nil {
				return req, err
			}
			req.Select = append(req.Select, s)
			offset = nextOffset
		}
	}
	if offset+2 <= len(data) {
		compCount := int(binary.LittleEndian.Uint16(data[offset : offset+2]))
		offset += 2
		if compCount > 0 {
			req.Computations = make(map[string]storage.ComputeOp, compCount)
			for i := 0; i < compCount; i++ {
				outField, nextOffset, err := readString(data, offset)
				if err != nil {
					return req, err
				}
				op, nextOffset, err := readComputeOp(data, nextOffset)
				if err != nil {
					return req, err
				}
				req.Computations[outField] = op
				offset = nextOffset
			}
		}
	}
	return req, nil
}

// EncodeGetPayload serializes record name for OpGet
func EncodeGetPayload(name string) []byte {
	return appendString(nil, name)
}

// DecodeGetPayload deserializes record name from OpGet
func DecodeGetPayload(data []byte) (string, error) {
	name, _, err := readString(data, 0)
	return name, err
}

// EncodeGetResultPayload serializes properties for OpGetResponse
func EncodeGetResultPayload(opStatus uint8, props map[string]float32) []byte {
	buf := []byte{opStatus}
	var countBytes [2]byte
	binary.LittleEndian.PutUint16(countBytes[:], uint16(len(props)))
	buf = append(buf, countBytes[:]...)

	for k, v := range props {
		buf = appendString(buf, k)
		var valBytes [4]byte
		binary.LittleEndian.PutUint32(valBytes[:], math.Float32bits(v))
		buf = append(buf, valBytes[:]...)
	}
	return buf
}

// DecodeGetResultPayload deserializes properties from OpGetResponse
func DecodeGetResultPayload(data []byte) (uint8, map[string]float32, error) {
	if len(data) < 3 {
		return 0, nil, errors.New("truncated get result payload")
	}
	opStatus := data[0]
	attrCount := int(binary.LittleEndian.Uint16(data[1:3]))
	offset := 3

	props := make(map[string]float32, attrCount)
	for i := 0; i < attrCount; i++ {
		key, nextOffset, err := readString(data, offset)
		if err != nil {
			return 0, nil, err
		}
		offset = nextOffset
		if offset+4 > len(data) {
			return 0, nil, errors.New("truncated attr value in get result")
		}
		val := math.Float32frombits(binary.LittleEndian.Uint32(data[offset : offset+4]))
		offset += 4
		props[key] = val
	}
	return opStatus, props, nil
}

// EncodeEdgePayloadWithDirection serializes edge mutation or relation vector configuration for OpEdge.
func EncodeEdgePayloadWithDirection(action, source, relation, target string, weight float32, direction *spatial.Point2D) []byte {
	var actByte byte
	switch action {
	case "remove":
		actByte = 1
	case "set_relation":
		actByte = 2
	default:
		actByte = 0
	}
	buf := []byte{actByte}
	buf = appendString(buf, source)
	buf = appendString(buf, relation)
	buf = appendString(buf, target)
	var wBytes [4]byte
	binary.LittleEndian.PutUint32(wBytes[:], math.Float32bits(weight))
	buf = append(buf, wBytes[:]...)
	if direction != nil {
		var dirBytes [8]byte
		binary.LittleEndian.PutUint32(dirBytes[0:4], math.Float32bits(direction.X))
		binary.LittleEndian.PutUint32(dirBytes[4:8], math.Float32bits(direction.Y))
		buf = append(buf, dirBytes[:]...)
	}
	return buf
}

// EncodeEdgePayload serializes edge data for OpEdge
func EncodeEdgePayload(action, source, relation, target string, weight float32) []byte {
	return EncodeEdgePayloadWithDirection(action, source, relation, target, weight, nil)
}

// DecodeEdgePayloadWithDirection deserializes edge data including optional direction from OpEdge
func DecodeEdgePayloadWithDirection(data []byte) (action, source, relation, target string, weight float32, direction *spatial.Point2D, err error) {
	if len(data) < 1 {
		return "", "", "", "", 0, nil, errors.New("truncated edge payload")
	}
	switch data[0] {
	case 1:
		action = "remove"
	case 2:
		action = "set_relation"
	default:
		action = "add"
	}
	offset := 1
	source, offset, err = readString(data, offset)
	if err != nil {
		return "", "", "", "", 0, nil, err
	}
	relation, offset, err = readString(data, offset)
	if err != nil {
		return "", "", "", "", 0, nil, err
	}
	target, offset, err = readString(data, offset)
	if err != nil {
		return "", "", "", "", 0, nil, err
	}
	if offset+4 > len(data) {
		return "", "", "", "", 0, nil, errors.New("truncated edge weight")
	}
	weight = math.Float32frombits(binary.LittleEndian.Uint32(data[offset : offset+4]))
	offset += 4
	if offset+8 <= len(data) {
		dx := math.Float32frombits(binary.LittleEndian.Uint32(data[offset : offset+4]))
		dy := math.Float32frombits(binary.LittleEndian.Uint32(data[offset+4 : offset+8]))
		direction = &spatial.Point2D{X: dx, Y: dy}
	}
	return action, source, relation, target, weight, direction, nil
}

// DecodeEdgePayload deserializes edge data from OpEdge
func DecodeEdgePayload(data []byte) (action, source, relation, target string, weight float32, err error) {
	action, source, relation, target, weight, _, err = DecodeEdgePayloadWithDirection(data)
	return action, source, relation, target, weight, err
}

// EncodeCausalPayload serializes causal query input
func EncodeCausalPayload(seeds []string, relation string, limitK int) []byte {
	var countBytes [2]byte
	binary.LittleEndian.PutUint16(countBytes[:], uint16(len(seeds)))
	buf := append([]byte(nil), countBytes[:]...)
	for _, seed := range seeds {
		buf = appendString(buf, seed)
	}
	buf = appendString(buf, relation)
	var kBytes [4]byte
	binary.LittleEndian.PutUint32(kBytes[:], uint32(limitK))
	buf = append(buf, kBytes[:]...)
	return buf
}

// DecodeCausalPayload deserializes causal query input
func DecodeCausalPayload(data []byte) (seeds []string, relation string, limitK int, err error) {
	if len(data) < 2 {
		return nil, "", 0, errors.New("truncated causal payload")
	}
	seedCount := int(binary.LittleEndian.Uint16(data[0:2]))
	offset := 2

	seeds = make([]string, 0, seedCount)
	for i := 0; i < seedCount; i++ {
		seed, nextOffset, sErr := readString(data, offset)
		if sErr != nil {
			return nil, "", 0, sErr
		}
		offset = nextOffset
		seeds = append(seeds, seed)
	}
	relation, offset, err = readString(data, offset)
	if err != nil {
		return nil, "", 0, err
	}
	if offset+4 > len(data) {
		return nil, "", 0, errors.New("truncated limitK")
	}
	limitK = int(binary.LittleEndian.Uint32(data[offset : offset+4]))
	return seeds, relation, limitK, nil
}

// EncodeCausalResultPayload serializes causal results
func EncodeCausalResultPayload(results []storage.CausalResult) []byte {
	var countBytes [4]byte
	binary.LittleEndian.PutUint32(countBytes[:], uint32(len(results)))
	buf := append([]byte(nil), countBytes[:]...)

	for _, r := range results {
		buf = appendString(buf, r.Word)
		var scoreBytes [4]byte
		binary.LittleEndian.PutUint32(scoreBytes[:], math.Float32bits(r.Score))
		buf = append(buf, scoreBytes[:]...)
	}
	return buf
}

// DecodeCausalResultPayload deserializes causal results
func DecodeCausalResultPayload(data []byte) ([]storage.CausalResult, error) {
	if len(data) < 4 {
		return nil, errors.New("truncated causal result count")
	}
	count := int(binary.LittleEndian.Uint32(data[0:4]))
	offset := 4

	results := make([]storage.CausalResult, 0, count)
	for i := 0; i < count; i++ {
		word, nextOffset, err := readString(data, offset)
		if err != nil {
			return nil, err
		}
		offset = nextOffset
		if offset+4 > len(data) {
			return nil, errors.New("truncated score in causal result")
		}
		score := math.Float32frombits(binary.LittleEndian.Uint32(data[offset : offset+4]))
		offset += 4
		results = append(results, storage.CausalResult{
			Word:  word,
			Score: score,
		})
	}
	return results, nil
}

// EncodeOKPayload serializes standard success message
func EncodeOKPayload(message string) []byte {
	return appendString(nil, message)
}

// DecodeOKPayload deserializes standard success message
func DecodeOKPayload(data []byte) (string, error) {
	msg, _, err := readString(data, 0)
	return msg, err
}

// EncodeErrorPayload serializes error code and message
func EncodeErrorPayload(code uint16, message string) []byte {
	var codeBytes [2]byte
	binary.LittleEndian.PutUint16(codeBytes[:], code)
	buf := append([]byte(nil), codeBytes[:]...)
	return appendString(buf, message)
}

// DecodeErrorPayload deserializes error code and message
func DecodeErrorPayload(data []byte) (uint16, string, error) {
	if len(data) < 2 {
		return 0, "", errors.New("truncated error payload")
	}
	code := binary.LittleEndian.Uint16(data[0:2])
	msg, _, err := readString(data, 2)
	return code, msg, err
}

// MemoryNegotiationRequest represents client's estimated ingestion workload.
type MemoryNegotiationRequest struct {
	TargetRecords            uint32
	EstimatedRecordSizeBytes uint32
	ClientBatchSize          uint32
}

// MemoryNegotiationResponse represents server's negotiated pacing strategy.
type MemoryNegotiationResponse struct {
	Status             uint8  // MemoryStatusGranted, MemoryStatusThrottled, MemoryStatusHold
	HostUsedPct        uint8  // Percentage of host RAM in use (e.g. 60)
	HeadroomBytes      uint64 // Available bytes before hitting threshold
	AllocatedBatchSize uint32 // Negotiated batch size to release
	ReleaseIntervalMs  uint32 // Delay to hold between batch releases
}

// EncodeMemoryNegotiationRequest serializes client's memory request.
func EncodeMemoryNegotiationRequest(req MemoryNegotiationRequest) []byte {
	buf := make([]byte, 12)
	binary.LittleEndian.PutUint32(buf[0:4], req.TargetRecords)
	binary.LittleEndian.PutUint32(buf[4:8], req.EstimatedRecordSizeBytes)
	binary.LittleEndian.PutUint32(buf[8:12], req.ClientBatchSize)
	return buf
}

// DecodeMemoryNegotiationRequest deserializes client's memory request.
func DecodeMemoryNegotiationRequest(data []byte) (MemoryNegotiationRequest, error) {
	if len(data) < 12 {
		return MemoryNegotiationRequest{}, errors.New("truncated memory negotiation request")
	}
	return MemoryNegotiationRequest{
		TargetRecords:            binary.LittleEndian.Uint32(data[0:4]),
		EstimatedRecordSizeBytes: binary.LittleEndian.Uint32(data[4:8]),
		ClientBatchSize:          binary.LittleEndian.Uint32(data[8:12]),
	}, nil
}

// EncodeMemoryNegotiationResponse serializes server's pacing strategy.
func EncodeMemoryNegotiationResponse(resp MemoryNegotiationResponse) []byte {
	buf := make([]byte, 18)
	buf[0] = resp.Status
	buf[1] = resp.HostUsedPct
	binary.LittleEndian.PutUint64(buf[2:10], resp.HeadroomBytes)
	binary.LittleEndian.PutUint32(buf[10:14], resp.AllocatedBatchSize)
	binary.LittleEndian.PutUint32(buf[14:18], resp.ReleaseIntervalMs)
	return buf
}

// DecodeMemoryNegotiationResponse deserializes server's pacing strategy.
func DecodeMemoryNegotiationResponse(data []byte) (MemoryNegotiationResponse, error) {
	if len(data) < 18 {
		return MemoryNegotiationResponse{}, errors.New("truncated memory negotiation response")
	}
	return MemoryNegotiationResponse{
		Status:             data[0],
		HostUsedPct:        data[1],
		HeadroomBytes:      binary.LittleEndian.Uint64(data[2:10]),
		AllocatedBatchSize: binary.LittleEndian.Uint32(data[10:14]),
		ReleaseIntervalMs:  binary.LittleEndian.Uint32(data[14:18]),
	}, nil
}

// --- FILTER & COMPUTATION CODECS ---

func appendFilterCondition(buf []byte, f storage.FilterCondition) []byte {
	buf = appendString(buf, f.Field)
	buf = appendString(buf, f.Operator)
	var valBytes [8]byte
	binary.LittleEndian.PutUint32(valBytes[0:4], math.Float32bits(f.Value))
	binary.LittleEndian.PutUint32(valBytes[4:8], math.Float32bits(f.ValueHigh))
	return append(buf, valBytes[:]...)
}

func readFilterCondition(data []byte, offset int) (storage.FilterCondition, int, error) {
	field, nextOffset, err := readString(data, offset)
	if err != nil {
		return storage.FilterCondition{}, offset, err
	}
	op, nextOffset, err := readString(data, nextOffset)
	if err != nil {
		return storage.FilterCondition{}, offset, err
	}
	if nextOffset+8 > len(data) {
		return storage.FilterCondition{}, offset, errors.New("truncated filter values")
	}
	val := math.Float32frombits(binary.LittleEndian.Uint32(data[nextOffset : nextOffset+4]))
	valHigh := math.Float32frombits(binary.LittleEndian.Uint32(data[nextOffset+4 : nextOffset+8]))
	return storage.FilterCondition{
		Field:     field,
		Operator:  op,
		Value:     val,
		ValueHigh: valHigh,
	}, nextOffset + 8, nil
}

func appendComputeOp(buf []byte, op storage.ComputeOp) []byte {
	switch v := op.(type) {
	case storage.OpDivide:
		buf = append(buf, 1)
		buf = appendString(buf, v.FieldA)
		buf = appendString(buf, v.FieldB)
	case storage.OpMultiply:
		buf = append(buf, 2)
		buf = appendString(buf, v.FieldA)
		buf = appendString(buf, v.FieldB)
	case storage.OpAdd:
		buf = append(buf, 3)
		buf = appendString(buf, v.FieldA)
		buf = appendString(buf, v.FieldB)
	case storage.OpSubtract:
		buf = append(buf, 4)
		buf = appendString(buf, v.FieldA)
		buf = appendString(buf, v.FieldB)
	case storage.OpAddDays:
		buf = append(buf, 5)
		buf = appendString(buf, v.Field)
		var b [4]byte
		binary.LittleEndian.PutUint32(b[:], math.Float32bits(v.Days))
		buf = append(buf, b[:]...)
	case storage.OpSubtractDays:
		buf = append(buf, 6)
		buf = appendString(buf, v.Field)
		var b [4]byte
		binary.LittleEndian.PutUint32(b[:], math.Float32bits(v.Days))
		buf = append(buf, b[:]...)
	case storage.OpDaysBetween:
		buf = append(buf, 7)
		buf = appendString(buf, v.FieldA)
		buf = appendString(buf, v.FieldB)
	case storage.OpAssign:
		buf = append(buf, 8)
		buf = appendString(buf, v.Value)
	default:
		buf = append(buf, 0)
	}
	return buf
}

func readComputeOp(data []byte, offset int) (storage.ComputeOp, int, error) {
	if offset+1 > len(data) {
		return nil, offset, errors.New("truncated compute op tag")
	}
	tag := data[offset]
	offset++
	switch tag {
	case 1:
		fA, nextOffset, err := readString(data, offset)
		if err != nil {
			return nil, offset, err
		}
		fB, nextOffset, err := readString(data, nextOffset)
		if err != nil {
			return nil, offset, err
		}
		return storage.OpDivide{FieldA: fA, FieldB: fB}, nextOffset, nil
	case 2:
		fA, nextOffset, err := readString(data, offset)
		if err != nil {
			return nil, offset, err
		}
		fB, nextOffset, err := readString(data, nextOffset)
		if err != nil {
			return nil, offset, err
		}
		return storage.OpMultiply{FieldA: fA, FieldB: fB}, nextOffset, nil
	case 3:
		fA, nextOffset, err := readString(data, offset)
		if err != nil {
			return nil, offset, err
		}
		fB, nextOffset, err := readString(data, nextOffset)
		if err != nil {
			return nil, offset, err
		}
		return storage.OpAdd{FieldA: fA, FieldB: fB}, nextOffset, nil
	case 4:
		fA, nextOffset, err := readString(data, offset)
		if err != nil {
			return nil, offset, err
		}
		fB, nextOffset, err := readString(data, nextOffset)
		if err != nil {
			return nil, offset, err
		}
		return storage.OpSubtract{FieldA: fA, FieldB: fB}, nextOffset, nil
	case 5:
		field, nextOffset, err := readString(data, offset)
		if err != nil {
			return nil, offset, err
		}
		if nextOffset+4 > len(data) {
			return nil, offset, errors.New("truncated days")
		}
		days := math.Float32frombits(binary.LittleEndian.Uint32(data[nextOffset : nextOffset+4]))
		return storage.OpAddDays{Field: field, Days: days}, nextOffset + 4, nil
	case 6:
		field, nextOffset, err := readString(data, offset)
		if err != nil {
			return nil, offset, err
		}
		if nextOffset+4 > len(data) {
			return nil, offset, errors.New("truncated days")
		}
		days := math.Float32frombits(binary.LittleEndian.Uint32(data[nextOffset : nextOffset+4]))
		return storage.OpSubtractDays{Field: field, Days: days}, nextOffset + 4, nil
	case 7:
		fA, nextOffset, err := readString(data, offset)
		if err != nil {
			return nil, offset, err
		}
		fB, nextOffset, err := readString(data, nextOffset)
		if err != nil {
			return nil, offset, err
		}
		return storage.OpDaysBetween{FieldA: fA, FieldB: fB}, nextOffset, nil
	case 8:
		val, nextOffset, err := readString(data, offset)
		if err != nil {
			return nil, offset, err
		}
		return storage.OpAssign{Value: val}, nextOffset, nil
	default:
		return nil, offset, fmt.Errorf("unknown compute op tag %d", tag)
	}
}

// --- LIST CODECS ---

// ListRequest specifies QueryOptions in wire format.
type ListRequest struct {
	Limit                int
	Offset               int
	IncludeAllOperations bool
	OperationFilter      *uint8
	AllowedOperations    []uint8
	Select               []string
	Filters              []storage.FilterCondition
	Computations         map[string]storage.ComputeOp
}

// ListResponse specifies the results of an OpList query.
type ListResponse struct {
	TotalCount int
	HasMore    bool
	Items      []storage.EntityRecord
}

// EncodeListRequest serializes ListRequest into wire bytes.
func EncodeListRequest(req ListRequest) []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(req.Limit))
	binary.LittleEndian.PutUint32(buf[4:8], uint32(req.Offset))

	flags := byte(0)
	if req.IncludeAllOperations {
		flags |= 1
	}
	if req.OperationFilter != nil {
		flags |= 2
	}
	buf = append(buf, flags)
	if req.OperationFilter != nil {
		buf = append(buf, *req.OperationFilter)
	}

	buf = append(buf, byte(len(req.AllowedOperations)))
	buf = append(buf, req.AllowedOperations...)

	var countBytes [2]byte
	binary.LittleEndian.PutUint16(countBytes[:], uint16(len(req.Select)))
	buf = append(buf, countBytes[:]...)
	for _, s := range req.Select {
		buf = appendString(buf, s)
	}

	binary.LittleEndian.PutUint16(countBytes[:], uint16(len(req.Filters)))
	buf = append(buf, countBytes[:]...)
	for _, f := range req.Filters {
		buf = appendFilterCondition(buf, f)
	}

	binary.LittleEndian.PutUint16(countBytes[:], uint16(len(req.Computations)))
	buf = append(buf, countBytes[:]...)
	for k, comp := range req.Computations {
		buf = appendString(buf, k)
		buf = appendComputeOp(buf, comp)
	}

	return buf
}

// DecodeListRequest deserializes ListRequest from wire bytes.
func DecodeListRequest(data []byte) (ListRequest, error) {
	if len(data) < 9 {
		return ListRequest{}, errors.New("truncated list request header")
	}
	req := ListRequest{
		Limit:  int(binary.LittleEndian.Uint32(data[0:4])),
		Offset: int(binary.LittleEndian.Uint32(data[4:8])),
	}
	offset := 8
	flags := data[offset]
	offset++
	req.IncludeAllOperations = (flags & 1) != 0

	if (flags & 2) != 0 {
		if offset >= len(data) {
			return ListRequest{}, errors.New("truncated op filter")
		}
		opF := data[offset]
		req.OperationFilter = &opF
		offset++
	}

	if offset >= len(data) {
		return ListRequest{}, errors.New("truncated allowed ops length")
	}
	allowedCount := int(data[offset])
	offset++
	if offset+allowedCount > len(data) {
		return ListRequest{}, errors.New("truncated allowed ops")
	}
	if allowedCount > 0 {
		req.AllowedOperations = make([]uint8, allowedCount)
		copy(req.AllowedOperations, data[offset:offset+allowedCount])
		offset += allowedCount
	}

	if offset+2 > len(data) {
		return ListRequest{}, errors.New("truncated select count")
	}
	selectCount := int(binary.LittleEndian.Uint16(data[offset : offset+2]))
	offset += 2
	for i := 0; i < selectCount; i++ {
		s, nextOffset, err := readString(data, offset)
		if err != nil {
			return ListRequest{}, err
		}
		req.Select = append(req.Select, s)
		offset = nextOffset
	}

	if offset+2 > len(data) {
		return ListRequest{}, errors.New("truncated filters count")
	}
	filterCount := int(binary.LittleEndian.Uint16(data[offset : offset+2]))
	offset += 2
	for i := 0; i < filterCount; i++ {
		fc, nextOffset, err := readFilterCondition(data, offset)
		if err != nil {
			return ListRequest{}, err
		}
		req.Filters = append(req.Filters, fc)
		offset = nextOffset
	}

	if offset+2 <= len(data) {
		compCount := int(binary.LittleEndian.Uint16(data[offset : offset+2]))
		offset += 2
		if compCount > 0 {
			req.Computations = make(map[string]storage.ComputeOp, compCount)
			for i := 0; i < compCount; i++ {
				outField, nextOffset, err := readString(data, offset)
				if err != nil {
					return ListRequest{}, err
				}
				op, nextOffset, err := readComputeOp(data, nextOffset)
				if err != nil {
					return ListRequest{}, err
				}
				req.Computations[outField] = op
				offset = nextOffset
			}
		}
	}

	return req, nil
}

// EncodeListResponse serializes ListResponse into wire bytes.
func EncodeListResponse(resp ListResponse) []byte {
	buf := make([]byte, 9)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(resp.TotalCount))
	if resp.HasMore {
		buf[4] = 1
	} else {
		buf[4] = 0
	}
	binary.LittleEndian.PutUint32(buf[5:9], uint32(len(resp.Items)))

	for _, item := range resp.Items {
		buf = appendString(buf, item.Name)
		buf = append(buf, item.Operation)
		var countBytes [2]byte
		binary.LittleEndian.PutUint16(countBytes[:], uint16(len(item.Props)))
		buf = append(buf, countBytes[:]...)
		for k, v := range item.Props {
			buf = appendString(buf, k)
			var valBytes [4]byte
			binary.LittleEndian.PutUint32(valBytes[:], math.Float32bits(v))
			buf = append(buf, valBytes[:]...)
		}
	}
	return buf
}

// DecodeListResponse deserializes ListResponse from wire bytes.
func DecodeListResponse(data []byte) (ListResponse, error) {
	if len(data) < 9 {
		return ListResponse{}, errors.New("truncated list response header")
	}
	resp := ListResponse{
		TotalCount: int(binary.LittleEndian.Uint32(data[0:4])),
		HasMore:    data[4] == 1,
	}
	itemCount := int(binary.LittleEndian.Uint32(data[5:9]))
	offset := 9

	resp.Items = make([]storage.EntityRecord, 0, itemCount)
	for i := 0; i < itemCount; i++ {
		name, nextOffset, err := readString(data, offset)
		if err != nil {
			return ListResponse{}, err
		}
		if nextOffset+3 > len(data) {
			return ListResponse{}, errors.New("truncated item header")
		}
		op := data[nextOffset]
		propCount := int(binary.LittleEndian.Uint16(data[nextOffset+1 : nextOffset+3]))
		offset = nextOffset + 3

		props := make(map[string]float32, propCount)
		for p := 0; p < propCount; p++ {
			k, nOffset, err := readString(data, offset)
			if err != nil {
				return ListResponse{}, err
			}
			if nOffset+4 > len(data) {
				return ListResponse{}, errors.New("truncated property float value")
			}
			val := math.Float32frombits(binary.LittleEndian.Uint32(data[nOffset : nOffset+4]))
			props[k] = val
			offset = nOffset + 4
		}

		resp.Items = append(resp.Items, storage.EntityRecord{
			Name:      name,
			Operation: op,
			Props:     props,
		})
	}

	return resp, nil
}

// --- UPDATE CODECS ---

// UpdateRequest specifies UpdateOptions in wire format.
type UpdateRequest struct {
	Limit          int
	Offset         int
	TargetEntities []string
	Filters        []storage.FilterCondition
	Mutations      map[string]storage.ComputeOp
}

// EncodeUpdateRequest serializes UpdateRequest into wire bytes.
func EncodeUpdateRequest(req UpdateRequest) []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(req.Limit))
	binary.LittleEndian.PutUint32(buf[4:8], uint32(req.Offset))

	var countBytes [4]byte
	binary.LittleEndian.PutUint32(countBytes[:], uint32(len(req.TargetEntities)))
	buf = append(buf, countBytes[:]...)
	for _, e := range req.TargetEntities {
		buf = appendString(buf, e)
	}

	var count16 [2]byte
	binary.LittleEndian.PutUint16(count16[:], uint16(len(req.Filters)))
	buf = append(buf, count16[:]...)
	for _, f := range req.Filters {
		buf = appendFilterCondition(buf, f)
	}

	binary.LittleEndian.PutUint16(count16[:], uint16(len(req.Mutations)))
	buf = append(buf, count16[:]...)
	for k, m := range req.Mutations {
		buf = appendString(buf, k)
		buf = appendComputeOp(buf, m)
	}

	return buf
}

// DecodeUpdateRequest deserializes UpdateRequest from wire bytes.
func DecodeUpdateRequest(data []byte) (UpdateRequest, error) {
	if len(data) < 12 {
		return UpdateRequest{}, errors.New("truncated update request")
	}
	req := UpdateRequest{
		Limit:  int(binary.LittleEndian.Uint32(data[0:4])),
		Offset: int(binary.LittleEndian.Uint32(data[4:8])),
	}
	targetCount := int(binary.LittleEndian.Uint32(data[8:12]))
	offset := 12

	for i := 0; i < targetCount; i++ {
		ent, nextOffset, err := readString(data, offset)
		if err != nil {
			return UpdateRequest{}, err
		}
		req.TargetEntities = append(req.TargetEntities, ent)
		offset = nextOffset
	}

	if offset+2 > len(data) {
		return UpdateRequest{}, errors.New("truncated filter count")
	}
	filterCount := int(binary.LittleEndian.Uint16(data[offset : offset+2]))
	offset += 2
	for i := 0; i < filterCount; i++ {
		fc, nextOffset, err := readFilterCondition(data, offset)
		if err != nil {
			return UpdateRequest{}, err
		}
		req.Filters = append(req.Filters, fc)
		offset = nextOffset
	}

	if offset+2 <= len(data) {
		mutCount := int(binary.LittleEndian.Uint16(data[offset : offset+2]))
		offset += 2
		if mutCount > 0 {
			req.Mutations = make(map[string]storage.ComputeOp, mutCount)
			for i := 0; i < mutCount; i++ {
				outField, nextOffset, err := readString(data, offset)
				if err != nil {
					return UpdateRequest{}, err
				}
				op, nextOffset, err := readComputeOp(data, nextOffset)
				if err != nil {
					return UpdateRequest{}, err
				}
				req.Mutations[outField] = op
				offset = nextOffset
			}
		}
	}

	return req, nil
}

// EncodeUpdateResponse serializes the updated count.
func EncodeUpdateResponse(count int) []byte {
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], uint32(count))
	return b[:]
}

// DecodeUpdateResponse deserializes the updated count.
func DecodeUpdateResponse(data []byte) (int, error) {
	if len(data) < 4 {
		return 0, errors.New("truncated update response")
	}
	return int(binary.LittleEndian.Uint32(data[0:4])), nil
}

// --- GRAPH & CHAIN CODECS ---

// GraphRequest specifies a spatial graph beam step query in wire format.
type GraphRequest struct {
	Inputs    []string
	Relation  string
	LimitK    int
	OrderMode uint8
	BeamWidth float32
}

// GraphResponsePayload holds the results of OpGraph.
type GraphResponsePayload struct {
	Anchor  spatial.Point2D
	Results []storage.GraphQueryResult
}

// EncodeGraphRequest serializes GraphRequest.
func EncodeGraphRequest(req GraphRequest) []byte {
	buf := appendString(nil, req.Relation)
	var b [9]byte
	binary.LittleEndian.PutUint32(b[0:4], uint32(req.LimitK))
	b[4] = req.OrderMode
	binary.LittleEndian.PutUint32(b[5:9], math.Float32bits(req.BeamWidth))
	buf = append(buf, b[:]...)

	var countBytes [2]byte
	binary.LittleEndian.PutUint16(countBytes[:], uint16(len(req.Inputs)))
	buf = append(buf, countBytes[:]...)
	for _, in := range req.Inputs {
		buf = appendString(buf, in)
	}
	return buf
}

// DecodeGraphRequest deserializes GraphRequest.
func DecodeGraphRequest(data []byte) (GraphRequest, error) {
	rel, offset, err := readString(data, 0)
	if err != nil {
		return GraphRequest{}, err
	}
	if offset+11 > len(data) {
		return GraphRequest{}, errors.New("truncated graph request header")
	}
	limitK := int(binary.LittleEndian.Uint32(data[offset : offset+4]))
	orderMode := data[offset+4]
	beamWidth := math.Float32frombits(binary.LittleEndian.Uint32(data[offset+5 : offset+9]))
	inputCount := int(binary.LittleEndian.Uint16(data[offset+9 : offset+11]))
	offset += 11

	inputs := make([]string, 0, inputCount)
	for i := 0; i < inputCount; i++ {
		in, nextOffset, err := readString(data, offset)
		if err != nil {
			return GraphRequest{}, err
		}
		inputs = append(inputs, in)
		offset = nextOffset
	}

	return GraphRequest{
		Inputs:    inputs,
		Relation:  rel,
		LimitK:    limitK,
		OrderMode: orderMode,
		BeamWidth: beamWidth,
	}, nil
}

// EncodeGraphResponsePayload serializes GraphResponsePayload.
func EncodeGraphResponsePayload(resp GraphResponsePayload) []byte {
	var b [8]byte
	binary.LittleEndian.PutUint32(b[0:4], math.Float32bits(resp.Anchor.X))
	binary.LittleEndian.PutUint32(b[4:8], math.Float32bits(resp.Anchor.Y))
	buf := append([]byte(nil), b[:]...)

	var countBytes [4]byte
	binary.LittleEndian.PutUint32(countBytes[:], uint32(len(resp.Results)))
	buf = append(buf, countBytes[:]...)

	for _, r := range resp.Results {
		buf = appendString(buf, r.Word)
		var valBytes [20]byte
		binary.LittleEndian.PutUint32(valBytes[0:4], math.Float32bits(r.Score))
		binary.LittleEndian.PutUint32(valBytes[4:8], math.Float32bits(r.T))
		binary.LittleEndian.PutUint32(valBytes[8:12], math.Float32bits(r.DistSqPerp))
		binary.LittleEndian.PutUint32(valBytes[12:16], math.Float32bits(r.Coord.X))
		binary.LittleEndian.PutUint32(valBytes[16:20], math.Float32bits(r.Coord.Y))
		buf = append(buf, valBytes[:]...)
	}
	return buf
}

// DecodeGraphResponsePayload deserializes GraphResponsePayload.
func DecodeGraphResponsePayload(data []byte) (GraphResponsePayload, error) {
	if len(data) < 12 {
		return GraphResponsePayload{}, errors.New("truncated graph response header")
	}
	anchor := spatial.Point2D{
		X: math.Float32frombits(binary.LittleEndian.Uint32(data[0:4])),
		Y: math.Float32frombits(binary.LittleEndian.Uint32(data[4:8])),
	}
	resultCount := int(binary.LittleEndian.Uint32(data[8:12]))
	offset := 12

	results := make([]storage.GraphQueryResult, 0, resultCount)
	for i := 0; i < resultCount; i++ {
		word, nextOffset, err := readString(data, offset)
		if err != nil {
			return GraphResponsePayload{}, err
		}
		if nextOffset+20 > len(data) {
			return GraphResponsePayload{}, errors.New("truncated graph result item")
		}
		score := math.Float32frombits(binary.LittleEndian.Uint32(data[nextOffset : nextOffset+4]))
		tVal := math.Float32frombits(binary.LittleEndian.Uint32(data[nextOffset+4 : nextOffset+8]))
		distSqPerp := math.Float32frombits(binary.LittleEndian.Uint32(data[nextOffset+8 : nextOffset+12]))
		x := math.Float32frombits(binary.LittleEndian.Uint32(data[nextOffset+12 : nextOffset+16]))
		y := math.Float32frombits(binary.LittleEndian.Uint32(data[nextOffset+16 : nextOffset+20]))
		offset = nextOffset + 20

		results = append(results, storage.GraphQueryResult{
			Word:       word,
			Score:      score,
			T:          tVal,
			DistSqPerp: distSqPerp,
			Coord:      spatial.Point2D{X: x, Y: y},
		})
	}

	return GraphResponsePayload{Anchor: anchor, Results: results}, nil
}

// GraphChainRequest specifies a multi-hop graph chain query.
type GraphChainRequest struct {
	Inputs        []string
	RelationChain []string
	LimitK        int
	BeamWidth     float32
}

// EncodeGraphChainRequest serializes GraphChainRequest.
func EncodeGraphChainRequest(req GraphChainRequest) []byte {
	var b [8]byte
	binary.LittleEndian.PutUint32(b[0:4], uint32(req.LimitK))
	binary.LittleEndian.PutUint32(b[4:8], math.Float32bits(req.BeamWidth))
	buf := append([]byte(nil), b[:]...)

	var countBytes [2]byte
	binary.LittleEndian.PutUint16(countBytes[:], uint16(len(req.Inputs)))
	buf = append(buf, countBytes[:]...)
	for _, in := range req.Inputs {
		buf = appendString(buf, in)
	}

	binary.LittleEndian.PutUint16(countBytes[:], uint16(len(req.RelationChain)))
	buf = append(buf, countBytes[:]...)
	for _, rel := range req.RelationChain {
		buf = appendString(buf, rel)
	}
	return buf
}

// DecodeGraphChainRequest deserializes GraphChainRequest.
func DecodeGraphChainRequest(data []byte) (GraphChainRequest, error) {
	if len(data) < 10 {
		return GraphChainRequest{}, errors.New("truncated graph chain header")
	}
	limitK := int(binary.LittleEndian.Uint32(data[0:4]))
	beamWidth := math.Float32frombits(binary.LittleEndian.Uint32(data[4:8]))
	inputCount := int(binary.LittleEndian.Uint16(data[8:10]))
	offset := 10

	inputs := make([]string, 0, inputCount)
	for i := 0; i < inputCount; i++ {
		in, nextOffset, err := readString(data, offset)
		if err != nil {
			return GraphChainRequest{}, err
		}
		inputs = append(inputs, in)
		offset = nextOffset
	}

	if offset+2 > len(data) {
		return GraphChainRequest{}, errors.New("truncated relation chain count")
	}
	relCount := int(binary.LittleEndian.Uint16(data[offset : offset+2]))
	offset += 2

	relations := make([]string, 0, relCount)
	for i := 0; i < relCount; i++ {
		rel, nextOffset, err := readString(data, offset)
		if err != nil {
			return GraphChainRequest{}, err
		}
		relations = append(relations, rel)
		offset = nextOffset
	}

	return GraphChainRequest{
		Inputs:        inputs,
		RelationChain: relations,
		LimitK:        limitK,
		BeamWidth:     beamWidth,
	}, nil
}

// --- SCHEMA CODECS ---

// EncodeAttachSchemaPayload serializes a raw schema definition string.
func EncodeAttachSchemaPayload(schemaJSON string) []byte {
	return appendString(nil, schemaJSON)
}

// DecodeAttachSchemaPayload deserializes a raw schema definition string.
func DecodeAttachSchemaPayload(data []byte) (string, error) {
	s, _, err := readString(data, 0)
	return s, err
}

// EncodeGetSchemaPayload encodes schema response string.
func EncodeGetSchemaPayload(schemaJSON string) []byte {
	return appendString(nil, schemaJSON)
}

// DecodeGetSchemaPayload decodes schema response string.
func DecodeGetSchemaPayload(data []byte) (string, error) {
	s, _, err := readString(data, 0)
	return s, err
}

// --- MATH CODECS ---

// MathRequest specifies an in-storage math computation.
type MathRequest struct {
	Op     string // "distance", "centroid", "cosine_similarity", "nearest"
	Words  []string
	LimitK int
}

// MathResponse holds the result of OpMath.
type MathResponse struct {
	Value     float32
	Coord     spatial.Point2D
	Roles     [4]float32
	Neighbors []storage.GraphQueryResult
}

// EncodeMathRequest serializes MathRequest.
func EncodeMathRequest(req MathRequest) []byte {
	buf := appendString(nil, req.Op)
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], uint32(req.LimitK))
	buf = append(buf, b[:]...)

	var countBytes [2]byte
	binary.LittleEndian.PutUint16(countBytes[:], uint16(len(req.Words)))
	buf = append(buf, countBytes[:]...)
	for _, w := range req.Words {
		buf = appendString(buf, w)
	}
	return buf
}

// DecodeMathRequest deserializes MathRequest.
func DecodeMathRequest(data []byte) (MathRequest, error) {
	op, offset, err := readString(data, 0)
	if err != nil {
		return MathRequest{}, err
	}
	if offset+6 > len(data) {
		return MathRequest{}, errors.New("truncated math request")
	}
	limitK := int(binary.LittleEndian.Uint32(data[offset : offset+4]))
	wordCount := int(binary.LittleEndian.Uint16(data[offset+4 : offset+6]))
	offset += 6

	words := make([]string, 0, wordCount)
	for i := 0; i < wordCount; i++ {
		w, nextOffset, err := readString(data, offset)
		if err != nil {
			return MathRequest{}, err
		}
		words = append(words, w)
		offset = nextOffset
	}

	return MathRequest{Op: op, Words: words, LimitK: limitK}, nil
}

// EncodeMathResponse serializes MathResponse.
func EncodeMathResponse(resp MathResponse) []byte {
	var b [28]byte
	binary.LittleEndian.PutUint32(b[0:4], math.Float32bits(resp.Value))
	binary.LittleEndian.PutUint32(b[4:8], math.Float32bits(resp.Coord.X))
	binary.LittleEndian.PutUint32(b[8:12], math.Float32bits(resp.Coord.Y))
	for i := 0; i < 4; i++ {
		binary.LittleEndian.PutUint32(b[12+i*4:16+i*4], math.Float32bits(resp.Roles[i]))
	}
	buf := append([]byte(nil), b[:]...)

	var countBytes [4]byte
	binary.LittleEndian.PutUint32(countBytes[:], uint32(len(resp.Neighbors)))
	buf = append(buf, countBytes[:]...)
	for _, n := range resp.Neighbors {
		buf = appendString(buf, n.Word)
		var nb [20]byte
		binary.LittleEndian.PutUint32(nb[0:4], math.Float32bits(n.Score))
		binary.LittleEndian.PutUint32(nb[4:8], math.Float32bits(n.T))
		binary.LittleEndian.PutUint32(nb[8:12], math.Float32bits(n.DistSqPerp))
		binary.LittleEndian.PutUint32(nb[12:16], math.Float32bits(n.Coord.X))
		binary.LittleEndian.PutUint32(nb[16:20], math.Float32bits(n.Coord.Y))
		buf = append(buf, nb[:]...)
	}
	return buf
}

// DecodeMathResponse deserializes MathResponse.
func DecodeMathResponse(data []byte) (MathResponse, error) {
	if len(data) < 32 {
		return MathResponse{}, errors.New("truncated math response")
	}
	val := math.Float32frombits(binary.LittleEndian.Uint32(data[0:4]))
	coord := spatial.Point2D{
		X: math.Float32frombits(binary.LittleEndian.Uint32(data[4:8])),
		Y: math.Float32frombits(binary.LittleEndian.Uint32(data[8:12])),
	}
	var roles [4]float32
	for i := 0; i < 4; i++ {
		roles[i] = math.Float32frombits(binary.LittleEndian.Uint32(data[12+i*4 : 16+i*4]))
	}
	neighborCount := int(binary.LittleEndian.Uint32(data[28:32]))
	offset := 32

	neighbors := make([]storage.GraphQueryResult, 0, neighborCount)
	for i := 0; i < neighborCount; i++ {
		w, nextOffset, err := readString(data, offset)
		if err != nil {
			return MathResponse{}, err
		}
		if nextOffset+20 > len(data) {
			return MathResponse{}, errors.New("truncated math neighbor item")
		}
		sc := math.Float32frombits(binary.LittleEndian.Uint32(data[nextOffset : nextOffset+4]))
		tVal := math.Float32frombits(binary.LittleEndian.Uint32(data[nextOffset+4 : nextOffset+8]))
		distSq := math.Float32frombits(binary.LittleEndian.Uint32(data[nextOffset+8 : nextOffset+12]))
		x := math.Float32frombits(binary.LittleEndian.Uint32(data[nextOffset+12 : nextOffset+16]))
		y := math.Float32frombits(binary.LittleEndian.Uint32(data[nextOffset+16 : nextOffset+20]))
		offset = nextOffset + 20

		neighbors = append(neighbors, storage.GraphQueryResult{
			Word:       w,
			Score:      sc,
			T:          tVal,
			DistSqPerp: distSq,
			Coord:      spatial.Point2D{X: x, Y: y},
		})
	}

	return MathResponse{
		Value:     val,
		Coord:     coord,
		Roles:     roles,
		Neighbors: neighbors,
	}, nil
}

// --- RESET MODEL CODECS ---

// EncodeResetModelPayload encodes the confirm flag for OpResetModel.
func EncodeResetModelPayload(confirm bool) []byte {
	if confirm {
		return []byte{1}
	}
	return []byte{0}
}

// DecodeResetModelPayload decodes the confirm flag for OpResetModel.
func DecodeResetModelPayload(data []byte) (bool, error) {
	if len(data) < 1 {
		return false, errors.New("truncated reset model payload")
	}
	return data[0] == 1, nil
}

// --- PARTITION CATALOG & INSPECT CODECS ---

// PartitionCatalogItemDTO represents a single partition catalog entry over AnchorWire.
type PartitionCatalogItemDTO struct {
	ID             string    `json:"id"`
	Model          string    `json:"model"`
	PartitionFile  string    `json:"partition_file"`
	TargetLocation string    `json:"target_location"`
	SinkType       string    `json:"sink_type"`
	RecordCount    int       `json:"record_count"`
	CutoffDate     string    `json:"cutoff_date"`
	MinDate        float32   `json:"min_date"`
	MaxDate        float32   `json:"max_date"`
	CreatedAt      time.Time `json:"created_at"`
	SizeBytes      int64     `json:"size_bytes"`
}

// EncodePartitionCatalogRequest encodes model filter for OpPartitionCatalog.
func EncodePartitionCatalogRequest(model string) []byte {
	return []byte(model)
}

// DecodePartitionCatalogRequest decodes model filter for OpPartitionCatalog.
func DecodePartitionCatalogRequest(data []byte) (string, error) {
	return string(data), nil
}

// EncodePartitionCatalogResponse encodes catalog items for OpPartitionCatalog.
func EncodePartitionCatalogResponse(items []PartitionCatalogItemDTO) ([]byte, error) {
	return json.Marshal(items)
}

// DecodePartitionCatalogResponse decodes catalog items from OpPartitionCatalog response.
func DecodePartitionCatalogResponse(data []byte) ([]PartitionCatalogItemDTO, error) {
	var items []PartitionCatalogItemDTO
	if err := json.Unmarshal(data, &items); err != nil {
		return nil, fmt.Errorf("failed decoding partition catalog response: %w", err)
	}
	return items, nil
}

// PartitionInspectWireRequest represents parameters for OpPartitionInspect.
type PartitionInspectWireRequest struct {
	PartitionFile string `json:"partition_file"`
	RecordName    string `json:"record_name,omitempty"`
	Limit         int    `json:"limit,omitempty"`
}

// PartitionRecordWireItem holds a record in an inspected partition.
type PartitionRecordWireItem struct {
	Name  string             `json:"name"`
	Props map[string]float32 `json:"props"`
}

// PartitionInspectWireResponse represents result of OpPartitionInspect.
type PartitionInspectWireResponse struct {
	PartitionFile string                    `json:"partition_file"`
	RecordName    string                    `json:"record_name,omitempty"`
	Props         map[string]float32        `json:"props,omitempty"`
	Records       []PartitionRecordWireItem `json:"records,omitempty"`
}

// EncodePartitionInspectRequest encodes request for OpPartitionInspect.
func EncodePartitionInspectRequest(req PartitionInspectWireRequest) ([]byte, error) {
	return json.Marshal(req)
}

// DecodePartitionInspectRequest decodes request for OpPartitionInspect.
func DecodePartitionInspectRequest(data []byte) (PartitionInspectWireRequest, error) {
	var req PartitionInspectWireRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return req, fmt.Errorf("failed decoding partition inspect request: %w", err)
	}
	return req, nil
}

// EncodePartitionInspectResponse encodes response for OpPartitionInspect.
func EncodePartitionInspectResponse(resp PartitionInspectWireResponse) ([]byte, error) {
	return json.Marshal(resp)
}

// DecodePartitionInspectResponse decodes response for OpPartitionInspect.
func DecodePartitionInspectResponse(data []byte) (PartitionInspectWireResponse, error) {
	var resp PartitionInspectWireResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return resp, fmt.Errorf("failed decoding partition inspect response: %w", err)
	}
	return resp, nil
}

