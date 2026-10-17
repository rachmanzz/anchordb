package wire

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"

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
	OpSave      byte = 0x10
	OpSaveBatch byte = 0x11
	OpGet       byte = 0x12
	OpEdge      byte = 0x13
	OpCausal    byte = 0x14
	OpSync      byte = 0x15

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

// DecodeAuthPayload decodes auth token
func DecodeAuthPayload(data []byte) (string, error) {
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

// EncodeEdgePayload serializes edge data for OpEdge
func EncodeEdgePayload(action, source, relation, target string, weight float32) []byte {
	var actByte byte
	if action == "remove" {
		actByte = 1
	}
	buf := []byte{actByte}
	buf = appendString(buf, source)
	buf = appendString(buf, relation)
	buf = appendString(buf, target)
	var wBytes [4]byte
	binary.LittleEndian.PutUint32(wBytes[:], math.Float32bits(weight))
	buf = append(buf, wBytes[:]...)
	return buf
}

// DecodeEdgePayload deserializes edge data from OpEdge
func DecodeEdgePayload(data []byte) (action, source, relation, target string, weight float32, err error) {
	if len(data) < 1 {
		return "", "", "", "", 0, errors.New("truncated edge payload")
	}
	if data[0] == 1 {
		action = "remove"
	} else {
		action = "add"
	}
	offset := 1
	source, offset, err = readString(data, offset)
	if err != nil {
		return "", "", "", "", 0, err
	}
	relation, offset, err = readString(data, offset)
	if err != nil {
		return "", "", "", "", 0, err
	}
	target, offset, err = readString(data, offset)
	if err != nil {
		return "", "", "", "", 0, err
	}
	if offset+4 > len(data) {
		return "", "", "", "", 0, errors.New("truncated edge weight")
	}
	weight = math.Float32frombits(binary.LittleEndian.Uint32(data[offset : offset+4]))
	return action, source, relation, target, weight, nil
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
