package wire

import (
	"bytes"
	"testing"

	"github.com/rachmanzz/anchordb/pkg/spatial"
	"github.com/rachmanzz/anchordb/pkg/storage"
)

func TestWire_FrameRoundtrip(t *testing.T) {
	orig := Frame{
		Magic:   Magic,
		Version: Version,
		OpCode:  OpSave,
		Flags:   FlagNone,
		Seq:     42,
		Payload: []byte("test payload content"),
	}

	var buf bytes.Buffer
	if err := WriteFrame(&buf, orig); err != nil {
		t.Fatalf("WriteFrame failed: %v", err)
	}

	decoded, err := ReadFrame(&buf)
	if err != nil {
		t.Fatalf("ReadFrame failed: %v", err)
	}

	if decoded.OpCode != orig.OpCode {
		t.Errorf("expected OpCode 0x%02X, got 0x%02X", orig.OpCode, decoded.OpCode)
	}
	if decoded.Seq != orig.Seq {
		t.Errorf("expected Seq %d, got %d", orig.Seq, decoded.Seq)
	}
	if !bytes.Equal(decoded.Payload, orig.Payload) {
		t.Errorf("payload mismatch: expected %q, got %q", orig.Payload, decoded.Payload)
	}
}

func TestWire_CorruptChecksum(t *testing.T) {
	orig := Frame{
		Magic:   Magic,
		Version: Version,
		OpCode:  OpPing,
		Seq:     1,
		Payload: []byte("hello"),
	}

	var buf bytes.Buffer
	if err := WriteFrame(&buf, orig); err != nil {
		t.Fatalf("WriteFrame failed: %v", err)
	}

	// Corrupt payload byte
	data := buf.Bytes()
	data[len(data)-1] ^= 0xFF

	r := bytes.NewReader(data)
	if _, err := ReadFrame(r); err == nil {
		t.Fatalf("expected checksum error on corrupted payload")
	}
}

func TestWire_SavePayload(t *testing.T) {
	item := storage.BatchRecordItem{
		Name:      "sku_test_01",
		Operation: storage.OpStatusActive,
		Props: map[string]float32{
			"price": 19.99,
			"stock": 250.0,
		},
		Coord: &spatial.Point2D{X: -6.2, Y: 106.8},
		Roles: &[4]float32{0.5, 0.5, 0.1, 0.1},
	}

	encoded := EncodeSavePayload(item)
	decoded, err := DecodeSavePayload(encoded)
	if err != nil {
		t.Fatalf("DecodeSavePayload failed: %v", err)
	}

	if decoded.Name != item.Name {
		t.Errorf("expected name %s, got %s", item.Name, decoded.Name)
	}
	if decoded.Operation != item.Operation {
		t.Errorf("expected op %d, got %d", item.Operation, decoded.Operation)
	}
	if decoded.Props["price"] != item.Props["price"] {
		t.Errorf("expected price %f, got %f", item.Props["price"], decoded.Props["price"])
	}
	if decoded.Coord == nil || decoded.Coord.X != item.Coord.X {
		t.Errorf("expected coord %+v, got %+v", item.Coord, decoded.Coord)
	}
}

func TestWire_SaveBatchPayload(t *testing.T) {
	records := []storage.BatchRecordItem{
		{
			Name:      "item_1",
			Operation: storage.OpStatusActive,
			Props:     map[string]float32{"a": 1.0},
		},
		{
			Name:      "item_2",
			Operation: storage.OpStatusActive,
			Props:     map[string]float32{"b": 2.5},
		},
	}

	encoded := EncodeSaveBatchPayload(records)
	decoded, err := DecodeSaveBatchPayload(encoded)
	if err != nil {
		t.Fatalf("DecodeSaveBatchPayload failed: %v", err)
	}

	if len(decoded) != len(records) {
		t.Fatalf("expected %d records, got %d", len(records), len(decoded))
	}
	if decoded[0].Name != "item_1" || decoded[1].Props["b"] != 2.5 {
		t.Errorf("unexpected decoded batch: %+v", decoded)
	}
}

func TestWire_EdgePayload(t *testing.T) {
	action, src, rel, tgt, weight := "add", "node_a", "connects_to", "node_b", float32(0.85)
	encoded := EncodeEdgePayload(action, src, rel, tgt, weight)

	dAct, dSrc, dRel, dTgt, dWeight, err := DecodeEdgePayload(encoded)
	if err != nil {
		t.Fatalf("DecodeEdgePayload failed: %v", err)
	}
	if dAct != action || dSrc != src || dRel != rel || dTgt != tgt || dWeight != weight {
		t.Fatalf("decoded edge mismatch: got %s, %s, %s, %s, %f", dAct, dSrc, dRel, dTgt, dWeight)
	}
}

func TestWire_CausalPayload(t *testing.T) {
	seeds := []string{"hub_1", "hub_2"}
	rel := "routes"
	limitK := 10

	encoded := EncodeCausalPayload(seeds, rel, limitK)
	dSeeds, dRel, dK, err := DecodeCausalPayload(encoded)
	if err != nil {
		t.Fatalf("DecodeCausalPayload failed: %v", err)
	}
	if len(dSeeds) != 2 || dSeeds[0] != "hub_1" || dRel != rel || dK != limitK {
		t.Fatalf("causal payload mismatch: got %v, %s, %d", dSeeds, dRel, dK)
	}

	results := []storage.CausalResult{
		{Word: "sku_1", Score: 0.95},
		{Word: "sku_2", Score: 0.45},
	}
	resBytes := EncodeCausalResultPayload(results)
	dResults, err := DecodeCausalResultPayload(resBytes)
	if err != nil {
		t.Fatalf("DecodeCausalResultPayload failed: %v", err)
	}
	if len(dResults) != 2 || dResults[0].Word != "sku_1" || dResults[0].Score != 0.95 {
		t.Fatalf("causal result mismatch: got %+v", dResults)
	}
}

func TestWire_ErrorPayload(t *testing.T) {
	code := uint16(404)
	msg := "record not found"

	encoded := EncodeErrorPayload(code, msg)
	dCode, dMsg, err := DecodeErrorPayload(encoded)
	if err != nil {
		t.Fatalf("DecodeErrorPayload failed: %v", err)
	}
	if dCode != code || dMsg != msg {
		t.Errorf("error payload mismatch: got %d, %s", dCode, dMsg)
	}
}

func TestWire_MemoryNegotiationPayload(t *testing.T) {
	req := MemoryNegotiationRequest{
		TargetRecords:            1000000,
		EstimatedRecordSizeBytes: 512,
		ClientBatchSize:          1000,
	}

	reqBytes := EncodeMemoryNegotiationRequest(req)
	dReq, err := DecodeMemoryNegotiationRequest(reqBytes)
	if err != nil {
		t.Fatalf("DecodeMemoryNegotiationRequest failed: %v", err)
	}
	if dReq != req {
		t.Fatalf("negotiation request mismatch: got %+v, want %+v", dReq, req)
	}

	resp := MemoryNegotiationResponse{
		Status:             MemoryStatusThrottled,
		HostUsedPct:        60,
		HeadroomBytes:      5 * 1024 * 1024 * 1024,
		AllocatedBatchSize: 500,
		ReleaseIntervalMs:  15,
	}

	respBytes := EncodeMemoryNegotiationResponse(resp)
	dResp, err := DecodeMemoryNegotiationResponse(respBytes)
	if err != nil {
		t.Fatalf("DecodeMemoryNegotiationResponse failed: %v", err)
	}
	if dResp != resp {
		t.Fatalf("negotiation response mismatch: got %+v, want %+v", dResp, resp)
	}
}
