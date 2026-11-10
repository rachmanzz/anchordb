package wire

import (
	"bytes"
	"encoding/binary"
	"math"
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

func TestWire_GetRequestPayload(t *testing.T) {
	req := GetRequest{
		Name:   "item_speed",
		Select: []string{"distance", "duration", "speed"},
		Computations: map[string]storage.ComputeOp{
			"speed": storage.OpDivide{FieldA: "distance", FieldB: "duration"},
		},
	}
	encoded := EncodeGetRequest(req)
	dReq, err := DecodeGetRequest(encoded)
	if err != nil {
		t.Fatalf("DecodeGetRequest failed: %v", err)
	}
	if dReq.Name != req.Name || len(dReq.Select) != 3 || len(dReq.Computations) != 1 {
		t.Fatalf("unexpected decoded get request: %+v", dReq)
	}
}

func TestWire_ListRequestPayload(t *testing.T) {
	opVal := storage.OpStatusActive
	req := ListRequest{
		Limit:                25,
		Offset:               50,
		IncludeAllOperations: true,
		OperationFilter:      &opVal,
		AllowedOperations:    []uint8{storage.OpStatusActive, storage.OpStatusTombstone},
		Select:               []string{"price", "stock", "revenue"},
		Filters: []storage.FilterCondition{
			{Field: "price", Operator: ">", Value: 10.0},
			{Field: "stock", Operator: "BETWEEN", Value: 5.0, ValueHigh: 100.0},
		},
		Computations: map[string]storage.ComputeOp{
			"revenue": storage.OpMultiply{FieldA: "price", FieldB: "stock"},
		},
	}
	encoded := EncodeListRequest(req)
	dReq, err := DecodeListRequest(encoded)
	if err != nil {
		t.Fatalf("DecodeListRequest failed: %v", err)
	}
	if dReq.Limit != 25 || dReq.Offset != 50 || !dReq.IncludeAllOperations {
		t.Fatalf("header fields mismatch: %+v", dReq)
	}
	if dReq.OperationFilter == nil || *dReq.OperationFilter != opVal {
		t.Fatalf("operation filter mismatch: %+v", dReq.OperationFilter)
	}
	if len(dReq.AllowedOperations) != 2 || len(dReq.Select) != 3 || len(dReq.Filters) != 2 || len(dReq.Computations) != 1 {
		t.Fatalf("slices/maps mismatch in list request: %+v", dReq)
	}

	resp := ListResponse{
		TotalCount: 1,
		HasMore:    true,
		Items: []storage.EntityRecord{
			{
				Name:      "item_1",
				Operation: storage.OpStatusActive,
				Props:     map[string]float32{"price": 10.5, "stock": 50},
			},
		},
	}
	respBytes := EncodeListResponse(resp)
	dResp, err := DecodeListResponse(respBytes)
	if err != nil {
		t.Fatalf("DecodeListResponse failed: %v", err)
	}
	if dResp.TotalCount != 1 || !dResp.HasMore || len(dResp.Items) != 1 || dResp.Items[0].Props["price"] != 10.5 {
		t.Fatalf("unexpected list response: %+v", dResp)
	}
}

func TestWire_ListResponsePayload_Typed(t *testing.T) {
	largeInt := int64(9007199254740993)
	preciseFloat := 123456.789012345
	largeUint := uint64(18446744073709551614)

	resp := ListResponse{
		TotalCount: 1,
		HasMore:    false,
		Items: []storage.EntityRecord{
			{
				Name:      "item_typed",
				Operation: storage.OpStatusActive,
				Props:     map[string]float32{"legacy": 10.5},
				Attrs: []storage.AttrEntry{
					storage.NewInt64Attr("big_id", largeInt),
					storage.NewUint64Attr("huge_uint", largeUint),
					storage.NewFloat64Attr("precise_f64", preciseFloat),
					storage.NewFloat32Attr("legacy", 10.5),
				},
			},
		},
	}
	respBytes := EncodeListResponse(resp)
	dResp, err := DecodeListResponse(respBytes)
	if err != nil {
		t.Fatalf("DecodeListResponse failed: %v", err)
	}
	if len(dResp.Items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(dResp.Items))
	}
	item := dResp.Items[0]
	if len(item.Attrs) != 4 {
		t.Fatalf("expected 4 attrs, got %d", len(item.Attrs))
	}

	var foundInt int64
	var foundUint uint64
	var foundFloat float64
	for _, a := range item.Attrs {
		switch a.Key {
		case "big_id":
			foundInt = a.Int64()
		case "huge_uint":
			foundUint = a.Uint64()
		case "precise_f64":
			foundFloat = a.Float64()
		}
	}
	if foundInt != largeInt {
		t.Fatalf("expected big_id %d, got %d", largeInt, foundInt)
	}
	if foundUint != largeUint {
		t.Fatalf("expected huge_uint %d, got %d", largeUint, foundUint)
	}
	if foundFloat != preciseFloat {
		t.Fatalf("expected precise_f64 %f, got %f", preciseFloat, foundFloat)
	}
	// Verify legacy Props map was also transparently populated
	if item.Props["legacy"] != 10.5 {
		t.Fatalf("expected legacy prop 10.5, got %v", item.Props["legacy"])
	}
}

func TestWire_FilterCondition_64BitExact(t *testing.T) {
	largeInt := int64(9007199254740993)
	largeUint := uint64(18446744073709551614)

	condInt := storage.FilterCondition{
		Field:          "user_id",
		Operator:       "=",
		ValueInt64:     largeInt,
		ValueHighInt64: largeInt,
		ValType:        storage.AttrTypeInt64,
	}
	buf := appendFilterCondition(nil, condInt)
	dCondInt, _, err := readFilterCondition(buf, 0)
	if err != nil {
		t.Fatalf("readFilterCondition failed: %v", err)
	}
	if dCondInt.ValType != storage.AttrTypeInt64 || dCondInt.ValueInt64 != largeInt || dCondInt.Operator != "=" {
		t.Fatalf("unexpected decoded int filter: %+v", dCondInt)
	}

	condUint := storage.FilterCondition{
		Field:           "tx_id",
		Operator:        ">=",
		ValueUint64:     largeUint,
		ValueHighUint64: largeUint,
		ValType:         storage.AttrTypeUint64,
	}
	bufUint := appendFilterCondition(nil, condUint)
	dCondUint, _, err := readFilterCondition(bufUint, 0)
	if err != nil {
		t.Fatalf("readFilterCondition uint failed: %v", err)
	}
	if dCondUint.ValType != storage.AttrTypeUint64 || dCondUint.ValueUint64 != largeUint || dCondUint.Operator != ">=" {
		t.Fatalf("unexpected decoded uint filter: %+v", dCondUint)
	}
}

func TestWire_UpdateRequestPayload(t *testing.T) {
	req := UpdateRequest{
		Limit:          100,
		Offset:         0,
		TargetEntities: []string{"user_1", "user_2"},
		Filters: []storage.FilterCondition{
			{Field: "points", Operator: ">=", Value: 50},
		},
		Mutations: map[string]storage.ComputeOp{
			"points": storage.OpAdd{FieldA: "points", FieldB: "bonus"},
		},
	}
	encoded := EncodeUpdateRequest(req)
	dReq, err := DecodeUpdateRequest(encoded)
	if err != nil {
		t.Fatalf("DecodeUpdateRequest failed: %v", err)
	}
	if dReq.Limit != 100 || len(dReq.TargetEntities) != 2 || len(dReq.Filters) != 1 || len(dReq.Mutations) != 1 {
		t.Fatalf("unexpected update request: %+v", dReq)
	}

	resBytes := EncodeUpdateResponse(42)
	dCount, err := DecodeUpdateResponse(resBytes)
	if err != nil || dCount != 42 {
		t.Fatalf("unexpected update response: count=%d, err=%v", dCount, err)
	}
}

func TestWire_GraphPayload(t *testing.T) {
	req := GraphRequest{
		Inputs:    []string{"seed_a", "seed_b"},
		Relation:  "route",
		LimitK:    5,
		OrderMode: uint8(storage.OrderByScoreDesc),
		BeamWidth: 0.75,
	}
	encoded := EncodeGraphRequest(req)
	dReq, err := DecodeGraphRequest(encoded)
	if err != nil {
		t.Fatalf("DecodeGraphRequest failed: %v", err)
	}
	if len(dReq.Inputs) != 2 || dReq.Relation != "route" || dReq.LimitK != 5 || dReq.BeamWidth != 0.75 {
		t.Fatalf("unexpected graph request: %+v", dReq)
	}

	resp := GraphResponsePayload{
		Anchor: spatial.Point2D{X: 1.5, Y: 2.5},
		Results: []storage.GraphQueryResult{
			{Word: "tgt_1", Score: 0.9, T: 1.2, DistSqPerp: 0.05, Coord: spatial.Point2D{X: 3.0, Y: 4.0}},
		},
	}
	respBytes := EncodeGraphResponsePayload(resp)
	dResp, err := DecodeGraphResponsePayload(respBytes)
	if err != nil {
		t.Fatalf("DecodeGraphResponsePayload failed: %v", err)
	}
	if dResp.Anchor.X != 1.5 || len(dResp.Results) != 1 || dResp.Results[0].Word != "tgt_1" || dResp.Results[0].T != 1.2 {
		t.Fatalf("unexpected graph response: %+v", dResp)
	}
}

func TestWire_GraphChainPayload(t *testing.T) {
	req := GraphChainRequest{
		Inputs:        []string{"start_hub"},
		RelationChain: []string{"routes_to", "supplies"},
		LimitK:        3,
		BeamWidth:     0.5,
	}
	encoded := EncodeGraphChainRequest(req)
	dReq, err := DecodeGraphChainRequest(encoded)
	if err != nil {
		t.Fatalf("DecodeGraphChainRequest failed: %v", err)
	}
	if len(dReq.Inputs) != 1 || len(dReq.RelationChain) != 2 || dReq.LimitK != 3 || dReq.BeamWidth != 0.5 {
		t.Fatalf("unexpected graph chain request: %+v", dReq)
	}
}

func TestWire_SchemaPayload(t *testing.T) {
	schemaJSON := `{"price":"float","quantity":"int"}`
	encoded := EncodeAttachSchemaPayload(schemaJSON)
	dSchema, err := DecodeAttachSchemaPayload(encoded)
	if err != nil || dSchema != schemaJSON {
		t.Fatalf("unexpected attach schema payload: got %q, want %q", dSchema, schemaJSON)
	}

	getEncoded := EncodeGetSchemaPayload(schemaJSON)
	dGetSchema, err := DecodeGetSchemaPayload(getEncoded)
	if err != nil || dGetSchema != schemaJSON {
		t.Fatalf("unexpected get schema payload: got %q, want %q", dGetSchema, schemaJSON)
	}
}

func TestWire_MathPayload(t *testing.T) {
	req := MathRequest{
		Op:     "nearest",
		Words:  []string{"cluster_node"},
		LimitK: 10,
	}
	encoded := EncodeMathRequest(req)
	dReq, err := DecodeMathRequest(encoded)
	if err != nil {
		t.Fatalf("DecodeMathRequest failed: %v", err)
	}
	if dReq.Op != "nearest" || len(dReq.Words) != 1 || dReq.LimitK != 10 {
		t.Fatalf("unexpected math request: %+v", dReq)
	}

	resp := MathResponse{
		Value: 3.14,
		Coord: spatial.Point2D{X: 10, Y: 20},
		Roles: [4]float32{1, 2, 3, 4},
		Neighbors: []storage.GraphQueryResult{
			{Word: "neighbor_1", Score: 0.88, T: 0.5, DistSqPerp: 0.1, Coord: spatial.Point2D{X: 11, Y: 21}},
		},
	}
	respBytes := EncodeMathResponse(resp)
	dResp, err := DecodeMathResponse(respBytes)
	if err != nil {
		t.Fatalf("DecodeMathResponse failed: %v", err)
	}
	if dResp.Value != 3.14 || dResp.Coord.X != 10 || len(dResp.Neighbors) != 1 || dResp.Neighbors[0].Word != "neighbor_1" {
		t.Fatalf("unexpected math response: %+v", dResp)
	}
}

func TestWire_ResetModelPayload(t *testing.T) {
	b := EncodeResetModelPayload(true)
	val, err := DecodeResetModelPayload(b)
	if err != nil || !val {
		t.Fatalf("unexpected reset model payload: got %v, err %v", val, err)
	}
}

func TestWire_EdgeWithDirectionPayload(t *testing.T) {
	dir := spatial.Point2D{X: 0, Y: -1}
	encoded := EncodeEdgePayloadWithDirection("set_relation", "", "down", "", 0, &dir)
	act, src, rel, tgt, w, dDir, err := DecodeEdgePayloadWithDirection(encoded)
	if err != nil {
		t.Fatalf("DecodeEdgePayloadWithDirection failed: %v", err)
	}
	if act != "set_relation" || rel != "down" || dDir == nil || dDir.Y != -1 {
		t.Fatalf("unexpected edge direction payload: act=%s, rel=%s, dir=%+v", act, rel, dDir)
	}
	_ = src
	_ = tgt
	_ = w
}

func TestWire_DecodeListResponse_BoundsAndTypeSafety(t *testing.T) {
	// 1. Reject oversized itemCount on 9-byte header without OOM
	malformedHeader := make([]byte, 9)
	binary.LittleEndian.PutUint32(malformedHeader[0:4], 1)
	malformedHeader[4] = 0
	binary.LittleEndian.PutUint32(malformedHeader[5:9], 0xFFFFFFFF)
	if _, err := DecodeListResponse(malformedHeader); err == nil {
		t.Fatalf("expected DecodeListResponse to reject oversized itemCount")
	}

	// 2. Reject invalid AttrType in typed list item
	validResp := EncodeListResponse(ListResponse{
		TotalCount: 1,
		Items: []storage.EntityRecord{
			{
				Name:      "rec1",
				Operation: storage.OpStatusActive,
				Attrs:     []storage.AttrEntry{storage.NewInt64Attr("k", 123)},
			},
		},
	})
	corrupted := append([]byte(nil), validResp...)
	// Offset: 9 (header) + 2+4 ("rec1") + 1 (op) + 2 (rawCount) + 2+1 ("k") = 21 -> attrType byte
	corrupted[21] = 99
	if _, err := DecodeListResponse(corrupted); err == nil {
		t.Fatalf("expected DecodeListResponse to reject unknown AttrType 99")
	}

	// 3. Mixed BETWEEN filter condition roundtrip (Int64 low, Uint64 high)
	listReq := ListRequest{
		Limit: 10,
		Filters: []storage.FilterCondition{
			{
				Field:           "val",
				Operator:        "BETWEEN",
				ValueInt64:      -50,
				ValueHighUint64: math.MaxUint64,
				ValType:         storage.AttrTypeInt64,
				ValHighType:     storage.AttrTypeUint64,
				HasHighType:     true,
				HasFloat64:      true,
			},
		},
	}
	reqBytes := EncodeListRequest(listReq)
	decodedReq, err := DecodeListRequest(reqBytes)
	if err != nil {
		t.Fatalf("DecodeListRequest failed: %v", err)
	}
	if len(decodedReq.Filters) != 1 {
		t.Fatalf("expected 1 filter, got %d", len(decodedReq.Filters))
	}
	fc := decodedReq.Filters[0]
	if !fc.HasHighType || fc.ValType != storage.AttrTypeInt64 || fc.ValHighType != storage.AttrTypeUint64 {
		t.Fatalf("unexpected decoded mixed filter types: %+v", fc)
	}
	if fc.ValueInt64 != -50 || fc.ValueHighUint64 != math.MaxUint64 {
		t.Fatalf("unexpected decoded mixed filter bounds: low=%d high=%d", fc.ValueInt64, fc.ValueHighUint64)
	}
}
