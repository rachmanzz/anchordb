package storage

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/rachmanzz/anchordb/pkg/spatial"
)

func TestSchema_ParseFieldSpec(t *testing.T) {
	tests := []struct {
		input    string
		baseType string
		isArray  bool
		optional bool
		wantErr  bool
	}{
		{"int", "int", false, false, false},
		{"float", "float", false, false, false},
		{"bool", "bool", false, false, false},
		{"date", "date", false, false, false},
		{"int?", "int", false, true, false},
		{"float?", "float", false, true, false},
		{"bool?", "bool", false, true, false},
		{"date?", "date", false, true, false},
		{"string", "", false, false, true},
		{"string?", "", false, false, true},
		{"string[]", "", false, false, true},
		{"float[]?", "", false, false, true},
		{"int[]?", "", false, false, true},
		{"unknown", "", false, false, true},
		{"", "", false, false, true},
	}

	for _, tt := range tests {
		spec, err := ParseFieldSpec(tt.input)
		if tt.wantErr {
			if err == nil {
				t.Errorf("ParseFieldSpec(%q) expected error, got nil", tt.input)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseFieldSpec(%q) unexpected error: %v", tt.input, err)
			continue
		}
		if spec.BaseType != tt.baseType || spec.IsArray != tt.isArray || spec.Optional != tt.optional {
			t.Errorf("ParseFieldSpec(%q) got %+v, want base=%s isArray=%v opt=%v",
				tt.input, spec, tt.baseType, tt.isArray, tt.optional)
		}
	}
}

func TestSchema_AttachCompareAndValidation(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "vehicles.anchordb")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	// Unsupported string / array types must be rejected at AttachSchema
	if err := db.AttachSchema(ModelSchema{"color": "string"}); !errors.Is(err, ErrInvalidFieldType) {
		t.Fatalf("expected ErrInvalidFieldType for string schema field, got %v", err)
	}
	if err := db.AttachSchema(ModelSchema{"embedding": "float[]"}); !errors.Is(err, ErrInvalidFieldType) {
		t.Fatalf("expected ErrInvalidFieldType for array schema field, got %v", err)
	}

	schema1 := ModelSchema{
		"color_code": "int",
		"age":        "int",
		"speed":      "float",
		"is_active":  "bool?",
		"registered": "date?",
	}

	// 1. Initial Attach must succeed
	if err := db.AttachSchema(schema1); err != nil {
		t.Fatalf("first AttachSchema failed: %v", err)
	}

	// 2. Attach with identical schema must succeed
	if err := db.AttachSchema(schema1); err != nil {
		t.Fatalf("subsequent identical AttachSchema failed: %v", err)
	}

	// 3. Attach with mismatched schema must be REJECTED
	mismatchedSchema := ModelSchema{
		"color_code": "int",
		"age":        "float", // Type mismatch: float instead of int
	}
	err = db.AttachSchema(mismatchedSchema)
	if !errors.Is(err, ErrSchemaMismatch) {
		t.Fatalf("expected ErrSchemaMismatch, got %v", err)
	}

	// 4. Missing required field on Save must fail
	err = db.SaveRecord("car_01", map[string]float32{
		"color_code": 1.0,
		// "age" missing!
		"speed": 80.0,
	})
	if !errors.Is(err, ErrMissingRequiredField) {
		t.Fatalf("expected ErrMissingRequiredField, got %v", err)
	}

	// 5. Unknown field on Save must fail
	err = db.SaveRecord("car_01", map[string]float32{
		"color_code":  1.0,
		"age":         3.0,
		"speed":       80.0,
		"rogue_field": 99.0, // not in schema
	})
	if !errors.Is(err, ErrUnknownField) {
		t.Fatalf("expected ErrUnknownField, got %v", err)
	}

	// 6. Fractional value for int field on Save must fail
	err = db.SaveRecord("car_01", map[string]float32{
		"color_code": 1.0,
		"age":        3.5, // fractional value for int!
		"speed":      80.0,
	})
	if !errors.Is(err, ErrInvalidFieldType) {
		t.Fatalf("expected ErrInvalidFieldType for fractional int on save, got %v", err)
	}

	// 7. Valid record (including optional fields omitted) must succeed
	err = db.SaveRecord("car_01", map[string]float32{
		"color_code": 1.0,
		"age":        3.0,
		"speed":      80.0,
	})
	if err != nil {
		t.Fatalf("valid SaveRecord failed: %v", err)
	}

	// 8. UpdateRecords must also enforce schema field types (reject age=0.5)
	_, err = db.UpdateRecords(UpdateOptions{
		TargetEntities: []string{"car_01"},
		Mutations: map[string]ComputeOp{
			"age": OpAdd{FieldA: "0", FieldB: "0.5"},
		},
	})
	if !errors.Is(err, ErrInvalidFieldType) {
		t.Fatalf("expected ErrInvalidFieldType when updating int field to 0.5 (targeted), got %v", err)
	}
	// Scan path must also reject non-integer update on int field
	_, err = db.UpdateRecords(UpdateOptions{
		Mutations: map[string]ComputeOp{
			"age": OpAdd{FieldA: "age", FieldB: "0.5"},
		},
	})
	if !errors.Is(err, ErrInvalidFieldType) {
		t.Fatalf("expected ErrInvalidFieldType when updating int field to 3.5 (scan), got %v", err)
	}
	// Verify age remained 3.0 after rejected updates
	gotProps, err := db.GetRecord("car_01")
	if err != nil || gotProps["age"] != 3.0 {
		t.Fatalf("expected car_01 age to remain 3.0 after rejected update, got %v (err=%v)", gotProps["age"], err)
	}

	// Valid integer update must succeed
	updated, err := db.UpdateRecords(UpdateOptions{
		TargetEntities: []string{"car_01"},
		Mutations: map[string]ComputeOp{
			"age": OpAdd{FieldA: "age", FieldB: "1"},
		},
	})
	if err != nil || updated != 1 {
		t.Fatalf("expected valid integer update to succeed (updated=1), got updated=%d err=%v", updated, err)
	}

	// 9. Verify persistence across DB re-open
	if err := db.Close(); err != nil {
		t.Fatalf("db close failed: %v", err)
	}

	reopenedDB, err := Open(dbPath)
	if err != nil {
		t.Fatalf("reopening db failed: %v", err)
	}
	defer reopenedDB.Close()

	loadedSchema, ok := reopenedDB.GetSchema()
	if !ok {
		t.Fatalf("expected schema to be loaded on reopened db")
	}
	if err := loadedSchema.Compare(schema1); err != nil {
		t.Fatalf("reopened schema mismatch: %v", err)
	}

	// Verify that saving invalid record still fails on reopened DB
	err = reopenedDB.SaveRecord("car_02", map[string]float32{
		"color_code": 1.0, // missing age!
	})
	if !errors.Is(err, ErrMissingRequiredField) {
		t.Fatalf("expected ErrMissingRequiredField on reopened DB, got %v", err)
	}
}

func TestSchema_ResetAllowsNewSchema(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "reset_test.anchordb")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	schemaA := ModelSchema{"x": "float"}
	if err := db.AttachSchema(schemaA); err != nil {
		t.Fatalf("attach schemaA failed: %v", err)
	}

	schemaB := ModelSchema{"y": "int"}
	// Should fail before reset
	if err := db.AttachSchema(schemaB); !errors.Is(err, ErrSchemaMismatch) {
		t.Fatalf("expected ErrSchemaMismatch, got %v", err)
	}

	// Reset schema
	if err := db.ResetSchema(); err != nil {
		t.Fatalf("ResetSchema failed: %v", err)
	}

	// Now attaching schemaB must succeed
	if err := db.AttachSchema(schemaB); err != nil {
		t.Fatalf("attach schemaB after reset failed: %v", err)
	}

	loaded, ok := db.GetSchema()
	if !ok || loaded["y"] != "int" {
		t.Fatalf("expected schemaB attached after reset, got %+v", loaded)
	}
}

func TestSchema_ResetKeepsLockWhenSidecarRemovalFails(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "model.anchordb")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.AttachSchema(ModelSchema{"x": "float"}); err != nil {
		t.Fatal(err)
	}
	sidecar := dbPath + ".schema"
	if err := os.Remove(sidecar); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(sidecar, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sidecar, "keep"), []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := db.ResetSchema(); err == nil {
		t.Fatal("ResetSchema should report sidecar removal failure")
	}
	if schema, locked := db.GetSchema(); !locked || schema["x"] != "float" {
		t.Fatalf("schema lock changed after failed reset: locked=%v schema=%v", locked, schema)
	}
}

func TestSchema_WithComputeStatsAndGeometry(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "schema_stats.anchordb")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// 1. Attach and lock schema
	schema := ModelSchema{
		"voltage":   "float",
		"is_active": "bool",
	}
	if err := db.AttachSchema(schema); err != nil {
		t.Fatalf("AttachSchema failed: %v", err)
	}

	// 2. Reject un-declared fields (schema enforcement intact)
	err = db.SaveRecord("sensor_bad", map[string]float32{"voltage": 220.0, "unknown_field": 99.0})
	if !errors.Is(err, ErrUnknownField) {
		t.Fatalf("expected ErrUnknownField, got %v", err)
	}

	// 3. Save valid schema-conforming records with spatial coordinates
	_, err = db.SaveRecordWithNode("sensor_jkt", OpStatusActive, map[string]float32{
		"voltage":   220.0,
		"is_active": 1.0,
	}, spatial.Point2D{X: 106.8272, Y: -6.1754}, [4]float32{1, 0, 0, 0})
	if err != nil {
		t.Fatalf("SaveRecordWithNode 1 failed: %v", err)
	}

	_, err = db.SaveRecordWithNode("sensor_bdg", OpStatusActive, map[string]float32{
		"voltage":   240.0,
		"is_active": 1.0,
	}, spatial.Point2D{X: 107.6186, Y: -6.9025}, [4]float32{0, 1, 0, 0})
	if err != nil {
		t.Fatalf("SaveRecordWithNode 2 failed: %v", err)
	}

	// 4. ComputeStats on schema field "voltage"
	stats, err := db.ComputeStats("voltage")
	if err != nil {
		t.Fatalf("ComputeStats failed under locked schema: %v", err)
	}
	if stats.Count != 2 || stats.Mean != 230.0 || stats.Min != 220.0 || stats.Max != 240.0 {
		t.Fatalf("unexpected stats under locked schema: %+v", stats)
	}

	// 5. Query nodes in polygon under locked schema
	poly, err := spatial.NewPolygon2D([]spatial.Point2D{
		{X: 106.0, Y: -6.0},
		{X: 108.0, Y: -6.0},
		{X: 108.0, Y: -8.0},
		{X: 106.0, Y: -8.0},
	})
	if err != nil {
		t.Fatalf("NewPolygon2D failed: %v", err)
	}

	nodes, err := db.FindNodesInPolygon(poly, 10)
	if err != nil {
		t.Fatalf("FindNodesInPolygon failed under locked schema: %v", err)
	}
	if len(nodes) != 2 {
		t.Fatalf("expected 2 nodes in polygon, got %d", len(nodes))
	}

	// 6. Haversine distance under locked schema
	dist, err := db.CalculateHaversineDistance("sensor_jkt", "sensor_bdg")
	if err != nil {
		t.Fatalf("CalculateHaversineDistance failed under locked schema: %v", err)
	}
	if dist < 115 || dist > 125 {
		t.Fatalf("unexpected distance under locked schema: %f", dist)
	}
}
