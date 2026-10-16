package storage

import (
	"errors"
	"path/filepath"
	"testing"
)

func TestSchema_ParseFieldSpec(t *testing.T) {
	tests := []struct {
		input    string
		baseType string
		isArray  bool
		optional bool
		wantErr  bool
	}{
		{"string", "string", false, false, false},
		{"int", "int", false, false, false},
		{"float", "float", false, false, false},
		{"bool", "bool", false, false, false},
		{"date", "date", false, false, false},
		{"string?", "string", false, true, false},
		{"int?", "int", false, true, false},
		{"string[]", "string", true, false, false},
		{"float[]?", "float", true, true, false},
		{"int[]?", "int", true, true, false},
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

	schema1 := ModelSchema{
		"color":            "string",
		"age":              "int",
		"speed":            "float",
		"brand":            "string?",
		"tags":             "string[]?",
		"vector_embedding": "float[]?",
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
		"color": "string",
		"age":   "string", // Type mismatch: string instead of int
	}
	err = db.AttachSchema(mismatchedSchema)
	if !errors.Is(err, ErrSchemaMismatch) {
		t.Fatalf("expected ErrSchemaMismatch, got %v", err)
	}

	// 4. Missing required field on Save must fail
	err = db.SaveRecord("car_01", map[string]float32{
		"color": 1.0,
		// "age" missing!
		"speed": 80.0,
	})
	if !errors.Is(err, ErrMissingRequiredField) {
		t.Fatalf("expected ErrMissingRequiredField, got %v", err)
	}

	// 5. Unknown field on Save must fail
	err = db.SaveRecord("car_01", map[string]float32{
		"color":       1.0,
		"age":         3.0,
		"speed":       80.0,
		"rogue_field": 99.0, // not in schema
	})
	if !errors.Is(err, ErrUnknownField) {
		t.Fatalf("expected ErrUnknownField, got %v", err)
	}

	// 6. Valid record (including optional brand omitted) must succeed
	err = db.SaveRecord("car_01", map[string]float32{
		"color": 1.0,
		"age":   3.0,
		"speed": 80.0,
	})
	if err != nil {
		t.Fatalf("valid SaveRecord failed: %v", err)
	}

	// 7. Verify persistence across DB re-open
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
		"color": 1.0, // missing age!
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
