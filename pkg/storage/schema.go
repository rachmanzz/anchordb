package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
)

var (
	// ErrSchemaMismatch is returned when an incoming schema does not match the registered contract.
	ErrSchemaMismatch = errors.New("schema mismatch: incoming schema does not match registered model contract")
	// ErrSchemaLocked indicates that the model schema is locked and cannot be modified.
	ErrSchemaLocked = errors.New("model schema is locked and cannot be modified")
	// ErrMissingRequiredField is returned when a required schema field is omitted during writes.
	ErrMissingRequiredField = errors.New("missing required field")
	// ErrUnknownField is returned when a write payload contains a field not in the schema.
	ErrUnknownField = errors.New("unknown field not defined in schema")
	// ErrInvalidFieldType is returned when a field's value or type spec does not match expectations.
	ErrInvalidFieldType = errors.New("invalid field type")
)

// FieldSpec represents parsed metadata for a single schema field.
type FieldSpec struct {
	BaseType string `json:"base_type"` // "int", "float", "bool", "date"
	IsArray  bool   `json:"is_array"`  // always false; scalar float32 storage does not support arrays
	Optional bool   `json:"optional"`  // true if ends with ?
	Raw      string `json:"raw"`       // normalized raw string representation
}

// ParseFieldSpec parses a raw type string (e.g. "int", "float?", "bool", "date?").
// Only scalar numeric/temporal types supported by AnchorDB's float32 attribute storage
// ("int", "float", "bool", "date") and the optional "?" suffix are accepted.
func ParseFieldSpec(raw string) (FieldSpec, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return FieldSpec{}, fmt.Errorf("%w: empty type specification", ErrInvalidFieldType)
	}

	optional := false
	if strings.HasSuffix(s, "?") {
		optional = true
		s = strings.TrimSuffix(s, "?")
	}

	if strings.HasSuffix(s, "[]") || strings.Contains(s, "[]") {
		return FieldSpec{}, fmt.Errorf("%w: array type %q is not supported by scalar float32 attribute storage", ErrInvalidFieldType, raw)
	}

	base := strings.ToLower(strings.TrimSpace(s))
	switch base {
	case "int", "integer", "int32", "int64":
		base = "int"
	case "float", "float32", "float64", "number":
		base = "float"
	case "bool", "boolean":
		base = "bool"
	case "date", "datetime":
		base = "date"
	default:
		return FieldSpec{}, fmt.Errorf("%w: unsupported base type %q (supported types: int, float, bool, date)", ErrInvalidFieldType, base)
	}

	norm := base
	if optional {
		norm += "?"
	}

	return FieldSpec{
		BaseType: base,
		IsArray:  false,
		Optional: optional,
		Raw:      norm,
	}, nil
}

// ModelSchema is a mapping of field names to their type specifications.
type ModelSchema map[string]string

// Parse validates and returns parsed field specifications for all fields in the schema.
func (s ModelSchema) Parse() (map[string]FieldSpec, error) {
	specs := make(map[string]FieldSpec, len(s))
	for k, v := range s {
		name := strings.TrimSpace(k)
		if name == "" {
			return nil, fmt.Errorf("%w: field name cannot be empty", ErrInvalidFieldType)
		}
		spec, err := ParseFieldSpec(v)
		if err != nil {
			return nil, fmt.Errorf("field %q: %w", name, err)
		}
		specs[name] = spec
	}
	return specs, nil
}

// Compare checks if other is identical to s in field names, types, array dimensions, and optionality.
func (s ModelSchema) Compare(other ModelSchema) error {
	sSpecs, err := s.Parse()
	if err != nil {
		return fmt.Errorf("canonical schema invalid: %w", err)
	}
	otherSpecs, err := other.Parse()
	if err != nil {
		return fmt.Errorf("incoming schema invalid: %w", err)
	}

	if len(sSpecs) != len(otherSpecs) {
		return fmt.Errorf("%w: field count mismatch (expected %d fields, got %d)", ErrSchemaMismatch, len(sSpecs), len(otherSpecs))
	}

	for k, expected := range sSpecs {
		actual, exists := otherSpecs[k]
		if !exists {
			return fmt.Errorf("%w: missing required field definition %q", ErrSchemaMismatch, k)
		}
		if expected.BaseType != actual.BaseType ||
			expected.IsArray != actual.IsArray ||
			expected.Optional != actual.Optional {
			return fmt.Errorf("%w: field %q mismatch (expected %s, got %s)", ErrSchemaMismatch, k, expected.Raw, actual.Raw)
		}
	}
	return nil
}

// validateFieldValue enforces type-level value constraints for a single scalar float32 attribute.
func validateFieldValue(field string, val float32, spec FieldSpec) error {
	if spec.IsArray {
		return fmt.Errorf("%w: field %q array type %q is not supported by scalar float32 storage", ErrInvalidFieldType, field, spec.Raw)
	}
	if math.IsNaN(float64(val)) || math.IsInf(float64(val), 0) {
		return fmt.Errorf("%w: field %q requires finite value, got %v", ErrInvalidFieldType, field, val)
	}
	switch spec.BaseType {
	case "int":
		if val != float32(int64(val)) {
			return fmt.Errorf("%w: field %q expects integer value, got %v", ErrInvalidFieldType, field, val)
		}
	case "bool":
		if val != 0.0 && val != 1.0 {
			return fmt.Errorf("%w: field %q expects boolean (0.0 or 1.0), got %v", ErrInvalidFieldType, field, val)
		}
	case "float":
		// Any finite float32 is valid.
	case "date":
		if val != float32(int64(val)) {
			return fmt.Errorf("%w: field %q expects integer epoch day/timestamp for date, got %v", ErrInvalidFieldType, field, val)
		}
	default:
		return fmt.Errorf("%w: field %q has unsupported base type %q", ErrInvalidFieldType, field, spec.BaseType)
	}
	return nil
}

// ValidateProps validates that numeric record attributes conform to the schema contract.
func (s ModelSchema) ValidateProps(props map[string]float32) error {
	specs, err := s.Parse()
	if err != nil {
		return err
	}

	// 1. Check for unexpected fields not declared in schema
	for k := range props {
		if _, ok := specs[k]; !ok {
			return fmt.Errorf("%w: field %q is not defined in model schema", ErrUnknownField, k)
		}
	}

	// 2. Check for missing required fields
	for k, spec := range specs {
		if !spec.Optional {
			if _, exists := props[k]; !exists {
				return fmt.Errorf("%w: field %q is required", ErrMissingRequiredField, k)
			}
		}
	}

	// 3. Type-level value constraints
	for k, val := range props {
		if err := validateFieldValue(k, val, specs[k]); err != nil {
			return err
		}
	}

	return nil
}

// persistSchemaFileAtomically writes the schema sidecar to a temporary file in
// the same directory, fsyncs the file, atomically renames it into place, and
// fsyncs the parent directory so that a power loss cannot leave a missing or
// truncated sidecar after AttachSchema reports success.
func persistSchemaFileAtomically(schemaPath string, data []byte) error {
	dir := filepath.Dir(schemaPath)
	tmp, err := os.CreateTemp(dir, ".schema_*.tmp")
	if err != nil {
		return fmt.Errorf("create temp schema file: %w", err)
	}
	tmpPath := tmp.Name()
	cleanup := func() {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
	}

	if _, err := tmp.Write(data); err != nil {
		cleanup()
		return fmt.Errorf("write temp schema file: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		cleanup()
		return fmt.Errorf("sync temp schema file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("close temp schema file: %w", err)
	}
	if err := os.Chmod(tmpPath, 0600); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("chmod temp schema file: %w", err)
	}
	if err := os.Rename(tmpPath, schemaPath); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("rename schema file: %w", err)
	}
	if err := syncDir(dir); err != nil {
		return fmt.Errorf("sync schema directory: %w", err)
	}
	return nil
}

// AttachSchema registers and locks a schema for the database model.
// If the model already has a locked schema, it compares the incoming schema.
// If identical, it succeeds. If incompatible, it returns ErrSchemaMismatch.
func (db *AnchorDB) AttachSchema(s ModelSchema) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return ErrDatabaseClosed
	}

	// If already locked, compare strictly
	if db.schemaLocked && db.schema != nil {
		return db.schema.Compare(s)
	}

	// Validate incoming schema
	parsed, err := s.Parse()
	if err != nil {
		return err
	}

	// Normalize schema map
	norm := make(ModelSchema, len(parsed))
	for k, spec := range parsed {
		norm[k] = spec.Raw
	}

	// Persist schema to disk sidecar file atomically and durably
	if db.filePath != "" {
		schemaPath := db.filePath + ".schema"
		data, err := json.MarshalIndent(norm, "", "  ")
		if err != nil {
			return fmt.Errorf("failed to marshal schema: %w", err)
		}
		if err := persistSchemaFileAtomically(schemaPath, data); err != nil {
			return fmt.Errorf("failed to persist schema file: %w", err)
		}
	}

	db.schema = norm
	db.schemaLocked = true
	return nil
}

// GetSchema returns a copy of the registered model schema if one exists.
func (db *AnchorDB) GetSchema() (ModelSchema, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if !db.schemaLocked || db.schema == nil {
		return nil, false
	}
	cp := make(ModelSchema, len(db.schema))
	for k, v := range db.schema {
		cp[k] = v
	}
	return cp, true
}

// ResetSchema clears the registered schema from memory and disk (administrative use only).
func (db *AnchorDB) ResetSchema() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.filePath != "" {
		schemaPath := db.filePath + ".schema"
		if err := os.Remove(schemaPath); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("failed to remove schema file: %w", err)
		}
		if err := syncDir(filepath.Dir(schemaPath)); err != nil {
			return fmt.Errorf("failed to sync schema directory: %w", err)
		}
	}

	db.schema = nil
	db.schemaLocked = false
	return nil
}

// loadSchemaLocked reads the schema from disk sidecar if present.
func (db *AnchorDB) loadSchemaLocked() error {
	if db.filePath == "" {
		return nil
	}
	schemaPath := db.filePath + ".schema"
	data, err := os.ReadFile(schemaPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	var s ModelSchema
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("failed to parse schema file %s: %w", schemaPath, err)
	}

	parsed, err := s.Parse()
	if err != nil {
		return fmt.Errorf("invalid stored schema in %s: %w", schemaPath, err)
	}

	norm := make(ModelSchema, len(parsed))
	for k, spec := range parsed {
		norm[k] = spec.Raw
	}

	db.schema = norm
	db.schemaLocked = true
	return nil
}
