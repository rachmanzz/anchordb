package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
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
	BaseType string `json:"base_type"` // "string", "int", "float", "bool", "date"
	IsArray  bool   `json:"is_array"`  // true if array (e.g. string[], float[])
	Optional bool   `json:"optional"`  // true if ends with ?
	Raw      string `json:"raw"`       // normalized raw string representation
}

// ParseFieldSpec parses a raw type string (e.g. "string", "int?", "string[]", "float[]?").
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

	isArray := false
	if strings.HasSuffix(s, "[]") {
		isArray = true
		s = strings.TrimSuffix(s, "[]")
	}

	// Handle case if modifier was written as type?[]
	if strings.HasSuffix(s, "?") {
		optional = true
		s = strings.TrimSuffix(s, "?")
	}

	base := strings.ToLower(strings.TrimSpace(s))
	switch base {
	case "string", "str", "text":
		base = "string"
	case "int", "integer", "int32", "int64":
		base = "int"
	case "float", "float32", "float64", "number":
		base = "float"
	case "bool", "boolean":
		base = "bool"
	case "date", "datetime":
		base = "date"
	default:
		return FieldSpec{}, fmt.Errorf("%w: unsupported base type %q", ErrInvalidFieldType, base)
	}

	norm := base
	if isArray {
		norm += "[]"
	}
	if optional {
		norm += "?"
	}

	return FieldSpec{
		BaseType: base,
		IsArray:  isArray,
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
		spec := specs[k]
		if spec.IsArray {
			// Array fields in flat float32 props are not represented as scalar floats
			continue
		}
		switch spec.BaseType {
		case "int":
			if val != float32(int64(val)) {
				return fmt.Errorf("%w: field %q expects integer value, got %v", ErrInvalidFieldType, k, val)
			}
		case "bool":
			if val != 0.0 && val != 1.0 {
				return fmt.Errorf("%w: field %q expects boolean (0.0 or 1.0), got %v", ErrInvalidFieldType, k, val)
			}
		}
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

	// Persist schema to disk sidecar file
	if db.filePath != "" {
		schemaPath := db.filePath + ".schema"
		data, err := json.MarshalIndent(norm, "", "  ")
		if err != nil {
			return fmt.Errorf("failed to marshal schema: %w", err)
		}
		if err := os.WriteFile(schemaPath, data, 0644); err != nil {
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
		_ = os.Remove(schemaPath)
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
