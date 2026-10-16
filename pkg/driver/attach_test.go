package driver_test

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/rachmanzz/anchordb/pkg/driver"
	"github.com/rachmanzz/anchordb/pkg/storage"
)

func TestDriver_AttachSchema_Embedded(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "anchordb_driver_attach_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	ctx := context.Background()

	// 1. Open client without schema initially
	client, err := driver.OpenEmbedded(tmpDir, driver.WithModel("devices"))
	if err != nil {
		t.Fatalf("failed to open embedded client: %v", err)
	}
	defer client.Close()

	// Initially no schema attached
	_, err = client.GetSchema(ctx)
	if err == nil {
		t.Fatalf("expected error when getting non-existent schema, got nil")
	}

	// 2. Attach valid schema
	schema := driver.Schema{
		"voltage": "float",
		"status":  "int",
		"model":   "string?",
		"tags":    "string[]?",
	}
	if err := client.Attach(schema); err != nil {
		t.Fatalf("client.Attach failed: %v", err)
	}

	// Verify schema retrieved matches
	gotSchema, err := client.GetSchema(ctx)
	if err != nil {
		t.Fatalf("client.GetSchema failed: %v", err)
	}
	if gotSchema["voltage"] != "float" || gotSchema["status"] != "int" {
		t.Fatalf("retrieved schema mismatch: %v", gotSchema)
	}

	// 3. Attach identical schema -> should succeed (idempotent contract check)
	identicalSchema := driver.Schema{
		"voltage": "float",
		"status":  "int",
		"model":   "string?",
		"tags":    "string[]?",
	}
	if err := client.Attach(identicalSchema); err != nil {
		t.Fatalf("re-attaching identical schema failed: %v", err)
	}

	// 4. Attach conflicting schema -> must fail with ErrSchemaMismatch
	conflictingSchema := driver.Schema{
		"voltage": "int", // changed from float
		"status":  "int",
	}
	err = client.Attach(conflictingSchema)
	if err == nil {
		t.Fatalf("expected ErrSchemaMismatch, got nil")
	}
	if !errors.Is(err, storage.ErrSchemaMismatch) {
		t.Fatalf("expected ErrSchemaMismatch, got %v", err)
	}

	// 5. Save record obeying schema -> success
	err = client.Save("dev_1", map[string]float32{
		"voltage": 220.5,
		"status":  1,
	})
	if err != nil {
		t.Fatalf("valid record save failed: %v", err)
	}

	// 6. Save record missing required field -> fails
	err = client.Save("dev_invalid", map[string]float32{
		"status": 1, // missing "voltage"
	})
	if err == nil {
		t.Fatalf("expected error on missing required field, got nil")
	}
	if !errors.Is(err, storage.ErrMissingRequiredField) {
		t.Fatalf("expected ErrMissingRequiredField, got %v", err)
	}

	// 7. Save record with unknown field -> fails
	err = client.Save("dev_unknown", map[string]float32{
		"voltage": 220.5,
		"status":  1,
		"hacker":  99.9,
	})
	if err == nil {
		t.Fatalf("expected error on unknown field, got nil")
	}
	if !errors.Is(err, storage.ErrUnknownField) {
		t.Fatalf("expected ErrUnknownField, got %v", err)
	}

	// 8. AttachModel on a clean model
	userClient, err := client.AttachModel("users", driver.Schema{
		"age":   "int",
		"name":  "string?",
		"roles": "string[]?",
	})
	if err != nil {
		t.Fatalf("client.AttachModel failed: %v", err)
	}
	if userClient.ModelName() != "users" {
		t.Fatalf("expected model users, got %s", userClient.ModelName())
	}
	err = userClient.Save("user_1", map[string]float32{"age": 30})
	if err != nil {
		t.Fatalf("saving to newly attached model failed: %v", err)
	}

	// Close initial client so we can test re-opening with flock
	_ = client.Close()

	// 9. Re-open client with WithAttachSchema option:
	// 9a. Connecting with matching schema -> success
	client2, err := driver.Connect(tmpDir,
		driver.WithModel("devices"),
		driver.WithAttachSchema(schema),
	)
	if err != nil {
		t.Fatalf("reconnect with matching schema failed: %v", err)
	}

	// 9b. Administrative Reset on client2
	err = client2.ResetModel(ctx, false)
	if err == nil {
		t.Fatalf("expected error without confirm=true, got nil")
	}

	err = client2.ResetModel(ctx, true)
	if err != nil {
		t.Fatalf("client2.ResetModel(confirm=true) failed: %v", err)
	}

	// After reset, schema is unmounted
	_, err = client2.GetSchema(ctx)
	if err == nil {
		t.Fatalf("expected schema to be gone after reset, got nil")
	}

	// Now a completely different schema can be attached
	newSchema := driver.Schema{
		"new_field": "string",
	}
	if err := client2.Attach(newSchema); err != nil {
		t.Fatalf("attaching new schema after reset failed: %v", err)
	}
	_ = client2.Close()

	// 10. Connecting with mismatched schema on the reset/new schema -> rejected
	_, err = driver.Connect(tmpDir,
		driver.WithModel("devices"),
		driver.WithAttachSchema(driver.Schema{"mismatched_field": "int"}),
	)
	if err == nil {
		t.Fatalf("expected connection rejection on schema mismatch, got nil")
	}
	if !errors.Is(err, storage.ErrSchemaMismatch) {
		t.Fatalf("expected ErrSchemaMismatch on connect, got %v", err)
	}
}
