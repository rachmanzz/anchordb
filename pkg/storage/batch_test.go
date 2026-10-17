package storage

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/rachmanzz/anchordb/pkg/spatial"
)

func TestAnchorDB_SaveRecordsBatch(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "batch_test.anchordb")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open anchordb: %v", err)
	}

	const n = 50
	items := make([]BatchRecordItem, n)
	for i := 0; i < n; i++ {
		items[i] = BatchRecordItem{
			Name:      fmt.Sprintf("entity_%03d", i),
			Operation: OpStatusActive,
			Props: map[string]float32{
				"score": float32(i * 10),
				"val":   float32(i) + 0.5,
			},
		}
	}

	names, err := db.SaveRecordsBatch(items)
	if err != nil {
		t.Fatalf("SaveRecordsBatch failed: %v", err)
	}
	if len(names) != n {
		t.Fatalf("expected %d names, got %d", n, len(names))
	}

	for i := 0; i < n; i++ {
		rec, err := db.GetRecord(fmt.Sprintf("entity_%03d", i))
		if err != nil {
			t.Fatalf("GetRecord failed for entity_%03d: %v", i, err)
		}
		if rec["score"] != float32(i*10) {
			t.Fatalf("expected score %f, got %f", float32(i*10), rec["score"])
		}
	}
	_ = db.Close()

	// Reopen to ensure WAL recovery works seamlessly
	db2, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to reopen anchordb: %v", err)
	}
	defer db2.Close()

	for i := 0; i < n; i++ {
		rec, err := db2.GetRecord(fmt.Sprintf("entity_%03d", i))
		if err != nil {
			t.Fatalf("GetRecord after reopen failed for entity_%03d: %v", i, err)
		}
		if rec["score"] != float32(i*10) {
			t.Fatalf("expected score %f after reopen, got %f", float32(i*10), rec["score"])
		}
	}
}

func TestAnchorDB_SaveRecordsBatch_SchemaValidation(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "batch_schema.anchordb")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open anchordb: %v", err)
	}
	defer db.Close()

	schema := ModelSchema{
		"age":   "int",
		"score": "float",
	}
	if err := db.AttachSchema(schema); err != nil {
		t.Fatalf("AttachSchema failed: %v", err)
	}

	// Valid batch
	validItems := []BatchRecordItem{
		{
			Name: "user_1",
			Props: map[string]float32{
				"age":   25,
				"score": 99.5,
			},
		},
		{
			Name: "user_2",
			Props: map[string]float32{
				"age":   30,
				"score": 88.0,
			},
		},
	}
	if _, err := db.SaveRecordsBatch(validItems); err != nil {
		t.Fatalf("expected valid batch to succeed, got %v", err)
	}

	// Invalid batch (item 1 missing required field 'score')
	invalidItems := []BatchRecordItem{
		{
			Name: "user_3",
			Props: map[string]float32{
				"age":   40,
				"score": 75.0,
			},
		},
		{
			Name: "user_4",
			Props: map[string]float32{
				"age": 45, // missing score
			},
		},
	}
	if _, err := db.SaveRecordsBatch(invalidItems); err == nil {
		t.Fatalf("expected invalid batch to fail schema validation")
	}

	// Verify user_3 was rolled back / never committed
	if _, err := db.GetRecord("user_3"); err == nil {
		t.Fatalf("expected user_3 to not exist after batch error")
	}
}

func TestAnchorDB_SaveRecordsBatch_WithGeometry(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "batch_geo.anchordb")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open anchordb: %v", err)
	}

	items := []BatchRecordItem{
		{
			Name:  "atm_1",
			Coord: &spatial.Point2D{X: -6.175, Y: 106.827},
			Roles: &[4]float32{0.8, 0.2, 0.1, 0.1},
			Props: map[string]float32{"cash_level": 500000},
		},
		{
			Name:  "atm_2",
			Coord: &spatial.Point2D{X: -7.257, Y: 112.752},
			Roles: &[4]float32{0.5, 0.5, 0.1, 0.1},
			Props: map[string]float32{"cash_level": 800000},
		},
	}

	if _, err := db.SaveRecordsBatch(items); err != nil {
		t.Fatalf("SaveRecordsBatch with geometry failed: %v", err)
	}

	dist, err := db.CalculateDistance("atm_1", "atm_2")
	if err != nil {
		t.Fatalf("Distance failed: %v", err)
	}
	if dist <= 0 {
		t.Fatalf("expected positive distance between atms, got %f", dist)
	}
	_ = db.Close()

	// Reopen and check geometry
	db2, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to reopen: %v", err)
	}
	defer db2.Close()

	dist2, err := db2.CalculateDistance("atm_1", "atm_2")
	if err != nil {
		t.Fatalf("Distance after reopen failed: %v", err)
	}
	if dist2 != dist {
		t.Fatalf("expected distance %f after reopen, got %f", dist, dist2)
	}
}
