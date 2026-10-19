package partition

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCatalog_OpenAndRegister(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "catalog-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	catalogPath := filepath.Join(tmpDir, "partitions", "catalog.json")
	cat, err := OpenCatalog(catalogPath)
	if err != nil {
		t.Fatalf("OpenCatalog failed: %v", err)
	}

	entry1 := &CatalogEntry{
		ID:             "part-1",
		Model:          "financial_logs",
		PartitionFile:  "2026-10-19_part1.anchordb",
		TargetLocation: "s3://my-bucket/partitions/2026-10-19_part1.anchordb",
		SinkType:       "s3",
		RecordCount:    2,
		CutoffDate:     "2026-04-22",
		MinDate:        20100,
		MaxDate:        20200,
		RecordNames:    []string{"tx_archived_1", "tx_archived_2"},
		CreatedAt:      time.Now().UTC(),
		SizeBytes:      1024,
	}

	if err := cat.RegisterPartition(entry1); err != nil {
		t.Fatalf("RegisterPartition failed: %v", err)
	}

	// Verify FindRecord
	found, ok := cat.FindRecord("financial_logs", "tx_archived_1")
	if !ok || found.PartitionFile != "2026-10-19_part1.anchordb" {
		t.Fatalf("expected to find tx_archived_1 in part1, got %+v (ok=%v)", found, ok)
	}

	found2, ok2 := cat.FindRecord("financial_logs", "tx_archived_2")
	if !ok2 || found2.TargetLocation != entry1.TargetLocation {
		t.Fatalf("expected to find tx_archived_2, got %+v (ok=%v)", found2, ok2)
	}

	if _, ok := cat.FindRecord("financial_logs", "nonexistent"); ok {
		t.Fatalf("expected nonexistent record not to be found")
	}

	// Verify ListEntries
	entries := cat.ListEntries("financial_logs")
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if len(cat.ListEntries("other_model")) != 0 {
		t.Fatalf("expected 0 entries for other_model")
	}

	// Verify persistence: Reopen catalog
	cat2, err := OpenCatalog(catalogPath)
	if err != nil {
		t.Fatalf("reopening OpenCatalog failed: %v", err)
	}
	foundReopened, ok := cat2.FindRecord("financial_logs", "tx_archived_1")
	if !ok || foundReopened.PartitionFile != "2026-10-19_part1.anchordb" {
		t.Fatalf("reopened catalog failed to locate tx_archived_1: %+v", foundReopened)
	}
}
