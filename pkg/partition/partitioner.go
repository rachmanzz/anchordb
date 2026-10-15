package partition

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/rachmanzz/anchordb/pkg/storage"
)

// PartitionPolicy defines rules for archiving and partitioning historical records
type PartitionPolicy struct {
	ModelName     string        // e.g. "logs" or "transaksi"
	DateField     string        // e.g. "tanggal" or "timestamp"
	RetentionDays float32       // e.g. 180 (6 bulan)
	Sink          PartitionSink // LocalDiskSink or S3Sink
	FileFormat    string        // e.g. "{date}_{uuid}.anchordb"
	PruneHotDB    bool          // If true, deletes archived records from active hot database
}

// PartitionResult contains execution details of a completed partition run
type PartitionResult struct {
	ModelName      string    `json:"model_name"`
	ArchivedCount  int       `json:"archived_count"`
	TargetLocation string    `json:"target_location"`
	CutoffDate     string    `json:"cutoff_date"`
	ExecutionTime  time.Time `json:"execution_time"`
	DurationMs     float64   `json:"duration_ms"`
}

// ExecutePartition archives historical records older than RetentionDays into an isolated partition file
func ExecutePartition(db *storage.AnchorDB, policy PartitionPolicy) (*PartitionResult, error) {
	t0 := time.Now()

	if policy.DateField == "" {
		policy.DateField = "tanggal"
	}
	if policy.RetentionDays <= 0 {
		policy.RetentionDays = 180 // Default 6 bulan
	}
	if policy.Sink == nil {
		policy.Sink = NewLocalDiskSink("./data/partitions")
	}

	// 1. Calculate cutoff epoch days (e.g. today - 180 days)
	nowDays := float32(time.Now().Unix() / 86400)
	cutoffDays := nowDays - policy.RetentionDays
	cutoffDateStr := storage.FormatDate(cutoffDays)

	// 2. Query active records strictly before cutoff date
	activeOp := storage.OpStatusActive
	records, _, err := db.ListRecordsWithPaging(storage.QueryOptions{
		OperationFilter: &activeOp,
		Filters: []storage.FilterCondition{
			{
				Field:    policy.DateField,
				Operator: "<",
				Value:    cutoffDays,
			},
		},
		Limit: 1000000, // Maximum batch
	})
	if err != nil {
		return nil, fmt.Errorf("failed querying partition records: %w", err)
	}

	if len(records) == 0 {
		return &PartitionResult{
			ModelName:      policy.ModelName,
			ArchivedCount:  0,
			TargetLocation: "none (no historical records older than cutoff)",
			CutoffDate:     cutoffDateStr,
			ExecutionTime:  t0,
			DurationMs:     time.Since(t0).Seconds() * 1000,
		}, nil
	}

	archivedNames := make([]string, len(records))
	for i, r := range records {
		archivedNames[i] = r.Name
	}

	// Phase A: Transition records to SafeDelete (0) to lock from mutations & hide from regular queries
	db.SetOperationStatus(archivedNames, storage.OpStatusSafeDelete)

	// Helper for rollback if archival fails
	rollback := func(causeErr error) error {
		db.SetOperationStatus(archivedNames, storage.OpStatusActive)
		return causeErr
	}

	// 3. Create formatted partition file name: e.g. 2026-10-15_uuid.anchordb
	currentDateStr := time.Now().UTC().Format("2006-01-02")
	partitionFilename := FormatPartitionFilename(currentDateStr, policy.FileFormat)

	// 4. Create isolated temporary .anchordb file for the partition
	tempDir, err := os.MkdirTemp("", "anchordb_part_*")
	if err != nil {
		return nil, rollback(err)
	}
	defer os.RemoveAll(tempDir)

	tempDBPath := filepath.Join(tempDir, partitionFilename)
	partDB, err := storage.Open(tempDBPath)
	if err != nil {
		return nil, rollback(fmt.Errorf("failed creating partition database: %w", err))
	}

	for _, r := range records {
		_ = partDB.SaveRecord(r.Name, r.Props)
	}

	if err := partDB.Sync(); err != nil {
		_ = partDB.Close()
		return nil, rollback(fmt.Errorf("failed syncing partition database: %w", err))
	}
	_ = partDB.Close()

	// 5. Read binary partition file data
	data, err := os.ReadFile(tempDBPath)
	if err != nil {
		return nil, rollback(fmt.Errorf("failed reading partition file data: %w", err))
	}

	// 6. Write to sink (Local directory or S3)
	targetLoc, err := policy.Sink.SavePartition(partitionFilename, data)
	if err != nil {
		return nil, rollback(fmt.Errorf("failed saving to sink %s: %w", policy.Sink.Name(), err))
	}

	// Phase B: Archival succeeded!
	if policy.PruneHotDB {
		// Transition to Tombstone (2) and sweep
		db.SetOperationStatus(archivedNames, storage.OpStatusTombstone)
		_ = db.CollectTombstones(len(archivedNames))
		_ = db.Sync() // Compacts active database file on disk!
	} else {
		// Kept in hot DB - restore back to Active (1)
		db.SetOperationStatus(archivedNames, storage.OpStatusActive)
	}

	return &PartitionResult{
		ModelName:      policy.ModelName,
		ArchivedCount:  len(records),
		TargetLocation: targetLoc,
		CutoffDate:     cutoffDateStr,
		ExecutionTime:  t0,
		DurationMs:     time.Since(t0).Seconds() * 1000,
	}, nil
}
