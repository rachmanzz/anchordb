// Package partition provides partition policies, historical data archiving, and cold storage sinks.
package partition

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/rachmanzz/anchordb/pkg/storage"
)

// ErrPartitionConflict is returned when records selected for partitioning mutate before archival completes.
var ErrPartitionConflict = errors.New("partition records changed before archival lock")

// PartitionPolicy defines rules for archiving and partitioning historical records.
//
//nolint:revive // Stutter maintained for API backward compatibility.
type PartitionPolicy struct {
	ModelName     string        // e.g. "logs" or "transactions"
	DateField     string        // e.g. "date" or "timestamp"
	RetentionDays float32       // e.g. 180 (6 months)
	Sink          PartitionSink // LocalDiskSink or S3Sink
	FileFormat    string        // e.g. "{date}_{uuid}.anchordb"
	PruneHotDB    bool          // If true, deletes archived records from active hot database
	Catalog       *Catalog      // Optional: registers completed partition into persistent catalog
}

// Policy is an alias for PartitionPolicy without package stutter.
type Policy = PartitionPolicy

// PartitionResult contains execution details of a completed partition run.
//
//nolint:revive // Stutter maintained for API backward compatibility.
type PartitionResult struct {
	ModelName      string    `json:"model_name"`
	ArchivedCount  int       `json:"archived_count"`
	TargetLocation string    `json:"target_location"`
	CutoffDate     string    `json:"cutoff_date"`
	ExecutionTime  time.Time `json:"execution_time"`
	DurationMs     float64   `json:"duration_ms"`
	PartitionFile  string    `json:"partition_file,omitempty"`
	RecordNames    []string  `json:"record_names,omitempty"`
	MinDate        float32   `json:"min_date,omitempty"`
	MaxDate        float32   `json:"max_date,omitempty"`
	HasMore        bool      `json:"has_more,omitempty"`
}

// ExecutePartition archives historical records older than RetentionDays into an isolated partition file
func ExecutePartition(db *storage.AnchorDB, policy PartitionPolicy) (*PartitionResult, error) {
	t0 := time.Now()

	if policy.DateField == "" {
		policy.DateField = "date"
	}
	if policy.RetentionDays <= 0 {
		policy.RetentionDays = 180 // Default 6 months
	}
	if policy.Sink == nil {
		policy.Sink = NewLocalDiskSink("./data/partitions")
	}

	// 1. Calculate cutoff epoch days (e.g. today - 180 days)
	nowDays := float32(time.Now().Unix() / 86400)
	cutoffDays := nowDays - policy.RetentionDays
	cutoffDateStr := storage.FormatDate(cutoffDays)

	const maxPartitionBatch = 1000000
	// Select and transition candidates while holding the same database write
	// lock. This prevents a save/update from slipping between the archive
	// snapshot and the SafeDelete state change.
	records, err := db.LockPartitionRecords(policy.DateField, cutoffDays, maxPartitionBatch)
	if err != nil {
		return nil, fmt.Errorf("failed locking partition records: %w", err)
	}
	hasMore := len(records) == maxPartitionBatch

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
	var minDate, maxDate float32
	for i, r := range records {
		archivedNames[i] = r.Name
		val := r.Props[policy.DateField]
		if i == 0 || val < minDate {
			minDate = val
		}
		if i == 0 || val > maxDate {
			maxDate = val
		}
	}

	// Helper for rollback if archival fails -> marks records as OpStatusRetry (3)
	rollback := func(causeErr error) error {
		if _, err := db.CompletePartitionRecords(archivedNames, storage.OpStatusRetry); err != nil {
			return fmt.Errorf("%w (also failed to restore records to retry: %v)", causeErr, err)
		}
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
	partDB, err := storage.OpenWithOptions(tempDBPath, storage.Options{
		WAL: storage.WALOptions{SyncMode: storage.SyncNone},
	})
	if err != nil {
		return nil, rollback(fmt.Errorf("failed creating partition database: %w", err))
	}
	if schema, hasSchema := db.GetSchema(); hasSchema {
		if sErr := partDB.AttachSchema(schema); sErr != nil {
			_ = partDB.Close()
			return nil, rollback(fmt.Errorf("failed attaching schema to partition database: %w", sErr))
		}
	}

	batchItems := make([]storage.BatchRecordItem, len(records))
	for i, r := range records {
		batchItems[i] = storage.BatchRecordItem{
			Name:      r.Name,
			Operation: storage.OpStatusActive,
			Props:     r.Props,
			Attrs:     r.Attrs,
		}
	}
	if _, saveErr := partDB.SaveRecordsBatch(batchItems); saveErr != nil {
		_ = partDB.Close()
		return nil, rollback(fmt.Errorf("failed writing batch records to partition: %w", saveErr))
	}

	if err := partDB.Sync(); err != nil {
		_ = partDB.Close()
		return nil, rollback(fmt.Errorf("failed syncing partition database: %w", err))
	}
	if err := partDB.Close(); err != nil {
		return nil, rollback(fmt.Errorf("failed closing partition database: %w", err))
	}

	fi, err := os.Stat(tempDBPath)
	if err != nil {
		return nil, rollback(fmt.Errorf("failed stating partition archive: %w", err))
	}
	tempFileSize := fi.Size()

	// 5. Hand the partition file straight to the sink. Streaming from disk means
	//    a large archive is never buffered entirely in memory (OOM risk). The
	//    temp file lives until the deferred RemoveAll above runs.
	targetLoc, err := policy.Sink.SavePartitionFromFile(partitionFilename, tempDBPath)
	if err != nil {
		return nil, rollback(fmt.Errorf("failed saving to sink %s: %w", policy.Sink.Name(), err))
	}

	registeredFilename := partitionFilename
	if _, isS3 := policy.Sink.(*S3Sink); !isS3 && targetLoc != "" {
		registeredFilename = filepath.Base(targetLoc)
	}

	// Phase B: Register durable catalog entry BEFORE mutating/pruning hot DB so
	// cold-tier lookups can never miss already-pruned records across crash windows.
	if policy.Catalog != nil {
		sinkType := "local"
		var s3Meta *CatalogS3Metadata
		if s3Sink, isS3 := policy.Sink.(*S3Sink); isS3 {
			sinkType = "s3"
			s3Meta = &CatalogS3Metadata{
				Endpoint:  s3Sink.Endpoint,
				Bucket:    s3Sink.Bucket,
				Prefix:    s3Sink.Prefix,
				Region:    s3Sink.Region,
				AccessKey: s3Sink.AccessKey,
			}
		}
		entryID := registeredFilename
		if policy.ModelName != "" {
			entryID = policy.ModelName + "/" + registeredFilename
		}
		if regErr := policy.Catalog.RegisterPartition(&CatalogEntry{
			ID:             entryID,
			Model:          policy.ModelName,
			PartitionFile:  registeredFilename,
			TargetLocation: targetLoc,
			SinkType:       sinkType,
			RecordCount:    len(records),
			CutoffDate:     cutoffDateStr,
			MinDate:        minDate,
			MaxDate:        maxDate,
			RecordNames:    archivedNames,
			CreatedAt:      t0,
			SizeBytes:      tempFileSize,
			S3Metadata:     s3Meta,
		}); regErr != nil {
			return nil, rollback(fmt.Errorf("partition saved to %s but failed to register in catalog: %w", targetLoc, regErr))
		}
	}

	if policy.PruneHotDB {
		// Transition to Tombstone (2) and sweep only the newly archived records.
		if _, err := db.CompletePartitionRecords(archivedNames, storage.OpStatusTombstone); err != nil {
			return nil, fmt.Errorf("partition saved to %s but failed to mark source records for deletion: %w", targetLoc, err)
		}
		if _, err := db.CollectNamedTombstones(archivedNames); err != nil {
			return nil, fmt.Errorf("partition saved to %s but failed to collect archived records: %w", targetLoc, err)
		}
		if err := db.Sync(); err != nil { // Compacts active database file on disk!
			return nil, fmt.Errorf("partition saved to %s but failed to sync source database: %w", targetLoc, err)
		}
	} else {
		// Kept in hot DB - restore back to Active (1)
		if _, err := db.CompletePartitionRecords(archivedNames, storage.OpStatusActive); err != nil {
			return nil, fmt.Errorf("partition saved to %s but failed to restore source records: %w", targetLoc, err)
		}
	}

	return &PartitionResult{
		ModelName:      policy.ModelName,
		ArchivedCount:  len(records),
		TargetLocation: targetLoc,
		CutoffDate:     cutoffDateStr,
		ExecutionTime:  t0,
		DurationMs:     time.Since(t0).Seconds() * 1000,
		PartitionFile:  registeredFilename,
		RecordNames:    archivedNames,
		MinDate:        minDate,
		MaxDate:        maxDate,
		HasMore:        hasMore,
	}, nil
}
