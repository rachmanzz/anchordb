package partition

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rachmanzz/anchordb/pkg/scheduler"
	"github.com/rachmanzz/anchordb/pkg/storage"
)

func TestS3SinkUsesBucketPathAndSigV4(t *testing.T) {
	var signed bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/bucket/prefix/object.anchordb" {
			t.Errorf("unexpected object path %q", r.URL.Path)
		}
		if r.Header.Get("Authorization") == "" || r.Header.Get("x-amz-date") == "" {
			t.Errorf("expected signed S3 request, headers=%v", r.Header)
		} else {
			signed = true
		}
		payload, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read payload: %v", err)
		}
		hash := sha256.Sum256(payload)
		if got, want := r.Header.Get("x-amz-content-sha256"), hex.EncodeToString(hash[:]); got != want {
			t.Errorf("payload hash header = %q, want %q", got, want)
		}
		parsedTime, err := time.Parse("20060102T150405Z", r.Header.Get("x-amz-date"))
		if err != nil {
			t.Errorf("invalid x-amz-date: %v", err)
		} else {
			wantReq, err := http.NewRequest(http.MethodPut, "http://"+r.Host+r.URL.RequestURI(), nil)
			if err != nil {
				t.Errorf("create signature verification request: %v", err)
				return
			}
			signS3Request(wantReq, hex.EncodeToString(hash[:]), "access", "secret", "us-east-1", parsedTime)
			if got, want := r.Header.Get("Authorization"), wantReq.Header.Get("Authorization"); got != want {
				t.Errorf("SigV4 authorization mismatch: got %q want %q", got, want)
			}
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	sink := NewS3Sink(server.URL, "bucket", "prefix", "access", "secret")
	// The test server speaks plaintext http; opt in explicitly.
	sink.AllowInsecureHTTP = true
	if _, err := sink.SavePartition("object.anchordb", []byte("payload")); err != nil {
		t.Fatal(err)
	}
	if !signed {
		t.Fatal("request was not signed")
	}
}

func TestPartition_Archive6Months_AndPrune(t *testing.T) {
	tmpDir := t.TempDir()
	hotDBPath := filepath.Join(tmpDir, "logs.anchordb")
	partDir := filepath.Join(tmpDir, "partitions")

	db, err := storage.Open(hotDBPath)
	if err != nil {
		t.Fatalf("failed to open hot db: %v", err)
	}
	defer db.Close()

	// Calculate dates:
	// Today: 2026-10-15
	// 8 months ago: ~240 days ago (must be archived)
	// 7 months ago: ~210 days ago (must be archived)
	// 2 months ago: ~60 days ago (must REMAIN in hot DB)
	// Today: 0 days (must REMAIN in hot DB)
	nowDays := float32(time.Now().Unix() / 86400)

	_ = db.SaveRecord("log_8_months_ago", map[string]float32{
		"date":   nowDays - 240,
		"status": 500,
	})
	_ = db.SaveRecord("log_7_months_ago", map[string]float32{
		"date":   nowDays - 210,
		"status": 502,
	})
	_ = db.SaveRecord("log_2_months_ago", map[string]float32{
		"date":   nowDays - 60,
		"status": 200,
	})
	_ = db.SaveRecord("log_today", map[string]float32{
		"date":   nowDays,
		"status": 200,
	})

	_ = db.Sync()

	if len(db.EntityStore) != 4 {
		t.Fatalf("expected 4 records initially, got %d", len(db.EntityStore))
	}

	// Create Sink & Policy: archive data older than 6 months (180 days) to partition file format {date}_{uuid}.anchordb
	sink := NewLocalDiskSink(partDir)
	policy := PartitionPolicy{
		ModelName:     "logs",
		DateField:     "date",
		RetentionDays: 180, // last 6 months
		Sink:          sink,
		FileFormat:    "{date}_{uuid}.anchordb",
		PruneHotDB:    true, // prune from hot db
	}

	// Execute partition
	result, err := ExecutePartition(db, policy)
	if err != nil {
		t.Fatalf("ExecutePartition failed: %v", err)
	}

	if result.ArchivedCount != 2 {
		t.Fatalf("expected 2 historical records archived, got %d", result.ArchivedCount)
	}

	t.Logf("[OK] Partition successful: %d records archived to: %s", result.ArchivedCount, result.TargetLocation)

	// Verify partition filename format: YYYY-MM-DD_<uuid>.anchordb
	filename := filepath.Base(result.TargetLocation)
	if !strings.HasSuffix(filename, ".anchordb") || len(filename) < 20 {
		t.Fatalf("invalid partition filename format: %s", filename)
	}
	t.Logf("[OK] Partition file format verified: %s", filename)

	// Verify Hot DB: 2 old records pruned, 2 recent records preserved!
	if len(db.EntityStore) != 2 {
		t.Fatalf("expected 2 records remaining in hot DB, got %d", len(db.EntityStore))
	}
	if _, exists := db.EntityStore["log_today"]; !exists {
		t.Fatalf("expected log_today still in hot DB")
	}
	if _, exists := db.EntityStore["log_2_months_ago"]; !exists {
		t.Fatalf("expected log_2_months_ago still in hot DB")
	}
	if _, exists := db.EntityStore["log_8_months_ago"]; exists {
		t.Fatalf("expected log_8_months_ago pruned from hot DB")
	}

	// Verify partition archive: open standalone and verify archived records are intact
	archivedDB, err := storage.Open(result.TargetLocation)
	if err != nil {
		t.Fatalf("failed to open archived partition file: %v", err)
	}
	defer archivedDB.Close()

	if len(archivedDB.EntityStore) != 2 {
		t.Fatalf("expected 2 records inside archived partition, got %d", len(archivedDB.EntityStore))
	}
	t.Logf("[OK] Standalone partition archive verified successfully!")
}

func TestScheduler_CalculateNextRun_2300(t *testing.T) {
	// Test schedule "23:00"
	now := time.Date(2026, 10, 15, 10, 0, 0, 0, time.UTC) // 10:00 AM UTC
	next, err := scheduler.CalculateNextRun("23:00", now)
	if err != nil {
		t.Fatalf("failed: %v", err)
	}

	if next.Hour() != 23 || next.Minute() != 0 || next.Day() != 15 {
		t.Fatalf("expected today 23:00, got %v", next)
	}

	// If past 23:00 today (e.g. 23:30), must schedule for tomorrow at 23:00
	nowPast := time.Date(2026, 10, 15, 23, 30, 0, 0, time.UTC)
	nextTomorrow, err := scheduler.CalculateNextRun("23:00", nowPast)
	if err != nil {
		t.Fatalf("failed: %v", err)
	}

	if nextTomorrow.Day() != 16 || nextTomorrow.Hour() != 23 {
		t.Fatalf("expected tomorrow 23:00, got %v", nextTomorrow)
	}
	t.Logf("[OK] Internal schedule '23:00' verified accurate!")
}

func TestPartition_S3Sink_FailSafe(t *testing.T) {
	tmpDir := t.TempDir()
	hotDBPath := filepath.Join(tmpDir, "hot_logs.anchordb")

	db, err := storage.Open(hotDBPath)
	if err != nil {
		t.Fatalf("failed to open hot db: %v", err)
	}
	defer db.Close()

	nowDays := float32(time.Now().Unix() / 86400)
	_ = db.SaveRecord("old_record_1", map[string]float32{"date": nowDays - 300})
	_ = db.SaveRecord("old_record_2", map[string]float32{"date": nowDays - 250})
	_ = db.Sync()

	// S3 Sink pointing to unreachable address so upload fails
	s3Sink := NewS3Sink("http://127.0.0.1:54321", "non-existent-bucket", "prefix", "key", "secret")

	// Execute partition with PruneHotDB: true
	_, err = ExecutePartition(db, PartitionPolicy{
		ModelName:     "hot_logs",
		DateField:     "date",
		RetentionDays: 180,
		Sink:          s3Sink,
		PruneHotDB:    true,
	})

	if err == nil {
		t.Fatalf("expected partition to fail on unreachable S3, got nil")
	}

	// CRITICAL VERIFICATION: Hot DB must NOT be pruned when sink fails!
	if len(db.EntityStore) != 2 {
		t.Fatalf("CRITICAL FAIL-SAFE VIOLATION: Hot DB was pruned despite S3 upload failure! Remaining records: %d", len(db.EntityStore))
	}

	// Verify failed records are now marked as OpStatusRetry (3)
	_, op1, err := db.GetRecordWithOperation("old_record_1")
	if err != nil || op1 != storage.OpStatusRetry {
		t.Fatalf("expected old_record_1 to have OpStatusRetry (3), got %d (err: %v)", op1, err)
	}
	t.Logf("[OK] Fail-safe verified! Hot DB preserved and marked with OpStatusRetry (3) on S3 upload failure.")

	// Next run with working local sink must pick up the OpStatusRetry records and archive them successfully!
	localSink := NewLocalDiskSink(filepath.Join(tmpDir, "local_retry_archive"))
	retryRes, err := ExecutePartition(db, PartitionPolicy{
		ModelName:     "hot_logs",
		DateField:     "date",
		RetentionDays: 180,
		Sink:          localSink,
		FileFormat:    "{date}_{uuid}.anchordb",
		PruneHotDB:    true,
	})
	if err != nil {
		t.Fatalf("subsequent partition retry failed: %v", err)
	}
	if retryRes.ArchivedCount != 2 {
		t.Fatalf("expected 2 records archived on retry, got %d", retryRes.ArchivedCount)
	}
	if len(db.EntityStore) != 0 {
		t.Fatalf("expected all records pruned after successful retry, remaining: %d", len(db.EntityStore))
	}
	t.Logf("[OK] Subsequent partition retry successfully picked up OpStatusRetry records and archived them!")
}

func TestPartition_NoMatchingRecords(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "fresh.anchordb")
	db, err := storage.Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// Add record from today (younger than 180 days cutoff)
	nowDays := float32(time.Now().Unix() / 86400)
	_ = db.SaveRecord("today_record", map[string]float32{"date": nowDays})
	_ = db.Sync()

	res, err := ExecutePartition(db, PartitionPolicy{
		ModelName:     "metrics",
		DateField:     "date",
		RetentionDays: 180,
		Sink:          NewLocalDiskSink(filepath.Join(tmpDir, "partitions")),
	})
	if err != nil {
		t.Fatalf("ExecutePartition: %v", err)
	}
	if res.ArchivedCount != 0 {
		t.Fatalf("expected 0 archived records, got %d", res.ArchivedCount)
	}
	if len(db.EntityStore) != 1 {
		t.Fatalf("expected record to remain in hot db, got %d", len(db.EntityStore))
	}
}

func TestFormatPartitionFilename_Variations(t *testing.T) {
	dateStr := "2026-10-16"

	// Default template "{date}_{uuid}.anchordb"
	name1 := FormatPartitionFilename(dateStr, "")
	if !strings.HasPrefix(name1, "2026-10-16_") || !strings.HasSuffix(name1, ".anchordb") {
		t.Errorf("unexpected default filename: %q", name1)
	}

	// Custom template without uuid
	name2 := FormatPartitionFilename(dateStr, "partition_{date}.db")
	if name2 != "partition_2026-10-16.db" {
		t.Errorf("unexpected custom filename: %q", name2)
	}
}
