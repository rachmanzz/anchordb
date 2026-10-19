package partition

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rachmanzz/anchordb/pkg/storage"
)

const (
	defaultMinIOEndpoint  = "http://localhost:9000"
	defaultMinIOAccessKey = "minioadmin"
	defaultMinIOSecretKey = "minioadmin"
	defaultMinIOBucket    = "anchordb-test-partitions"
)

// checkMinIOAvailable verifies if MinIO is actively running and responding on localhost:9000.
func checkMinIOAvailable(t *testing.T, endpoint string) bool {
	t.Helper()
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(endpoint + "/minio/health/live")
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

// ensureMinIOBucket creates the target bucket in MinIO via S3 PUT bucket API if it does not exist.
func ensureMinIOBucket(t *testing.T, endpoint, bucket, accessKey, secretKey string) {
	t.Helper()
	reqURL := fmt.Sprintf("%s/%s", strings.TrimRight(endpoint, "/"), bucket)
	req, err := http.NewRequest(http.MethodPut, reqURL, nil)
	if err != nil {
		t.Fatalf("ensureMinIOBucket: failed creating request: %v", err)
	}

	emptyHash := sha256.Sum256([]byte{})
	emptyHex := hex.EncodeToString(emptyHash[:])
	signS3Request(req, emptyHex, accessKey, secretKey, "us-east-1", time.Now().UTC())

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("ensureMinIOBucket: failed sending PUT bucket request: %v", err)
	}
	defer resp.Body.Close()

	// 200 OK or 409 Conflict (BucketAlreadyOwnedByYou / BucketAlreadyExists) are acceptable.
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusConflict {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("ensureMinIOBucket: unexpected status %d: %s", resp.StatusCode, string(body))
	}
}

// downloadMinIOObject downloads an object from MinIO via authenticated S3 GET.
func downloadMinIOObject(t *testing.T, endpoint, bucket, key, accessKey, secretKey, targetPath string) {
	t.Helper()
	reqURL := fmt.Sprintf("%s/%s/%s", strings.TrimRight(endpoint, "/"), bucket, strings.TrimLeft(key, "/"))
	req, err := http.NewRequest(http.MethodGet, reqURL, nil)
	if err != nil {
		t.Fatalf("downloadMinIOObject: failed creating request: %v", err)
	}

	emptyHash := sha256.Sum256([]byte{})
	emptyHex := hex.EncodeToString(emptyHash[:])
	signS3Request(req, emptyHex, accessKey, secretKey, "us-east-1", time.Now().UTC())

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("downloadMinIOObject: GET %s failed: %v", reqURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("downloadMinIOObject: unexpected status %d: %s", resp.StatusCode, string(body))
	}

	out, err := os.Create(targetPath)
	if err != nil {
		t.Fatalf("downloadMinIOObject: failed creating destination file: %v", err)
	}
	defer out.Close()

	if _, err := io.Copy(out, resp.Body); err != nil {
		t.Fatalf("downloadMinIOObject: failed writing object body: %v", err)
	}
}

// TestS3MinIO_LivePartitionAndVerify executes live partitioning against MinIO under Podman.
func TestS3MinIO_LivePartitionAndVerify(t *testing.T) {
	endpoint := defaultMinIOEndpoint
	if envEndpoint := os.Getenv("MINIO_ENDPOINT"); envEndpoint != "" {
		endpoint = envEndpoint
	}

	if !checkMinIOAvailable(t, endpoint) {
		t.Skipf("MinIO is not responding at %s. Skipping live S3 integration test.", endpoint)
	}

	bucket := defaultMinIOBucket
	accessKey := defaultMinIOAccessKey
	secretKey := defaultMinIOSecretKey
	prefix := fmt.Sprintf("partitions/test_%d", time.Now().UnixNano())

	ensureMinIOBucket(t, endpoint, bucket, accessKey, secretKey)

	// 1. Setup local AnchorDB hot database
	tmpDir := t.TempDir()
	hotDBPath := filepath.Join(tmpDir, "hot_events.anchordb")
	db, err := storage.Open(hotDBPath)
	if err != nil {
		t.Fatalf("failed to open hot db: %v", err)
	}
	defer db.Close()

	nowDays := float32(time.Now().Unix() / 86400)

	// Populate test records:
	// 2 records older than 180 days (must be archived & pruned)
	// 2 records newer than 180 days (must remain in hot DB)
	testRecords := map[string]map[string]float32{
		"event_old_240d":   {"date": nowDays - 240, "amount": 1500.50, "fraud": 0},
		"event_old_200d":   {"date": nowDays - 200, "amount": 99.00, "fraud": 1},
		"event_recent_60d": {"date": nowDays - 60, "amount": 450.25, "fraud": 0},
		"event_today":      {"date": nowDays, "amount": 12.00, "fraud": 0},
	}

	for name, props := range testRecords {
		if err := db.SaveRecord(name, props); err != nil {
			t.Fatalf("failed saving record %s: %v", name, err)
		}
	}
	if err := db.Sync(); err != nil {
		t.Fatalf("failed syncing hot db: %v", err)
	}

	// 2. Configure S3Sink targeting live MinIO
	s3Sink := NewS3Sink(endpoint, bucket, prefix, accessKey, secretKey)
	s3Sink.AllowInsecureHTTP = true // Plaintext HTTP allowed for local MinIO test

	policy := PartitionPolicy{
		ModelName:     "events",
		DateField:     "date",
		RetentionDays: 180,
		Sink:          s3Sink,
		FileFormat:    "{date}_{uuid}.anchordb",
		PruneHotDB:    true,
	}

	// 3. Trigger Partitioning
	result, err := ExecutePartition(db, policy)
	if err != nil {
		t.Fatalf("ExecutePartition failed: %v", err)
	}

	t.Logf("Partition Result: Model=%s ArchivedCount=%d TargetLocation=%s CutoffDate=%s Duration=%.2fms",
		result.ModelName, result.ArchivedCount, result.TargetLocation, result.CutoffDate, result.DurationMs)

	if result.ArchivedCount != 2 {
		t.Fatalf("expected ArchivedCount=2, got %d", result.ArchivedCount)
	}

	expectedPrefix := fmt.Sprintf("s3://%s/%s/", bucket, prefix)
	if !strings.HasPrefix(result.TargetLocation, expectedPrefix) {
		t.Fatalf("expected TargetLocation to start with %q, got %q", expectedPrefix, result.TargetLocation)
	}

	// 4. Verify hot database state:
	// Old records must be pruned (not found or tombstoned)
	for _, oldRecord := range []string{"event_old_240d", "event_old_200d"} {
		if _, err := db.GetRecord(oldRecord); err == nil {
			t.Fatalf("expected %s to be pruned from hot db, but it still exists", oldRecord)
		}
	}
	// Recent records must still exist in hot database
	for _, recentRecord := range []string{"event_recent_60d", "event_today"} {
		props, err := db.GetRecord(recentRecord)
		if err != nil {
			t.Fatalf("expected %s to remain in hot db, err: %v", recentRecord, err)
		}
		expectedAmount := testRecords[recentRecord]["amount"]
		if props["amount"] != expectedAmount {
			t.Fatalf("record %s amount mismatch: got %v, want %v", recentRecord, props["amount"], expectedAmount)
		}
	}

	// 5. Download the archived .anchordb file from MinIO and verify contents
	objectKey := strings.TrimPrefix(result.TargetLocation, fmt.Sprintf("s3://%s/", bucket))
	downloadedPath := filepath.Join(tmpDir, "downloaded_archive.anchordb")
	downloadMinIOObject(t, endpoint, bucket, objectKey, accessKey, secretKey, downloadedPath)

	fi, err := os.Stat(downloadedPath)
	if err != nil || fi.Size() == 0 {
		t.Fatalf("downloaded archive file is empty or missing: %v", err)
	}
	t.Logf("Successfully downloaded archived partition from MinIO: size=%d bytes", fi.Size())

	// Open the downloaded partition with AnchorDB engine
	partDB, err := storage.Open(downloadedPath)
	if err != nil {
		t.Fatalf("failed opening downloaded archive with storage.Open: %v", err)
	}
	defer partDB.Close()

	// Verify archived records exist with correct properties in the partition file
	for _, archivedName := range []string{"event_old_240d", "event_old_200d"} {
		props, err := partDB.GetRecord(archivedName)
		if err != nil {
			t.Fatalf("expected %s inside downloaded partition file, err: %v", archivedName, err)
		}
		expectedAmount := testRecords[archivedName]["amount"]
		if props["amount"] != expectedAmount {
			t.Fatalf("archived %s amount mismatch: got %v, want %v", archivedName, props["amount"], expectedAmount)
		}
		t.Logf("Verified archived record %q in downloaded partition: amount=%.2f, fraud=%.0f",
			archivedName, props["amount"], props["fraud"])
	}

	// Recent records must NOT be inside the partition file
	for _, recentRecord := range []string{"event_recent_60d", "event_today"} {
		if _, err := partDB.GetRecord(recentRecord); err == nil {
			t.Fatalf("recent record %s should NOT be in the partition archive", recentRecord)
		}
	}

	t.Log("Live S3/MinIO partition and retrieval verification: 100% SUCCESS")
}
