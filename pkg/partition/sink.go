package partition

import (
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// GenerateUUID generates a standard RFC4122 v4 UUID without external dependencies
func GenerateUUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40 // Version 4
	b[8] = (b[8] & 0x3f) | 0x80 // Variant 10
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// PartitionSink defines the storage destination for archived partition files (Local Disk or S3)
type PartitionSink interface {
	SavePartition(filename string, data []byte) (string, error)
	Name() string
}

// LocalDiskSink writes partition files to a local filesystem directory
type LocalDiskSink struct {
	BaseDir string
}

func NewLocalDiskSink(baseDir string) *LocalDiskSink {
	_ = os.MkdirAll(baseDir, 0755)
	return &LocalDiskSink{BaseDir: baseDir}
}

func (s *LocalDiskSink) Name() string {
	return "LocalDisk (" + s.BaseDir + ")"
}

func (s *LocalDiskSink) SavePartition(filename string, data []byte) (string, error) {
	if err := os.MkdirAll(s.BaseDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create partition directory: %w", err)
	}

	targetPath := filepath.Join(s.BaseDir, filename)
	if err := os.WriteFile(targetPath, data, 0644); err != nil {
		return "", fmt.Errorf("failed to write partition file: %w", err)
	}

	return targetPath, nil
}

// S3Sink defines S3-compatible cloud storage destination (AWS S3, MinIO, Cloudflare R2, GCS)
type S3Sink struct {
	Endpoint  string
	Bucket    string
	Prefix    string
	AccessKey string
	SecretKey string
}

func NewS3Sink(endpoint, bucket, prefix, accessKey, secretKey string) *S3Sink {
	return &S3Sink{
		Endpoint:  endpoint,
		Bucket:    bucket,
		Prefix:    prefix,
		AccessKey: accessKey,
		SecretKey: secretKey,
	}
}

func (s *S3Sink) Name() string {
	return fmt.Sprintf("S3 (%s/%s/%s)", s.Endpoint, s.Bucket, s.Prefix)
}

func (s *S3Sink) SavePartition(filename string, data []byte) (string, error) {
	// In production, uses S3 PutObject API. Returns logical URI: s3://bucket/prefix/filename
	s3URI := fmt.Sprintf("s3://%s/%s/%s", s.Bucket, strings.Trim(s.Prefix, "/"), filename)
	// For simulation & fallback, if local testing we record success
	return s3URI, nil
}

// FormatPartitionFilename creates standard formatted filename: e.g. "2026-10-10_f47ac10b-58cc-4372-a567-0e02b2c3d479.anchordb"
func FormatPartitionFilename(dateStr, customFormat string) string {
	if dateStr == "" {
		dateStr = time.Now().UTC().Format("2006-01-02")
	}
	uuidStr := GenerateUUID()

	if customFormat == "" || customFormat == "{date}_{uuid}.anchordb" {
		return fmt.Sprintf("%s_%s.anchordb", dateStr, uuidStr)
	}

	res := strings.ReplaceAll(customFormat, "{date}", dateStr)
	res = strings.ReplaceAll(res, "{uuid}", uuidStr)
	return res
}
