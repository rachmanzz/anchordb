package partition

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

// GenerateUUID generates a standard RFC4122 v4 UUID
func GenerateUUID() string {
	return uuid.NewString()
}

// PartitionSink defines the storage destination for archived partition files (Local Disk or S3)
type PartitionSink interface {
	SavePartition(filename string, data []byte) (string, error)
	Name() string
}

// LocalDiskSink saves partition files to a local directory (e.g. /data/partitions/...)
type LocalDiskSink struct {
	BaseDir string
}

func NewLocalDiskSink(baseDir string) *LocalDiskSink {
	return &LocalDiskSink{BaseDir: baseDir}
}

func (s *LocalDiskSink) Name() string {
	return fmt.Sprintf("LocalDisk (%s)", s.BaseDir)
}

func (s *LocalDiskSink) SavePartition(filename string, data []byte) (string, error) {
	if err := os.MkdirAll(s.BaseDir, 0755); err != nil {
		return "", fmt.Errorf("failed creating partition directory: %w", err)
	}

	targetPath := filepath.Join(s.BaseDir, filename)
	if err := os.WriteFile(targetPath, data, 0644); err != nil {
		return "", fmt.Errorf("failed writing partition file %s: %w", targetPath, err)
	}

	return targetPath, nil
}

// S3Sink defines S3-compatible cloud storage destination (AWS S3, MinIO, Cloudflare R2, GCS)
type S3Sink struct {
	Endpoint   string
	Bucket     string
	Prefix     string
	AccessKey  string
	SecretKey  string
	HTTPClient *http.Client
}

func NewS3Sink(endpoint, bucket, prefix, accessKey, secretKey string) *S3Sink {
	return &S3Sink{
		Endpoint:   endpoint,
		Bucket:     bucket,
		Prefix:     prefix,
		AccessKey:  accessKey,
		SecretKey:  secretKey,
		HTTPClient: &http.Client{Timeout: 30 * time.Second},
	}
}

func (s *S3Sink) Name() string {
	return fmt.Sprintf("S3 (%s/%s/%s)", s.Endpoint, s.Bucket, s.Prefix)
}

func (s *S3Sink) SavePartition(filename string, data []byte) (string, error) {
	if s.Bucket == "" {
		return "", fmt.Errorf("S3Sink: bucket name cannot be empty")
	}

	endpoint := s.Endpoint
	if endpoint == "" {
		endpoint = fmt.Sprintf("https://%s.s3.amazonaws.com", s.Bucket)
	}
	endpoint = strings.TrimRight(endpoint, "/")

	key := path.Join(strings.Trim(s.Prefix, "/"), filename)
	uploadURL := fmt.Sprintf("%s/%s", endpoint, key)

	req, err := http.NewRequest(http.MethodPut, uploadURL, bytes.NewReader(data))
	if err != nil {
		return "", fmt.Errorf("S3Sink: failed creating request: %w", err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Content-Length", strconv.Itoa(len(data)))

	if s.AccessKey != "" {
		req.SetBasicAuth(s.AccessKey, s.SecretKey)
	}

	client := s.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}

	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("S3Sink: upload to %s failed: %w", uploadURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return "", fmt.Errorf("S3Sink: server returned status %d: %s", resp.StatusCode, string(snippet))
	}

	return fmt.Sprintf("s3://%s/%s", s.Bucket, key), nil
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
