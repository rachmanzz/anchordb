package partition

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
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
	if filename == "" || filepath.Base(filename) != filename || filename == "." || filename == ".." {
		return "", fmt.Errorf("invalid partition filename %q", filename)
	}
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
	Region     string
	AccessKey  string
	SecretKey  string
	HTTPClient *http.Client
}

func NewS3Sink(endpoint, bucket, prefix, accessKey, secretKey string) *S3Sink {
	return &S3Sink{
		Endpoint:   endpoint,
		Bucket:     bucket,
		Prefix:     prefix,
		Region:     "us-east-1",
		AccessKey:  accessKey,
		SecretKey:  secretKey,
		HTTPClient: &http.Client{Timeout: 30 * time.Second},
	}
}

func (s *S3Sink) Name() string {
	return fmt.Sprintf("S3 (%s/%s/%s)", s.Endpoint, s.Bucket, s.Prefix)
}

func (s *S3Sink) SavePartition(filename string, data []byte) (string, error) {
	if filename == "" || filepath.Base(filename) != filename || filename == "." || filename == ".." {
		return "", fmt.Errorf("S3Sink: invalid partition filename %q", filename)
	}
	if s.Bucket == "" {
		return "", fmt.Errorf("S3Sink: bucket name cannot be empty")
	}

	endpoint := s.Endpoint
	if endpoint == "" {
		endpoint = "https://s3.amazonaws.com"
	}
	baseURL, err := url.Parse(endpoint)
	if err != nil || (baseURL.Scheme != "http" && baseURL.Scheme != "https") || baseURL.Host == "" {
		return "", fmt.Errorf("S3Sink: invalid endpoint %q", endpoint)
	}
	if baseURL.RawQuery != "" || baseURL.Fragment != "" {
		return "", fmt.Errorf("S3Sink: endpoint must not contain query or fragment")
	}
	key := path.Join(strings.Trim(s.Prefix, "/"), filename)
	objectPath := path.Join(strings.TrimSuffix(baseURL.Path, "/"), s.Bucket, key)
	baseURL.Path = objectPath
	baseURL.RawPath = ""

	req, err := http.NewRequest(http.MethodPut, baseURL.String(), bytes.NewReader(data))
	if err != nil {
		return "", fmt.Errorf("S3Sink: failed creating request: %w", err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.ContentLength = int64(len(data))

	if s.AccessKey != "" {
		if s.SecretKey == "" {
			return "", fmt.Errorf("S3Sink: secret key cannot be empty when access key is set")
		}
		region := s.Region
		if region == "" {
			region = "us-east-1"
		}
		signS3Request(req, data, s.AccessKey, s.SecretKey, region, time.Now().UTC())
	}

	client := s.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}

	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("S3Sink: upload to %s failed: %w", req.URL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return "", fmt.Errorf("S3Sink: server returned status %d: %s", resp.StatusCode, string(snippet))
	}

	return fmt.Sprintf("s3://%s/%s", s.Bucket, key), nil
}

func signS3Request(req *http.Request, payload []byte, accessKey, secretKey, region string, now time.Time) {
	const algorithm = "AWS4-HMAC-SHA256"
	date := now.Format("20060102")
	amzDate := now.Format("20060102T150405Z")
	payloadHash := sha256.Sum256(payload)
	payloadHex := hex.EncodeToString(payloadHash[:])
	req.Header.Set("x-amz-content-sha256", payloadHex)
	req.Header.Set("x-amz-date", amzDate)
	req.Host = req.URL.Host
	canonicalHeaders := "host:" + req.URL.Host + "\n" +
		"x-amz-content-sha256:" + payloadHex + "\n" +
		"x-amz-date:" + amzDate + "\n"
	signedHeaders := "host;x-amz-content-sha256;x-amz-date"
	canonicalRequest := strings.Join([]string{
		req.Method,
		req.URL.EscapedPath(),
		req.URL.Query().Encode(),
		canonicalHeaders,
		signedHeaders,
		payloadHex,
	}, "\n")
	canonicalHash := sha256.Sum256([]byte(canonicalRequest))
	scope := date + "/" + region + "/s3/aws4_request"
	stringToSign := algorithm + "\n" + amzDate + "\n" + scope + "\n" + hex.EncodeToString(canonicalHash[:])
	dateKey := hmacSHA256([]byte("AWS4"+secretKey), date)
	regionKey := hmacSHA256(dateKey, region)
	serviceKey := hmacSHA256(regionKey, "s3")
	signingKey := hmacSHA256(serviceKey, "aws4_request")
	signature := hex.EncodeToString(hmacSHA256(signingKey, stringToSign))
	req.Header.Set("Authorization", algorithm+" Credential="+accessKey+"/"+scope+", SignedHeaders="+signedHeaders+", Signature="+signature)
}

func hmacSHA256(key []byte, value string) []byte {
	h := hmac.New(sha256.New, key)
	_, _ = h.Write([]byte(value))
	return h.Sum(nil)
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
