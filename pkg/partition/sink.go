package partition

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"errors"
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

// PartitionSink defines the storage destination for archived partition files (Local Disk or S3).
//
//nolint:revive // Stutter maintained for API backward compatibility.
type PartitionSink interface {
	// SavePartitionFromFile stores the file at srcPath under the partition name
	// filename, streaming it so an arbitrarily large archive is never buffered
	// fully in memory.
	SavePartitionFromFile(filename, srcPath string) (string, error)
	Name() string
}

// Sink is an alias for PartitionSink without package stutter.
type Sink = PartitionSink

// LocalDiskSink saves partition files to a local directory (e.g. /data/partitions/...)
type LocalDiskSink struct {
	BaseDir string
}

// NewLocalDiskSink creates a sink targeting a local filesystem directory.
func NewLocalDiskSink(baseDir string) *LocalDiskSink {
	return &LocalDiskSink{BaseDir: baseDir}
}

// Name returns the descriptive name of the local disk sink.
func (s *LocalDiskSink) Name() string {
	return fmt.Sprintf("LocalDisk (%s)", s.BaseDir)
}

// SavePartitionFromFile copies the partition file from srcPath into the sink.
func (s *LocalDiskSink) SavePartitionFromFile(filename, srcPath string) (string, error) {
	if filename == "" || filepath.Base(filename) != filename || filename == "." || filename == ".." {
		return "", fmt.Errorf("invalid partition filename %q", filename)
	}
	src, err := os.Open(srcPath)
	if err != nil {
		return "", fmt.Errorf("failed opening source partition %s: %w", srcPath, err)
	}
	defer src.Close()
	return s.savePartition(filename, src)
}

func (s *LocalDiskSink) savePartition(filename string, src io.Reader) (string, error) {
	if err := mkdirPartitionDirDurably(s.BaseDir); err != nil {
		return "", fmt.Errorf("failed creating partition directory: %w", err)
	}

	targetPath := filepath.Join(s.BaseDir, filename)
	// Write to a temp file in the destination directory, fsync, then rename so a
	// crash never leaves a partially written partition under the final name.
	tmp, err := os.CreateTemp(s.BaseDir, ".partition_*.tmp")
	if err != nil {
		return "", fmt.Errorf("failed creating temp partition file: %w", err)
	}
	tmpPath := tmp.Name()
	cleanup := func() {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
	}
	if _, err := io.Copy(tmp, src); err != nil {
		cleanup()
		return "", fmt.Errorf("failed writing partition file %s: %w", targetPath, err)
	}
	if err := tmp.Sync(); err != nil {
		cleanup()
		return "", fmt.Errorf("failed syncing partition file %s: %w", targetPath, err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return "", fmt.Errorf("failed closing partition file %s: %w", targetPath, err)
	}
	if err := os.Chmod(tmpPath, 0600); err != nil {
		_ = os.Remove(tmpPath)
		return "", fmt.Errorf("failed setting partition file permissions: %w", err)
	}
	// Publish via os.Link instead of os.Rename so an existing archive (e.g. when a
	// custom format like "{date}.anchordb" runs twice on the same day) is never
	// overwritten. If targetPath already exists, disambiguate with a UUID suffix.
	if err := os.Link(tmpPath, targetPath); err != nil {
		if os.IsExist(err) {
			ext := filepath.Ext(filename)
			stem := strings.TrimSuffix(filename, ext)
			uniqueName := fmt.Sprintf("%s_%s%s", stem, GenerateUUID(), ext)
			targetPath = filepath.Join(s.BaseDir, uniqueName)
			if linkErr := os.Link(tmpPath, targetPath); linkErr != nil {
				_ = os.Remove(tmpPath)
				return "", fmt.Errorf("failed publishing partition file %s: %w", targetPath, linkErr)
			}
		} else {
			_ = os.Remove(tmpPath)
			return "", fmt.Errorf("failed publishing partition file %s: %w", targetPath, err)
		}
	}
	_ = os.Remove(tmpPath)
	// The archive must survive a crash before its source can be pruned.
	if err := syncPartitionDir(s.BaseDir); err != nil {
		return "", fmt.Errorf("failed syncing partition directory for %s: %w", targetPath, err)
	}
	return targetPath, nil
}

// mkdirPartitionDirDurably also commits each newly created directory entry.
// A durable archive filename is insufficient if its parent directory vanishes.
func mkdirPartitionDirDurably(dir string) error {
	var missing []string
	for current := filepath.Clean(dir); ; current = filepath.Dir(current) {
		if _, err := os.Stat(current); err == nil {
			break
		} else if !os.IsNotExist(err) {
			return err
		}
		missing = append(missing, current)
		if filepath.Dir(current) == current {
			return fmt.Errorf("no existing ancestor for %s", dir)
		}
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	for i := len(missing) - 1; i >= 0; i-- {
		if err := syncPartitionDir(filepath.Dir(missing[i])); err != nil {
			return err
		}
	}
	return nil
}

func syncPartitionDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// SavePartition writes raw byte payload to the target partition file.
func (s *LocalDiskSink) SavePartition(filename string, data []byte) (string, error) {
	if filename == "" || filepath.Base(filename) != filename || filename == "." || filename == ".." {
		return "", fmt.Errorf("invalid partition filename %q", filename)
	}
	return s.savePartition(filename, bytes.NewReader(data))
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
	// AllowInsecureHTTP permits sending SigV4 credentials to a plaintext http
	// endpoint. Leave false unless the endpoint is a trusted local/loopback
	// service, since the Authorization header is otherwise sent in the clear.
	AllowInsecureHTTP bool
}

// NewS3Sink creates an S3-compatible partition sink.
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

// Name returns the descriptive name of the S3 sink.
func (s *S3Sink) Name() string {
	return fmt.Sprintf("S3 (%s/%s/%s)", s.Endpoint, s.Bucket, s.Prefix)
}

// SavePartitionFromFile streams the local partition file into the S3 bucket.
func (s *S3Sink) SavePartitionFromFile(filename, srcPath string) (string, error) {
	src, err := os.Open(srcPath)
	if err != nil {
		return "", fmt.Errorf("S3Sink: open source partition %s: %w", srcPath, err)
	}
	defer src.Close()
	fi, err := src.Stat()
	if err != nil {
		return "", fmt.Errorf("S3Sink: stat source partition %s: %w", srcPath, err)
	}
	return s.put(filename, src, fi.Size())
}

// SavePartition uploads raw bytes as an object to the S3 bucket.
func (s *S3Sink) SavePartition(filename string, data []byte) (string, error) {
	return s.put(filename, bytes.NewReader(data), int64(len(data)))
}

// put uploads an object whose bytes come from body. body must be seekable so the
// payload can be hashed for SigV4 and then re-read for transmission without
// buffering the whole object in memory.
func (s *S3Sink) put(filename string, body io.ReadSeeker, size int64) (string, error) {
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
	// Never transmit SigV4 credentials (Authorization header) over plaintext HTTP.
	if baseURL.Scheme == "http" && s.AccessKey != "" && !s.AllowInsecureHTTP {
		return "", fmt.Errorf("S3Sink: refusing to send credentials over plaintext http endpoint %q; use https or set AllowInsecureHTTP", s.Endpoint)
	}
	key := path.Join(strings.Trim(s.Prefix, "/"), filename)
	objectPath := path.Join(strings.TrimSuffix(baseURL.Path, "/"), s.Bucket, key)
	baseURL.Path = objectPath
	baseURL.RawPath = ""

	// Hash the payload by streaming it, then rewind for the actual upload.
	hasher := sha256.New()
	if _, err := io.Copy(hasher, body); err != nil {
		return "", fmt.Errorf("S3Sink: hash payload: %w", err)
	}
	if _, err := body.Seek(0, io.SeekStart); err != nil {
		return "", fmt.Errorf("S3Sink: rewind payload: %w", err)
	}
	payloadHex := hex.EncodeToString(hasher.Sum(nil))

	req, err := http.NewRequest(http.MethodPut, baseURL.String(), body)
	if err != nil {
		return "", fmt.Errorf("S3Sink: failed creating request: %w", err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.ContentLength = size

	if s.AccessKey != "" {
		if s.SecretKey == "" {
			return "", fmt.Errorf("S3Sink: secret key cannot be empty when access key is set")
		}
		region := s.Region
		if region == "" {
			region = "us-east-1"
		}
		signS3Request(req, payloadHex, s.AccessKey, s.SecretKey, region, time.Now().UTC())
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

// S3ObjectInfo represents an object entry in an S3 bucket partition listing.
type S3ObjectInfo struct {
	Key          string    `xml:"Key" json:"key"`
	LastModified time.Time `xml:"LastModified" json:"last_modified"`
	Size         int64     `xml:"Size" json:"size"`
}

type listBucketResult struct {
	XMLName  xml.Name       `xml:"ListBucketResult"`
	Contents []S3ObjectInfo `xml:"Contents"`
}

// FetchPartition downloads an object from S3 using SigV4 authentication and atomically writes it to destPath.
func (s *S3Sink) FetchPartition(ctx context.Context, keyOrFilename, destPath string) error {
	if keyOrFilename == "" {
		return errors.New("S3Sink: object key or filename cannot be empty")
	}
	if s.Bucket == "" {
		return errors.New("S3Sink: bucket name cannot be empty")
	}

	endpoint := s.Endpoint
	if endpoint == "" {
		endpoint = "https://s3.amazonaws.com"
	}
	baseURL, err := url.Parse(endpoint)
	if err != nil || (baseURL.Scheme != "http" && baseURL.Scheme != "https") || baseURL.Host == "" {
		return fmt.Errorf("S3Sink: invalid endpoint %q", endpoint)
	}
	if baseURL.Scheme == "http" && s.AccessKey != "" && !s.AllowInsecureHTTP {
		return fmt.Errorf("S3Sink: refusing to send credentials over plaintext http endpoint %q", s.Endpoint)
	}

	// Clean key
	key := keyOrFilename
	if strings.HasPrefix(key, "s3://") {
		prefix := fmt.Sprintf("s3://%s/", s.Bucket)
		key = strings.TrimPrefix(key, prefix)
	}
	key = strings.TrimPrefix(key, "/")
	if !strings.Contains(key, "/") && s.Prefix != "" {
		key = path.Join(strings.Trim(s.Prefix, "/"), key)
	}

	objectPath := path.Join(strings.TrimSuffix(baseURL.Path, "/"), s.Bucket, key)
	baseURL.Path = objectPath
	baseURL.RawPath = ""

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL.String(), nil)
	if err != nil {
		return fmt.Errorf("S3Sink: create GET request: %w", err)
	}

	const emptySHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	if s.AccessKey != "" {
		if s.SecretKey == "" {
			return errors.New("S3Sink: secret key cannot be empty when access key is set")
		}
		region := s.Region
		if region == "" {
			region = "us-east-1"
		}
		signS3Request(req, emptySHA256, s.AccessKey, s.SecretKey, region, time.Now().UTC())
	}

	client := s.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("S3Sink: download from %s failed: %w", req.URL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("S3Sink: download returned HTTP %d: %s", resp.StatusCode, string(snippet))
	}

	if err := os.MkdirAll(filepath.Dir(destPath), 0o750); err != nil {
		return fmt.Errorf("S3Sink: create destination directory: %w", err)
	}

	tmpFile := fmt.Sprintf("%s.download.%d", destPath, time.Now().UnixNano())
	f, err := os.OpenFile(tmpFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("S3Sink: create temporary download file: %w", err)
	}

	if _, err := io.Copy(f, resp.Body); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpFile)
		return fmt.Errorf("S3Sink: stream download: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpFile)
		return fmt.Errorf("S3Sink: sync temporary download file: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmpFile)
		return fmt.Errorf("S3Sink: close temporary download file: %w", err)
	}

	if err := os.Rename(tmpFile, destPath); err != nil {
		_ = os.Remove(tmpFile)
		return fmt.Errorf("S3Sink: atomic rename download file: %w", err)
	}

	return nil
}

// ListPartitions lists all .anchordb partition objects in the S3 bucket with the configured prefix.
func (s *S3Sink) ListPartitions(ctx context.Context) ([]S3ObjectInfo, error) {
	if s.Bucket == "" {
		return nil, errors.New("S3Sink: bucket name cannot be empty")
	}

	endpoint := s.Endpoint
	if endpoint == "" {
		endpoint = "https://s3.amazonaws.com"
	}
	baseURL, err := url.Parse(endpoint)
	if err != nil || (baseURL.Scheme != "http" && baseURL.Scheme != "https") || baseURL.Host == "" {
		return nil, fmt.Errorf("S3Sink: invalid endpoint %q", endpoint)
	}
	if baseURL.Scheme == "http" && s.AccessKey != "" && !s.AllowInsecureHTTP {
		return nil, fmt.Errorf("S3Sink: refusing to send credentials over plaintext http endpoint %q", s.Endpoint)
	}

	bucketPath := path.Join(strings.TrimSuffix(baseURL.Path, "/"), s.Bucket)
	baseURL.Path = bucketPath
	baseURL.RawPath = ""

	q := url.Values{}
	q.Set("list-type", "2")
	cleanPrefix := strings.Trim(s.Prefix, "/")
	if cleanPrefix != "" {
		q.Set("prefix", cleanPrefix)
	}
	baseURL.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("S3Sink: create list request: %w", err)
	}

	const emptySHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	if s.AccessKey != "" {
		if s.SecretKey == "" {
			return nil, errors.New("S3Sink: secret key cannot be empty when access key is set")
		}
		region := s.Region
		if region == "" {
			region = "us-east-1"
		}
		signS3Request(req, emptySHA256, s.AccessKey, s.SecretKey, region, time.Now().UTC())
	}

	client := s.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("S3Sink: list request to %s failed: %w", req.URL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("S3Sink: list returned HTTP %d: %s", resp.StatusCode, string(snippet))
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("S3Sink: read list response: %w", err)
	}

	var res listBucketResult
	if err := xml.Unmarshal(bodyBytes, &res); err != nil {
		return nil, fmt.Errorf("S3Sink: parse XML list response: %w", err)
	}

	var partitions []S3ObjectInfo
	for _, item := range res.Contents {
		if strings.HasSuffix(item.Key, ".anchordb") {
			partitions = append(partitions, item)
		}
	}

	return partitions, nil
}

func signS3Request(req *http.Request, payloadHex, accessKey, secretKey, region string, now time.Time) {
	const algorithm = "AWS4-HMAC-SHA256"
	date := now.Format("20060102")
	amzDate := now.Format("20060102T150405Z")
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
