package partition

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/rachmanzz/anchordb/pkg/storage"
)

// RecordDTO holds user-facing properties for a partition record.
type RecordDTO struct {
	Name  string             `json:"name"`
	Props map[string]float32 `json:"props"`
}

// S3PartitionCache manages a local disk cache for partition database files retrieved from S3.
type S3PartitionCache struct {
	mu       sync.Mutex
	cacheDir string
}

// NewS3PartitionCache initializes a partition cache directory on disk.
func NewS3PartitionCache(cacheDir string) (*S3PartitionCache, error) {
	if cacheDir == "" {
		return nil, errors.New("s3 cache directory cannot be empty")
	}
	if err := os.MkdirAll(cacheDir, 0o750); err != nil {
		return nil, fmt.Errorf("failed creating s3 cache directory: %w", err)
	}
	return &S3PartitionCache{
		cacheDir: cacheDir,
	}, nil
}

// CacheDir returns the underlying directory on disk.
func (c *S3PartitionCache) CacheDir() string {
	return c.cacheDir
}

// EnsurePartition returns the path to a locally cached copy of the partition file,
// fetching it from S3 if not already present.
func (c *S3PartitionCache) EnsurePartition(ctx context.Context, sink *S3Sink, keyOrFilename string) (string, error) {
	if sink == nil {
		return "", errors.New("s3 sink cannot be nil")
	}
	if keyOrFilename == "" {
		return "", errors.New("key or filename cannot be empty")
	}

	cleanBase := filepath.Base(keyOrFilename)
	if !strings.HasSuffix(cleanBase, ".anchordb") {
		cleanBase += ".anchordb"
	}

	targetPath := filepath.Join(c.cacheDir, cleanBase)

	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if already cached
	if fi, err := os.Stat(targetPath); err == nil && fi.Size() > 0 {
		return targetPath, nil
	}

	// Fetch from S3
	if err := sink.FetchPartition(ctx, keyOrFilename, targetPath); err != nil {
		return "", fmt.Errorf("failed fetching partition from s3: %w", err)
	}

	return targetPath, nil
}

// InspectPartition fetches (if needed), opens, and queries an S3 partition file.
func (c *S3PartitionCache) InspectPartition(ctx context.Context, sink *S3Sink, keyOrFilename, recordName string, limit int) ([]RecordDTO, map[string]float32, error) {
	localPath, err := c.EnsurePartition(ctx, sink, keyOrFilename)
	if err != nil {
		return nil, nil, err
	}

	pdb, err := storage.Open(localPath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed opening cached partition %s: %w", localPath, err)
	}
	defer pdb.Close()

	if recordName != "" {
		props, err := pdb.GetRecord(recordName)
		if err != nil {
			return nil, nil, fmt.Errorf("record %q not found in partition: %w", recordName, err)
		}
		return nil, props, nil
	}

	if limit <= 0 || limit > 1000 {
		limit = 50
	}

	records, _, err := pdb.ListRecordsWithPaging(storage.QueryOptions{Limit: limit})
	if err != nil {
		return nil, nil, fmt.Errorf("failed listing records in partition: %w", err)
	}

	out := make([]RecordDTO, len(records))
	for i, r := range records {
		out[i] = RecordDTO{
			Name:  r.Name,
			Props: r.Props,
		}
	}

	return out, nil, nil
}
