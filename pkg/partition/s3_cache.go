package partition

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rachmanzz/anchordb/pkg/storage"
)

const (
	defaultS3CacheMaxBytes       int64 = 1 << 30 // 1 GiB
	defaultS3CacheMaxOpenHandles       = 32
)

// RecordDTO holds user-facing properties for a partition record.
type RecordDTO struct {
	Name  string              `json:"name"`
	Props map[string]float32  `json:"props"`
	Attrs []storage.AttrEntry `json:"attrs,omitempty"`
}

// S3CacheOptions configures capacity, TTL, and open-handle bounds for S3PartitionCache.
type S3CacheOptions struct {
	// MaxBytes bounds total disk usage of cached partition files (0 = 1 GiB default, <0 = unlimited).
	MaxBytes int64
	// TTL expires cached partition files after this duration (0 = never expire).
	TTL time.Duration
	// MaxOpenHandles bounds simultaneously open read-only partition DB handles (0 = 32 default).
	MaxOpenHandles int
}

type cacheHandle struct {
	db   *storage.AnchorDB
	path string
	refs int
}

type cacheEntry struct {
	key       string
	path      string
	gen       uint64
	size      int64
	fetchedAt time.Time
	lastUsed  time.Time
	refs      int
	openMu    sync.Mutex
	handle    *cacheHandle
}

type flightCall struct {
	done    chan struct{}
	cancel  context.CancelFunc
	waiters atomic.Int64
	val     string
	err     error
}

// S3PartitionCache manages a bounded local disk and handle cache for partition database files retrieved from S3.
type S3PartitionCache struct {
	mu             sync.Mutex
	cacheDir       string
	maxBytes       int64
	ttl            time.Duration
	maxOpenHandles int
	totalBytes     int64
	entries        map[string]*cacheEntry

	flightMu sync.Mutex
	flights  map[string]*flightCall
}

// NewS3PartitionCache initializes a partition cache directory on disk with default bounds.
func NewS3PartitionCache(cacheDir string) (*S3PartitionCache, error) {
	return NewS3PartitionCacheWithOptions(cacheDir, S3CacheOptions{})
}

// NewS3PartitionCacheWithOptions initializes a partition cache directory with custom capacity, TTL, and handle bounds.
func NewS3PartitionCacheWithOptions(cacheDir string, opts S3CacheOptions) (*S3PartitionCache, error) {
	if cacheDir == "" {
		return nil, errors.New("s3 cache directory cannot be empty")
	}
	if err := os.MkdirAll(cacheDir, 0o750); err != nil {
		return nil, fmt.Errorf("failed creating s3 cache directory: %w", err)
	}
	maxBytes := opts.MaxBytes
	if maxBytes == 0 {
		maxBytes = defaultS3CacheMaxBytes
	}
	maxHandles := opts.MaxOpenHandles
	if maxHandles <= 0 {
		maxHandles = defaultS3CacheMaxOpenHandles
	}
	c := &S3PartitionCache{
		cacheDir:       cacheDir,
		maxBytes:       maxBytes,
		ttl:            opts.TTL,
		maxOpenHandles: maxHandles,
		entries:        make(map[string]*cacheEntry),
		flights:        make(map[string]*flightCall),
	}
	c.scanExistingLocked()
	return c, nil
}

func (c *S3PartitionCache) scanExistingLocked() {
	dirEntries, err := os.ReadDir(c.cacheDir)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Printf("[S3_CACHE] Failed reading cache directory %s: %v", c.cacheDir, err)
		}
		return
	}
	for _, de := range dirEntries {
		if de.IsDir() || !strings.HasSuffix(de.Name(), ".anchordb") {
			continue
		}
		info, err := de.Info()
		if err != nil {
			log.Printf("[S3_CACHE] Failed stating cached partition %s: %v", de.Name(), err)
			continue
		}
		if info.Size() <= 0 {
			continue
		}
		path := filepath.Join(c.cacheDir, de.Name())
		diskKey := "disk:" + path
		c.entries[diskKey] = &cacheEntry{
			key:       diskKey,
			path:      path,
			size:      info.Size(),
			fetchedAt: info.ModTime(),
			lastUsed:  info.ModTime(),
		}
		c.totalBytes += info.Size()
	}
}

// CacheDir returns the underlying directory on disk.
func (c *S3PartitionCache) CacheDir() string {
	return c.cacheDir
}

func (c *S3PartitionCache) cacheKeyAndPath(sink *S3Sink, keyOrFilename string, gen uint64) (string, string) {
	cleanBase := filepath.Base(keyOrFilename)
	if cleanBase == "." || cleanBase == "/" || cleanBase == "" {
		cleanBase = "partition"
	}
	if !strings.HasSuffix(cleanBase, ".anchordb") {
		cleanBase += ".anchordb"
	}
	fullKey := sink.Bucket + "\x00" + sink.Prefix + "\x00" + keyOrFilename
	sum := sha256.Sum256([]byte(fullKey))
	prefix := hex.EncodeToString(sum[:8])
	if gen == 0 {
		return fullKey, filepath.Join(c.cacheDir, prefix+"_"+cleanBase)
	}
	return fullKey, filepath.Join(c.cacheDir, fmt.Sprintf("%s_v%d_%s", prefix, gen, cleanBase))
}

func removePartitionFiles(path string) {
	if path == "" {
		return
	}
	_ = os.Remove(path)
	_ = os.Remove(path + ".wal")
	_ = os.Remove(path + ".lock")
}

func (c *S3PartitionCache) isFresh(entry *cacheEntry, now time.Time) bool {
	if entry == nil {
		return false
	}
	if c.ttl > 0 && now.Sub(entry.fetchedAt) > c.ttl {
		return false
	}
	fi, err := os.Stat(entry.path)
	return err == nil && fi.Size() > 0
}

// EnsurePartition returns the path to a locally cached copy of the partition file,
// fetching it from S3 if missing or expired. Concurrent misses for the same key share
// one S3 download without holding the cache mutex across network I/O.
func (c *S3PartitionCache) EnsurePartition(ctx context.Context, sink *S3Sink, keyOrFilename string) (string, error) {
	_, path, err := c.ensurePartitionPinned(ctx, sink, keyOrFilename, false)
	return path, err
}

func (c *S3PartitionCache) ensurePartitionPinned(ctx context.Context, sink *S3Sink, keyOrFilename string, pin bool) (*cacheEntry, string, error) {
	if sink == nil {
		return nil, "", errors.New("s3 sink cannot be nil")
	}
	if keyOrFilename == "" {
		return nil, "", errors.New("key or filename cannot be empty")
	}
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}

	cacheKey, basePath := c.cacheKeyAndPath(sink, keyOrFilename, 0)
	now := time.Now()

	c.mu.Lock()
	if entry, ok := c.entries[cacheKey]; ok && c.isFresh(entry, now) {
		entry.lastUsed = now
		if pin {
			entry.refs++
		}
		path := entry.path
		c.mu.Unlock()
		return entry, path, nil
	}
	c.mu.Unlock()

	_, sfErr := c.doSingleFlight(ctx, cacheKey, func(fetchCtx context.Context) (string, error) {
		// Re-check freshness under c.mu after winning or joining singleflight.
		now := time.Now()
		var targetGen uint64
		targetPath := basePath
		c.mu.Lock()
		if entry, ok := c.entries[cacheKey]; ok {
			if c.isFresh(entry, now) {
				entry.lastUsed = now
				path := entry.path
				c.mu.Unlock()
				return path, nil
			}
			// If readers still hold the previous generation's handle/flock,
			// fetch the refreshed copy to a distinct generation path.
			if entry.refs > 0 || entry.handle != nil {
				targetGen = entry.gen + 1
				_, targetPath = c.cacheKeyAndPath(sink, keyOrFilename, targetGen)
			} else {
				targetGen = entry.gen
				if entry.path != "" {
					targetPath = entry.path
				}
			}
		}
		c.mu.Unlock()

		if err := sink.FetchPartition(fetchCtx, keyOrFilename, targetPath); err != nil {
			removePartitionFiles(targetPath)
			return "", fmt.Errorf("failed fetching partition from s3: %w", err)
		}

		fi, err := os.Stat(targetPath)
		if err != nil {
			return "", fmt.Errorf("failed stating fetched partition %s: %w", targetPath, err)
		}

		var staleHandle *cacheHandle
		var toClose []*cacheHandle

		c.mu.Lock()
		now = time.Now()
		// Clean up any pre-existing startup disk-scan placeholder for this path.
		diskKey := "disk:" + targetPath
		if diskEntry, hasDisk := c.entries[diskKey]; hasDisk {
			c.totalBytes -= diskEntry.size
			delete(c.entries, diskKey)
		}

		entry, exists := c.entries[cacheKey]
		if !exists {
			entry = &cacheEntry{key: cacheKey, path: targetPath, gen: targetGen}
			c.entries[cacheKey] = entry
		} else {
			c.totalBytes -= entry.size
			if entry.handle != nil {
				if entry.handle.refs == 0 {
					staleHandle = entry.handle
				}
				entry.handle = nil
			}
			entry.path = targetPath
			entry.gen = targetGen
		}
		entry.size = fi.Size()
		entry.fetchedAt = now
		entry.lastUsed = now
		c.totalBytes += entry.size
		toClose = c.evictLocked(cacheKey)
		c.mu.Unlock()

		if staleHandle != nil {
			if staleHandle.db != nil {
				_ = staleHandle.db.Close()
			}
			if staleHandle.path != "" && staleHandle.path != targetPath {
				removePartitionFiles(staleHandle.path)
			}
		}
		for _, h := range toClose {
			if h.db != nil {
				_ = h.db.Close()
			}
			removePartitionFiles(h.path)
		}
		return targetPath, nil
	})
	if sfErr != nil {
		return nil, "", sfErr
	}

	c.mu.Lock()
	entry, ok := c.entries[cacheKey]
	if !ok {
		c.mu.Unlock()
		return nil, "", fmt.Errorf("cached partition %s evicted before acquire", keyOrFilename)
	}
	entry.lastUsed = time.Now()
	if pin {
		entry.refs++
	}
	finalPath := entry.path
	c.mu.Unlock()
	return entry, finalPath, nil
}

func (c *S3PartitionCache) doSingleFlight(ctx context.Context, key string, fn func(context.Context) (string, error)) (string, error) {
	c.flightMu.Lock()
	call, ok := c.flights[key]
	if !ok {
		fetchCtx, fetchCancel := context.WithCancel(context.Background())
		call = &flightCall{
			done:   make(chan struct{}),
			cancel: fetchCancel,
		}
		call.waiters.Add(1)
		c.flights[key] = call
		c.flightMu.Unlock()

		go func() {
			defer func() {
				if r := recover(); r != nil {
					call.err = fmt.Errorf("s3 cache singleflight panic: %v", r)
				}
				c.flightMu.Lock()
				delete(c.flights, key)
				c.flightMu.Unlock()
				fetchCancel()
				close(call.done)
			}()
			call.val, call.err = fn(fetchCtx)
		}()
	} else {
		call.waiters.Add(1)
		c.flightMu.Unlock()
	}

	defer func() {
		if call.waiters.Add(-1) == 0 {
			call.cancel()
		}
	}()

	select {
	case <-call.done:
		return call.val, call.err
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

// evictLocked enforces maxBytes disk budget by evicting least-recently-used idle entries.
// Caller must hold c.mu.
func (c *S3PartitionCache) evictLocked(keepKey string) []*cacheHandle {
	if c.maxBytes <= 0 || c.totalBytes <= c.maxBytes {
		return nil
	}
	candidates := make([]*cacheEntry, 0, len(c.entries))
	for k, e := range c.entries {
		if k == keepKey || e.refs > 0 {
			continue
		}
		candidates = append(candidates, e)
	}
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].lastUsed.Before(candidates[j].lastUsed)
	})

	pathInUse := func(path, skipKey string) bool {
		for k, other := range c.entries {
			if k != skipKey && other.path == path {
				return true
			}
		}
		return false
	}

	var toClose []*cacheHandle
	for _, e := range candidates {
		if c.totalBytes <= c.maxBytes {
			break
		}
		if e.handle != nil {
			if e.handle.refs == 0 {
				toClose = append(toClose, e.handle)
			}
			e.handle = nil
		} else if !pathInUse(e.path, e.key) {
			removePartitionFiles(e.path)
		}
		c.totalBytes -= e.size
		delete(c.entries, e.key)
	}
	return toClose
}

// trimOpenHandlesLocked closes idle open DB handles when open count exceeds maxOpenHandles.
// Caller must hold c.mu.
func (c *S3PartitionCache) trimOpenHandlesLocked(keepKey string) []*cacheHandle {
	var openIdle []*cacheEntry
	openCount := 0
	for k, e := range c.entries {
		if e.handle != nil {
			openCount++
			if k != keepKey && e.refs == 0 && e.handle.refs == 0 {
				openIdle = append(openIdle, e)
			}
		}
	}
	if openCount <= c.maxOpenHandles || len(openIdle) == 0 {
		return nil
	}
	sort.Slice(openIdle, func(i, j int) bool {
		return openIdle[i].lastUsed.Before(openIdle[j].lastUsed)
	})
	var toClose []*cacheHandle
	for _, e := range openIdle {
		if openCount <= c.maxOpenHandles {
			break
		}
		toClose = append(toClose, &cacheHandle{db: e.handle.db})
		e.handle = nil
		openCount--
	}
	return toClose
}

func (c *S3PartitionCache) acquireHandle(ctx context.Context, sink *S3Sink, keyOrFilename string) (*cacheEntry, *cacheHandle, error) {
	entry, localPath, err := c.ensurePartitionPinned(ctx, sink, keyOrFilename, true)
	if err != nil {
		return nil, nil, err
	}

	c.mu.Lock()
	h := entry.handle
	if h != nil {
		h.refs++
		c.mu.Unlock()
		return entry, h, nil
	}
	c.mu.Unlock()

	entry.openMu.Lock()
	c.mu.Lock()
	h = entry.handle
	currentPath := entry.path
	if h != nil {
		h.refs++
		c.mu.Unlock()
		entry.openMu.Unlock()
		return entry, h, nil
	}
	c.mu.Unlock()

	if currentPath != "" {
		localPath = currentPath
	}
	if _, statErr := os.Stat(localPath); statErr != nil {
		entry.openMu.Unlock()
		c.releasePin(entry)
		return nil, nil, fmt.Errorf("cached partition missing %s: %w", localPath, statErr)
	}

	opened, openErr := storage.OpenWithOptions(localPath, storage.Options{
		WAL: storage.WALOptions{SyncMode: storage.SyncNone},
	})
	if openErr != nil {
		entry.openMu.Unlock()
		c.releasePin(entry)
		return nil, nil, fmt.Errorf("failed opening cached partition %s: %w", localPath, openErr)
	}

	h = &cacheHandle{
		db:   opened,
		path: localPath,
		refs: 1,
	}
	c.mu.Lock()
	entry.handle = h
	toClose := c.trimOpenHandlesLocked(entry.key)
	c.mu.Unlock()
	entry.openMu.Unlock()

	for _, stale := range toClose {
		if stale.db != nil {
			_ = stale.db.Close()
		}
	}
	return entry, h, nil
}

func (c *S3PartitionCache) releasePin(entry *cacheEntry) {
	c.mu.Lock()
	if entry.refs > 0 {
		entry.refs--
	}
	c.mu.Unlock()
}

func (c *S3PartitionCache) releaseHandle(entry *cacheEntry, h *cacheHandle) {
	var closeSuperseded bool
	var removeSupersededPath string
	c.mu.Lock()
	if entry.refs > 0 {
		entry.refs--
	}
	if h != nil {
		if h.refs > 0 {
			h.refs--
		}
		if h.refs == 0 && entry.handle != h {
			closeSuperseded = true
			if h.path != "" && entry.path != h.path {
				removeSupersededPath = h.path
			}
		}
	}
	c.mu.Unlock()
	if closeSuperseded && h.db != nil {
		_ = h.db.Close()
		if removeSupersededPath != "" {
			removePartitionFiles(removeSupersededPath)
		}
	}
}

// Close releases all open cached partition database handles.
func (c *S3PartitionCache) Close() error {
	c.mu.Lock()
	var toClose []*storage.AnchorDB
	for _, e := range c.entries {
		if e.handle != nil {
			toClose = append(toClose, e.handle.db)
			e.handle = nil
		}
	}
	c.mu.Unlock()

	var closeErr error
	for _, db := range toClose {
		if db == nil {
			continue
		}
		if err := db.Close(); err != nil {
			closeErr = errors.Join(closeErr, err)
		}
	}
	return closeErr
}

// GetRecordTyped fetches (if needed) and reads a single typed record from a cached S3 partition
// without reopening the file per query or triggering flock contention.
func (c *S3PartitionCache) GetRecordTyped(ctx context.Context, sink *S3Sink, keyOrFilename, recordName string, computations map[string]storage.ComputeOp, selectFields []string) (storage.CompactRecord, error) {
	entry, h, err := c.acquireHandle(ctx, sink, keyOrFilename)
	if err != nil {
		return storage.CompactRecord{}, err
	}
	defer c.releaseHandle(entry, h)

	if len(computations) > 0 || len(selectFields) > 0 {
		return h.db.GetRecordTypedWithCompute(recordName, computations, selectFields...)
	}
	return h.db.GetRecordTyped(recordName)
}

// InspectPartitionTyped fetches (if needed), opens, and queries an S3 partition file returning typed attributes.
func (c *S3PartitionCache) InspectPartitionTyped(ctx context.Context, sink *S3Sink, keyOrFilename, recordName string, limit int) ([]RecordDTO, map[string]float32, []storage.AttrEntry, error) {
	records, _, props, attrs, err := c.InspectPartitionTypedWithHasMore(ctx, sink, keyOrFilename, recordName, limit)
	return records, props, attrs, err
}

// InspectPartitionTypedWithHasMore fetches and queries an S3 partition file returning records plus pagination hasMore.
func (c *S3PartitionCache) InspectPartitionTypedWithHasMore(ctx context.Context, sink *S3Sink, keyOrFilename, recordName string, limit int) ([]RecordDTO, bool, map[string]float32, []storage.AttrEntry, error) {
	entry, h, err := c.acquireHandle(ctx, sink, keyOrFilename)
	if err != nil {
		return nil, false, nil, nil, err
	}
	defer c.releaseHandle(entry, h)

	if recordName != "" {
		cr, err := h.db.GetRecordTyped(recordName)
		if err != nil {
			return nil, false, nil, nil, fmt.Errorf("record %q not found in partition: %w", recordName, err)
		}
		return nil, false, cr.ToMap(), cr.ExpandAttrs(), nil
	}

	if limit <= 0 || limit > 1000 {
		limit = 50
	}

	records, hasMore, err := h.db.ListRecordsWithPaging(storage.QueryOptions{Limit: limit})
	if err != nil {
		return nil, false, nil, nil, fmt.Errorf("failed listing records in partition: %w", err)
	}

	out := make([]RecordDTO, len(records))
	for i, r := range records {
		out[i] = RecordDTO{
			Name:  r.Name,
			Props: r.Props,
			Attrs: r.Attrs,
		}
	}

	return out, hasMore, nil, nil, nil
}

// InspectPartition fetches (if needed), opens, and queries an S3 partition file.
func (c *S3PartitionCache) InspectPartition(ctx context.Context, sink *S3Sink, keyOrFilename, recordName string, limit int) ([]RecordDTO, map[string]float32, error) {
	records, props, _, err := c.InspectPartitionTyped(ctx, sink, keyOrFilename, recordName, limit)
	return records, props, err
}
