package partition

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// CatalogEntry represents metadata for an archived partition file in local or cold cloud storage.
type CatalogEntry struct {
	ID             string             `json:"id"`
	Model          string             `json:"model"`
	PartitionFile  string             `json:"partition_file"`
	TargetLocation string             `json:"target_location"`
	SinkType       string             `json:"sink_type"` // "local" or "s3"
	RecordCount    int                `json:"record_count"`
	CutoffDate     string             `json:"cutoff_date"`
	MinDate        float32            `json:"min_date"`
	MaxDate        float32            `json:"max_date"`
	RecordNames    []string           `json:"record_names,omitempty"`
	CreatedAt      time.Time          `json:"created_at"`
	SizeBytes      int64              `json:"size_bytes"`
	S3Metadata     *CatalogS3Metadata `json:"s3_metadata,omitempty"`
}

// CatalogS3Metadata holds cloud sink parameters for S3 retrieval without exposing sensitive secrets.
type CatalogS3Metadata struct {
	Endpoint  string `json:"endpoint"`
	Bucket    string `json:"bucket"`
	Prefix    string `json:"prefix"`
	Region    string `json:"region"`
	AccessKey string `json:"access_key,omitempty"`
}

// Catalog manages persistent partition manifests and provides fast in-memory indexing
// of archived historical records across storage tiers.
type Catalog struct {
	mu          sync.RWMutex
	filePath    string
	entries     []*CatalogEntry
	recordIndex map[string]map[string]*CatalogEntry // model -> recordName -> entry
	fileIndex   map[string]*CatalogEntry            // partitionFilename -> entry
}

// OpenCatalog loads or initializes a partition catalog from the specified JSON file.
func OpenCatalog(filePath string) (*Catalog, error) {
	if filePath == "" {
		return nil, errors.New("partition catalog file path cannot be empty")
	}

	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("failed creating catalog directory: %w", err)
	}

	c := &Catalog{
		filePath:    filePath,
		entries:     make([]*CatalogEntry, 0),
		recordIndex: make(map[string]map[string]*CatalogEntry),
		fileIndex:   make(map[string]*CatalogEntry),
	}

	if data, err := os.ReadFile(filePath); err == nil && len(data) > 0 {
		var entries []*CatalogEntry
		if err := json.Unmarshal(data, &entries); err != nil {
			return nil, fmt.Errorf("failed unmarshaling catalog file %s: %w", filePath, err)
		}
		for _, e := range entries {
			c.indexEntry(e)
		}
	} else if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("failed reading catalog file %s: %w", filePath, err)
	}

	return c, nil
}

func (c *Catalog) indexEntry(e *CatalogEntry) {
	c.entries = append(c.entries, e)
	if e.PartitionFile != "" {
		c.fileIndex[e.PartitionFile] = e
	}
	if e.TargetLocation != "" {
		c.fileIndex[e.TargetLocation] = e
	}

	if _, ok := c.recordIndex[e.Model]; !ok {
		c.recordIndex[e.Model] = make(map[string]*CatalogEntry)
	}
	for _, name := range e.RecordNames {
		c.recordIndex[e.Model][name] = e
	}
}

// RegisterPartition records a newly completed partition and atomically flushes the catalog to disk.
func (c *Catalog) RegisterPartition(e *CatalogEntry) error {
	if e == nil {
		return errors.New("catalog entry cannot be nil")
	}
	if e.ID == "" {
		e.ID = e.PartitionFile
	}
	if e.CreatedAt.IsZero() {
		e.CreatedAt = time.Now().UTC()
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.indexEntry(e)
	return c.saveLocked()
}

func (c *Catalog) saveLocked() error {
	data, err := json.MarshalIndent(c.entries, "", "  ")
	if err != nil {
		return fmt.Errorf("failed marshaling partition catalog: %w", err)
	}

	tmpFile := fmt.Sprintf("%s.tmp.%d", c.filePath, time.Now().UnixNano())
	if err := os.WriteFile(tmpFile, data, 0o600); err != nil {
		return fmt.Errorf("failed writing temporary catalog file: %w", err)
	}

	if err := os.Rename(tmpFile, c.filePath); err != nil {
		_ = os.Remove(tmpFile)
		return fmt.Errorf("failed atomically publishing catalog file: %w", err)
	}

	return nil
}

// ListEntries returns all registered partition entries, optionally filtered by model.
func (c *Catalog) ListEntries(model string) []*CatalogEntry {
	c.mu.RLock()
	defer c.mu.RUnlock()

	out := make([]*CatalogEntry, 0, len(c.entries))
	for _, e := range c.entries {
		if model == "" || e.Model == model {
			out = append(out, e)
		}
	}
	return out
}

// FindRecord searches the partition catalog for a specific archived record name in the given model.
func (c *Catalog) FindRecord(model, recordName string) (*CatalogEntry, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	modelMap, ok := c.recordIndex[model]
	if !ok {
		return nil, false
	}
	entry, ok := modelMap[recordName]
	return entry, ok
}

// GetEntry returns a partition catalog entry by its filename or target location.
func (c *Catalog) GetEntry(fileOrLoc string) (*CatalogEntry, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	e, ok := c.fileIndex[fileOrLoc]
	if !ok {
		// Also check base filename
		base := filepath.Base(fileOrLoc)
		e, ok = c.fileIndex[base]
	}
	return e, ok
}
