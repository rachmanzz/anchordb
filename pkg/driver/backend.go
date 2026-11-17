// Package driver provides high-level client drivers, query builders, and backend connectors for AnchorDB.
package driver

import (
	"bytes"
	"container/list"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rachmanzz/anchordb/pkg/scheduler"
	"github.com/rachmanzz/anchordb/pkg/spatial"
	"github.com/rachmanzz/anchordb/pkg/storage"
	"github.com/rachmanzz/anchordb/pkg/wire"
)

// PartitionMeta represents metadata for an archived partition file.
type PartitionMeta struct {
	Name      string    `json:"name"`
	SizeBytes int64     `json:"size_bytes"`
	ModTime   time.Time `json:"mod_time"`
}

// PartitionRecord represents an entity record inside an archived partition.
type PartitionRecord struct {
	Name  string              `json:"name"`
	Props map[string]float32  `json:"props"`
	Attrs []storage.AttrEntry `json:"attrs,omitempty"`
}

// PartitionInspectResult represents the response when inspecting an archived partition file.
type PartitionInspectResult struct {
	Success       bool                `json:"success"`
	PartitionFile string              `json:"partition_file"`
	Name          string              `json:"name,omitempty"`
	Props         map[string]float32  `json:"props,omitempty"`
	Attrs         []storage.AttrEntry `json:"attrs,omitempty"`
	Records       []PartitionRecord   `json:"records,omitempty"`
	Count         int                 `json:"count,omitempty"`
	HasMore       bool                `json:"has_more,omitempty"`
}

// backend defines the internal execution contract shared by local, embedded, and remote engine drivers.
type backend interface {
	Save(ctx context.Context, model, name string, op *uint8, props map[string]float32) (string, error)
	SaveTyped(ctx context.Context, model, name string, op *uint8, attrs []storage.AttrEntry) (string, error)
	SaveBatch(ctx context.Context, model string, records []storage.BatchRecordItem) ([]string, error)
	Get(ctx context.Context, model, name string, selectFields []string, computations map[string]storage.ComputeOp) (*EntityResult, error)
	GetTyped(ctx context.Context, model, name string) (storage.CompactRecord, error)
	List(ctx context.Context, model string, opts storage.QueryOptions) (*PagedListResult, error)
	Update(ctx context.Context, model string, opts storage.UpdateOptions) (int, error)
	Graph(ctx context.Context, model string, inputs []string, relation string, limitK int, orderMode storage.OrderByMode, beamWidth float32) (*GraphResponse, error)
	GraphChain(ctx context.Context, model string, inputs []string, relations []string, limitK int, beamWidth float32) (*ChainResponse, error)
	Edge(ctx context.Context, model, action, source, relation, target string, weight float32) error
	Causal(ctx context.Context, model string, seeds []string, relation string, relations []string, limitK int) ([]storage.CausalResult, error)
	Distance(ctx context.Context, model, wordA, wordB string) (float32, error)
	Centroid(ctx context.Context, model string, words []string) (CentroidResult, error)
	CosineSimilarity(ctx context.Context, model, wordA, wordB string) (float32, error)
	Nearest(ctx context.Context, model, word string, limitK int) ([]storage.GraphQueryResult, error)
	AppendNode(ctx context.Context, model, word string, coord spatial.Point2D, roles [4]float32) error
	SetRelation(ctx context.Context, model, name string, direction spatial.Point2D) error
	Sync(ctx context.Context, model string) error
	Close() error
	DB() *storage.AnchorDB
	IsRemote() bool
	ListPartitions(ctx context.Context, model string) ([]PartitionMeta, error)
	InspectPartition(ctx context.Context, model, partitionFile, recordName string, limit int) (*PartitionInspectResult, error)
	Attach(ctx context.Context, model string, schema storage.ModelSchema) error
	GetSchema(ctx context.Context, model string) (storage.ModelSchema, error)
	ResetModel(ctx context.Context, model string, confirm bool) error
}

// -----------------------------------------------------------------------------
// Local Backend (Embedded / In-Memory / File)
// -----------------------------------------------------------------------------

type localBackend struct {
	filePath string
	db       *storage.AnchorDB
}

func newLocalBackend(filePath string) (*localBackend, error) {
	db, err := storage.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open local AnchorDB at %s: %w", filePath, err)
	}
	return &localBackend{filePath: filePath, db: db}, nil
}

func (b *localBackend) defaultModel() string {
	base := strings.TrimSuffix(filepath.Base(b.filePath), ".anchordb")
	if base == "" || base == "." || base == "/" {
		return "default"
	}
	return base
}

func (b *localBackend) checkModel(model string) error {
	model = strings.TrimSpace(model)
	if model == "" || model == "default" || model == b.defaultModel() {
		return nil
	}
	return fmt.Errorf("local single-file backend only supports model %q (got %q); open a directory in embedded mode for multi-model isolation", b.defaultModel(), model)
}

func (b *localBackend) Save(ctx context.Context, model, name string, op *uint8, props map[string]float32) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := b.checkModel(model); err != nil {
		return "", err
	}
	opStatus := storage.OpStatusActive
	if op != nil {
		opStatus = *op
	}
	return b.db.SaveRecordWithIDAndOp(name, opStatus, props)
}

func (b *localBackend) SaveTyped(ctx context.Context, model, name string, op *uint8, attrs []storage.AttrEntry) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := b.checkModel(model); err != nil {
		return "", err
	}
	opStatus := storage.OpStatusActive
	if op != nil {
		opStatus = *op
	}
	return b.db.SaveRecordTypedWithIDAndOp(name, opStatus, attrs)
}

func (b *localBackend) GetTyped(ctx context.Context, model, name string) (storage.CompactRecord, error) {
	if err := ctx.Err(); err != nil {
		return storage.CompactRecord{}, err
	}
	if err := b.checkModel(model); err != nil {
		return storage.CompactRecord{}, err
	}
	return b.db.GetRecordTyped(name)
}

func (b *localBackend) SaveBatch(ctx context.Context, model string, records []storage.BatchRecordItem) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := b.checkModel(model); err != nil {
		return nil, err
	}
	return b.db.SaveRecordsBatch(records)
}

func (b *localBackend) Get(ctx context.Context, model string, name string, selectFields []string, computations map[string]storage.ComputeOp) (*EntityResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := b.checkModel(model); err != nil {
		return nil, err
	}
	cr, err := b.db.GetRecordTypedWithCompute(name, computations, selectFields...)
	if err != nil {
		return nil, err
	}
	return &EntityResult{Name: name, props: cr.ToMap(), attrs: cr.ExpandAttrs()}, nil
}

func (b *localBackend) List(ctx context.Context, model string, opts storage.QueryOptions) (*PagedListResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := b.checkModel(model); err != nil {
		return nil, err
	}
	opts.Context = ctx
	records, hasMore, err := b.db.ListRecordsWithPaging(opts)
	if err != nil {
		return nil, err
	}
	items := make([]EntityResult, len(records))
	for i, r := range records {
		items[i] = EntityResult{
			Name:  r.Name,
			props: r.Props,
			attrs: r.Attrs,
		}
	}
	return &PagedListResult{
		Items:   items,
		HasMore: hasMore,
	}, nil
}

func (b *localBackend) Update(ctx context.Context, model string, opts storage.UpdateOptions) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := b.checkModel(model); err != nil {
		return 0, err
	}
	opts.Context = ctx
	return b.db.UpdateRecords(opts)
}

func (b *localBackend) Graph(ctx context.Context, model string, inputs []string, relation string, limitK int, orderMode storage.OrderByMode, beamWidth float32) (*GraphResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := b.checkModel(model); err != nil {
		return nil, err
	}
	results, anchor, err := b.db.ExecuteGraphStepWithContext(ctx, inputs, relation, limitK, orderMode, beamWidth)
	if err != nil {
		return nil, err
	}
	return &GraphResponse{Anchor: anchor, Results: results}, nil
}

func (b *localBackend) GraphChain(ctx context.Context, model string, inputs []string, relations []string, limitK int, beamWidth float32) (*ChainResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := b.checkModel(model); err != nil {
		return nil, err
	}
	steps, err := b.db.ExecuteGraphChainWithContext(ctx, inputs, relations, limitK, beamWidth)
	if err != nil {
		return nil, err
	}
	seq := make([]string, len(steps))
	for i, step := range steps {
		seq[i] = step.Word
	}
	return &ChainResponse{
		Sequence: seq,
		Steps:    steps,
	}, nil
}

func (b *localBackend) Edge(ctx context.Context, model, action, source, relation, target string, weight float32) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := b.checkModel(model); err != nil {
		return err
	}
	switch strings.ToLower(action) {
	case "remove", "delete", "unlink":
		return b.db.RemoveEdge(source, relation, target)
	default:
		return b.db.AddEdge(source, relation, target, weight)
	}
}

func (b *localBackend) Causal(ctx context.Context, model string, seeds []string, relation string, relations []string, limitK int) ([]storage.CausalResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := b.checkModel(model); err != nil {
		return nil, err
	}
	if len(relations) > 0 {
		return b.db.ExecuteCausalChain(ctx, seeds, relations, limitK)
	}
	return b.db.ExecuteCausalStep(ctx, seeds, relation, limitK)
}

func (b *localBackend) Distance(_ context.Context, model, wordA, wordB string) (float32, error) {
	if err := b.checkModel(model); err != nil {
		return 0, err
	}
	return b.db.CalculateDistance(wordA, wordB)
}

func (b *localBackend) Centroid(_ context.Context, model string, words []string) (CentroidResult, error) {
	if err := b.checkModel(model); err != nil {
		return CentroidResult{}, err
	}
	coord, roles, err := b.db.CalculateCentroid(words)
	return CentroidResult{Coord: coord, Roles: roles}, err
}

func (b *localBackend) CosineSimilarity(_ context.Context, model, wordA, wordB string) (float32, error) {
	if err := b.checkModel(model); err != nil {
		return 0, err
	}
	return b.db.CalculateCosineSimilarity(wordA, wordB)
}

func (b *localBackend) Nearest(_ context.Context, model, word string, limitK int) ([]storage.GraphQueryResult, error) {
	if err := b.checkModel(model); err != nil {
		return nil, err
	}
	return b.db.FindNearestNeighbors(word, limitK)
}

func (b *localBackend) AppendNode(ctx context.Context, model, word string, coord spatial.Point2D, roles [4]float32) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := b.checkModel(model); err != nil {
		return err
	}
	_, err := b.db.AppendNode(word, coord, roles, 1)
	return err
}

func (b *localBackend) SetRelation(ctx context.Context, model, name string, direction spatial.Point2D) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := b.checkModel(model); err != nil {
		return err
	}
	return b.db.SetRelation(name, direction)
}

func (b *localBackend) Sync(_ context.Context, model string) error {
	if err := b.checkModel(model); err != nil {
		return err
	}
	return b.db.Sync()
}

func (b *localBackend) Close() error {
	return b.db.Close()
}

func (b *localBackend) DB() *storage.AnchorDB {
	return b.db
}

func (b *localBackend) IsRemote() bool {
	return false
}

func listPartitionsInDir(partDir string) ([]PartitionMeta, error) {
	entries, err := os.ReadDir(partDir)
	if err != nil {
		if os.IsNotExist(err) {
			return []PartitionMeta{}, nil
		}
		return nil, fmt.Errorf("failed to read partitions directory: %w", err)
	}
	var out []PartitionMeta
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".anchordb") {
			continue
		}
		info, err := e.Info()
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, fmt.Errorf("failed to stat partition %q: %w", e.Name(), err)
		}
		out = append(out, PartitionMeta{
			Name:      e.Name(),
			SizeBytes: info.Size(),
			ModTime:   info.ModTime(),
		})
	}
	return out, nil
}

func (b *localBackend) ListPartitions(_ context.Context, model string) ([]PartitionMeta, error) {
	if err := b.checkModel(model); err != nil {
		return nil, err
	}
	return listPartitionsInDir(filepath.Join(filepath.Dir(b.filePath), "partitions"))
}

func inspectPartitionFile(ctx context.Context, baseDir, partitionFile, recordName string, limit int) (res *PartitionInspectResult, retErr error) {
	cleanFilename := filepath.Base(partitionFile)
	fullPath := filepath.Join(baseDir, "partitions", cleanFilename)
	if _, err := os.Stat(fullPath); err != nil {
		return nil, fmt.Errorf("partition file not found: %s", cleanFilename)
	}

	pdb, err := storage.Open(fullPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open partition: %w", err)
	}
	defer func() {
		if closeErr := pdb.Close(); closeErr != nil && retErr == nil {
			retErr = fmt.Errorf("failed closing partition %q: %w", cleanFilename, closeErr)
			res = nil
		}
	}()

	if recordName != "" {
		cr, err := pdb.GetRecordTyped(recordName)
		if err != nil {
			return nil, fmt.Errorf("record %q not found in partition: %w", recordName, err)
		}
		return &PartitionInspectResult{
			Success:       true,
			PartitionFile: cleanFilename,
			Name:          recordName,
			Props:         cr.ToMap(),
			Attrs:         cr.ExpandAttrs(),
		}, nil
	}

	if limit <= 0 || limit > 100 {
		limit = 10
	}
	records, hasMore, err := pdb.ListRecordsWithPaging(storage.QueryOptions{Limit: limit, Context: ctx})
	if err != nil {
		return nil, fmt.Errorf("failed to list records in partition: %w", err)
	}

	out := make([]PartitionRecord, len(records))
	for i, r := range records {
		out[i] = PartitionRecord{Name: r.Name, Props: r.Props, Attrs: r.Attrs}
	}

	return &PartitionInspectResult{
		Success:       true,
		PartitionFile: cleanFilename,
		Records:       out,
		Count:         len(out),
		HasMore:       hasMore,
	}, nil
}

func (b *localBackend) InspectPartition(ctx context.Context, model, partitionFile, recordName string, limit int) (*PartitionInspectResult, error) {
	if err := b.checkModel(model); err != nil {
		return nil, err
	}
	return inspectPartitionFile(ctx, filepath.Dir(b.filePath), partitionFile, recordName, limit)
}

func (b *localBackend) Attach(ctx context.Context, model string, schema storage.ModelSchema) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := b.checkModel(model); err != nil {
		return err
	}
	return b.db.AttachSchema(schema)
}

func (b *localBackend) GetSchema(ctx context.Context, model string) (storage.ModelSchema, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := b.checkModel(model); err != nil {
		return nil, err
	}
	s, ok := b.db.GetSchema()
	if !ok {
		return nil, fmt.Errorf("no schema attached to model")
	}
	return s, nil
}

func (b *localBackend) ResetModel(ctx context.Context, model string, confirm bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := b.checkModel(model); err != nil {
		return err
	}
	if !confirm {
		return fmt.Errorf("confirm must be true to reset model schema")
	}
	return b.db.ResetSchema()
}

// -----------------------------------------------------------------------------
// Embedded Backend (In-Process Engine with WAL, Multi-Model, Scheduler & Workers)
// -----------------------------------------------------------------------------

type embeddedBackend struct {
	mu            sync.RWMutex
	dataDir       string
	cfg           clientConfig
	models        map[string]*storage.AnchorDB
	modelPins     map[string]*atomic.Int64
	modelDraining map[string]chan struct{}
	lruMu         sync.Mutex
	modelLRU      *list.List
	modelElems    map[string]*list.Element
	scheduler     *scheduler.InternalScheduler
	closed        bool
}

type embeddedSyncTask struct {
	backend  *embeddedBackend
	interval time.Duration
}

func (t *embeddedSyncTask) Schedule() string {
	return fmt.Sprintf("@every %s", t.interval.String())
}

func (t *embeddedSyncTask) Execute(ctx context.Context) error {
	return t.backend.Sync(ctx, "all")
}

func newEmbeddedBackend(dataDir string, cfg clientConfig) (*embeddedBackend, error) {
	if dataDir == "" {
		dataDir = "."
	}
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory %s: %w", dataDir, err)
	}

	b := &embeddedBackend{
		dataDir:       dataDir,
		cfg:           cfg,
		models:        make(map[string]*storage.AnchorDB),
		modelPins:     make(map[string]*atomic.Int64),
		modelDraining: make(map[string]chan struct{}),
		modelLRU:      list.New(),
		modelElems:    make(map[string]*list.Element),
	}

	if cfg.enableScheduler || cfg.syncInterval > 0 {
		sched := scheduler.NewInternalScheduler()
		if cfg.syncInterval > 0 {
			if err := sched.RegisterTask("embedded_periodic_sync", &embeddedSyncTask{backend: b, interval: cfg.syncInterval}); err != nil {
				_ = b.Close()
				return nil, fmt.Errorf("failed to register embedded periodic sync task: %w", err)
			}
		}
		sched.Start()
		b.scheduler = sched
	}

	if cfg.model != "" {
		if _, release, err := b.acquireDB(cfg.model); err != nil {
			_ = b.Close()
			return nil, err
		} else {
			release()
		}
	}

	return b, nil
}

func makeEmbeddedRelease(pin *atomic.Int64) func() {
	if pin == nil {
		return func() {}
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			pin.Add(-1)
		})
	}
}

func (b *embeddedBackend) touchModelOrder(modelName string) {
	b.lruMu.Lock()
	defer b.lruMu.Unlock()
	if b.modelLRU == nil {
		b.modelLRU = list.New()
	}
	if b.modelElems == nil {
		b.modelElems = make(map[string]*list.Element)
	}
	if elem, ok := b.modelElems[modelName]; ok {
		b.modelLRU.MoveToFront(elem)
		return
	}
	b.modelElems[modelName] = b.modelLRU.PushFront(modelName)
}

func (b *embeddedBackend) ensurePinLocked(modelName string) *atomic.Int64 {
	if b.modelPins == nil {
		b.modelPins = make(map[string]*atomic.Int64)
	}
	pin, ok := b.modelPins[modelName]
	if !ok {
		pin = &atomic.Int64{}
		b.modelPins[modelName] = pin
	}
	return pin
}

func (b *embeddedBackend) pickIdleVictimLocked(skip map[string]bool) (string, *storage.AnchorDB) {
	b.lruMu.Lock()
	if b.modelLRU != nil {
		for elem := b.modelLRU.Back(); elem != nil; elem = elem.Prev() {
			candidate, _ := elem.Value.(string)
			if skip[candidate] {
				continue
			}
			vdb, ok := b.models[candidate]
			if !ok {
				continue
			}
			if pin := b.modelPins[candidate]; pin != nil && pin.Load() > 0 {
				continue
			}
			b.modelLRU.Remove(elem)
			delete(b.modelElems, candidate)
			b.lruMu.Unlock()
			return candidate, vdb
		}
	}
	b.lruMu.Unlock()
	return "", nil
}

func (b *embeddedBackend) evictIdleModelWithUnlockLocked(skip map[string]bool) (bool, error) {
	victim, vdb := b.pickIdleVictimLocked(skip)
	if victim == "" || vdb == nil {
		return false, nil
	}
	delete(b.models, victim)
	delete(b.modelPins, victim)
	if b.modelDraining == nil {
		b.modelDraining = make(map[string]chan struct{})
	}
	done := make(chan struct{})
	b.modelDraining[victim] = done
	b.mu.Unlock()

	if syncErr := vdb.Sync(); syncErr != nil {
		b.mu.Lock()
		delete(b.modelDraining, victim)
		close(done)
		if !b.closed && b.models[victim] == nil {
			b.models[victim] = vdb
			b.ensurePinLocked(victim)
			b.touchModelOrder(victim)
		}
		skip[victim] = true
		return true, fmt.Errorf("failed syncing evicted embedded model %q: %w", victim, syncErr)
	}

	closeErr := vdb.Close()
	b.mu.Lock()
	delete(b.modelDraining, victim)
	close(done)
	if closeErr != nil {
		return true, fmt.Errorf("failed closing evicted embedded model %q: %w", victim, closeErr)
	}
	return true, nil
}

func validateEmbeddedModelName(model string) (string, error) {
	model = strings.TrimSpace(model)
	if model == "" {
		return "default", nil
	}
	if len(model) > 64 {
		return "", fmt.Errorf("invalid model name %q: exceeds 64 characters", model)
	}
	if filepath.Base(model) != model || strings.Contains(model, "..") || strings.Contains(model, "/") || strings.Contains(model, "\\") {
		return "", fmt.Errorf("invalid model name %q: path separators are not allowed", model)
	}
	for _, ch := range model {
		if (ch < 'a' || ch > 'z') && (ch < 'A' || ch > 'Z') && (ch < '0' || ch > '9') && ch != '_' && ch != '-' {
			return "", fmt.Errorf("invalid character %q in model name %q", ch, model)
		}
	}
	return model, nil
}

func (b *embeddedBackend) acquireDB(model string) (*storage.AnchorDB, func(), error) {
	return b.acquireDBInternal(model, true)
}

func (b *embeddedBackend) acquireDBInternal(model string, pinned bool) (*storage.AnchorDB, func(), error) {
	cleanModel, err := validateEmbeddedModelName(model)
	if err != nil {
		return nil, nil, err
	}

	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return nil, nil, fmt.Errorf("embedded engine is closed")
	}
	db, ok := b.models[cleanModel]
	var pin *atomic.Int64
	if ok && pinned {
		pin = b.modelPins[cleanModel]
		if pin != nil {
			pin.Add(1)
		}
	}
	b.mu.RUnlock()

	if ok && (!pinned || pin != nil) {
		b.touchModelOrder(cleanModel)
		if !pinned {
			return db, func() {}, nil
		}
		return db, makeEmbeddedRelease(pin), nil
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	skip := make(map[string]bool)
	for {
		if b.closed {
			return nil, nil, fmt.Errorf("embedded engine is closed")
		}
		if waitCh, draining := b.modelDraining[cleanModel]; draining {
			b.mu.Unlock()
			<-waitCh
			b.mu.Lock()
			continue
		}
		if db, ok := b.models[cleanModel]; ok {
			pin := b.ensurePinLocked(cleanModel)
			if pinned {
				pin.Add(1)
			}
			b.touchModelOrder(cleanModel)
			if !pinned {
				return db, func() {}, nil
			}
			return db, makeEmbeddedRelease(pin), nil
		}

		const maxEmbeddedModels = 500
		if len(b.models) >= maxEmbeddedModels {
			evicted, evErr := b.evictIdleModelWithUnlockLocked(skip)
			if evErr != nil {
				return nil, nil, evErr
			}
			if evicted {
				continue
			}
		}
		break
	}

	filePath := filepath.Join(b.dataDir, cleanModel+".anchordb")
	newDB, err := storage.Open(filePath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open embedded model %q at %s: %w", cleanModel, filePath, err)
	}
	b.models[cleanModel] = newDB
	pin = b.ensurePinLocked(cleanModel)
	if pinned {
		pin.Add(1)
	}
	b.touchModelOrder(cleanModel)
	if !pinned {
		return newDB, func() {}, nil
	}
	return newDB, makeEmbeddedRelease(pin), nil
}

func (b *embeddedBackend) Save(ctx context.Context, model, name string, op *uint8, props map[string]float32) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	db, release, err := b.acquireDB(model)
	if err != nil {
		return "", err
	}
	defer release()
	opStatus := storage.OpStatusActive
	if op != nil {
		opStatus = *op
	}
	return db.SaveRecordWithIDAndOp(name, opStatus, props)
}

func (b *embeddedBackend) SaveTyped(ctx context.Context, model, name string, op *uint8, attrs []storage.AttrEntry) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	db, release, err := b.acquireDB(model)
	if err != nil {
		return "", err
	}
	defer release()
	opStatus := storage.OpStatusActive
	if op != nil {
		opStatus = *op
	}
	return db.SaveRecordTypedWithIDAndOp(name, opStatus, attrs)
}

func (b *embeddedBackend) GetTyped(ctx context.Context, model, name string) (storage.CompactRecord, error) {
	if err := ctx.Err(); err != nil {
		return storage.CompactRecord{}, err
	}
	db, release, err := b.acquireDB(model)
	if err != nil {
		return storage.CompactRecord{}, err
	}
	defer release()
	return db.GetRecordTyped(name)
}

func (b *embeddedBackend) SaveBatch(ctx context.Context, model string, records []storage.BatchRecordItem) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	db, release, err := b.acquireDB(model)
	if err != nil {
		return nil, err
	}
	defer release()
	return db.SaveRecordsBatch(records)
}

func (b *embeddedBackend) Get(ctx context.Context, model, name string, selectFields []string, computations map[string]storage.ComputeOp) (*EntityResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	db, release, err := b.acquireDB(model)
	if err != nil {
		return nil, err
	}
	defer release()
	cr, err := db.GetRecordTypedWithCompute(name, computations, selectFields...)
	if err != nil {
		return nil, err
	}
	return &EntityResult{Name: name, props: cr.ToMap(), attrs: cr.ExpandAttrs()}, nil
}

func (b *embeddedBackend) List(ctx context.Context, model string, opts storage.QueryOptions) (*PagedListResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	db, release, err := b.acquireDB(model)
	if err != nil {
		return nil, err
	}
	defer release()
	opts.Context = ctx
	records, hasMore, err := db.ListRecordsWithPaging(opts)
	if err != nil {
		return nil, err
	}
	items := make([]EntityResult, len(records))
	for i, r := range records {
		items[i] = EntityResult{
			Name:  r.Name,
			props: r.Props,
			attrs: r.Attrs,
		}
	}
	return &PagedListResult{
		Items:   items,
		HasMore: hasMore,
	}, nil
}

func (b *embeddedBackend) Update(ctx context.Context, model string, opts storage.UpdateOptions) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	db, release, err := b.acquireDB(model)
	if err != nil {
		return 0, err
	}
	defer release()
	opts.Context = ctx
	return db.UpdateRecords(opts)
}

func (b *embeddedBackend) Graph(ctx context.Context, model string, inputs []string, relation string, limitK int, orderMode storage.OrderByMode, beamWidth float32) (*GraphResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	db, release, err := b.acquireDB(model)
	if err != nil {
		return nil, err
	}
	defer release()
	results, anchor, err := db.ExecuteGraphStepWithContext(ctx, inputs, relation, limitK, orderMode, beamWidth)
	if err != nil {
		return nil, err
	}
	return &GraphResponse{Anchor: anchor, Results: results}, nil
}

func (b *embeddedBackend) GraphChain(ctx context.Context, model string, inputs []string, relations []string, limitK int, beamWidth float32) (*ChainResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	db, release, err := b.acquireDB(model)
	if err != nil {
		return nil, err
	}
	defer release()
	steps, err := db.ExecuteGraphChainWithContext(ctx, inputs, relations, limitK, beamWidth)
	if err != nil {
		return nil, err
	}
	seq := make([]string, len(steps))
	for i, step := range steps {
		seq[i] = step.Word
	}
	return &ChainResponse{
		Sequence: seq,
		Steps:    steps,
	}, nil
}

func (b *embeddedBackend) Edge(ctx context.Context, model, action, source, relation, target string, weight float32) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	db, release, err := b.acquireDB(model)
	if err != nil {
		return err
	}
	defer release()
	switch strings.ToLower(action) {
	case "remove", "delete", "unlink":
		return db.RemoveEdge(source, relation, target)
	default:
		return db.AddEdge(source, relation, target, weight)
	}
}

func (b *embeddedBackend) Causal(ctx context.Context, model string, seeds []string, relation string, relations []string, limitK int) ([]storage.CausalResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	db, release, err := b.acquireDB(model)
	if err != nil {
		return nil, err
	}
	defer release()
	if len(relations) > 0 {
		return db.ExecuteCausalChain(ctx, seeds, relations, limitK)
	}
	return db.ExecuteCausalStep(ctx, seeds, relation, limitK)
}

func (b *embeddedBackend) Distance(_ context.Context, model, wordA, wordB string) (float32, error) {
	db, release, err := b.acquireDB(model)
	if err != nil {
		return 0, err
	}
	defer release()
	return db.CalculateDistance(wordA, wordB)
}

func (b *embeddedBackend) Centroid(_ context.Context, model string, words []string) (CentroidResult, error) {
	db, release, err := b.acquireDB(model)
	if err != nil {
		return CentroidResult{}, err
	}
	defer release()
	coord, roles, err := db.CalculateCentroid(words)
	return CentroidResult{Coord: coord, Roles: roles}, err
}

func (b *embeddedBackend) CosineSimilarity(_ context.Context, model, wordA, wordB string) (float32, error) {
	db, release, err := b.acquireDB(model)
	if err != nil {
		return 0, err
	}
	defer release()
	return db.CalculateCosineSimilarity(wordA, wordB)
}

func (b *embeddedBackend) Nearest(_ context.Context, model, word string, limitK int) ([]storage.GraphQueryResult, error) {
	db, release, err := b.acquireDB(model)
	if err != nil {
		return nil, err
	}
	defer release()
	return db.FindNearestNeighbors(word, limitK)
}

func (b *embeddedBackend) AppendNode(ctx context.Context, model, word string, coord spatial.Point2D, roles [4]float32) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	db, release, err := b.acquireDB(model)
	if err != nil {
		return err
	}
	defer release()
	_, err = db.AppendNode(word, coord, roles, 1)
	return err
}

func (b *embeddedBackend) SetRelation(ctx context.Context, model, name string, direction spatial.Point2D) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	db, release, err := b.acquireDB(model)
	if err != nil {
		return err
	}
	defer release()
	return db.SetRelation(name, direction)
}

func (b *embeddedBackend) Sync(_ context.Context, model string) error {
	if model != "" && model != "all" {
		db, release, err := b.acquireDB(model)
		if err != nil {
			return err
		}
		defer release()
		return db.Sync()
	}
	b.mu.RLock()
	names := make([]string, 0, len(b.models))
	for name := range b.models {
		names = append(names, name)
	}
	b.mu.RUnlock()

	var syncErrs []error
	for _, name := range names {
		db, release, err := b.acquireDB(name)
		if err != nil {
			syncErrs = append(syncErrs, err)
			continue
		}
		if err := db.Sync(); err != nil {
			syncErrs = append(syncErrs, err)
		}
		release()
	}
	return errors.Join(syncErrs...)
}

func (b *embeddedBackend) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	sched := b.scheduler
	b.mu.Unlock()

	if sched != nil {
		sched.Stop()
	}

	b.mu.Lock()
	for len(b.modelDraining) > 0 {
		var waitCh chan struct{}
		for _, ch := range b.modelDraining {
			waitCh = ch
			break
		}
		b.mu.Unlock()
		<-waitCh
		b.mu.Lock()
	}
	defer b.mu.Unlock()

	var closeErrs []error
	for _, db := range b.models {
		if err := db.Close(); err != nil {
			closeErrs = append(closeErrs, err)
		}
	}
	b.models = make(map[string]*storage.AnchorDB)
	b.modelPins = make(map[string]*atomic.Int64)
	b.lruMu.Lock()
	b.modelLRU = nil
	b.modelElems = nil
	b.lruMu.Unlock()
	return errors.Join(closeErrs...)
}

func (b *embeddedBackend) DB() *storage.AnchorDB {
	db, _, err := b.acquireDBInternal(b.cfg.model, false)
	if err != nil {
		return nil
	}
	return db
}

func (b *embeddedBackend) IsRemote() bool {
	return false
}

func (b *embeddedBackend) ListPartitions(_ context.Context, _ string) ([]PartitionMeta, error) {
	return listPartitionsInDir(filepath.Join(b.dataDir, "partitions"))
}

func (b *embeddedBackend) InspectPartition(ctx context.Context, _, partitionFile, recordName string, limit int) (*PartitionInspectResult, error) {
	return inspectPartitionFile(ctx, b.dataDir, partitionFile, recordName, limit)
}

func (b *embeddedBackend) Attach(ctx context.Context, model string, schema storage.ModelSchema) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	db, release, err := b.acquireDB(model)
	if err != nil {
		return err
	}
	defer release()
	return db.AttachSchema(schema)
}

func (b *embeddedBackend) GetSchema(ctx context.Context, model string) (storage.ModelSchema, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	db, release, err := b.acquireDB(model)
	if err != nil {
		return nil, err
	}
	defer release()
	s, ok := db.GetSchema()
	if !ok {
		return nil, fmt.Errorf("no schema attached to model %q", model)
	}
	return s, nil
}

func (b *embeddedBackend) ResetModel(ctx context.Context, model string, confirm bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !confirm {
		return fmt.Errorf("confirm must be true to reset model schema")
	}
	db, release, err := b.acquireDB(model)
	if err != nil {
		return err
	}
	defer release()
	return db.ResetSchema()
}

// -----------------------------------------------------------------------------
// Remote Backend (HTTP Engine Client)
// -----------------------------------------------------------------------------

type remoteBackend struct {
	baseURL    string
	cfg        clientConfig
	httpClient *http.Client
}

func newRemoteBackend(baseURL string, cfg clientConfig) (*remoteBackend, error) {
	baseURL = strings.TrimRight(baseURL, "/")
	client := cfg.httpClient
	if client == nil {
		client = &http.Client{
			Transport: &http.Transport{
				Proxy: http.ProxyFromEnvironment,
				DialContext: (&net.Dialer{
					Timeout:   30 * time.Second,
					KeepAlive: 30 * time.Second,
				}).DialContext,
				ForceAttemptHTTP2:     true,
				MaxIdleConns:          1024,
				MaxIdleConnsPerHost:   256,
				IdleConnTimeout:       90 * time.Second,
				TLSHandshakeTimeout:   10 * time.Second,
				ExpectContinueTimeout: 1 * time.Second,
			},
			Timeout: cfg.timeout,
		}
	}
	return &remoteBackend{
		baseURL:    baseURL,
		cfg:        cfg,
		httpClient: client,
	}, nil
}

func (b *remoteBackend) doJSON(ctx context.Context, method, endpoint string, reqPayload, respPayload any) error {
	urlStr := b.baseURL + endpoint

	var bodyReader io.Reader
	if reqPayload != nil {
		data, err := json.Marshal(reqPayload)
		if err != nil {
			return fmt.Errorf("failed to marshal request payload: %w", err)
		}
		bodyReader = bytes.NewReader(data)
	}

	req, err := http.NewRequestWithContext(ctx, method, urlStr, bodyReader)
	if err != nil {
		return fmt.Errorf("failed to create http request: %w", err)
	}

	if reqPayload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if b.cfg.token != "" {
		req.Header.Set("Authorization", "Bearer "+b.cfg.token)
	}

	resp, err := b.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("remote engine request failed (%s %s): %w", method, endpoint, err)
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode >= 400 {
		var errResp struct {
			Error string `json:"error"`
		}
		if jsonErr := json.Unmarshal(bodyBytes, &errResp); jsonErr == nil && errResp.Error != "" {
			return fmt.Errorf("engine error (status %d): %s", resp.StatusCode, errResp.Error)
		}
		return fmt.Errorf("engine returned HTTP %d: %s", resp.StatusCode, string(bodyBytes))
	}

	if len(bodyBytes) > 0 {
		var statusEnvelope struct {
			Success *bool  `json:"success"`
			Error   string `json:"error"`
			Message string `json:"message"`
		}
		if jsonErr := json.Unmarshal(bodyBytes, &statusEnvelope); jsonErr == nil && statusEnvelope.Success != nil && !*statusEnvelope.Success {
			msg := statusEnvelope.Error
			if msg == "" {
				msg = statusEnvelope.Message
			}
			if msg == "" {
				msg = "request failed (success=false)"
			}
			return fmt.Errorf("engine error (status %d): %s", resp.StatusCode, msg)
		}
	}

	if respPayload != nil && len(bodyBytes) > 0 {
		if err := json.Unmarshal(bodyBytes, respPayload); err != nil {
			return fmt.Errorf("failed to decode response payload: %w", err)
		}
	}

	return nil
}

func (b *remoteBackend) Save(ctx context.Context, model, name string, op *uint8, props map[string]float32) (string, error) {
	req := map[string]interface{}{
		"model": model,
		"name":  name,
		"props": props,
	}
	if op != nil {
		req["operation"] = *op
	}

	var resp struct {
		Success bool   `json:"success"`
		Name    string `json:"name"`
		Message string `json:"message"`
	}
	if err := b.doJSON(ctx, http.MethodPost, "/v1/save", req, &resp); err != nil {
		return "", err
	}
	return resp.Name, nil
}

func (b *remoteBackend) SaveTyped(ctx context.Context, model, name string, op *uint8, attrs []storage.AttrEntry) (string, error) {
	req := map[string]interface{}{
		"model": model,
		"name":  name,
		"attrs": attrs,
	}
	if op != nil {
		req["operation"] = *op
	}

	var resp struct {
		Success bool   `json:"success"`
		Name    string `json:"name"`
		Message string `json:"message"`
	}
	if err := b.doJSON(ctx, http.MethodPost, "/v1/save", req, &resp); err != nil {
		return "", err
	}
	return resp.Name, nil
}

func (b *remoteBackend) GetTyped(ctx context.Context, model, name string) (storage.CompactRecord, error) {
	req := map[string]interface{}{
		"model": model,
		"name":  name,
	}

	var resp struct {
		Success   bool                `json:"success"`
		Name      string              `json:"name"`
		Operation uint8               `json:"operation"`
		Props     map[string]float32  `json:"props"`
		Attrs     []storage.AttrEntry `json:"attrs"`
	}
	if err := b.doJSON(ctx, http.MethodPost, "/v1/get", req, &resp); err != nil {
		return storage.CompactRecord{}, err
	}
	if len(resp.Attrs) > 0 {
		return storage.CompactRecord{
			Operation: resp.Operation,
			Attrs:     resp.Attrs,
		}, nil
	}
	attrs := make([]storage.AttrEntry, 0, len(resp.Props))
	for k, v := range resp.Props {
		attrs = append(attrs, storage.NewFloat32Attr(k, v))
	}
	return storage.CompactRecord{
		Operation: resp.Operation,
		Attrs:     attrs,
	}, nil
}

func (b *remoteBackend) SaveBatch(ctx context.Context, model string, records []storage.BatchRecordItem) ([]string, error) {
	req := map[string]interface{}{
		"model":   model,
		"records": records,
	}

	var resp struct {
		Success bool     `json:"success"`
		Count   int      `json:"count"`
		Names   []string `json:"names"`
		Message string   `json:"message"`
	}
	if err := b.doJSON(ctx, http.MethodPost, "/v1/save_batch", req, &resp); err != nil {
		return nil, err
	}
	return resp.Names, nil
}

func (b *remoteBackend) Get(ctx context.Context, model, name string, selectFields []string, computations map[string]storage.ComputeOp) (*EntityResult, error) {
	req := map[string]interface{}{
		"model":  model,
		"name":   name,
		"select": selectFields,
	}
	if len(computations) > 0 {
		compMap := make(map[string]string, len(computations))
		for k, v := range computations {
			compMap[k] = serializeComputeOp(v)
		}
		req["computations"] = compMap
	}

	var resp struct {
		Success   bool                `json:"success"`
		Name      string              `json:"name"`
		Operation uint8               `json:"operation"`
		Props     map[string]float32  `json:"props"`
		Attrs     []storage.AttrEntry `json:"attrs"`
	}
	if err := b.doJSON(ctx, http.MethodPost, "/v1/get", req, &resp); err != nil {
		return nil, err
	}
	return &EntityResult{
		Name:  resp.Name,
		props: resp.Props,
		attrs: resp.Attrs,
	}, nil
}

func (b *remoteBackend) List(ctx context.Context, model string, opts storage.QueryOptions) (*PagedListResult, error) {
	req := map[string]interface{}{
		"model":                  model,
		"select":                 opts.Select,
		"limit":                  opts.Limit,
		"offset":                 opts.Offset,
		"include_all_operations": opts.IncludeAllOperations,
	}
	if opts.OperationFilter != nil {
		req["operation"] = *opts.OperationFilter
	}
	if len(opts.AllowedOperations) > 0 {
		req["allowed_operations"] = opts.AllowedOperations
	}
	if len(opts.Filters) > 0 {
		req["filters"] = serializeFilters(opts.Filters)
	}
	if len(opts.Computations) > 0 {
		compMap := make(map[string]string, len(opts.Computations))
		for k, v := range opts.Computations {
			compMap[k] = serializeComputeOp(v)
		}
		req["computations"] = compMap
	}

	var resp struct {
		Success bool `json:"success"`
		Items   []struct {
			Name      string              `json:"name"`
			Operation uint8               `json:"operation"`
			Props     map[string]float32  `json:"props"`
			Attrs     []storage.AttrEntry `json:"attrs"`
		} `json:"items"`
		HasMore bool `json:"has_more"`
		Count   int  `json:"count"`
	}
	if err := b.doJSON(ctx, http.MethodPost, "/v1/list", req, &resp); err != nil {
		return nil, err
	}

	items := make([]EntityResult, len(resp.Items))
	for i, r := range resp.Items {
		items[i] = EntityResult{
			Name:  r.Name,
			props: r.Props,
			attrs: r.Attrs,
		}
	}
	return &PagedListResult{
		Items:   items,
		HasMore: resp.HasMore,
	}, nil
}

func (b *remoteBackend) Update(ctx context.Context, model string, opts storage.UpdateOptions) (int, error) {
	req := map[string]interface{}{
		"model":  model,
		"in":     opts.TargetEntities,
		"limit":  opts.Limit,
		"offset": opts.Offset,
	}
	if len(opts.Filters) > 0 {
		req["filters"] = serializeFilters(opts.Filters)
	}
	if len(opts.Mutations) > 0 {
		mutMap := make(map[string]string, len(opts.Mutations))
		for k, v := range opts.Mutations {
			mutMap[k] = serializeComputeOp(v)
		}
		req["mutations"] = mutMap
	}

	var resp struct {
		Success      bool `json:"success"`
		UpdatedCount int  `json:"updated_count"`
	}
	if err := b.doJSON(ctx, http.MethodPost, "/v1/update", req, &resp); err != nil {
		return 0, err
	}
	return resp.UpdatedCount, nil
}

func (b *remoteBackend) Graph(ctx context.Context, model string, inputs []string, relation string, limitK int, orderMode storage.OrderByMode, beamWidth float32) (*GraphResponse, error) {
	req := map[string]interface{}{
		"model":      model,
		"inputs":     inputs,
		"relation":   relation,
		"limit_k":    limitK,
		"order_by":   orderMode,
		"beam_width": beamWidth,
	}
	var resp struct {
		Success bool                       `json:"success"`
		Anchor  spatial.Point2D            `json:"anchor"`
		Results []storage.GraphQueryResult `json:"results"`
	}
	if err := b.doJSON(ctx, http.MethodPost, "/v1/graph", req, &resp); err != nil {
		return nil, err
	}
	return &GraphResponse{Anchor: resp.Anchor, Results: resp.Results}, nil
}

func (b *remoteBackend) GraphChain(ctx context.Context, model string, inputs []string, relations []string, limitK int, beamWidth float32) (*ChainResponse, error) {
	req := map[string]interface{}{
		"model":      model,
		"inputs":     inputs,
		"relations":  relations,
		"beam_width": beamWidth,
		"limit_k":    limitK,
	}
	var resp struct {
		Success  bool                       `json:"success"`
		Sequence []string                   `json:"sequence"`
		Steps    []storage.GraphQueryResult `json:"steps"`
	}
	if err := b.doJSON(ctx, http.MethodPost, "/v1/graph", req, &resp); err != nil {
		return nil, err
	}
	return &ChainResponse{Sequence: resp.Sequence, Steps: resp.Steps}, nil
}

func (b *remoteBackend) Edge(ctx context.Context, model, action, source, relation, target string, weight float32) error {
	req := map[string]interface{}{
		"model":    model,
		"action":   action,
		"source":   source,
		"relation": relation,
		"target":   target,
		"weight":   weight,
	}
	var resp struct {
		Success   bool `json:"success"`
		EdgeCount int  `json:"edge_count"`
	}
	return b.doJSON(ctx, http.MethodPost, "/v1/edge", req, &resp)
}

func (b *remoteBackend) Causal(ctx context.Context, model string, seeds []string, relation string, relations []string, limitK int) ([]storage.CausalResult, error) {
	req := map[string]interface{}{
		"model":     model,
		"seeds":     seeds,
		"relation":  relation,
		"relations": relations,
		"limit_k":   limitK,
	}
	var resp struct {
		Success bool                   `json:"success"`
		Results []storage.CausalResult `json:"results"`
	}
	if err := b.doJSON(ctx, http.MethodPost, "/v1/causal", req, &resp); err != nil {
		return nil, err
	}
	return resp.Results, nil
}

func (b *remoteBackend) Distance(ctx context.Context, model, wordA, wordB string) (float32, error) {
	req := map[string]interface{}{
		"model": model,
		"op":    "distance",
		"words": []string{wordA, wordB},
	}
	var resp struct {
		Success  bool    `json:"success"`
		Distance float32 `json:"distance"`
	}
	if err := b.doJSON(ctx, http.MethodPost, "/v1/math", req, &resp); err != nil {
		return 0, err
	}
	return resp.Distance, nil
}

func (b *remoteBackend) Centroid(ctx context.Context, model string, words []string) (CentroidResult, error) {
	req := map[string]interface{}{
		"model": model,
		"op":    "centroid",
		"words": words,
	}
	var resp struct {
		Success bool            `json:"success"`
		Coord   spatial.Point2D `json:"coord"`
		Roles   [4]float32      `json:"roles"`
	}
	if err := b.doJSON(ctx, http.MethodPost, "/v1/math", req, &resp); err != nil {
		return CentroidResult{}, err
	}
	return CentroidResult{Coord: resp.Coord, Roles: resp.Roles}, nil
}

func (b *remoteBackend) CosineSimilarity(ctx context.Context, model, wordA, wordB string) (float32, error) {
	req := map[string]interface{}{
		"model": model,
		"op":    "cosine_similarity",
		"words": []string{wordA, wordB},
	}
	var resp struct {
		Success    bool    `json:"success"`
		Similarity float32 `json:"similarity"`
	}
	if err := b.doJSON(ctx, http.MethodPost, "/v1/math", req, &resp); err != nil {
		return 0, err
	}
	return resp.Similarity, nil
}

func (b *remoteBackend) Nearest(ctx context.Context, model, word string, limitK int) ([]storage.GraphQueryResult, error) {
	req := map[string]interface{}{
		"model":   model,
		"op":      "nearest",
		"words":   []string{word},
		"limit_k": limitK,
	}
	var resp struct {
		Success   bool                       `json:"success"`
		Neighbors []storage.GraphQueryResult `json:"neighbors"`
	}
	if err := b.doJSON(ctx, http.MethodPost, "/v1/math", req, &resp); err != nil {
		return nil, err
	}
	return resp.Neighbors, nil
}

func (b *remoteBackend) AppendNode(ctx context.Context, model, word string, coord spatial.Point2D, roles [4]float32) error {
	req := map[string]interface{}{
		"model": model,
		"name":  word,
		"coord": coord,
		"roles": roles,
	}
	var resp struct {
		Success bool `json:"success"`
	}
	return b.doJSON(ctx, http.MethodPost, "/v1/save", req, &resp)
}

func (b *remoteBackend) SetRelation(ctx context.Context, model, name string, direction spatial.Point2D) error {
	req := map[string]interface{}{
		"model":     model,
		"action":    "set_relation",
		"relation":  name,
		"direction": direction,
	}
	var resp struct {
		Success bool `json:"success"`
	}
	return b.doJSON(ctx, http.MethodPost, "/v1/edge", req, &resp)
}

func (b *remoteBackend) Sync(ctx context.Context, model string) error {
	endpoint := "/v1/sync"
	if model != "" {
		endpoint += "?model=" + url.QueryEscape(model)
	}
	var resp struct {
		Success bool   `json:"success"`
		Message string `json:"message"`
	}
	return b.doJSON(ctx, http.MethodPost, endpoint, nil, &resp)
}

func (b *remoteBackend) Close() error {
	if b.httpClient != nil {
		b.httpClient.CloseIdleConnections()
	}
	return nil
}

func (b *remoteBackend) DB() *storage.AnchorDB {
	return nil
}

func (b *remoteBackend) IsRemote() bool {
	return true
}

func (b *remoteBackend) ListPartitions(ctx context.Context, _ string) ([]PartitionMeta, error) {
	var resp struct {
		Success    bool            `json:"success"`
		Partitions []PartitionMeta `json:"partitions"`
	}
	if err := b.doJSON(ctx, http.MethodGet, "/v1/partition/list", nil, &resp); err != nil {
		return nil, err
	}
	return resp.Partitions, nil
}

func (b *remoteBackend) InspectPartition(ctx context.Context, _, partitionFile, recordName string, limit int) (*PartitionInspectResult, error) {
	req := map[string]interface{}{
		"partition_file": partitionFile,
	}
	if recordName != "" {
		req["name"] = recordName
	}
	if limit > 0 {
		req["limit"] = limit
	}

	var resp struct {
		Success       bool                `json:"success"`
		PartitionFile string              `json:"partition_file"`
		Name          string              `json:"name,omitempty"`
		Props         map[string]float32  `json:"props,omitempty"`
		Attrs         []storage.AttrEntry `json:"attrs,omitempty"`
		Records       []PartitionRecord   `json:"records,omitempty"`
		Count         int                 `json:"count,omitempty"`
		HasMore       bool                `json:"has_more,omitempty"`
	}
	if err := b.doJSON(ctx, http.MethodPost, "/v1/partition/inspect", req, &resp); err != nil {
		return nil, err
	}
	return &PartitionInspectResult{
		Success:       resp.Success,
		PartitionFile: resp.PartitionFile,
		Name:          resp.Name,
		Props:         resp.Props,
		Attrs:         resp.Attrs,
		Records:       resp.Records,
		Count:         resp.Count,
		HasMore:       resp.HasMore,
	}, nil
}

func (b *remoteBackend) Attach(ctx context.Context, model string, schema storage.ModelSchema) error {
	req := map[string]interface{}{
		"model":  model,
		"schema": schema,
	}
	var resp struct {
		Success bool   `json:"success"`
		Model   string `json:"model"`
		Status  string `json:"status"`
	}
	err := b.doJSON(ctx, http.MethodPost, "/v1/model/attach", req, &resp)
	if err != nil {
		if strings.Contains(err.Error(), "schema_mismatch") || strings.Contains(err.Error(), "schema mismatch") {
			return fmt.Errorf("%w: %v", storage.ErrSchemaMismatch, err)
		}
		return err
	}
	return nil
}

func (b *remoteBackend) GetSchema(ctx context.Context, model string) (storage.ModelSchema, error) {
	endpoint := "/v1/model/schema"
	if model != "" {
		endpoint += "?model=" + url.QueryEscape(model)
	}
	var resp struct {
		Success bool                `json:"success"`
		Model   string              `json:"model"`
		Locked  bool                `json:"locked"`
		Schema  storage.ModelSchema `json:"schema"`
	}
	if err := b.doJSON(ctx, http.MethodGet, endpoint, nil, &resp); err != nil {
		return nil, err
	}
	return resp.Schema, nil
}

func (b *remoteBackend) ResetModel(ctx context.Context, model string, confirm bool) error {
	req := map[string]interface{}{
		"model":   model,
		"confirm": confirm,
	}
	var resp struct {
		Success bool   `json:"success"`
		Model   string `json:"model"`
		Message string `json:"message"`
	}
	return b.doJSON(ctx, http.MethodPost, "/v1/model/reset", req, &resp)
}

// -----------------------------------------------------------------------------
// Serialization Helpers
// -----------------------------------------------------------------------------

func serializeComputeOp(op storage.ComputeOp) string {
	switch o := op.(type) {
	case storage.OpDivide:
		return fmt.Sprintf("%s / %s", o.FieldA, o.FieldB)
	case storage.OpMultiply:
		return fmt.Sprintf("%s * %s", o.FieldA, o.FieldB)
	case storage.OpAdd:
		return fmt.Sprintf("%s + %s", o.FieldA, o.FieldB)
	case storage.OpSubtract:
		return fmt.Sprintf("%s - %s", o.FieldA, o.FieldB)
	case storage.OpAssign:
		return o.Value
	case storage.OpAddDays:
		return fmt.Sprintf("%s + %s", o.Field, strconv.FormatFloat(float64(o.Days), 'g', -1, 32))
	case storage.OpSubtractDays:
		return fmt.Sprintf("%s - %s", o.Field, strconv.FormatFloat(float64(o.Days), 'g', -1, 32))
	case storage.OpDaysBetween:
		return fmt.Sprintf("%s - %s", o.FieldA, o.FieldB)
	default:
		return fmt.Sprintf("%v", op)
	}
}

func serializeFilters(filters []storage.FilterCondition) []map[string]string {
	out := make([]map[string]string, len(filters))
	for i, f := range filters {
		valStr := strconv.FormatFloat(float64(f.Value), 'g', -1, 32)
		if f.ValType == storage.AttrTypeInt64 {
			valStr = strconv.FormatInt(f.ValueInt64, 10)
		} else if f.ValType == storage.AttrTypeUint64 {
			valStr = strconv.FormatUint(f.ValueUint64, 10)
		} else if f.HasFloat64 {
			valStr = strconv.FormatFloat(f.Value64, 'g', -1, 64)
		}
		m := map[string]string{
			"field":    f.Field,
			"operator": f.Operator,
			"value":    valStr,
		}
		if f.Operator == "BETWEEN" {
			highType := f.ValType
			if f.HasHighType {
				highType = f.ValHighType
			}
			if highType == storage.AttrTypeInt64 {
				m["value_high"] = strconv.FormatInt(f.ValueHighInt64, 10)
			} else if highType == storage.AttrTypeUint64 {
				m["value_high"] = strconv.FormatUint(f.ValueHighUint64, 10)
			} else if f.HasFloat64 {
				m["value_high"] = strconv.FormatFloat(f.ValueHigh64, 'g', -1, 64)
			} else {
				m["value_high"] = strconv.FormatFloat(float64(f.ValueHigh), 'g', -1, 32)
			}
		}
		out[i] = m
	}
	return out
}

// -----------------------------------------------------------------------------
// Wire Backend (AnchorWire Binary TCP Client Pool)
// -----------------------------------------------------------------------------

type wireBackend struct {
	addr      string
	cfg       clientConfig
	opts      []wire.ClientOption
	poolSize  int
	sem       chan struct{}
	idle      chan *wire.Client
	clients   map[*wire.Client]struct{}
	mu        sync.Mutex
	closed    bool
	closeChan chan struct{}
}

func newWireBackend(addr string, cfg clientConfig) (*wireBackend, error) {
	poolSize := cfg.poolSize
	if poolSize <= 0 {
		poolSize = runtime.NumCPU() * 2
		if poolSize < 4 {
			poolSize = 4
		}
		if poolSize > 32 {
			poolSize = 32
		}
	}

	opts := []wire.ClientOption{
		wire.WithClientModel(cfg.model),
		wire.WithClientTimeout(cfg.timeout),
	}
	if cfg.token != "" {
		opts = append(opts, wire.WithClientToken(cfg.token))
	}
	if cfg.adaptivePacing {
		opts = append(opts, wire.WithClientAdaptivePacing(cfg.targetRecords))
	}

	sem := make(chan struct{}, poolSize)
	idle := make(chan *wire.Client, poolSize)
	clients := make(map[*wire.Client]struct{}, poolSize)

	for i := 0; i < poolSize; i++ {
		wc, err := wire.Connect(addr, opts...)
		if err != nil {
			for c := range clients {
				_ = c.Close()
			}
			return nil, fmt.Errorf("failed to initialize wire connection %d/%d: %w", i+1, poolSize, err)
		}
		clients[wc] = struct{}{}
		idle <- wc
	}

	return &wireBackend{
		addr:      addr,
		cfg:       cfg,
		opts:      opts,
		poolSize:  poolSize,
		sem:       sem,
		idle:      idle,
		clients:   clients,
		closeChan: make(chan struct{}),
	}, nil
}

func (b *wireBackend) dial() (*wire.Client, error) {
	wc, err := wire.Connect(b.addr, b.opts...)
	if err != nil {
		return nil, err
	}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		_ = wc.Close()
		return nil, errors.New("wire client closed")
	}
	b.clients[wc] = struct{}{}
	b.mu.Unlock()
	return wc, nil
}

func (b *wireBackend) acquire(ctx context.Context) (*wire.Client, error) {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil, errors.New("wire client closed")
	}
	b.mu.Unlock()

	select {
	case b.sem <- struct{}{}:
	case <-b.closeChan:
		return nil, errors.New("wire client closed")
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	select {
	case wc := <-b.idle:
		b.mu.Lock()
		closed := b.closed
		b.mu.Unlock()
		if closed {
			_ = wc.Close()
			<-b.sem
			return nil, errors.New("wire client closed")
		}
		return wc, nil
	case <-b.closeChan:
		<-b.sem
		return nil, errors.New("wire client closed")
	default:
		wc, err := b.dial()
		if err != nil {
			<-b.sem
			return nil, err
		}
		return wc, nil
	}
}

func (b *wireBackend) release(wc *wire.Client, healthy bool) {
	defer func() {
		<-b.sem
	}()
	if wc == nil {
		return
	}

	b.mu.Lock()
	closed := b.closed
	if !healthy || closed {
		delete(b.clients, wc)
	}
	b.mu.Unlock()

	if !healthy || closed {
		_ = wc.Close()
		return
	}

	select {
	case b.idle <- wc:
	default:
		b.mu.Lock()
		delete(b.clients, wc)
		b.mu.Unlock()
		_ = wc.Close()
	}
}

func isHealthyWireResult(err error) bool {
	if err == nil {
		return true
	}
	var wireErr *wire.WireError
	return errors.As(err, &wireErr)
}

func (b *wireBackend) withClient(ctx context.Context, model string, fn func(wc *wire.Client) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	wc, err := b.acquire(ctx)
	if err != nil {
		return err
	}

	healthy := false
	defer func() {
		b.release(wc, healthy)
	}()

	stopCancel := context.AfterFunc(ctx, func() {
		_ = wc.Close()
	})
	defer stopCancel()

	if model != "" && model != wc.ModelName() {
		if err := wc.SelectModel(model); err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			healthy = isHealthyWireResult(err)
			return err
		}
	}
	err = fn(wc)
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	healthy = isHealthyWireResult(err)
	return err
}

func (b *wireBackend) Save(ctx context.Context, model, name string, op *uint8, props map[string]float32) (string, error) {
	opStatus := storage.OpStatusActive
	if op != nil {
		opStatus = *op
	}
	var res string
	err := b.withClient(ctx, model, func(wc *wire.Client) error {
		var err error
		res, err = wc.Save(storage.BatchRecordItem{
			Name:      name,
			Operation: opStatus,
			Props:     props,
		})
		return err
	})
	return res, err
}

func (b *wireBackend) SaveTyped(ctx context.Context, model, name string, op *uint8, attrs []storage.AttrEntry) (string, error) {
	opStatus := storage.OpStatusActive
	if op != nil {
		opStatus = *op
	}
	var res string
	err := b.withClient(ctx, model, func(wc *wire.Client) error {
		var err error
		res, err = wc.SaveTyped(name, opStatus, attrs)
		return err
	})
	return res, err
}

func (b *wireBackend) GetTyped(ctx context.Context, model, name string) (storage.CompactRecord, error) {
	var cr storage.CompactRecord
	err := b.withClient(ctx, model, func(wc *wire.Client) error {
		var err error
		cr, err = wc.GetTyped(name)
		return err
	})
	return cr, err
}

func (b *wireBackend) SaveBatch(ctx context.Context, model string, records []storage.BatchRecordItem) ([]string, error) {
	var names []string
	err := b.withClient(ctx, model, func(wc *wire.Client) error {
		var err error
		names, err = wc.SaveBatch(records)
		return err
	})
	return names, err
}

func (b *wireBackend) Get(ctx context.Context, model, name string, selectFields []string, computations map[string]storage.ComputeOp) (*EntityResult, error) {
	var cr storage.CompactRecord
	err := b.withClient(ctx, model, func(wc *wire.Client) error {
		var err error
		cr, err = wc.GetWithOptionsTyped(wire.GetRequest{
			Name:         name,
			Select:       selectFields,
			Computations: computations,
		})
		return err
	})
	if err != nil {
		return nil, err
	}
	return &EntityResult{
		Name:  name,
		props: cr.ToMap(),
		attrs: cr.ExpandAttrs(),
	}, nil
}

func (b *wireBackend) List(ctx context.Context, model string, opts storage.QueryOptions) (*PagedListResult, error) {
	req := wire.ListRequest{
		Limit:                opts.Limit,
		Offset:               opts.Offset,
		IncludeAllOperations: opts.IncludeAllOperations,
		OperationFilter:      opts.OperationFilter,
		AllowedOperations:    opts.AllowedOperations,
		Select:               opts.Select,
		Filters:              opts.Filters,
		Computations:         opts.Computations,
	}
	var resp wire.ListResponse
	err := b.withClient(ctx, model, func(wc *wire.Client) error {
		var err error
		resp, err = wc.List(req)
		return err
	})
	if err != nil {
		return nil, err
	}
	items := make([]EntityResult, len(resp.Items))
	for i, r := range resp.Items {
		items[i] = EntityResult{
			Name:  r.Name,
			props: r.Props,
			attrs: r.Attrs,
		}
	}
	return &PagedListResult{
		Items:   items,
		HasMore: resp.HasMore,
	}, nil
}

func (b *wireBackend) Update(ctx context.Context, model string, opts storage.UpdateOptions) (int, error) {
	req := wire.UpdateRequest{
		Limit:          opts.Limit,
		Offset:         opts.Offset,
		TargetEntities: opts.TargetEntities,
		Filters:        opts.Filters,
		Mutations:      opts.Mutations,
	}
	var count int
	err := b.withClient(ctx, model, func(wc *wire.Client) error {
		var err error
		count, err = wc.Update(req)
		return err
	})
	return count, err
}

func (b *wireBackend) Graph(ctx context.Context, model string, inputs []string, relation string, limitK int, orderMode storage.OrderByMode, beamWidth float32) (*GraphResponse, error) {
	req := wire.GraphRequest{
		Inputs:    inputs,
		Relation:  relation,
		LimitK:    limitK,
		OrderMode: uint8(orderMode),
		BeamWidth: beamWidth,
	}
	var resp wire.GraphResponsePayload
	err := b.withClient(ctx, model, func(wc *wire.Client) error {
		var err error
		resp, err = wc.Graph(req)
		return err
	})
	if err != nil {
		return nil, err
	}
	return &GraphResponse{Anchor: resp.Anchor, Results: resp.Results}, nil
}

func (b *wireBackend) GraphChain(ctx context.Context, model string, inputs []string, relations []string, limitK int, beamWidth float32) (*ChainResponse, error) {
	req := wire.GraphChainRequest{
		Inputs:        inputs,
		RelationChain: relations,
		LimitK:        limitK,
		BeamWidth:     beamWidth,
	}
	var results []storage.GraphQueryResult
	err := b.withClient(ctx, model, func(wc *wire.Client) error {
		var err error
		results, err = wc.GraphChain(req)
		return err
	})
	if err != nil {
		return nil, err
	}
	seq := make([]string, len(results))
	for i, step := range results {
		seq[i] = step.Word
	}
	return &ChainResponse{
		Sequence: seq,
		Steps:    results,
	}, nil
}

func (b *wireBackend) Edge(ctx context.Context, model, action, source, relation, target string, weight float32) error {
	return b.withClient(ctx, model, func(wc *wire.Client) error {
		return wc.Edge(action, source, relation, target, weight)
	})
}

func (b *wireBackend) Causal(ctx context.Context, model string, seeds []string, relation string, relations []string, limitK int) ([]storage.CausalResult, error) {
	var finalResults []storage.CausalResult
	err := b.withClient(ctx, model, func(wc *wire.Client) error {
		var err error
		if len(relations) > 0 {
			finalResults, err = wc.CausalChain(seeds, relations, limitK)
			return err
		}
		finalResults, err = wc.Causal(seeds, relation, limitK)
		return err
	})
	return finalResults, err
}

func (b *wireBackend) Distance(ctx context.Context, model, wordA, wordB string) (float32, error) {
	var val float32
	err := b.withClient(ctx, model, func(wc *wire.Client) error {
		resp, err := wc.Math(wire.MathRequest{
			Op:    "distance",
			Words: []string{wordA, wordB},
		})
		if err != nil {
			return err
		}
		val = resp.Value
		return nil
	})
	return val, err
}

func (b *wireBackend) Centroid(ctx context.Context, model string, words []string) (CentroidResult, error) {
	var res CentroidResult
	err := b.withClient(ctx, model, func(wc *wire.Client) error {
		resp, err := wc.Math(wire.MathRequest{
			Op:    "centroid",
			Words: words,
		})
		if err != nil {
			return err
		}
		res = CentroidResult{Coord: resp.Coord, Roles: resp.Roles}
		return nil
	})
	return res, err
}

func (b *wireBackend) CosineSimilarity(ctx context.Context, model, wordA, wordB string) (float32, error) {
	var val float32
	err := b.withClient(ctx, model, func(wc *wire.Client) error {
		resp, err := wc.Math(wire.MathRequest{
			Op:    "cosine_similarity",
			Words: []string{wordA, wordB},
		})
		if err != nil {
			return err
		}
		val = resp.Value
		return nil
	})
	return val, err
}

func (b *wireBackend) Nearest(ctx context.Context, model, word string, limitK int) ([]storage.GraphQueryResult, error) {
	var res []storage.GraphQueryResult
	err := b.withClient(ctx, model, func(wc *wire.Client) error {
		resp, err := wc.Math(wire.MathRequest{
			Op:     "nearest",
			Words:  []string{word},
			LimitK: limitK,
		})
		if err != nil {
			return err
		}
		res = resp.Neighbors
		return nil
	})
	return res, err
}

func (b *wireBackend) AppendNode(ctx context.Context, model, word string, coord spatial.Point2D, roles [4]float32) error {
	return b.withClient(ctx, model, func(wc *wire.Client) error {
		_, err := wc.Save(storage.BatchRecordItem{
			Name:      word,
			Operation: storage.OpStatusActive,
			Coord:     &coord,
			Roles:     &roles,
		})
		return err
	})
}

func (b *wireBackend) SetRelation(ctx context.Context, model, name string, direction spatial.Point2D) error {
	return b.withClient(ctx, model, func(wc *wire.Client) error {
		return wc.SetRelation(name, direction)
	})
}

func (b *wireBackend) Sync(ctx context.Context, model string) error {
	return b.withClient(ctx, model, func(wc *wire.Client) error {
		return wc.Sync()
	})
}

func (b *wireBackend) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	close(b.closeChan)
	for {
		select {
		case wc := <-b.idle:
			if wc != nil {
				delete(b.clients, wc)
				_ = wc.Close()
			}
		default:
			goto drained
		}
	}
drained:
	toClose := make([]*wire.Client, 0, len(b.clients))
	for wc := range b.clients {
		toClose = append(toClose, wc)
	}
	clear(b.clients)
	b.mu.Unlock()

	var lastErr error
	for _, wc := range toClose {
		if err := wc.Close(); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

func (b *wireBackend) DB() *storage.AnchorDB {
	return nil
}

func (b *wireBackend) IsRemote() bool {
	return true
}

func (b *wireBackend) defaultModel() string {
	if b.cfg.model != "" {
		return b.cfg.model
	}
	return "default"
}

func (b *wireBackend) ListPartitions(ctx context.Context, model string) ([]PartitionMeta, error) {
	var out []PartitionMeta
	if model == "" {
		model = b.defaultModel()
	}
	err := b.withClient(ctx, model, func(wc *wire.Client) error {
		items, err := wc.PartitionCatalog(model)
		if err != nil {
			return err
		}
		out = make([]PartitionMeta, len(items))
		for i, item := range items {
			out[i] = PartitionMeta{
				Name:      item.PartitionFile,
				SizeBytes: item.SizeBytes,
				ModTime:   item.CreatedAt,
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (b *wireBackend) InspectPartition(ctx context.Context, model, partitionFile, recordName string, limit int) (*PartitionInspectResult, error) {
	var result *PartitionInspectResult
	if model == "" {
		model = b.defaultModel()
	}
	err := b.withClient(ctx, model, func(wc *wire.Client) error {
		resp, err := wc.PartitionInspect(wire.PartitionInspectWireRequest{
			PartitionFile: partitionFile,
			RecordName:    recordName,
			Limit:         limit,
		})
		if err != nil {
			return err
		}
		records := make([]PartitionRecord, len(resp.Records))
		for i, r := range resp.Records {
			records[i] = PartitionRecord{
				Name:  r.Name,
				Props: r.Props,
				Attrs: r.Attrs,
			}
		}
		result = &PartitionInspectResult{
			Success:       true,
			PartitionFile: resp.PartitionFile,
			Name:          resp.RecordName,
			Props:         resp.Props,
			Attrs:         resp.Attrs,
			Records:       records,
			Count:         len(records),
			HasMore:       resp.HasMore,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (b *wireBackend) Attach(ctx context.Context, model string, schema storage.ModelSchema) error {
	data, err := json.Marshal(schema)
	if err != nil {
		return err
	}
	return b.withClient(ctx, model, func(wc *wire.Client) error {
		return wc.AttachSchema(string(data))
	})
}

func (b *wireBackend) GetSchema(ctx context.Context, model string) (storage.ModelSchema, error) {
	var ms storage.ModelSchema
	err := b.withClient(ctx, model, func(wc *wire.Client) error {
		raw, err := wc.GetSchema()
		if err != nil {
			return err
		}
		return json.Unmarshal([]byte(raw), &ms)
	})
	return ms, err
}

func (b *wireBackend) ResetModel(ctx context.Context, model string, confirm bool) error {
	return b.withClient(ctx, model, func(wc *wire.Client) error {
		return wc.ResetModel(confirm)
	})
}
