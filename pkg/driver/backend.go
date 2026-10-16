package driver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/rachmanzz/anchordb/pkg/scheduler"
	"github.com/rachmanzz/anchordb/pkg/spatial"
	"github.com/rachmanzz/anchordb/pkg/storage"
	"github.com/rachmanzz/anchordb/pkg/worker"
)

// PartitionMeta represents metadata for an archived partition file.
type PartitionMeta struct {
	Name      string    `json:"name"`
	SizeBytes int64     `json:"size_bytes"`
	ModTime   time.Time `json:"mod_time"`
}

// PartitionRecord represents an entity record inside an archived partition.
type PartitionRecord struct {
	Name  string             `json:"name"`
	Props map[string]float32 `json:"props"`
}

// PartitionInspectResult represents the response when inspecting an archived partition file.
type PartitionInspectResult struct {
	Success       bool               `json:"success"`
	PartitionFile string             `json:"partition_file"`
	Name          string             `json:"name,omitempty"`
	Props         map[string]float32 `json:"props,omitempty"`
	Records       []PartitionRecord  `json:"records,omitempty"`
	Count         int                `json:"count,omitempty"`
}

// backend defines the internal execution contract shared by local, embedded, and remote engine drivers.
type backend interface {
	Save(ctx context.Context, model, name string, op *uint8, props map[string]float32) (string, error)
	Get(ctx context.Context, model, name string, selectFields []string, computations map[string]storage.ComputeOp) (*EntityResult, error)
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
	ListPartitions(ctx context.Context) ([]PartitionMeta, error)
	InspectPartition(ctx context.Context, partitionFile, recordName string, limit int) (*PartitionInspectResult, error)
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

func (b *localBackend) Save(ctx context.Context, model, name string, op *uint8, props map[string]float32) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	opStatus := storage.OpStatusActive
	if op != nil {
		opStatus = *op
	}
	return b.db.SaveRecordWithIDAndOp(name, opStatus, props)
}

func (b *localBackend) Get(ctx context.Context, model, name string, selectFields []string, computations map[string]storage.ComputeOp) (*EntityResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var props map[string]float32
	var err error
	if len(computations) == 0 {
		props, err = b.db.GetRecord(name, selectFields...)
	} else {
		props, err = b.db.GetRecordWithCompute(name, computations, selectFields...)
	}
	if err != nil {
		return nil, err
	}
	return &EntityResult{Name: name, props: props}, nil
}

func (b *localBackend) List(ctx context.Context, model string, opts storage.QueryOptions) (*PagedListResult, error) {
	if err := ctx.Err(); err != nil {
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
	opts.Context = ctx
	return b.db.UpdateRecords(opts)
}

func (b *localBackend) Graph(ctx context.Context, model string, inputs []string, relation string, limitK int, orderMode storage.OrderByMode, beamWidth float32) (*GraphResponse, error) {
	if err := ctx.Err(); err != nil {
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
	if len(relations) > 0 {
		return b.db.ExecuteCausalChain(ctx, seeds, relations, limitK)
	}
	return b.db.ExecuteCausalStep(ctx, seeds, relation, limitK)
}

func (b *localBackend) Distance(ctx context.Context, model, wordA, wordB string) (float32, error) {
	return b.db.CalculateDistance(wordA, wordB)
}

func (b *localBackend) Centroid(ctx context.Context, model string, words []string) (CentroidResult, error) {
	coord, roles, err := b.db.CalculateCentroid(words)
	return CentroidResult{Coord: coord, Roles: roles}, err
}

func (b *localBackend) CosineSimilarity(ctx context.Context, model, wordA, wordB string) (float32, error) {
	return b.db.CalculateCosineSimilarity(wordA, wordB)
}

func (b *localBackend) Nearest(ctx context.Context, model, word string, limitK int) ([]storage.GraphQueryResult, error) {
	return b.db.FindNearestNeighbors(word, limitK)
}

func (b *localBackend) AppendNode(ctx context.Context, model, word string, coord spatial.Point2D, roles [4]float32) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_, err := b.db.AppendNode(word, coord, roles, 1)
	return err
}

func (b *localBackend) SetRelation(ctx context.Context, model, name string, direction spatial.Point2D) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return b.db.SetRelation(name, direction)
}

func (b *localBackend) Sync(ctx context.Context, model string) error {
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

func (b *localBackend) ListPartitions(ctx context.Context) ([]PartitionMeta, error) {
	partDir := filepath.Join(filepath.Dir(b.filePath), "partitions")
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
			continue
		}
		out = append(out, PartitionMeta{
			Name:      e.Name(),
			SizeBytes: info.Size(),
			ModTime:   info.ModTime(),
		})
	}
	return out, nil
}

func (b *localBackend) InspectPartition(ctx context.Context, partitionFile, recordName string, limit int) (*PartitionInspectResult, error) {
	cleanFilename := filepath.Base(partitionFile)
	fullPath := filepath.Join(filepath.Dir(b.filePath), "partitions", cleanFilename)
	if _, err := os.Stat(fullPath); err != nil {
		return nil, fmt.Errorf("partition file not found: %s", cleanFilename)
	}

	pdb, err := storage.Open(fullPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open partition: %w", err)
	}
	defer pdb.Close()

	if recordName != "" {
		props, err := pdb.GetRecord(recordName)
		if err != nil {
			return nil, fmt.Errorf("record %q not found in partition: %w", recordName, err)
		}
		return &PartitionInspectResult{
			Success:       true,
			PartitionFile: cleanFilename,
			Name:          recordName,
			Props:         props,
		}, nil
	}

	if limit <= 0 || limit > 100 {
		limit = 10
	}
	records, _, err := pdb.ListRecordsWithPaging(storage.QueryOptions{Limit: limit, Context: ctx})
	if err != nil {
		return nil, fmt.Errorf("failed to list records in partition: %w", err)
	}

	out := make([]PartitionRecord, len(records))
	for i, r := range records {
		out[i] = PartitionRecord{Name: r.Name, Props: r.Props}
	}

	return &PartitionInspectResult{
		Success:       true,
		PartitionFile: cleanFilename,
		Records:       out,
		Count:         len(out),
	}, nil
}

func (b *localBackend) Attach(ctx context.Context, model string, schema storage.ModelSchema) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return b.db.AttachSchema(schema)
}

func (b *localBackend) GetSchema(ctx context.Context, model string) (storage.ModelSchema, error) {
	if err := ctx.Err(); err != nil {
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
	if !confirm {
		return fmt.Errorf("confirm must be true to reset model schema")
	}
	return b.db.ResetSchema()
}

// -----------------------------------------------------------------------------
// Embedded Backend (In-Process Engine with WAL, Multi-Model, Scheduler & Workers)
// -----------------------------------------------------------------------------

type embeddedBackend struct {
	mu         sync.RWMutex
	dataDir    string
	cfg        clientConfig
	models     map[string]*storage.AnchorDB
	workerPool *worker.Pool
	scheduler  *scheduler.InternalScheduler
	closed     bool
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
		dataDir: dataDir,
		cfg:     cfg,
		models:  make(map[string]*storage.AnchorDB),
	}

	if cfg.workerConcurrency > 0 {
		pool := worker.NewPool(worker.Config{
			Concurrency: cfg.workerConcurrency,
		})
		pool.Start()
		b.workerPool = pool
	}

	if cfg.enableScheduler || cfg.syncInterval > 0 {
		sched := scheduler.NewInternalScheduler()
		if cfg.syncInterval > 0 {
			_ = sched.RegisterTask("embedded_periodic_sync", &embeddedSyncTask{backend: b, interval: cfg.syncInterval})
		}
		sched.Start()
		b.scheduler = sched
	}

	if cfg.model != "" {
		if _, err := b.getDB(cfg.model); err != nil {
			_ = b.Close()
			return nil, err
		}
	}

	return b, nil
}

func (b *embeddedBackend) getDB(model string) (*storage.AnchorDB, error) {
	if model == "" {
		model = "default"
	}
	cleanModel := filepath.Base(model)
	if cleanModel == "." || cleanModel == "/" || cleanModel == ".." {
		cleanModel = "default"
	}

	b.mu.RLock()
	db, ok := b.models[cleanModel]
	closed := b.closed
	b.mu.RUnlock()

	if closed {
		return nil, fmt.Errorf("embedded engine is closed")
	}
	if ok {
		return db, nil
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return nil, fmt.Errorf("embedded engine is closed")
	}
	if db, ok := b.models[cleanModel]; ok {
		return db, nil
	}

	filePath := filepath.Join(b.dataDir, cleanModel+".anchordb")
	newDB, err := storage.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open embedded model %q at %s: %w", cleanModel, filePath, err)
	}
	b.models[cleanModel] = newDB
	return newDB, nil
}

func (b *embeddedBackend) Save(ctx context.Context, model, name string, op *uint8, props map[string]float32) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	db, err := b.getDB(model)
	if err != nil {
		return "", err
	}
	opStatus := storage.OpStatusActive
	if op != nil {
		opStatus = *op
	}
	return db.SaveRecordWithIDAndOp(name, opStatus, props)
}

func (b *embeddedBackend) Get(ctx context.Context, model, name string, selectFields []string, computations map[string]storage.ComputeOp) (*EntityResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	db, err := b.getDB(model)
	if err != nil {
		return nil, err
	}
	var props map[string]float32
	if len(computations) == 0 {
		props, err = db.GetRecord(name, selectFields...)
	} else {
		props, err = db.GetRecordWithCompute(name, computations, selectFields...)
	}
	if err != nil {
		return nil, err
	}
	return &EntityResult{Name: name, props: props}, nil
}

func (b *embeddedBackend) List(ctx context.Context, model string, opts storage.QueryOptions) (*PagedListResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	db, err := b.getDB(model)
	if err != nil {
		return nil, err
	}
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
	db, err := b.getDB(model)
	if err != nil {
		return 0, err
	}
	opts.Context = ctx
	return db.UpdateRecords(opts)
}

func (b *embeddedBackend) Graph(ctx context.Context, model string, inputs []string, relation string, limitK int, orderMode storage.OrderByMode, beamWidth float32) (*GraphResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	db, err := b.getDB(model)
	if err != nil {
		return nil, err
	}
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
	db, err := b.getDB(model)
	if err != nil {
		return nil, err
	}
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
	db, err := b.getDB(model)
	if err != nil {
		return err
	}
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
	db, err := b.getDB(model)
	if err != nil {
		return nil, err
	}
	if len(relations) > 0 {
		return db.ExecuteCausalChain(ctx, seeds, relations, limitK)
	}
	return db.ExecuteCausalStep(ctx, seeds, relation, limitK)
}

func (b *embeddedBackend) Distance(ctx context.Context, model, wordA, wordB string) (float32, error) {
	db, err := b.getDB(model)
	if err != nil {
		return 0, err
	}
	return db.CalculateDistance(wordA, wordB)
}

func (b *embeddedBackend) Centroid(ctx context.Context, model string, words []string) (CentroidResult, error) {
	db, err := b.getDB(model)
	if err != nil {
		return CentroidResult{}, err
	}
	coord, roles, err := db.CalculateCentroid(words)
	return CentroidResult{Coord: coord, Roles: roles}, err
}

func (b *embeddedBackend) CosineSimilarity(ctx context.Context, model, wordA, wordB string) (float32, error) {
	db, err := b.getDB(model)
	if err != nil {
		return 0, err
	}
	return db.CalculateCosineSimilarity(wordA, wordB)
}

func (b *embeddedBackend) Nearest(ctx context.Context, model, word string, limitK int) ([]storage.GraphQueryResult, error) {
	db, err := b.getDB(model)
	if err != nil {
		return nil, err
	}
	return db.FindNearestNeighbors(word, limitK)
}

func (b *embeddedBackend) AppendNode(ctx context.Context, model, word string, coord spatial.Point2D, roles [4]float32) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	db, err := b.getDB(model)
	if err != nil {
		return err
	}
	_, err = db.AppendNode(word, coord, roles, 1)
	return err
}

func (b *embeddedBackend) SetRelation(ctx context.Context, model, name string, direction spatial.Point2D) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	db, err := b.getDB(model)
	if err != nil {
		return err
	}
	return db.SetRelation(name, direction)
}

func (b *embeddedBackend) Sync(ctx context.Context, model string) error {
	if model != "" && model != "all" {
		db, err := b.getDB(model)
		if err != nil {
			return err
		}
		return db.Sync()
	}
	b.mu.RLock()
	dbs := make([]*storage.AnchorDB, 0, len(b.models))
	for _, db := range b.models {
		dbs = append(dbs, db)
	}
	b.mu.RUnlock()

	var lastErr error
	for _, db := range dbs {
		if err := db.Sync(); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

func (b *embeddedBackend) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil
	}
	b.closed = true

	if b.scheduler != nil {
		b.scheduler.Stop()
	}
	if b.workerPool != nil {
		_ = b.workerPool.Shutdown()
	}

	var lastErr error
	for _, db := range b.models {
		if err := db.Close(); err != nil {
			lastErr = err
		}
	}
	b.models = make(map[string]*storage.AnchorDB)
	return lastErr
}

func (b *embeddedBackend) DB() *storage.AnchorDB {
	db, err := b.getDB(b.cfg.model)
	if err != nil {
		return nil
	}
	return db
}

func (b *embeddedBackend) IsRemote() bool {
	return false
}

func (b *embeddedBackend) ListPartitions(ctx context.Context) ([]PartitionMeta, error) {
	partDir := filepath.Join(b.dataDir, "partitions")
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
			continue
		}
		out = append(out, PartitionMeta{
			Name:      e.Name(),
			SizeBytes: info.Size(),
			ModTime:   info.ModTime(),
		})
	}
	return out, nil
}

func (b *embeddedBackend) InspectPartition(ctx context.Context, partitionFile, recordName string, limit int) (*PartitionInspectResult, error) {
	cleanFilename := filepath.Base(partitionFile)
	fullPath := filepath.Join(b.dataDir, "partitions", cleanFilename)
	if _, err := os.Stat(fullPath); err != nil {
		return nil, fmt.Errorf("partition file not found: %s", cleanFilename)
	}

	pdb, err := storage.Open(fullPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open partition: %w", err)
	}
	defer pdb.Close()

	if recordName != "" {
		props, err := pdb.GetRecord(recordName)
		if err != nil {
			return nil, fmt.Errorf("record %q not found in partition: %w", recordName, err)
		}
		return &PartitionInspectResult{
			Success:       true,
			PartitionFile: cleanFilename,
			Name:          recordName,
			Props:         props,
		}, nil
	}

	if limit <= 0 || limit > 100 {
		limit = 10
	}
	records, _, err := pdb.ListRecordsWithPaging(storage.QueryOptions{Limit: limit, Context: ctx})
	if err != nil {
		return nil, fmt.Errorf("failed to list records in partition: %w", err)
	}

	out := make([]PartitionRecord, len(records))
	for i, r := range records {
		out[i] = PartitionRecord{Name: r.Name, Props: r.Props}
	}

	return &PartitionInspectResult{
		Success:       true,
		PartitionFile: cleanFilename,
		Records:       out,
		Count:         len(out),
	}, nil
}

func (b *embeddedBackend) Attach(ctx context.Context, model string, schema storage.ModelSchema) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	db, err := b.getDB(model)
	if err != nil {
		return err
	}
	return db.AttachSchema(schema)
}

func (b *embeddedBackend) GetSchema(ctx context.Context, model string) (storage.ModelSchema, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	db, err := b.getDB(model)
	if err != nil {
		return nil, err
	}
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
	db, err := b.getDB(model)
	if err != nil {
		return err
	}
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
		Success   bool               `json:"success"`
		Name      string             `json:"name"`
		Operation uint8              `json:"operation"`
		Props     map[string]float32 `json:"props"`
	}
	if err := b.doJSON(ctx, http.MethodPost, "/v1/get", req, &resp); err != nil {
		return nil, err
	}
	return &EntityResult{Name: resp.Name, props: resp.Props}, nil
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
			Name      string             `json:"name"`
			Operation uint8              `json:"operation"`
			Props     map[string]float32 `json:"props"`
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
		}
	}
	return &PagedListResult{
		Items:   items,
		HasMore: resp.HasMore,
	}, nil
}

func (b *remoteBackend) Update(ctx context.Context, model string, opts storage.UpdateOptions) (int, error) {
	req := map[string]interface{}{
		"model": model,
		"in":    opts.TargetEntities,
		"limit": opts.Limit,
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

func (b *remoteBackend) ListPartitions(ctx context.Context) ([]PartitionMeta, error) {
	var resp struct {
		Success    bool            `json:"success"`
		Partitions []PartitionMeta `json:"partitions"`
	}
	if err := b.doJSON(ctx, http.MethodGet, "/v1/partition/list", nil, &resp); err != nil {
		return nil, err
	}
	return resp.Partitions, nil
}

func (b *remoteBackend) InspectPartition(ctx context.Context, partitionFile, recordName string, limit int) (*PartitionInspectResult, error) {
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
		Success       bool               `json:"success"`
		PartitionFile string             `json:"partition_file"`
		Name          string             `json:"name,omitempty"`
		Props         map[string]float32 `json:"props,omitempty"`
		Records       []PartitionRecord  `json:"records,omitempty"`
		Count         int                `json:"count,omitempty"`
	}
	if err := b.doJSON(ctx, http.MethodPost, "/v1/partition/inspect", req, &resp); err != nil {
		return nil, err
	}
	return &PartitionInspectResult{
		Success:       resp.Success,
		PartitionFile: resp.PartitionFile,
		Name:          resp.Name,
		Props:         resp.Props,
		Records:       resp.Records,
		Count:         resp.Count,
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
		return fmt.Sprintf("%s + %g", o.Field, o.Days)
	case storage.OpSubtractDays:
		return fmt.Sprintf("%s - %g", o.Field, o.Days)
	case storage.OpDaysBetween:
		return fmt.Sprintf("%s - %s", o.FieldA, o.FieldB)
	default:
		return fmt.Sprintf("%v", op)
	}
}

func serializeFilters(filters []storage.FilterCondition) []map[string]string {
	out := make([]map[string]string, len(filters))
	for i, f := range filters {
		m := map[string]string{
			"field":    f.Field,
			"operator": f.Operator,
			"value":    fmt.Sprintf("%g", f.Value),
		}
		if f.Operator == "BETWEEN" {
			m["value_high"] = fmt.Sprintf("%g", f.ValueHigh)
		}
		out[i] = m
	}
	return out
}
