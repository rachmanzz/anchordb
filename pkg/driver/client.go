package driver

import (
	"context"
	"fmt"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/rachmanzz/anchordb/pkg/spatial"
	"github.com/rachmanzz/anchordb/pkg/storage"
)

// Client is the official AnchorDB client instance supporting both remote engine
// connections and local embedded storage.
type Client struct {
	backend backend
	model   string
	dsn     string
}

// Connect opens an AnchorDB connection.
//
// Target formats:
//
//   - Remote Engine (Production):
//     "http://localhost:7447"
//     "https://localhost:7447"
//     "anchordb://localhost:7447"
//     "anchordb://token@localhost:7447/model_name"
//
//   - Remote Engine (Production / HTTP REST):
//     "http://localhost:7447"
//     "https://localhost:7447"
//     "anchordb://localhost:7447"
//     "anchordb://token@localhost:7447/model_name"
//
//   - Embedded Engine Mode (In-Process Runtime, Zero Network):
//     "./data"
//     "/var/data/anchordb"
//     "embedded://./data"
//     "anchordb://./data"
//     "./data/plant_iot.anchordb"
func Connect(dsn string, opts ...Option) (*Client, error) {
	cfg := defaultClientConfig()
	for _, opt := range opts {
		opt(&cfg)
	}

	dsn = strings.TrimSpace(dsn)
	if dsn == "" {
		return nil, fmt.Errorf("empty DSN or target address")
	}

	// 1. Direct HTTP/HTTPS remote URL
	if strings.HasPrefix(dsn, "http://") || strings.HasPrefix(dsn, "https://") {
		u, err := url.Parse(dsn)
		if err != nil {
			return nil, fmt.Errorf("invalid engine URL: %w", err)
		}
		if u.User != nil {
			if pass, ok := u.User.Password(); ok && pass != "" {
				cfg.token = pass
			} else if u.User.Username() != "" {
				cfg.token = u.User.Username()
			}
		}
		q := u.Query()
		if tok := q.Get("token"); tok != "" {
			cfg.token = tok
		}
		if mdl := q.Get("model"); mdl != "" {
			cfg.model = mdl
		}
		pathModel := strings.Trim(u.Path, "/")
		if pathModel != "" && cfg.model == "default" {
			cfg.model = pathModel
		}

		baseURL := fmt.Sprintf("%s://%s", u.Scheme, u.Host)
		backend, err := newRemoteBackend(baseURL, cfg)
		if err != nil {
			return nil, err
		}
		return &Client{backend: backend, model: cfg.model, dsn: dsn}, nil
	}

	// 2. embedded:// scheme
	if strings.HasPrefix(dsn, "embedded://") {
		cleanPath := strings.TrimPrefix(dsn, "embedded://")
		if idx := strings.Index(cleanPath, "?"); idx != -1 {
			cleanPath = cleanPath[:idx]
		}
		return OpenEmbedded(cleanPath, opts...)
	}

	// 3. anchordb:// scheme
	if strings.HasPrefix(dsn, "anchordb://") {
		raw := strings.TrimPrefix(dsn, "anchordb://")

		// Check if it's a file or directory path
		if strings.HasPrefix(raw, ".") || strings.HasPrefix(raw, "/") ||
			(strings.Contains(raw, ".anchordb") && !strings.Contains(raw, ":7447")) {
			cleanPath := raw
			if idx := strings.Index(cleanPath, "?"); idx != -1 {
				cleanPath = cleanPath[:idx]
			}
			if !strings.HasSuffix(cleanPath, ".anchordb") {
				return OpenEmbedded(cleanPath, opts...)
			}
			dir := filepath.Dir(cleanPath)
			model := strings.TrimSuffix(filepath.Base(cleanPath), ".anchordb")
			allOpts := append([]Option{WithModel(model)}, opts...)
			return OpenEmbedded(dir, allOpts...)
		}

		// Otherwise, parse as network host
		u, err := url.Parse("http://" + raw)
		if err != nil {
			return nil, fmt.Errorf("invalid anchordb URI: %w", err)
		}
		if u.User != nil {
			if pass, ok := u.User.Password(); ok && pass != "" {
				cfg.token = pass
			} else if u.User.Username() != "" {
				cfg.token = u.User.Username()
			}
		}
		q := u.Query()
		if tok := q.Get("token"); tok != "" {
			cfg.token = tok
		}
		if mdl := q.Get("model"); mdl != "" {
			cfg.model = mdl
		}
		pathModel := strings.Trim(u.Path, "/")
		if pathModel != "" && cfg.model == "default" {
			cfg.model = pathModel
		}

		baseURL := "http://" + u.Host
		backend, err := newRemoteBackend(baseURL, cfg)
		if err != nil {
			return nil, err
		}
		return &Client{backend: backend, model: cfg.model, dsn: dsn}, nil
	}

	// 4. Local directory or file path directly
	cleanPath := dsn
	if idx := strings.Index(cleanPath, "?"); idx != -1 {
		cleanPath = cleanPath[:idx]
	}
	if !strings.HasSuffix(cleanPath, ".anchordb") {
		return OpenEmbedded(cleanPath, opts...)
	}
	dir := filepath.Dir(cleanPath)
	model := strings.TrimSuffix(filepath.Base(cleanPath), ".anchordb")
	allOpts := append([]Option{WithModel(model)}, opts...)
	return OpenEmbedded(dir, allOpts...)
}

// OpenEmbedded initializes an in-process AnchorDB engine targeting dataDir.
// It runs the full engine runtime (WAL, file locking, multi-model routing, worker pool,
// and optional background sync/scheduler) directly in-process with zero network overhead.
func OpenEmbedded(dataDir string, opts ...Option) (*Client, error) {
	cfg := defaultClientConfig()
	for _, opt := range opts {
		opt(&cfg)
	}
	backend, err := newEmbeddedBackend(dataDir, cfg)
	if err != nil {
		return nil, err
	}
	return &Client{backend: backend, model: cfg.model, dsn: dataDir}, nil
}

// OpenLocal opens a local AnchorDB file directly.
// Deprecated: Use OpenEmbedded(dataDir) to run the full in-process AnchorDB engine.
func OpenLocal(filePath string) (*Client, error) {
	if idx := strings.Index(filePath, "?"); idx != -1 {
		filePath = filePath[:idx]
	}
	backend, err := newLocalBackend(filePath)
	if err != nil {
		return nil, err
	}
	return &Client{backend: backend, model: "default", dsn: filePath}, nil
}

// ConnectRemote connects to a running AnchorDB engine server over HTTP/REST.
func ConnectRemote(baseURL string, opts ...Option) (*Client, error) {
	cfg := defaultClientConfig()
	for _, opt := range opts {
		opt(&cfg)
	}
	backend, err := newRemoteBackend(baseURL, cfg)
	if err != nil {
		return nil, err
	}
	return &Client{backend: backend, model: cfg.model, dsn: baseURL}, nil
}

// Model returns a shallow copy of the client targeting a different model database.
func (c *Client) Model(name string) *Client {
	if name == "" {
		name = "default"
	}
	return &Client{
		backend: c.backend,
		model:   name,
		dsn:     c.dsn,
	}
}

// ModelName returns the currently active model name.
func (c *Client) ModelName() string {
	return c.model
}

// IsRemote reports true if the client communicates over the network with an AnchorDB engine.
func (c *Client) IsRemote() bool {
	return c.backend.IsRemote()
}

// IsLocal reports true if the client wraps an embedded local AnchorDB file.
func (c *Client) IsLocal() bool {
	return !c.backend.IsRemote()
}

// DB returns the underlying raw storage engine if running in local mode; nil if remote.
func (c *Client) DB() *storage.AnchorDB {
	return c.backend.DB()
}

// Close gracefully closes local memory mappings or releases idle network connections.
func (c *Client) Close() error {
	return c.backend.Close()
}

// Sync flushes modifications to disk.
func (c *Client) Sync() error {
	return c.backend.Sync(context.Background(), c.model)
}

// ListPartitions queries the engine to retrieve metadata on archived partition files.
func (c *Client) ListPartitions(ctx context.Context) ([]PartitionMeta, error) {
	return c.backend.ListPartitions(ctx)
}

// InspectPartition inspects an archived partition file through the engine runtime without exposing raw file descriptors.
func (c *Client) InspectPartition(ctx context.Context, partitionFile, recordName string, limit int) (*PartitionInspectResult, error) {
	return c.backend.InspectPartition(ctx, partitionFile, recordName, limit)
}

// InspectPartitionRecord retrieves a single record from an archived partition file through the engine.
func (c *Client) InspectPartitionRecord(ctx context.Context, partitionFile, recordName string) (*EntityResult, error) {
	res, err := c.backend.InspectPartition(ctx, partitionFile, recordName, 0)
	if err != nil {
		return nil, err
	}
	return &EntityResult{Name: recordName, props: res.Props}, nil
}

// Graph returns a new fluent query builder for spatial ray casting and multi-hop reasoning.
func (c *Client) Graph() *GraphBuilder {
	return newGraphBuilder(c.backend, c.model)
}

// Math returns direct numerical and mathematical operations engine on the graph.
func (c *Client) Math() *MathEngine {
	return &MathEngine{backend: c.backend, model: c.model}
}

// CentroidResult holds the spatial and role centroid.
type CentroidResult struct {
	Coord spatial.Point2D
	Roles [4]float32
}

// MathEngine provides direct mathematical operators on graph concepts.
type MathEngine struct {
	backend backend
	model   string
}

// Distance computes exact Euclidean distance between two concepts in-storage.
func (m *MathEngine) Distance(wordA, wordB string) (float32, error) {
	return m.backend.Distance(context.Background(), m.model, wordA, wordB)
}

// Centroid calculates central mass coordinate and average role distribution.
func (m *MathEngine) Centroid(words ...string) (CentroidResult, error) {
	return m.backend.Centroid(context.Background(), m.model, words)
}

// CosineSimilarity computes cosine angle between semantic roles of two words.
func (m *MathEngine) CosineSimilarity(wordA, wordB string) (float32, error) {
	return m.backend.CosineSimilarity(context.Background(), m.model, wordA, wordB)
}

// Nearest finds the K spatially closest concepts around a target word.
func (m *MathEngine) Nearest(word string, k int) ([]storage.GraphQueryResult, error) {
	return m.backend.Nearest(context.Background(), m.model, word, k)
}

// Link registers a directed, weighted edge source --relation--> target.
func (c *Client) Link(source, relation, target string, weight float32) error {
	return c.backend.Edge(context.Background(), c.model, "add", source, relation, target, weight)
}

// Unlink removes a directed edge.
func (c *Client) Unlink(source, relation, target string) error {
	return c.backend.Edge(context.Background(), c.model, "remove", source, relation, target, 0)
}

// Causal follows a single relation from a seed word and returns targets ranked by edge weight.
func (c *Client) Causal(seed, relation string, k int) ([]storage.CausalResult, error) {
	return c.backend.Causal(context.Background(), c.model, []string{seed}, relation, nil, k)
}

// CausalChain follows a sequence of relations, multiplying weights along the way.
func (c *Client) CausalChain(seeds, relations []string, k int) ([]storage.CausalResult, error) {
	return c.backend.Causal(context.Background(), c.model, seeds, "", relations, k)
}

// AppendNode registers a concept node with 2D spatial coordinate and 4D semantic role vector.
func (c *Client) AppendNode(word string, coord spatial.Point2D, roles [4]float32) error {
	return c.backend.AppendNode(context.Background(), c.model, word, coord, roles)
}

// SetRelation configures a directional spatial vector relation for ray-casting traversals.
func (c *Client) SetRelation(name string, direction spatial.Point2D) error {
	return c.backend.SetRelation(context.Background(), c.model, name, direction)
}

// -----------------------------------------------------------------------------
// Operators & Date Helpers
// -----------------------------------------------------------------------------

// Divide creates an in-storage division operator (a / b).
func Divide(fieldA, fieldB string) storage.ComputeOp {
	return storage.OpDivide{FieldA: fieldA, FieldB: fieldB}
}

// Multiply creates an in-storage multiplication operator (a * b).
func Multiply(fieldA, fieldB string) storage.ComputeOp {
	return storage.OpMultiply{FieldA: fieldA, FieldB: fieldB}
}

// Add creates an in-storage addition operator (a + b).
// Both fieldA and fieldB can be attribute names or stringified numeric literals (e.g. "10").
func Add(fieldA, fieldB string) storage.ComputeOp {
	return storage.OpAdd{FieldA: fieldA, FieldB: fieldB}
}

// AddConst creates an in-storage addition operator adding a numeric constant to an attribute (field + val).
func AddConst(field string, val float32) storage.ComputeOp {
	return storage.OpAdd{FieldA: field, FieldB: fmt.Sprintf("%g", val)}
}

// Subtract creates an in-storage subtraction operator (a - b).
func Subtract(fieldA, fieldB string) storage.ComputeOp {
	return storage.OpSubtract{FieldA: fieldA, FieldB: fieldB}
}

// SubtractConst creates an in-storage subtraction operator subtracting a numeric constant from an attribute (field - val).
func SubtractConst(field string, val float32) storage.ComputeOp {
	return storage.OpSubtract{FieldA: field, FieldB: fmt.Sprintf("%g", val)}
}

// MultiplyConst creates an in-storage multiplication operator multiplying an attribute by a numeric constant (field * val).
func MultiplyConst(field string, val float32) storage.ComputeOp {
	return storage.OpMultiply{FieldA: field, FieldB: fmt.Sprintf("%g", val)}
}

// DivideConst creates an in-storage division operator dividing an attribute by a numeric constant (field / val).
func DivideConst(field string, val float32) storage.ComputeOp {
	return storage.OpDivide{FieldA: field, FieldB: fmt.Sprintf("%g", val)}
}

// Date converts a "YYYY-MM-DD" string to numeric Epoch Days.
func Date(dateStr string) (float32, error) {
	return storage.ParseDate(dateStr)
}

// MustDate is like Date but panics on malformed input.
func MustDate(dateStr string) float32 {
	d, err := storage.ParseDate(dateStr)
	if err != nil {
		panic(fmt.Sprintf("invalid date format %q: %v", dateStr, err))
	}
	return d
}

// FormatDate converts numeric Epoch Days back to a "YYYY-MM-DD" string.
func FormatDate(epochDays float32) string {
	return storage.FormatDate(epochDays)
}

// AddDays creates an in-storage operator to add N days to a date field.
func AddDays(field string, days float32) storage.ComputeOp {
	return storage.OpAddDays{Field: field, Days: days}
}

// SubtractDays creates an in-storage operator to subtract N days from a date field.
func SubtractDays(field string, days float32) storage.ComputeOp {
	return storage.OpSubtractDays{Field: field, Days: days}
}

// DaysBetween creates an in-storage operator to calculate days between fieldA and fieldB.
func DaysBetween(fieldA, fieldB string) storage.ComputeOp {
	return storage.OpDaysBetween{FieldA: fieldA, FieldB: fieldB}
}

// Value creates an operator assigning a constant numeric value.
func Value(val float32) storage.ComputeOp {
	return storage.OpAssign{Value: fmt.Sprintf("%g", val)}
}

// Assign creates an operator assigning a literal value or copying another field.
func Assign(valueOrField string) storage.ComputeOp {
	return storage.OpAssign{Value: valueOrField}
}

// -----------------------------------------------------------------------------
// CRUD & Builders
// -----------------------------------------------------------------------------

// Save stores an entity with its raw numeric properties in AnchorDB.
func (c *Client) Save(name string, props map[string]float32) error {
	_, err := c.backend.Save(context.Background(), c.model, name, nil, props)
	return err
}

// SaveAutoID stores an entity with an automatically generated UUIDv4 identifier.
func (c *Client) SaveAutoID(props map[string]float32) (string, error) {
	return c.backend.Save(context.Background(), c.model, "", nil, props)
}

// SaveWithID stores an entity with its raw numeric properties in AnchorDB, returning the effective name identifier.
func (c *Client) SaveWithID(name string, props map[string]float32) (string, error) {
	return c.backend.Save(context.Background(), c.model, name, nil, props)
}

// Get creates a query builder to retrieve an entity and optionally compute derived fields.
func (c *Client) Get(name string) *GetBuilder {
	return &GetBuilder{
		backend:      c.backend,
		model:        c.model,
		name:         name,
		computations: make(map[string]storage.ComputeOp),
	}
}

// GetBuilder provides fluent computed attribute extraction and column projection.
type GetBuilder struct {
	backend      backend
	model        string
	name         string
	selectFields []string
	computations map[string]storage.ComputeOp
}

// Select specifies which columns/attributes to retrieve (projection).
func (b *GetBuilder) Select(fields ...string) *GetBuilder {
	b.selectFields = append(b.selectFields, fields...)
	return b
}

// Compute adds an on-the-fly mathematical computation to be executed in AnchorDB.
func (b *GetBuilder) Compute(outputField string, op storage.ComputeOp) *GetBuilder {
	b.computations[outputField] = op
	return b
}

// Execute retrieves the stored data and calculates all derived fields.
func (b *GetBuilder) Execute(ctx context.Context) (*EntityResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return b.backend.Get(ctx, b.model, b.name, b.selectFields, b.computations)
}

// EntityResult wraps retrieved and computed properties.
type EntityResult struct {
	Name  string
	props map[string]float32
}

// Get returns the numeric value of a field (stored or computed).
func (r *EntityResult) Get(field string) float32 {
	return r.props[field]
}

// Has checks if a field is present in the returned result.
func (r *EntityResult) Has(field string) bool {
	_, ok := r.props[field]
	return ok
}

// All returns all stored and computed fields.
func (r *EntityResult) All() map[string]float32 {
	return r.props
}

// Props returns all stored and computed fields.
func (r *EntityResult) Props() map[string]float32 {
	return r.props
}

// GetDate returns the field value formatted as a YYYY-MM-DD date string.
func (r *EntityResult) GetDate(field string) string {
	val := r.props[field]
	return storage.FormatDate(val)
}

// GetTimestamp returns the field value formatted as a YYYY-MM-DD HH:MM:SS timestamp string.
func (r *EntityResult) GetTimestamp(field string) string {
	val := r.props[field]
	return storage.FormatTimestamp(val)
}

// List creates a query builder to list, filter, compute, and paginate multiple entities.
func (c *Client) List() *ListBuilder {
	return &ListBuilder{
		backend:      c.backend,
		model:        c.model,
		computations: make(map[string]storage.ComputeOp),
		limit:        10,
		offset:       0,
	}
}

// ListBuilder provides fluent filtering, computing, and pagination on collections.
type ListBuilder struct {
	backend      backend
	model        string
	err          error
	selectFields []string
	filters      []storage.FilterCondition
	computations map[string]storage.ComputeOp
	limit        int
	offset       int
}

func toFloat32Value(v any) (float32, error) {
	switch val := v.(type) {
	case float32:
		return val, nil
	case float64:
		return float32(val), nil
	case int:
		return float32(val), nil
	case int64:
		return float32(val), nil
	case string:
		if d, err := storage.ParseDate(val); err == nil {
			return d, nil
		}
		if f, err := strconv.ParseFloat(val, 32); err == nil {
			return float32(f), nil
		}
		return 0, fmt.Errorf("invalid numeric or date value %q", val)
	default:
		return 0, fmt.Errorf("unsupported filter value type %T", v)
	}
}

// Select specifies which columns/attributes to retrieve.
func (b *ListBuilder) Select(fields ...string) *ListBuilder {
	b.selectFields = append(b.selectFields, fields...)
	return b
}

// Where adds a filter condition (supports numbers or date strings).
func (b *ListBuilder) Where(field, op string, value any) *ListBuilder {
	converted, err := toFloat32Value(value)
	if err != nil {
		b.err = err
		return b
	}
	b.filters = append(b.filters, storage.FilterCondition{
		Field:    field,
		Operator: op,
		Value:    converted,
	})
	return b
}

// WhereDate adds an exact date match condition (field = "YYYY-MM-DD").
func (b *ListBuilder) WhereDate(field, dateStr string) *ListBuilder {
	return b.Where(field, "=", dateStr)
}

// WhereBefore matches records where field < value (numbers or dates).
func (b *ListBuilder) WhereBefore(field string, value any) *ListBuilder {
	return b.Where(field, "<", value)
}

// WhereAfter matches records where field > value (numbers or dates).
func (b *ListBuilder) WhereAfter(field string, value any) *ListBuilder {
	return b.Where(field, ">", value)
}

// Between matches records within range [min, max].
func (b *ListBuilder) Between(field string, min, max any) *ListBuilder {
	low, err := toFloat32Value(min)
	if err != nil {
		b.err = err
		return b
	}
	high, err := toFloat32Value(max)
	if err != nil {
		b.err = err
		return b
	}
	b.filters = append(b.filters, storage.FilterCondition{
		Field:     field,
		Operator:  "BETWEEN",
		Value:     low,
		ValueHigh: high,
	})
	return b
}

// From is an alias for Between.
func (b *ListBuilder) From(field string, min, max any) *ListBuilder {
	return b.Between(field, min, max)
}

// BetweenDate matches records where date is in [startDate, endDate].
func (b *ListBuilder) BetweenDate(field, startDate, endDate string) *ListBuilder {
	return b.Between(field, startDate, endDate)
}

// Compute adds an on-the-fly computed attribute to each retrieved entity.
func (b *ListBuilder) Compute(field string, op storage.ComputeOp) *ListBuilder {
	b.computations[field] = op
	return b
}

// Limit sets maximum number of records to retrieve.
func (b *ListBuilder) Limit(limit int) *ListBuilder {
	b.limit = limit
	return b
}

// Offset sets number of records to skip.
func (b *ListBuilder) Offset(offset int) *ListBuilder {
	b.offset = offset
	return b
}

// Page configures pagination with 1-based page index and pageSize.
func (b *ListBuilder) Page(page, pageSize int) *ListBuilder {
	if page < 1 {
		page = 1
	}
	b.limit = pageSize
	b.offset = (page - 1) * pageSize
	return b
}

// PagedListResult holds paginated entities and cursor flag.
type PagedListResult struct {
	Items   []EntityResult
	HasMore bool
}

// Execute runs the query with filtering, computing, and pagination.
func (b *ListBuilder) Execute(ctx context.Context) (*PagedListResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if b.err != nil {
		return nil, b.err
	}
	return b.backend.List(ctx, b.model, storage.QueryOptions{
		Context:      ctx,
		Select:       b.selectFields,
		Filters:      b.filters,
		Computations: b.computations,
		Limit:        b.limit,
		Offset:       b.offset,
	})
}

// Update creates a builder to perform in-place batch updates with mathematical calculations.
func (c *Client) Update() *UpdateBuilder {
	return &UpdateBuilder{
		backend:   c.backend,
		model:     c.model,
		mutations: make(map[string]storage.ComputeOp),
		limit:     10000,
	}
}

// UpdateBuilder provides fluent batch mutation with in-storage calculations.
type UpdateBuilder struct {
	backend        backend
	model          string
	err            error
	targetEntities []string
	filters        []storage.FilterCondition
	mutations      map[string]storage.ComputeOp
	limit          int
	offset         int
}

// In restricts the update to specific entity names.
func (b *UpdateBuilder) In(names ...string) *UpdateBuilder {
	b.targetEntities = append(b.targetEntities, names...)
	return b
}

// Where adds a filter condition for update targets.
func (b *UpdateBuilder) Where(field, op string, value any) *UpdateBuilder {
	converted, err := toFloat32Value(value)
	if err != nil {
		b.err = err
		return b
	}
	b.filters = append(b.filters, storage.FilterCondition{
		Field:    field,
		Operator: op,
		Value:    converted,
	})
	return b
}

// WhereDate adds exact date match for update.
func (b *UpdateBuilder) WhereDate(field, dateStr string) *UpdateBuilder {
	return b.Where(field, "=", dateStr)
}

// WhereBefore matches records where field < value.
func (b *UpdateBuilder) WhereBefore(field string, value any) *UpdateBuilder {
	return b.Where(field, "<", value)
}

// WhereAfter matches records where field > value.
func (b *UpdateBuilder) WhereAfter(field string, value any) *UpdateBuilder {
	return b.Where(field, ">", value)
}

// Between bounds update to records within [min, max].
func (b *UpdateBuilder) Between(field string, min, max any) *UpdateBuilder {
	low, err := toFloat32Value(min)
	if err != nil {
		b.err = err
		return b
	}
	high, err := toFloat32Value(max)
	if err != nil {
		b.err = err
		return b
	}
	b.filters = append(b.filters, storage.FilterCondition{
		Field:     field,
		Operator:  "BETWEEN",
		Value:     low,
		ValueHigh: high,
	})
	return b
}

// From is an alias for Between.
func (b *UpdateBuilder) From(field string, min, max any) *UpdateBuilder {
	return b.Between(field, min, max)
}

// BetweenDate bounds update to records where date is in [startDate, endDate].
func (b *UpdateBuilder) BetweenDate(field, startDate, endDate string) *UpdateBuilder {
	return b.Between(field, startDate, endDate)
}

// Set defines an attribute to update using in-storage math.
func (b *UpdateBuilder) Set(field string, op storage.ComputeOp) *UpdateBuilder {
	b.mutations[field] = op
	return b
}

// Limit bounds the maximum number of records to update.
func (b *UpdateBuilder) Limit(limit int) *UpdateBuilder {
	b.limit = limit
	return b
}

// Offset sets the offset index to start updating matching records.
func (b *UpdateBuilder) Offset(offset int) *UpdateBuilder {
	b.offset = offset
	return b
}

// Execute performs the update, returning the count of modified entities.
func (b *UpdateBuilder) Execute(ctx context.Context) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if b.err != nil {
		return 0, b.err
	}
	return b.backend.Update(ctx, b.model, storage.UpdateOptions{
		Context:        ctx,
		TargetEntities: b.targetEntities,
		Filters:        b.filters,
		Mutations:      b.mutations,
		Limit:          b.limit,
		Offset:         b.offset,
	})
}
