package driver

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/rachmanzz/anchordb/pkg/spatial"
	"github.com/rachmanzz/anchordb/pkg/storage"
)

// Client is the dedicated AnchorDB client instance.
type Client struct {
	db  *storage.AnchorDB
	dsn string
}

// Connect opens an AnchorDB connection from a DSN string.
// Supported formats:
// - "anchordb://./path/to/data.anchordb?mode=mmap"
// - "./path/to/data.anchordb"
func Connect(dsn string) (*Client, error) {
	filePath := dsn

	if strings.HasPrefix(dsn, "anchordb://") {
		u, err := url.Parse(dsn)
		if err != nil {
			return nil, fmt.Errorf("invalid DSN: %w", err)
		}
		filePath = u.Host + u.Path
	}

	db, err := storage.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to AnchorDB at %s: %w", filePath, err)
	}

	return &Client{
		db:  db,
		dsn: dsn,
	}, nil
}

// Graph returns a new fluent query builder.
func (c *Client) Graph() *GraphBuilder {
	return NewGraphBuilder(c.db)
}

// Math returns direct numerical and mathematical operations engine on the graph.
func (c *Client) Math() *MathEngine {
	return &MathEngine{db: c.db}
}

// CentroidResult holds the spatial and role centroid
type CentroidResult struct {
	Coord spatial.Point2D
	Roles [4]float32
}

// MathEngine provides direct mathematical operators on graph concepts
type MathEngine struct {
	db *storage.AnchorDB
}

// Distance computes exact Euclidean distance between two concepts in-storage
func (m *MathEngine) Distance(wordA, wordB string) (float32, error) {
	return m.db.CalculateDistance(wordA, wordB)
}

// Centroid calculates central mass coordinate and average role distribution
func (m *MathEngine) Centroid(words ...string) (CentroidResult, error) {
	coord, roles, err := m.db.CalculateCentroid(words)
	return CentroidResult{Coord: coord, Roles: roles}, err
}

// CosineSimilarity computes cosine angle between semantic roles of two words
func (m *MathEngine) CosineSimilarity(wordA, wordB string) (float32, error) {
	return m.db.CalculateCosineSimilarity(wordA, wordB)
}

// Nearest finds the K spatially closest concepts around a target word
func (m *MathEngine) Nearest(word string, k int) ([]storage.GraphQueryResult, error) {
	return m.db.FindNearestNeighbors(word, k)
}

// DB returns the underlying raw computational AnchorDB engine if needed.
func (c *Client) DB() *storage.AnchorDB {
	return c.db
}

// Link registers a directed, weighted edge source --relation--> target. Used to
// store explicit cause-and-effect links that the geometric relations cannot
// express.
func (c *Client) Link(source, relation, target string, weight float32) error {
	return c.db.AddEdge(source, relation, target, weight)
}

// Unlink removes a directed edge.
func (c *Client) Unlink(source, relation, target string) error {
	return c.db.RemoveEdge(source, relation, target)
}

// Causal follows a single relation from a seed word and returns targets ranked
// by edge weight.
func (c *Client) Causal(seed, relation string, k int) ([]storage.CausalResult, error) {
	return c.db.ExecuteCausalStep(context.Background(), []string{seed}, relation, k)
}

// CausalChain follows a sequence of relations, multiplying weights along the way.
func (c *Client) CausalChain(seeds, relations []string, k int) ([]storage.CausalResult, error) {
	return c.db.ExecuteCausalChain(context.Background(), seeds, relations, k)
}

// Close gracefully closes database memory mappings.
func (c *Client) Close() error {
	return c.db.Close()
}

// Divide creates an in-storage division operator (a / b)
func Divide(fieldA, fieldB string) storage.ComputeOp {
	return storage.OpDivide{FieldA: fieldA, FieldB: fieldB}
}

// Multiply creates an in-storage multiplication operator (a * b)
func Multiply(fieldA, fieldB string) storage.ComputeOp {
	return storage.OpMultiply{FieldA: fieldA, FieldB: fieldB}
}

// Add creates an in-storage addition operator (a + b)
func Add(fieldA, fieldB string) storage.ComputeOp {
	return storage.OpAdd{FieldA: fieldA, FieldB: fieldB}
}

// Subtract creates an in-storage subtraction operator (a - b)
func Subtract(fieldA, fieldB string) storage.ComputeOp {
	return storage.OpSubtract{FieldA: fieldA, FieldB: fieldB}
}

// Date converts a "YYYY-MM-DD" string to numeric Epoch Days for AnchorDB.
// It returns an error for malformed input rather than panicking. Use MustDate
// for compile-time-known date literals where a panic on bad input is acceptable.
func Date(dateStr string) (float32, error) {
	return storage.ParseDate(dateStr)
}

// MustDate is like Date but panics on malformed input. It is intended for
// hard-coded date literals in examples and tests.
func MustDate(dateStr string) float32 {
	d, err := storage.ParseDate(dateStr)
	if err != nil {
		panic(fmt.Sprintf("invalid date format %q: %v", dateStr, err))
	}
	return d
}

// AddDays creates an in-storage operator to add N days to a date field
func AddDays(field string, days float32) storage.ComputeOp {
	return storage.OpAddDays{Field: field, Days: days}
}

// SubtractDays creates an in-storage operator to subtract N days from a date field
func SubtractDays(field string, days float32) storage.ComputeOp {
	return storage.OpSubtractDays{Field: field, Days: days}
}

// DaysBetween creates an in-storage operator to calculate days between fieldA and fieldB
func DaysBetween(fieldA, fieldB string) storage.ComputeOp {
	return storage.OpDaysBetween{FieldA: fieldA, FieldB: fieldB}
}

// Value creates an operator assigning a constant numeric value (e.g. 0 for non-active, 1 for active)
func Value(val float32) storage.ComputeOp {
	return storage.OpAssign{Value: fmt.Sprintf("%g", val)}
}

// Assign creates an operator assigning a literal value or copying another field
func Assign(valueOrField string) storage.ComputeOp {
	return storage.OpAssign{Value: valueOrField}
}

// Save stores an entity with its raw numeric properties in AnchorDB (SAVING).
// If name is empty, a UUIDv4 is automatically generated.
func (c *Client) Save(name string, props map[string]float32) error {
	return c.db.SaveRecord(name, props)
}

// SaveAutoID stores an entity with an automatically generated UUIDv4 identifier.
func (c *Client) SaveAutoID(props map[string]float32) (string, error) {
	return c.db.SaveRecordWithID("", props)
}

// SaveWithID stores an entity with its raw numeric properties in AnchorDB, returning the effective name identifier.
// If name is an empty string, a UUIDv4 is automatically generated.
func (c *Client) SaveWithID(name string, props map[string]float32) (string, error) {
	return c.db.SaveRecordWithID(name, props)
}

// Get creates a query builder to retrieve an entity and optionally compute derived fields (GETTING & CALCULATING).
func (c *Client) Get(name string) *GetBuilder {
	return &GetBuilder{
		db:           c.db,
		name:         name,
		computations: make(map[string]storage.ComputeOp),
	}
}

// GetBuilder provides fluent computed attribute extraction and column projection
type GetBuilder struct {
	db           *storage.AnchorDB
	name         string
	selectFields []string
	computations map[string]storage.ComputeOp
}

// Select specifies which columns/attributes to retrieve (projection).
// e.g., Select("jarak", "waktu", "kecepatan")
func (b *GetBuilder) Select(fields ...string) *GetBuilder {
	b.selectFields = append(b.selectFields, fields...)
	return b
}

// Compute adds an on-the-fly mathematical computation to be executed by AnchorDB
func (b *GetBuilder) Compute(outputField string, op storage.ComputeOp) *GetBuilder {
	b.computations[outputField] = op
	return b
}

// Execute retrieves the stored data and calculates all derived fields in-storage
func (b *GetBuilder) Execute(ctx context.Context) (*EntityResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var props map[string]float32
	var err error

	if len(b.computations) == 0 {
		props, err = b.db.GetRecord(b.name, b.selectFields...)
	} else {
		props, err = b.db.GetRecordWithCompute(b.name, b.computations, b.selectFields...)
	}

	if err != nil {
		return nil, err
	}

	return &EntityResult{Name: b.name, props: props}, nil
}

// EntityResult wraps retrieved and computed properties
type EntityResult struct {
	Name  string
	props map[string]float32
}

// Get returns the numeric value of a field (stored or computed)
func (r *EntityResult) Get(field string) float32 {
	return r.props[field]
}

// Has checks if a field is present in the returned result
func (r *EntityResult) Has(field string) bool {
	_, ok := r.props[field]
	return ok
}

// All returns all stored and computed fields
func (r *EntityResult) All() map[string]float32 {
	return r.props
}

// GetDate returns the field value formatted as a YYYY-MM-DD date string
func (r *EntityResult) GetDate(field string) string {
	val := r.props[field]
	return storage.FormatDate(val)
}

// GetTimestamp returns the field value formatted as a YYYY-MM-DD HH:MM:SS timestamp string
func (r *EntityResult) GetTimestamp(field string) string {
	val := r.props[field]
	return storage.FormatTimestamp(val)
}

// List creates a query builder to list, filter, compute, and paginate multiple entities.
func (c *Client) List() *ListBuilder {
	return &ListBuilder{
		db:           c.db,
		computations: make(map[string]storage.ComputeOp),
		limit:        10,
		offset:       0,
	}
}

// ListBuilder provides fluent filtering, computing, and pagination on collections
type ListBuilder struct {
	db           *storage.AnchorDB
	err          error
	selectFields []string
	filters      []storage.FilterCondition
	computations map[string]storage.ComputeOp
	limit        int
	offset       int
}

// toFloat32Value converts any numeric or date literal value to float32
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

// Select specifies which columns/attributes to retrieve (projection).
// e.g., Select("jarak", "waktu", "kecepatan")
func (b *ListBuilder) Select(fields ...string) *ListBuilder {
	b.selectFields = append(b.selectFields, fields...)
	return b
}

// Where adds a filter condition (supports numbers or date strings)
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

// WhereDate adds an exact date match condition (field = "YYYY-MM-DD")
func (b *ListBuilder) WhereDate(field, dateStr string) *ListBuilder {
	return b.Where(field, "=", dateStr)
}

// WhereBefore matches records where field < value (supports numbers or dates)
func (b *ListBuilder) WhereBefore(field string, value any) *ListBuilder {
	return b.Where(field, "<", value)
}

// WhereAfter matches records where field > value (supports numbers or dates)
func (b *ListBuilder) WhereAfter(field string, value any) *ListBuilder {
	return b.Where(field, ">", value)
}

// Between matches records within range [min, max] (supports numbers or dates)
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

// From is an alias for Between(field, min, max)
func (b *ListBuilder) From(field string, min, max any) *ListBuilder {
	return b.Between(field, min, max)
}

// BetweenDate matches records where date is in [startDate, endDate]
func (b *ListBuilder) BetweenDate(field, startDate, endDate string) *ListBuilder {
	return b.Between(field, startDate, endDate)
}

// Compute adds an on-the-fly computed attribute to each retrieved entity
func (b *ListBuilder) Compute(field string, op storage.ComputeOp) *ListBuilder {
	b.computations[field] = op
	return b
}

// Limit sets maximum number of records to retrieve (Constant-Time Bound)
func (b *ListBuilder) Limit(limit int) *ListBuilder {
	b.limit = limit
	return b
}

// Offset sets number of records to skip
func (b *ListBuilder) Offset(offset int) *ListBuilder {
	b.offset = offset
	return b
}

// Page configures pagination with 1-based page index and pageSize
func (b *ListBuilder) Page(page, pageSize int) *ListBuilder {
	if page < 1 {
		page = 1
	}
	b.limit = pageSize
	b.offset = (page - 1) * pageSize
	return b
}

// PagedListResult holds paginated entities and cursor flag
type PagedListResult struct {
	Items   []EntityResult
	HasMore bool
}

// Execute runs the query with in-storage filtering, computing, and early termination
func (b *ListBuilder) Execute(ctx context.Context) (*PagedListResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if b.err != nil {
		return nil, b.err
	}
	records, hasMore, err := b.db.ListRecordsWithPaging(storage.QueryOptions{
		Context:      ctx,
		Select:       b.selectFields,
		Filters:      b.filters,
		Computations: b.computations,
		Limit:        b.limit,
		Offset:       b.offset,
	})
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

// Update creates a builder to perform in-storage batch updates with mathematical calculations.
func (c *Client) Update() *UpdateBuilder {
	return &UpdateBuilder{
		db:        c.db,
		mutations: make(map[string]storage.ComputeOp),
		limit:     10000,
	}
}

// UpdateBuilder provides fluent batch mutation with in-storage calculations
type UpdateBuilder struct {
	db             *storage.AnchorDB
	err            error
	targetEntities []string
	filters        []storage.FilterCondition
	mutations      map[string]storage.ComputeOp
	limit          int
	offset         int
}

// In restricts the update to specific entity names (Direct O(1) key access)
func (b *UpdateBuilder) In(names ...string) *UpdateBuilder {
	b.targetEntities = append(b.targetEntities, names...)
	return b
}

// Where adds a filter condition for which entities should be updated (supports numbers or dates)
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

// WhereDate adds exact date match for update
func (b *UpdateBuilder) WhereDate(field, dateStr string) *UpdateBuilder {
	return b.Where(field, "=", dateStr)
}

// WhereBefore matches records where field < value (number or date)
func (b *UpdateBuilder) WhereBefore(field string, value any) *UpdateBuilder {
	return b.Where(field, "<", value)
}

// WhereAfter matches records where field > value (number or date)
func (b *UpdateBuilder) WhereAfter(field string, value any) *UpdateBuilder {
	return b.Where(field, ">", value)
}

// Between bounds update to records within [min, max] (number or date)
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

// From is an alias for Between
func (b *UpdateBuilder) From(field string, min, max any) *UpdateBuilder {
	return b.Between(field, min, max)
}

// BetweenDate bounds update to records where date is in [startDate, endDate]
func (b *UpdateBuilder) BetweenDate(field, startDate, endDate string) *UpdateBuilder {
	return b.Between(field, startDate, endDate)
}

// Set defines an attribute to update using in-storage math (e.g. Set("jarak", driver.Add("jarak", "10")))
func (b *UpdateBuilder) Set(field string, op storage.ComputeOp) *UpdateBuilder {
	b.mutations[field] = op
	return b
}

// Limit bounds the maximum number of records to update (Constant-Time Bound O(K))
func (b *UpdateBuilder) Limit(limit int) *UpdateBuilder {
	b.limit = limit
	return b
}

// Offset sets the offset index to start updating matching records
func (b *UpdateBuilder) Offset(offset int) *UpdateBuilder {
	b.offset = offset
	return b
}

// Execute performs the in-place calculation and update in-storage, returning the count of modified entities
func (b *UpdateBuilder) Execute(ctx context.Context) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if b.err != nil {
		return 0, b.err
	}
	return b.db.UpdateRecords(storage.UpdateOptions{
		Context:        ctx,
		TargetEntities: b.targetEntities,
		Filters:        b.filters,
		Mutations:      b.mutations,
		Limit:          b.limit,
		Offset:         b.offset,
	})
}
