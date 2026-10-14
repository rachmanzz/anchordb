package storage

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/rachmanzz/anchordb/pkg/spatial"
)

// OrderByMode defines the sorting strategy in AnchorDB
type OrderByMode int

const (
	OrderByCompositeDesc OrderByMode = iota // Default: Score descending with t tie-breaker
	OrderByDistanceAsc                      // Distance along ray t ascending
	OrderByScoreDesc                        // Pure score descending
)

// GraphQueryResult holds a result produced by internal storage computation
type GraphQueryResult struct {
	Word       string          `json:"word"`
	Coord      spatial.Point2D `json:"coord"`
	T          float32         `json:"t"`
	DistSqPerp float32         `json:"dist_sq_perp"`
	Score      float32         `json:"score"`
}

// AttrEntry stores a single numeric attribute key-value pair in a compact flat slice
type AttrEntry struct {
	Key   string
	Value float32
}

// CompactRecord stores properties in a contiguous slice, saving ~70% RAM vs Go hmap
type CompactRecord []AttrEntry

func (cr CompactRecord) Get(key string) (float32, bool) {
	for i := range cr {
		if cr[i].Key == key {
			return cr[i].Value, true
		}
	}
	return 0, false
}

func (cr CompactRecord) Set(key string, val float32) CompactRecord {
	for i := range cr {
		if cr[i].Key == key {
			cr[i].Value = val
			return cr
		}
	}
	return append(cr, AttrEntry{Key: key, Value: val})
}

func (cr CompactRecord) ToMap() map[string]float32 {
	m := make(map[string]float32, len(cr))
	for i := range cr {
		m[cr[i].Key] = cr[i].Value
	}
	return m
}

func (cr CompactRecord) Project(selectFields []string) map[string]float32 {
	if len(selectFields) == 0 {
		return cr.ToMap()
	}
	m := make(map[string]float32, len(selectFields))
	for _, f := range selectFields {
		if val, exists := cr.Get(f); exists {
			m[f] = val
		}
	}
	return m
}

// AnchorDB is the computational spatial graph storage engine.
type AnchorDB struct {
	mu sync.RWMutex

	file      *os.File
	mmapData  []byte
	isMmapped bool

	Header Header

	// Spatial Grid in RAM for ultra-fast pointer lookup: sparse cell mapping (0 overhead for empty cells)
	Grid map[uint32][]uint32

	// Lexicon dictionary: word -> node record index
	Lexicon map[string]uint32

	// Relations map: relationName -> shift vector T
	Relations map[string]spatial.Point2D

	// Zero-copy mmap node tracking:
	// Synced nodes are read directly from mmapData without heap allocation.
	// Pending nodes are newly appended nodes in memory awaiting the next Sync().
	syncedNodeCount uint64
	pendingNodes    []BinaryNodeRecord

	// Active string pool for newly appended nodes
	stringPool []byte

	// EntityStore stores key-value numerical attributes for entities: entityName -> CompactRecord
	EntityStore map[string]CompactRecord

	// String interning pool for entity attribute keys (e.g. "jarak", "waktu", "kecepatan")
	attrPool map[string]string

	// EntityOrder maintains deterministic insertion order for constant-time sequential scanning
	EntityOrder []string

	filePath string
}

// ParseDate converts YYYY-MM-DD string to numeric Epoch Days (days since 1970-01-01)
func ParseDate(dateStr string) (float32, error) {
	t, err := time.Parse("2006-01-02", dateStr)
	if err != nil {
		return 0, err
	}
	days := float32(t.Unix() / 86400)
	return days, nil
}

// FormatDate converts numeric Epoch Days back to YYYY-MM-DD string
func FormatDate(epochDays float32) string {
	sec := int64(math.Round(float64(epochDays))) * 86400
	return time.Unix(sec, 0).UTC().Format("2006-01-02")
}

// ParseTimestamp converts YYYY-MM-DD HH:MM:SS or RFC3339 string to Unix timestamp seconds
func ParseTimestamp(tsStr string) (float32, error) {
	t, err := time.Parse("2006-01-02 15:04:05", tsStr)
	if err != nil {
		t, err = time.Parse(time.RFC3339, tsStr)
		if err != nil {
			return 0, err
		}
	}
	return float32(t.Unix()), nil
}

// FormatTimestamp converts Unix timestamp seconds to YYYY-MM-DD HH:MM:SS string
func FormatTimestamp(unixSec float32) string {
	return time.Unix(int64(unixSec), 0).UTC().Format("2006-01-02 15:04:05")
}

// resolveValue resolves a field name, numeric literal, or date literal string.
func resolveValue(token string, props map[string]float32) (float32, error) {
	if val, ok := props[token]; ok {
		return val, nil
	}
	if parsed, err := strconv.ParseFloat(token, 32); err == nil {
		return float32(parsed), nil
	}
	if t, err := time.Parse("2006-01-02", token); err == nil {
		return float32(t.Unix() / 86400), nil
	}
	return 0, fmt.Errorf("field, numeric constant, or date %q not found", token)
}

// ComputeOp defines an in-flight computation rule
type ComputeOp interface {
	Apply(props map[string]float32) (float32, error)
}

// OpDivide computes FieldA / FieldB (where fields can be attribute names or literal numbers)
type OpDivide struct {
	FieldA string
	FieldB string
}

func (op OpDivide) Apply(props map[string]float32) (float32, error) {
	valA, err := resolveValue(op.FieldA, props)
	if err != nil {
		return 0, err
	}
	valB, err := resolveValue(op.FieldB, props)
	if err != nil {
		return 0, err
	}
	if valB == 0 {
		return 0, fmt.Errorf("division by zero: %s is 0", op.FieldB)
	}
	return valA / valB, nil
}

// OpMultiply computes FieldA * FieldB
type OpMultiply struct {
	FieldA string
	FieldB string
}

func (op OpMultiply) Apply(props map[string]float32) (float32, error) {
	valA, err := resolveValue(op.FieldA, props)
	if err != nil {
		return 0, err
	}
	valB, err := resolveValue(op.FieldB, props)
	if err != nil {
		return 0, err
	}
	return valA * valB, nil
}

// OpAdd computes FieldA + FieldB
type OpAdd struct {
	FieldA string
	FieldB string
}

func (op OpAdd) Apply(props map[string]float32) (float32, error) {
	valA, err := resolveValue(op.FieldA, props)
	if err != nil {
		return 0, err
	}
	valB, err := resolveValue(op.FieldB, props)
	if err != nil {
		return 0, err
	}
	return valA + valB, nil
}

// OpSubtract computes FieldA - FieldB
type OpSubtract struct {
	FieldA string
	FieldB string
}

func (op OpSubtract) Apply(props map[string]float32) (float32, error) {
	valA, err := resolveValue(op.FieldA, props)
	if err != nil {
		return 0, err
	}
	valB, err := resolveValue(op.FieldB, props)
	if err != nil {
		return 0, err
	}
	return valA - valB, nil
}

// OpAddDays adds N days to a date field (stored as epoch days)
type OpAddDays struct {
	Field string
	Days  float32
}

func (op OpAddDays) Apply(props map[string]float32) (float32, error) {
	val, err := resolveValue(op.Field, props)
	if err != nil {
		return 0, err
	}
	return val + op.Days, nil
}

// OpSubtractDays subtracts N days from a date field
type OpSubtractDays struct {
	Field string
	Days  float32
}

func (op OpSubtractDays) Apply(props map[string]float32) (float32, error) {
	val, err := resolveValue(op.Field, props)
	if err != nil {
		return 0, err
	}
	return val - op.Days, nil
}

// OpDaysBetween calculates number of days between two dates (FieldA - FieldB)
type OpDaysBetween struct {
	FieldA string
	FieldB string
}

func (op OpDaysBetween) Apply(props map[string]float32) (float32, error) {
	valA, err := resolveValue(op.FieldA, props)
	if err != nil {
		return 0, err
	}
	valB, err := resolveValue(op.FieldB, props)
	if err != nil {
		return 0, err
	}
	return valA - valB, nil
}

// OpAssign assigns a constant numeric value, date literal, or copies another field
type OpAssign struct {
	Value string
}

func (op OpAssign) Apply(props map[string]float32) (float32, error) {
	return resolveValue(op.Value, props)
}

// FilterCondition defines a field comparison in Where clauses
type FilterCondition struct {
	Field     string
	Operator  string // "=", "!=", ">", ">=", "<", "<=", "BETWEEN"
	Value     float32
	ValueHigh float32 // Used for "BETWEEN" range [Value, ValueHigh]
}

func (f FilterCondition) Matches(props map[string]float32) bool {
	val, exists := props[f.Field]
	if !exists {
		return false
	}
	switch f.Operator {
	case "=":
		return val == f.Value
	case "!=":
		return val != f.Value
	case ">":
		return val > f.Value
	case ">=":
		return val >= f.Value
	case "<":
		return val < f.Value
	case "<=":
		return val <= f.Value
	case "BETWEEN":
		return val >= f.Value && val <= f.ValueHigh
	default:
		return false
	}
}

// QueryOptions defines criteria for List queries with pagination and column selection (Projection)
type QueryOptions struct {
	Select       []string // Projection: list of fields to return (empty means return all fields)
	Filters      []FilterCondition
	Computations map[string]ComputeOp
	Limit        int
	Offset       int
}

// projectProperties filters props to only include fields specified in selectFields.
// If selectFields is empty, it returns all props unmodified.
func projectProperties(props map[string]float32, selectFields []string) map[string]float32 {
	if len(selectFields) == 0 {
		return props
	}
	projected := make(map[string]float32, len(selectFields))
	for _, f := range selectFields {
		if val, exists := props[f]; exists {
			projected[f] = val
		}
	}
	return projected
}

// EntityRecord represents a returned entity with properties
type EntityRecord struct {
	Name  string             `json:"name"`
	Props map[string]float32 `json:"props"`
}

func (db *AnchorDB) internAttr(s string) string {
	if interned, exists := db.attrPool[s]; exists {
		return interned
	}
	db.attrPool[s] = s
	return s
}

func (db *AnchorDB) totalNodes() int {
	return int(db.syncedNodeCount) + len(db.pendingNodes)
}

func (db *AnchorDB) getNode(idx uint32) BinaryNodeRecord {
	if uint64(idx) < db.syncedNodeCount && db.isMmapped && db.mmapData != nil {
		offset := int(db.Header.NodesOffset) + int(idx)*RecordSize
		if offset+RecordSize <= len(db.mmapData) {
			var rec BinaryNodeRecord
			DecodeRecord(db.mmapData[offset:offset+RecordSize], &rec)
			return rec
		}
	}
	pIdx := int(idx) - int(db.syncedNodeCount)
	if pIdx >= 0 && pIdx < len(db.pendingNodes) {
		return db.pendingNodes[pIdx]
	}
	return BinaryNodeRecord{}
}

func (db *AnchorDB) markNodeDeleted(idx uint32) {
	if uint64(idx) < db.syncedNodeCount && db.isMmapped && db.mmapData != nil {
		offset := int(db.Header.NodesOffset) + int(idx)*RecordSize
		if offset+8 <= len(db.mmapData) {
			binary.LittleEndian.PutUint64(db.mmapData[offset:offset+8], 0)
		}
	} else {
		pIdx := int(idx) - int(db.syncedNodeCount)
		if pIdx >= 0 && pIdx < len(db.pendingNodes) {
			db.pendingNodes[pIdx].ID = 0
		}
	}
}

// Open creates or opens an AnchorDB file using memory mapping.
func Open(filePath string) (*AnchorDB, error) {
	db := &AnchorDB{
		Grid:        make(map[uint32][]uint32),
		Lexicon:     make(map[string]uint32),
		Relations:   make(map[string]spatial.Point2D),
		EntityStore: make(map[string]CompactRecord),
		attrPool:    make(map[string]string),
		EntityOrder: make([]string, 0),
		filePath:    filePath,
	}

	// Register default relation vectors
	db.Relations["keturunan"] = spatial.Point2D{X: 0.0, Y: -0.8}
	db.Relations["pertumbuhan"] = spatial.Point2D{X: 0.5, Y: 0.0}
	db.Relations["edukasi"] = spatial.Point2D{X: 0.0, Y: -0.6}
	db.Relations["kausalitas"] = spatial.Point2D{X: 0.4, Y: -0.4}
	db.Relations["vokalisasi"] = spatial.Point2D{X: -0.5, Y: 0.0}
	db.Relations["perubahan_fase"] = spatial.Point2D{X: 0.0, Y: -0.7}
	db.Relations["pembakaran"] = spatial.Point2D{X: 0.6, Y: -0.6}

	// Check if file exists
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		// Initialize empty database file
		if err := db.initEmptyFile(filePath); err != nil {
			return nil, fmt.Errorf("failed to init empty anchordb file: %w", err)
		}
	}

	// Open file
	f, err := os.OpenFile(filePath, os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", filePath, err)
	}
	db.file = f

	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}

	fileSize := fi.Size()
	if fileSize < HeaderSize {
		_ = f.Close()
		return nil, fmt.Errorf("corrupt file: size %d smaller than header", fileSize)
	}

	// Memory map the file via Linux mmap
	mmapData, err := syscall.Mmap(int(f.Fd()), 0, int(fileSize), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("mmap failed: %w", err)
	}
	db.mmapData = mmapData
	db.isMmapped = true

	// Decode Header
	hdr, err := DecodeHeader(mmapData[:HeaderSize])
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	db.Header = *hdr

	// Build in-memory index from the mmapped records
	if err := db.buildIndex(); err != nil {
		_ = db.Close()
		return nil, err
	}

	// Load entity attributes from disk
	if err := db.loadEntities(); err != nil {
		_ = db.Close()
		return nil, err
	}

	return db, nil
}

func (db *AnchorDB) initEmptyFile(filePath string) error {
	f, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	hdr := Header{
		Magic:        MagicHeader,
		Version:      DBVersion,
		NodeCount:    0,
		GridCellsX:   GridDim,
		GridCellsY:   GridDim,
		GridOffset:   HeaderSize,
		NodesOffset:  HeaderSize,
		StringOffset: HeaderSize,
		EntityCount:  0,
		EntityOffset: HeaderSize,
	}

	var buf [HeaderSize]byte
	hdr.Encode(buf[:])

	_, err = f.Write(buf[:])
	return err
}

func (db *AnchorDB) buildIndex() error {
	nodeCount := int(db.Header.NodeCount)
	db.syncedNodeCount = uint64(nodeCount)
	db.pendingNodes = nil

	// Reconstruct string pool buffer from mmap so reopen preserves words and AppendNode can append cleanly
	if db.Header.EntityOffset > db.Header.StringOffset && int(db.Header.EntityOffset) <= len(db.mmapData) {
		poolSlice := db.mmapData[db.Header.StringOffset:db.Header.EntityOffset]
		db.stringPool = make([]byte, len(poolSlice))
		copy(db.stringPool, poolSlice)
	}

	// Zero-copy indexing: decode stack variable without heap duplication
	var rec BinaryNodeRecord
	for i := 0; i < nodeCount; i++ {
		offset := int(db.Header.NodesOffset) + i*RecordSize
		if offset+RecordSize > len(db.mmapData) {
			break
		}
		DecodeRecord(db.mmapData[offset:offset+RecordSize], &rec)

		// String lookup
		wordOffset := int(db.Header.StringOffset) + int(rec.WordOffset)
		wordLen := int(rec.WordLen)
		if wordOffset+wordLen <= len(db.mmapData) {
			word := string(db.mmapData[wordOffset : wordOffset+wordLen])
			db.Lexicon[word] = uint32(i)
		}

		// Insert into spatial grid cell
		p := spatial.Point2D{X: rec.X, Y: rec.Y}
		cell := PointToCell(p, GridDim)
		idx := CellIndex(cell, GridDim)
		db.Grid[uint32(idx)] = append(db.Grid[uint32(idx)], uint32(i))
	}

	return nil
}

func (db *AnchorDB) loadEntities() error {
	if db.Header.EntityCount == 0 || db.Header.EntityOffset == 0 {
		return nil
	}
	if int(db.Header.EntityOffset) >= len(db.mmapData) {
		return nil
	}
	data := db.mmapData[db.Header.EntityOffset:]
	offset := 0
	for i := uint64(0); i < db.Header.EntityCount; i++ {
		if offset+2 > len(data) {
			break
		}
		nameLen := int(binary.LittleEndian.Uint16(data[offset : offset+2]))
		offset += 2
		if offset+nameLen+2 > len(data) {
			break
		}
		name := string(data[offset : offset+nameLen])
		offset += nameLen

		attrCount := int(binary.LittleEndian.Uint16(data[offset : offset+2]))
		offset += 2

		cr := make(CompactRecord, 0, attrCount)
		for a := 0; a < attrCount; a++ {
			if offset+2 > len(data) {
				break
			}
			keyLen := int(binary.LittleEndian.Uint16(data[offset : offset+2]))
			offset += 2
			if offset+keyLen+4 > len(data) {
				break
			}
			key := db.internAttr(string(data[offset : offset+keyLen]))
			offset += keyLen
			valBits := binary.LittleEndian.Uint32(data[offset : offset+4])
			offset += 4
			cr = append(cr, AttrEntry{Key: key, Value: math.Float32frombits(valBits)})
		}
		db.EntityStore[name] = cr
		db.EntityOrder = append(db.EntityOrder, name)
	}
	return nil
}

// AppendNode adds a new node record into AnchorDB.
func (db *AnchorDB) AppendNode(word string, coord spatial.Point2D, roles [4]float32, domainID uint16) (uint64, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	// If already exists, return existing
	if idx, exists := db.Lexicon[word]; exists {
		return db.getNode(idx).ID, nil
	}

	newID := uint64(db.totalNodes() + 1)
	wordBytes := []byte(word)
	wordOffset := uint32(len(db.stringPool))
	db.stringPool = append(db.stringPool, wordBytes...)

	rec := BinaryNodeRecord{
		ID:         newID,
		X:          coord.X,
		Y:          coord.Y,
		Roles:      roles,
		DomainID:   domainID,
		WordOffset: wordOffset,
		WordLen:    uint16(len(wordBytes)),
	}

	recordIndex := uint32(db.totalNodes())
	db.pendingNodes = append(db.pendingNodes, rec)
	db.Lexicon[word] = recordIndex

	// Insert into grid
	cell := PointToCell(coord, GridDim)
	idx := CellIndex(cell, GridDim)
	db.Grid[uint32(idx)] = append(db.Grid[uint32(idx)], recordIndex)

	db.Header.NodeCount = uint64(db.totalNodes())

	return newID, nil
}

// appendNodeLocked adds a node without acquiring mutex (caller must hold db.mu.Lock)
func (db *AnchorDB) appendNodeLocked(word string, coord spatial.Point2D, roles [4]float32, domainID uint16) uint64 {
	if idx, exists := db.Lexicon[word]; exists {
		return db.getNode(idx).ID
	}

	newID := uint64(db.totalNodes() + 1)
	wordBytes := []byte(word)
	wordOffset := uint32(len(db.stringPool))
	db.stringPool = append(db.stringPool, wordBytes...)

	rec := BinaryNodeRecord{
		ID:         newID,
		X:          coord.X,
		Y:          coord.Y,
		Roles:      roles,
		DomainID:   domainID,
		WordOffset: wordOffset,
		WordLen:    uint16(len(wordBytes)),
	}

	recordIndex := uint32(db.totalNodes())
	db.pendingNodes = append(db.pendingNodes, rec)
	db.Lexicon[word] = recordIndex

	cell := PointToCell(coord, GridDim)
	idx := CellIndex(cell, GridDim)
	db.Grid[uint32(idx)] = append(db.Grid[uint32(idx)], recordIndex)

	db.Header.NodeCount = uint64(db.totalNodes())
	return newID
}

// SaveRecord stores an entity and its raw numeric properties in AnchorDB (SAVING).
// If name is an empty string, a unique UUIDv4 is automatically generated.
func (db *AnchorDB) SaveRecord(name string, props map[string]float32) error {
	_, err := db.SaveRecordWithID(name, props)
	return err
}

// SaveRecordWithID stores an entity and its raw numeric properties, returning the assigned name.
// If name is an empty string or whitespace, a unique UUIDv4 is automatically generated.
func (db *AnchorDB) SaveRecordWithID(name string, props map[string]float32) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		name = uuid.NewString()
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	cr := make(CompactRecord, 0, len(props))
	for k, v := range props {
		cr = append(cr, AttrEntry{Key: db.internAttr(k), Value: v})
	}
	if _, exists := db.EntityStore[name]; !exists {
		db.EntityOrder = append(db.EntityOrder, name)
	}
	db.EntityStore[name] = cr

	// Ensure entity also exists in the geometric graph
	if _, exists := db.Lexicon[name]; !exists {
		angle := float32(len(name)) * 0.5
		coord := spatial.Point2D{
			X: float32(math.Cos(float64(angle))) * 0.5,
			Y: float32(math.Sin(float64(angle))) * 0.5,
		}
		db.appendNodeLocked(name, coord, [4]float32{0.5, 0.5, 0.5, 0.5}, 1)
	}

	return name, nil
}

// GetRecord retrieves stored raw properties of an entity (GETTING), optionally selecting specific fields.
func (db *AnchorDB) GetRecord(name string, selectFields ...string) (map[string]float32, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	cr, exists := db.EntityStore[name]
	if !exists {
		return nil, fmt.Errorf("record not found: %q", name)
	}

	return cr.Project(selectFields), nil
}

// GetRecordWithCompute retrieves raw properties AND computes derived fields in-storage (CALCULATING),
// optionally selecting specific fields (SELECTING / PROJECTION).
// e.g., input {jarak: 100, waktu: 2, luas: 500} -> with Compute("kecepatan", Divide("jarak", "waktu"))
// and selectFields "jarak", "waktu", "kecepatan" -> returns {jarak: 100, waktu: 2, kecepatan: 50} (luas tidak diambil!)
func (db *AnchorDB) GetRecordWithCompute(name string, computations map[string]ComputeOp, selectFields ...string) (map[string]float32, error) {
	// Retrieve raw without projection first so computations have access to all fields
	props, err := db.GetRecord(name)
	if err != nil {
		return nil, err
	}

	// Calculate computed fields on-the-fly in storage
	for outputField, op := range computations {
		val, err := op.Apply(props)
		if err != nil {
			return nil, fmt.Errorf("calculation failed for field %q: %w", outputField, err)
		}
		props[outputField] = val
	}

	return projectProperties(props, selectFields), nil
}

// ListRecordsWithPaging retrieves entities with filtering, on-the-fly computation, and constant-time bounded paging.
func (db *AnchorDB) ListRecordsWithPaging(opts QueryOptions) ([]EntityRecord, bool, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	limit := opts.Limit
	if limit <= 0 {
		limit = 10
	}
	offset := opts.Offset
	if offset < 0 {
		offset = 0
	}

	totalEntities := len(db.EntityOrder)
	if offset >= totalEntities {
		return nil, false, nil
	}

	// FAST PATH: If no filters and no calculations, directly slice in O(1)!
	if len(opts.Filters) == 0 && len(opts.Computations) == 0 {
		end := offset + limit
		hasMore := false
		if end < totalEntities {
			hasMore = true
		} else {
			end = totalEntities
		}

		results := make([]EntityRecord, end-offset)
		for i := offset; i < end; i++ {
			name := db.EntityOrder[i]
			cr := db.EntityStore[name]
			results[i-offset] = EntityRecord{
				Name:  name,
				Props: cr.Project(opts.Select),
			}
		}
		return results, hasMore, nil
	}

	// FILTERED / COMPUTED PATH: Stream sequentially with early termination at limit
	var results []EntityRecord
	skipped := 0
	hasMore := false

	for i := 0; i < totalEntities; i++ {
		name := db.EntityOrder[i]
		cr := db.EntityStore[name]
		props := cr.ToMap()

		// Apply in-storage computations
		calcFailed := false
		for outField, op := range opts.Computations {
			computedVal, err := op.Apply(props)
			if err != nil {
				calcFailed = true
				break
			}
			props[outField] = computedVal
		}
		if calcFailed {
			continue
		}

		// Apply Where conditions (matches both raw and computed fields)
		matches := true
		for _, filter := range opts.Filters {
			if !filter.Matches(props) {
				matches = false
				break
			}
		}
		if !matches {
			continue
		}

		// Handle pagination Offset
		if skipped < offset {
			skipped++
			continue
		}

		// Early Termination: Stop immediately when limit is reached! (Constant-time bound O(K))
		if len(results) >= limit {
			hasMore = true
			break
		}

		// Project to selected columns
		finalProps := projectProperties(props, opts.Select)
		results = append(results, EntityRecord{
			Name:  name,
			Props: finalProps,
		})
	}

	return results, hasMore, nil
}

// UpdateOptions defines criteria and mutations for batch in-storage updates
type UpdateOptions struct {
	TargetEntities []string             // If specified, updates directly via O(1) key access
	Filters        []FilterCondition    // Optional Where conditions
	Mutations      map[string]ComputeOp // e.g., "jarak" -> OpAdd{FieldA: "jarak", FieldB: "10"}
	Limit          int                  // Maximum records to update (Constant-Time Bound O(K))
	Offset         int
}

// UpdateRecords updates entity attributes in-place using in-storage arithmetic mutations with constant-time early exit.
func (db *AnchorDB) UpdateRecords(opts UpdateOptions) (int, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if len(opts.Mutations) == 0 {
		return 0, nil
	}

	updatedCount := 0

	// PATH 1: Targeted direct list update (O(K) constant time)
	if len(opts.TargetEntities) > 0 {
		limit := opts.Limit
		if limit <= 0 || limit > len(opts.TargetEntities) {
			limit = len(opts.TargetEntities)
		}

		for _, name := range opts.TargetEntities {
			if updatedCount >= limit {
				break
			}
			cr, exists := db.EntityStore[name]
			if !exists {
				continue
			}
			props := cr.ToMap()

			// Check filters if any
			matches := true
			for _, f := range opts.Filters {
				if !f.Matches(props) {
					matches = false
					break
				}
			}
			if !matches {
				continue
			}

			// Apply in-place mathematical mutations based on snapshot
			pending := make(map[string]float32, len(opts.Mutations))
			for field, op := range opts.Mutations {
				newVal, err := op.Apply(props)
				if err == nil {
					pending[field] = newVal
				}
			}
			for k, v := range pending {
				cr = cr.Set(db.internAttr(k), v)
			}
			db.EntityStore[name] = cr
			updatedCount++
		}
		return updatedCount, nil
	}

	// PATH 2: Bounded scan with early termination (O(K) constant-time bound)
	limit := opts.Limit
	if limit <= 0 {
		limit = 10000 // default batch limit
	}
	skipped := 0
	offset := opts.Offset

	for _, name := range db.EntityOrder {
		if updatedCount >= limit {
			break // Waktu konstan tercapai: stop setelah Limit terpenuhi!
		}

		cr := db.EntityStore[name]
		props := cr.ToMap()

		// Check filters
		matches := true
		for _, f := range opts.Filters {
			if !f.Matches(props) {
				matches = false
				break
			}
		}
		if !matches {
			continue
		}

		if skipped < offset {
			skipped++
			continue
		}

		// Apply in-place mathematical mutations based on snapshot
		pending := make(map[string]float32, len(opts.Mutations))
		for field, op := range opts.Mutations {
			newVal, err := op.Apply(props)
			if err == nil {
				pending[field] = newVal
			}
		}
		for k, v := range pending {
			cr = cr.Set(db.internAttr(k), v)
		}
		db.EntityStore[name] = cr
		updatedCount++
	}

	return updatedCount, nil
}

// DeleteRecords removes specified entity names from EntityStore, EntityOrder, and graph index.
func (db *AnchorDB) DeleteRecords(names []string) int {
	db.mu.Lock()
	defer db.mu.Unlock()

	toDelete := make(map[string]bool, len(names))
	for _, n := range names {
		toDelete[n] = true
		delete(db.EntityStore, n)

		// Clean up from graph index if present
		if nodeIdx, exists := db.Lexicon[n]; exists {
			delete(db.Lexicon, n)
			db.markNodeDeleted(nodeIdx)
		}
	}

	newOrder := make([]string, 0, len(db.EntityOrder)-len(toDelete))
	deletedCount := 0
	for _, n := range db.EntityOrder {
		if toDelete[n] {
			deletedCount++
		} else {
			newOrder = append(newOrder, n)
		}
	}
	db.EntityOrder = newOrder
	db.Header.EntityCount = uint64(len(db.EntityOrder))
	return deletedCount
}

// Sync flushes all pending memory records to disk.
func (db *AnchorDB) Sync() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Calculate file offsets
	nodesOffset := uint64(HeaderSize)
	totNodes := db.totalNodes()
	nodesSize := uint64(totNodes * RecordSize)
	stringOffset := nodesOffset + nodesSize
	stringSize := uint64(len(db.stringPool))
	entityOffset := stringOffset + stringSize

	db.Header.NodesOffset = nodesOffset
	db.Header.StringOffset = stringOffset
	db.Header.NodeCount = uint64(totNodes)
	db.Header.EntityCount = uint64(len(db.EntityOrder))
	db.Header.EntityOffset = entityOffset

	// Create atomic temp file in same directory (guarantees atomic POSIX rename on same filesystem)
	tempFile, err := os.CreateTemp(filepath.Dir(db.filePath), ".anchordb_sync_*.tmp")
	if err != nil {
		return fmt.Errorf("failed to create temp file for sync: %w", err)
	}
	tempPath := tempFile.Name()
	defer func() {
		if tempFile != nil {
			_ = tempFile.Close()
			_ = os.Remove(tempPath)
		}
	}()

	writer := bufio.NewWriterSize(tempFile, 64*1024)

	// 1. Header (64 bytes)
	var hdrBuf [HeaderSize]byte
	db.Header.Encode(hdrBuf[:])
	if _, err := writer.Write(hdrBuf[:]); err != nil {
		return err
	}

	// 2. Nodes Array (written in 1024-node batches, read safely from db.getNode without unmapping early)
	const nodeBatchSize = 1024
	for i := 0; i < totNodes; i += nodeBatchSize {
		end := i + nodeBatchSize
		if end > totNodes {
			end = totNodes
		}
		batch := make([]byte, (end-i)*RecordSize)
		for j := i; j < end; j++ {
			rec := db.getNode(uint32(j))
			rec.Encode(batch[(j-i)*RecordSize : (j-i+1)*RecordSize])
		}
		if _, err := writer.Write(batch); err != nil {
			return err
		}
	}

	// 3. String Pool (streamed write)
	if len(db.stringPool) > 0 {
		if _, err := writer.Write(db.stringPool); err != nil {
			return err
		}
	}

	// 4. Entity Section (streamed directly to disk buffer without monolithic allocation)
	var lBuf [2]byte
	var vBuf [4]byte
	for _, name := range db.EntityOrder {
		cr := db.EntityStore[name]
		nameBytes := []byte(name)

		// 2 bytes name length + name bytes
		binary.LittleEndian.PutUint16(lBuf[:], uint16(len(nameBytes)))
		if _, err := writer.Write(lBuf[:]); err != nil {
			return err
		}
		if _, err := writer.Write(nameBytes); err != nil {
			return err
		}

		// 2 bytes attr count
		binary.LittleEndian.PutUint16(lBuf[:], uint16(len(cr)))
		if _, err := writer.Write(lBuf[:]); err != nil {
			return err
		}

		for _, attr := range cr {
			kBytes := []byte(attr.Key)
			binary.LittleEndian.PutUint16(lBuf[:], uint16(len(kBytes)))
			if _, err := writer.Write(lBuf[:]); err != nil {
				return err
			}
			if _, err := writer.Write(kBytes); err != nil {
				return err
			}

			binary.LittleEndian.PutUint32(vBuf[:], math.Float32bits(attr.Value))
			if _, err := writer.Write(vBuf[:]); err != nil {
				return err
			}
		}
	}

	if err := writer.Flush(); err != nil {
		return err
	}
	if err := tempFile.Sync(); err != nil {
		return err
	}
	_ = tempFile.Close()

	// Unmap previous file view and close old file descriptor before atomic rename
	if db.isMmapped && db.mmapData != nil {
		_ = syscall.Munmap(db.mmapData)
		db.mmapData = nil
		db.isMmapped = false
	}
	if db.file != nil {
		_ = db.file.Close()
	}

	// Atomic rename replaces the database file cleanly without corruption risk
	if err := os.Rename(tempPath, db.filePath); err != nil {
		return fmt.Errorf("atomic rename failed: %w", err)
	}
	tempFile = nil // Prevent defer from removing the renamed file

	// Re-open newly synced file
	newF, err := os.OpenFile(db.filePath, os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	db.file = newF

	// Re-mmap
	fi, err := db.file.Stat()
	if err != nil {
		return err
	}
	fileSize := fi.Size()
	mmapData, err := syscall.Mmap(int(db.file.Fd()), 0, int(fileSize), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return err
	}
	db.mmapData = mmapData
	db.isMmapped = true
	db.syncedNodeCount = db.Header.NodeCount
	db.pendingNodes = nil

	return nil
}

// ExecuteGraphStepWithOrder runs full internal numerical graph compute in memory with custom ordering:
func (db *AnchorDB) ExecuteGraphStepWithOrder(inputs []string, relation string, limitK int, order OrderByMode) ([]GraphQueryResult, spatial.Point2D, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if len(inputs) == 0 {
		return nil, spatial.Point2D{}, fmt.Errorf("no inputs provided")
	}

	// 1. Calculate Barycenter & Role Deficit from raw node records
	var sumCoord spatial.Point2D
	var sumRoles [4]float32
	inputIndices := make(map[uint32]bool, len(inputs))

	for _, word := range inputs {
		idx, exists := db.Lexicon[word]
		if !exists {
			return nil, spatial.Point2D{}, fmt.Errorf("word not found in anchor db: %q", word)
		}
		rec := db.getNode(idx)
		sumCoord = sumCoord.Add(spatial.Point2D{X: rec.X, Y: rec.Y})
		for r := 0; r < 4; r++ {
			sumRoles[r] += rec.Roles[r]
		}
		inputIndices[idx] = true
	}

	count := float32(len(inputs))
	barycenter := sumCoord.Mul(1.0 / count)

	// Deficit: Target [1.0, 1.0, 1.0, 0.0]
	targetEquilibrium := [4]float32{1.0, 1.0, 1.0, 0.0}
	var deficit [4]float32
	for r := 0; r < 4; r++ {
		d := targetEquilibrium[r] - sumRoles[r]
		if d < 0 {
			d = 0
		}
		deficit[r] = d
	}

	// Get Ray Direction
	shift, hasRel := db.Relations[relation]
	var rayDir spatial.Point2D
	if hasRel {
		rayDir = shift.Normalize()
	} else {
		rayDir = spatial.Point2D{X: 0.0, Y: -1.0}
	}

	ray := spatial.NewRay(barycenter, rayDir)
	beamWidthSq := float32(0.05)

	var candidates []GraphQueryResult
	visitedNodes := make(map[uint32]bool)

	// 2. DDA Ray Marching: Steps cell-by-cell in order of forward distance t
	TraverseRayDDA(ray, GridDim, 2.0, func(cell CellCoord) bool {
		idx := CellIndex(cell, GridDim)
		nodeList := db.Grid[uint32(idx)]

		for _, nodeIdx := range nodeList {
			if inputIndices[nodeIdx] || visitedNodes[nodeIdx] {
				continue
			}
			visitedNodes[nodeIdx] = true

			rec := db.getNode(nodeIdx)
			if rec.ID == 0 {
				continue // tombstone / deleted node
			}
			targetCoord := spatial.Point2D{X: rec.X, Y: rec.Y}

			// In-register Pythagoras
			t, distSqPerp := ray.Project(targetCoord)
			if t <= 0 || distSqPerp > beamWidthSq*4 {
				continue
			}

			// Exponential Wave Absorption
			beamEnergy := ray.BeamEnergy(distSqPerp, beamWidthSq)
			if beamEnergy <= 0.01 {
				continue
			}

			// Role Fit Dot Product
			roleFit := rec.Roles[0]*deficit[0] + rec.Roles[1]*deficit[1] + rec.Roles[2]*deficit[2] + rec.Roles[3]*deficit[3]
			// Distance attenuation along the straight ray beam (closer nodes along t receive beam first)
			finalScore := beamEnergy * (1.0 + 1.5*roleFit) / (1.0 + 0.5*t)

			// Retrieve word from string pool
			word := db.getWord(rec.WordOffset, rec.WordLen)

			candidates = append(candidates, GraphQueryResult{
				Word:       word,
				Coord:      targetCoord,
				T:          t,
				DistSqPerp: distSqPerp,
				Score:      finalScore,
			})
		}

		// Early Termination condition:
		// If we've found sufficient high-scoring candidates in nearby cells, stop!
		if len(candidates) >= limitK*5 {
			return false // BREAK TRAVERSAL! WAKTU KONSTAN TERCAPAI!
		}
		return true
	})

	// Sort top-K according to selected OrderByMode with strict transitive comparison
	switch order {
	case OrderByDistanceAsc:
		slices.SortFunc(candidates, func(a, b GraphQueryResult) int {
			if a.T < b.T {
				return -1
			}
			if a.T > b.T {
				return 1
			}
			return 0
		})
	case OrderByScoreDesc:
		slices.SortFunc(candidates, func(a, b GraphQueryResult) int {
			if a.Score > b.Score {
				return -1
			}
			if a.Score < b.Score {
				return 1
			}
			return 0
		})
	default: // OrderByCompositeDesc: prioritize highest score, then closest distance
		slices.SortFunc(candidates, func(a, b GraphQueryResult) int {
			if a.Score > b.Score {
				return -1
			}
			if a.Score < b.Score {
				return 1
			}
			if a.T < b.T {
				return -1
			}
			if a.T > b.T {
				return 1
			}
			return 0
		})
	}

	limit := limitK
	if limit > len(candidates) {
		limit = len(candidates)
	}

	return candidates[:limit], barycenter, nil
}

// ExecuteGraphStep executes graph query with default OrderByCompositeDesc.
func (db *AnchorDB) ExecuteGraphStep(inputs []string, relation string, limitK int) ([]GraphQueryResult, spatial.Point2D, error) {
	return db.ExecuteGraphStepWithOrder(inputs, relation, limitK, OrderByCompositeDesc)
}

// CalculateDistance computes direct Euclidean spatial distance between two words in-storage.
func (db *AnchorDB) CalculateDistance(wordA, wordB string) (float32, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	idxA, existsA := db.Lexicon[wordA]
	if !existsA {
		return 0, fmt.Errorf("word not found: %q", wordA)
	}
	idxB, existsB := db.Lexicon[wordB]
	if !existsB {
		return 0, fmt.Errorf("word not found: %q", wordB)
	}

	recA := db.getNode(idxA)
	recB := db.getNode(idxB)

	pA := spatial.Point2D{X: recA.X, Y: recA.Y}
	pB := spatial.Point2D{X: recB.X, Y: recB.Y}

	return pA.Distance(pB), nil
}

// CalculateCentroid computes the central gravitational coordinate and average roles for a set of words.
func (db *AnchorDB) CalculateCentroid(words []string) (spatial.Point2D, [4]float32, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if len(words) == 0 {
		return spatial.Point2D{}, [4]float32{}, fmt.Errorf("empty word list")
	}

	var sumCoord spatial.Point2D
	var sumRoles [4]float32

	for _, w := range words {
		idx, exists := db.Lexicon[w]
		if !exists {
			return spatial.Point2D{}, [4]float32{}, fmt.Errorf("word not found: %q", w)
		}
		rec := db.getNode(idx)
		sumCoord = sumCoord.Add(spatial.Point2D{X: rec.X, Y: rec.Y})
		for r := 0; r < 4; r++ {
			sumRoles[r] += rec.Roles[r]
		}
	}

	count := float32(len(words))
	centroid := sumCoord.Mul(1.0 / count)
	avgRoles := [4]float32{
		sumRoles[0] / count,
		sumRoles[1] / count,
		sumRoles[2] / count,
		sumRoles[3] / count,
	}

	return centroid, avgRoles, nil
}

// CalculateCosineSimilarity computes the cosine similarity of semantic roles between two words in-storage.
func (db *AnchorDB) CalculateCosineSimilarity(wordA, wordB string) (float32, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	idxA, existsA := db.Lexicon[wordA]
	if !existsA {
		return 0, fmt.Errorf("word not found: %q", wordA)
	}
	idxB, existsB := db.Lexicon[wordB]
	if !existsB {
		return 0, fmt.Errorf("word not found: %q", wordB)
	}

	rolesA := db.getNode(idxA).Roles
	rolesB := db.getNode(idxB).Roles

	dot := float32(0.0)
	magA := float32(0.0)
	magB := float32(0.0)

	for i := 0; i < 4; i++ {
		dot += rolesA[i] * rolesB[i]
		magA += rolesA[i] * rolesA[i]
		magB += rolesB[i] * rolesB[i]
	}

	if magA == 0 || magB == 0 {
		return 0, nil
	}

	return dot / float32(math.Sqrt(float64(magA))*math.Sqrt(float64(magB))), nil
}

// FindNearestNeighbors finds the K spatially closest concepts to a target word in the manifold.
func (db *AnchorDB) FindNearestNeighbors(word string, limitK int) ([]GraphQueryResult, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	targetIdx, exists := db.Lexicon[word]
	if !exists {
		return nil, fmt.Errorf("word not found: %q", word)
	}

	targetRec := db.getNode(targetIdx)
	targetCoord := spatial.Point2D{X: targetRec.X, Y: targetRec.Y}

	var results []GraphQueryResult

	// Scan through nodes
	totNodes := db.totalNodes()
	for idx := 0; idx < totNodes; idx++ {
		if uint32(idx) == targetIdx {
			continue
		}
		rec := db.getNode(uint32(idx))
		if rec.ID == 0 {
			continue
		}
		p := spatial.Point2D{X: rec.X, Y: rec.Y}
		dist := p.Distance(targetCoord)
		w := db.getWord(rec.WordOffset, rec.WordLen)

		results = append(results, GraphQueryResult{
			Word:       w,
			Coord:      p,
			T:          dist, // store distance in T
			DistSqPerp: p.DistanceSq(targetCoord),
			Score:      1.0 / (1.0 + dist),
		})
	}

	slices.SortFunc(results, func(a, b GraphQueryResult) int {
		if a.T < b.T {
			return -1
		}
		if a.T > b.T {
			return 1
		}
		return 0
	})

	if limitK > len(results) {
		limitK = len(results)
	}
	return results[:limitK], nil
}

func (db *AnchorDB) getWord(offset uint32, length uint16) string {
	if int(offset)+int(length) <= len(db.stringPool) {
		return string(db.stringPool[offset : offset+uint32(length)])
	}
	// Fallback directly to mmap memory if stringPool slice is not yet initialized
	absOffset := int(db.Header.StringOffset) + int(offset)
	if absOffset+int(length) <= len(db.mmapData) {
		return string(db.mmapData[absOffset : absOffset+int(length)])
	}
	return ""
}

// ExecuteGraphChain runs multi-step recursive reasoning entirely in-storage.
func (db *AnchorDB) ExecuteGraphChain(initialInputs []string, relationChain []string, limitK int) ([]GraphQueryResult, error) {
	currentInputs := initialInputs
	var chainResults []GraphQueryResult

	for _, rel := range relationChain {
		results, _, err := db.ExecuteGraphStep(currentInputs, rel, limitK)
		if err != nil {
			return nil, err
		}
		if len(results) == 0 {
			break
		}
		top := results[0]
		chainResults = append(chainResults, top)
		currentInputs = []string{top.Word}
	}

	return chainResults, nil
}

// Close closes the database and releases mmap resources.
func (db *AnchorDB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.isMmapped && db.mmapData != nil {
		_ = syscall.Munmap(db.mmapData)
		db.mmapData = nil
		db.isMmapped = false
	}

	if db.file != nil {
		_ = db.file.Close()
		db.file = nil
	}
	return nil
}
