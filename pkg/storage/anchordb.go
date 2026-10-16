package storage

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/rachmanzz/anchordb/pkg/spatial"
)

// ErrDatabaseClosed is returned by mutations and Sync after Close, so a write
// can never be acknowledged without WAL durability.
var ErrDatabaseClosed = errors.New("anchordb: database is closed")

// ErrRecordLocked indicates the record is in an in-flight partition operation.
var ErrRecordLocked = errors.New("anchordb: record is locked for partitioning")

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

// Operation lifecycle states
const (
	OpStatusSafeDelete uint8 = 0 // In-flight safe-delete / pending archive / locked from mutation
	OpStatusActive     uint8 = 1 // Normal active record (default)
	OpStatusTombstone  uint8 = 2 // Marked for background garbage collector
	OpStatusRetry      uint8 = 3 // Execution failed or interrupted by crash/shutdown; pending retry
)

// AttrEntry stores a single numeric attribute key-value pair in a compact flat slice
type AttrEntry struct {
	Key   string
	Value float32
}

// CompactRecord stores properties and operation lifecycle state in a compact structure
type CompactRecord struct {
	Operation uint8
	Attrs     []AttrEntry
}

func (cr CompactRecord) Get(key string) (float32, bool) {
	for i := range cr.Attrs {
		if cr.Attrs[i].Key == key {
			return cr.Attrs[i].Value, true
		}
	}
	return 0, false
}

func (cr CompactRecord) Set(key string, val float32) CompactRecord {
	for i := range cr.Attrs {
		if cr.Attrs[i].Key == key {
			cr.Attrs[i].Value = val
			return cr
		}
	}
	cr.Attrs = append(cr.Attrs, AttrEntry{Key: key, Value: val})
	return cr
}

func (cr CompactRecord) ToMap() map[string]float32 {
	m := make(map[string]float32, len(cr.Attrs))
	for i := range cr.Attrs {
		m[cr.Attrs[i].Key] = cr.Attrs[i].Value
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

// MatchesFilter evaluates a FilterCondition directly on CompactRecord without allocating a map on the heap.
func (cr CompactRecord) MatchesFilter(f FilterCondition) bool {
	val, exists := cr.Get(f.Field)
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

// AnchorDB is the computational spatial graph storage engine.
type AnchorDB struct {
	mu     sync.RWMutex
	syncMu sync.Mutex
	wal    *WALManager
	closed bool // guarded by mu; set once by Close

	file      *os.File
	lockFile  *os.File
	mmapData  []byte
	isMmapped bool

	Header Header

	// Spatial Grid in RAM for ultra-fast pointer lookup: sparse cell mapping (0 overhead for empty cells)
	Grid map[uint32][]uint32

	// Lexicon dictionary: word -> node record index
	Lexicon map[string]uint32

	// relations maps relation names to query direction vectors. Guarded by mu.
	relations map[string]spatial.Point2D
	// partitionLocks distinguishes active archive reservations from legacy
	// lifecycle records that happen to carry OpStatusSafeDelete.
	partitionLocks map[string]bool

	// Zero-copy mmap node tracking:
	// Synced nodes are read directly from mmapData without heap allocation.
	// Pending nodes are newly appended nodes in memory awaiting the next Sync().
	syncedNodeCount uint64
	pendingNodes    []BinaryNodeRecord

	// deletedNodeIdx records node indices tombstoned since the last Sync, so a
	// deletion that races with Sync's unlocked snapshot/remap window can be
	// re-applied to the freshly mapped file (otherwise the node is resurrected).
	// Guarded by mu.
	deletedNodeIdx map[uint32]bool

	// Active string pool for newly appended nodes
	stringPool []byte

	// EntityStore stores key-value numerical attributes for entities: entityName -> CompactRecord
	EntityStore map[string]CompactRecord

	// String interning pool for entity attribute keys (e.g. "distance", "duration", "speed")
	attrPool map[string]string

	// EntityOrder maintains deterministic insertion order for constant-time sequential scanning
	EntityOrder []string

	// Causal edges are explicit per-edge relationships (source --relation--> target).
	// edges holds every edge in insertion order; deleted edges are tombstoned with
	// an empty Source. adjOut/adjIn index edges by source/target for O(1) traversal.
	edges  []EdgeRecord
	adjOut map[string][]int
	adjIn  map[string][]int
	// edgeLive counts live edges (O(1) EdgeCount); edgeTombstones counts
	// tombstoned slots pending compaction.
	edgeLive       int
	edgeTombstones int

	// entitySectionEnd is the file offset where the entity section ends (and, for
	// v3 files, where the optional edge section begins). Populated while loading.
	entitySectionEnd int

	filePath string
}

// SetRelation registers or replaces a query direction vector safely.
func (db *AnchorDB) SetRelation(name string, direction spatial.Point2D) error {
	name = strings.TrimSpace(name)
	if name == "" {
		return fmt.Errorf("relation name must not be empty")
	}
	if direction.X == 0 && direction.Y == 0 ||
		math.IsNaN(float64(direction.X)) || math.IsNaN(float64(direction.Y)) ||
		math.IsInf(float64(direction.X), 0) || math.IsInf(float64(direction.Y), 0) {
		return fmt.Errorf("relation %q direction must be finite and non-zero", name)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrDatabaseClosed
	}
	db.relations[name] = direction
	return nil
}

// Relation returns a registered query direction vector.
func (db *AnchorDB) Relation(name string) (spatial.Point2D, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	direction, ok := db.relations[name]
	return direction, ok
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

// QueryOptions defines criteria for List queries with pagination, column selection, and operation filtering
type QueryOptions struct {
	Context              context.Context
	IncludeAllOperations bool     // If true, returns records regardless of operation status (0, 1, 2, 3)
	OperationFilter      *uint8   // If specified, filters specifically by this operation status
	AllowedOperations    []uint8  // If specified, returns records whose operation matches any in this list
	Select               []string // Projection: list of fields to return (empty means return all fields)
	Filters              []FilterCondition
	Computations         map[string]ComputeOp
	Limit                int
	Offset               int
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

// EntityRecord represents a returned entity with properties and operation lifecycle state
type EntityRecord struct {
	Name      string             `json:"name"`
	Operation uint8              `json:"operation"`
	Props     map[string]float32 `json:"props"`
}

func (db *AnchorDB) internAttr(s string) string {
	if interned, exists := db.attrPool[s]; exists {
		return interned
	}
	// Bound the pool: interning only exists to share string headers, so when the
	// pool grows past the cap we rebuild it from the keys still referenced by live
	// records. Without this, applications that use many distinct/dynamic attribute
	// keys would grow the pool without limit.
	if len(db.attrPool) >= maxAttrPoolEntries {
		db.compactAttrPoolLocked()
		if interned, exists := db.attrPool[s]; exists {
			return interned
		}
	}
	db.attrPool[s] = s
	return s
}

// maxAttrPoolEntries is the number of interned attribute keys allowed before the
// pool is compacted down to the keys still referenced by live records.
const maxAttrPoolEntries = 1 << 17 // 131072

// compactAttrPoolLocked rebuilds the interning pool from the keys referenced by
// live records, dropping entries for keys no longer in use. Caller must hold
// db.mu (write lock).
func (db *AnchorDB) compactAttrPoolLocked() {
	if len(db.attrPool) == 0 {
		return
	}
	referenced := make(map[string]string, len(db.attrPool))
	for _, cr := range db.EntityStore {
		for i := range cr.Attrs {
			k := cr.Attrs[i].Key
			referenced[k] = k
		}
	}
	db.attrPool = referenced
}

// CompactAttrPool rebuilds the attribute interning pool, releasing memory held
// for attribute keys that are no longer referenced. It is called automatically
// once the pool exceeds an internal cap; this exported form allows callers to
// trigger it at a convenient time (e.g. after a bulk delete).
func (db *AnchorDB) CompactAttrPool() {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return
	}
	db.compactAttrPoolLocked()
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
	db.zeroNodeLocked(idx)
	if db.deletedNodeIdx == nil {
		db.deletedNodeIdx = make(map[uint32]bool)
	}
	db.deletedNodeIdx[idx] = true
}

// zeroNodeLocked clears the node's ID in the current mapping or pending slice.
// Caller must hold db.mu.
func (db *AnchorDB) zeroNodeLocked(idx uint32) {
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

// reapplyDeletesLocked re-applies node tombstones recorded since the last Sync to
// the current mapping, then clears the record. This closes the race where Sync
// snapshots a live node, a concurrent delete tombstones it, and Sync then swaps
// in the snapshot (which still contains the live node). Caller must hold db.mu.
func (db *AnchorDB) reapplyDeletesLocked() {
	for idx := range db.deletedNodeIdx {
		db.zeroNodeLocked(idx)
	}
	clear(db.deletedNodeIdx)
}

// mmapLen converts a file size to the int length expected by syscall.Mmap,
// rejecting sizes that overflow int on 32-bit platforms (where Go's int is 32
// bits and a >2GiB file cannot be mapped as a single region).
func mmapLen(fileSize int64) (int, error) {
	if fileSize < 0 {
		return 0, fmt.Errorf("invalid negative file size %d", fileSize)
	}
	if fileSize > int64(math.MaxInt) {
		return 0, fmt.Errorf("database file too large for this platform: %d bytes (max %d)", fileSize, int64(math.MaxInt))
	}
	return int(fileSize), nil
}

// Options configures an AnchorDB instance.
type Options struct {
	// WAL controls write-ahead-log durability. The zero value is not valid;
	// use DefaultOptions() or set WAL explicitly.
	WAL WALOptions
}

// DefaultOptions returns production defaults (durable WAL: fsync every write).
func DefaultOptions() Options {
	return Options{WAL: DefaultWALOptions()}
}

// Open creates or opens an AnchorDB file using memory mapping with default options.
func Open(filePath string) (*AnchorDB, error) {
	return OpenWithOptions(filePath, DefaultOptions())
}

// OpenWithOptions creates or opens an AnchorDB file with explicit options.
func OpenWithOptions(filePath string, opts Options) (*AnchorDB, error) {
	db := &AnchorDB{
		Grid:           make(map[uint32][]uint32),
		Lexicon:        make(map[string]uint32),
		relations:      make(map[string]spatial.Point2D),
		partitionLocks: make(map[string]bool),
		EntityStore:    make(map[string]CompactRecord),
		attrPool:       make(map[string]string),
		EntityOrder:    make([]string, 0),
		adjOut:         make(map[string][]int),
		adjIn:          make(map[string][]int),
		filePath:       filePath,
	}

	// Keep the lock on a sidecar rather than the database inode: Sync atomically
	// replaces the database file, while this descriptor remains locked.
	lockFile, err := os.OpenFile(filePath+".lock", os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return nil, fmt.Errorf("failed to open database lock: %w", err)
	}
	if err := syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = lockFile.Close()
		return nil, fmt.Errorf("database is already open: %w", err)
	}
	db.lockFile = lockFile
	openErr := func(err error) (*AnchorDB, error) {
		_ = syscall.Flock(int(lockFile.Fd()), syscall.LOCK_UN)
		_ = lockFile.Close()
		db.lockFile = nil
		return nil, err
	}

	// Check if file exists
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		// Initialize empty database file
		if err := db.initEmptyFile(filePath); err != nil {
			return openErr(fmt.Errorf("failed to init empty anchordb file: %w", err))
		}
	}

	// Open file
	f, err := os.OpenFile(filePath, os.O_RDWR|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return openErr(fmt.Errorf("failed to open file %s: %w", filePath, err))
	}
	db.file = f

	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return openErr(err)
	}

	fileSize := fi.Size()
	if fileSize < HeaderSize {
		_ = f.Close()
		return openErr(fmt.Errorf("corrupt file: size %d smaller than header", fileSize))
	}

	// Memory map the file via Linux mmap
	mmapSize, err := mmapLen(fileSize)
	if err != nil {
		_ = f.Close()
		return openErr(err)
	}
	mmapData, err := syscall.Mmap(int(f.Fd()), 0, mmapSize, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		_ = f.Close()
		return openErr(fmt.Errorf("mmap failed: %w", err))
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

	// Load the optional causal-edge section (format version >= 3)
	if err := db.loadEdges(); err != nil {
		_ = db.Close()
		return nil, err
	}

	// Initialize WAL for append-only durability
	walPath := filePath + ".wal"
	wal, err := OpenWALWithOptions(walPath, opts.WAL)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to open wal: %w", err)
	}
	db.wal = wal

	// Replay WAL for crash recovery (restores mutations lost before the last Sync)
	_, err = db.wal.ReplayEvents(func(ev WALEvent) error {
		switch ev.Op {
		case WalOpSave:
			db.applyWALSaveLocked(ev.Name, ev.Record)
		case WalOpDelete:
			db.applyWALDeleteLocked(ev.Name)
		case WalOpNode:
			db.appendNodeLocked(ev.Word, ev.Coord, ev.Roles, ev.DomainID)
		case WalOpEdge:
			db.upsertEdge(ev.Edge.Source, ev.Edge.Relation, ev.Edge.Target, ev.Edge.Weight)
		case WalOpEdgeDelete:
			if idx := db.findEdge(ev.Edge.Source, ev.Edge.Relation, ev.Edge.Target); idx >= 0 {
				db.tombstoneEdgeLocked(idx)
			}
		}
		return nil
	})
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to replay wal: %w", err)
	}

	// Replayed edge deletions may have left many tombstones; reclaim them once.
	db.maybeCompactEdgesLocked()

	// Reconcile interrupted operations from ungraceful shutdown/crash:
	// Any records trapped in OpStatusSafeDelete (0) are automatically transitioned to OpStatusRetry (3).
	if _, err := db.reconcileInterruptedOperationsLocked(); err != nil {
		_ = db.Close()
		return nil, err
	}

	return db, nil
}

// reconcileInterruptedOperationsLocked detects any records left in OpStatusSafeDelete (0).
// If a record is still in SafeDelete during Open(), it means a previous process crashed or shut down
// before finalizing archival (to Tombstone) or completing rollback.
// It transitions them to OpStatusRetry (3) and records the mutation in WAL.
func (db *AnchorDB) reconcileInterruptedOperationsLocked() (int, error) {
	reconciled := 0
	for _, name := range db.EntityOrder {
		cr, exists := db.EntityStore[name]
		if exists && cr.Operation == OpStatusSafeDelete {
			cr.Operation = OpStatusRetry
			db.EntityStore[name] = cr
			if db.wal != nil {
				if err := db.wal.WriteSave(name, cr); err != nil {
					clear(db.partitionLocks)
					return reconciled, fmt.Errorf("failed to record reconciliation for %q: %w", name, err)
				}
			}
			reconciled++
		}
	}
	clear(db.partitionLocks)
	return reconciled, nil
}

func (db *AnchorDB) applyWALSaveLocked(name string, cr CompactRecord) {
	internedAttrs := make([]AttrEntry, 0, len(cr.Attrs))
	for _, attr := range cr.Attrs {
		internedAttrs = append(internedAttrs, AttrEntry{
			Key:   db.internAttr(attr.Key),
			Value: attr.Value,
		})
	}
	if _, exists := db.EntityStore[name]; !exists {
		db.EntityOrder = append(db.EntityOrder, name)
	}
	db.EntityStore[name] = CompactRecord{
		Operation: cr.Operation,
		Attrs:     internedAttrs,
	}

	// Ensure entity also exists in the geometric graph
	if _, exists := db.Lexicon[name]; !exists {
		angle := float32(len(name)) * 0.5
		coord := spatial.Point2D{
			X: float32(math.Cos(float64(angle))) * 0.5,
			Y: float32(math.Sin(float64(angle))) * 0.5,
		}
		db.appendNodeLocked(name, coord, [4]float32{0.5, 0.5, 0.5, 0.5}, 1)
	}
}

func (db *AnchorDB) applyWALDeleteLocked(name string) {
	delete(db.EntityStore, name)
	if nodeIdx, exists := db.Lexicon[name]; exists {
		delete(db.Lexicon, name)
		db.markNodeDeleted(nodeIdx)
	}
	for i, n := range db.EntityOrder {
		if n == name {
			db.EntityOrder = append(db.EntityOrder[:i], db.EntityOrder[i+1:]...)
			break
		}
	}
}

func (db *AnchorDB) initEmptyFile(filePath string) error {
	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return err
	}

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

	if _, err := f.Write(buf[:]); err != nil {
		_ = f.Close()
		return err
	}
	// Make the header (file data) and the directory entry durable before the
	// database is used. Without this, a crash shortly after first Open() can leave
	// a headerless/truncated file that fails to open permanently.
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return syncDir(filepath.Dir(filePath))
}

func (db *AnchorDB) buildIndex() error {
	fileSize := uint64(len(db.mmapData))
	if db.Header.NodesOffset < HeaderSize || db.Header.NodesOffset > fileSize {
		return fmt.Errorf("corrupt file: invalid node offset %d", db.Header.NodesOffset)
	}
	nodeBytes := db.Header.NodeCount * RecordSize
	if db.Header.NodeCount != 0 && nodeBytes/db.Header.NodeCount != RecordSize {
		return fmt.Errorf("corrupt file: node section size overflow")
	}
	if nodeBytes > fileSize-db.Header.NodesOffset {
		return fmt.Errorf("corrupt file: node section exceeds file size")
	}
	nodeEnd := db.Header.NodesOffset + nodeBytes
	if db.Header.StringOffset < nodeEnd || db.Header.StringOffset > fileSize ||
		db.Header.EntityOffset < db.Header.StringOffset || db.Header.EntityOffset > fileSize {
		return fmt.Errorf("corrupt file: invalid string/entity section offsets")
	}
	if db.Header.Version >= 2 && db.Header.EntityCount > 0 && db.Header.EntityOffset == fileSize {
		return fmt.Errorf("corrupt file: entity section is missing")
	}
	if db.Header.NodeCount > uint64(math.MaxInt) {
		return fmt.Errorf("corrupt file: node count exceeds addressable memory")
	}
	if db.Header.NodeCount > 0 && db.Header.StringOffset == db.Header.EntityOffset {
		return fmt.Errorf("corrupt file: string pool is missing")
	}
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
			return fmt.Errorf("corrupt file: truncated node record %d", i)
		}
		DecodeRecord(db.mmapData[offset:offset+RecordSize], &rec)

		// String lookup
		if uint64(rec.WordOffset)+uint64(rec.WordLen) > db.Header.EntityOffset-db.Header.StringOffset {
			return fmt.Errorf("corrupt file: invalid word range in node %d", i)
		}
		wordOffset := int(db.Header.StringOffset) + int(rec.WordOffset)
		wordLen := int(rec.WordLen)
		word := string(db.mmapData[wordOffset : wordOffset+wordLen])
		db.Lexicon[word] = uint32(i)

		// Insert into spatial grid cell
		p := spatial.Point2D{X: rec.X, Y: rec.Y}
		cell := PointToCell(p, GridDim)
		idx := CellIndex(cell, GridDim)
		db.Grid[uint32(idx)] = append(db.Grid[uint32(idx)], uint32(i))
	}

	return nil
}

func (db *AnchorDB) loadEntities() error {
	db.entitySectionEnd = int(db.Header.EntityOffset)
	if db.Header.EntityCount == 0 || db.Header.EntityOffset == 0 {
		return nil
	}
	data := db.mmapData[db.Header.EntityOffset:]
	offset := 0
	loaded := uint64(0)
	for i := uint64(0); i < db.Header.EntityCount; i++ {
		if offset+2 > len(data) {
			return fmt.Errorf("corrupt file: truncated entity name length at entity %d", i)
		}
		nameLen := int(binary.LittleEndian.Uint16(data[offset : offset+2]))
		offset += 2
		if offset+nameLen > len(data) {
			return fmt.Errorf("corrupt file: truncated entity name at entity %d", i)
		}
		name := string(data[offset : offset+nameLen])
		offset += nameLen

		opStatus := OpStatusActive
		if db.Header.Version >= 2 {
			if offset+1 > len(data) {
				return fmt.Errorf("corrupt file: truncated operation status at entity %d", i)
			}
			opStatus = data[offset]
			offset += 1
		}

		if offset+2 > len(data) {
			return fmt.Errorf("corrupt file: truncated attribute count at entity %d", i)
		}
		attrCount := int(binary.LittleEndian.Uint16(data[offset : offset+2]))
		offset += 2

		attrs := make([]AttrEntry, 0, attrCount)
		for a := 0; a < attrCount; a++ {
			if offset+2 > len(data) {
				return fmt.Errorf("corrupt file: truncated attribute key length at entity %d", i)
			}
			keyLen := int(binary.LittleEndian.Uint16(data[offset : offset+2]))
			offset += 2
			if offset+keyLen+4 > len(data) {
				return fmt.Errorf("corrupt file: truncated attribute value at entity %d", i)
			}
			key := db.internAttr(string(data[offset : offset+keyLen]))
			offset += keyLen
			valBits := binary.LittleEndian.Uint32(data[offset : offset+4])
			offset += 4
			attrs = append(attrs, AttrEntry{Key: key, Value: math.Float32frombits(valBits)})
		}
		db.EntityStore[name] = CompactRecord{Operation: opStatus, Attrs: attrs}
		db.EntityOrder = append(db.EntityOrder, name)
		loaded++
	}
	if loaded != db.Header.EntityCount {
		return fmt.Errorf("corrupt file: loaded %d of %d entities", loaded, db.Header.EntityCount)
	}
	// For v3 the edge section (if any) follows the entity section, so trailing
	// bytes are expected. For v1/v2 the entity section must run to EOF.
	db.entitySectionEnd = int(db.Header.EntityOffset) + offset
	if db.Header.Version < 3 && offset != len(data) {
		return fmt.Errorf("corrupt file: %d trailing bytes after entity section", len(data)-offset)
	}
	return nil
}

// AppendNode adds a new node record into AnchorDB.
func (db *AnchorDB) AppendNode(word string, coord spatial.Point2D, roles [4]float32, domainID uint16) (uint64, error) {
	if err := validateNodeGeometry(coord, roles); err != nil {
		return 0, err
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return 0, ErrDatabaseClosed
	}

	// If already exists, return existing
	if idx, exists := db.Lexicon[word]; exists {
		return db.getNode(idx).ID, nil
	}
	if db.wal != nil {
		if err := db.wal.WriteNode(word, coord, roles, domainID); err != nil {
			return 0, fmt.Errorf("failed to write node %q to wal: %w", word, err)
		}
	}
	return db.appendNodeLocked(word, coord, roles, domainID), nil
}

// validateNodeGeometry rejects non-finite coordinates and role weights. Such
// values would otherwise poison spatial lookups (int(NaN) yields an
// implementation-defined cell index) and downstream score arithmetic.
func validateNodeGeometry(coord spatial.Point2D, roles [4]float32) error {
	if math.IsNaN(float64(coord.X)) || math.IsInf(float64(coord.X), 0) ||
		math.IsNaN(float64(coord.Y)) || math.IsInf(float64(coord.Y), 0) {
		return fmt.Errorf("node coordinate must be finite, got (%v, %v)", coord.X, coord.Y)
	}
	for i, r := range roles {
		if math.IsNaN(float64(r)) || math.IsInf(float64(r), 0) {
			return fmt.Errorf("node role[%d] must be finite, got %v", i, r)
		}
	}
	return nil
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

// SaveRecord stores an entity and its raw numeric properties in AnchorDB with default OpStatusActive (1).
// If name is an empty string, a unique UUIDv4 is automatically generated.
func (db *AnchorDB) SaveRecord(name string, props map[string]float32) error {
	_, err := db.SaveRecordWithID(name, props)
	return err
}

// SaveRecordWithIDAndOp stores an entity with an explicit operation lifecycle state.
func (db *AnchorDB) SaveRecordWithIDAndOp(name string, opStatus uint8, props map[string]float32) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		name = uuid.NewString()
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return "", ErrDatabaseClosed
	}
	if opStatus == OpStatusSafeDelete {
		return "", fmt.Errorf("cannot save record with internal partition-lock status")
	}
	if db.partitionLocks[name] {
		return "", fmt.Errorf("%w: %q", ErrRecordLocked, name)
	}

	attrs := make([]AttrEntry, 0, len(props))
	for k, v := range props {
		attrs = append(attrs, AttrEntry{Key: db.internAttr(k), Value: v})
	}
	cr := CompactRecord{
		Operation: opStatus,
		Attrs:     attrs,
	}

	// Persist to append-only WAL before applying to in-memory store
	if db.wal != nil {
		if err := db.wal.WriteSave(name, cr); err != nil {
			return "", fmt.Errorf("failed to write to wal: %w", err)
		}
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

// SaveRecordWithID stores an entity with default OpStatusActive (1).
func (db *AnchorDB) SaveRecordWithID(name string, props map[string]float32) (string, error) {
	return db.SaveRecordWithIDAndOp(name, OpStatusActive, props)
}

// GetRecord retrieves stored raw properties of an entity (GETTING), optionally selecting specific fields.
// Returns error if entity does not exist or has been marked as OpStatusTombstone (2).
func (db *AnchorDB) GetRecord(name string, selectFields ...string) (map[string]float32, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return nil, ErrDatabaseClosed
	}

	cr, exists := db.EntityStore[name]
	if !exists || cr.Operation == OpStatusTombstone {
		return nil, fmt.Errorf("record not found: %q", name)
	}

	return cr.Project(selectFields), nil
}

// GetRecordWithOperation retrieves stored properties along with the operation lifecycle state.
func (db *AnchorDB) GetRecordWithOperation(name string, selectFields ...string) (map[string]float32, uint8, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return nil, 0, ErrDatabaseClosed
	}

	cr, exists := db.EntityStore[name]
	if !exists || cr.Operation == OpStatusTombstone {
		return nil, 0, fmt.Errorf("record not found: %q", name)
	}

	return cr.Project(selectFields), cr.Operation, nil
}

// GetRecordWithCompute retrieves raw properties AND computes derived fields in-storage (CALCULATING),
// optionally selecting specific fields (SELECTING / PROJECTION).
// e.g., input {distance: 100, duration: 2, area: 500} -> with Compute("speed", Divide("distance", "duration"))
// and selectFields "distance", "duration", "speed" -> returns {distance: 100, duration: 2, speed: 50} (area is not returned!)
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

	// Stream sequentially with Operation lifecycle filtering and early termination at limit
	var results []EntityRecord
	skipped := 0
	hasMore := false

	for i := 0; i < totalEntities; i++ {
		if opts.Context != nil {
			if err := opts.Context.Err(); err != nil {
				return nil, false, err
			}
		}
		name := db.EntityOrder[i]
		cr := db.EntityStore[name]

		// Filter by Operation lifecycle state
		if len(opts.AllowedOperations) > 0 {
			matchedOp := false
			for _, allowed := range opts.AllowedOperations {
				if cr.Operation == allowed {
					matchedOp = true
					break
				}
			}
			if !matchedOp {
				continue
			}
		} else if opts.OperationFilter != nil {
			if cr.Operation != *opts.OperationFilter {
				continue
			}
		} else if !opts.IncludeAllOperations {
			// By default, only return active records (Operation == OpStatusActive)
			if cr.Operation != OpStatusActive {
				continue
			}
		}

		// Fast filter check before heap allocation if no computations
		if len(opts.Computations) == 0 && len(opts.Filters) > 0 {
			matches := true
			for _, filter := range opts.Filters {
				if !cr.MatchesFilter(filter) {
					matches = false
					break
				}
			}
			if !matches {
				continue
			}
		}

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

		// Apply Where conditions if computations were present
		if len(opts.Computations) > 0 {
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
			Name:      name,
			Operation: cr.Operation,
			Props:     finalProps,
		})
	}

	return results, hasMore, nil
}

// UpdateOptions defines criteria and mutations for batch in-storage updates
type UpdateOptions struct {
	Context        context.Context
	TargetEntities []string             // If specified, updates directly via O(1) key access
	Filters        []FilterCondition    // Optional Where conditions
	Mutations      map[string]ComputeOp // e.g., "distance" -> OpAdd{FieldA: "distance", FieldB: "10"}
	Limit          int                  // Maximum records to update (Constant-Time Bound O(K))
	Offset         int
}

// UpdateRecords updates entity attributes in-place using in-storage arithmetic mutations with constant-time early exit.
// If an error occurs after one or more records have been committed, the returned
// count reports those committed records; batch updates are not transactional.
func (db *AnchorDB) UpdateRecords(opts UpdateOptions) (int, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return 0, ErrDatabaseClosed
	}

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
		offset := opts.Offset
		if offset < 0 {
			offset = 0
		}
		skipped := 0

		for _, name := range opts.TargetEntities {
			if opts.Context != nil {
				if err := opts.Context.Err(); err != nil {
					return updatedCount, err
				}
			}
			if updatedCount >= limit {
				break
			}
			cr, exists := db.EntityStore[name]
			if !exists || cr.Operation != OpStatusActive {
				continue
			}

			// Check filters without allocating map on heap
			matches := true
			for _, f := range opts.Filters {
				if !cr.MatchesFilter(f) {
					matches = false
					break
				}
			}
			if !matches {
				continue
			}

			// Offset counts matching records, mirroring the scan path below.
			if skipped < offset {
				skipped++
				continue
			}

			props := cr.ToMap()

			// Apply in-place mathematical mutations based on snapshot
			pending := make(map[string]float32, len(opts.Mutations))
			for field, op := range opts.Mutations {
				newVal, err := op.Apply(props)
				if err != nil {
					return updatedCount, fmt.Errorf("update %q field %q: %w", name, field, err)
				}
				pending[field] = newVal
			}
			for k, v := range pending {
				cr = cr.Set(db.internAttr(k), v)
			}
			if db.wal != nil {
				if err := db.wal.WriteSave(name, cr); err != nil {
					return updatedCount, fmt.Errorf("failed to write update for %q to wal: %w", name, err)
				}
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
		if opts.Context != nil {
			if err := opts.Context.Err(); err != nil {
				return updatedCount, err
			}
		}
		if updatedCount >= limit {
			break // Constant-time bound reached: stop once Limit is satisfied!
		}

		cr := db.EntityStore[name]
		if cr.Operation != OpStatusActive {
			continue
		}

		// Check filters without allocating map on heap
		matches := true
		for _, f := range opts.Filters {
			if !cr.MatchesFilter(f) {
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

		props := cr.ToMap()

		// Apply in-place mathematical mutations based on snapshot
		pending := make(map[string]float32, len(opts.Mutations))
		for field, op := range opts.Mutations {
			newVal, err := op.Apply(props)
			if err != nil {
				return updatedCount, fmt.Errorf("update %q field %q: %w", name, field, err)
			}
			pending[field] = newVal
		}
		for k, v := range pending {
			cr = cr.Set(db.internAttr(k), v)
		}
		if db.wal != nil {
			if err := db.wal.WriteSave(name, cr); err != nil {
				return updatedCount, fmt.Errorf("failed to write update for %q to wal: %w", name, err)
			}
		}
		db.EntityStore[name] = cr
		updatedCount++
	}

	return updatedCount, nil
}

// SetOperationStatus updates the operation lifecycle state for the specified entity names.
// Persists the changes to WAL and updates EntityStore. Returns the number of entities updated.
func (db *AnchorDB) SetOperationStatus(names []string, status uint8) (int, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return 0, ErrDatabaseClosed
	}

	updated := 0
	for _, name := range names {
		cr, exists := db.EntityStore[name]
		if !exists {
			continue
		}
		if cr.Operation == status {
			continue
		}
		if db.partitionLocks[name] || status == OpStatusSafeDelete && cr.Operation == OpStatusSafeDelete {
			return updated, fmt.Errorf("%w: use partition locking APIs to change this state for %q", ErrRecordLocked, name)
		}
		if db.wal != nil {
			updatedRecord := cr
			updatedRecord.Operation = status
			if err := db.wal.WriteSave(name, updatedRecord); err != nil {
				return updated, fmt.Errorf("failed to write operation status for %q to wal: %w", name, err)
			}
		}
		cr.Operation = status
		db.EntityStore[name] = cr
		updated++
	}
	return updated, nil
}

// CompletePartitionRecords transitions records held in SafeDelete after the
// partition snapshot has either been archived or must be released for retry.
func (db *AnchorDB) CompletePartitionRecords(names []string, status uint8) (int, error) {
	if status != OpStatusActive && status != OpStatusTombstone && status != OpStatusRetry {
		return 0, fmt.Errorf("invalid partition completion status %d", status)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return 0, ErrDatabaseClosed
	}
	updated := 0
	for _, name := range names {
		cr, exists := db.EntityStore[name]
		if !exists || cr.Operation != OpStatusSafeDelete || !db.partitionLocks[name] {
			continue
		}
		cr.Operation = status
		if db.wal != nil {
			if err := db.wal.WriteSave(name, cr); err != nil {
				return updated, fmt.Errorf("failed to complete partition state for %q: %w", name, err)
			}
		}
		db.EntityStore[name] = cr
		delete(db.partitionLocks, name)
		updated++
	}
	return updated, nil
}

// LockPartitionRecords atomically snapshots eligible records and marks them
// SafeDelete. The caller may then archive the returned immutable property maps
// without racing a concurrent update.
func (db *AnchorDB) LockPartitionRecords(dateField string, cutoff float32, limit int) ([]EntityRecord, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return nil, ErrDatabaseClosed
	}
	if dateField == "" {
		return nil, fmt.Errorf("partition date field must not be empty")
	}
	if math.IsNaN(float64(cutoff)) || math.IsInf(float64(cutoff), 0) {
		return nil, fmt.Errorf("partition cutoff must be finite")
	}
	if limit <= 0 {
		return nil, fmt.Errorf("partition record limit must be greater than zero")
	}

	records := make([]EntityRecord, 0)
	for _, name := range db.EntityOrder {
		cr, exists := db.EntityStore[name]
		if !exists || (cr.Operation != OpStatusActive && cr.Operation != OpStatusRetry) {
			continue
		}
		date, exists := cr.Get(dateField)
		if !exists || date >= cutoff {
			continue
		}
		if len(records) >= limit {
			break
		}
		records = append(records, EntityRecord{
			Name:      name,
			Operation: cr.Operation,
			Props:     cr.ToMap(),
		})
	}
	if len(records) == 0 {
		return records, nil
	}

	// Persist each lifecycle transition before exposing it in memory. If a later
	// WAL write fails, roll the already locked prefix back to Retry where possible.
	locked := make([]EntityRecord, 0, len(records))
	for _, record := range records {
		cr := db.EntityStore[record.Name]
		updated := cr
		updated.Operation = OpStatusSafeDelete
		if db.wal != nil {
			if err := db.wal.WriteSave(record.Name, updated); err != nil {
				var rollbackErrs []error
				for _, written := range locked {
					current := db.EntityStore[written.Name]
					current.Operation = OpStatusRetry
					if db.wal != nil {
						if rollbackErr := db.wal.WriteSave(written.Name, current); rollbackErr != nil {
							rollbackErrs = append(rollbackErrs, fmt.Errorf("restore %q to retry: %w", written.Name, rollbackErr))
							continue
						}
					}
					db.EntityStore[written.Name] = current
					delete(db.partitionLocks, written.Name)
				}
				return nil, errors.Join(fmt.Errorf("failed to persist partition lock for %q: %w", record.Name, err), errors.Join(rollbackErrs...))
			}
		}
		db.EntityStore[record.Name] = updated
		db.partitionLocks[record.Name] = true
		locked = append(locked, record)
	}
	return records, nil
}

// CollectTombstones purges up to maxBatch entities marked with OpStatusTombstone (2).
// It returns the number deleted and any WAL persistence error.
func (db *AnchorDB) CollectTombstones(maxBatch int) (int, error) {
	if maxBatch <= 0 {
		maxBatch = 1000
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return 0, ErrDatabaseClosed
	}

	var toPurge []string
	for _, name := range db.EntityOrder {
		if cr, exists := db.EntityStore[name]; exists && cr.Operation == OpStatusTombstone {
			toPurge = append(toPurge, name)
			if len(toPurge) >= maxBatch {
				break
			}
		}
	}

	if len(toPurge) == 0 {
		return 0, nil
	}

	purgedSet := make(map[string]bool, len(toPurge))
	purged := 0
	for _, n := range toPurge {
		if db.wal != nil {
			if err := db.wal.WriteDelete(n); err != nil {
				db.deleteEntityLocked(purgedSet)
				db.removeEntityNamesLocked(purgedSet)
				return purged, fmt.Errorf("failed to write tombstone deletion for %q to wal: %w", n, err)
			}
		}
		purgedSet[n] = true
		purged++
	}
	db.deleteEntityLocked(purgedSet)
	db.removeEntityNamesLocked(purgedSet)
	return purged, nil
}

// DeleteRecords removes specified entity names from EntityStore, EntityOrder, and graph index.
// It returns the number deleted and any WAL persistence error.
func (db *AnchorDB) DeleteRecords(names []string) (int, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return 0, ErrDatabaseClosed
	}
	for _, name := range names {
		if db.partitionLocks[name] {
			return 0, fmt.Errorf("%w: %q", ErrRecordLocked, name)
		}
	}

	toDelete := make(map[string]bool, len(names))
	for _, n := range names {
		toDelete[n] = true
	}
	deleteNames := make([]string, 0, len(toDelete))
	for n := range toDelete {
		deleteNames = append(deleteNames, n)
	}
	sort.Strings(deleteNames)
	persistedDelete := make(map[string]bool, len(toDelete))
	for _, n := range deleteNames {
		if db.wal != nil {
			if err := db.wal.WriteDelete(n); err != nil {
				db.deleteEntityLocked(persistedDelete)
				deleted := db.removeEntityNamesLocked(persistedDelete)
				return deleted, fmt.Errorf("failed to write deletion for %q to wal: %w", n, err)
			}
		}
		persistedDelete[n] = true
	}
	db.deleteEntityLocked(persistedDelete)
	deletedCount := db.removeEntityNamesLocked(persistedDelete)
	return deletedCount, nil
}

func (db *AnchorDB) deleteEntityLocked(names map[string]bool) {
	for name := range names {
		delete(db.EntityStore, name)
		if nodeIdx, exists := db.Lexicon[name]; exists {
			delete(db.Lexicon, name)
			db.markNodeDeleted(nodeIdx)
		}
	}
}

// removeEntityNamesLocked filters persisted deletions from the ordered entity
// index and updates the on-disk header count. Caller must hold db.mu.Lock.
func (db *AnchorDB) removeEntityNamesLocked(deleted map[string]bool) int {
	if len(deleted) == 0 {
		return 0
	}
	newOrder := make([]string, 0, len(db.EntityOrder))
	removed := 0
	for _, name := range db.EntityOrder {
		if deleted[name] {
			removed++
			continue
		}
		newOrder = append(newOrder, name)
	}
	db.EntityOrder = newOrder
	db.Header.EntityCount = uint64(len(newOrder))
	return removed
}

// Sync flushes pending records to disk and truncates the WAL log up to the snapshot point.
// Read and write queries are NOT blocked during disk serialization and kernel fsync.
func (db *AnchorDB) Sync() error {
	db.syncMu.Lock()
	defer db.syncMu.Unlock()

	// 1. Take in-memory snapshot under brief write lock (microsecond latency)
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return ErrDatabaseClosed
	}
	totNodes := db.totalNodes()
	nodesSnapshot := make([]BinaryNodeRecord, totNodes)
	for j := 0; j < totNodes; j++ {
		nodesSnapshot[j] = db.getNode(uint32(j))
	}
	stringPoolSnapshot := make([]byte, len(db.stringPool))
	copy(stringPoolSnapshot, db.stringPool)

	entityOrderSnapshot := make([]string, len(db.EntityOrder))
	copy(entityOrderSnapshot, db.EntityOrder)

	entityStoreSnapshot := make(map[string]CompactRecord, len(db.EntityStore))
	for _, name := range entityOrderSnapshot {
		cr := db.EntityStore[name]
		attrsCopy := make([]AttrEntry, len(cr.Attrs))
		copy(attrsCopy, cr.Attrs)
		entityStoreSnapshot[name] = CompactRecord{
			Operation: cr.Operation,
			Attrs:     attrsCopy,
		}
	}

	edgesSnapshot := make([]EdgeRecord, len(db.edges))
	copy(edgesSnapshot, db.edges)

	hdrSnapshot := db.Header
	hdrSnapshot.Version = DBVersion // Ensure synced header carries the current format version
	nodesOffset := uint64(HeaderSize)
	nodesSize := uint64(totNodes * RecordSize)
	stringOffset := nodesOffset + nodesSize
	stringSize := uint64(len(stringPoolSnapshot))
	entityOffset := stringOffset + stringSize

	hdrSnapshot.NodesOffset = nodesOffset
	hdrSnapshot.StringOffset = stringOffset
	hdrSnapshot.NodeCount = uint64(totNodes)
	hdrSnapshot.EntityCount = uint64(len(entityOrderSnapshot))
	hdrSnapshot.EntityOffset = entityOffset

	var walCheckpointOffset int64
	if db.wal != nil {
		var err error
		walCheckpointOffset, err = db.wal.CurrentOffset()
		if err != nil {
			db.mu.Unlock()
			return fmt.Errorf("failed to capture wal checkpoint offset: %w", err)
		}
	}
	db.mu.Unlock() // UNLOCK: Read/Write queries are free to proceed while disk I/O runs!

	// 2. Stream snapshot to temp file and fsync - TOTALLY UNLOCKED!
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
	hdrSnapshot.Encode(hdrBuf[:])
	if _, err := writer.Write(hdrBuf[:]); err != nil {
		return err
	}

	// 2. Nodes Array (written in 1024-node batches from snapshot)
	const nodeBatchSize = 1024
	for i := 0; i < totNodes; i += nodeBatchSize {
		end := i + nodeBatchSize
		if end > totNodes {
			end = totNodes
		}
		batch := make([]byte, (end-i)*RecordSize)
		for j := i; j < end; j++ {
			nodesSnapshot[j].Encode(batch[(j-i)*RecordSize : (j-i+1)*RecordSize])
		}
		if _, err := writer.Write(batch); err != nil {
			return err
		}
	}

	// 3. String Pool (streamed write from snapshot)
	if len(stringPoolSnapshot) > 0 {
		if _, err := writer.Write(stringPoolSnapshot); err != nil {
			return err
		}
	}

	// 4. Entity Section (streamed directly to disk buffer from snapshot)
	var lBuf [2]byte
	var vBuf [4]byte
	var opBuf [1]byte
	for _, name := range entityOrderSnapshot {
		cr := entityStoreSnapshot[name]
		nameBytes := []byte(name)

		// The binary format stores lengths as uint16; reject oversized values
		// instead of silently truncating them into a corrupt file.
		if len(nameBytes) > math.MaxUint16 {
			return fmt.Errorf("entity name %q too long (%d bytes) for binary format", name, len(nameBytes))
		}
		if len(cr.Attrs) > math.MaxUint16 {
			return fmt.Errorf("entity %q has too many attributes (%d) for binary format", name, len(cr.Attrs))
		}

		// 2 bytes name length + name bytes
		binary.LittleEndian.PutUint16(lBuf[:], uint16(len(nameBytes)))
		if _, err := writer.Write(lBuf[:]); err != nil {
			return err
		}
		if _, err := writer.Write(nameBytes); err != nil {
			return err
		}

		// 1 byte operation status (DBVersion >= 2)
		opBuf[0] = cr.Operation
		if _, err := writer.Write(opBuf[:]); err != nil {
			return err
		}

		// 2 bytes attr count
		binary.LittleEndian.PutUint16(lBuf[:], uint16(len(cr.Attrs)))
		if _, err := writer.Write(lBuf[:]); err != nil {
			return err
		}

		for _, attr := range cr.Attrs {
			kBytes := []byte(attr.Key)
			if len(kBytes) > math.MaxUint16 {
				return fmt.Errorf("attribute key %q too long (%d bytes) for entity %q", attr.Key, len(kBytes), name)
			}
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

	// 5. Causal-edge section (format version >= 3) follows the entity section.
	if err := writeEdgeSection(writer, edgesSnapshot); err != nil {
		return err
	}

	if err := writer.Flush(); err != nil {
		return err
	}
	if err := tempFile.Sync(); err != nil {
		return err
	}
	_ = tempFile.Close()

	// 3. Briefly acquire db.mu.Lock only for atomic file swap and WAL checkpointing
	db.mu.Lock()
	defer db.mu.Unlock()

	// Atomic rename replaces the database file cleanly without corruption risk
	if err := os.Rename(tempPath, db.filePath); err != nil {
		return fmt.Errorf("atomic rename failed: %w", err)
	}
	tempFile = nil // Prevent defer from removing the renamed file

	// Fsync the parent directory so the rename is committed to filesystem
	// metadata. If this fails we must NOT checkpoint the WAL below: a crash could
	// otherwise roll the directory back to the pre-rename file while the WAL that
	// carried the snapshot's mutations has already been truncated (silent data loss).
	dirSyncErr := syncDir(filepath.Dir(db.filePath))

	// Re-open newly synced file
	newF, err := os.OpenFile(db.filePath, os.O_RDWR, 0644)
	if err != nil {
		return err
	}

	// Re-mmap
	fi, err := newF.Stat()
	if err != nil {
		_ = newF.Close()
		return err
	}
	fileSize := fi.Size()
	mmapSize, err := mmapLen(fileSize)
	if err != nil {
		_ = newF.Close()
		return err
	}
	mmapData, err := syscall.Mmap(int(newF.Fd()), 0, mmapSize, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		_ = newF.Close()
		return err
	}
	oldFile, oldMmap, oldMapped := db.file, db.mmapData, db.isMmapped
	if oldMapped && oldMmap != nil {
		if err := syscall.Munmap(oldMmap); err != nil {
			_ = syscall.Munmap(mmapData)
			_ = newF.Close()
			return fmt.Errorf("release previous database mapping: %w", err)
		}
	}
	db.file = newF
	db.mmapData = mmapData
	db.isMmapped = true
	db.Header = hdrSnapshot
	if oldFile != nil {
		if err := oldFile.Close(); err != nil {
			return fmt.Errorf("close previous database file: %w", err)
		}
	}

	// Prune pending nodes that were captured in snapshot
	if uint64(len(db.pendingNodes)) >= (uint64(totNodes) - db.syncedNodeCount) {
		addedSince := uint64(totNodes) - db.syncedNodeCount
		db.pendingNodes = db.pendingNodes[addedSince:]
	} else {
		db.pendingNodes = nil
	}
	db.syncedNodeCount = uint64(totNodes)

	// Re-apply tombstones recorded while db.mu was released during serialization
	// so a delete that raced this Sync is not lost in the freshly mapped file.
	db.reapplyDeletesLocked()

	// Do not discard WAL entries until the replacement file is durable. If the
	// directory fsync failed, keep the WAL so a crash can still replay mutations.
	if dirSyncErr != nil {
		return fmt.Errorf("database replaced but directory fsync failed; WAL retained for recovery: %w", dirSyncErr)
	}

	// Checkpoint WAL up to the snapshot offset
	if db.wal != nil {
		if err := db.wal.Checkpoint(walCheckpointOffset); err != nil {
			return fmt.Errorf("failed to checkpoint wal: %w", err)
		}
	}

	return nil
}

// ExecuteGraphStepWithOrder runs full internal numerical graph compute in memory with custom ordering:
func (db *AnchorDB) ExecuteGraphStepWithOrder(inputs []string, relation string, limitK int, order OrderByMode) ([]GraphQueryResult, spatial.Point2D, error) {
	return db.ExecuteGraphStepWithOptions(inputs, relation, limitK, order, 0.05)
}

// ExecuteGraphStepWithOptions executes a spatial graph step with an explicit
// beam width. Existing ExecuteGraphStep APIs retain the historical 0.05 width.
func (db *AnchorDB) ExecuteGraphStepWithOptions(inputs []string, relation string, limitK int, order OrderByMode, beamWidth float32) ([]GraphQueryResult, spatial.Point2D, error) {
	return db.ExecuteGraphStepWithContext(context.Background(), inputs, relation, limitK, order, beamWidth)
}

// ExecuteGraphStepWithContext is ExecuteGraphStepWithOptions with cancellation
// checks during spatial traversal.
func (db *AnchorDB) ExecuteGraphStepWithContext(ctx context.Context, inputs []string, relation string, limitK int, order OrderByMode, beamWidth float32) ([]GraphQueryResult, spatial.Point2D, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return nil, spatial.Point2D{}, ErrDatabaseClosed
	}

	shift, hasRel := db.relations[relation]
	if !hasRel {
		return nil, spatial.Point2D{}, fmt.Errorf("unknown relation %q", relation)
	}
	if len(inputs) == 0 {
		return nil, spatial.Point2D{}, fmt.Errorf("no inputs provided")
	}
	if limitK < 0 {
		return nil, spatial.Point2D{}, fmt.Errorf("limitK must not be negative")
	}
	if beamWidth <= 0 || math.IsNaN(float64(beamWidth)) || math.IsInf(float64(beamWidth), 0) {
		return nil, spatial.Point2D{}, fmt.Errorf("beam width must be finite and greater than zero")
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
	rayDir := shift.Normalize()

	ray := spatial.NewRay(barycenter, rayDir)
	beamWidthSq := beamWidth * beamWidth
	cellSize := 2.0 / float32(GridDim)
	cellRadius := int(math.Ceil(float64(2 * beamWidth / cellSize)))
	if cellRadius > GridDim {
		cellRadius = GridDim
	}

	var candidates []GraphQueryResult
	visitedNodes := make(map[uint32]bool)
	visitedCells := make(map[int]bool)
	var contextErr error

	// 2. DDA Ray Marching: Steps cell-by-cell in order of forward distance t
	TraverseRayDDA(ray, GridDim, 2.0, func(cell CellCoord) bool {
		if err := ctx.Err(); err != nil {
			contextErr = err
			return false
		}
		minX, maxX := max(0, cell.X-cellRadius), min(GridDim-1, cell.X+cellRadius)
		minY, maxY := max(0, cell.Y-cellRadius), min(GridDim-1, cell.Y+cellRadius)
		for y := minY; y <= maxY; y++ {
			for x := minX; x <= maxX; x++ {
				cellIndex := CellIndex(CellCoord{X: x, Y: y}, GridDim)
				if visitedCells[cellIndex] {
					continue
				}
				visitedCells[cellIndex] = true
				nodeList := db.Grid[uint32(cellIndex)]
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
			}
		}

		return true
	})
	if contextErr != nil {
		return nil, spatial.Point2D{}, contextErr
	}

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
	return db.ExecuteGraphStepWithOptions(inputs, relation, limitK, OrderByCompositeDesc, 0.05)
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
	if limitK < 0 {
		return nil, fmt.Errorf("limitK must not be negative")
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return nil, ErrDatabaseClosed
	}

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
	return db.ExecuteGraphChainWithOptions(initialInputs, relationChain, limitK, 0.05)
}

// ExecuteGraphChainWithOptions runs each graph step with the configured beam width.
func (db *AnchorDB) ExecuteGraphChainWithOptions(initialInputs []string, relationChain []string, limitK int, beamWidth float32) ([]GraphQueryResult, error) {
	return db.ExecuteGraphChainWithContext(context.Background(), initialInputs, relationChain, limitK, beamWidth)
}

// ExecuteGraphChainWithContext runs a chain while observing cancellation between
// graph steps and during each step's traversal.
func (db *AnchorDB) ExecuteGraphChainWithContext(ctx context.Context, initialInputs []string, relationChain []string, limitK int, beamWidth float32) ([]GraphQueryResult, error) {
	if limitK < 0 {
		return nil, fmt.Errorf("limitK must not be negative")
	}
	if beamWidth <= 0 || math.IsNaN(float64(beamWidth)) || math.IsInf(float64(beamWidth), 0) {
		return nil, fmt.Errorf("beam width must be finite and greater than zero")
	}
	currentInputs := initialInputs
	var chainResults []GraphQueryResult

	for _, rel := range relationChain {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		results, _, err := db.ExecuteGraphStepWithContext(ctx, currentInputs, rel, limitK, OrderByCompositeDesc, beamWidth)
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

// Close closes the database and releases mmap and WAL resources. It is idempotent
// and mutually exclusive with Sync (it waits for an in-flight Sync to finish), so a
// Sync can never resurrect a closed database. After Close, mutations and Sync fail
// with ErrDatabaseClosed instead of silently succeeding without WAL durability.
func (db *AnchorDB) Close() error {
	db.syncMu.Lock()
	defer db.syncMu.Unlock()
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return nil
	}
	db.closed = true

	if db.wal != nil {
		_ = db.wal.Close()
		db.wal = nil
	}

	if db.isMmapped && db.mmapData != nil {
		_ = syscall.Munmap(db.mmapData)
		db.mmapData = nil
		db.isMmapped = false
	}

	if db.file != nil {
		_ = db.file.Close()
		db.file = nil
	}
	if db.lockFile != nil {
		_ = syscall.Flock(int(db.lockFile.Fd()), syscall.LOCK_UN)
		_ = db.lockFile.Close()
		db.lockFile = nil
	}
	return nil
}
