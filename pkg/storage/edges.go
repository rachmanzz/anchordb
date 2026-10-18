package storage

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"sort"
)

// edgeSectionMagic marks the optional causal-edge section appended after the
// entity section in database format version >= 3.
var edgeSectionMagic = [4]byte{'E', 'D', 'G', '3'}

// EdgeRecord is a directed, weighted relationship between two graph nodes.
// It is the per-edge counterpart to the global relations map: instead of a
// single direction per relation name, each edge names its own Source and Target
// so cause-and-effect links can be stored explicitly (A --causes--> B).
type EdgeRecord struct {
	Source   string  `json:"source"`
	Relation string  `json:"relation"`
	Target   string  `json:"target"`
	Weight   float32 `json:"weight"`
}

// CausalResult is one ranked hop produced by a causal traversal.
type CausalResult struct {
	Word     string  `json:"word"`
	Relation string  `json:"relation"`
	Score    float32 `json:"score"`

	// Deprecated: Use Score instead.
	// Weight is retained only for backward compatibility
	// and will be removed in a future release.
	Weight float32 `json:"weight"`
}

// edgeKey uniquely identifies a directed relation between source and target for O(1) deduplication and updates.
type edgeKey struct {
	Source   string
	Relation string
	Target   string
}

func validateEdge(source, relation, target string, weight float32) error {
	if source == "" {
		return fmt.Errorf("edge source must not be empty")
	}
	if relation == "" {
		return fmt.Errorf("edge relation must not be empty")
	}
	if target == "" {
		return fmt.Errorf("edge target must not be empty")
	}
	if math.IsNaN(float64(weight)) || math.IsInf(float64(weight), 0) {
		return fmt.Errorf("edge weight must be finite, got %v", weight)
	}
	return nil
}

// AddEdge registers or replaces the directed edge source --relation--> target.
// An existing edge with the same (source, relation, target) has its weight
// updated in place, so writing the same edge twice is idempotent.
func (db *AnchorDB) AddEdge(source, relation, target string, weight float32) error {
	if err := validateEdge(source, relation, target, weight); err != nil {
		return err
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrDatabaseClosed
	}
	if db.wal != nil {
		if err := db.wal.WriteEdge(source, relation, target, weight); err != nil {
			return fmt.Errorf("failed to write edge to wal: %w", err)
		}
	}
	db.upsertEdge(source, relation, target, weight)
	return nil
}

// AddEdgesBatch registers or replaces a batch of directed edges under a single lock and WAL flush.
func (db *AnchorDB) AddEdgesBatch(items []EdgeRecord) error {
	if len(items) == 0 {
		return nil
	}
	for _, it := range items {
		if err := validateEdge(it.Source, it.Relation, it.Target, it.Weight); err != nil {
			return err
		}
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrDatabaseClosed
	}

	if db.wal != nil {
		if err := db.wal.WriteEdgeBatch(items); err != nil {
			return fmt.Errorf("failed to write edge batch to wal: %w", err)
		}
	}

	for _, it := range items {
		db.upsertEdge(it.Source, it.Relation, it.Target, it.Weight)
	}
	return nil
}

// upsertEdge is the WAL-free mutation used both by AddEdge (after a successful
// WAL write) and by WAL replay. Caller must hold db.mu.
func (db *AnchorDB) upsertEdge(source, relation, target string, weight float32) {
	key := edgeKey{Source: source, Relation: relation, Target: target}
	if db.edgeIndex != nil {
		if idx, ok := db.edgeIndex[key]; ok {
			db.edges[idx].Weight = weight
			return
		}
	} else {
		for _, i := range db.adjOut[source] {
			e := db.edges[i]
			if e.Source == source && e.Relation == relation && e.Target == target {
				db.edges[i].Weight = weight
				return
			}
		}
	}
	idx := len(db.edges)
	db.edges = append(db.edges, EdgeRecord{Source: source, Relation: relation, Target: target, Weight: weight})
	db.adjOut[source] = append(db.adjOut[source], idx)
	db.adjIn[target] = append(db.adjIn[target], idx)
	if db.edgeIndex == nil {
		db.edgeIndex = make(map[edgeKey]int)
	}
	db.edgeIndex[key] = idx
	db.edgeLive++
}

// edgeCompactionThreshold is the minimum number of tombstoned slots before an
// edge compaction is attempted (compaction also requires tombstones > live).
const edgeCompactionThreshold = 1024

// tombstoneEdgeLocked marks a live edge as deleted. Caller must hold db.mu.
func (db *AnchorDB) tombstoneEdgeLocked(idx int) {
	if idx < 0 || idx >= len(db.edges) || db.edges[idx].Source == "" {
		return
	}
	if db.edgeIndex != nil {
		delete(db.edgeIndex, edgeKey{Source: db.edges[idx].Source, Relation: db.edges[idx].Relation, Target: db.edges[idx].Target})
	}
	db.edges[idx] = EdgeRecord{}
	db.edgeLive--
	db.edgeTombstones++
}

// maybeCompactEdgesLocked rewrites the edge slice when tombstones dominate,
// reclaiming memory that would otherwise grow unbounded across add/remove
// cycles. Caller must hold db.mu.
func (db *AnchorDB) maybeCompactEdgesLocked() {
	if db.edgeTombstones < edgeCompactionThreshold || db.edgeTombstones <= db.edgeLive {
		return
	}
	live := make([]EdgeRecord, 0, db.edgeLive)
	out := make(map[string][]int)
	in := make(map[string][]int)
	idxMap := make(map[edgeKey]int, db.edgeLive)
	for _, e := range db.edges {
		if e.Source == "" {
			continue
		}
		i := len(live)
		live = append(live, e)
		out[e.Source] = append(out[e.Source], i)
		in[e.Target] = append(in[e.Target], i)
		idxMap[edgeKey{Source: e.Source, Relation: e.Relation, Target: e.Target}] = i
	}
	db.edges = live
	db.adjOut = out
	db.adjIn = in
	db.edgeIndex = idxMap
	db.edgeTombstones = 0
}

// RemoveEdge deletes the directed edge source --relation--> target.
func (db *AnchorDB) RemoveEdge(source, relation, target string) error {
	if source == "" || relation == "" || target == "" {
		return fmt.Errorf("edge source, relation and target must not be empty")
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrDatabaseClosed
	}
	idx := db.findEdge(source, relation, target)
	if idx < 0 {
		return fmt.Errorf("edge not found: %s -[%s]-> %s", source, relation, target)
	}
	if db.wal != nil {
		if err := db.wal.WriteEdgeDelete(source, relation, target); err != nil {
			return fmt.Errorf("failed to write edge delete to wal: %w", err)
		}
	}
	db.tombstoneEdgeLocked(idx)
	db.maybeCompactEdgesLocked()
	return nil
}

// findEdge returns the index of a live edge or -1. Caller must hold db.mu.
func (db *AnchorDB) findEdge(source, relation, target string) int {
	if db.edgeIndex != nil {
		if idx, ok := db.edgeIndex[edgeKey{Source: source, Relation: relation, Target: target}]; ok {
			return idx
		}
		return -1
	}
	for _, i := range db.adjOut[source] {
		e := db.edges[i]
		if e.Source == source && e.Relation == relation && e.Target == target {
			return i
		}
	}
	return -1
}

// OutgoingEdges returns the live edges leaving source. If relation is empty,
// every relation is returned.
func (db *AnchorDB) OutgoingEdges(source, relation string) []EdgeRecord {
	db.mu.RLock()
	defer db.mu.RUnlock()
	var out []EdgeRecord
	for _, i := range db.adjOut[source] {
		e := db.edges[i]
		if e.Source == "" {
			continue
		}
		if relation != "" && e.Relation != relation {
			continue
		}
		out = append(out, e)
	}
	return out
}

// IncomingEdges returns the live edges arriving at target. If relation is
// empty, every relation is returned.
func (db *AnchorDB) IncomingEdges(target, relation string) []EdgeRecord {
	db.mu.RLock()
	defer db.mu.RUnlock()
	var out []EdgeRecord
	for _, i := range db.adjIn[target] {
		e := db.edges[i]
		if e.Source == "" {
			continue
		}
		if relation != "" && e.Relation != relation {
			continue
		}
		out = append(out, e)
	}
	return out
}

// EdgeCount returns the number of live edges.
func (db *AnchorDB) EdgeCount() int {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.edgeLive
}

// ExecuteCausalStep follows a single relation from a set of seed words and
// returns the reachable targets ranked by edge weight (descending). Duplicate
// targets are collapsed to their highest-weight incoming edge.
func (db *AnchorDB) ExecuteCausalStep(ctx context.Context, seeds []string, relation string, limitK int) ([]CausalResult, error) {
	if limitK < 0 {
		return nil, fmt.Errorf("limitK must not be negative")
	}
	if relation == "" {
		return nil, fmt.Errorf("relation must not be empty")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	best := make(map[string]CausalResult)
	for _, seed := range seeds {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		for _, i := range db.adjOut[seed] {
			e := db.edges[i]
			if e.Source == "" || e.Relation != relation {
				continue
			}
			if cur, ok := best[e.Target]; !ok || e.Weight > cur.Score {
				best[e.Target] = CausalResult{Word: e.Target, Relation: e.Relation, Score: e.Weight, Weight: e.Weight}
			}
		}
	}
	return rankCausal(best, limitK), nil
}

// ExecuteCausalChain follows a sequence of relations, multiplying the edge
// weights along each path. The frontier is bounded to reachable words and
// duplicate paths collapse to the highest score.
func (db *AnchorDB) ExecuteCausalChain(ctx context.Context, seeds []string, relations []string, limitK int) ([]CausalResult, error) {
	if limitK < 0 {
		return nil, fmt.Errorf("limitK must not be negative")
	}
	if len(relations) == 0 {
		return nil, fmt.Errorf("relations must not be empty")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	db.mu.RLock()
	defer db.mu.RUnlock()

	frontier := make(map[string]float32, len(seeds))
	for _, s := range seeds {
		frontier[s] = 1.0
	}
	var last map[string]float32
	for _, rel := range relations {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		next := make(map[string]float32)
		for word, incoming := range frontier {
			for _, i := range db.adjOut[word] {
				e := db.edges[i]
				if e.Source == "" || e.Relation != rel {
					continue
				}
				score := incoming * e.Weight
				if cur, ok := next[e.Target]; !ok || score > cur {
					next[e.Target] = score
				}
			}
		}
		last = next
		frontier = next
		if len(frontier) == 0 {
			break
		}
	}
	best := make(map[string]CausalResult, len(last))
	for word, score := range last {
		best[word] = CausalResult{Word: word, Relation: relations[len(relations)-1], Score: score, Weight: score}
	}
	return rankCausal(best, limitK), nil

}

func rankCausal(best map[string]CausalResult, limitK int) []CausalResult {
	out := make([]CausalResult, 0, len(best))
	for _, r := range best {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Score != out[j].Score {
			return out[i].Score > out[j].Score
		}
		return out[i].Word < out[j].Word
	})
	if limitK < len(out) {
		out = out[:limitK]
	}
	return out
}

// --- Persistence -----------------------------------------------------------

// encodeEdge writes one edge into buf and returns the extended buffer.
func encodeEdge(buf []byte, e EdgeRecord) ([]byte, error) {
	var err error
	if buf, err = appendWALString(buf, e.Source); err != nil {
		return nil, err
	}
	if buf, err = appendWALString(buf, e.Relation); err != nil {
		return nil, err
	}
	if buf, err = appendWALString(buf, e.Target); err != nil {
		return nil, err
	}
	var wb [4]byte
	binary.LittleEndian.PutUint32(wb[:], math.Float32bits(e.Weight))
	return append(buf, wb[:]...), nil
}

// decodeEdge parses one edge from the front of data, returning the record and
// the number of bytes consumed.
func decodeEdge(data []byte) (EdgeRecord, int, error) {
	src, off, err := readWALString(data, 0)
	if err != nil {
		return EdgeRecord{}, 0, err
	}
	rel, off, err := readWALString(data, off)
	if err != nil {
		return EdgeRecord{}, 0, err
	}
	tgt, off, err := readWALString(data, off)
	if err != nil {
		return EdgeRecord{}, 0, err
	}
	if off+4 > len(data) {
		return EdgeRecord{}, 0, fmt.Errorf("truncated edge weight")
	}
	weight := math.Float32frombits(binary.LittleEndian.Uint32(data[off : off+4]))
	return EdgeRecord{Source: src, Relation: rel, Target: tgt, Weight: weight}, off + 4, nil
}

// loadEdges parses the optional edge section. It is a no-op for format
// versions below 3, so v1/v2 files remain fully supported.
func (db *AnchorDB) loadEdges() error {
	if db.Header.Version < 3 {
		return nil
	}
	start := db.entitySectionEnd
	if start < 0 || start > len(db.mmapData) {
		return fmt.Errorf("corrupt file: invalid entity section end %d", start)
	}
	rest := db.mmapData[start:]
	if len(rest) == 0 {
		return nil
	}
	if len(rest) < 8 {
		return fmt.Errorf("corrupt file: truncated edge section header")
	}
	if !bytes.Equal(rest[0:4], edgeSectionMagic[:]) {
		return fmt.Errorf("corrupt file: missing edge section magic")
	}
	count := int(binary.LittleEndian.Uint32(rest[4:8]))
	offset := 8
	for i := 0; i < count; i++ {
		e, n, err := decodeEdge(rest[offset:])
		if err != nil {
			return fmt.Errorf("corrupt file: edge %d: %w", i, err)
		}
		offset += n
		idx := len(db.edges)
		db.edges = append(db.edges, e)
		db.adjOut[e.Source] = append(db.adjOut[e.Source], idx)
		db.adjIn[e.Target] = append(db.adjIn[e.Target], idx)
		if db.edgeIndex == nil {
			db.edgeIndex = make(map[edgeKey]int)
		}
		db.edgeIndex[edgeKey{Source: e.Source, Relation: e.Relation, Target: e.Target}] = idx
		db.edgeLive++
	}
	if offset != len(rest) {
		return fmt.Errorf("corrupt file: %d trailing bytes after edge section", len(rest)-offset)
	}
	return nil
}

// writeEdgeSection writes the edge section trailer into w for Sync.
func writeEdgeSection(w *bufio.Writer, edges []EdgeRecord) error {
	if _, err := w.Write(edgeSectionMagic[:]); err != nil {
		return err
	}
	live := 0
	for _, e := range edges {
		if e.Source != "" {
			live++
		}
	}
	var cb [4]byte
	binary.LittleEndian.PutUint32(cb[:], uint32(live))
	if _, err := w.Write(cb[:]); err != nil {
		return err
	}
	var buf []byte
	var err error
	for _, e := range edges {
		if e.Source == "" {
			continue
		}
		buf, err = encodeEdge(buf[:0], e)
		if err != nil {
			return err
		}
		if _, err := w.Write(buf); err != nil {
			return err
		}
	}
	return nil
}
