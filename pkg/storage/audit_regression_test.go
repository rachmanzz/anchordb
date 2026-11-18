package storage

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/rachmanzz/anchordb/pkg/spatial"
)

// Closing a WAL concurrently (or twice) must not panic with "close of closed
// channel"; Close is documented as idempotent and concurrency-safe.
func TestWALManagerCloseIsConcurrencySafe(t *testing.T) {
	wal, err := OpenWAL(filepath.Join(t.TempDir(), "concurrent.wal"))
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = wal.Close()
		}()
	}
	wg.Wait()

	// A subsequent sequential close must remain a harmless no-op.
	if err := wal.Close(); err != nil {
		t.Fatalf("second Close returned error: %v", err)
	}
}

// After Close, read APIs must surface ErrDatabaseClosed rather than silently
// returning zeroed/garbage records from a released mapping.
func TestReadsAfterCloseReturnErrDatabaseClosed(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "closed.anchordb"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := db.SaveRecordWithIDAndOp("alice", OpStatusActive, map[string]float32{"x": 1}); err != nil {
		t.Fatalf("save: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if _, err := db.GetRecord("alice"); err != ErrDatabaseClosed {
		t.Fatalf("GetRecord after close = %v, want ErrDatabaseClosed", err)
	}
	if _, _, err := db.GetRecordWithOperation("alice"); err != ErrDatabaseClosed {
		t.Fatalf("GetRecordWithOperation after close = %v, want ErrDatabaseClosed", err)
	}
	if _, err := db.FindNearestNeighbors("alice", 1); err != ErrDatabaseClosed {
		t.Fatalf("FindNearestNeighbors after close = %v, want ErrDatabaseClosed", err)
	}
}

// A delete that races with Sync's unlocked snapshot/remap window must not be
// lost when the stale snapshot (still holding the live node) is installed.
func TestSyncTombstoneReapplySurvivesStaleSnapshot(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "tomb.anchordb"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if err := db.SaveRecord("ghost", map[string]float32{"v": 1}); err != nil {
		t.Fatalf("save: %v", err)
	}
	if err := db.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}

	db.mu.RLock()
	rec, rErr := db.getNode(0)
	_, present := db.Lexicon["ghost"]
	db.mu.RUnlock()
	if rErr != nil || rec.ID == 0 || !present {
		t.Fatalf("precondition failed: node not persisted (id=%d present=%v err=%v)", rec.ID, present, rErr)
	}

	// Deleting records a tombstone for node 0 and zeroes it in the mapping.
	if _, err := db.DeleteRecords([]string{"ghost"}); err != nil {
		t.Fatalf("delete: %v", err)
	}

	// Simulate Sync installing a stale snapshot that still contains the live node.
	db.mu.Lock()
	offset := int(db.Header.NodesOffset)
	rec.Encode(db.mmapData[offset : offset+RecordSize])
	if checkRec, _ := db.getNode(0); checkRec.ID == 0 {
		db.mu.Unlock()
		t.Fatal("simulation setup failed: expected a live node in the mapping")
	}
	db.reapplyDeletesLocked()
	resurrectedRec, _ := db.getNode(0)
	resurrected := resurrectedRec.ID
	db.mu.Unlock()

	if resurrected != 0 {
		t.Fatalf("deleted node resurrected after snapshot remap: ID=%d", resurrected)
	}
}

func TestStorage_GraphAndMathMissingWordWrapsErrRecordNotFound(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "missing_words.anchordb"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if err := db.SaveRecord("present", map[string]float32{"v": 1}); err != nil {
		t.Fatalf("SaveRecord: %v", err)
	}
	if err := db.SetRelation("rel", spatial.Point2D{X: 1, Y: 0}); err != nil {
		t.Fatalf("SetRelation: %v", err)
	}

	if _, _, err := db.ExecuteGraphStepWithContext(context.Background(), []string{"missing"}, "rel", 5, OrderByCompositeDesc, 0.05); !errors.Is(err, ErrRecordNotFound) {
		t.Fatalf("ExecuteGraphStepWithContext missing word err = %v, want ErrRecordNotFound", err)
	}
	if _, err := db.ExecuteGraphChainWithContext(context.Background(), []string{"missing"}, []string{"rel"}, 5, 0.05); !errors.Is(err, ErrRecordNotFound) {
		t.Fatalf("ExecuteGraphChainWithContext missing word err = %v, want ErrRecordNotFound", err)
	}
	if _, err := db.CalculateDistance("missing", "present"); !errors.Is(err, ErrRecordNotFound) {
		t.Fatalf("CalculateDistance missing wordA err = %v, want ErrRecordNotFound", err)
	}
	if _, err := db.CalculateDistance("present", "missing"); !errors.Is(err, ErrRecordNotFound) {
		t.Fatalf("CalculateDistance missing wordB err = %v, want ErrRecordNotFound", err)
	}
	if _, err := db.CalculateHaversineDistance("missing", "present"); !errors.Is(err, ErrRecordNotFound) {
		t.Fatalf("CalculateHaversineDistance missing wordA err = %v, want ErrRecordNotFound", err)
	}
	if _, err := db.CalculateHaversineDistance("present", "missing"); !errors.Is(err, ErrRecordNotFound) {
		t.Fatalf("CalculateHaversineDistance missing wordB err = %v, want ErrRecordNotFound", err)
	}
	if _, _, err := db.CalculateCentroid([]string{"present", "missing"}); !errors.Is(err, ErrRecordNotFound) {
		t.Fatalf("CalculateCentroid missing word err = %v, want ErrRecordNotFound", err)
	}
	if _, err := db.CalculateCosineSimilarity("missing", "present"); !errors.Is(err, ErrRecordNotFound) {
		t.Fatalf("CalculateCosineSimilarity missing wordA err = %v, want ErrRecordNotFound", err)
	}
	if _, err := db.CalculateCosineSimilarity("present", "missing"); !errors.Is(err, ErrRecordNotFound) {
		t.Fatalf("CalculateCosineSimilarity missing wordB err = %v, want ErrRecordNotFound", err)
	}
	if _, err := db.FindNearestNeighbors("missing", 3); !errors.Is(err, ErrRecordNotFound) {
		t.Fatalf("FindNearestNeighbors missing word err = %v, want ErrRecordNotFound", err)
	}
}

func TestStorage_CompactRecordLifetimeSurvivesSyncDeleteAndClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lifetime.anchordb")
	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	if _, err := db.SaveRecordWithIDAndOp("k1", OpStatusActive, map[string]float32{"score": 42.5}); err != nil {
		t.Fatalf("save: %v", err)
	}
	if err := db.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}

	cr, err := db.GetRecordTyped("k1")
	if err != nil {
		t.Fatalf("GetRecordTyped: %v", err)
	}

	// Mutate, sync, delete, and close underlying DB while holding cr.
	if _, err := db.DeleteRecords([]string{"k1"}); err != nil {
		t.Fatalf("DeleteRecords: %v", err)
	}
	if err := db.Sync(); err != nil {
		t.Fatalf("sync2: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	var keys []string
	if err := cr.ForEachAttr(func(k string, v float32) bool {
		if v != 42.5 {
			t.Fatalf("unexpected value %v", v)
		}
		keys = append(keys, strings.Clone(k))
		return true
	}); err != nil {
		t.Fatalf("ForEachAttr after DB Close: %v", err)
	}
	if len(keys) != 1 || keys[0] != "score" {
		t.Fatalf("unexpected keys after DB Close: %v", keys)
	}

	var typedKeys []string
	if err := cr.ForEachAttrTyped(func(attr AttrEntry) bool {
		typedKeys = append(typedKeys, strings.Clone(attr.Key))
		return true
	}); err != nil {
		t.Fatalf("ForEachAttrTyped after DB Close: %v", err)
	}
	if len(typedKeys) != 1 || typedKeys[0] != "score" {
		t.Fatalf("unexpected typed keys after DB Close: %v", typedKeys)
	}
}
