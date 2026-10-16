package storage

import (
	"path/filepath"
	"sync"
	"testing"
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
	rec := db.getNode(0)
	_, present := db.Lexicon["ghost"]
	db.mu.RUnlock()
	if rec.ID == 0 || !present {
		t.Fatalf("precondition failed: node not persisted (id=%d present=%v)", rec.ID, present)
	}

	// Deleting records a tombstone for node 0 and zeroes it in the mapping.
	if _, err := db.DeleteRecords([]string{"ghost"}); err != nil {
		t.Fatalf("delete: %v", err)
	}

	// Simulate Sync installing a stale snapshot that still contains the live node.
	db.mu.Lock()
	offset := int(db.Header.NodesOffset)
	rec.Encode(db.mmapData[offset : offset+RecordSize])
	if db.getNode(0).ID == 0 {
		db.mu.Unlock()
		t.Fatal("simulation setup failed: expected a live node in the mapping")
	}
	db.reapplyDeletesLocked()
	resurrected := db.getNode(0).ID
	db.mu.Unlock()

	if resurrected != 0 {
		t.Fatalf("deleted node resurrected after snapshot remap: ID=%d", resurrected)
	}
}
