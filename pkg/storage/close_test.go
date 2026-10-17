package storage

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func openFDCount(t *testing.T) int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Skipf("/proc/self/fd unavailable: %v", err)
	}
	return len(entries)
}

// L4: Sync after Close must not resurrect the DB or leak a file descriptor, and
// mutations after Close must fail loudly instead of "succeeding" without WAL.
func TestClosedDBRejectsSyncAndMutations(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "closed.anchordb"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := db.SaveRecordWithID("a", map[string]float32{"x": 1}); err != nil {
		t.Fatalf("save: %v", err)
	}
	if err := db.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	before := openFDCount(t)

	if err := db.Sync(); !errors.Is(err, ErrDatabaseClosed) {
		t.Fatalf("Sync after Close: want ErrDatabaseClosed, got %v", err)
	}
	if _, err := db.SaveRecordWithID("b", map[string]float32{"x": 2}); !errors.Is(err, ErrDatabaseClosed) {
		t.Fatalf("Save after Close: want ErrDatabaseClosed, got %v", err)
	}
	if _, err := db.UpdateRecords(UpdateOptions{Mutations: map[string]ComputeOp{"x": OpAdd{FieldA: "x", FieldB: "1"}}}); !errors.Is(err, ErrDatabaseClosed) {
		t.Fatalf("Update after Close: want ErrDatabaseClosed, got %v", err)
	}
	if n, err := db.DeleteRecords([]string{"a"}); n != 0 || !errors.Is(err, ErrDatabaseClosed) {
		t.Fatalf("Delete after Close should report ErrDatabaseClosed, deleted %d: %v", n, err)
	}
	if n, err := db.CollectTombstones(10); n != 0 || !errors.Is(err, ErrDatabaseClosed) {
		t.Fatalf("CollectTombstones after Close should report ErrDatabaseClosed, got %d: %v", n, err)
	}
	if n, err := db.SetOperationStatus([]string{"a"}, OpStatusTombstone); n != 0 || !errors.Is(err, ErrDatabaseClosed) {
		t.Fatalf("SetOperationStatus after Close should be a no-op, got %d", n)
	}

	if after := openFDCount(t); after > before {
		t.Fatalf("fd leak after rejected Sync: before=%d after=%d", before, after)
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "idem.anchordb"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for i := 0; i < 3; i++ {
		if err := db.Close(); err != nil {
			t.Fatalf("close #%d: %v", i, err)
		}
	}
}

func TestCloseReportsBackgroundWALError(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "sync-error.anchordb"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	want := errors.New("background fsync failed")
	db.wal.mu.Lock()
	db.wal.syncErr = want
	db.wal.mu.Unlock()

	if err := db.Close(); !errors.Is(err, want) {
		t.Fatalf("close error = %v, want background sync error", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
}

// Close must wait for an in-flight Sync (they are mutually exclusive) so Sync
// can never write into a DB that is being torn down. Run under -race.
func TestConcurrentSyncAndClose(t *testing.T) {
	for i := 0; i < 20; i++ {
		db, err := Open(filepath.Join(t.TempDir(), "race.anchordb"))
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		for j := 0; j < 50; j++ {
			_, _ = db.SaveRecordWithID("", map[string]float32{"v": float32(j)})
		}

		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); _ = db.Sync() }()
		go func() { defer wg.Done(); _ = db.Close() }()
		wg.Wait()

		if err := db.Sync(); !errors.Is(err, ErrDatabaseClosed) {
			t.Fatalf("iter %d: Sync after Close: want ErrDatabaseClosed, got %v", i, err)
		}
	}
}
