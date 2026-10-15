package storage

import (
	"fmt"
	"path/filepath"
	"testing"
)

// TestEdgeCompactionReclaimsMemory ensures that repeated add/remove cycles do not
// grow the internal edge slice without bound.
func TestEdgeCompactionReclaimsMemory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "compact.anchordb")
	// SyncNone keeps the test fast; durability is covered elsewhere.
	db, err := OpenWithOptions(path, Options{WAL: WALOptions{SyncMode: SyncNone}})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	const n = 3000
	for i := 0; i < n; i++ {
		if err := db.AddEdge("s", "r", fmt.Sprintf("t%d", i), 1); err != nil {
			t.Fatalf("add edge %d: %v", i, err)
		}
	}
	if got := db.EdgeCount(); got != n {
		t.Fatalf("EdgeCount after adds = %d, want %d", got, n)
	}

	// Remove all but the last 10 edges.
	for i := 0; i < n-10; i++ {
		if err := db.RemoveEdge("s", "r", fmt.Sprintf("t%d", i)); err != nil {
			t.Fatalf("remove edge %d: %v", i, err)
		}
	}
	if got := db.EdgeCount(); got != 10 {
		t.Fatalf("EdgeCount after removes = %d, want 10", got)
	}

	db.mu.RLock()
	slots := len(db.edges)
	live := db.edgeLive
	db.mu.RUnlock()
	// Invariant: with compaction, tombstones never exceed the threshold while
	// also exceeding the live count, so slots <= live + threshold.
	if slots > live+edgeCompactionThreshold {
		t.Fatalf("edge slice was not compacted: %d slots for %d live edges", slots, live)
	}
	if slots > n/2 {
		t.Fatalf("edge slice grew too large: %d slots (started from %d adds)", slots, n)
	}

	// The 10 survivors must remain queryable.
	if got := db.OutgoingEdges("s", "r"); len(got) != 10 {
		t.Fatalf("OutgoingEdges after compaction = %d, want 10", len(got))
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Reopen: only the 10 live edges should be persisted.
	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	if got := reopened.EdgeCount(); got != 10 {
		t.Fatalf("EdgeCount after reopen = %d, want 10", got)
	}
}

// TestMmapLenGuard verifies that sizes overflowing int are rejected.
func TestMmapLenGuard(t *testing.T) {
	if _, err := mmapLen(-1); err == nil {
		t.Fatal("expected error for negative size")
	}
	if got, err := mmapLen(4096); err != nil || got != 4096 {
		t.Fatalf("mmapLen(4096) = %d, %v", got, err)
	}
}
