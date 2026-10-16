package storage

import (
	"path/filepath"
	"testing"
	"time"
)

// TestOpenWithOptionsSyncModes verifies that every WAL durability mode still
// persists mutations (via WAL replay on reopen) and that the option plumbing
// works. Durability under power loss is not simulated here.
func TestOpenWithOptionsSyncModes(t *testing.T) {
	modes := []struct {
		name string
		mode SyncMode
	}{
		{"always", SyncAlways},
		{"interval", SyncInterval},
		{"none", SyncNone},
	}

	for _, tc := range modes {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "mode.anchordb")
			db, err := OpenWithOptions(path, Options{WAL: WALOptions{
				SyncMode:     tc.mode,
				SyncInterval: 10 * time.Millisecond,
			}})
			if err != nil {
				t.Fatalf("open: %v", err)
			}

			if err := db.SaveRecord("rec-1", map[string]float32{"v": 42}); err != nil {
				t.Fatalf("save: %v", err)
			}
			if err := db.AddEdge("a", "causes", "b", 0.5); err != nil {
				t.Fatalf("add edge: %v", err)
			}

			// Let the interval sync fire at least once.
			if tc.mode == SyncInterval {
				time.Sleep(30 * time.Millisecond)
			}

			// Close WITHOUT Sync: recovery must come from WAL replay.
			if err := db.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}

			reopened, err := Open(path)
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			defer reopened.Close()

			rec, err := reopened.GetRecord("rec-1")
			if err != nil {
				t.Fatalf("get record after reopen: %v", err)
			}
			if rec["v"] != 42 {
				t.Fatalf("record not recovered, got %+v", rec)
			}
			if got := reopened.OutgoingEdges("a", "causes"); len(got) != 1 || got[0].Target != "b" {
				t.Fatalf("edge not recovered: %+v", got)
			}
		})
	}
}

// TestWALOptionsDefaultIsDurable documents that the zero-config paths fsync.
func TestWALOptionsDefaultIsDurable(t *testing.T) {
	if got := DefaultWALOptions().SyncMode; got != SyncAlways {
		t.Fatalf("DefaultWALOptions SyncMode = %v, want SyncAlways", got)
	}
	if got := DefaultOptions().WAL.SyncMode; got != SyncAlways {
		t.Fatalf("DefaultOptions WAL SyncMode = %v, want SyncAlways", got)
	}
}
