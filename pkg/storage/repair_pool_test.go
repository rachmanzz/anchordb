package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// A mid-WAL corruption must make Open fail with ErrWALCorrupt, and RepairWALFile
// must recover the database by discarding the bad tail (after backing it up).
func TestOpenRecoversAfterWALRepair(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "recov.anchordb")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := db.SaveRecord("alpha", map[string]float32{"x": 1}); err != nil {
		t.Fatalf("save alpha: %v", err)
	}
	if err := db.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
	// Writes after the checkpoint live only in the WAL until the next Sync.
	if err := db.SaveRecord("beta", map[string]float32{"y": 2}); err != nil {
		t.Fatalf("save beta: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Corrupt the final payload byte so the last frame's CRC no longer matches.
	walPath := dbPath + ".wal"
	f, err := os.OpenFile(walPath, os.O_RDWR, 0600)
	if err != nil {
		t.Fatalf("open wal for corruption: %v", err)
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		t.Fatalf("stat wal: %v", err)
	}
	if fi.Size() == 0 {
		f.Close()
		t.Fatal("expected a non-empty wal after unsynced writes")
	}
	var b [1]byte
	if _, err := f.ReadAt(b[:], fi.Size()-1); err != nil {
		f.Close()
		t.Fatalf("read wal byte: %v", err)
	}
	b[0] ^= 0xFF
	if _, err := f.WriteAt(b[:], fi.Size()-1); err != nil {
		f.Close()
		t.Fatalf("corrupt wal byte: %v", err)
	}
	_ = f.Close()

	// A corrupt WAL currently bricks Open...
	if _, err := Open(dbPath); err == nil {
		t.Fatal("expected Open to fail on a corrupt WAL")
	} else if !errors.Is(err, ErrWALCorrupt) {
		t.Fatalf("expected ErrWALCorrupt, got: %v", err)
	}

	// ...until the tail is repaired.
	res, err := RepairWALFile(walPath)
	if err != nil {
		t.Fatalf("RepairWALFile: %v", err)
	}
	if !res.Repaired || res.DiscardedBytes == 0 {
		t.Fatalf("expected the corrupt tail to be discarded, got %+v", res)
	}
	if _, err := os.Stat(res.BackupPath); err != nil {
		t.Fatalf("backup file missing: %v", err)
	}

	db2, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open after repair: %v", err)
	}
	defer db2.Close()
	if _, err := db2.GetRecord("alpha"); err != nil {
		t.Fatalf("persisted record alpha must survive repair: %v", err)
	}
}

// Repair must keep the intact prefix (only the corrupt suffix is discarded) and
// report where the discarded bytes were backed up.
func TestRepairWALKeepsValidPrefixAndBacksUpTail(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "r.wal")

	w, err := OpenWAL(walPath)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	if err := w.WriteSave("alpha", CompactRecord{Operation: OpStatusActive, Attrs: []AttrEntry{{Key: "x", Value: 1}}}); err != nil {
		t.Fatalf("write alpha: %v", err)
	}
	if err := w.WriteSave("beta", CompactRecord{Operation: OpStatusActive, Attrs: []AttrEntry{{Key: "y", Value: 2}}}); err != nil {
		t.Fatalf("write beta: %v", err)
	}
	_ = w.Close()

	// Corrupt beta's payload so the second frame fails its CRC check.
	f, err := os.OpenFile(walPath, os.O_RDWR, 0600)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		t.Fatalf("stat: %v", err)
	}
	var b [1]byte
	if _, err := f.ReadAt(b[:], fi.Size()-1); err != nil {
		f.Close()
		t.Fatalf("read: %v", err)
	}
	b[0] ^= 0xFF
	if _, err := f.WriteAt(b[:], fi.Size()-1); err != nil {
		f.Close()
		t.Fatalf("write: %v", err)
	}
	_ = f.Close()

	// Replay must surface the corruption.
	w2, err := OpenWAL(walPath)
	if err != nil {
		t.Fatalf("reopen wal: %v", err)
	}
	_, replayErr := w2.ReplayEvents(func(WALEvent) error { return nil })
	if !errors.Is(replayErr, ErrWALCorrupt) {
		t.Fatalf("expected ErrWALCorrupt from replay, got %v", replayErr)
	}
	_ = w2.Close()

	res, err := RepairWALFile(walPath)
	if err != nil {
		t.Fatalf("RepairWALFile: %v", err)
	}
	if !res.Repaired {
		t.Fatalf("expected repair, got %+v", res)
	}
	if res.ValidBytes == 0 {
		t.Fatal("the intact alpha frame should have been kept")
	}
	if res.BackupPath == "" {
		t.Fatal("expected a backup path for the discarded tail")
	}
	if _, err := os.Stat(res.BackupPath); err != nil {
		t.Fatalf("backup file missing: %v", err)
	}

	// Only the intact prefix should replay.
	w3, err := OpenWAL(walPath)
	if err != nil {
		t.Fatalf("reopen repaired wal: %v", err)
	}
	defer w3.Close()
	var names []string
	count, err := w3.ReplayEvents(func(ev WALEvent) error {
		names = append(names, ev.Name)
		return nil
	})
	if err != nil {
		t.Fatalf("replay after repair: %v", err)
	}
	if count != 1 || len(names) != 1 || names[0] != "alpha" {
		t.Fatalf("expected only alpha to survive repair, got count=%d names=%v", count, names)
	}
}

// CompactAttrPool must drop keys no longer referenced by any live record while
// keeping keys that are still in use.
func TestCompactAttrPoolDropsUnreferencedKeys(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "pool.anchordb"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if err := db.SaveRecord("keep", map[string]float32{"k1": 1, "k2": 2}); err != nil {
		t.Fatalf("save: %v", err)
	}

	// Intern a batch of keys that no live record references (simulating keys that
	// were interned earlier but whose records have since been removed).
	db.mu.Lock()
	for i := 0; i < 1000; i++ {
		db.internAttr(fmt.Sprintf("orphan_%d", i))
	}
	before := len(db.attrPool)
	db.mu.Unlock()
	if before < 1000+2 {
		t.Fatalf("expected orphans to be interned, pool size = %d", before)
	}

	db.CompactAttrPool()

	db.mu.RLock()
	after := len(db.attrPool)
	_, kept1 := db.attrPool["k1"]
	_, kept2 := db.attrPool["k2"]
	_, orphan := db.attrPool["orphan_0"]
	db.mu.RUnlock()

	if orphan {
		t.Fatal("unreferenced key should have been dropped from the pool")
	}
	if !kept1 || !kept2 {
		t.Fatal("keys still referenced by a live record must remain interned")
	}
	if after != 2 {
		t.Fatalf("pool should shrink to the 2 live keys, got %d", after)
	}
}
