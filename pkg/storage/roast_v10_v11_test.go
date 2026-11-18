package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/rachmanzz/anchordb/pkg/spatial"
)

func TestRoastV10_ConcurrentSyncAndLifecycleMutationsNoUseAfterMunmap(t *testing.T) {
	dir := t.TempDir()
	db, err := OpenWithOptions(filepath.Join(dir, "v10.anchordb"), Options{
		WAL: WALOptions{SyncMode: SyncNone},
	})
	if err != nil {
		t.Fatalf("OpenWithOptions: %v", err)
	}
	defer db.Close()

	const total = 40
	for i := 0; i < total; i++ {
		name := fmt.Sprintf("rec_%02d", i)
		if err := db.SaveRecord(name, map[string]float32{"date": 100, "v": float32(i)}); err != nil {
			t.Fatalf("SaveRecord(%s): %v", name, err)
		}
	}
	if err := db.Sync(); err != nil {
		t.Fatalf("initial Sync: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 8; i++ {
			if err := db.Sync(); err != nil {
				t.Errorf("concurrent Sync: %v", err)
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 8; i++ {
			recs, err := db.LockPartitionRecords("date", 200, 10)
			if err != nil {
				t.Errorf("LockPartitionRecords: %v", err)
				return
			}
			names := make([]string, len(recs))
			for j, r := range recs {
				names[j] = r.Name
			}
			if _, err := db.CompletePartitionRecords(names, OpStatusActive); err != nil {
				t.Errorf("CompletePartitionRecords: %v", err)
				return
			}
			if _, err := db.SetOperationStatus([]string{"rec_00", "rec_01"}, OpStatusRetry); err != nil {
				t.Errorf("SetOperationStatus(Retry): %v", err)
				return
			}
			if _, err := db.SetOperationStatus([]string{"rec_00", "rec_01"}, OpStatusActive); err != nil {
				t.Errorf("SetOperationStatus(Active): %v", err)
				return
			}
		}
	}()
	wg.Wait()

	// Must read every record cleanly after old mmap unmapping without SIGSEGV.
	for i := 0; i < total; i++ {
		name := fmt.Sprintf("rec_%02d", i)
		cr, err := db.GetRecordTyped(name)
		if err != nil {
			t.Fatalf("GetRecordTyped(%s) after concurrent Sync: %v", name, err)
		}
		if v, ok, err := cr.Get("v"); err != nil || !ok || v != float32(i) {
			t.Fatalf("unexpected value for %s: got (%v, %v, %v)", name, v, ok, err)
		}
	}
}

func TestRoastV11_ReopenSkipsDeletedNodeAndPreservesReconciledRetry(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "v11.anchordb")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := db.SaveRecordWithNode("alpha", OpStatusActive, map[string]float32{"x": 1}, spatial.Point2D{X: 0.2, Y: 0.3}, [4]float32{1, 0, 0, 0}); err != nil {
		t.Fatalf("SaveRecordWithNode(alpha): %v", err)
	}
	if _, err := db.SaveRecordWithNode("beta", OpStatusActive, map[string]float32{"date": 10}, spatial.Point2D{X: 0.4, Y: 0.5}, [4]float32{1, 0, 0, 0}); err != nil {
		t.Fatalf("SaveRecordWithNode(beta): %v", err)
	}
	if err := db.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	// Delete alpha, sync, and lock beta into SafeDelete before closing.
	if n, err := db.DeleteRecords([]string{"alpha"}); err != nil || n != 1 {
		t.Fatalf("DeleteRecords(alpha) = (%d, %v)", n, err)
	}
	if locked, err := db.LockPartitionRecords("date", 50, 10); err != nil || len(locked) != 1 {
		t.Fatalf("LockPartitionRecords = (%d, %v)", len(locked), err)
	}
	if err := db.Sync(); err != nil {
		t.Fatalf("Sync before reopen: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen: deleted node "alpha" must NOT be resurrected in Lexicon; "beta" must reconcile SafeDelete -> Retry.
	db2, err := Open(dbPath)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	crBeta, err := db2.GetRecordTyped("beta")
	if err != nil || crBeta.Operation != OpStatusRetry {
		_ = db2.Close()
		t.Fatalf("expected beta reconciled to OpStatusRetry, got op=%d err=%v", crBeta.Operation, err)
	}

	// Re-save "alpha" as a new node and verify spatial nearest lookup from "beta" finds it (ID != 0).
	if _, err := db2.SaveRecordWithNode("alpha", OpStatusActive, map[string]float32{"x": 2}, spatial.Point2D{X: 0.2, Y: 0.3}, [4]float32{1, 0, 0, 0}); err != nil {
		_ = db2.Close()
		t.Fatalf("re-SaveRecordWithNode(alpha): %v", err)
	}
	neighbors, err := db2.FindNearestNeighbors("beta", 5)
	if err != nil {
		_ = db2.Close()
		t.Fatalf("FindNearestNeighbors(beta): %v", err)
	}
	foundAlpha := false
	for _, nb := range neighbors {
		if nb.Word == "alpha" {
			foundAlpha = true
		}
	}
	if !foundAlpha {
		_ = db2.Close()
		t.Fatalf("expected re-saved alpha to have active spatial node ID != 0; neighbors=%+v", neighbors)
	}

	// Sync after crash reconciliation must persist OpStatusRetry instead of reverting to OpStatusSafeDelete.
	if err := db2.Sync(); err != nil {
		_ = db2.Close()
		t.Fatalf("Sync after reconciliation: %v", err)
	}
	if err := db2.Close(); err != nil {
		t.Fatalf("Close db2: %v", err)
	}

	db3, err := Open(dbPath)
	if err != nil {
		t.Fatalf("reopen db3: %v", err)
	}
	defer db3.Close()
	crBeta3, err := db3.GetRecordTyped("beta")
	if err != nil || crBeta3.Operation != OpStatusRetry {
		t.Fatalf("expected beta to remain OpStatusRetry across Sync+reopen, got op=%d err=%v", crBeta3.Operation, err)
	}
}

func TestRoastV11_RepairRefusesNonCorruptIOErrorAndFailedSyncReappliesDeletes(t *testing.T) {
	// 1. WALManager.Repair must NOT truncate on non-corrupt I/O read errors.
	dir := t.TempDir()
	walPath := filepath.Join(dir, "ioerr.wal")
	w, err := OpenWALWithOptions(walPath, WALOptions{SyncMode: SyncNone})
	if err != nil {
		t.Fatalf("OpenWALWithOptions: %v", err)
	}
	if err := w.WriteSave("keep_me", CompactRecord{Operation: OpStatusActive, Attrs: []AttrEntry{NewFloat32Attr("v", 1)}}); err != nil {
		_ = w.Close()
		t.Fatalf("WriteSave: %v", err)
	}
	origInfo, err := os.Stat(walPath)
	if err != nil || origInfo.Size() == 0 {
		_ = w.Close()
		t.Fatalf("Stat wal: size=%d err=%v", origInfo.Size(), err)
	}
	_ = w.Close()

	// Open write-only so scanWALLocked hits a non-ErrWALCorrupt read error (EBADF).
	wo, err := os.OpenFile(walPath, os.O_WRONLY, 0600)
	if err != nil {
		t.Fatalf("OpenFile O_WRONLY: %v", err)
	}
	wm := &WALManager{file: wo, path: walPath}
	res, repErr := wm.Repair()
	_ = wo.Close()
	if repErr == nil || errors.Is(repErr, ErrWALCorrupt) {
		t.Fatalf("expected non-ErrWALCorrupt I/O error from Repair, got res=%+v err=%v", res, repErr)
	}
	afterInfo, err := os.Stat(walPath)
	if err != nil || afterInfo.Size() != origInfo.Size() {
		t.Fatalf("Repair truncated WAL on non-corrupt I/O error: before=%d after=%d err=%v", origInfo.Size(), afterInfo.Size(), err)
	}

	// 2. Failed Sync() must still re-apply pending deletes (zeroNodeLocked) once isSyncing clears.
	roDir := t.TempDir()
	dbPath := filepath.Join(roDir, "syncfail.anchordb")
	db, err := OpenWithOptions(dbPath, Options{WAL: WALOptions{SyncMode: SyncNone}})
	if err != nil {
		t.Fatalf("Open syncfail: %v", err)
	}
	defer db.Close()
	if _, err := db.SaveRecordWithNode("n1", OpStatusActive, map[string]float32{"v": 1}, spatial.Point2D{X: 0.1, Y: 0.1}, [4]float32{1, 0, 0, 0}); err != nil {
		t.Fatalf("SaveRecordWithNode: %v", err)
	}
	if err := db.Sync(); err != nil {
		t.Fatalf("initial Sync: %v", err)
	}
	if n, err := db.DeleteRecords([]string{"n1"}); err != nil || n != 1 {
		t.Fatalf("DeleteRecords: (%d, %v)", n, err)
	}
	// Make parent directory read-only so Sync() fails when creating its temp file.
	if err := os.Chmod(roDir, 0555); err != nil {
		t.Fatalf("Chmod 0555: %v", err)
	}
	defer func() { _ = os.Chmod(roDir, 0755) }()
	if os.Geteuid() != 0 {
		if err := db.Sync(); err == nil {
			t.Fatal("expected Sync to fail in 0555 directory")
		}
		db.mu.RLock()
		nodeRec, _ := db.getNode(0)
		db.mu.RUnlock()
		if nodeRec.ID != 0 {
			t.Fatalf("expected deleted node ID to be zeroed after failed Sync, got ID=%d", nodeRec.ID)
		}
	}
}

func TestPE14_SyncPhysicallyCompactsDeletedNodesStringsAndGrid(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "compact.anchordb")
	db, err := OpenWithOptions(dbPath, Options{WAL: WALOptions{SyncMode: SyncNone}})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if _, err := db.SaveRecordWithNode("dead_node_long_word_payload_1", OpStatusActive, map[string]float32{"v": 1}, spatial.Point2D{X: 0.25, Y: 0.25}, [4]float32{1, 0, 0, 0}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SaveRecordWithNode("dead_node_long_word_payload_2", OpStatusActive, map[string]float32{"v": 2}, spatial.Point2D{X: 0.26, Y: 0.26}, [4]float32{1, 0, 0, 0}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SaveRecordWithNode("live_node", OpStatusActive, map[string]float32{"v": 3}, spatial.Point2D{X: 0.75, Y: 0.75}, [4]float32{1, 0, 0, 0}); err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	beforeSize := db.Header.EntityOffset

	if n, err := db.DeleteRecords([]string{"dead_node_long_word_payload_1", "dead_node_long_word_payload_2"}); err != nil || n != 2 {
		t.Fatalf("DeleteRecords = (%d, %v)", n, err)
	}
	// Grid must already be pruned before Sync.
	db.mu.RLock()
	for _, list := range db.Grid {
		for _, idx := range list {
			rec, _ := db.getNode(idx)
			if rec.ID == 0 {
				db.mu.RUnlock()
				t.Fatalf("stale tombstoned node index %d still in db.Grid before Sync", idx)
			}
		}
	}
	db.mu.RUnlock()

	if err := db.Sync(); err != nil {
		t.Fatalf("Sync compaction: %v", err)
	}
	if db.Header.NodeCount != 1 {
		t.Fatalf("expected compacted NodeCount=1, got %d", db.Header.NodeCount)
	}
	if db.Header.EntityOffset >= beforeSize {
		t.Fatalf("expected EntityOffset to shrink after node+string compaction, before=%d after=%d", beforeSize, db.Header.EntityOffset)
	}
	res, err := db.FindNodesInRadius(spatial.Point2D{X: 0.75, Y: 0.75}, 0.1, 10)
	if err != nil || len(res) != 1 || res[0].Word != "live_node" {
		t.Fatalf("unexpected FindNodesInRadius after compaction: res=%+v err=%v", res, err)
	}
}
