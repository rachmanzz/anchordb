package storage

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rachmanzz/anchordb/pkg/spatial"
)

func registerTestRelation(db *AnchorDB) error {
	return db.SetRelation("test-down", spatial.Point2D{Y: -1})
}

func TestAnchorDB_CreateAndQuery(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.anchordb")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open anchordb: %v", err)
	}
	defer db.Close()
	if err := registerTestRelation(db); err != nil {
		t.Fatal(err)
	}

	// Seed nodes
	_, _ = db.AppendNode("primary_cluster", spatial.Point2D{X: 0.2, Y: 0.9}, [4]float32{0.9, 0.1, 0.2, 0.2}, 1)
	_, _ = db.AppendNode("backup_cluster", spatial.Point2D{X: 0.2, Y: 0.9}, [4]float32{0.9, 0.1, 0.2, 0.2}, 1)
	_, _ = db.AppendNode("load_balancer", spatial.Point2D{X: 0.2, Y: 0.1}, [4]float32{0.2, 0.1, 0.95, 0.2}, 1)
	_, _ = db.AppendNode("service_mesh", spatial.Point2D{X: 0.2, Y: -0.5}, [4]float32{0.1, 0.1, 0.9, 0.5}, 1)
	_, _ = db.AppendNode("database_cluster", spatial.Point2D{X: 0.2, Y: -0.7}, [4]float32{0.2, 0.9, 0.5, 0.1}, 1)

	// Sync to disk
	if err := db.Sync(); err != nil {
		t.Fatalf("failed to sync db: %v", err)
	}

	// In-Storage Computational Query: primary_cluster + backup_cluster -> load_balancer
	results, anchor, err := db.ExecuteGraphStep([]string{"primary_cluster", "backup_cluster"}, "test-down", 5)
	if err != nil {
		t.Fatalf("ExecuteGraphStep failed: %v", err)
	}

	for i, res := range results {
		t.Logf("Candidate [%d]: %s, Score: %f, t: %f, distSq: %f", i, res.Word, res.Score, res.T, res.DistSqPerp)
	}

	if results[0].Word != "load_balancer" {
		t.Fatalf("expected top result 'load_balancer', got %q", results[0].Word)
	}

	t.Logf("Anchor: (%.2f, %.2f), Top-1: %s (Score: %.4f, t: %.2f)",
		anchor.X, anchor.Y, results[0].Word, results[0].Score, results[0].T)
}

func BenchmarkAnchorDB_ConstantTime_Scale(b *testing.B) {
	tmpDir := b.TempDir()
	dbPath := filepath.Join(tmpDir, "scale.anchordb")

	db, err := Open(dbPath)
	if err != nil {
		b.Fatalf("failed to open: %v", err)
	}
	defer db.Close()
	if err := registerTestRelation(db); err != nil {
		b.Fatal(err)
	}

	// Seed key nodes
	_, _ = db.AppendNode("primary_cluster", spatial.Point2D{X: 0.2, Y: 0.9}, [4]float32{0.9, 0.1, 0.2, 0.2}, 1)
	_, _ = db.AppendNode("backup_cluster", spatial.Point2D{X: 0.2, Y: 0.9}, [4]float32{0.9, 0.1, 0.2, 0.2}, 1)
	_, _ = db.AppendNode("load_balancer", spatial.Point2D{X: 0.2, Y: 0.1}, [4]float32{0.2, 0.1, 0.95, 0.2}, 1)

	// Populate 50,000 synthetic random nodes across the spatial manifold
	rng := rand.New(rand.NewSource(42))
	for i := 0; i < 50000; i++ {
		word := fmt.Sprintf("node_%d", i)
		coord := spatial.Point2D{
			X: rng.Float32()*2.0 - 1.0,
			Y: rng.Float32()*2.0 - 1.0,
		}
		roles := [4]float32{rng.Float32(), rng.Float32(), rng.Float32(), rng.Float32()}
		_, _ = db.AppendNode(word, coord, roles, 1)
	}

	_ = db.Sync()

	inputs := []string{"primary_cluster", "backup_cluster"}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Pulling top 10 items from 50,000 records
		results, _, _ := db.ExecuteGraphStep(inputs, "test-down", 10)
		if len(results) == 0 {
			b.Fatalf("expected results")
		}
	}
}

func TestAnchorDB_DiskPersistence(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "persist.anchordb")

	// 1. Open and Save
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open anchordb: %v", err)
	}

	_ = db.SaveRecord("route_alpha", map[string]float32{
		"distance": 150.0,
		"duration": 2.5,
	})
	_ = db.SaveRecord("route_beta", map[string]float32{
		"distance": 780.0,
		"duration": 10.0,
	})

	if err := db.Sync(); err != nil {
		t.Fatalf("sync failed: %v", err)
	}
	_ = db.Close()

	// 2. Re-open from disk
	db2, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to re-open anchordb: %v", err)
	}
	defer db2.Close()

	if len(db2.EntityStore) != 2 {
		t.Fatalf("expected 2 entities restored, got %d", len(db2.EntityStore))
	}
	if len(db2.EntityOrder) != 2 {
		t.Fatalf("expected 2 entities in order, got %d", len(db2.EntityOrder))
	}

	// 3. Verify in-storage compute on restored data (GETTING + CALCULATING)
	computed, err := db2.GetRecordWithCompute("route_alpha", map[string]ComputeOp{
		"speed": OpDivide{FieldA: "distance", FieldB: "duration"},
	})
	if err != nil {
		t.Fatalf("GetRecordWithCompute failed: %v", err)
	}

	if computed["distance"] != 150.0 || computed["duration"] != 2.5 || computed["speed"] != 60.0 {
		t.Fatalf("unexpected computed values: %v", computed)
	}
	t.Logf("Persisted data successfully restored & computed: distance=%.1f, duration=%.1f, speed=%.1f",
		computed["distance"], computed["duration"], computed["speed"])
}

func TestAnchorDB_ConstantTimePaging_100k(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "scale100k.anchordb")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open: %v", err)
	}
	defer db.Close()

	// Insert 100,000 entity records
	totalRecords := 100000
	for i := 0; i < totalRecords; i++ {
		name := fmt.Sprintf("rec_%d", i)
		_ = db.SaveRecord(name, map[string]float32{
			"val": float32(i),
		})
	}

	// Pull 10,000 items from offset 0
	opts1 := QueryOptions{Limit: 10000, Offset: 0}
	res1, hasMore1, err := db.ListRecordsWithPaging(opts1)
	if err != nil || len(res1) != 10000 || !hasMore1 {
		t.Fatalf("failed pulling 10k items at offset 0: err=%v, count=%d, hasMore=%v", err, len(res1), hasMore1)
	}

	// Pull 10,000 items from offset 50,000 (deep offset)
	opts2 := QueryOptions{Limit: 10000, Offset: 50000}
	res2, hasMore2, err := db.ListRecordsWithPaging(opts2)
	if err != nil || len(res2) != 10000 || !hasMore2 {
		t.Fatalf("failed pulling 10k items at offset 50,000: err=%v, count=%d, hasMore=%v", err, len(res2), hasMore2)
	}

	t.Logf("Constant-Time retrieval verified: pulled %d items at offset 0 and %d items at offset 50,000",
		len(res1), len(res2))
}

func BenchmarkAnchorDB_Paging10k_From_100k(b *testing.B) {
	tmpDir := b.TempDir()
	dbPath := filepath.Join(tmpDir, "bench100k.anchordb")

	db, err := Open(dbPath)
	if err != nil {
		b.Fatalf("failed to open: %v", err)
	}
	defer db.Close()

	for i := 0; i < 100000; i++ {
		name := fmt.Sprintf("rec_%d", i)
		_ = db.SaveRecord(name, map[string]float32{"val": float32(i)})
	}

	opts := QueryOptions{Limit: 10000, Offset: 25000}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		results, _, err := db.ListRecordsWithPaging(opts)
		if err != nil || len(results) != 10000 {
			b.Fatalf("failed query")
		}
	}
}

func TestReopen_GraphQuery_PreservesWords(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "reopen_test.anchordb")

	// Phase 1: Open, populate nodes, and sync to disk
	db1, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	_, _ = db1.AppendNode("primary_cluster", spatial.Point2D{X: 0.2, Y: 0.9}, [4]float32{0.9, 0.1, 0.2, 0.2}, 1)
	_, _ = db1.AppendNode("backup_cluster", spatial.Point2D{X: 0.2, Y: 0.9}, [4]float32{0.9, 0.1, 0.2, 0.2}, 1)
	_, _ = db1.AppendNode("load_balancer", spatial.Point2D{X: 0.2, Y: 0.1}, [4]float32{0.2, 0.1, 0.95, 0.2}, 1)

	if err := db1.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}
	_ = db1.Close()

	// Phase 2: REOPEN from disk!
	db2, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer db2.Close()
	if err := registerTestRelation(db2); err != nil {
		t.Fatal(err)
	}

	// CRITICAL CHECK: ExecuteGraphStep must return non-empty word string "load_balancer", not ""!
	results, _, err := db2.ExecuteGraphStep([]string{"primary_cluster", "backup_cluster"}, "test-down", 5)
	if err != nil {
		t.Fatalf("ExecuteGraphStep after reopen failed: %v", err)
	}
	if len(results) == 0 {
		t.Fatalf("expected at least 1 result after reopen, got 0")
	}
	if results[0].Word != "load_balancer" {
		t.Fatalf("CRITICAL BUG: word lost after reopen! expected 'load_balancer', got %q", results[0].Word)
	}
	t.Logf("[OK] Reopen graph query succeeded! Top-1 word preserved: %q (Score: %f)", results[0].Word, results[0].Score)

	// Phase 3: Append new node to reopened DB and sync again
	_, _ = db2.AppendNode("service_mesh", spatial.Point2D{X: 0.2, Y: -0.5}, [4]float32{0.1, 0.1, 0.9, 0.5}, 1)
	if err := db2.Sync(); err != nil {
		t.Fatalf("Sync after append on reopened db failed: %v", err)
	}

	// Verify all words still intact
	resAfterAppend, _, _ := db2.ExecuteGraphStep([]string{"primary_cluster", "backup_cluster"}, "test-down", 5)
	if len(resAfterAppend) == 0 || resAfterAppend[0].Word != "load_balancer" {
		t.Fatalf("Word corrupted after appending to reopened db! got %q", resAfterAppend[0].Word)
	}
	t.Logf("[OK] String pool remains intact after subsequent append + sync on reopened db!")
}

func TestCompactRecord_MemoryFootprint(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "anchordb_mem_test_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "mem_test.anchordb")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}

	const n = 10000
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("vehicle_%d", i)
		_ = db.SaveRecord(name, map[string]float32{
			"distance": float32(100 + i),
			"duration": 2.5,
			"date":     20740, // 2026-10-14
		})
	}

	if err := db.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}
	_ = db.Close()

	// Reopen with zero-copy nodes & compact records
	reopened, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer reopened.Close()

	if len(reopened.EntityStore) != n {
		t.Fatalf("expected %d entities, got %d", n, len(reopened.EntityStore))
	}

	// Verify data accuracy
	v500, err := reopened.GetRecord("vehicle_500")
	if err != nil {
		t.Fatalf("failed to get vehicle_500: %v", err)
	}
	if v500["distance"] != 600 || v500["duration"] != 2.5 {
		t.Fatalf("data mismatch: got %v", v500)
	}

	// Verify fast projected query
	items, _, err := reopened.ListRecordsWithPaging(QueryOptions{
		Select: []string{"distance"},
		Limit:  10,
	})
	if err != nil || len(items) != 10 {
		t.Fatalf("paging failed: items=%d, err=%v", len(items), err)
	}
	if items[0].Props["distance"] != 100 || len(items[0].Props) != 1 {
		t.Fatalf("projection failed: %v", items[0].Props)
	}

	t.Logf("[OK] CompactRecord and Zero-Copy verification passed for %d records!", n)
}

func TestAnchorDB_AutoUUID(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "autouuid.anchordb")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open anchordb: %v", err)
	}
	defer db.Close()

	// 1. Save with empty string -> should generate UUID
	id1, err := db.SaveRecordWithID("", map[string]float32{"metric": 42.5})
	if err != nil {
		t.Fatalf("SaveRecordWithID failed: %v", err)
	}
	if len(id1) != 36 {
		t.Fatalf("expected 36-char UUID, got %q", id1)
	}

	// 2. Save with whitespace -> should also generate UUID
	id2, err := db.SaveRecordWithID("   ", map[string]float32{"metric": 99.0})
	if err != nil {
		t.Fatalf("SaveRecordWithID failed: %v", err)
	}
	if len(id2) != 36 || id2 == id1 {
		t.Fatalf("expected unique 36-char UUID, got %q (id1=%q)", id2, id1)
	}

	// 3. Save with custom explicit ID -> should preserve custom ID
	id3, err := db.SaveRecordWithID("custom_item_1", map[string]float32{"metric": 10.0})
	if err != nil {
		t.Fatalf("SaveRecordWithID failed: %v", err)
	}
	if id3 != "custom_item_1" {
		t.Fatalf("expected 'custom_item_1', got %q", id3)
	}

	// 4. Retrieve auto-generated UUID record
	rec1, err := db.GetRecord(id1)
	if err != nil {
		t.Fatalf("GetRecord failed for auto UUID: %v", err)
	}
	if rec1["metric"] != 42.5 {
		t.Fatalf("expected metric 42.5, got %v", rec1["metric"])
	}
}

func TestWAL_CrashRecovery(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "crash_test.anchordb")

	// 1. Open database and insert 200 records WITHOUT calling db.Sync()
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open anchordb: %v", err)
	}

	for i := 0; i < 200; i++ {
		name := fmt.Sprintf("rec_%03d", i)
		_, err := db.SaveRecordWithID(name, map[string]float32{
			"val": float32(i * 10),
		})
		if err != nil {
			t.Fatalf("SaveRecordWithID failed at %d: %v", i, err)
		}
	}

	// Update first 10 records
	for i := 0; i < 10; i++ {
		name := fmt.Sprintf("rec_%03d", i)
		_, err := db.UpdateRecords(UpdateOptions{
			TargetEntities: []string{name},
			Mutations: map[string]ComputeOp{
				"val": OpAdd{FieldA: "val", FieldB: "5"},
			},
		})
		if err != nil {
			t.Fatalf("UpdateRecords failed: %v", err)
		}
	}

	// Delete records 190-199
	toDelete := make([]string, 10)
	for i := 0; i < 10; i++ {
		toDelete[i] = fmt.Sprintf("rec_%03d", 190+i)
	}
	if _, err := db.DeleteRecords(toDelete); err != nil {
		t.Fatalf("DeleteRecords: %v", err)
	}

	// Simulate unexpected crash / kill: close without calling Sync()!
	// (Mutations were only in Go heap memory and .wal file, NOT in base .anchordb)
	_ = db.Close()

	// 2. Reopen the database from disk. WAL replay should restore all 190 records!
	db2, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to re-open anchordb after simulated crash: %v", err)
	}
	defer db2.Close()

	// Verify total entity count is 190
	if len(db2.EntityStore) != 190 {
		t.Fatalf("expected 190 entities after recovery, got %d", len(db2.EntityStore))
	}

	// Verify updated record rec_000: val should be 0 + 5 = 5
	rec0, err := db2.GetRecord("rec_000")
	if err != nil {
		t.Fatalf("GetRecord rec_000 failed: %v", err)
	}
	if rec0["val"] != 5.0 {
		t.Fatalf("expected rec_000 val=5.0, got %f", rec0["val"])
	}

	// Verify deleted record rec_195 does not exist
	if _, err := db2.GetRecord("rec_195"); err == nil {
		t.Fatalf("expected rec_195 to be deleted, but it was found")
	}

	// Verify intact record rec_050: val should be 500
	rec50, err := db2.GetRecord("rec_050")
	if err != nil {
		t.Fatalf("GetRecord rec_050 failed: %v", err)
	}
	if rec50["val"] != 500.0 {
		t.Fatalf("expected rec_050 val=500.0, got %f", rec50["val"])
	}

	// 3. Now call Sync() to checkpoint WAL, and verify WAL is truncated
	if err := db2.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}

	fi, err := os.Stat(dbPath + ".wal")
	if err != nil {
		t.Fatalf("stat wal failed: %v", err)
	}
	if fi.Size() != 0 {
		t.Fatalf("expected wal file to be truncated to 0 after sync, got %d bytes", fi.Size())
	}
}

func TestSync_NonBlockingConcurrentRead(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "concurrent_sync.anchordb")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open: %v", err)
	}
	defer db.Close()

	// Seed initial records
	const numRecords = 2000
	for i := 0; i < numRecords; i++ {
		name := fmt.Sprintf("entity_%04d", i)
		_, err := db.SaveRecordWithID(name, map[string]float32{
			"amount": float32(i),
			"score":  float32(i) * 0.1,
		})
		if err != nil {
			t.Fatalf("seed failed: %v", err)
		}
	}

	stopCh := make(chan struct{})
	var wg sync.WaitGroup

	// Launch 5 concurrent readers reading continuously
	for r := 0; r < 5; r++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			for {
				select {
				case <-stopCh:
					return
				default:
					idx := rand.Intn(numRecords)
					name := fmt.Sprintf("entity_%04d", idx)
					rec, err := db.GetRecord(name)
					if err != nil {
						t.Errorf("reader %d: GetRecord failed: %v", readerID, err)
						return
					}
					if rec["amount"] != float32(idx) {
						t.Errorf("reader %d: unexpected amount %v for %s", readerID, rec["amount"], name)
						return
					}
					time.Sleep(100 * time.Microsecond)
				}
			}
		}(r)
	}

	// Launch concurrent writer writing new records while Sync runs
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			select {
			case <-stopCh:
				return
			default:
				name := fmt.Sprintf("new_entity_%d", i)
				_, _ = db.SaveRecordWithID(name, map[string]float32{"amount": 999.0})
				time.Sleep(500 * time.Microsecond)
			}
		}
	}()

	// Trigger Sync while readers and writers are actively querying
	time.Sleep(5 * time.Millisecond)
	if err := db.Sync(); err != nil {
		t.Fatalf("Sync failed during concurrent load: %v", err)
	}

	close(stopCh)
	wg.Wait()
}

func TestMatchesFilter_ZeroAlloc(t *testing.T) {
	cr := CompactRecord{
		Operation: OpStatusActive,
		Attrs: []AttrEntry{
			{Key: "age", Value: 25.0},
			{Key: "score", Value: 95.5},
		},
	}

	tests := []struct {
		filter FilterCondition
		want   bool
	}{
		{FilterCondition{Field: "age", Operator: "=", Value: 25.0}, true},
		{FilterCondition{Field: "age", Operator: "=", Value: 26.0}, false},
		{FilterCondition{Field: "age", Operator: "!=", Value: 30.0}, true},
		{FilterCondition{Field: "age", Operator: ">", Value: 20.0}, true},
		{FilterCondition{Field: "age", Operator: "<", Value: 20.0}, false},
		{FilterCondition{Field: "score", Operator: ">=", Value: 95.5}, true},
		{FilterCondition{Field: "score", Operator: "<=", Value: 95.5}, true},
		{FilterCondition{Field: "score", Operator: "BETWEEN", Value: 90.0, ValueHigh: 100.0}, true},
		{FilterCondition{Field: "score", Operator: "BETWEEN", Value: 96.0, ValueHigh: 100.0}, false},
		{FilterCondition{Field: "non_existent", Operator: "=", Value: 1.0}, false},
	}

	for _, tc := range tests {
		got := cr.MatchesFilter(tc.filter)
		if got != tc.want {
			t.Errorf("MatchesFilter(%+v) = %v, want %v", tc.filter, got, tc.want)
		}
	}
}

func TestWALFailuresDoNotApplyMutations(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "wal-errors.anchordb"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := db.SaveRecordWithID("to-delete", map[string]float32{"value": 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SetOperationStatus([]string{"to-delete"}, OpStatusTombstone); err != nil {
		t.Fatal(err)
	}
	if err := db.wal.Close(); err != nil {
		t.Fatal(err)
	}

	beforeNodes := db.totalNodes()
	if _, err := db.AppendNode("node-not-persisted", spatial.Point2D{}, [4]float32{}, 1); err == nil {
		t.Fatal("AppendNode should report closed WAL")
	}
	if db.totalNodes() != beforeNodes {
		t.Fatal("AppendNode changed memory despite WAL failure")
	}

	if n, err := db.DeleteRecords([]string{"to-delete"}); n != 0 || err == nil {
		t.Fatalf("DeleteRecords should fail before deleting, got count=%d err=%v", n, err)
	}
	db.mu.RLock()
	_, stillExists := db.EntityStore["to-delete"]
	db.mu.RUnlock()
	if !stillExists {
		t.Fatal("DeleteRecords changed memory despite WAL failure")
	}

	if n, err := db.CollectTombstones(10); n != 0 || err == nil {
		t.Fatalf("CollectTombstones should fail before purging, got count=%d err=%v", n, err)
	}
	db.mu.RLock()
	_, stillExists = db.EntityStore["to-delete"]
	db.mu.RUnlock()
	if !stillExists {
		t.Fatal("CollectTombstones changed memory despite WAL failure")
	}
}

func TestWALCheckpointPreservesSuffix(t *testing.T) {
	wal, err := OpenWAL(filepath.Join(t.TempDir(), "checkpoint.wal"))
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()

	first := CompactRecord{Operation: OpStatusActive, Attrs: []AttrEntry{{Key: "v", Value: 1}}}
	second := CompactRecord{Operation: OpStatusActive, Attrs: []AttrEntry{{Key: "v", Value: 2}}}
	if err := wal.WriteSave("first", first); err != nil {
		t.Fatal(err)
	}
	checkpoint, err := wal.CurrentOffset()
	if err != nil {
		t.Fatal(err)
	}
	if err := wal.WriteSave("second", second); err != nil {
		t.Fatal(err)
	}
	if err := wal.Checkpoint(checkpoint); err != nil {
		t.Fatal(err)
	}
	var names []string
	if _, err := wal.Replay(func(_ byte, name string, _ CompactRecord, _ string, _ spatial.Point2D, _ [4]float32, _ uint16) error {
		names = append(names, name)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(names) != 1 || names[0] != "second" {
		t.Fatalf("replayed WAL suffix = %v, want [second]", names)
	}
}

func TestWALReplayTruncatesIncompleteTailBeforeFutureWrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tail.wal")
	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	if err := wal.WriteSave("before", CompactRecord{Operation: OpStatusActive}); err != nil {
		t.Fatal(err)
	}
	if _, err := wal.file.Write([]byte{walMagic, walVer, WalOpSave, 0}); err != nil {
		t.Fatal(err)
	}
	var names []string
	count, err := wal.Replay(func(_ byte, name string, _ CompactRecord, _ string, _ spatial.Point2D, _ [4]float32, _ uint16) error {
		names = append(names, name)
		return nil
	})
	if err != nil || count != 1 || len(names) != 1 || names[0] != "before" {
		t.Fatalf("Replay incomplete tail: count=%d names=%v err=%v", count, names, err)
	}
	if err := wal.WriteSave("after", CompactRecord{Operation: OpStatusActive}); err != nil {
		t.Fatalf("append after tail recovery: %v", err)
	}

	names = nil
	count, err = wal.Replay(func(_ byte, name string, _ CompactRecord, _ string, _ spatial.Point2D, _ [4]float32, _ uint16) error {
		names = append(names, name)
		return nil
	})
	if err != nil || count != 2 || len(names) != 2 || names[0] != "before" || names[1] != "after" {
		t.Fatalf("Replay after append: count=%d names=%v err=%v", count, names, err)
	}
}

func TestWALReplayFailsClosedOnCompleteCorruptFrame(t *testing.T) {
	path := filepath.Join(t.TempDir(), "corrupt.wal")
	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	if err := wal.WriteSave("durable", CompactRecord{Operation: OpStatusActive}); err != nil {
		t.Fatal(err)
	}
	if _, err := wal.file.Write([]byte{0, walVer, WalOpSave, 0, 0, 0, 0, 0, 0, 0, 0}); err != nil {
		t.Fatal(err)
	}
	_, err = wal.Replay(nil)
	if !errors.Is(err, ErrWALCorrupt) {
		t.Fatalf("Replay should report complete corrupt frame, got %v", err)
	}
	if err := wal.WriteSave("must-not-append", CompactRecord{Operation: OpStatusActive}); !errors.Is(err, ErrWALCorrupt) {
		t.Fatalf("write after corruption should fail closed, got %v", err)
	}
}

func TestRelationsAreValidatedAndUnknownRelationRejected(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "relations.anchordb"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, exists := db.Relation("dependency"); exists {
		t.Fatal("core database must not install domain-specific relation defaults")
	}
	if err := db.SetRelation("custom", spatial.Point2D{X: 1}); err != nil {
		t.Fatal(err)
	}
	if direction, exists := db.Relation("custom"); !exists || direction.X != 1 {
		t.Fatalf("custom relation not returned, direction=%+v exists=%v", direction, exists)
	}
	if err := db.SetRelation("zero", spatial.Point2D{}); err == nil {
		t.Fatal("zero direction should be rejected")
	}
	if _, _, err := db.ExecuteGraphStep(nil, "custom", 1); err == nil {
		t.Fatal("empty input should fail")
	}
	if _, err := db.AppendNode("input", spatial.Point2D{}, [4]float32{}, 1); err != nil {
		t.Fatal(err)
	}
	if _, _, err := db.ExecuteGraphStep([]string{"input"}, "unregistered", 1); err == nil || !strings.Contains(err.Error(), "unknown relation") {
		t.Fatalf("unknown relation should fail explicitly, got %v", err)
	}
	if _, _, err := db.ExecuteGraphStepWithOptions([]string{"x"}, "custom", -1, OrderByCompositeDesc, 0.05); err == nil {
		t.Fatal("negative TopK should fail")
	}
}

func TestOpenExclusivelyLocksDatabaseAcrossInstances(t *testing.T) {
	path := filepath.Join(t.TempDir(), "locked.anchordb")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if second, err := Open(path); err == nil {
		_ = second.Close()
		t.Fatal("second open should fail while first instance owns the database")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if reopened, err := Open(path); err != nil {
		t.Fatalf("open after release should succeed: %v", err)
	} else {
		_ = reopened.Close()
	}
}

func TestOpenRejectsTruncatedDatabaseSections(t *testing.T) {
	tests := []struct {
		name     string
		truncate func(t *testing.T, path string)
	}{
		{
			name: "node section",
			truncate: func(t *testing.T, path string) {
				if err := os.Truncate(path, HeaderSize+RecordSize-1); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "entity section",
			truncate: func(t *testing.T, path string) {
				info, err := os.Stat(path)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.Truncate(path, info.Size()-1); err != nil {
					t.Fatal(err)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "corrupt.anchordb")
			db, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.SaveRecordWithID("record", map[string]float32{"value": 1}); err != nil {
				t.Fatal(err)
			}
			if err := db.Sync(); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			tt.truncate(t, path)
			if opened, err := Open(path); err == nil {
				_ = opened.Close()
				t.Fatal("Open succeeded for truncated database")
			}
		})
	}
}

func TestFindNearestNeighborsRejectsNegativeLimit(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "nearest.anchordb"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.AppendNode("node", spatial.Point2D{}, [4]float32{}, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := db.FindNearestNeighbors("node", -1); err == nil {
		t.Fatal("negative neighbor limit should return an error")
	}
}

func TestPartitionLockSnapshotsAndBlocksConcurrentMutation(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "partition-lock.anchordb"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.SaveRecord("old", map[string]float32{"date": 1, "value": 10}); err != nil {
		t.Fatal(err)
	}
	records, err := db.LockPartitionRecords("date", 2, 10)
	if err != nil || len(records) != 1 {
		t.Fatalf("LockPartitionRecords = %d records, err=%v", len(records), err)
	}
	if records[0].Props["value"] != 10 {
		t.Fatalf("snapshot value = %v, want 10", records[0].Props["value"])
	}
	if err := db.SaveRecord("old", map[string]float32{"date": 1, "value": 20}); !errors.Is(err, ErrRecordLocked) {
		t.Fatalf("save to locked record should fail, got %v", err)
	}
	if n, err := db.DeleteRecords([]string{"old"}); n != 0 || !errors.Is(err, ErrRecordLocked) {
		t.Fatalf("delete locked record should fail, count=%d err=%v", n, err)
	}
	if _, err := db.CompletePartitionRecords([]string{"old"}, OpStatusRetry); err != nil {
		t.Fatal(err)
	}
	props, op, err := db.GetRecordWithOperation("old")
	if err != nil || op != OpStatusRetry || props["value"] != 10 {
		t.Fatalf("record after release = props %v op %d err %v", props, op, err)
	}
}

func TestWALReplayRecoversTruncatedFrameHeader(t *testing.T) {
	path := filepath.Join(t.TempDir(), "truncated-header.wal")
	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	if err := wal.WriteSave("safe", CompactRecord{Operation: OpStatusActive}); err != nil {
		t.Fatal(err)
	}
	validOffset, err := wal.CurrentOffset()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wal.file.Write([]byte{walMagic, walVer, WalOpSave}); err != nil {
		t.Fatal(err)
	}
	count, err := wal.Replay(func(_ byte, name string, _ CompactRecord, _ string, _ spatial.Point2D, _ [4]float32, _ uint16) error {
		if name != "safe" {
			t.Errorf("unexpected replayed name %q", name)
		}
		return nil
	})
	if err != nil || count != 1 {
		t.Fatalf("Replay truncated header: count=%d err=%v", count, err)
	}
	if info, err := wal.file.Stat(); err != nil || info.Size() != validOffset {
		t.Fatalf("truncated header recovery size = %v, want %d (err=%v)", info, validOffset, err)
	}
	if err := wal.WriteSave("after", CompactRecord{Operation: OpStatusActive}); err != nil {
		t.Fatalf("write after truncated-header recovery: %v", err)
	}
}

func TestOperationLifecycle_SafeDeleteAndCollector(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "lifecycle.adb")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	// 1. Save record with default OpStatusActive (1)
	name := "rec1"
	if err := db.SaveRecord(name, map[string]float32{"val": 100}); err != nil {
		t.Fatalf("SaveRecord failed: %v", err)
	}

	props, op, err := db.GetRecordWithOperation(name)
	if err != nil {
		t.Fatalf("GetRecordWithOperation failed: %v", err)
	}
	if op != OpStatusActive {
		t.Errorf("expected op %d, got %d", OpStatusActive, op)
	}
	if props["val"] != 100 {
		t.Errorf("expected val 100, got %v", props["val"])
	}

	// 2. Transition to SafeDelete (0)
	updated, err := db.SetOperationStatus([]string{name}, OpStatusSafeDelete)
	if err != nil {
		t.Fatalf("SetOperationStatus: %v", err)
	}
	if updated != 1 {
		t.Errorf("expected 1 record updated to SafeDelete, got %d", updated)
	}

	_, op, err = db.GetRecordWithOperation(name)
	if err != nil || op != OpStatusSafeDelete {
		t.Errorf("expected SafeDelete (0), got op %d, err %v", op, err)
	}

	// UpdateRecords must skip SafeDelete records
	mutCount, err := db.UpdateRecords(UpdateOptions{
		TargetEntities: []string{name},
		Mutations: map[string]ComputeOp{
			"val": OpAdd{FieldA: "val", FieldB: "50"},
		},
	})
	if err != nil {
		t.Fatalf("UpdateRecords error: %v", err)
	}
	if mutCount != 0 {
		t.Errorf("expected 0 updated records while in SafeDelete, got %d", mutCount)
	}

	// Default ListRecordsWithPaging should not return SafeDelete records
	records, _, _ := db.ListRecordsWithPaging(QueryOptions{})
	if len(records) != 0 {
		t.Errorf("expected 0 active records in ListRecordsWithPaging, got %d", len(records))
	}

	// With IncludeAllOperations, it returns the SafeDelete record
	recordsAll, _, _ := db.ListRecordsWithPaging(QueryOptions{IncludeAllOperations: true})
	if len(recordsAll) != 1 || recordsAll[0].Operation != OpStatusSafeDelete {
		t.Errorf("expected 1 SafeDelete record with IncludeAllOperations, got %+v", recordsAll)
	}

	// 3. Transition to Tombstone (2)
	if _, err := db.SetOperationStatus([]string{name}, OpStatusTombstone); err != nil {
		t.Fatalf("SetOperationStatus tombstone: %v", err)
	}
	_, err = db.GetRecord(name)
	if err == nil {
		t.Error("expected GetRecord to fail for tombstoned record")
	}

	// 4. Collect Tombstones
	purged, err := db.CollectTombstones(10)
	if err != nil {
		t.Fatalf("CollectTombstones: %v", err)
	}
	if purged != 1 {
		t.Errorf("expected 1 tombstone collected, got %d", purged)
	}

	// 5. Test persistence across Sync and Open
	name2 := "rec2"
	if err := db.SaveRecord(name2, map[string]float32{"score": 99}); err != nil {
		t.Fatalf("SaveRecord failed: %v", err)
	}
	if _, err := db.SetOperationStatus([]string{name2}, OpStatusSafeDelete); err != nil {
		t.Fatalf("SetOperationStatus safe-delete: %v", err)
	}
	if err := db.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}
	_ = db.Close()

	// Reopen simulates crash / shutdown while record was in SafeDelete:
	// Startup crash reconciler must automatically transition it to OpStatusRetry (3)!
	db2, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer db2.Close()

	_, op2, err := db2.GetRecordWithOperation(name2)
	if err != nil {
		t.Fatalf("GetRecordWithOperation after reopen failed: %v", err)
	}
	if op2 != OpStatusRetry {
		t.Errorf("expected op %d (OpStatusRetry) after startup crash reconciler, got %d", OpStatusRetry, op2)
	}
}

func TestAppendNodeRejectsNonFiniteGeometry(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "geometry.anchordb"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	nan := float32(math.NaN())
	inf := float32(math.Inf(1))

	if _, err := db.AppendNode("bad-x", spatial.Point2D{X: nan}, [4]float32{}, 1); err == nil {
		t.Fatal("NaN X coordinate should be rejected")
	}
	if _, err := db.AppendNode("bad-y", spatial.Point2D{Y: inf}, [4]float32{}, 1); err == nil {
		t.Fatal("Inf Y coordinate should be rejected")
	}
	if _, err := db.AppendNode("bad-role", spatial.Point2D{}, [4]float32{nan, 0, 0, 0}, 1); err == nil {
		t.Fatal("NaN role should be rejected")
	}
	if _, exists := db.Lexicon["bad-x"]; exists {
		t.Fatal("rejected node must not be indexed")
	}

	// A non-finite coordinate must still map to a valid grid cell defensively,
	// so WAL replay of a tampered frame cannot corrupt the spatial index.
	cell := PointToCell(spatial.Point2D{X: nan, Y: nan}, GridDim)
	if cell.X < 0 || cell.X >= GridDim || cell.Y < 0 || cell.Y >= GridDim {
		t.Fatalf("PointToCell produced out-of-range cell %+v", cell)
	}
}

func TestUpdateRecordsDirectTargetsHonorsOffset(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "update-offset.anchordb"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for _, name := range []string{"a", "b", "c"} {
		if err := db.SaveRecord(name, map[string]float32{"value": 1}); err != nil {
			t.Fatal(err)
		}
	}

	updated, err := db.UpdateRecords(UpdateOptions{
		TargetEntities: []string{"a", "b", "c"},
		Mutations:      map[string]ComputeOp{"value": OpAdd{FieldA: "value", FieldB: "1"}},
		Offset:         2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if updated != 1 {
		t.Fatalf("direct update honored offset: updated=%d, want 1", updated)
	}

	a, _ := db.GetRecord("a")
	b, _ := db.GetRecord("b")
	c, _ := db.GetRecord("c")
	if a["value"] != 1 || b["value"] != 1 {
		t.Fatalf("records before offset were mutated: a=%v b=%v", a["value"], b["value"])
	}
	if c["value"] != 2 {
		t.Fatalf("record at offset was not mutated: c=%v", c["value"])
	}
}

func TestMain(m *testing.M) {
	code := m.Run()
	os.Exit(code)
}
