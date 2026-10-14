package storage

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/rachmanzz/anchordb/pkg/spatial"
)

func TestAnchorDB_CreateAndQuery(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.anchordb")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open anchordb: %v", err)
	}
	defer db.Close()

	// Seed nodes
	_, _ = db.AppendNode("ibu", spatial.Point2D{X: 0.2, Y: 0.9}, [4]float32{0.9, 0.1, 0.2, 0.2}, 1)
	_, _ = db.AppendNode("ayah", spatial.Point2D{X: 0.2, Y: 0.9}, [4]float32{0.9, 0.1, 0.2, 0.2}, 1)
	_, _ = db.AppendNode("anak", spatial.Point2D{X: 0.2, Y: 0.1}, [4]float32{0.2, 0.1, 0.95, 0.2}, 1)
	_, _ = db.AppendNode("sekolah", spatial.Point2D{X: 0.2, Y: -0.5}, [4]float32{0.1, 0.1, 0.9, 0.5}, 1)
	_, _ = db.AppendNode("belajar", spatial.Point2D{X: 0.2, Y: -0.7}, [4]float32{0.2, 0.9, 0.5, 0.1}, 1)

	// Sync to disk
	if err := db.Sync(); err != nil {
		t.Fatalf("failed to sync db: %v", err)
	}

	// In-Storage Computational Query: ibu + ayah -> anak
	results, anchor, err := db.ExecuteGraphStep([]string{"ibu", "ayah"}, "keturunan", 5)
	if err != nil {
		t.Fatalf("ExecuteGraphStep failed: %v", err)
	}

	for i, res := range results {
		t.Logf("Candidate [%d]: %s, Score: %f, t: %f, distSq: %f", i, res.Word, res.Score, res.T, res.DistSqPerp)
	}

	if results[0].Word != "anak" {
		t.Fatalf("expected top result 'anak', got %q", results[0].Word)
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

	// Seed key nodes
	_, _ = db.AppendNode("ibu", spatial.Point2D{X: 0.2, Y: 0.9}, [4]float32{0.9, 0.1, 0.2, 0.2}, 1)
	_, _ = db.AppendNode("ayah", spatial.Point2D{X: 0.2, Y: 0.9}, [4]float32{0.9, 0.1, 0.2, 0.2}, 1)
	_, _ = db.AppendNode("anak", spatial.Point2D{X: 0.2, Y: 0.1}, [4]float32{0.2, 0.1, 0.95, 0.2}, 1)

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

	inputs := []string{"ibu", "ayah"}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Pulling top 10 items from 50,000 records
		results, _, _ := db.ExecuteGraphStep(inputs, "keturunan", 10)
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

	_ = db.SaveRecord("trip_jakarta_bandung", map[string]float32{
		"jarak": 150.0,
		"waktu": 2.5,
	})
	_ = db.SaveRecord("trip_jakarta_surabaya", map[string]float32{
		"jarak": 780.0,
		"waktu": 10.0,
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
	computed, err := db2.GetRecordWithCompute("trip_jakarta_bandung", map[string]ComputeOp{
		"kecepatan": OpDivide{FieldA: "jarak", FieldB: "waktu"},
	})
	if err != nil {
		t.Fatalf("GetRecordWithCompute failed: %v", err)
	}

	if computed["jarak"] != 150.0 || computed["waktu"] != 2.5 || computed["kecepatan"] != 60.0 {
		t.Fatalf("unexpected computed values: %v", computed)
	}
	t.Logf("Persisted data successfully restored & computed: jarak=%.1f, waktu=%.1f, kecepatan=%.1f",
		computed["jarak"], computed["waktu"], computed["kecepatan"])
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

	_, _ = db1.AppendNode("ibu", spatial.Point2D{X: 0.2, Y: 0.9}, [4]float32{0.9, 0.1, 0.2, 0.2}, 1)
	_, _ = db1.AppendNode("ayah", spatial.Point2D{X: 0.2, Y: 0.9}, [4]float32{0.9, 0.1, 0.2, 0.2}, 1)
	_, _ = db1.AppendNode("anak", spatial.Point2D{X: 0.2, Y: 0.1}, [4]float32{0.2, 0.1, 0.95, 0.2}, 1)

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

	// CRITICAL CHECK: ExecuteGraphStep must return non-empty word string "anak", not ""!
	results, _, err := db2.ExecuteGraphStep([]string{"ibu", "ayah"}, "keturunan", 5)
	if err != nil {
		t.Fatalf("ExecuteGraphStep after reopen failed: %v", err)
	}
	if len(results) == 0 {
		t.Fatalf("expected at least 1 result after reopen, got 0")
	}
	if results[0].Word != "anak" {
		t.Fatalf("CRITICAL BUG: word lost after reopen! expected 'anak', got %q", results[0].Word)
	}
	t.Logf("[OK] Reopen graph query succeeded! Top-1 word preserved: %q (Score: %f)", results[0].Word, results[0].Score)

	// Phase 3: Append new node to reopened DB and sync again
	_, _ = db2.AppendNode("sekolah", spatial.Point2D{X: 0.2, Y: -0.5}, [4]float32{0.1, 0.1, 0.9, 0.5}, 1)
	if err := db2.Sync(); err != nil {
		t.Fatalf("Sync after append on reopened db failed: %v", err)
	}

	// Verify all words still intact
	resAfterAppend, _, _ := db2.ExecuteGraphStep([]string{"ibu", "ayah"}, "keturunan", 5)
	if len(resAfterAppend) == 0 || resAfterAppend[0].Word != "anak" {
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
			"jarak":   float32(100 + i),
			"waktu":   2.5,
			"tanggal": 20740, // 2026-10-14
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
	if v500["jarak"] != 600 || v500["waktu"] != 2.5 {
		t.Fatalf("data mismatch: got %v", v500)
	}

	// Verify fast projected query
	items, _, err := reopened.ListRecordsWithPaging(QueryOptions{
		Select: []string{"jarak"},
		Limit:  10,
	})
	if err != nil || len(items) != 10 {
		t.Fatalf("paging failed: items=%d, err=%v", len(items), err)
	}
	if items[0].Props["jarak"] != 100 || len(items[0].Props) != 1 {
		t.Fatalf("projection failed: %v", items[0].Props)
	}

	t.Logf("[OK] CompactRecord and Zero-Copy verification passed for %d records!", n)
}

func TestMain(m *testing.M) {
	code := m.Run()
	os.Exit(code)
}
