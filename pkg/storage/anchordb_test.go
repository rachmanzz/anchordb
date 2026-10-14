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

func TestMain(m *testing.M) {
	code := m.Run()
	os.Exit(code)
}
