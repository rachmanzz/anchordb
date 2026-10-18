package storage

import (
	"math"
	"path/filepath"
	"strconv"
	"testing"
)

func TestComputeStats_EmptyDatabase(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "empty_stats.anchordb")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	stats, err := db.ComputeStats("amount")
	if err != nil {
		t.Fatalf("ComputeStats on empty db returned error: %v", err)
	}
	if stats.Count != 0 || stats.Sum != 0 || stats.Min != 0 || stats.Max != 0 || stats.Mean != 0 || stats.Variance != 0 || stats.StdDev != 0 {
		t.Errorf("expected all zero stats on empty db, got: %+v", stats)
	}
}

func TestComputeStats_BasicDataset(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "basic_stats.anchordb")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	// Values: 10, 20, 30, 40, 50
	values := []float32{10, 20, 30, 40, 50}
	for i, v := range values {
		_ = db.SaveRecord("item_"+strconv.Itoa(i), map[string]float32{
			"amount": v,
		})
	}

	stats, err := db.ComputeStats("amount")
	if err != nil {
		t.Fatalf("ComputeStats failed: %v", err)
	}

	if stats.Count != 5 {
		t.Errorf("expected count=5, got %d", stats.Count)
	}
	if stats.Sum != 150.0 {
		t.Errorf("expected sum=150, got %f", stats.Sum)
	}
	if stats.Min != 10.0 {
		t.Errorf("expected min=10, got %f", stats.Min)
	}
	if stats.Max != 50.0 {
		t.Errorf("expected max=50, got %f", stats.Max)
	}
	if math.Abs(stats.Mean-30.0) > 1e-5 {
		t.Errorf("expected mean=30, got %f", stats.Mean)
	}
	// Sample variance: 1000 / 4 = 250
	if math.Abs(stats.Variance-250.0) > 1e-5 {
		t.Errorf("expected variance=250, got %f", stats.Variance)
	}
	expectedStdDev := math.Sqrt(250.0)
	if math.Abs(stats.StdDev-expectedStdDev) > 1e-5 {
		t.Errorf("expected stddev=%f, got %f", expectedStdDev, stats.StdDev)
	}
}

func TestComputeStats_SingleAndIdenticalItems(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "single_stats.anchordb")
	db, _ := Open(dbPath)
	defer db.Close()

	// 1. Single item
	_ = db.SaveRecord("item_single", map[string]float32{"val": 42.0})
	stats, err := db.ComputeStats("val")
	if err != nil {
		t.Fatalf("ComputeStats failed: %v", err)
	}
	if stats.Count != 1 || stats.Mean != 42.0 || stats.Variance != 0.0 || stats.StdDev != 0.0 {
		t.Errorf("expected single item variance=0, got %+v", stats)
	}

	// 2. Identical items (all 7.0)
	dir2 := t.TempDir()
	db2, _ := Open(filepath.Join(dir2, "identical.anchordb"))
	defer db2.Close()

	for i := 0; i < 5; i++ {
		_ = db2.SaveRecord("rec_"+strconv.Itoa(i), map[string]float32{"score": 7.0})
	}
	stats2, err := db2.ComputeStats("score")
	if err != nil {
		t.Fatalf("ComputeStats failed: %v", err)
	}
	if stats2.Count != 5 || stats2.Mean != 7.0 || stats2.Variance != 0.0 || stats2.StdDev != 0.0 {
		t.Errorf("expected identical items variance=0, got %+v", stats2)
	}
}

func TestComputeStats_WithOptions_Filters(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "filtered_stats.anchordb")
	db, _ := Open(dbPath)
	defer db.Close()

	// 10, 20, 30, 40, 50
	for i, v := range []float32{10, 20, 30, 40, 50} {
		_ = db.SaveRecord("rec_"+strconv.Itoa(i), map[string]float32{"amount": v})
	}

	// Compute stats only for amount > 25 (30, 40, 50)
	opts := StatsOptions{
		Field: "amount",
		Filters: []FilterCondition{
			{Field: "amount", Operator: ">", Value: 25.0},
		},
	}
	stats, err := db.ComputeStatsWithOptions(opts)
	if err != nil {
		t.Fatalf("ComputeStatsWithOptions failed: %v", err)
	}

	if stats.Count != 3 {
		t.Fatalf("expected count=3, got %d", stats.Count)
	}
	if stats.Sum != 120.0 {
		t.Errorf("expected sum=120, got %f", stats.Sum)
	}
	if stats.Min != 30.0 || stats.Max != 50.0 {
		t.Errorf("expected min=30 max=50, got min=%f max=%f", stats.Min, stats.Max)
	}
	if math.Abs(stats.Mean-40.0) > 1e-5 {
		t.Errorf("expected mean=40, got %f", stats.Mean)
	}
}

func TestComputeStats_DatabaseClosed(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "closed_stats.anchordb")
	db, _ := Open(dbPath)
	db.Close()

	if _, err := db.ComputeStats("field"); err != ErrDatabaseClosed {
		t.Errorf("expected ErrDatabaseClosed, got %v", err)
	}
	if _, err := db.ComputeStats(""); err == nil {
		t.Errorf("expected error for empty field, got nil")
	}
}

func BenchmarkComputeStats_100k(b *testing.B) {
	dir := b.TempDir()
	dbPath := filepath.Join(dir, "bench_stats.anchordb")
	db, _ := Open(dbPath)
	defer db.Close()

	// Seed 100,000 entities
	items := make([]BatchRecordItem, 100000)
	for i := 0; i < 100000; i++ {
		items[i] = BatchRecordItem{
			Name: "sensor_" + strconv.Itoa(i),
			Props: map[string]float32{
				"voltage": float32(i%240) + 0.5,
			},
		}
	}
	_, _ = db.SaveRecordsBatch(items)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = db.ComputeStats("voltage")
	}
}
