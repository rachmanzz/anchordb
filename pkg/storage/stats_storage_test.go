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

func TestComputeStats_64BitPrecision(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "stats_64bit.anchordb")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	val1 := int64(5000000001) // Truncated in float32 (becomes 5000000000), preserved in float64
	val2 := int64(5000000009)

	_, _ = db.SaveRecordTyped("rec_1", []AttrEntry{NewInt64Attr("big_val", val1)})
	_, _ = db.SaveRecordTyped("rec_2", []AttrEntry{NewInt64Attr("big_val", val2)})

	stats, err := db.ComputeStats("big_val")
	if err != nil {
		t.Fatalf("ComputeStats failed: %v", err)
	}
	if stats.Count != 2 {
		t.Fatalf("expected count=2, got %d", stats.Count)
	}
	if int64(stats.Min64) != val1 {
		t.Errorf("expected Min64=%d, got %f", val1, stats.Min64)
	}
	if int64(stats.Max64) != val2 {
		t.Errorf("expected Max64=%d, got %f", val2, stats.Max64)
	}

	// Verify float32 loss vs float64 precision
	if float32(val1) == float32(val2) {
		// Float32 collapsed the precision, but Min64/Max64 differentiated them
		if stats.Min64 == stats.Max64 {
			t.Errorf("expected Min64 != Max64, both got %f", stats.Min64)
		}
	}
}

func TestComputeStats_ExactIntegerExtremes(t *testing.T) {
	openStatsDB := func(t *testing.T, name string) *AnchorDB {
		t.Helper()
		db, err := Open(filepath.Join(t.TempDir(), name+".anchordb"))
		if err != nil {
			t.Fatalf("Open failed: %v", err)
		}
		t.Cleanup(func() { db.Close() })
		return db
	}

	t.Run("uint64 above 2^53 stays exact", func(t *testing.T) {
		db := openStatsDB(t, "stats_u64exact")
		hi := uint64(math.MaxUint64)
		lo := uint64(math.MaxUint64 - 1000)
		if _, err := db.SaveRecordTyped("a", []AttrEntry{NewUint64Attr("big", hi)}); err != nil {
			t.Fatalf("save failed: %v", err)
		}
		if _, err := db.SaveRecordTyped("b", []AttrEntry{NewUint64Attr("big", lo)}); err != nil {
			t.Fatalf("save failed: %v", err)
		}
		stats, err := db.ComputeStats("big")
		if err != nil {
			t.Fatalf("ComputeStats failed: %v", err)
		}
		// Legacy float64 path collapses both values; the exact fields must not.
		if stats.Min64 != stats.Max64 {
			t.Errorf("precondition broken: expected legacy Min64==Max64 collapse, got %f vs %f", stats.Min64, stats.Max64)
		}
		if stats.MinType != AttrTypeUint64 || stats.MaxType != AttrTypeUint64 {
			t.Fatalf("expected uint64 domain tags, got min=%v max=%v", stats.MinType, stats.MaxType)
		}
		if stats.MinUint != lo || stats.MaxUint != hi {
			t.Fatalf("expected exact [%d, %d], got [%d, %d]", lo, hi, stats.MinUint, stats.MaxUint)
		}
	})

	t.Run("int64 extremes stay exact", func(t *testing.T) {
		db := openStatsDB(t, "stats_i64exact")
		if _, err := db.SaveRecordTyped("a", []AttrEntry{NewInt64Attr("n", math.MinInt64)}); err != nil {
			t.Fatalf("save failed: %v", err)
		}
		if _, err := db.SaveRecordTyped("b", []AttrEntry{NewInt64Attr("n", math.MinInt64+1000)}); err != nil {
			t.Fatalf("save failed: %v", err)
		}
		stats, err := db.ComputeStats("n")
		if err != nil {
			t.Fatalf("ComputeStats failed: %v", err)
		}
		if stats.MinType != AttrTypeInt64 || stats.MaxType != AttrTypeInt64 {
			t.Fatalf("expected int64 domain tags, got min=%v max=%v", stats.MinType, stats.MaxType)
		}
		if stats.MinInt != math.MinInt64 || stats.MaxInt != math.MinInt64+1000 {
			t.Fatalf("expected exact [%d, %d], got [%d, %d]", int64(math.MinInt64), int64(math.MinInt64+1000), stats.MinInt, stats.MaxInt)
		}
	})

	t.Run("mixed int64 and uint64 resolve exactly", func(t *testing.T) {
		db := openStatsDB(t, "stats_mixed")
		if _, err := db.SaveRecordTyped("a", []AttrEntry{NewInt64Attr("m", -5)}); err != nil {
			t.Fatalf("save failed: %v", err)
		}
		if _, err := db.SaveRecordTyped("b", []AttrEntry{NewUint64Attr("m", 10)}); err != nil {
			t.Fatalf("save failed: %v", err)
		}
		stats, err := db.ComputeStats("m")
		if err != nil {
			t.Fatalf("ComputeStats failed: %v", err)
		}
		if stats.MinType != AttrTypeInt64 || stats.MinInt != -5 {
			t.Fatalf("expected overall min int64 -5, got type=%v val=%d", stats.MinType, stats.MinInt)
		}
		if stats.MaxType != AttrTypeUint64 || stats.MaxUint != 10 {
			t.Fatalf("expected overall max uint64 10, got type=%v val=%d", stats.MaxType, stats.MaxUint)
		}
	})

	t.Run("float field stays on float domain", func(t *testing.T) {
		db := openStatsDB(t, "stats_float")
		if _, err := db.SaveRecordTyped("a", []AttrEntry{NewFloat32Attr("f", 1.5)}); err != nil {
			t.Fatalf("save failed: %v", err)
		}
		if _, err := db.SaveRecordTyped("b", []AttrEntry{NewFloat32Attr("f", 2.5)}); err != nil {
			t.Fatalf("save failed: %v", err)
		}
		stats, err := db.ComputeStats("f")
		if err != nil {
			t.Fatalf("ComputeStats failed: %v", err)
		}
		if stats.MinType != AttrTypeFloat64 || stats.MaxType != AttrTypeFloat64 {
			t.Fatalf("expected float64 domain tags, got min=%v max=%v", stats.MinType, stats.MaxType)
		}
		if stats.Min64 != 1.5 || stats.Max64 != 2.5 {
			t.Fatalf("expected [1.5, 2.5], got [%f, %f]", stats.Min64, stats.Max64)
		}
	})
}
