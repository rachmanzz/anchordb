package storage

import (
	"errors"
	"math"
	"path/filepath"
	"sort"
	"strconv"
	"testing"
)

func referenceQuantile(t *testing.T, vals []float64, p float64) float64 {
	t.Helper()
	if len(vals) == 0 {
		t.Fatal("reference needs values")
	}
	cp := append([]float64(nil), vals...)
	sort.Float64s(cp)
	k := int(math.Ceil(p*float64(len(cp)))) - 1
	if k < 0 {
		k = 0
	}
	if k > len(cp)-1 {
		k = len(cp) - 1
	}
	return cp[k]
}

func TestSelectKthFloat64(t *testing.T) {
	shapes := map[string][]float64{
		"sorted":   {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
		"reverse":  {20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1},
		"dupes":    {5, 5, 5, 5, 1, 5, 9, 5, 5, 5, 5, 5, 5, 5, 5},
		"single":   {42},
		"negative": {-3.5, -100, 0, 7.25, -0.5, 42, 1e9, -1e9},
		"fraction": {0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9},
	}
	for name, vals := range shapes {
		sorted := append([]float64(nil), vals...)
		sort.Float64s(sorted)
		for k := 0; k < len(vals); k++ {
			cp := append([]float64(nil), vals...)
			if got := selectKthFloat64(cp, k); got != sorted[k] {
				t.Fatalf("%s k=%d: got %v want %v", name, k, got, sorted[k])
			}
		}
	}
}

func TestQuantileRankEdges(t *testing.T) {
	if got := quantileRank(0.5, 1); got != 0 {
		t.Fatalf("N=1: got %d", got)
	}
	if got := quantileRank(0.99, 100); got != 98 {
		t.Fatalf("P99 N=100: got %d", got)
	}
	if got := quantileRank(0.0, 10); got != 0 {
		t.Fatalf("p=0: got %d", got)
	}
	if got := quantileRank(1.0, 10); got != 9 {
		t.Fatalf("p=1: got %d", got)
	}
}

func TestComputeStats_Percentiles(t *testing.T) {
	seed := func(t *testing.T, name string, vals []float64) *AnchorDB {
		t.Helper()
		db, err := Open(filepath.Join(t.TempDir(), name+".anchordb"))
		if err != nil {
			t.Fatalf("Open failed: %v", err)
		}
		t.Cleanup(func() { db.Close() })
		for i, v := range vals {
			if _, err := db.SaveRecordTyped("r"+strconv.Itoa(i), []AttrEntry{NewFloat64Attr("v", v)}); err != nil {
				t.Fatalf("save failed: %v", err)
			}
		}
		return db
	}

	t.Run("matches sort reference on three distributions", func(t *testing.T) {
		dists := map[string][]float64{
			"uniform": {5, 1, 9, 3, 7, 2, 8, 4, 6, 10},
			"skew":    {1, 1, 1, 1, 1, 1, 1, 1, 1, 100},
			"withneg": {-50, -10, 0, 0.5, 10, 20, 30, 40, 50, 60},
			"hundred": func() []float64 {
				v := make([]float64, 100)
				for i := range v {
					v[i] = float64((i*37)%100) + 0.25
				}
				return v
			}(),
		}
		for name, vals := range dists {
			db := seed(t, "pct_"+name, vals)
			stats, err := db.ComputeStatsWithOptions(StatsOptions{Field: "v", Percentiles: []float64{0.5, 0.9, 0.99}})
			if err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			if stats.P50 != referenceQuantile(t, vals, 0.5) || stats.P90 != referenceQuantile(t, vals, 0.9) || stats.P99 != referenceQuantile(t, vals, 0.99) {
				t.Fatalf("%s: got P50=%v P90=%v P99=%v", name, stats.P50, stats.P90, stats.P99)
			}
		}
	})

	t.Run("empty and degenerate populations", func(t *testing.T) {
		db := seed(t, "pct_empty", nil)
		stats, err := db.ComputeStatsWithOptions(StatsOptions{Field: "v", Percentiles: []float64{0.5}})
		if err != nil {
			t.Fatalf("empty: %v", err)
		}
		if stats.Count != 0 || stats.P50 != 0 {
			t.Fatalf("empty population must yield zeros, got %+v", stats)
		}

		db1 := seed(t, "pct_one", []float64{7.5})
		s1, err := db1.ComputeStatsWithOptions(StatsOptions{Field: "v", Percentiles: []float64{0.5, 0.9, 0.99}})
		if err != nil {
			t.Fatalf("single: %v", err)
		}
		if s1.P50 != 7.5 || s1.P90 != 7.5 || s1.P99 != 7.5 {
			t.Fatalf("single value must repeat, got %+v", s1)
		}

		dbn := seed(t, "pct_nan", []float64{1, 2, 3})
		if _, err := dbn.SaveRecordTyped("nan", []AttrEntry{NewFloat64Attr("v", math.NaN())}); err != nil {
			t.Fatalf("save NaN failed: %v", err)
		}
		sn, err := dbn.ComputeStatsWithOptions(StatsOptions{Field: "v", Percentiles: []float64{0.5}})
		if err != nil {
			t.Fatalf("nan: %v", err)
		}
		if sn.P50 != 2 {
			t.Fatalf("NaN must be excluded from percentile population, got P50=%v", sn.P50)
		}
	})

	t.Run("not requested means zero and unchanged behavior", func(t *testing.T) {
		db := seed(t, "pct_off", []float64{1, 2, 3, 4, 5})
		stats, err := db.ComputeStats("v")
		if err != nil {
			t.Fatalf("compute: %v", err)
		}
		if stats.P50 != 0 || stats.P90 != 0 || stats.P99 != 0 {
			t.Fatalf("unrequested quantiles must stay zero, got %+v", stats)
		}
		if stats.Mean != 3 || stats.Count != 5 {
			t.Fatalf("legacy aggregates must not move, got %+v", stats)
		}
	})

	t.Run("invalid requests fail fast", func(t *testing.T) {
		db := seed(t, "pct_bad", []float64{1, 2, 3})
		for _, ps := range [][]float64{{1.5}, {-0.1}, {math.NaN()}, {0.95}, {0.5, 0.75}} {
			if _, err := db.ComputeStatsWithOptions(StatsOptions{Field: "v", Percentiles: ps}); err == nil {
				t.Fatalf("expected error for percentiles %v", ps)
			}
		}
	})

	t.Run("large integers differentiate when float images differ", func(t *testing.T) {
		db := seed(t, "pct_u64", []float64{float64(uint64(math.MaxUint64 - 4096)), float64(uint64(math.MaxUint64))})
		stats, err := db.ComputeStatsWithOptions(StatsOptions{Field: "v", Percentiles: []float64{0.5, 0.9, 0.99}})
		if err != nil {
			t.Fatalf("compute: %v", err)
		}
		if stats.P50 == stats.P99 {
			t.Fatalf("representable large integers must differentiate, both %v", stats.P50)
		}
	})

	t.Run("unrepresentable collapse is documented behavior", func(t *testing.T) {
		db := seed(t, "pct_collapse", []float64{float64(uint64(math.MaxUint64 - 1000)), float64(uint64(math.MaxUint64))})
		stats, err := db.ComputeStatsWithOptions(StatsOptions{Field: "v", Percentiles: []float64{0.5, 0.99}})
		if err != nil {
			t.Fatalf("compute: %v", err)
		}
		if stats.P50 != stats.P99 {
			t.Fatalf("values rounding to one float64 cannot split: P50=%v P99=%v", stats.P50, stats.P99)
		}
	})

	t.Run("filters scope the percentile population", func(t *testing.T) {
		db := seed(t, "pct_filter", []float64{1, 2, 3, 100, 200})
		stats, err := db.ComputeStatsWithOptions(StatsOptions{
			Field:       "v",
			Percentiles: []float64{0.5},
			Filters:     []FilterCondition{{Field: "v", Operator: "<", Value: 50}},
		})
		if err != nil {
			t.Fatalf("compute: %v", err)
		}
		if stats.Count != 3 || stats.P50 != 2 {
			t.Fatalf("filtered P50 must be median of {1,2,3}, got %+v", stats)
		}
	})
}

func sumBins(bins []uint32) uint64 {
	var s uint64
	for _, b := range bins {
		s += uint64(b)
	}
	return s
}

func TestComputeHistogram(t *testing.T) {
	seed := func(t *testing.T, name string, vals []float64) *AnchorDB {
		t.Helper()
		db, err := Open(filepath.Join(t.TempDir(), name+".anchordb"))
		if err != nil {
			t.Fatalf("Open failed: %v", err)
		}
		t.Cleanup(func() { db.Close() })
		for i, v := range vals {
			if _, err := db.SaveRecordTyped("r"+strconv.Itoa(i), []AttrEntry{NewFloat64Attr("v", v)}); err != nil {
				t.Fatalf("save failed: %v", err)
			}
		}
		return db
	}

	t.Run("bin counts sum to Count", func(t *testing.T) {
		dists := map[string][]float64{
			"uniform": {5, 1, 9, 3, 7, 2, 8, 4, 6, 10},
			"skew":    {1, 1, 1, 1, 1, 1, 1, 1, 1, 100},
			"floats":  {0.1, 0.2, 0.35, 1.5, 2.75, 3.125, 9.99},
		}
		for name, vals := range dists {
			db := seed(t, "hist_"+name, vals)
			h, err := db.ComputeHistogram("v", 4)
			if err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			if h.Count != uint64(len(vals)) || sumBins(h.Bins) != h.Count {
				t.Fatalf("%s: sum(bins) must equal Count, got %+v", name, h)
			}
			if len(h.Bins) != 4 || len(h.Edges) != 5 {
				t.Fatalf("%s: wrong shape, got %+v", name, h)
			}
		}
	})

	t.Run("max lands in last bin with exact upper edge", func(t *testing.T) {
		db := seed(t, "hist_max", []float64{0, 10})
		h, err := db.ComputeHistogram("v", 10)
		if err != nil {
			t.Fatalf("compute: %v", err)
		}
		if h.Bins[9] != 1 || h.Bins[0] != 1 {
			t.Fatalf("endpoints must sit in edge bins, got %+v", h.Bins)
		}
		if h.Edges[10] != 10 {
			t.Fatalf("last edge must equal Max exactly, got %v", h.Edges[10])
		}
	})

	t.Run("degenerate and invalid inputs", func(t *testing.T) {
		db := seed(t, "hist_one", []float64{5, 5, 5})
		h, err := db.ComputeHistogram("v", 4)
		if err != nil {
			t.Fatalf("compute: %v", err)
		}
		if h.Count != 3 || len(h.Bins) != 1 || h.Bins[0] != 3 {
			t.Fatalf("single value must collapse to one full bin, got %+v", h)
		}

		dbe := seed(t, "hist_empty", nil)
		he, err := dbe.ComputeHistogram("v", 4)
		if err != nil {
			t.Fatalf("empty: %v", err)
		}
		if he.Count != 0 || he.Bins != nil {
			t.Fatalf("empty population must yield empty histogram, got %+v", he)
		}

		for _, bad := range []int{0, -3} {
			if _, err := db.ComputeHistogram("v", bad); err == nil {
				t.Fatalf("expected error for bins=%d", bad)
			}
		}
		if _, err := db.ComputeHistogram("", 4); err == nil {
			t.Fatalf("expected error for empty field")
		}
	})

	t.Run("NaN excluded from bins and Count", func(t *testing.T) {
		db := seed(t, "hist_nan", []float64{1, 2, 3})
		if _, err := db.SaveRecordTyped("nan", []AttrEntry{NewFloat64Attr("v", math.NaN())}); err != nil {
			t.Fatalf("save NaN failed: %v", err)
		}
		h, err := db.ComputeHistogram("v", 2)
		if err != nil {
			t.Fatalf("compute: %v", err)
		}
		if h.Count != 3 || sumBins(h.Bins) != 3 {
			t.Fatalf("NaN must be excluded, got %+v", h)
		}
	})

	t.Run("closed database rejected", func(t *testing.T) {
		db := seed(t, "hist_closed", []float64{1, 2, 3})
		if err := db.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
		if _, err := db.ComputeHistogram("v", 2); !errors.Is(err, ErrDatabaseClosed) {
			t.Fatalf("expected ErrDatabaseClosed, got %v", err)
		}
	})

	t.Run("filters scope the binned population", func(t *testing.T) {
		db := seed(t, "hist_filter", []float64{1, 2, 3, 100, 200})
		h, err := db.ComputeHistogramWithOptions(HistogramOptions{
			Field:   "v",
			Bins:    2,
			Filters: []FilterCondition{{Field: "v", Operator: "<", Value: 50}},
		})
		if err != nil {
			t.Fatalf("compute: %v", err)
		}
		if h.Count != 3 || sumBins(h.Bins) != 3 {
			t.Fatalf("filtered histogram must cover {1,2,3} only, got %+v", h)
		}
	})
}

func outlierNames(recs []EntityRecord) []string {
	names := make([]string, 0, len(recs))
	for _, r := range recs {
		names = append(names, r.Name)
	}
	sort.Strings(names)
	return names
}

func TestFilterZScore(t *testing.T) {
	seed := func(t *testing.T, name string, vals []float64) *AnchorDB {
		t.Helper()
		db, err := Open(filepath.Join(t.TempDir(), name+".anchordb"))
		if err != nil {
			t.Fatalf("Open failed: %v", err)
		}
		t.Cleanup(func() { db.Close() })
		for i, v := range vals {
			if _, err := db.SaveRecordTyped("r"+strconv.Itoa(i), []AttrEntry{NewFloat64Attr("v", v)}); err != nil {
				t.Fatalf("save failed: %v", err)
			}
		}
		return db
	}

	t.Run("injected outlier recalled at 2 sigma, clean at 3", func(t *testing.T) {
		db := seed(t, "z_inject", []float64{10, 10, 10, 10, 10, 100})
		got, err := db.FilterZScore("v", 2.0)
		if err != nil {
			t.Fatalf("filter: %v", err)
		}
		if names := outlierNames(got); len(names) != 1 || names[0] != "r5" {
			t.Fatalf("expected only r5, got %v", names)
		}
		got, err = db.FilterZScore("v", 3.0)
		if err != nil {
			t.Fatalf("filter: %v", err)
		}
		if len(got) != 0 {
			t.Fatalf("expected no outliers at 3 sigma, got %v", outlierNames(got))
		}
	})

	t.Run("zero variance and empty yield empty without error", func(t *testing.T) {
		db := seed(t, "z_const", []float64{5, 5, 5})
		got, err := db.FilterZScore("v", 1.0)
		if err != nil {
			t.Fatalf("filter: %v", err)
		}
		if len(got) != 0 {
			t.Fatalf("constant column must yield no outliers, got %v", outlierNames(got))
		}
		dbe := seed(t, "z_empty", nil)
		got, err = dbe.FilterZScore("v", 1.0)
		if err != nil {
			t.Fatalf("empty: %v", err)
		}
		if len(got) != 0 {
			t.Fatalf("empty population must yield empty, got %v", outlierNames(got))
		}
	})

	t.Run("invalid config rejected", func(t *testing.T) {
		db := seed(t, "z_bad", []float64{1, 2, 3})
		for _, th := range []float64{-1, math.NaN()} {
			if _, err := db.FilterZScore("v", th); err == nil {
				t.Fatalf("expected error for threshold %v", th)
			}
		}
		if _, err := db.FilterZScore("", 2.0); err == nil {
			t.Fatalf("expected error for empty field")
		}
	})

	t.Run("tombstoned records excluded", func(t *testing.T) {
		db := seed(t, "z_tomb", []float64{10, 10, 10, 10, 10, 100})
		if _, err := db.DeleteRecords([]string{"r5"}); err != nil {
			t.Fatalf("delete: %v", err)
		}
		got, err := db.FilterZScore("v", 0.5)
		if err != nil {
			t.Fatalf("filter: %v", err)
		}
		for _, n := range outlierNames(got) {
			if n == "r5" {
				t.Fatalf("tombstoned record must not match, got %v", outlierNames(got))
			}
		}
	})

	t.Run("closed database rejected", func(t *testing.T) {
		db := seed(t, "z_closed", []float64{1, 2, 3})
		if err := db.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
		if _, err := db.FilterZScore("v", 1.0); !errors.Is(err, ErrDatabaseClosed) {
			t.Fatalf("expected ErrDatabaseClosed, got %v", err)
		}
		if _, err := db.FilterIQROutlier("v", 1.5); !errors.Is(err, ErrDatabaseClosed) {
			t.Fatalf("expected ErrDatabaseClosed, got %v", err)
		}
	})
}

func TestFilterIQROutlier(t *testing.T) {
	seed := func(t *testing.T, name string, vals []float64) *AnchorDB {
		t.Helper()
		db, err := Open(filepath.Join(t.TempDir(), name+".anchordb"))
		if err != nil {
			t.Fatalf("Open failed: %v", err)
		}
		t.Cleanup(func() { db.Close() })
		for i, v := range vals {
			if _, err := db.SaveRecordTyped("r"+strconv.Itoa(i), []AttrEntry{NewFloat64Attr("v", v)}); err != nil {
				t.Fatalf("save failed: %v", err)
			}
		}
		return db
	}

	t.Run("fences catch the far value", func(t *testing.T) {
		db := seed(t, "iqr_basic", []float64{1, 2, 3, 4, 5, 6, 7, 8, 100})
		got, err := db.FilterIQROutlier("v", 1.5)
		if err != nil {
			t.Fatalf("filter: %v", err)
		}
		if names := outlierNames(got); len(names) != 1 || names[0] != "r8" {
			t.Fatalf("expected only r8, got %v", names)
		}
	})

	t.Run("zero IQR and invalid multiplier", func(t *testing.T) {
		db := seed(t, "iqr_const", []float64{4, 4, 4, 4})
		got, err := db.FilterIQROutlier("v", 1.5)
		if err != nil {
			t.Fatalf("filter: %v", err)
		}
		if len(got) != 0 {
			t.Fatalf("zero IQR must yield empty, got %v", outlierNames(got))
		}
		for _, k := range []float64{0, -1, math.NaN()} {
			if _, err := db.FilterIQROutlier("v", k); err == nil {
				t.Fatalf("expected error for k=%v", k)
			}
		}
		if _, err := db.FilterIQROutlier("", 1.5); err == nil {
			t.Fatalf("expected error for empty field")
		}
	})
}

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

func BenchmarkComputeStats_Percentiles_100k(b *testing.B) {
	dir := b.TempDir()
	dbPath := filepath.Join(dir, "bench_stats_pct.anchordb")
	db, _ := Open(dbPath)
	defer db.Close()

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
		_, _ = db.ComputeStatsWithOptions(StatsOptions{Field: "voltage", Percentiles: []float64{0.5, 0.9, 0.99}})
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
