package storage

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/rachmanzz/anchordb/pkg/spatial"
)

// BenchmarkWAL_SyncModes benchmarks WAL write throughput across the 3 durability modes:
// SyncAlways (fsync per write), SyncInterval (periodic fsync), and SyncNone (OS cache).
func BenchmarkWAL_SyncNone(b *testing.B) {
	path := filepath.Join(b.TempDir(), "bench_none.anchordb")
	db, err := OpenWithOptions(path, Options{WAL: WALOptions{SyncMode: SyncNone}})
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer db.Close()

	props := map[string]float32{"distance": 100, "duration": 2}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = db.SaveRecord(fmt.Sprintf("rec_%d", i), props)
	}
}

func BenchmarkWAL_SyncInterval(b *testing.B) {
	path := filepath.Join(b.TempDir(), "bench_interval.anchordb")
	db, err := OpenWithOptions(path, Options{WAL: WALOptions{
		SyncMode:     SyncInterval,
		SyncInterval: 50 * time.Millisecond,
	}})
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer db.Close()

	props := map[string]float32{"distance": 100, "duration": 2}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = db.SaveRecord(fmt.Sprintf("rec_%d", i), props)
	}
}

func BenchmarkWAL_SyncAlways(b *testing.B) {
	path := filepath.Join(b.TempDir(), "bench_always.anchordb")
	db, err := OpenWithOptions(path, Options{WAL: WALOptions{SyncMode: SyncAlways}})
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer db.Close()

	props := map[string]float32{"distance": 100, "duration": 2}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = db.SaveRecord(fmt.Sprintf("rec_%d", i), props)
	}
}

// BenchmarkInStorageCompute_Arithmetic measures raw CPU latency of in-storage formula evaluation.
func BenchmarkInStorageCompute_Arithmetic(b *testing.B) {
	props := map[string]float32{
		"distance": 120.0,
		"duration": 2.0,
	}
	op := OpDivide{FieldA: "distance", FieldB: "duration"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = op.Apply(props)
	}
}

// BenchmarkSpatial_TraverseRayDDA measures throughput of Fast Voxel DDA traversal across the 256x256 grid.
func BenchmarkSpatial_TraverseRayDDA(b *testing.B) {
	ray := spatial.Ray{
		Origin:    spatial.Point2D{X: -0.8, Y: -0.8},
		Direction: spatial.Point2D{X: 1.0, Y: 1.0},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		TraverseRayDDA(ray, GridDim, 1.5, func(_ CellCoord) bool {
			return true
		})
	}
}

// BenchmarkGraph_ExecuteCausalChain measures multi-hop causal reasoning traversal across chained edges.
func BenchmarkGraph_ExecuteCausalChain(b *testing.B) {
	path := filepath.Join(b.TempDir(), "bench_graph.anchordb")
	db, err := OpenWithOptions(path, Options{WAL: WALOptions{SyncMode: SyncNone}})
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// Seed chain: step0 -> step1 -> step2 -> step3 -> step4
	for i := 0; i < 4; i++ {
		_ = db.AddEdge(fmt.Sprintf("step%d", i), "triggers", fmt.Sprintf("step%d", i+1), 0.95)
	}

	ctx := context.Background()
	seeds := []string{"step0"}
	relations := []string{"triggers", "triggers", "triggers", "triggers"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = db.ExecuteCausalChain(ctx, seeds, relations, 5)
	}
}
