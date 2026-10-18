package storage

import (
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/rachmanzz/anchordb/pkg/spatial"
)

func TestEdgeAddQueryReplaceRemove(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "edges.anchordb"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// A rational causal graph: a crop failure and a fuel price rise both cause
	// food inflation, and a crop failure can also trigger a food crisis.
	if err := db.AddEdge("crop_failure", "causes", "food_inflation", 0.8); err != nil {
		t.Fatal(err)
	}
	if err := db.AddEdge("fuel_price_rise", "causes", "food_inflation", 0.5); err != nil {
		t.Fatal(err)
	}
	if err := db.AddEdge("crop_failure", "triggers", "food_crisis", 0.3); err != nil {
		t.Fatal(err)
	}

	// Idempotent replace: same (source, relation, target) updates weight in place.
	if err := db.AddEdge("crop_failure", "causes", "food_inflation", 0.95); err != nil {
		t.Fatal(err)
	}
	if got := db.EdgeCount(); got != 3 {
		t.Fatalf("EdgeCount = %d, want 3", got)
	}

	out := db.OutgoingEdges("crop_failure", "")
	if len(out) != 2 {
		t.Fatalf("outgoing all = %d, want 2", len(out))
	}
	outCauses := db.OutgoingEdges("crop_failure", "causes")
	if len(outCauses) != 1 || outCauses[0].Weight != 0.95 || outCauses[0].Target != "food_inflation" {
		t.Fatalf("outgoing causes = %+v", outCauses)
	}

	in := db.IncomingEdges("food_inflation", "")
	if len(in) != 2 {
		t.Fatalf("incoming = %d, want 2", len(in))
	}

	if err := db.RemoveEdge("crop_failure", "causes", "food_inflation"); err != nil {
		t.Fatal(err)
	}
	if got := db.EdgeCount(); got != 2 {
		t.Fatalf("EdgeCount after remove = %d, want 2", got)
	}
	if got := db.OutgoingEdges("crop_failure", "causes"); len(got) != 0 {
		t.Fatalf("removed edge still visible: %+v", got)
	}
	if err := db.RemoveEdge("crop_failure", "causes", "food_inflation"); err == nil {
		t.Fatal("expected error removing missing edge")
	}
}

func TestEdgeValidation(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "edges.anchordb"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.AddEdge("", "causes", "x", 1); err == nil {
		t.Fatal("expected empty source error")
	}
	if err := db.AddEdge("a", "", "x", 1); err == nil {
		t.Fatal("expected empty relation error")
	}
	if err := db.AddEdge("a", "causes", "", 1); err == nil {
		t.Fatal("expected empty target error")
	}
	if err := db.AddEdge("a", "causes", "x", float32(math.NaN())); err == nil {
		t.Fatal("expected NaN weight error")
	}
	if err := db.AddEdge("a", "causes", "x", float32(math.Inf(1))); err == nil {
		t.Fatal("expected Inf weight error")
	}
}

func TestEdgePersistenceAcrossSyncReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "edges.anchordb")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.AppendNode("a", spatial.Point2D{}, [4]float32{}, 0); err != nil {
		t.Fatal(err)
	}
	if err := db.AddEdge("a", "causes", "b", 0.7); err != nil {
		t.Fatal(err)
	}
	if err := db.AddEdge("b", "causes", "c", 0.4); err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	if db.Header.Version != DBVersion {
		t.Fatalf("synced version = %d, want %d", db.Header.Version, DBVersion)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	if got := reopened.EdgeCount(); got != 2 {
		t.Fatalf("EdgeCount after reopen = %d, want 2", got)
	}
	if got := reopened.OutgoingEdges("a", "causes"); len(got) != 1 || got[0].Target != "b" || got[0].Weight != 0.7 {
		t.Fatalf("edge a->b lost: %+v", got)
	}
}

func TestEdgeWALRecoveryWithoutSync(t *testing.T) {
	path := filepath.Join(t.TempDir(), "edges.anchordb")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.AddEdge("x", "causes", "y", 0.6); err != nil {
		t.Fatal(err)
	}
	if err := db.AddEdge("z", "causes", "y", 0.2); err != nil {
		t.Fatal(err)
	}
	// Close WITHOUT Sync: recovery must come from WAL replay alone.
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	if got := reopened.EdgeCount(); got != 2 {
		t.Fatalf("EdgeCount from WAL = %d, want 2", got)
	}
	if got := reopened.IncomingEdges("y", "causes"); len(got) != 2 {
		t.Fatalf("incoming y from WAL = %d, want 2", len(got))
	}
}

func TestEdgeRemovalPersistsAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "edges.anchordb")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.AddEdge("a", "causes", "b", 1); err != nil {
		t.Fatal(err)
	}
	if err := db.AddEdge("a", "causes", "c", 1); err != nil {
		t.Fatal(err)
	}
	if err := db.RemoveEdge("a", "causes", "b"); err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	out := reopened.OutgoingEdges("a", "causes")
	if len(out) != 1 || out[0].Target != "c" {
		t.Fatalf("after reopen outgoing = %+v, want only a->c", out)
	}
}

func TestEdgeClosedDatabase(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "edges.anchordb"))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.AddEdge("a", "causes", "b", 1); !errors.Is(err, ErrDatabaseClosed) {
		t.Fatalf("AddEdge on closed db = %v, want ErrDatabaseClosed", err)
	}
	if err := db.RemoveEdge("a", "causes", "b"); !errors.Is(err, ErrDatabaseClosed) {
		t.Fatalf("RemoveEdge on closed db = %v, want ErrDatabaseClosed", err)
	}
}

func TestExecuteCausalStepAndChain(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "causal.anchordb"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// crop_failure -> food_inflation (0.9); fuel_price_rise -> food_inflation (0.4);
	// food_inflation -> food_crisis (0.7).
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(db.AddEdge("crop_failure", "causes", "food_inflation", 0.9))
	must(db.AddEdge("fuel_price_rise", "causes", "food_inflation", 0.4))
	must(db.AddEdge("food_inflation", "causes", "food_crisis", 0.7))

	step, err := db.ExecuteCausalStep(context.Background(), []string{"crop_failure", "fuel_price_rise"}, "causes", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(step) != 1 || step[0].Word != "food_inflation" {
		t.Fatalf("causal step = %+v, want [food_inflation]", step)
	}
	// Duplicate target collapses to the highest-weight incoming edge (0.9).
	if step[0].Score != 0.9 {
		t.Fatalf("collapsed score = %v, want 0.9", step[0].Score)
	}

	chain, err := db.ExecuteCausalChain(context.Background(), []string{"crop_failure"}, []string{"causes", "causes"}, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(chain) != 1 || chain[0].Word != "food_crisis" {
		t.Fatalf("causal chain = %+v, want [food_crisis]", chain)
	}
	if math.Abs(float64(chain[0].Score)-0.63) > 1e-6 {
		t.Fatalf("chain score = %v, want 0.63", chain[0].Score)
	}

	if _, err := db.ExecuteCausalStep(context.Background(), []string{"crop_failure"}, "causes", -1); err == nil {
		t.Fatal("expected negative limitK error")
	}
	if _, err := db.ExecuteCausalStep(context.Background(), []string{"crop_failure"}, "", 10); err == nil {
		t.Fatal("expected empty relation error")
	}
}

func TestLegacyReplayIgnoresEdges(t *testing.T) {
	wal, err := OpenWAL(filepath.Join(t.TempDir(), "edge.wal"))
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()

	if err := wal.WriteSave("saved", CompactRecord{Operation: OpStatusActive}); err != nil {
		t.Fatal(err)
	}
	if err := wal.WriteEdge("a", "causes", "b", 0.5); err != nil {
		t.Fatal(err)
	}

	// The legacy callback must still work and must not choke on edge opcodes.
	var names []string
	if _, err := wal.Replay(func(_ byte, name string, _ CompactRecord, _ string, _ spatial.Point2D, _ [4]float32, _ uint16) error {
		names = append(names, name)
		return nil
	}); err != nil {
		t.Fatalf("legacy Replay failed: %v", err)
	}
	if len(names) != 1 || names[0] != "saved" {
		t.Fatalf("legacy Replay delivered %v, want [saved]", names)
	}

	// ReplayEvents exposes the edge opcode to new callers.
	var edges []EdgeRecord
	if _, err := wal.ReplayEvents(func(ev WALEvent) error {
		if ev.Op == WalOpEdge {
			edges = append(edges, ev.Edge)
		}
		return nil
	}); err != nil {
		t.Fatalf("ReplayEvents failed: %v", err)
	}
	if len(edges) != 1 || edges[0].Source != "a" || edges[0].Target != "b" || edges[0].Weight != 0.5 {
		t.Fatalf("ReplayEvents edges = %+v", edges)
	}
}

// TestOpenLegacyVersion2File verifies that a database written by an older
// binary (header version 2, no edge section) is still openable.
func TestOpenLegacyVersion2File(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.anchordb")
	hdr := Header{
		Magic:        MagicHeader,
		Version:      2,
		NodeCount:    0,
		GridCellsX:   GridDim,
		GridCellsY:   GridDim,
		GridOffset:   HeaderSize,
		NodesOffset:  HeaderSize,
		StringOffset: HeaderSize,
		EntityCount:  0,
		EntityOffset: HeaderSize,
	}
	var buf [HeaderSize]byte
	hdr.Encode(buf[:])
	if err := os.WriteFile(path, buf[:], 0644); err != nil {
		t.Fatal(err)
	}

	db, err := Open(path)
	if err != nil {
		t.Fatalf("opening legacy v2 file failed: %v", err)
	}
	defer db.Close()
	if db.Header.Version != 2 {
		t.Fatalf("version = %d, want 2", db.Header.Version)
	}
	if got := db.EdgeCount(); got != 0 {
		t.Fatalf("legacy file should have 0 edges, got %d", got)
	}
	// New edges can be added to an upgraded file and persisted.
	if err := db.AddEdge("a", "causes", "b", 1); err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
}

func TestAddEdgesBatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "batch_edges.anchordb")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}

	items := []EdgeRecord{
		{Source: "h1", Relation: "rel1", Target: "t1", Weight: 0.1},
		{Source: "h1", Relation: "rel2", Target: "t2", Weight: 0.2},
		{Source: "h2", Relation: "rel1", Target: "t3", Weight: 0.3},
		{Source: "h1", Relation: "rel1", Target: "t1", Weight: 0.99}, // replacement
	}
	if err := db.AddEdgesBatch(items); err != nil {
		t.Fatalf("AddEdgesBatch failed: %v", err)
	}
	if got := db.EdgeCount(); got != 3 {
		t.Fatalf("EdgeCount = %d, want 3", got)
	}
	out := db.OutgoingEdges("h1", "rel1")
	if len(out) != 1 || out[0].Weight != 0.99 {
		t.Fatalf("unexpected out edge: %+v", out)
	}

	// Close without sync to verify WAL replay
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got := reopened.EdgeCount(); got != 3 {
		t.Fatalf("reopened EdgeCount = %d, want 3", got)
	}
	outReopened := reopened.OutgoingEdges("h1", "rel1")
	if len(outReopened) != 1 || outReopened[0].Weight != 0.99 {
		t.Fatalf("reopened out edge = %+v", outReopened)
	}
}
