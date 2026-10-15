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

	if err := db.AddEdge("harga_cabai", "sebab", "inflasi_pangan", 0.8); err != nil {
		t.Fatal(err)
	}
	if err := db.AddEdge("gagal_panen", "sebab", "inflasi_pangan", 0.5); err != nil {
		t.Fatal(err)
	}
	if err := db.AddEdge("harga_cabai", "akibat", "gagal_panen", 0.3); err != nil {
		t.Fatal(err)
	}

	// Idempotent replace: same (source, relation, target) updates weight in place.
	if err := db.AddEdge("harga_cabai", "sebab", "inflasi_pangan", 0.95); err != nil {
		t.Fatal(err)
	}
	if got := db.EdgeCount(); got != 3 {
		t.Fatalf("EdgeCount = %d, want 3", got)
	}

	out := db.OutgoingEdges("harga_cabai", "")
	if len(out) != 2 {
		t.Fatalf("outgoing all = %d, want 2", len(out))
	}
	outSebab := db.OutgoingEdges("harga_cabai", "sebab")
	if len(outSebab) != 1 || outSebab[0].Weight != 0.95 || outSebab[0].Target != "inflasi_pangan" {
		t.Fatalf("outgoing sebab = %+v", outSebab)
	}

	in := db.IncomingEdges("inflasi_pangan", "")
	if len(in) != 2 {
		t.Fatalf("incoming = %d, want 2", len(in))
	}

	if err := db.RemoveEdge("harga_cabai", "sebab", "inflasi_pangan"); err != nil {
		t.Fatal(err)
	}
	if got := db.EdgeCount(); got != 2 {
		t.Fatalf("EdgeCount after remove = %d, want 2", got)
	}
	if got := db.OutgoingEdges("harga_cabai", "sebab"); len(got) != 0 {
		t.Fatalf("removed edge still visible: %+v", got)
	}
	if err := db.RemoveEdge("harga_cabai", "sebab", "inflasi_pangan"); err == nil {
		t.Fatal("expected error removing missing edge")
	}
}

func TestEdgeValidation(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "edges.anchordb"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.AddEdge("", "sebab", "x", 1); err == nil {
		t.Fatal("expected empty source error")
	}
	if err := db.AddEdge("a", "", "x", 1); err == nil {
		t.Fatal("expected empty relation error")
	}
	if err := db.AddEdge("a", "sebab", "", 1); err == nil {
		t.Fatal("expected empty target error")
	}
	if err := db.AddEdge("a", "sebab", "x", float32(math.NaN())); err == nil {
		t.Fatal("expected NaN weight error")
	}
	if err := db.AddEdge("a", "sebab", "x", float32(math.Inf(1))); err == nil {
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
	if err := db.AddEdge("a", "sebab", "b", 0.7); err != nil {
		t.Fatal(err)
	}
	if err := db.AddEdge("b", "sebab", "c", 0.4); err != nil {
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
	if got := reopened.OutgoingEdges("a", "sebab"); len(got) != 1 || got[0].Target != "b" || got[0].Weight != 0.7 {
		t.Fatalf("edge a->b lost: %+v", got)
	}
}

func TestEdgeWALRecoveryWithoutSync(t *testing.T) {
	path := filepath.Join(t.TempDir(), "edges.anchordb")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.AddEdge("x", "sebab", "y", 0.6); err != nil {
		t.Fatal(err)
	}
	if err := db.AddEdge("z", "sebab", "y", 0.2); err != nil {
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
	if got := reopened.IncomingEdges("y", "sebab"); len(got) != 2 {
		t.Fatalf("incoming y from WAL = %d, want 2", len(got))
	}
}

func TestEdgeRemovalPersistsAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "edges.anchordb")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.AddEdge("a", "sebab", "b", 1); err != nil {
		t.Fatal(err)
	}
	if err := db.AddEdge("a", "sebab", "c", 1); err != nil {
		t.Fatal(err)
	}
	if err := db.RemoveEdge("a", "sebab", "b"); err != nil {
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
	out := reopened.OutgoingEdges("a", "sebab")
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
	if err := db.AddEdge("a", "sebab", "b", 1); !errors.Is(err, ErrDatabaseClosed) {
		t.Fatalf("AddEdge on closed db = %v, want ErrDatabaseClosed", err)
	}
	if err := db.RemoveEdge("a", "sebab", "b"); !errors.Is(err, ErrDatabaseClosed) {
		t.Fatalf("RemoveEdge on closed db = %v, want ErrDatabaseClosed", err)
	}
}

func TestExecuteCausalStepAndChain(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "causal.anchordb"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// cabai -> inflasi (0.9); gagal_panen -> inflasi (0.4); inflasi -> krisis (0.7)
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(db.AddEdge("cabai", "sebab", "inflasi", 0.9))
	must(db.AddEdge("gagal_panen", "sebab", "inflasi", 0.4))
	must(db.AddEdge("inflasi", "sebab", "krisis", 0.7))

	step, err := db.ExecuteCausalStep(context.Background(), []string{"cabai", "gagal_panen"}, "sebab", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(step) != 1 || step[0].Word != "inflasi" {
		t.Fatalf("causal step = %+v, want [inflasi]", step)
	}
	// Duplicate target collapses to the highest-weight incoming edge (0.9).
	if step[0].Score != 0.9 {
		t.Fatalf("collapsed score = %v, want 0.9", step[0].Score)
	}

	chain, err := db.ExecuteCausalChain(context.Background(), []string{"cabai"}, []string{"sebab", "sebab"}, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(chain) != 1 || chain[0].Word != "krisis" {
		t.Fatalf("causal chain = %+v, want [krisis]", chain)
	}
	if math.Abs(float64(chain[0].Score)-0.63) > 1e-6 {
		t.Fatalf("chain score = %v, want 0.63", chain[0].Score)
	}

	if _, err := db.ExecuteCausalStep(context.Background(), []string{"cabai"}, "sebab", -1); err == nil {
		t.Fatal("expected negative limitK error")
	}
	if _, err := db.ExecuteCausalStep(context.Background(), []string{"cabai"}, "", 10); err == nil {
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
	if err := wal.WriteEdge("a", "sebab", "b", 0.5); err != nil {
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
	if err := db.AddEdge("a", "sebab", "b", 1); err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
}
