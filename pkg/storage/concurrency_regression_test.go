package storage

import (
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"sync"
	"testing"

	"github.com/rachmanzz/anchordb/pkg/spatial"
)

func TestSync_ConcurrentDeletePreservesEntityMapping(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "remap_delete.anchordb")
	db, err := OpenWithOptions(dbPath, Options{WAL: WALOptions{SyncMode: SyncNone}})
	if err != nil {
		t.Fatalf("OpenWithOptions: %v", err)
	}
	defer func() { _ = db.Close() }()

	const total = 40
	for i := 0; i < total; i++ {
		name := fmt.Sprintf("rec-%02d", i)
		if _, err := db.SaveRecordTyped(name, []AttrEntry{NewInt64Attr("id", int64(i))}); err != nil {
			t.Fatalf("SaveRecordTyped(%s): %v", name, err)
		}
	}
	if err := db.Sync(); err != nil {
		t.Fatalf("initial Sync: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for round := 0; round < 5; round++ {
			_ = db.Sync()
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			name := fmt.Sprintf("rec-%02d", i)
			_, _ = db.DeleteRecords([]string{name})
		}
	}()
	wg.Wait()

	if err := db.Sync(); err != nil {
		t.Fatalf("final Sync: %v", err)
	}

	for i := 10; i < total; i++ {
		name := fmt.Sprintf("rec-%02d", i)
		cr, err := db.GetRecordTyped(name)
		if err != nil {
			t.Fatalf("GetRecordTyped(%s): %v", name, err)
		}
		got, ok, err := cr.GetInt("id")
		if err != nil || !ok || got != int64(i) {
			t.Fatalf("entity %s mapped to wrong value: got %d (ok=%v, err=%v), want %d", name, got, ok, err, i)
		}
	}
}

func TestClose_ConcurrentWithTriggerAutoSyncNoPanic(t *testing.T) {
	for round := 0; round < 50; round++ {
		dbPath := filepath.Join(t.TempDir(), fmt.Sprintf("close_%d.anchordb", round))
		db, err := OpenWithOptions(dbPath, Options{WAL: WALOptions{SyncMode: SyncNone}})
		if err != nil {
			t.Fatalf("OpenWithOptions: %v", err)
		}
		if _, err := db.SaveRecordTyped("item", []AttrEntry{NewFloat64Attr("x", 1.5)}); err != nil {
			t.Fatalf("SaveRecordTyped: %v", err)
		}

		var wg sync.WaitGroup
		for g := 0; g < 3; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				db.TriggerAutoSync()
				if err := db.Close(); err != nil {
					t.Errorf("concurrent Close returned error: %v", err)
				}
			}()
		}
		wg.Wait()
	}
}

func TestWAL_SyncErrorRejectsSingleEntryWrites(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "sync_err.wal")
	wal, err := OpenWALWithOptions(walPath, WALOptions{SyncMode: SyncNone})
	if err != nil {
		t.Fatalf("OpenWALWithOptions: %v", err)
	}
	defer func() { _ = wal.Close() }()

	simulated := errors.New("simulated disk io error")
	wal.mu.Lock()
	wal.syncErr = simulated
	wal.mu.Unlock()

	err = wal.WriteSave("a", CompactRecord{Operation: OpStatusActive})
	if !errors.Is(err, simulated) {
		t.Fatalf("expected WriteSave to propagate w.syncErr, got %v", err)
	}

	wal.mu.Lock()
	wal.syncErr = nil
	wal.corrupt = true
	wal.mu.Unlock()

	batchErr := wal.WriteSaveBatch([]BatchSaveItem{{Name: "a", CR: CompactRecord{Operation: OpStatusActive}}})
	if !errors.Is(batchErr, ErrWALCorrupt) {
		t.Fatalf("expected WriteSaveBatch to wrap ErrWALCorrupt, got %v", batchErr)
	}
	edgeErr := wal.WriteEdgeBatch([]EdgeRecord{{Source: "a", Relation: "r", Target: "b", Weight: 1}})
	if !errors.Is(edgeErr, ErrWALCorrupt) {
		t.Fatalf("expected WriteEdgeBatch to wrap ErrWALCorrupt, got %v", edgeErr)
	}
}

func TestSchema_FloatRejectsIntegersAndStatsSkipsNaN(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "schema_nan.anchordb")
	db, err := OpenWithOptions(dbPath, Options{WAL: WALOptions{SyncMode: SyncNone}})
	if err != nil {
		t.Fatalf("OpenWithOptions: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.AttachSchema(ModelSchema{
		"score": "float?",
		"v":     "float?",
	}); err != nil {
		t.Fatalf("AttachSchema: %v", err)
	}
	if _, err := db.SaveRecordTyped("bad-int", []AttrEntry{NewInt64Attr("score", 42)}); !errors.Is(err, ErrInvalidFieldType) {
		t.Fatalf("expected schema 'float' to reject int64 with ErrInvalidFieldType, got %v", err)
	}

	statsDBPath := filepath.Join(t.TempDir(), "stats_nan.anchordb")
	statsDB, err := OpenWithOptions(statsDBPath, Options{WAL: WALOptions{SyncMode: SyncNone}})
	if err != nil {
		t.Fatalf("OpenWithOptions statsDB: %v", err)
	}
	defer func() { _ = statsDB.Close() }()

	if _, err := statsDB.SaveRecordTyped("r1", []AttrEntry{NewFloat64Attr("v", 10)}); err != nil {
		t.Fatal(err)
	}
	if _, err := statsDB.SaveRecordTyped("r2", []AttrEntry{NewFloat64Attr("v", 20)}); err != nil {
		t.Fatal(err)
	}
	if _, err := statsDB.SaveRecordTyped("rnan", []AttrEntry{NewFloat64Attr("v", math.NaN())}); err != nil {
		t.Fatal(err)
	}
	stats, err := statsDB.ComputeStatsWithOptions(StatsOptions{Field: "v", Percentiles: []float64{0.5}})
	if err != nil {
		t.Fatalf("ComputeStatsWithOptions: %v", err)
	}
	if stats.Count != 2 || stats.Sum != 30 || stats.Mean != 15 || stats.Min64 != 10 || stats.Max64 != 20 || math.IsNaN(stats.StdDev) || stats.P50 != 10 {
		t.Fatalf("expected NaN excluded cleanly from all stats, got %+v", stats)
	}
}

func TestFilterAndCompute_MixedFloatAndUint64Precision(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "precision.anchordb")
	db, err := OpenWithOptions(dbPath, Options{WAL: WALOptions{SyncMode: SyncNone}})
	if err != nil {
		t.Fatalf("OpenWithOptions: %v", err)
	}
	defer func() { _ = db.Close() }()

	const bigA = int64((1 << 53) + 1) // 9007199254740993 (not exactly representable in float64)
	const bigB = int64((1 << 53) + 3) // 9007199254740995
	if _, err := db.SaveRecordTyped("a", []AttrEntry{NewInt64Attr("id", bigA), NewUint64Attr("u", 10)}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SaveRecordTyped("b", []AttrEntry{NewInt64Attr("id", bigB)}); err != nil {
		t.Fatal(err)
	}

	// Mixed float64 lower bound (1<<53 + 2.0 == 9007199254740994.0, exact in IEEE float64)
	// must reject bigA (<) and match bigB (>) without losing exact int64 ordering.
	res, _, err := db.ListRecordsWithPaging(QueryOptions{
		Filters: []FilterCondition{{
			Field:      "id",
			Operator:   "=",
			Value64:    float64((1 << 53) + 2),
			HasFloat64: true,
			ValType:    AttrTypeFloat64,
		}},
	})
	if err != nil || len(res) != 0 {
		t.Fatalf("expected 0 matches for exact equality against (1<<53)+2, got %d (err=%v)", len(res), err)
	}

	got, err := db.GetRecordTypedWithCompute("a", map[string]ComputeOp{
		"u2": OpAddDays{Field: "u", Days: 5},
	})
	if err != nil {
		t.Fatalf("GetRecordTypedWithCompute: %v", err)
	}
	attr, ok, err := got.GetTyped("u2")
	if err != nil || !ok || attr.Type != AttrTypeUint64 || attr.Uint64() != 15 {
		t.Fatalf("expected uint64 exact result 15, got %+v (ok=%v, err=%v)", attr, ok, err)
	}
}

func TestWALReplay_ExcludesEdgeCountFromHandledTotal(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "edge_replay.wal")
	wm, err := OpenWALWithOptions(walPath, WALOptions{SyncMode: SyncNone})
	if err != nil {
		t.Fatalf("OpenWALWithOptions: %v", err)
	}
	defer func() { _ = wm.Close() }()

	if err := wm.WriteSave("rec1", NewCompactRecord(OpStatusActive, []AttrEntry{NewInt64Attr("v", 1)})); err != nil {
		t.Fatalf("WriteSave: %v", err)
	}
	if err := wm.WriteEdge("rec1", "links", "rec2", 0.5); err != nil {
		t.Fatalf("WriteEdge: %v", err)
	}
	if err := wm.WriteEdgeDelete("rec1", "links", "rec2"); err != nil {
		t.Fatalf("WriteEdgeDelete: %v", err)
	}

	var legacyCallbacks int
	legacyCount, err := wm.Replay(func(op byte, name string, cr CompactRecord, word string, coord spatial.Point2D, roles [4]float32, domainID uint16) error {
		legacyCallbacks++
		return nil
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if legacyCount != 1 || legacyCallbacks != 1 {
		t.Fatalf("expected legacy Replay count=1 (edges excluded), got count=%d callbacks=%d", legacyCount, legacyCallbacks)
	}

	evCount, err := wm.ReplayEvents(func(WALEvent) error { return nil })
	if err != nil {
		t.Fatalf("ReplayEvents: %v", err)
	}
	if evCount != 3 {
		t.Fatalf("expected ReplayEvents count=3, got %d", evCount)
	}
}

func TestCompactRecord_ValidateRawDetectsCorruption(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "validate_raw.anchordb")
	db, err := OpenWithOptions(dbPath, Options{WAL: WALOptions{SyncMode: SyncNone}})
	if err != nil {
		t.Fatalf("OpenWithOptions: %v", err)
	}
	defer func() { _ = db.Close() }()

	if _, err := db.SaveRecordTyped("r1", []AttrEntry{NewInt64Attr("k", 42)}); err != nil {
		t.Fatalf("SaveRecordTyped: %v", err)
	}
	if err := db.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	cr, err := db.GetRecordTyped("r1")
	if err != nil {
		t.Fatalf("GetRecordTyped: %v", err)
	}
	if err := cr.ValidateRaw(); err != nil {
		t.Fatalf("expected valid CompactRecord.Raw to validate, got %v", err)
	}

	corrupt := CompactRecord{Raw: append([]byte(nil), cr.Raw[:len(cr.Raw)-2]...)}
	if err := corrupt.ValidateRaw(); err == nil {
		t.Fatal("expected ValidateRaw to reject truncated TLV payload")
	}

	// Corrupt typed attribute tag to invalid type (99).
	badTag := CompactRecord{Raw: []byte{0x01, 0x80, 99, 0x01, 0x00, 'a', 0, 0, 0, 0}}
	if err := badTag.ValidateRaw(); err == nil {
		t.Fatal("expected ValidateRaw to reject unknown AttrType 99")
	}
}
