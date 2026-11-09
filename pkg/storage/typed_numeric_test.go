package storage

import (
	"encoding/binary"
	"math"
	"os"
	"path/filepath"
	"testing"
	"unsafe"

	"github.com/rachmanzz/anchordb/pkg/spatial"
)

func TestAttrEntry_SizeAndZeroPointers(t *testing.T) {
	entrySize := unsafe.Sizeof(AttrEntry{})
	if entrySize != 32 {
		t.Fatalf("expected AttrEntry size 32 bytes, got %d", entrySize)
	}
}

func TestTypedNumeric_IntegerPrecisionLossPrevention(t *testing.T) {
	// A number above 2^24 (16,777,216)
	exactLargeInt := int64(100_000_001)
	exactTimestamp := int64(1_700_000_000_123_456)
	exactLargeUint := uint64(18_446_744_073_709_551_000)

	// In float32, largeInt loses exactness:
	f32 := float32(exactLargeInt)
	if int64(f32) == exactLargeInt {
		t.Fatalf("sanity check failed: float32 was expected to truncate %d", exactLargeInt)
	}

	// Create CompactRecord with typed attributes
	cr := CompactRecord{Operation: OpStatusActive}
	cr = cr.SetInt("counter", exactLargeInt)
	cr = cr.SetInt("timestamp", exactTimestamp)
	cr = cr.SetUint("uid", exactLargeUint)
	cr = cr.Set("ratio", 0.85)

	// Verify exact integer retrieval from in-memory Attrs
	cVal, ok := cr.GetInt("counter")
	if !ok || cVal != exactLargeInt {
		t.Fatalf("expected counter %d, got %d (ok=%v)", exactLargeInt, cVal, ok)
	}

	tsVal, ok := cr.GetInt("timestamp")
	if !ok || tsVal != exactTimestamp {
		t.Fatalf("expected timestamp %d, got %d (ok=%v)", exactTimestamp, tsVal, ok)
	}

	uidVal, ok := cr.GetUint("uid")
	if !ok || uidVal != exactLargeUint {
		t.Fatalf("expected uid %d, got %d (ok=%v)", exactLargeUint, uidVal, ok)
	}

	rVal, ok := cr.Get("ratio")
	if !ok || rVal != 0.85 {
		t.Fatalf("expected ratio 0.85, got %f (ok=%v)", rVal, ok)
	}
}

func TestTypedNumeric_DiskPersistenceAndMmap(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "typed_test.anchordb")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}

	exactLargeInt := int64(50_000_000_001)
	exactUint := uint64(999_888_777_666_555)

	attrs := []AttrEntry{
		NewInt64Attr("views", exactLargeInt),
		NewUint64Attr("user_id", exactUint),
		NewFloat32Attr("score", 99.5),
	}

	_, err = db.SaveRecordTyped("post_123", attrs)
	if err != nil {
		t.Fatalf("save typed record: %v", err)
	}

	// Flush to disk
	if err := db.Sync(); err != nil {
		t.Fatalf("sync db: %v", err)
	}

	if db.Header.Version != DBVersion {
		t.Fatalf("expected header version %d, got %d", DBVersion, db.Header.Version)
	}

	// Verify mmap zero-copy read on open db
	rec, err := db.GetRecordTyped("post_123")
	if err != nil {
		t.Fatalf("get record typed: %v", err)
	}

	vVal, ok := rec.GetInt("views")
	if !ok || vVal != exactLargeInt {
		t.Fatalf("mmap read views expected %d, got %d", exactLargeInt, vVal)
	}

	uVal, ok := rec.GetUint("user_id")
	if !ok || uVal != exactUint {
		t.Fatalf("mmap read user_id expected %d, got %d", exactUint, uVal)
	}

	sVal, ok := rec.Get("score")
	if !ok || sVal != 99.5 {
		t.Fatalf("mmap read score expected 99.5, got %f", sVal)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	// Reopen and verify reading from freshly mapped binary file
	reopenedDB, err := Open(dbPath)
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer reopenedDB.Close()

	rec2, err := reopenedDB.GetRecordTyped("post_123")
	if err != nil {
		t.Fatalf("reopened get record typed: %v", err)
	}

	vVal2, ok := rec2.GetInt("views")
	if !ok || vVal2 != exactLargeInt {
		t.Fatalf("reopened views expected %d, got %d", exactLargeInt, vVal2)
	}

	uVal2, ok := rec2.GetUint("user_id")
	if !ok || uVal2 != exactUint {
		t.Fatalf("reopened user_id expected %d, got %d", exactUint, uVal2)
	}

	sVal2, ok := rec2.Get("score")
	if !ok || sVal2 != 99.5 {
		t.Fatalf("reopened score expected 99.5, got %f", sVal2)
	}
}

func TestTypedNumeric_WALCrashRecovery(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "wal_typed_recovery.anchordb")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}

	exactLargeInt := int64(88_888_888_888)
	exactUint := uint64(12_345_678_901_234)

	attrs := []AttrEntry{
		NewInt64Attr("followers", exactLargeInt),
		NewUint64Attr("channel_id", exactUint),
	}

	// Save with Node to test combined WalOpSaveNode frame
	_, err = db.SaveRecordTypedWithNode("streamer_1", OpStatusActive, attrs, spatial.Point2D{X: 1.0, Y: 2.0}, [4]float32{0.1, 0.2, 0.3, 0.4})
	if err != nil {
		t.Fatalf("save record typed with node: %v", err)
	}

	// Deliberately simulate crash WITHOUT calling db.Sync()
	// Just close the DB file descriptors so WAL holds the mutations
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	// Reopen DB - WAL replay must restore the exact 64-bit integer values!
	recoveredDB, err := Open(dbPath)
	if err != nil {
		t.Fatalf("reopen db after crash: %v", err)
	}
	defer recoveredDB.Close()

	rec, err := recoveredDB.GetRecordTyped("streamer_1")
	if err != nil {
		t.Fatalf("get recovered record: %v", err)
	}

	fVal, ok := rec.GetInt("followers")
	if !ok || fVal != exactLargeInt {
		t.Fatalf("recovered followers expected %d, got %d", exactLargeInt, fVal)
	}

	cVal, ok := rec.GetUint("channel_id")
	if !ok || cVal != exactUint {
		t.Fatalf("recovered channel_id expected %d, got %d", exactUint, cVal)
	}
}

func TestTypedNumeric_LegacyV3BackwardCompatibility(t *testing.T) {
	// Build a simulated v3 file with legacy float32 attributes (RawTypedFlag not set)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "legacy_v3.anchordb")

	f, err := os.OpenFile(dbPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatal(err)
	}

	hdr := Header{
		Magic:        MagicHeader,
		Version:      3, // Legacy Version 3
		NodeCount:    0,
		GridCellsX:   GridDim,
		GridCellsY:   GridDim,
		GridOffset:   HeaderSize,
		NodesOffset:  HeaderSize,
		StringOffset: HeaderSize,
		EntityCount:  1,
		EntityOffset: HeaderSize,
	}

	var hBuf [HeaderSize]byte
	hdr.Encode(hBuf[:])
	if _, err := f.Write(hBuf[:]); err != nil {
		t.Fatal(err)
	}

	// Write legacy entity entry:
	// [2B nameLen][name "legacy_user"][1B op][2B attrCount = 1 (no RawTypedFlag)][2B kLen][k "legacy_score"][4B float32 42.5]
	var lBuf [2]byte
	var vBuf [4]byte
	name := "legacy_user"
	nameBytes := []byte(name)
	lBuf[0] = byte(len(nameBytes))
	lBuf[1] = 0
	f.Write(lBuf[:])
	f.Write(nameBytes)
	f.Write([]byte{OpStatusActive})

	// Attr count = 1 (legacy, no RawTypedFlag)
	lBuf[0] = 1
	lBuf[1] = 0
	f.Write(lBuf[:])

	kBytes := []byte("legacy_score")
	lBuf[0] = byte(len(kBytes))
	lBuf[1] = 0
	f.Write(lBuf[:])
	f.Write(kBytes)
	binary.LittleEndian.PutUint32(vBuf[:], math.Float32bits(42.5))
	f.Write(vBuf[:])

	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	f.Close()

	// Open the v3 file with the v4 engine
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open legacy v3 file: %v", err)
	}

	rec, err := db.GetRecordTyped("legacy_user")
	if err != nil {
		t.Fatalf("failed to get legacy record: %v", err)
	}

	val, ok := rec.Get("legacy_score")
	if !ok || val != 42.5 {
		t.Fatalf("expected legacy score 42.5, got %f (ok=%v)", val, ok)
	}

	// Now sync - it should cleanly upgrade file to DBVersion 4
	if err := db.Sync(); err != nil {
		t.Fatalf("sync legacy db upgrade: %v", err)
	}

	if db.Header.Version != DBVersion {
		t.Fatalf("expected upgraded header version %d, got %d", DBVersion, db.Header.Version)
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestTypedNumeric_CrossConversionPrecision(t *testing.T) {
	exactLargeInt := int64(77_000_000_001)
	exactLargeUint := uint64(88_000_000_002)

	cr := CompactRecord{Operation: OpStatusActive}
	cr = cr.SetUint("u_as_i", exactLargeUint)
	cr = cr.SetInt("i_as_u", exactLargeInt)

	// Cross-read u_as_i with GetInt()
	iVal, ok := cr.GetInt("u_as_i")
	if !ok || iVal != int64(exactLargeUint) {
		t.Fatalf("expected cross-conversion GetInt to yield %d, got %d", int64(exactLargeUint), iVal)
	}

	// Cross-read i_as_u with GetUint()
	uVal, ok := cr.GetUint("i_as_u")
	if !ok || uVal != uint64(exactLargeInt) {
		t.Fatalf("expected cross-conversion GetUint to yield %d, got %d", uint64(exactLargeInt), uVal)
	}
}

func TestTypedNumeric_KeyDeduplication(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "dedup_test.anchordb")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	attrs := []AttrEntry{
		NewInt64Attr("views", 10),
		NewFloat32Attr("ratio", 1.5),
		NewInt64Attr("views", 999), // Duplicated key: last write wins
	}

	_, err = db.SaveRecordTyped("post_dup", attrs)
	if err != nil {
		t.Fatalf("save typed: %v", err)
	}

	rec, err := db.GetRecordTyped("post_dup")
	if err != nil {
		t.Fatalf("get typed: %v", err)
	}

	if rec.AttrCount() != 2 {
		t.Fatalf("expected exactly 2 attributes after deduplication, got %d", rec.AttrCount())
	}

	viewsVal, ok := rec.GetInt("views")
	if !ok || viewsVal != 999 {
		t.Fatalf("expected last-write-wins value 999, got %d", viewsVal)
	}
}

func TestTypedNumeric_SchemaValidationOnTyped(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "schema_typed_test.anchordb")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	schema := ModelSchema{
		"views": "int",
		"uid":   "uint",
		"score": "float",
	}
	if err := db.AttachSchema(schema); err != nil {
		t.Fatalf("attach schema: %v", err)
	}

	// 1. Missing required field
	_, err = db.SaveRecordTyped("item1", []AttrEntry{
		NewInt64Attr("views", 100),
	})
	if err == nil {
		t.Fatal("expected error on missing required field, got nil")
	}

	// 2. Unknown field
	_, err = db.SaveRecordTyped("item2", []AttrEntry{
		NewInt64Attr("views", 100),
		NewUint64Attr("uid", 1),
		NewFloat32Attr("score", 4.5),
		NewFloat32Attr("bogus_field", 1.0),
	})
	if err == nil {
		t.Fatal("expected error on unknown field, got nil")
	}

	// 3. Valid large 64-bit integer values (> 2^24)
	largeViews := int64(50_000_000_000)
	largeUID := uint64(99_999_999_999)
	_, err = db.SaveRecordTyped("item3", []AttrEntry{
		NewInt64Attr("views", largeViews),
		NewUint64Attr("uid", largeUID),
		NewFloat32Attr("score", 9.8),
	})
	if err != nil {
		t.Fatalf("expected valid large integer to pass schema validation, got: %v", err)
	}
}

func TestTypedNumeric_ZeroAllocForEach(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "zero_alloc.anchordb")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}

	attrs := []AttrEntry{
		NewInt64Attr("counter", 100_000_000_001),
		NewUint64Attr("user_id", 888_777_666_555),
		NewFloat32Attr("metric", 3.1415),
	}
	_, err = db.SaveRecordTyped("rec_1", attrs)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}

	rec, err := db.GetRecordTyped("rec_1")
	if err != nil {
		t.Fatal(err)
	}
	if len(rec.Raw) == 0 {
		t.Fatal("expected synced record to point directly to mmap raw bytes")
	}

	// Verify zero allocations in ForEachAttrTyped
	allocsTyped := testing.AllocsPerRun(100, func() {
		count := 0
		rec.ForEachAttrTyped(func(attr AttrEntry) bool {
			count++
			return true
		})
	})
	if allocsTyped > 0 {
		t.Fatalf("expected 0 allocs per run for ForEachAttrTyped, got %f", allocsTyped)
	}

	// Verify zero allocations in ForEachAttr
	allocsFloat := testing.AllocsPerRun(100, func() {
		count := 0
		rec.ForEachAttr(func(k string, v float32) bool {
			count++
			return true
		})
	})
	if allocsFloat > 0 {
		t.Fatalf("expected 0 allocs per run for ForEachAttr, got %f", allocsFloat)
	}

	db.Close()
}

func TestTypedNumeric_CorruptTypeTagFailClosed(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "corrupt_tag.anchordb")

	f, err := os.OpenFile(dbPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatal(err)
	}

	hdr := Header{
		Magic:        MagicHeader,
		Version:      DBVersion,
		EntityCount:  1,
		EntityOffset: HeaderSize,
	}
	var hBuf [HeaderSize]byte
	hdr.Encode(hBuf[:])
	f.Write(hBuf[:])

	// Write entity with invalid type tag 0xFF
	var lBuf [2]byte
	name := "corrupt_node"
	nameBytes := []byte(name)
	binary.LittleEndian.PutUint16(lBuf[:], uint16(len(nameBytes)))
	f.Write(lBuf[:])
	f.Write(nameBytes)
	f.Write([]byte{OpStatusActive})

	// attrCount with RawTypedFlag set
	binary.LittleEndian.PutUint16(lBuf[:], 1|RawTypedFlag)
	f.Write(lBuf[:])

	// Invalid type tag 0x7F
	f.Write([]byte{0x7F})
	kBytes := []byte("field1")
	binary.LittleEndian.PutUint16(lBuf[:], uint16(len(kBytes)))
	f.Write(lBuf[:])
	f.Write(kBytes)
	f.Write([]byte{0, 0, 0, 0}) // 4 bytes dummy

	f.Sync()
	f.Close()

	// Opening must detect invalid attribute type tag and fail closed
	_, err = Open(dbPath)
	if err == nil {
		t.Fatal("expected Open to fail on invalid attribute type tag, got nil")
	}
}

func TestTypedNumeric_GetRecordTypedSurvivesSyncAndClose(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "uaf_safe.anchordb")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}

	const exactCounter int64 = 9_007_199_254_740_993
	_, err = db.SaveRecordTyped("rec_uaf", []AttrEntry{
		NewInt64Attr("counter", exactCounter),
		NewUint64Attr("uid", 18_446_744_073_709_551_615),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}

	// Capture CompactRecord while synced to mmap
	rec, err := db.GetRecordTyped("rec_uaf")
	if err != nil {
		t.Fatal(err)
	}

	// Trigger another Sync (which unmaps oldMmap) and then Close (which unmaps current mmap)
	_, err = db.SaveRecordTyped("rec_other", []AttrEntry{NewInt64Attr("counter", 1)})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Accessing rec after munmap must not SIGSEGV and must return exact values
	if v, ok := rec.GetInt("counter"); !ok || v != exactCounter {
		t.Fatalf("expected counter=%d after Close, got %d (ok=%v)", exactCounter, v, ok)
	}
	if u, ok := rec.GetUint("uid"); !ok || u != 18_446_744_073_709_551_615 {
		t.Fatalf("expected uid=max_uint64 after Close, got %d (ok=%v)", u, ok)
	}
}

func TestTypedNumeric_SetAttrCopyOnWrite(t *testing.T) {
	orig := CompactRecord{
		Operation: OpStatusActive,
		Attrs: []AttrEntry{
			NewInt64Attr("views", 100),
		},
	}
	mutated := orig.SetInt("views", 999)

	if v, _ := orig.GetInt("views"); v != 100 {
		t.Fatalf("SetInt mutated original CompactRecord in-place: got %d, want 100", v)
	}
	if v, _ := mutated.GetInt("views"); v != 999 {
		t.Fatalf("expected mutated views=999, got %d", v)
	}
}

func TestTypedNumeric_StructLiteralNormalizationAndInvalidType(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "literal_norm.anchordb")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// 1. Struct literal with Type=AttrTypeInt64 and IntVal=4200, omitting Value
	_, err = db.SaveRecordTyped("lit_1", []AttrEntry{
		{Key: " omitted_val", Type: AttrTypeInt64, IntVal: 4200},
	})
	if err != nil {
		t.Fatal(err)
	}
	rec, err := db.GetRecordTyped("lit_1")
	if err != nil {
		t.Fatal(err)
	}
	if fv, ok := rec.Get(" omitted_val"); !ok || fv != 4200 {
		t.Fatalf("expected normalized float32 Value=4200 before Sync, got %v (ok=%v)", fv, ok)
	}

	// 2. Invalid AttrType must fail cleanly without panicking in WAL
	_, err = db.SaveRecordTyped("bad_type", []AttrEntry{
		{Key: "corrupt", Type: AttrType(99), IntVal: 1},
	})
	if err == nil {
		t.Fatal("expected error for invalid AttrType(99), got nil")
	}
}

func TestTypedNumeric_Float32MaxInt64Boundary(t *testing.T) {
	// float32(math.MaxInt64) rounds up to 0x1p63 (9223372036854775808.0), which exceeds math.MaxInt64 by 1.
	attr := NewFloat32Attr("huge", float32(math.MaxInt64))
	if attr.IntVal != 0 {
		t.Fatalf("expected IntVal=0 for out-of-range float32(math.MaxInt64) (0x1p63), got %d", attr.IntVal)
	}
	if attr.Int64() != 0 {
		t.Fatalf("expected Int64()=0 for 0x1p63, got %d", attr.Int64())
	}

	negAttr := NewFloat32Attr("neg", -12.0)
	if negAttr.Uint64() != 0 {
		t.Fatalf("expected Uint64()=0 for negative float32, got %d", negAttr.Uint64())
	}

	schema := ModelSchema{"i": "int", "u": "uint"}
	if err := schema.ValidateProps(map[string]float32{"i": float32(math.MaxInt64), "u": 1}); err == nil {
		t.Fatal("expected schema validation to reject 0x1p63 as signed int")
	}
	if err := schema.ValidateProps(map[string]float32{"i": 1, "u": float32(math.MaxUint64)}); err == nil {
		t.Fatal("expected schema validation to reject 0x1p64 as uint")
	}
}

func TestTypedNumeric_LockPartitionPreservesAttrs(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "part_typed.anchordb")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const exactID int64 = 9_007_199_254_740_995
	_, err = db.SaveRecordTyped("old_event", []AttrEntry{
		NewFloat32Attr("date", 100),
		NewInt64Attr("snowflake_id", exactID),
	})
	if err != nil {
		t.Fatal(err)
	}

	locked, err := db.LockPartitionRecords("date", 200, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(locked) != 1 || len(locked[0].Attrs) != 2 {
		t.Fatalf("expected 1 locked record with 2 typed attrs, got %+v", locked)
	}
}

func TestTypedNumeric_Float64PrecisionAndMmap(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "f64_precision.anchordb")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}

	const exactFloat64 = 9007199254740991.0 // 2^53 - 1, exact in float64 but rounded in float32
	const smallFloat64 = 0.12345678901234567

	// Sanity check: float32 loses precision
	if float64(float32(exactFloat64)) == exactFloat64 {
		t.Fatalf("sanity check failed: float32 representation should not equal exactFloat64")
	}

	attrs := []AttrEntry{
		NewFloat64Attr("large_f64", exactFloat64),
		NewFloat64Attr("small_f64", smallFloat64),
	}

	_, err = db.SaveRecordTyped("f64_rec", attrs)
	if err != nil {
		t.Fatalf("save typed f64 record: %v", err)
	}

	// 1. In-memory check before Sync
	rec, err := db.GetRecordTyped("f64_rec")
	if err != nil {
		t.Fatalf("get record typed: %v", err)
	}
	lf, ok := rec.GetFloat64("large_f64")
	if !ok || lf != exactFloat64 {
		t.Fatalf("in-memory large_f64 expected %f, got %f (ok=%v)", exactFloat64, lf, ok)
	}
	sf, ok := rec.GetFloat64("small_f64")
	if !ok || sf != smallFloat64 {
		t.Fatalf("in-memory small_f64 expected %f, got %f (ok=%v)", smallFloat64, sf, ok)
	}

	// 2. Sync to disk & verify mmap read
	if err := db.Sync(); err != nil {
		t.Fatalf("sync db: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	reopened, err := Open(dbPath)
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer reopened.Close()

	rec2, err := reopened.GetRecordTyped("f64_rec")
	if err != nil {
		t.Fatalf("reopened get record typed: %v", err)
	}
	lf2, ok := rec2.GetFloat64("large_f64")
	if !ok || lf2 != exactFloat64 {
		t.Fatalf("mmap large_f64 expected %f, got %f (ok=%v)", exactFloat64, lf2, ok)
	}
	sf2, ok := rec2.GetFloat64("small_f64")
	if !ok || sf2 != smallFloat64 {
		t.Fatalf("mmap small_f64 expected %f, got %f (ok=%v)", smallFloat64, sf2, ok)
	}
}

func TestTypedNumeric_Float64WALRecovery(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "wal_f64.anchordb")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}

	const exactF64 = 123456789.987654321
	_, err = db.SaveRecordTyped("wal_f64_rec", []AttrEntry{
		NewFloat64Attr("metric", exactF64),
	})
	if err != nil {
		t.Fatal(err)
	}

	// Crash without calling db.Sync()
	_ = db.Close()

	// Reopen triggers WAL replay
	reopened, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	rec, err := reopened.GetRecordTyped("wal_f64_rec")
	if err != nil {
		t.Fatalf("get recovered record: %v", err)
	}
	val, ok := rec.GetFloat64("metric")
	if !ok || val != exactF64 {
		t.Fatalf("recovered f64 expected %f, got %f (ok=%v)", exactF64, val, ok)
	}
}

func TestTypedNumeric_Float64MixedArithmetic(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "arith_f64.anchordb")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const bigInt int64 = 9007199254740992 // 2^53
	_, err = db.SaveRecordTyped("acc_1", []AttrEntry{
		NewInt64Attr("principal", bigInt),
		NewFloat64Attr("interest_rate", 0.05),
	})
	if err != nil {
		t.Fatal(err)
	}

	// Compute OpMultiply: principal * interest_rate
	res, err := db.GetRecordTypedWithCompute("acc_1", map[string]ComputeOp{
		"interest": OpMultiply{FieldA: "principal", FieldB: "interest_rate"},
	})
	if err != nil {
		t.Fatal(err)
	}

	interest, ok := res.GetFloat64("interest")
	expectedInterest := float64(bigInt) * 0.05
	if !ok || interest != expectedInterest {
		t.Fatalf("expected interest %f, got %f (ok=%v)", expectedInterest, interest, ok)
	}
}

func TestTypedNumeric_Float64SchemaValidation(t *testing.T) {
	schema := ModelSchema{
		"standard_float": "float",
		"single_prec":    "float32",
		"double_prec":    "float64",
	}

	// Valid attributes
	validAttrs := []AttrEntry{
		NewFloat32Attr("standard_float", 1.5),
		NewFloat32Attr("single_prec", 2.5),
		NewFloat64Attr("double_prec", 3.141592653589793),
	}
	if err := schema.ValidateAttrEntries(validAttrs); err != nil {
		t.Fatalf("expected valid attrs to pass schema validation, got: %v", err)
	}

	// Double precision passed into float32 base type must fail
	invalidAttrs := []AttrEntry{
		NewFloat32Attr("standard_float", 1.5),
		NewFloat64Attr("single_prec", 2.5), // invalid: requires float32
		NewFloat64Attr("double_prec", 3.14),
	}
	if err := schema.ValidateAttrEntries(invalidAttrs); err == nil {
		t.Fatalf("expected float64 value in float32 field to fail validation")
	}

	// Non-finite float64 must fail
	nanAttrs := []AttrEntry{
		NewFloat32Attr("standard_float", 1.5),
		NewFloat32Attr("single_prec", 2.5),
		NewFloat64Attr("double_prec", math.NaN()),
	}
	if err := schema.ValidateAttrEntries(nanAttrs); err == nil {
		t.Fatalf("expected NaN float64 to fail validation")
	}
}

func TestAuditPhase2_ExactInt64ArithmeticAndFloat32SchemaUpdate(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "audit_p2.anchordb")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.AttachSchema(ModelSchema{
		"counter": "int",
		"voltage": "float32",
		"uid":     "uint",
	}); err != nil {
		t.Fatal(err)
	}

	const startCounter int64 = 9007199254740995 // > 2^53 (loses precision if converted to float64)
	const startUID uint64 = 18446744073709551610

	_, err = db.SaveRecordTyped("sensor_1", []AttrEntry{
		NewInt64Attr("counter", startCounter),
		NewFloat32Attr("voltage", 12.0),
		NewUint64Attr("uid", startUID),
	})
	if err != nil {
		t.Fatal(err)
	}

	updated, err := db.UpdateRecords(UpdateOptions{
		TargetEntities: []string{"sensor_1"},
		Mutations: map[string]ComputeOp{
			"counter": OpAdd{FieldA: "counter", FieldB: "1"},
			"voltage": OpAdd{FieldA: "voltage", FieldB: "0.1"},
			"uid":     OpAdd{FieldA: "uid", FieldB: "5"},
		},
	})
	if err != nil {
		t.Fatalf("UpdateRecords failed: %v", err)
	}
	if updated != 1 {
		t.Fatalf("expected 1 updated record, got %d", updated)
	}

	cr, err := db.GetRecordTyped("sensor_1")
	if err != nil {
		t.Fatal(err)
	}

	cAttr, ok := cr.GetTyped("counter")
	if !ok || cAttr.Type != AttrTypeInt64 || cAttr.Int64() != startCounter+1 {
		t.Fatalf("expected AttrTypeInt64(%d), got type=%d val=%d", startCounter+1, cAttr.Type, cAttr.Int64())
	}

	vAttr, ok := cr.GetTyped("voltage")
	if !ok || vAttr.Type != AttrTypeFloat32 || vAttr.Float32() != float32(12.0+0.1) {
		t.Fatalf("expected AttrTypeFloat32(12.1), got type=%d val=%f", vAttr.Type, vAttr.Float32())
	}

	uAttr, ok := cr.GetTyped("uid")
	if !ok || uAttr.Type != AttrTypeUint64 || uAttr.Uint64() != startUID+5 {
		t.Fatalf("expected AttrTypeUint64(%d), got type=%d val=%d", startUID+5, uAttr.Type, uAttr.Uint64())
	}
}

func TestAuditPhase2_ExpandAttrsImmutabilityAndDeterministicCompute(t *testing.T) {
	cr := NewCompactRecord(OpStatusActive, []AttrEntry{
		NewInt64Attr("a", 10),
		NewFloat64Attr("b", 20.5),
	})

	expanded := cr.ExpandAttrs()
	expanded[0].IntVal = 99999

	if v, _ := cr.GetInt("a"); v != 10 {
		t.Fatalf("ExpandAttrs mutated original CompactRecord: got %d, want 10", v)
	}

	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "det_compute.anchordb"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.SaveRecordTyped("rec1", []AttrEntry{
		NewInt64Attr("x", 10),
		NewInt64Attr("y", 20),
	})
	if err != nil {
		t.Fatal(err)
	}

	computations := map[string]ComputeOp{
		"z_sum":  OpAdd{FieldA: "x", FieldB: "y"},
		"a_prod": OpMultiply{FieldA: "x", FieldB: "y"},
	}

	first, err := db.GetRecordTypedWithCompute("rec1", computations)
	if err != nil {
		t.Fatal(err)
	}
	firstAttrs := first.ExpandAttrs()
	if len(firstAttrs) != 4 {
		t.Fatalf("expected 4 attrs, got %d", len(firstAttrs))
	}
	expectedKeys := []string{"x", "y", "a_prod", "z_sum"}
	for i, want := range expectedKeys {
		if firstAttrs[i].Key != want {
			t.Fatalf("at index %d: got key %q, want %q", i, firstAttrs[i].Key, want)
		}
	}
}
