package storage

import (
	"fmt"
	"path/filepath"
	"runtime"
	"testing"
)

func BenchmarkLayout_SequentialScan_Baseline(b *testing.B) {
	attrs := SampleBenchmarkAttrs()
	raw := EncodeBaselineTLV(attrs)
	b.SetBytes(int64(len(raw)))
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		DecodeBaselineTLVScan(raw, func(a AttrEntry) bool {
			return true
		})
	}
}

func BenchmarkLayout_SequentialScan_FixedWidth(b *testing.B) {
	attrs := SampleBenchmarkAttrs()
	raw := EncodeFixedWidthSlot(attrs)
	b.SetBytes(int64(len(raw)))
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		DecodeFixedWidthSlotScan(raw, func(a AttrEntry) bool {
			return true
		})
	}
}

func BenchmarkLayout_SequentialScan_PackedColumns(b *testing.B) {
	attrs := SampleBenchmarkAttrs()
	raw := EncodePackedTypedColumns(attrs)
	b.SetBytes(int64(len(raw)))
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		DecodePackedTypedColumnsScan(raw, func(a AttrEntry) bool {
			return true
		})
	}
}

func BenchmarkLayout_RandomRead_Baseline(b *testing.B) {
	attrs := SampleBenchmarkAttrs()
	raw := EncodeBaselineTLV(attrs)
	b.SetBytes(int64(len(raw)))
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = DecodeBaselineTLVGet(raw, "views")    // First
		_, _ = DecodeBaselineTLVGet(raw, "rating")   // Middle
		_, _ = DecodeBaselineTLVGet(raw, "stat_dev") // Last
	}
}

func BenchmarkLayout_RandomRead_FixedWidth(b *testing.B) {
	attrs := SampleBenchmarkAttrs()
	raw := EncodeFixedWidthSlot(attrs)
	b.SetBytes(int64(len(raw)))
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = DecodeFixedWidthSlotGet(raw, "views")    // First
		_, _ = DecodeFixedWidthSlotGet(raw, "rating")   // Middle
		_, _ = DecodeFixedWidthSlotGet(raw, "stat_dev") // Last
	}
}

func BenchmarkLayout_RandomRead_PackedColumns(b *testing.B) {
	attrs := SampleBenchmarkAttrs()
	raw := EncodePackedTypedColumns(attrs)
	b.SetBytes(int64(len(raw)))
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = DecodePackedTypedColumnsGet(raw, "views")    // First
		_, _ = DecodePackedTypedColumnsGet(raw, "rating")   // Middle
		_, _ = DecodePackedTypedColumnsGet(raw, "stat_dev") // Last
	}
}

func BenchmarkLayout_Encode_BaselinePrealloc(b *testing.B) {
	attrs := SampleBenchmarkAttrs()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = EncodeBaselineTLV(attrs)
	}
}

func BenchmarkLayout_Encode_FixedWidth(b *testing.B) {
	attrs := SampleBenchmarkAttrs()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = EncodeFixedWidthSlot(attrs)
	}
}

func BenchmarkLayout_Encode_PackedColumns(b *testing.B) {
	attrs := SampleBenchmarkAttrs()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = EncodePackedTypedColumns(attrs)
	}
}

func wideTestAttrs() []AttrEntry {
	attrs := make([]AttrEntry, 32)
	for i := 0; i < 32; i++ {
		key := "attr_key_" + string(rune('a'+(i%26))) + string(rune('0'+(i/10))) + string(rune('0'+(i%10)))
		switch i % 4 {
		case 0:
			attrs[i] = NewUint64Attr(key, uint64(i*1000000000))
		case 1:
			attrs[i] = NewInt64Attr(key, int64(-i*5000000))
		case 2:
			attrs[i] = NewFloat32Attr(key, float32(i)*1.5)
		case 3:
			attrs[i] = NewFloat64Attr(key, float64(i)*3.1415926535)
		}
	}
	return attrs
}

func BenchmarkLayout_Wide32_RandomRead_Baseline(b *testing.B) {
	attrs := wideTestAttrs()
	raw := EncodeBaselineTLV(attrs)
	firstKey := attrs[0].Key
	midKey := attrs[16].Key
	lastKey := attrs[31].Key
	b.SetBytes(int64(len(raw)))
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = DecodeBaselineTLVGet(raw, firstKey)
		_, _ = DecodeBaselineTLVGet(raw, midKey)
		_, _ = DecodeBaselineTLVGet(raw, lastKey)
	}
}

func BenchmarkLayout_Wide32_RandomRead_FixedWidth(b *testing.B) {
	attrs := wideTestAttrs()
	raw := EncodeFixedWidthSlot(attrs)
	firstKey := attrs[0].Key
	midKey := attrs[16].Key
	lastKey := attrs[31].Key
	b.SetBytes(int64(len(raw)))
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = DecodeFixedWidthSlotGet(raw, firstKey)
		_, _ = DecodeFixedWidthSlotGet(raw, midKey)
		_, _ = DecodeFixedWidthSlotGet(raw, lastKey)
	}
}

func BenchmarkLayout_Wide32_RandomRead_PackedColumns(b *testing.B) {
	attrs := wideTestAttrs()
	raw := EncodePackedTypedColumns(attrs)
	firstKey := attrs[0].Key
	midKey := attrs[16].Key
	lastKey := attrs[31].Key
	b.SetBytes(int64(len(raw)))
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = DecodePackedTypedColumnsGet(raw, firstKey)
		_, _ = DecodePackedTypedColumnsGet(raw, midKey)
		_, _ = DecodePackedTypedColumnsGet(raw, lastKey)
	}
}

func BenchmarkLayout_WALWriteAndRecovery(b *testing.B) {
	attrs := SampleBenchmarkAttrs()
	path := filepath.Join(b.TempDir(), "bench_wal_typed.anchordb")
	db, err := OpenWithOptions(path, Options{WAL: WALOptions{SyncMode: SyncNone}})
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer db.Close()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = db.SaveRecordTyped(fmt.Sprintf("rec_%d", i), attrs)
	}
}

func BenchmarkLayout_MmapAndGCOverhead(b *testing.B) {
	attrs := SampleBenchmarkAttrs()
	path := filepath.Join(b.TempDir(), "bench_mmap_typed.anchordb")
	db, err := OpenWithOptions(path, Options{WAL: WALOptions{SyncMode: SyncNone}})
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer db.Close()

	const recordCount = 1024
	names := make([]string, recordCount)
	for i := 0; i < recordCount; i++ {
		names[i] = fmt.Sprintf("rec_%d", i)
		_, _ = db.SaveRecordTyped(names[i], attrs)
	}
	if err := db.Sync(); err != nil {
		b.Fatalf("Sync: %v", err)
	}
	runtime.GC()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		cr := db.EntityStore[names[i&(recordCount-1)]]
		_, _ = cr.GetUint("views")
		_, _ = cr.Get("rating")
		_, _ = cr.GetFloat64("stat_dev")
	}
}

func TestLayoutCandidates_Correctness(t *testing.T) {
	attrs := SampleBenchmarkAttrs()

	// 1. Baseline
	rawBase := EncodeBaselineTLV(attrs)
	for _, a := range attrs {
		got, ok := DecodeBaselineTLVGet(rawBase, a.Key)
		if !ok {
			t.Fatalf("Baseline missing key %q", a.Key)
		}
		if got.Type != a.Type || got.Int64() != a.Int64() || got.Float64() != a.Float64() {
			t.Fatalf("Baseline mismatch for key %q: expected %+v, got %+v", a.Key, a, got)
		}
	}

	// 2. Fixed Width Slot
	rawFixed := EncodeFixedWidthSlot(attrs)
	for _, a := range attrs {
		got, ok := DecodeFixedWidthSlotGet(rawFixed, a.Key)
		if !ok {
			t.Fatalf("FixedWidth missing key %q", a.Key)
		}
		if got.Type != a.Type || got.Int64() != a.Int64() || got.Float64() != a.Float64() {
			t.Fatalf("FixedWidth mismatch for key %q: expected %+v, got %+v", a.Key, a, got)
		}
	}

	// 3. Packed Columns
	rawPacked := EncodePackedTypedColumns(attrs)
	for _, a := range attrs {
		got, ok := DecodePackedTypedColumnsGet(rawPacked, a.Key)
		if !ok {
			t.Fatalf("PackedColumns missing key %q", a.Key)
		}
		if got.Type != a.Type || got.Int64() != a.Int64() || got.Float64() != a.Float64() {
			t.Fatalf("PackedColumns mismatch for key %q: expected %+v, got %+v", a.Key, a, got)
		}
	}

	// 4. Audit F-01 Regression: Empty key and truncated buffer bounds safety
	emptyKeyAttrs := []AttrEntry{NewInt64Attr("", 99)}
	rawEmptyBase := EncodeBaselineTLV(emptyKeyAttrs)
	if got, ok := DecodeBaselineTLVGet(rawEmptyBase, ""); !ok || got.Int64() != 99 {
		t.Fatalf("expected empty key lookup to succeed, got ok=%v, val=%d", ok, got.Int64())
	}
	// Truncated right before value bytes (after 2B count + 1B type + 2B kLen(0) = 5 bytes)
	truncatedRaw := rawEmptyBase[:5]
	crTruncated := CompactRecord{Operation: OpStatusActive, Raw: truncatedRaw}
	if _, ok := crTruncated.GetTyped(""); ok {
		t.Fatalf("expected truncated CompactRecord.Raw to return false without panic")
	}
	if _, ok := DecodeBaselineTLVGet(truncatedRaw, ""); ok {
		t.Fatalf("expected truncated DecodeBaselineTLVGet to return false without panic")
	}

	// 5. Audit F-03: Verify RunLayoutBenchmark populates all 4 roadmap dimensions
	report := RunLayoutBenchmark(1000)
	if report.WALWriteOpsSec <= 0 || report.WALRecoveryOpsSec <= 0 || report.MmapZeroCopyReadOpsSec <= 0 {
		t.Fatalf("expected WAL and Mmap metrics to be positive, got %+v", report)
	}

	t.Logf("Sizes for 8 mixed attrs:\n  Baseline TLV:  %d bytes\n  Fixed-Width:   %d bytes\n  Packed Columns:%d bytes",
		len(rawBase), len(rawFixed), len(rawPacked))
}
