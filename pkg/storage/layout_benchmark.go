package storage

import (
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"time"
	"unsafe"
)

// Experimental candidate layout flags used exclusively for Phase 3 benchmark evaluation.
const (
	RawFixedSlotFlag uint16 = 0x4000
	RawPackedColFlag uint16 = 0x2000
)

// LayoutEvaluationReport contains measured performance and space footprint
// metrics comparing the candidate storage layouts across all 4 Phase 3 dimensions.
type LayoutEvaluationReport struct {
	Iterations               int
	BaselineSizeBytes        int
	FixedWidthSizeBytes      int
	PackedColSizeBytes       int
	BaselineScanOpsSec       float64
	FixedWidthScanOpsSec     float64
	PackedColScanOpsSec      float64
	BaselineScanThroughput   float64 // MB/s
	FixedWidthScanThroughput float64 // MB/s
	PackedColScanThroughput  float64 // MB/s
	BaselineReadLatencyNs    float64
	FixedWidthReadLatencyNs  float64
	PackedColReadLatencyNs   float64
	BaselineReadOpsSec       float64
	FixedWidthReadOpsSec     float64
	PackedColReadOpsSec      float64
	WALWriteOpsSec           float64
	WALRecoveryOpsSec        float64
	MmapZeroCopyReadOpsSec   float64
	MmapHeapBytesPerRecord   uint64
	MmapGCPauseDuration      time.Duration
}

// SampleBenchmarkAttrs returns 8 representative mixed-type numeric attributes.
func SampleBenchmarkAttrs() []AttrEntry {
	return []AttrEntry{
		NewUint64Attr("views", 12500000000),
		NewUint64Attr("likes", 48000000),
		NewInt64Attr("balance", -5000000),
		NewInt64Attr("created_at", 1731140000),
		NewFloat32Attr("rating", 4.85),
		NewFloat32Attr("score", 98.5),
		NewFloat32Attr("weight", 0.75),
		NewFloat64Attr("stat_dev", 0.0001234567890123),
	}
}

// EncodeBaselineTLV encodes attributes in the sequential TLV format with exact buffer preallocation.
func EncodeBaselineTLV(attrs []AttrEntry) []byte {
	totalLen := 2
	for _, a := range attrs {
		totalLen += 1 + 2 + len(a.Key)
		if a.Type == AttrTypeFloat32 {
			totalLen += 4
		} else {
			totalLen += 8
		}
	}
	buf := make([]byte, totalLen)
	binary.LittleEndian.PutUint16(buf[:2], uint16(len(attrs))|RawTypedFlag)
	offset := 2
	for _, a := range attrs {
		buf[offset] = byte(a.Type)
		offset++
		kLen := len(a.Key)
		binary.LittleEndian.PutUint16(buf[offset:offset+2], uint16(kLen))
		offset += 2
		copy(buf[offset:offset+kLen], a.Key)
		offset += kLen
		if a.Type == AttrTypeFloat32 {
			binary.LittleEndian.PutUint32(buf[offset:offset+4], math.Float32bits(a.Value))
			offset += 4
		} else {
			binary.LittleEndian.PutUint64(buf[offset:offset+8], uint64(a.IntVal))
			offset += 8
		}
	}
	return buf
}

// DecodeBaselineTLVGet retrieves an attribute by key from a baseline TLV byte slice with zero allocations.
func DecodeBaselineTLVGet(raw []byte, key string) (AttrEntry, bool) {
	if len(raw) < 2 {
		return AttrEntry{}, false
	}
	countRaw := binary.LittleEndian.Uint16(raw[:2])
	attrCount := int(countRaw &^ RawTypedFlag)
	offset := 2
	for i := 0; i < attrCount; i++ {
		if offset+3 > len(raw) {
			return AttrEntry{}, false
		}
		tTag := AttrType(raw[offset])
		offset++
		kLen := int(binary.LittleEndian.Uint16(raw[offset : offset+2]))
		offset += 2

		valSize := 4
		if tTag == AttrTypeInt64 || tTag == AttrTypeUint64 || tTag == AttrTypeFloat64 {
			valSize = 8
		} else if tTag != AttrTypeFloat32 {
			return AttrEntry{}, false
		}
		if offset+kLen+valSize > len(raw) {
			return AttrEntry{}, false
		}

		if kLen == len(key) && (kLen == 0 || unsafe.String(&raw[offset], kLen) == key) {
			vOff := offset + kLen
			if tTag == AttrTypeFloat32 {
				valBits := binary.LittleEndian.Uint32(raw[vOff : vOff+4])
				return NewFloat32Attr(key, math.Float32frombits(valBits)), true
			} else if tTag == AttrTypeInt64 {
				v := int64(binary.LittleEndian.Uint64(raw[vOff : vOff+8]))
				return AttrEntry{Key: key, Type: AttrTypeInt64, Value: float32(v), IntVal: v}, true
			} else if tTag == AttrTypeUint64 {
				v := binary.LittleEndian.Uint64(raw[vOff : vOff+8])
				return AttrEntry{Key: key, Type: AttrTypeUint64, Value: float32(v), IntVal: int64(v)}, true
			} else if tTag == AttrTypeFloat64 {
				u64 := binary.LittleEndian.Uint64(raw[vOff : vOff+8])
				f64 := math.Float64frombits(u64)
				return AttrEntry{Key: key, Type: AttrTypeFloat64, Value: float32(f64), IntVal: int64(u64)}, true
			}
		}
		offset += kLen + valSize
	}
	return AttrEntry{}, false
}

// DecodeBaselineTLVScan iterates all attributes in a baseline TLV byte slice sequentially.
func DecodeBaselineTLVScan(raw []byte, fn func(attr AttrEntry) bool) {
	if len(raw) < 2 {
		return
	}
	countRaw := binary.LittleEndian.Uint16(raw[:2])
	attrCount := int(countRaw &^ RawTypedFlag)
	offset := 2
	for i := 0; i < attrCount; i++ {
		if offset+3 > len(raw) {
			return
		}
		tTag := AttrType(raw[offset])
		offset++
		kLen := int(binary.LittleEndian.Uint16(raw[offset : offset+2]))
		offset += 2
		valSize := 4
		if tTag == AttrTypeInt64 || tTag == AttrTypeUint64 || tTag == AttrTypeFloat64 {
			valSize = 8
		} else if tTag != AttrTypeFloat32 {
			return
		}
		if offset+kLen+valSize > len(raw) {
			return
		}
		var kStr string
		if kLen > 0 {
			kStr = unsafe.String(&raw[offset], kLen)
		}
		offset += kLen

		if tTag == AttrTypeFloat32 {
			valBits := binary.LittleEndian.Uint32(raw[offset : offset+4])
			offset += 4
			if !fn(NewFloat32Attr(kStr, math.Float32frombits(valBits))) {
				return
			}
		} else if tTag == AttrTypeInt64 {
			v := int64(binary.LittleEndian.Uint64(raw[offset : offset+8]))
			offset += 8
			if !fn(AttrEntry{Key: kStr, Type: AttrTypeInt64, Value: float32(v), IntVal: v}) {
				return
			}
		} else if tTag == AttrTypeUint64 {
			v := binary.LittleEndian.Uint64(raw[offset : offset+8])
			offset += 8
			if !fn(AttrEntry{Key: kStr, Type: AttrTypeUint64, Value: float32(v), IntVal: int64(v)}) {
				return
			}
		} else if tTag == AttrTypeFloat64 {
			u64 := binary.LittleEndian.Uint64(raw[offset : offset+8])
			offset += 8
			f64 := math.Float64frombits(u64)
			if !fn(AttrEntry{Key: kStr, Type: AttrTypeFloat64, Value: float32(f64), IntVal: int64(u64)}) {
				return
			}
		}
	}
}

// EncodeFixedWidthSlot encodes attributes into Candidate 1: Fixed-Width Slot (8-Byte Aligned)
// with a 4-byte header [2B count|Flags][2B valsOffset] so value offset resolution is O(1) without a pre-pass loop.
func EncodeFixedWidthSlot(attrs []AttrEntry) []byte {
	n := len(attrs)
	keysLen := 0
	for _, a := range attrs {
		keysLen += len(a.Key)
	}

	tableOffset := 4
	keysOffset := tableOffset + n*3
	valsOffset := keysOffset + keysLen
	if rem := valsOffset % 8; rem != 0 {
		valsOffset += (8 - rem)
	}
	totalLen := valsOffset + n*8

	buf := make([]byte, totalLen)
	binary.LittleEndian.PutUint16(buf[0:2], uint16(n)|RawTypedFlag|RawFixedSlotFlag)
	binary.LittleEndian.PutUint16(buf[2:4], uint16(valsOffset))

	curKeyOffset := keysOffset
	for i, a := range attrs {
		dOff := tableOffset + i*3
		buf[dOff] = byte(a.Type)
		binary.LittleEndian.PutUint16(buf[dOff+1:dOff+3], uint16(len(a.Key)))

		copy(buf[curKeyOffset:curKeyOffset+len(a.Key)], a.Key)
		curKeyOffset += len(a.Key)

		vOff := valsOffset + i*8
		if a.Type == AttrTypeFloat32 {
			binary.LittleEndian.PutUint32(buf[vOff:vOff+4], math.Float32bits(a.Value))
		} else {
			binary.LittleEndian.PutUint64(buf[vOff:vOff+8], uint64(a.IntVal))
		}
	}
	return buf
}

// DecodeFixedWidthSlotGet retrieves an attribute by key from Candidate 1 using O(1) valsOffset lookup.
func DecodeFixedWidthSlotGet(raw []byte, key string) (AttrEntry, bool) {
	if len(raw) < 4 {
		return AttrEntry{}, false
	}
	countRaw := binary.LittleEndian.Uint16(raw[0:2])
	attrCount := int(countRaw &^ (RawTypedFlag | RawFixedSlotFlag))
	valsOffset := int(binary.LittleEndian.Uint16(raw[2:4]))

	tableOffset := 4
	keysOffset := tableOffset + attrCount*3
	if keysOffset > valsOffset || valsOffset+attrCount*8 > len(raw) {
		return AttrEntry{}, false
	}

	curKeyOffset := keysOffset
	for i := 0; i < attrCount; i++ {
		dOff := tableOffset + i*3
		tTag := AttrType(raw[dOff])
		kLen := int(binary.LittleEndian.Uint16(raw[dOff+1 : dOff+3]))
		if curKeyOffset+kLen > valsOffset {
			return AttrEntry{}, false
		}
		match := kLen == len(key) && (kLen == 0 || unsafe.String(&raw[curKeyOffset], kLen) == key)
		curKeyOffset += kLen

		if match {
			vOff := valsOffset + i*8
			if tTag == AttrTypeFloat32 {
				valBits := binary.LittleEndian.Uint32(raw[vOff : vOff+4])
				return NewFloat32Attr(key, math.Float32frombits(valBits)), true
			} else if tTag == AttrTypeInt64 {
				v := int64(binary.LittleEndian.Uint64(raw[vOff : vOff+8]))
				return AttrEntry{Key: key, Type: AttrTypeInt64, Value: float32(v), IntVal: v}, true
			} else if tTag == AttrTypeUint64 {
				v := binary.LittleEndian.Uint64(raw[vOff : vOff+8])
				return AttrEntry{Key: key, Type: AttrTypeUint64, Value: float32(v), IntVal: int64(v)}, true
			} else if tTag == AttrTypeFloat64 {
				u64 := binary.LittleEndian.Uint64(raw[vOff : vOff+8])
				f64 := math.Float64frombits(u64)
				return AttrEntry{Key: key, Type: AttrTypeFloat64, Value: float32(f64), IntVal: int64(u64)}, true
			}
			return AttrEntry{}, false
		}
	}
	return AttrEntry{}, false
}

// DecodeFixedWidthSlotScan iterates all attributes from Candidate 1 in a single pass.
func DecodeFixedWidthSlotScan(raw []byte, fn func(attr AttrEntry) bool) {
	if len(raw) < 4 {
		return
	}
	countRaw := binary.LittleEndian.Uint16(raw[0:2])
	attrCount := int(countRaw &^ (RawTypedFlag | RawFixedSlotFlag))
	valsOffset := int(binary.LittleEndian.Uint16(raw[2:4]))

	tableOffset := 4
	keysOffset := tableOffset + attrCount*3
	if keysOffset > valsOffset || valsOffset+attrCount*8 > len(raw) {
		return
	}

	curKeyOffset := keysOffset
	for i := 0; i < attrCount; i++ {
		dOff := tableOffset + i*3
		tTag := AttrType(raw[dOff])
		kLen := int(binary.LittleEndian.Uint16(raw[dOff+1 : dOff+3]))
		if curKeyOffset+kLen > valsOffset {
			return
		}
		var kStr string
		if kLen > 0 {
			kStr = unsafe.String(&raw[curKeyOffset], kLen)
		}
		curKeyOffset += kLen

		vOff := valsOffset + i*8
		if tTag == AttrTypeFloat32 {
			valBits := binary.LittleEndian.Uint32(raw[vOff : vOff+4])
			if !fn(NewFloat32Attr(kStr, math.Float32frombits(valBits))) {
				return
			}
		} else if tTag == AttrTypeInt64 {
			v := int64(binary.LittleEndian.Uint64(raw[vOff : vOff+8]))
			if !fn(AttrEntry{Key: kStr, Type: AttrTypeInt64, Value: float32(v), IntVal: v}) {
				return
			}
		} else if tTag == AttrTypeUint64 {
			v := binary.LittleEndian.Uint64(raw[vOff : vOff+8])
			if !fn(AttrEntry{Key: kStr, Type: AttrTypeUint64, Value: float32(v), IntVal: int64(v)}) {
				return
			}
		} else if tTag == AttrTypeFloat64 {
			u64 := binary.LittleEndian.Uint64(raw[vOff : vOff+8])
			f64 := math.Float64frombits(u64)
			if !fn(AttrEntry{Key: kStr, Type: AttrTypeFloat64, Value: float32(f64), IntVal: int64(u64)}) {
				return
			}
		} else {
			return
		}
	}
}

// EncodePackedTypedColumns encodes attributes into Candidate 2: Packed Typed Columns
// with a 4-byte header [2B count|Flags][2B valsOffset] so value offset resolution is O(1) without a pre-pass loop.
func EncodePackedTypedColumns(attrs []AttrEntry) []byte {
	n := len(attrs)
	keysLen := 0
	valBytesLen := 0
	for _, a := range attrs {
		keysLen += len(a.Key)
		if a.Type == AttrTypeFloat32 {
			valBytesLen += 4
		} else {
			valBytesLen += 8
		}
	}

	tableOffset := 4
	keysOffset := tableOffset + n*5
	valsOffset := keysOffset + keysLen
	totalLen := valsOffset + valBytesLen

	buf := make([]byte, totalLen)
	binary.LittleEndian.PutUint16(buf[0:2], uint16(n)|RawTypedFlag|RawPackedColFlag)
	binary.LittleEndian.PutUint16(buf[2:4], uint16(valsOffset))

	curKeyOffset := keysOffset
	curValOffset := 0

	for i, a := range attrs {
		dOff := tableOffset + i*5
		buf[dOff] = byte(a.Type)
		binary.LittleEndian.PutUint16(buf[dOff+1:dOff+3], uint16(curValOffset))
		binary.LittleEndian.PutUint16(buf[dOff+3:dOff+5], uint16(len(a.Key)))

		copy(buf[curKeyOffset:curKeyOffset+len(a.Key)], a.Key)
		curKeyOffset += len(a.Key)

		vOff := valsOffset + curValOffset
		if a.Type == AttrTypeFloat32 {
			binary.LittleEndian.PutUint32(buf[vOff:vOff+4], math.Float32bits(a.Value))
			curValOffset += 4
		} else {
			binary.LittleEndian.PutUint64(buf[vOff:vOff+8], uint64(a.IntVal))
			curValOffset += 8
		}
	}
	return buf
}

// DecodePackedTypedColumnsGet retrieves an attribute by key from Candidate 2 using O(1) valsOffset lookup.
func DecodePackedTypedColumnsGet(raw []byte, key string) (AttrEntry, bool) {
	if len(raw) < 4 {
		return AttrEntry{}, false
	}
	countRaw := binary.LittleEndian.Uint16(raw[0:2])
	attrCount := int(countRaw &^ (RawTypedFlag | RawPackedColFlag))
	valsOffset := int(binary.LittleEndian.Uint16(raw[2:4]))

	tableOffset := 4
	keysOffset := tableOffset + attrCount*5
	if keysOffset > valsOffset || valsOffset > len(raw) {
		return AttrEntry{}, false
	}

	curKeyOffset := keysOffset
	for i := 0; i < attrCount; i++ {
		dOff := tableOffset + i*5
		tTag := AttrType(raw[dOff])
		valRelOffset := int(binary.LittleEndian.Uint16(raw[dOff+1 : dOff+3]))
		kLen := int(binary.LittleEndian.Uint16(raw[dOff+3 : dOff+5]))
		if curKeyOffset+kLen > valsOffset {
			return AttrEntry{}, false
		}
		match := kLen == len(key) && (kLen == 0 || unsafe.String(&raw[curKeyOffset], kLen) == key)
		curKeyOffset += kLen

		if match {
			vOff := valsOffset + valRelOffset
			if tTag == AttrTypeFloat32 {
				if vOff+4 > len(raw) {
					return AttrEntry{}, false
				}
				valBits := binary.LittleEndian.Uint32(raw[vOff : vOff+4])
				return NewFloat32Attr(key, math.Float32frombits(valBits)), true
			} else if tTag == AttrTypeInt64 {
				if vOff+8 > len(raw) {
					return AttrEntry{}, false
				}
				v := int64(binary.LittleEndian.Uint64(raw[vOff : vOff+8]))
				return AttrEntry{Key: key, Type: AttrTypeInt64, Value: float32(v), IntVal: v}, true
			} else if tTag == AttrTypeUint64 {
				if vOff+8 > len(raw) {
					return AttrEntry{}, false
				}
				v := binary.LittleEndian.Uint64(raw[vOff : vOff+8])
				return AttrEntry{Key: key, Type: AttrTypeUint64, Value: float32(v), IntVal: int64(v)}, true
			} else if tTag == AttrTypeFloat64 {
				if vOff+8 > len(raw) {
					return AttrEntry{}, false
				}
				u64 := binary.LittleEndian.Uint64(raw[vOff : vOff+8])
				f64 := math.Float64frombits(u64)
				return AttrEntry{Key: key, Type: AttrTypeFloat64, Value: float32(f64), IntVal: int64(u64)}, true
			}
			return AttrEntry{}, false
		}
	}
	return AttrEntry{}, false
}

// DecodePackedTypedColumnsScan iterates all attributes from Candidate 2 in a single pass.
func DecodePackedTypedColumnsScan(raw []byte, fn func(attr AttrEntry) bool) {
	if len(raw) < 4 {
		return
	}
	countRaw := binary.LittleEndian.Uint16(raw[0:2])
	attrCount := int(countRaw &^ (RawTypedFlag | RawPackedColFlag))
	valsOffset := int(binary.LittleEndian.Uint16(raw[2:4]))

	tableOffset := 4
	keysOffset := tableOffset + attrCount*5
	if keysOffset > valsOffset || valsOffset > len(raw) {
		return
	}

	curKeyOffset := keysOffset
	for i := 0; i < attrCount; i++ {
		dOff := tableOffset + i*5
		tTag := AttrType(raw[dOff])
		valRelOffset := int(binary.LittleEndian.Uint16(raw[dOff+1 : dOff+3]))
		kLen := int(binary.LittleEndian.Uint16(raw[dOff+3 : dOff+5]))
		if curKeyOffset+kLen > valsOffset {
			return
		}
		var kStr string
		if kLen > 0 {
			kStr = unsafe.String(&raw[curKeyOffset], kLen)
		}
		curKeyOffset += kLen

		vOff := valsOffset + valRelOffset
		if tTag == AttrTypeFloat32 {
			if vOff+4 > len(raw) {
				return
			}
			valBits := binary.LittleEndian.Uint32(raw[vOff : vOff+4])
			if !fn(NewFloat32Attr(kStr, math.Float32frombits(valBits))) {
				return
			}
		} else if tTag == AttrTypeInt64 {
			if vOff+8 > len(raw) {
				return
			}
			v := int64(binary.LittleEndian.Uint64(raw[vOff : vOff+8]))
			if !fn(AttrEntry{Key: kStr, Type: AttrTypeInt64, Value: float32(v), IntVal: v}) {
				return
			}
		} else if tTag == AttrTypeUint64 {
			if vOff+8 > len(raw) {
				return
			}
			v := binary.LittleEndian.Uint64(raw[vOff : vOff+8])
			if !fn(AttrEntry{Key: kStr, Type: AttrTypeUint64, Value: float32(v), IntVal: int64(v)}) {
				return
			}
		} else if tTag == AttrTypeFloat64 {
			if vOff+8 > len(raw) {
				return
			}
			u64 := binary.LittleEndian.Uint64(raw[vOff : vOff+8])
			f64 := math.Float64frombits(u64)
			if !fn(AttrEntry{Key: kStr, Type: AttrTypeFloat64, Value: float32(f64), IntVal: int64(u64)}) {
				return
			}
		} else {
			return
		}
	}
}

// RunLayoutBenchmark runs empirical performance and space footprint comparisons
// across all 4 dimensions mandated by the Phase 3 roadmap.
func RunLayoutBenchmark(iterations int) LayoutEvaluationReport {
	if iterations <= 0 {
		iterations = 1_000_000
	}
	attrs := SampleBenchmarkAttrs()
	rawBaseline := EncodeBaselineTLV(attrs)
	rawFixed := EncodeFixedWidthSlot(attrs)
	rawPacked := EncodePackedTypedColumns(attrs)

	report := LayoutEvaluationReport{
		Iterations:          iterations,
		BaselineSizeBytes:   len(rawBaseline),
		FixedWidthSizeBytes: len(rawFixed),
		PackedColSizeBytes:  len(rawPacked),
	}

	// 1. Sequential Scan
	t0 := time.Now()
	for i := 0; i < iterations; i++ {
		DecodeBaselineTLVScan(rawBaseline, func(_ AttrEntry) bool { return true })
	}
	durBaseScan := time.Since(t0)
	report.BaselineScanOpsSec = float64(iterations) / durBaseScan.Seconds()
	report.BaselineScanThroughput = (float64(iterations*len(rawBaseline)) / (1024 * 1024)) / durBaseScan.Seconds()

	t1 := time.Now()
	for i := 0; i < iterations; i++ {
		DecodeFixedWidthSlotScan(rawFixed, func(_ AttrEntry) bool { return true })
	}
	durFixedScan := time.Since(t1)
	report.FixedWidthScanOpsSec = float64(iterations) / durFixedScan.Seconds()
	report.FixedWidthScanThroughput = (float64(iterations*len(rawFixed)) / (1024 * 1024)) / durFixedScan.Seconds()

	t2 := time.Now()
	for i := 0; i < iterations; i++ {
		DecodePackedTypedColumnsScan(rawPacked, func(_ AttrEntry) bool { return true })
	}
	durPackedScan := time.Since(t2)
	report.PackedColScanOpsSec = float64(iterations) / durPackedScan.Seconds()
	report.PackedColScanThroughput = (float64(iterations*len(rawPacked)) / (1024 * 1024)) / durPackedScan.Seconds()

	// 2. Random Point Read (First, Middle, Last)
	t3 := time.Now()
	for i := 0; i < iterations; i++ {
		_, _ = DecodeBaselineTLVGet(rawBaseline, "views")
		_, _ = DecodeBaselineTLVGet(rawBaseline, "rating")
		_, _ = DecodeBaselineTLVGet(rawBaseline, "stat_dev")
	}
	durBaseRead := time.Since(t3)
	report.BaselineReadLatencyNs = float64(durBaseRead.Nanoseconds()) / float64(iterations*3)
	report.BaselineReadOpsSec = float64(iterations*3) / durBaseRead.Seconds()

	t4 := time.Now()
	for i := 0; i < iterations; i++ {
		_, _ = DecodeFixedWidthSlotGet(rawFixed, "views")
		_, _ = DecodeFixedWidthSlotGet(rawFixed, "rating")
		_, _ = DecodeFixedWidthSlotGet(rawFixed, "stat_dev")
	}
	durFixedRead := time.Since(t4)
	report.FixedWidthReadLatencyNs = float64(durFixedRead.Nanoseconds()) / float64(iterations*3)
	report.FixedWidthReadOpsSec = float64(iterations*3) / durFixedRead.Seconds()

	t5 := time.Now()
	for i := 0; i < iterations; i++ {
		_, _ = DecodePackedTypedColumnsGet(rawPacked, "views")
		_, _ = DecodePackedTypedColumnsGet(rawPacked, "rating")
		_, _ = DecodePackedTypedColumnsGet(rawPacked, "stat_dev")
	}
	durPackedRead := time.Since(t5)
	report.PackedColReadLatencyNs = float64(durPackedRead.Nanoseconds()) / float64(iterations*3)
	report.PackedColReadOpsSec = float64(iterations*3) / durPackedRead.Seconds()

	// 3 & 4. WAL Write/Recovery Speed & Mmap/GC Overhead on Real AnchorDB Engine
	dir, err := os.MkdirTemp("", "bench_phase3_layout_*")
	if err == nil {
		defer os.RemoveAll(dir)
		const engineRecords = 10_000
		entityNames := make([]string, engineRecords)
		for i := 0; i < engineRecords; i++ {
			entityNames[i] = fmt.Sprintf("entity_%d", i)
		}
		dbPath := filepath.Join(dir, "layout.anchordb")
		db, err := OpenWithOptions(dbPath, Options{
			WAL: WALOptions{SyncMode: SyncNone},
		})
		if err == nil {
			tWrite := time.Now()
			for i := 0; i < engineRecords; i++ {
				_, _ = db.SaveRecordTyped(entityNames[i], attrs)
			}
			writeDur := time.Since(tWrite)
			report.WALWriteOpsSec = float64(engineRecords) / writeDur.Seconds()
			_ = db.Close()

			// Measure WAL Crash Recovery Speed (reopening before Sync replays all 10,000 WAL frames)
			tRecover := time.Now()
			dbRecovered, err := OpenWithOptions(dbPath, Options{
				WAL: WALOptions{SyncMode: SyncNone},
			})
			recoverDur := time.Since(tRecover)
			if err == nil {
				report.WALRecoveryOpsSec = float64(engineRecords) / recoverDur.Seconds()

				// Sync to mmap (.adb) so all records point to zero-copy Raw slices
				_ = dbRecovered.Sync()
				runtime.GC()
				var memBefore, memAfter runtime.MemStats
				runtime.ReadMemStats(&memBefore)

				tMmapRead := time.Now()
				for i := 0; i < engineRecords; i++ {
					if cr, ok := dbRecovered.EntityStore[entityNames[i]]; ok {
						_, _ = cr.GetUint("views")
						_, _ = cr.Get("rating")
						_, _ = cr.GetFloat64("stat_dev")
					}
				}
				mmapReadDur := time.Since(tMmapRead)
				report.MmapZeroCopyReadOpsSec = float64(engineRecords*3) / mmapReadDur.Seconds()

				runtime.ReadMemStats(&memAfter)
				if memAfter.TotalAlloc >= memBefore.TotalAlloc {
					report.MmapHeapBytesPerRecord = (memAfter.TotalAlloc - memBefore.TotalAlloc) / engineRecords
				}

				tGC := time.Now()
				runtime.GC()
				report.MmapGCPauseDuration = time.Since(tGC)
				_ = dbRecovered.Close()
			}
		}
	}

	return report
}
