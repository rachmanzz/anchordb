package system

import (
	"math"
	"os"
	"runtime/debug"
	"sync"
	"testing"
)

func TestParseMemoryBytes(t *testing.T) {
	tests := []struct {
		input    string
		expected int64
		hasErr   bool
	}{
		{"500MB", 500 * 1024 * 1024, false},
		{"500mb", 500 * 1024 * 1024, false},
		{"500M", 500 * 1024 * 1024, false},
		{"500MiB", 500 * 1024 * 1024, false},
		{"1GB", 1024 * 1024 * 1024, false},
		{"1G", 1024 * 1024 * 1024, false},
		{"1GiB", 1024 * 1024 * 1024, false},
		{"5GB", 5 * 1024 * 1024 * 1024, false},
		{"2.5GB", int64(2.5 * 1024 * 1024 * 1024), false},
		{"100KB", 100 * 1024, false},
		{"1024", 1024, false},
		{"", 0, true},
		{"invalid", 0, true},
		{"-500MB", 0, true},
		{"NaN", 0, true},
		{"Inf", 0, true},
		{"999999999999999999999999999999999999999999999999T", 0, true},
	}

	for _, tt := range tests {
		got, err := ParseMemoryBytes(tt.input)
		if (err != nil) != tt.hasErr {
			t.Errorf("ParseMemoryBytes(%q) error = %v, wantErr %v", tt.input, err, tt.hasErr)
			continue
		}
		if !tt.hasErr && got != tt.expected {
			t.Errorf("ParseMemoryBytes(%q) = %d, want %d", tt.input, got, tt.expected)
		}
	}
}

func TestFormatBytesSupportsLargeValues(t *testing.T) {
	if got := FormatBytes(1 << 50); got == "" {
		t.Fatal("expected formatted value for pebibyte range")
	}
	if got := FormatBytes(math.MaxInt64 - 1); got == "" {
		t.Fatal("expected formatted value near MaxInt64")
	}
}

func TestApplyMemoryLimit(t *testing.T) {
	original := debug.SetMemoryLimit(-1)
	defer debug.SetMemoryLimit(original)

	bytes, summary, err := ApplyMemoryLimit("1GB")
	if err != nil {
		t.Fatalf("ApplyMemoryLimit failed: %v", err)
	}

	expected := int64(1024 * 1024 * 1024)
	if bytes != expected {
		t.Errorf("expected %d bytes, got %d", expected, bytes)
	}
	if summary == "" {
		t.Errorf("expected non-empty summary")
	}

	stats := GetMemoryStats()
	if stats.LimitBytes != expected {
		t.Errorf("expected stats.LimitBytes == %d, got %d", expected, stats.LimitBytes)
	}
	if stats.LimitHuman != "1.00 GB" {
		t.Errorf("expected stats.LimitHuman == '1.00 GB', got %q", stats.LimitHuman)
	}
}

func TestDetectCgroupMemoryLimit_NonCrashing(t *testing.T) {
	// DetectCgroupMemoryLimit reads host/container cgroup paths.
	// In local dev/CI, it either returns a positive byte limit or an error, but must never panic.
	limit, err := DetectCgroupMemoryLimit()
	if err == nil {
		if limit <= 0 {
			t.Errorf("expected positive cgroup limit if err is nil, got %d", limit)
		}
	} else {
		if limit != 0 {
			t.Errorf("expected limit = 0 on error, got %d", limit)
		}
	}
}

func TestFormatBytes_BoundaryValues(t *testing.T) {
	tests := []struct {
		input int64
		want  string
	}{
		{0, "Unlimited"},
		{-1, "Unlimited"},
		{500, "500 B"},
		{1024, "1.00 KB"},
		{1024 * 1024, "1.00 MB"},
		{1024 * 1024 * 1024, "1.00 GB"},
		{math.MaxInt64, "Unlimited"},
	}

	for _, tt := range tests {
		got := FormatBytes(tt.input)
		if got != tt.want {
			t.Errorf("FormatBytes(%d) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestGetMemoryHeadroom(t *testing.T) {
	h := GetMemoryHeadroom()
	if h.TotalBytes == 0 {
		t.Errorf("expected TotalBytes > 0")
	}
	if h.AvailableBytes == 0 && h.HeadroomBytes == 0 {
		t.Errorf("expected AvailableBytes or HeadroomBytes > 0")
	}
	if h.Source == "" {
		t.Errorf("expected non-empty Source")
	}
	if h.UsedPercent > 100 {
		t.Errorf("expected UsedPercent <= 100, got %d", h.UsedPercent)
	}
}

func TestMemoryMonitor_StrideConfig(t *testing.T) {
	// Test default stride = 5000
	mDefault := NewMemoryMonitor(0)
	defer mDefault.Stop()
	if mDefault.Stride() != DefaultMemoryCheckStride {
		t.Errorf("expected default stride %d, got %d", DefaultMemoryCheckStride, mDefault.Stride())
	}

	// Test explicit stride
	mExplicit := NewMemoryMonitor(1000)
	defer mExplicit.Stop()
	if mExplicit.Stride() != 1000 {
		t.Errorf("expected stride 1000, got %d", mExplicit.Stride())
	}

	// Test environment variable stride
	origEnv := os.Getenv("ANCHORDB_MEMORY_CHECK_STRIDE")
	defer func() {
		if origEnv != "" {
			_ = os.Setenv("ANCHORDB_MEMORY_CHECK_STRIDE", origEnv)
		} else {
			_ = os.Unsetenv("ANCHORDB_MEMORY_CHECK_STRIDE")
		}
	}()

	_ = os.Setenv("ANCHORDB_MEMORY_CHECK_STRIDE", "12345")
	mEnv := NewMemoryMonitor(0)
	defer mEnv.Stop()
	if mEnv.Stride() != 12345 {
		t.Errorf("expected env stride 12345, got %d", mEnv.Stride())
	}
}

func TestMemoryMonitor_ConcurrentLockFree(t *testing.T) {
	m := NewMemoryMonitor(100)
	defer m.Stop()

	var wg sync.WaitGroup
	const numGoroutines = 20
	const opsPerGoroutine = 500

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				m.RecordMutation(1)
				h := m.GetHeadroom()
				if h.TotalBytes == 0 {
					t.Errorf("unexpected zero TotalBytes during concurrent reads")
					return
				}
			}
		}()
	}
	wg.Wait()
}
