package system

import (
	"runtime/debug"
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
