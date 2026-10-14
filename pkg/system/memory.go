package system

import (
	"fmt"
	"math"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
)

// MemoryStats provides live runtime memory statistics
type MemoryStats struct {
	LimitBytes     int64  `json:"limit_bytes"`
	LimitHuman     string `json:"limit_human"`
	HeapAllocBytes uint64 `json:"heap_alloc_bytes"`
	HeapAllocHuman string `json:"heap_alloc_human"`
	HeapSysBytes   uint64 `json:"heap_sys_bytes"`
	HeapSysHuman   string `json:"heap_sys_human"`
	NumGC          uint32 `json:"num_gc"`
	CgroupLimit    int64  `json:"cgroup_limit_bytes,omitempty"`
}

// ParseMemoryBytes parses human-readable memory strings like "500MB", "1GB", "5GB", "512MiB", "1073741824".
func ParseMemoryBytes(s string) (int64, error) {
	s = strings.TrimSpace(strings.ToUpper(s))
	if s == "" {
		return 0, fmt.Errorf("empty memory limit string")
	}

	// Multipliers (powers of 1024 as per IEC/standard systems practice for RAM)
	var multiplier int64 = 1
	var numStr string

	if strings.HasSuffix(s, "TIB") || strings.HasSuffix(s, "TB") {
		multiplier = 1024 * 1024 * 1024 * 1024
		numStr = strings.TrimSuffix(strings.TrimSuffix(s, "TIB"), "TB")
	} else if strings.HasSuffix(s, "T") {
		multiplier = 1024 * 1024 * 1024 * 1024
		numStr = strings.TrimSuffix(s, "T")
	} else if strings.HasSuffix(s, "GIB") || strings.HasSuffix(s, "GB") {
		multiplier = 1024 * 1024 * 1024
		numStr = strings.TrimSuffix(strings.TrimSuffix(s, "GIB"), "GB")
	} else if strings.HasSuffix(s, "G") {
		multiplier = 1024 * 1024 * 1024
		numStr = strings.TrimSuffix(s, "G")
	} else if strings.HasSuffix(s, "MIB") || strings.HasSuffix(s, "MB") {
		multiplier = 1024 * 1024
		numStr = strings.TrimSuffix(strings.TrimSuffix(s, "MIB"), "MB")
	} else if strings.HasSuffix(s, "M") {
		multiplier = 1024 * 1024
		numStr = strings.TrimSuffix(s, "M")
	} else if strings.HasSuffix(s, "KIB") || strings.HasSuffix(s, "KB") {
		multiplier = 1024
		numStr = strings.TrimSuffix(strings.TrimSuffix(s, "KIB"), "KB")
	} else if strings.HasSuffix(s, "K") {
		multiplier = 1024
		numStr = strings.TrimSuffix(s, "K")
	} else if strings.HasSuffix(s, "B") {
		multiplier = 1
		numStr = strings.TrimSuffix(s, "B")
	} else {
		// Pure number without suffix -> assume bytes
		multiplier = 1
		numStr = s
	}

	numStr = strings.TrimSpace(numStr)
	val, err := strconv.ParseFloat(numStr, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid numeric value in memory limit %q: %w", s, err)
	}

	if val <= 0 {
		return 0, fmt.Errorf("memory limit must be greater than 0, got %v", val)
	}

	totalBytes := int64(val * float64(multiplier))
	return totalBytes, nil
}

// FormatBytes formats byte counts into human-readable strings like "500.00 MB", "1.00 GB".
func FormatBytes(b int64) string {
	if b == math.MaxInt64 || b <= 0 {
		return "Unlimited"
	}
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	units := []string{"KB", "MB", "GB", "TB"}
	return fmt.Sprintf("%.2f %s", float64(b)/float64(div), units[exp])
}

// DetectCgroupMemoryLimit attempts to read cgroup memory limits in Linux containers (cgroup v2 & v1).
func DetectCgroupMemoryLimit() (int64, error) {
	// 1. Try cgroup v2
	if data, err := os.ReadFile("/sys/fs/cgroup/memory.max"); err == nil {
		str := strings.TrimSpace(string(data))
		if str != "max" && str != "" {
			if limit, err := strconv.ParseInt(str, 10, 64); err == nil && limit > 0 && limit < (1<<60) {
				return limit, nil
			}
		}
	}

	// 2. Try cgroup v1
	if data, err := os.ReadFile("/sys/fs/cgroup/memory/memory.limit_in_bytes"); err == nil {
		str := strings.TrimSpace(string(data))
		if limit, err := strconv.ParseInt(str, 10, 64); err == nil && limit > 0 && limit < (1<<60) {
			return limit, nil
		}
	}

	return 0, fmt.Errorf("no cgroup limit found or unlimited")
}

// ApplyMemoryLimit sets the Go soft memory limit based on explicit config or container cgroup limits.
// Returns the applied limit in bytes and a human-readable summary.
func ApplyMemoryLimit(explicitConfig string) (int64, string, error) {
	var targetLimit int64
	var source string

	config := strings.TrimSpace(explicitConfig)
	if config == "" {
		config = os.Getenv("ANCHORDB_MEMORY_LIMIT")
	}

	if config != "" {
		parsed, err := ParseMemoryBytes(config)
		if err != nil {
			return 0, "", fmt.Errorf("invalid ANCHORDB_MEMORY_LIMIT: %w", err)
		}
		targetLimit = parsed
		source = fmt.Sprintf("explicit configuration (%s)", config)
	} else if cgroupLimit, err := DetectCgroupMemoryLimit(); err == nil && cgroupLimit > 0 {
		// If running in container with memory limit (e.g. 1GB), set soft limit to 85% of container limit
		// to leave headroom for OS buffers and prevent container OOM killer.
		targetLimit = int64(float64(cgroupLimit) * 0.85)
		source = fmt.Sprintf("auto-detected container cgroup (%s -> 85%% soft limit)", FormatBytes(cgroupLimit))
	} else {
		// No limit specified, return current runtime setting
		current := debug.SetMemoryLimit(-1)
		return current, "unlimited (default OS/runtime)", nil
	}

	debug.SetMemoryLimit(targetLimit)
	summary := fmt.Sprintf("%s [%s]", FormatBytes(targetLimit), source)
	return targetLimit, summary, nil
}

// GetMemoryStats returns current heap, sys, and configured memory limits.
func GetMemoryStats() MemoryStats {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	currentLimit := debug.SetMemoryLimit(-1)
	cgroupLimit, _ := DetectCgroupMemoryLimit()

	return MemoryStats{
		LimitBytes:     currentLimit,
		LimitHuman:     FormatBytes(currentLimit),
		HeapAllocBytes: m.HeapAlloc,
		HeapAllocHuman: FormatBytes(int64(m.HeapAlloc)),
		HeapSysBytes:   m.Sys,
		HeapSysHuman:   FormatBytes(int64(m.Sys)),
		NumGC:          m.NumGC,
		CgroupLimit:    cgroupLimit,
	}
}
