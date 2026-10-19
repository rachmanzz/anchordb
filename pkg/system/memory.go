// Package system provides memory monitoring, runtime metrics, and memory bounds management.
package system

import (
	"fmt"
	"math"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
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

	// Multipliers (powers of 1024 as per IEC/standard systems practice for RAM).
	// Every branch below assigns it before use.
	var multiplier int64
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
	if math.IsNaN(val) || math.IsInf(val, 0) || val > float64(math.MaxInt64)/float64(multiplier) {
		return 0, fmt.Errorf("memory limit %q exceeds supported range", s)
	}
	bytes := val * float64(multiplier)
	// float64(math.MaxInt64) rounds up to 2^63, so reject equality too before
	// converting (which would otherwise wrap to a negative int64).
	if math.IsInf(bytes, 0) || bytes >= float64(math.MaxInt64) {
		return 0, fmt.Errorf("memory limit %q exceeds supported range", s)
	}
	totalBytes := int64(bytes)
	if totalBytes <= 0 {
		return 0, fmt.Errorf("memory limit %q is below one byte", s)
	}
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
	units := []string{"KB", "MB", "GB", "TB", "PB", "EB"}
	if exp >= len(units) {
		return fmt.Sprintf("%.2f EiB", float64(b)/float64(div))
	}
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

// MemoryHeadroom describes system-level and container-level memory headroom for admission control.
type MemoryHeadroom struct {
	TotalBytes     uint64 `json:"total_bytes"`
	AvailableBytes uint64 `json:"available_bytes"`
	UsedPercent    uint8  `json:"used_percent"`
	HeadroomBytes  uint64 `json:"headroom_bytes"`
	CgroupLimited  bool   `json:"cgroup_limited"`
	Source         string `json:"source"`
}

// DefaultMemoryCheckStride is the default number of record mutations between background headroom samplings.
const DefaultMemoryCheckStride uint64 = 5000

// MemoryMonitor tracks memory headroom asynchronously using a configurable stride counter
// and background sampling, eliminating lock and syscall overhead on high-throughput hot paths.
type MemoryMonitor struct {
	current atomic.Pointer[MemoryHeadroom]
	counter atomic.Uint64
	stride  uint64
	trigger chan struct{}
	stop    chan struct{}
	done    chan struct{}
}

// NewMemoryMonitor creates and starts a MemoryMonitor.
// If stride is 0, it reads the ANCHORDB_MEMORY_CHECK_STRIDE environment variable,
// falling back to DefaultMemoryCheckStride (5000).
func NewMemoryMonitor(stride uint64) *MemoryMonitor {
	if stride == 0 {
		if envStr := os.Getenv("ANCHORDB_MEMORY_CHECK_STRIDE"); envStr != "" {
			if parsed, err := strconv.ParseUint(strings.TrimSpace(envStr), 10, 64); err == nil && parsed > 0 {
				stride = parsed
			}
		}
	}
	if stride == 0 {
		stride = DefaultMemoryCheckStride
	}

	m := &MemoryMonitor{
		stride:  stride,
		trigger: make(chan struct{}, 1),
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}

	// Immediate synchronous baseline sample
	initial := computeMemoryHeadroom()
	m.current.Store(&initial)

	go m.run()
	return m
}

func (m *MemoryMonitor) run() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer func() {
		ticker.Stop()
		close(m.done)
	}()

	for {
		select {
		case <-m.stop:
			return
		case <-ticker.C:
			fresh := computeMemoryHeadroom()
			m.current.Store(&fresh)
		case <-m.trigger:
			fresh := computeMemoryHeadroom()
			m.current.Store(&fresh)
		}
	}
}

// RecordMutation increments the mutation counter by delta and triggers an asynchronous
// memory check if a stride boundary is crossed. It never blocks.
func (m *MemoryMonitor) RecordMutation(delta uint64) {
	if m == nil || m.stride == 0 || delta == 0 {
		return
	}
	newCount := m.counter.Add(delta)
	oldCount := newCount - delta
	if (newCount / m.stride) > (oldCount / m.stride) {
		select {
		case m.trigger <- struct{}{}:
		default:
		}
	}
}

// GetHeadroom returns the latest sampled memory headroom without locks or syscalls.
func (m *MemoryMonitor) GetHeadroom() MemoryHeadroom {
	if m == nil {
		return computeMemoryHeadroom()
	}
	p := m.current.Load()
	if p == nil {
		fresh := computeMemoryHeadroom()
		m.current.Store(&fresh)
		return fresh
	}
	return *p
}

// Stop cleanly terminates the monitor's background sampling goroutine.
func (m *MemoryMonitor) Stop() {
	if m == nil {
		return
	}
	select {
	case <-m.stop:
		// already stopped
	default:
		close(m.stop)
		<-m.done
	}
}

// Stride returns the configured stride for this monitor.
func (m *MemoryMonitor) Stride() uint64 {
	if m == nil {
		return DefaultMemoryCheckStride
	}
	return m.stride
}

var (
	defaultMonitor     *MemoryMonitor
	defaultMonitorOnce sync.Once
)

// GetDefaultMemoryMonitor returns the global default MemoryMonitor singleton.
func GetDefaultMemoryMonitor() *MemoryMonitor {
	defaultMonitorOnce.Do(func() {
		defaultMonitor = NewMemoryMonitor(0)
	})
	return defaultMonitor
}

// GetMemoryHeadroom returns the current host and container memory headroom using the default monitor.
// This is lock-free and avoids syscall storms on hot paths.
func GetMemoryHeadroom() MemoryHeadroom {
	return GetDefaultMemoryMonitor().GetHeadroom()
}

// RecordMutation records delta mutations against the default monitor's stride counter.
func RecordMutation(delta uint64) {
	GetDefaultMemoryMonitor().RecordMutation(delta)
}

func computeMemoryHeadroom() MemoryHeadroom {
	var hostTotal, hostAvailable uint64
	var hasHostInfo bool

	// 1. Try reading /proc/meminfo (Linux host/container)
	if data, err := os.ReadFile("/proc/meminfo"); err == nil {
		lines := strings.Split(string(data), "\n")
		for _, line := range lines {
			if strings.HasPrefix(line, "MemTotal:") {
				fields := strings.Fields(line)
				if len(fields) >= 2 {
					if kb, err := strconv.ParseUint(fields[1], 10, 64); err == nil {
						hostTotal = kb * 1024
					}
				}
			} else if strings.HasPrefix(line, "MemAvailable:") {
				fields := strings.Fields(line)
				if len(fields) >= 2 {
					if kb, err := strconv.ParseUint(fields[1], 10, 64); err == nil {
						hostAvailable = kb * 1024
					}
				}
			}
		}
		if hostTotal > 0 && hostAvailable > 0 {
			hasHostInfo = true
		}
	}

	// 2. Check container cgroup limits and non-reclaimable memory usage
	cgroupLimit, _ := DetectCgroupMemoryLimit()
	cgroupUsed, hasCgroupUsed := readCgroupMemoryUsage()

	// 3. Synthesize effective memory headroom
	if hasHostInfo {
		hostUsed := hostTotal - hostAvailable
		hostUsedPct := uint8((hostUsed * 100) / hostTotal)

		if cgroupLimit > 0 && hasCgroupUsed && uint64(cgroupLimit) > cgroupUsed {
			cgroupAvailable := uint64(cgroupLimit) - cgroupUsed
			cgroupUsedPct := uint8((cgroupUsed * 100) / uint64(cgroupLimit))

			// Pick the more constrained metric
			effectiveUsedPct := hostUsedPct
			if cgroupUsedPct > effectiveUsedPct {
				effectiveUsedPct = cgroupUsedPct
			}

			effectiveHeadroom := hostAvailable
			if cgroupAvailable < effectiveHeadroom {
				effectiveHeadroom = cgroupAvailable
			}

			return MemoryHeadroom{
				TotalBytes:     uint64(cgroupLimit),
				AvailableBytes: effectiveHeadroom,
				UsedPercent:    effectiveUsedPct,
				HeadroomBytes:  effectiveHeadroom,
				CgroupLimited:  true,
				Source:         "cgroup+proc/meminfo",
			}
		}

		return MemoryHeadroom{
			TotalBytes:     hostTotal,
			AvailableBytes: hostAvailable,
			UsedPercent:    hostUsedPct,
			HeadroomBytes:  hostAvailable,
			CgroupLimited:  false,
			Source:         "proc/meminfo",
		}
	}

	// 4. Fallback for environments where /proc/meminfo is inaccessible
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	currentLimit := debug.SetMemoryLimit(-1)

	if currentLimit > 0 && currentLimit < math.MaxInt64 && uint64(currentLimit) > m.Sys {
		avail := uint64(currentLimit) - m.Sys
		usedPct := uint8((m.Sys * 100) / uint64(currentLimit))
		return MemoryHeadroom{
			TotalBytes:     uint64(currentLimit),
			AvailableBytes: avail,
			UsedPercent:    usedPct,
			HeadroomBytes:  avail,
			CgroupLimited:  cgroupLimit > 0,
			Source:         "runtime.GOMEMLIMIT",
		}
	}

	// Safe conservative fallback
	return MemoryHeadroom{
		TotalBytes:     m.Sys * 2,
		AvailableBytes: m.Sys,
		UsedPercent:    50,
		HeadroomBytes:  m.Sys,
		CgroupLimited:  false,
		Source:         "runtime.SysFallback",
	}
}

// readCgroupMemoryUsage reads non-reclaimable memory usage in containers.
// It excludes clean page cache (inactive_file and clean active_file) which Linux
// automatically evicts under memory pressure without triggering OOM.
func readCgroupMemoryUsage() (uint64, bool) {
	// 1. Try cgroup v2 memory.stat for real non-reclaimable memory (anon + file_dirty + slab_unreclaimable)
	if data, err := os.ReadFile("/sys/fs/cgroup/memory.stat"); err == nil {
		var anon, fileDirty, slabUnrec uint64
		var hasAnon bool
		lines := strings.Split(string(data), "\n")
		for _, line := range lines {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				switch fields[0] {
				case "anon":
					if v, err := strconv.ParseUint(fields[1], 10, 64); err == nil {
						anon = v
						hasAnon = true
					}
				case "file_dirty":
					if v, err := strconv.ParseUint(fields[1], 10, 64); err == nil {
						fileDirty = v
					}
				case "slab_unreclaimable":
					if v, err := strconv.ParseUint(fields[1], 10, 64); err == nil {
						slabUnrec = v
					}
				}
			}
		}
		if hasAnon {
			return anon + fileDirty + slabUnrec, true
		}
	}

	// 2. Try cgroup v1 memory.stat
	if data, err := os.ReadFile("/sys/fs/cgroup/memory/memory.stat"); err == nil {
		var rss, dirty uint64
		var hasRSS bool
		lines := strings.Split(string(data), "\n")
		for _, line := range lines {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				switch fields[0] {
				case "total_rss", "rss":
					if v, err := strconv.ParseUint(fields[1], 10, 64); err == nil {
						rss = v
						hasRSS = true
					}
				case "total_dirty", "dirty":
					if v, err := strconv.ParseUint(fields[1], 10, 64); err == nil {
						dirty = v
					}
				}
			}
		}
		if hasRSS {
			return rss + dirty, true
		}
	}

	// 3. Fallback to Go runtime MemStats (actual virtual/heap memory held from OS)
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	if m.Sys > m.HeapReleased {
		return m.Sys - m.HeapReleased, true
	} else if m.Sys > 0 {
		return m.Sys, true
	}

	// 4. Last resort: raw cgroup memory.current
	return readCgroupMemoryCurrent()
}

func readCgroupMemoryCurrent() (uint64, bool) {
	// Try cgroup v2
	if data, err := os.ReadFile("/sys/fs/cgroup/memory.current"); err == nil {
		s := strings.TrimSpace(string(data))
		if s != "" && s != "max" {
			if v, err := strconv.ParseUint(s, 10, 64); err == nil {
				return v, true
			}
		}
	}
	// Try cgroup v1
	if data, err := os.ReadFile("/sys/fs/cgroup/memory/memory.usage_in_bytes"); err == nil {
		s := strings.TrimSpace(string(data))
		if s != "" {
			if v, err := strconv.ParseUint(s, 10, 64); err == nil {
				return v, true
			}
		}
	}
	return 0, false
}
