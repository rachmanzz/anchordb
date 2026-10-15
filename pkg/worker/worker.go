package worker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ErrQueueFull is returned when the worker pool queue is full
var ErrQueueFull = errors.New("worker task queue is full")

// ErrMemoryFull is returned when worker memory pressure exceeds threshold
var ErrMemoryFull = errors.New("worker memory limit exceeded")

// ErrDraining is returned when the pool is draining and rejects new work
var ErrDraining = errors.New("worker pool is draining, not accepting new tasks")

// ErrCanceled is returned/reported for queued tasks safely canceled before execution
var ErrCanceled = errors.New("task canceled before execution")

// ErrNoGlobalPool is returned by global helpers when no global pool was initialized
var ErrNoGlobalPool = errors.New("no global worker pool initialized")

// ErrWorkerUnavailable is returned when an asynchronous dispatch is requested
// but the pool is disabled or stopped.
var ErrWorkerUnavailable = errors.New("worker pool is not available for asynchronous dispatch")

// ErrShutdownTimeout is returned by Shutdown when the pool could not be fully
// stopped within (up to twice) ShutdownTimeout because a task ignored context
// cancellation. The call returns instead of blocking forever; a stuck task
// cannot be killed in Go and is left to finish on its own.
var ErrShutdownTimeout = errors.New("worker pool shutdown exceeded the configured timeout")

// Config holds configuration parameters for the background worker pool
type Config struct {
	Enabled         bool          `json:"enabled"`
	Concurrency     int           `json:"concurrency"`
	QueueSize       int           `json:"queue_size"`
	ShutdownTimeout time.Duration `json:"shutdown_timeout"`

	// Memory-aware admission control
	EnforceMemory   bool    `json:"enforce_memory"`
	MaxMemoryBytes  int64   `json:"max_memory_bytes"` // 0 = use current GOMEMLIMIT
	MemoryThreshold float64 `json:"memory_threshold"` // (0,1], default 0.85
}

// DefaultConfig provides recommended settings based on host CPU capacity
func DefaultConfig() Config {
	concurrency := runtime.NumCPU() * 2
	if concurrency < 2 {
		concurrency = 2
	}
	return Config{
		Enabled:         true,
		Concurrency:     concurrency,
		QueueSize:       1000,
		ShutdownTimeout: 5 * time.Second,
		EnforceMemory:   false,
		MaxMemoryBytes:  0,
		MemoryThreshold: 0.85,
	}
}

// PoolStats holds real-time telemetry metrics for the worker pool
type PoolStats struct {
	Enabled        bool   `json:"enabled"`
	Running        bool   `json:"running"`
	Concurrency    int    `json:"concurrency"`
	Dispatched     int64  `json:"dispatched"`
	Completed      int64  `json:"completed"`
	Failed         int64  `json:"failed"`
	Canceled       int64  `json:"canceled"`
	Retries        int64  `json:"retries"`
	InFlight       int64  `json:"in_flight"`
	Queued         int    `json:"queued"`
	Draining       bool   `json:"draining"`
	MemoryPressure bool   `json:"memory_pressure"`
	UsageBytes     uint64 `json:"usage_bytes"`
	UsageHuman     string `json:"usage_human"`
	UsageSource    string `json:"usage_source"`
	HeapAllocBytes uint64 `json:"heap_alloc_bytes"`
	HeapAllocHuman string `json:"heap_alloc_human"`
	LimitBytes     int64  `json:"limit_bytes"`
	LimitHuman     string `json:"limit_human"`
}

// Task represents a job to be executed with optional retry/fallback/hooks
type Task struct {
	ID    string
	Model string
	Name  string

	// Fn is the function to execute. If it returns error (or panics), retry/fallback logic applies.
	Fn func(ctx context.Context) error

	// Ctx allows per-task context; if nil, background context is used
	Ctx context.Context

	// RetryCount is maximum number of retry attempts AFTER the first attempt.
	RetryCount int

	// RetryBackoffBase is base backoff between retry attempts
	RetryBackoffBase time.Duration

	// RetryBackoffJitter adds random jitter up to this amount to backoff
	RetryBackoffJitter time.Duration

	// OnSuccess is called once if execution eventually succeeds
	OnSuccess func(ctx context.Context)

	// OnError is called on final failure
	OnError func(ctx context.Context, err error)

	// Fallback is called if all attempts fail; if returns nil -> treated as SUCCESS
	Fallback func(ctx context.Context, err error) error
}

// Option is a functional option to configure Task
type Option func(*Task)

// WithRetry sets retry count and base backoff
func WithRetry(count int, backoff time.Duration) Option {
	return func(t *Task) {
		if count < 0 {
			count = 0
		}
		t.RetryCount = count
		t.RetryBackoffBase = backoff
	}
}

// WithRetryJitter adds random jitter to retry backoff
func WithRetryJitter(jitter time.Duration) Option {
	return func(t *Task) {
		t.RetryBackoffJitter = jitter
	}
}

// WithOnSuccess registers success callback
func WithOnSuccess(fn func(ctx context.Context)) Option {
	return func(t *Task) {
		t.OnSuccess = fn
	}
}

// WithOnError registers final error callback
func WithOnError(fn func(ctx context.Context, err error)) Option {
	return func(t *Task) {
		t.OnError = fn
	}
}

// WithFallback registers fallback-on-error callback
func WithFallback(fn func(ctx context.Context, err error) error) Option {
	return func(t *Task) {
		t.Fallback = fn
	}
}

// WithContext sets per-task context
func WithContext(ctx context.Context) Option {
	return func(t *Task) {
		t.Ctx = ctx
	}
}

// WithID sets a unique identifier for the task to enable scoped tracking and cancellation
func WithID(id string) Option {
	return func(t *Task) {
		t.ID = id
	}
}

// WithModel sets the model/tenant name for the task to enable scoped operations
func WithModel(model string) Option {
	return func(t *Task) {
		t.Model = model
	}
}

type jobItem struct {
	task Task
}

// Pool manages concurrent background job execution using pure Go standard library channels
type Pool struct {
	cfg         Config
	lifecycleMu sync.Mutex   // serializes Start/Shutdown (and restart)
	chanMu      sync.RWMutex // guards taskCh/quitCh/taskClosed/quitClosed
	taskCh      chan jobItem
	quitCh      chan struct{}
	taskClosed  bool
	quitClosed  bool
	wg          sync.WaitGroup
	dispatched  atomic.Int64
	completed   atomic.Int64
	failed      atomic.Int64
	canceled    atomic.Int64
	retries     atomic.Int64
	inflight    atomic.Int64
	pending     atomic.Int64
	running     atomic.Bool
	draining    atomic.Bool
	stopped     atomic.Bool // true after Shutdown completed or gave up
	forced      atomic.Bool // true when the last Shutdown hit ErrShutdownTimeout
}

// NewPool initializes a pure Go worker pool instance
func NewPool(cfg Config) *Pool {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = runtime.NumCPU() * 2
		if cfg.Concurrency < 2 {
			cfg.Concurrency = 2
		}
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 1000
	}
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = 5 * time.Second
	}
	if cfg.MemoryThreshold <= 0 {
		cfg.MemoryThreshold = 0.85
	}
	if cfg.MemoryThreshold > 1 {
		cfg.MemoryThreshold = 1
	}

	return &Pool{
		cfg:    cfg,
		taskCh: make(chan jobItem, cfg.QueueSize),
		quitCh: make(chan struct{}),
	}
}

func fmtBytes(b int64) string {
	if b <= 0 {
		return "0 B"
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
		// int64 cannot exceed the exabyte range, so this is unreachable; keep a
		// safe fallback rather than clamping the unit while div keeps growing.
		return fmt.Sprintf("%.2f EiB", float64(b)/float64(div))
	}
	return fmt.Sprintf("%.2f %s", float64(b)/float64(div), units[exp])
}

// processMemoryUsage returns the best available estimate of the memory the
// process is currently using, together with a short label describing the source.
//
// It deliberately avoids HeapAlloc as the primary signal: AnchorDB stores records
// via syscall.Mmap (file-backed, resident) which never shows up in the Go heap,
// so a HeapAlloc-only gate would be blind to the dominant memory consumer.
// Preference order:
//  1. cgroup v2  (memory.current)        — authoritative inside containers
//  2. cgroup v1  (memory.usage_in_bytes) — authoritative, older kernels
//  3. /proc/self/statm RSS               — physical resident set (incl. mmap)
//  4. runtime.MemStats.Sys               — pure-Go fallback
func processMemoryUsage() (uint64, string) {
	if v, ok := readUintFile("/sys/fs/cgroup/memory.current"); ok {
		return v, "cgroup-v2(memory.current)"
	}
	if v, ok := readUintFile("/sys/fs/cgroup/memory/memory.usage_in_bytes"); ok {
		return v, "cgroup-v1(memory.usage_in_bytes)"
	}
	if v, ok := readProcRSS(); ok {
		return v, "proc/self/statm(RSS)"
	}
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.Sys, "runtime.Sys"
}

func readUintFile(path string) (uint64, bool) {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, false
	}
	s := strings.TrimSpace(string(b))
	if s == "" || s == "max" {
		return 0, false
	}
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

func readProcRSS() (uint64, bool) {
	b, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0, false
	}
	fields := strings.Fields(string(b))
	if len(fields) < 2 {
		return 0, false
	}
	pages, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return 0, false
	}
	return pages * uint64(os.Getpagesize()), true
}

func (p *Pool) checkMemory() error {
	if !p.cfg.EnforceMemory {
		return nil
	}
	limit := p.cfg.MaxMemoryBytes
	if limit <= 0 {
		limit = debug.SetMemoryLimit(-1)
	}
	if limit <= 0 {
		return nil // unlimited
	}

	usage, _ := processMemoryUsage()
	thresh := float64(limit) * p.cfg.MemoryThreshold
	if int64(usage) >= int64(thresh) {
		return ErrMemoryFull
	}
	return nil
}

// Start boots worker goroutines. It is idempotent while running: calling it on a
// live pool is a no-op. If the pool was previously shut down cleanly it rebuilds
// fresh channels and starts again; restarting after a *forced* shutdown (a task
// that ignored cancellation) is refused to avoid overlapping workers.
func (p *Pool) Start() {
	if !p.cfg.Enabled {
		log.Println("[WORKER] Worker pool dinonaktifkan oleh konfigurasi.")
		return
	}
	p.lifecycleMu.Lock()
	defer p.lifecycleMu.Unlock()
	if p.running.Load() {
		return
	}
	if p.stopped.Load() {
		if p.forced.Load() {
			log.Println("[WORKER] Tidak bisa restart setelah shutdown paksa (task bandel masih berjalan).")
			return
		}
		p.chanMu.Lock()
		p.taskCh = make(chan jobItem, p.cfg.QueueSize)
		p.quitCh = make(chan struct{})
		p.taskClosed = false
		p.quitClosed = false
		p.chanMu.Unlock()
		p.stopped.Store(false)
		p.pending.Store(0)
		p.inflight.Store(0)
	}
	p.draining.Store(false)
	p.running.Store(true)

	p.chanMu.RLock()
	taskCh, quitCh := p.taskCh, p.quitCh
	p.chanMu.RUnlock()
	for i := 0; i < p.cfg.Concurrency; i++ {
		p.wg.Add(1)
		go p.workerLoop(taskCh, quitCh)
	}
	log.Printf("[WORKER] Worker pool aktif dengan %d goroutine pekerja (antrean: %d)",
		p.cfg.Concurrency, p.cfg.QueueSize)
}

func (p *Pool) workerLoop(taskCh <-chan jobItem, quitCh <-chan struct{}) {
	defer p.wg.Done()
	for {
		select {
		case <-quitCh:
			return
		case job, ok := <-taskCh:
			if !ok {
				return
			}
			p.inflight.Add(1)
			p.executeJob(job.task, quitCh)
			p.inflight.Add(-1)
			p.pending.Add(-1)
		}
	}
}

func (p *Pool) executeJob(t Task, quitCh <-chan struct{}) {
	start := time.Now()
	ctx := t.Ctx
	if ctx == nil {
		ctx = context.Background()
	}

	var lastErr error
	attempts := 1 + t.RetryCount
	for i := 0; i < attempts; i++ {
		var err error
		func() {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("panic: %v", r)
				}
			}()
			err = t.Fn(ctx)
		}()

		if err == nil {
			p.completed.Add(1)
			if t.OnSuccess != nil {
				func() {
					defer func() {
						if r := recover(); r != nil {
							log.Printf("[WORKER] OnSuccess panic on task %q: %v", t.Name, r)
						}
					}()
					t.OnSuccess(ctx)
				}()
			}
			log.Printf("[WORKER] Task %q SUKSES (attempt %d/%d, durasi: %v)", t.Name, i+1, attempts, time.Since(start))
			return
		}

		lastErr = err
		if i < attempts-1 {
			p.retries.Add(1)
			d := t.RetryBackoffBase
			if d > 0 && t.RetryBackoffJitter > 0 {
				j := time.Duration(rand.Int63n(int64(t.RetryBackoffJitter)))
				d += j
			}
			if d > 0 {
				timer := time.NewTimer(d)
				select {
				case <-ctx.Done():
					timer.Stop()
					lastErr = ctx.Err()
					goto FINAL
				case <-quitCh:
					timer.Stop()
					lastErr = errors.New("worker shutting down")
					goto FINAL
				case <-timer.C:
				}
			}
			log.Printf("[WORKER] Task %q GAGAL (attempt %d/%d): %v; retry...", t.Name, i+1, attempts, err)
			continue
		}
		break
	}

FINAL:
	var finalErr = lastErr
	if finalErr == nil {
		finalErr = errors.New("unknown error")
	}

	if t.Fallback != nil {
		var fbErr error
		func() {
			defer func() {
				if r := recover(); r != nil {
					fbErr = fmt.Errorf("fallback panic: %v", r)
				}
			}()
			fbErr = t.Fallback(ctx, finalErr)
		}()
		if fbErr == nil {
			p.completed.Add(1)
			if t.OnSuccess != nil {
				func() {
					defer func() {
						if r := recover(); r != nil {
							log.Printf("[WORKER] OnSuccess panic on task %q (via fallback): %v", t.Name, r)
						}
					}()
					t.OnSuccess(ctx)
				}()
			}
			log.Printf("[WORKER] Task %q SUKSES via FALLBACK (durasi: %v)", t.Name, time.Since(start))
			return
		}
		finalErr = fbErr
	}

	p.failed.Add(1)
	if t.OnError != nil {
		func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("[WORKER] OnError panic on task %q: %v", t.Name, r)
				}
			}()
			t.OnError(ctx, finalErr)
		}()
	}
	log.Printf("[WORKER] Task %q GAGAL (all attempts): %v (durasi: %v)", t.Name, finalErr, time.Since(start))
}

// Dispatch submits an async task to the worker pool. If pool is disabled or not running, runs synchronously.
func (p *Pool) Dispatch(ctx context.Context, name string, fn func(ctx context.Context) error, opts ...Option) error {
	task := Task{
		Name: name,
		Fn:   fn,
		Ctx:  ctx,
	}
	for _, o := range opts {
		if o != nil {
			o(&task)
		}
	}
	return p.dispatchTask(task, true)
}

// DispatchAsync enqueues a task without falling back to synchronous execution.
// Use this when the caller promises that work will run in the background.
func (p *Pool) DispatchAsync(ctx context.Context, name string, fn func(ctx context.Context) error, opts ...Option) error {
	task := Task{Name: name, Fn: fn, Ctx: ctx}
	for _, o := range opts {
		if o != nil {
			o(&task)
		}
	}
	return p.dispatchTask(task, false)
}

// DispatchTask submits a Task to the worker pool.
func (p *Pool) DispatchTask(t Task) error {
	return p.dispatchTask(t, true)
}

func (p *Pool) dispatchTask(t Task, allowSync bool) error {
	if t.Fn == nil {
		return errors.New("task function is nil")
	}
	if p.draining.Load() {
		p.failed.Add(1)
		return fmt.Errorf("failed to dispatch task %q: %w", t.Name, ErrDraining)
	}
	if !p.cfg.Enabled || !p.running.Load() {
		if !allowSync {
			return ErrWorkerUnavailable
		}
		p.executeJobSync(t)
		return nil
	}

	if err := p.checkMemory(); err != nil {
		p.failed.Add(1)
		return fmt.Errorf("failed to dispatch task %q: %w", t.Name, err)
	}

	ctx := t.Ctx
	if ctx == nil {
		ctx = context.Background()
	}

	p.dispatched.Add(1)
	p.pending.Add(1)
	item := jobItem{task: t}

	// Hold the read lock across the non-blocking send so Shutdown cannot close
	// the channel in between (which would panic on send-to-closed). The closed /
	// draining state MUST be re-checked under this lock: the checks above are
	// only a fast path and race with Shutdown (TOCTOU).
	p.chanMu.RLock()
	defer p.chanMu.RUnlock()

	if p.taskClosed || p.draining.Load() {
		p.pending.Add(-1)
		p.failed.Add(1)
		return fmt.Errorf("failed to dispatch task %q: %w", t.Name, ErrDraining)
	}
	if !p.cfg.Enabled || !p.running.Load() {
		p.pending.Add(-1)
		p.failed.Add(1)
		if !allowSync {
			return ErrWorkerUnavailable
		}
		return fmt.Errorf("failed to dispatch task %q: %w", t.Name, ErrDraining)
	}

	select {
	case p.taskCh <- item:
		return nil
	case <-ctx.Done():
		p.pending.Add(-1)
		p.failed.Add(1)
		return ctx.Err()
	default:
		p.pending.Add(-1)
		p.failed.Add(1)
		return fmt.Errorf("failed to dispatch task %q: %w", t.Name, ErrQueueFull)
	}
}

func (p *Pool) executeJobSync(t Task) {
	start := time.Now()
	ctx := t.Ctx
	if ctx == nil {
		ctx = context.Background()
	}

	var lastErr error
	attempts := 1 + t.RetryCount
	for i := 0; i < attempts; i++ {
		var err error
		func() {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("panic: %v", r)
				}
			}()
			err = t.Fn(ctx)
		}()
		if err == nil {
			p.completed.Add(1)
			if t.OnSuccess != nil {
				func() {
					defer func() {
						if r := recover(); r != nil {
							log.Printf("[WORKER] OnSuccess panic on task %q: %v", t.Name, r)
						}
					}()
					t.OnSuccess(ctx)
				}()
			}
			log.Printf("[WORKER] Task %q SUKSES (sync, attempt %d/%d, durasi: %v)", t.Name, i+1, attempts, time.Since(start))
			return
		}
		lastErr = err
		if i < attempts-1 {
			p.retries.Add(1)
			d := t.RetryBackoffBase
			if d > 0 && t.RetryBackoffJitter > 0 {
				j := time.Duration(rand.Int63n(int64(t.RetryBackoffJitter)))
				d += j
			}
			if d > 0 {
				timer := time.NewTimer(d)
				select {
				case <-ctx.Done():
					timer.Stop()
					lastErr = ctx.Err()
					goto FINALSYNC
				case <-timer.C:
				}
			}
			log.Printf("[WORKER] Task %q GAGAL (sync, attempt %d/%d): %v; retry...", t.Name, i+1, attempts, err)
			continue
		}
		break
	}

FINALSYNC:
	var finalErr = lastErr
	if finalErr == nil {
		finalErr = errors.New("unknown error")
	}
	if t.Fallback != nil {
		var fbErr error
		func() {
			defer func() {
				if r := recover(); r != nil {
					fbErr = fmt.Errorf("fallback panic: %v", r)
				}
			}()
			fbErr = t.Fallback(ctx, finalErr)
		}()
		if fbErr == nil {
			p.completed.Add(1)
			if t.OnSuccess != nil {
				func() {
					defer func() {
						if r := recover(); r != nil {
							log.Printf("[WORKER] OnSuccess panic on task %q (via fallback): %v", t.Name, r)
						}
					}()
					t.OnSuccess(ctx)
				}()
			}
			log.Printf("[WORKER] Task %q SUKSES via FALLBACK (sync, durasi: %v)", t.Name, time.Since(start))
			return
		}
		finalErr = fbErr
	}
	p.failed.Add(1)
	if t.OnError != nil {
		func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("[WORKER] OnError panic on task %q: %v", t.Name, r)
				}
			}()
			t.OnError(ctx, finalErr)
		}()
	}
	log.Printf("[WORKER] Task %q GAGAL (sync, all attempts): %v (durasi: %v)", t.Name, finalErr, time.Since(start))
}

// Stats returns a telemetry snapshot of the worker pool
func (p *Pool) Stats() PoolStats {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	limit := p.cfg.MaxMemoryBytes
	if limit <= 0 {
		limit = debug.SetMemoryLimit(-1)
	}

	thresh := float64(limit)
	if limit > 0 {
		thresh = float64(limit) * p.cfg.MemoryThreshold
	}

	usage, source := processMemoryUsage()
	pressure := false
	if limit > 0 && int64(usage) >= int64(thresh) {
		pressure = true
	}

	return PoolStats{
		Enabled:        p.cfg.Enabled,
		Running:        p.running.Load(),
		Concurrency:    p.cfg.Concurrency,
		Dispatched:     p.dispatched.Load(),
		Completed:      p.completed.Load(),
		Failed:         p.failed.Load(),
		Canceled:       p.canceled.Load(),
		Retries:        p.retries.Load(),
		InFlight:       p.inflight.Load(),
		Queued:         p.Queued(),
		Draining:       p.draining.Load(),
		MemoryPressure: pressure,
		UsageBytes:     usage,
		UsageHuman:     fmtBytes(int64(usage)),
		UsageSource:    source,
		HeapAllocBytes: m.HeapAlloc,
		HeapAllocHuman: fmtBytes(int64(m.HeapAlloc)),
		LimitBytes:     limit,
		LimitHuman:     fmtBytes(limit),
	}
}

// InFlight returns the number of tasks currently being executed.
func (p *Pool) InFlight() int64 { return p.inflight.Load() }

// Queued returns the number of tasks waiting in the queue.
func (p *Pool) Queued() int {
	p.chanMu.RLock()
	defer p.chanMu.RUnlock()
	return len(p.taskCh)
}

// Draining reports whether the pool has stopped accepting new tasks.
func (p *Pool) Draining() bool { return p.draining.Load() }

// BeginDrain stops accepting new tasks (DispatchTask returns ErrDraining) without
// touching the queue or running workers. Use before WaitIdle for a graceful stop.
// It is reversible via Resume.
func (p *Pool) BeginDrain() {
	p.draining.Store(true)
}

// Resume re-opens intake after BeginDrain/WaitBeforeShutdown, letting the pool
// accept new tasks again without a full restart. It is a no-op if the pool has
// already been shut down.
func (p *Pool) Resume() {
	if p.running.Load() {
		p.draining.Store(false)
	}
}

// WaitIdle blocks until every dispatched task has finished (queue empty and no
// in-flight work), or until ctx is done. It does NOT stop the pool and does NOT
// cancel anything — it only waits. Returns ctx.Err() on timeout/cancel.
func (p *Pool) WaitIdle(ctx context.Context) error {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		if p.pending.Load() == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// SafeCancel drains all *queued* (not yet started) tasks and reports each as
// canceled via its OnError hook. In-flight tasks are left untouched, so this is
// safe to call concurrently and repeatedly. Returns the number of canceled tasks.
func (p *Pool) SafeCancel() int {
	return p.CancelMatching(func(t Task) bool { return true })
}

// CancelMatching drains queued tasks matching the predicate. Non-matching tasks
// are safely preserved in the queue in their relative order.
func (p *Pool) CancelMatching(predicate func(t Task) bool) int {
	if predicate == nil {
		return 0
	}
	p.chanMu.Lock()
	// Once shutdown closes the channel, workers own draining its remaining
	// buffered jobs. Do not consume jobs that cannot be safely requeued.
	if p.taskClosed {
		p.chanMu.Unlock()
		return 0
	}

	taskCh := p.taskCh
	var kept []jobItem
	var canceled []jobItem
	for {
		select {
		case job, ok := <-taskCh:
			if !ok {
				// taskClosed is changed under chanMu, so a closed channel here is
				// an invariant violation. Keep every removed job accounted for.
				canceled = append(canceled, kept...)
				kept = nil
				goto DONE
			}
			if predicate(job.task) {
				canceled = append(canceled, job)
			} else {
				kept = append(kept, job)
			}
		default:
			goto DONE
		}
	}

DONE:
	for _, job := range kept {
		// Reinsert while still holding chanMu, before producers can enqueue more.
		taskCh <- job
	}
	p.chanMu.Unlock()

	// User callbacks may dispatch more work or acquire pool locks. Invoke only
	// after releasing chanMu to avoid callback-induced deadlocks.
	for _, job := range canceled {
		p.canceled.Add(1)
		p.pending.Add(-1)
		p.reportCanceled(job.task)
	}
	return len(canceled)
}

// CancelByModel cancels all queued tasks belonging to the given model.
func (p *Pool) CancelByModel(model string) int {
	if model == "" {
		return 0
	}
	return p.CancelMatching(func(t Task) bool {
		return t.Model == model
	})
}

// CancelByID cancels a specific queued task by its task/job ID.
func (p *Pool) CancelByID(id string) bool {
	if id == "" {
		return false
	}
	return p.CancelMatching(func(t Task) bool {
		return t.ID == id
	}) > 0
}

func (p *Pool) reportCanceled(t Task) {
	ctx := t.Ctx
	if ctx == nil {
		ctx = context.Background()
	}
	if t.OnError != nil {
		func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("[WORKER] OnError panic on canceled task %q: %v", t.Name, r)
				}
			}()
			t.OnError(ctx, ErrCanceled)
		}()
	}
	log.Printf("[WORKER] Task %q DIBATALKAN sebelum eksekusi (antrean dibersihkan).", t.Name)
}

// Shutdown gracefully drains the queue and stops the pool. It first stops
// accepting new work, then closes the task channel so workers can drain remaining
// queued jobs, waiting up to ShutdownTimeout. If drain does not finish in time it
// force-signals workers via quitCh and waits one more ShutdownTimeout; if a stuck
// task still refuses to finish it returns ErrShutdownTimeout instead of blocking
// forever (Go cannot kill a goroutine). A successful shutdown is restartable via
// Start; after a forced (timed-out) shutdown Start is refused.
func (p *Pool) Shutdown() error {
	p.lifecycleMu.Lock()
	defer p.lifecycleMu.Unlock()

	if !p.running.CompareAndSwap(true, false) {
		return nil
	}
	log.Println("[WORKER] Menghentikan worker pool...")
	p.draining.Store(true)

	// Close the task channel: workers finish buffered jobs, then exit on !ok.
	p.chanMu.Lock()
	if !p.taskClosed {
		close(p.taskCh)
		p.taskClosed = true
	}
	p.chanMu.Unlock()

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		p.stopped.Store(true)
		p.forced.Store(false)
		log.Println("[WORKER] Worker pool berhasil dihentikan secara bersih (antrean ter-drain).")
		return nil
	case <-time.After(p.cfg.ShutdownTimeout):
		// Force workers out (abort retry backoff / unblock workerLoop selects).
		p.chanMu.Lock()
		if !p.quitClosed {
			close(p.quitCh)
			p.quitClosed = true
		}
		p.chanMu.Unlock()

		select {
		case <-done:
			p.stopped.Store(true)
			p.forced.Store(false)
			log.Println("[WORKER] Shutdown selesai setelah sinyal paksa (sebagian task mungkin terpotong).")
			return nil
		case <-time.After(p.cfg.ShutdownTimeout):
			p.stopped.Store(true)
			p.forced.Store(true)
			log.Println("[WORKER] Shutdown melebihi batas waktu; ada task yang mengabaikan pembatalan. Pool ditandai berhenti paksa.")
			return ErrShutdownTimeout
		}
	}
}

// WaitBeforeShutdown stops accepting new tasks and blocks until all queued and
// in-flight tasks complete, bounded by ctx. If ctx expires, remaining QUEUED
// tasks are safely canceled (in-flight work is never killed). It does not stop
// the pool; call Shutdown afterward. The drain is reversible: call Resume to
// accept new tasks again without restarting.
func (p *Pool) WaitBeforeShutdown(ctx context.Context) error {
	p.BeginDrain()
	if err := p.WaitIdle(ctx); err != nil {
		p.SafeCancel()
		return err
	}
	return nil
}

// ---------------------------------------------------------------------------
// Global pool registry & package-level helpers
// ---------------------------------------------------------------------------

var (
	globalMu   sync.RWMutex
	globalPool *Pool
	initMu     sync.Mutex // serializes InitGlobal so concurrent calls cannot leak pools
)

// InitGlobal creates and starts a process-wide worker pool, replacing any
// previously registered global pool (which is shut down first). Safe to call
// multiple times, including concurrently (calls are serialized); returns the
// new pool.
func InitGlobal(cfg Config) *Pool {
	initMu.Lock()
	defer initMu.Unlock()

	p := NewPool(cfg)
	p.Start()

	globalMu.Lock()
	prev := globalPool
	globalPool = p
	globalMu.Unlock()

	if prev != nil && prev != p {
		_ = prev.Shutdown()
	}
	return p
}

// Global returns the currently registered global pool, or nil if none.
func Global() *Pool {
	globalMu.RLock()
	defer globalMu.RUnlock()
	return globalPool
}

// SetGlobal registers an externally created pool as the global one (without
// creating or starting a new one). Pass nil to clear the registration.
func SetGlobal(p *Pool) {
	globalMu.Lock()
	globalPool = p
	globalMu.Unlock()
}

// WaitBeforeShutdown is the globally callable form of Pool.WaitBeforeShutdown:
// it stops the global pool from accepting new tasks and waits until all queued
// and in-flight tasks finish (bounded by ctx). On timeout it safely cancels the
// remaining *queued* tasks (in-flight work is never killed). Returns
// ErrNoGlobalPool when no global pool is registered.
func WaitBeforeShutdown(ctx context.Context) error {
	p := Global()
	if p == nil {
		return ErrNoGlobalPool
	}
	return p.WaitBeforeShutdown(ctx)
}

// SafeCancelation is the globally callable form of Pool.SafeCancel: it drains
// and cancels all queued (not yet started) tasks on the global pool, reporting
// each via its OnError(ErrCanceled) hook. In-flight tasks are never touched.
// Safe to call repeatedly and concurrently. Returns the number of canceled
// tasks, plus ErrNoGlobalPool when no global pool is registered.
func SafeCancelation() (int, error) {
	p := Global()
	if p == nil {
		return 0, ErrNoGlobalPool
	}
	return p.SafeCancel(), nil
}

// CancelByModel is the globally callable form of Pool.CancelByModel.
func CancelByModel(model string) (int, error) {
	p := Global()
	if p == nil {
		return 0, ErrNoGlobalPool
	}
	return p.CancelByModel(model), nil
}

// CancelByID is the globally callable form of Pool.CancelByID.
func CancelByID(id string) (bool, error) {
	p := Global()
	if p == nil {
		return false, ErrNoGlobalPool
	}
	return p.CancelByID(id), nil
}

// ShutdownGlobal performs a full graceful shutdown of the global pool:
// wait-before-shutdown bounded by ctx, then cancel any leftover queued tasks,
// then stop the pool and clear the registration.
func ShutdownGlobal(ctx context.Context) error {
	p := Global()
	if p == nil {
		return ErrNoGlobalPool
	}
	waitErr := p.WaitBeforeShutdown(ctx)
	if waitErr != nil {
		p.SafeCancel()
	}
	shutdownErr := p.Shutdown()

	globalMu.Lock()
	if globalPool == p {
		globalPool = nil
	}
	globalMu.Unlock()
	return errors.Join(waitErr, shutdownErr)
}
