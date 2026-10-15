package worker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"runtime"
	"runtime/debug"
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

// Config holds configuration parameters for the background worker pool
type Config struct {
	Enabled         bool          `json:"enabled"`
	Concurrency     int           `json:"concurrency"`
	QueueSize       int           `json:"queue_size"`
	ShutdownTimeout time.Duration `json:"shutdown_timeout"`

	// Memory-aware admission control
	EnforceMemory   bool    `json:"enforce_memory"`
	MaxMemoryBytes  int64   `json:"max_memory_bytes"` // 0 = use current GOMEMLIMIT
	MemoryThreshold float64 `json:"memory_threshold"` // 0-1, default 0.90
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
		MemoryThreshold: 0.90,
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
	Retries        int64  `json:"retries"`
	InFlight       int64  `json:"in_flight"`
	Queued         int    `json:"queued"`
	Draining       bool   `json:"draining"`
	MemoryPressure bool   `json:"memory_pressure"`
	HeapAllocBytes uint64 `json:"heap_alloc_bytes"`
	HeapAllocHuman string `json:"heap_alloc_human"`
	LimitBytes     int64  `json:"limit_bytes"`
	LimitHuman     string `json:"limit_human"`
}

// Task represents a job to be executed with optional retry/fallback/hooks
type Task struct {
	Name string

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

type jobItem struct {
	task Task
}

// Pool manages concurrent background job execution using pure Go standard library channels
type Pool struct {
	cfg        Config
	taskCh     chan jobItem
	quitCh     chan struct{}
	wg         sync.WaitGroup
	dispatched atomic.Int64
	completed  atomic.Int64
	failed     atomic.Int64
	retries    atomic.Int64
	inflight   atomic.Int64
	pending    atomic.Int64
	running    atomic.Bool
	draining   atomic.Bool
	taskOnce   sync.Once
	quitOnce   sync.Once
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
	if cfg.MemoryThreshold <= 0 || cfg.MemoryThreshold >= 1 {
		cfg.MemoryThreshold = 0.90
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
	units := []string{"KB", "MB", "GB", "TB", "PB"}
	if exp >= len(units) {
		exp = len(units) - 1
	}
	return fmt.Sprintf("%.2f %s", float64(b)/float64(div), units[exp])
}

func (p *Pool) checkMemory() error {
	if !p.cfg.EnforceMemory {
		return nil
	}
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	limit := p.cfg.MaxMemoryBytes
	if limit <= 0 {
		limit = debug.SetMemoryLimit(-1)
	}
	if limit <= 0 {
		return nil // unlimited
	}

	thresh := float64(limit) * p.cfg.MemoryThreshold
	if int64(m.HeapAlloc) >= int64(thresh) {
		return ErrMemoryFull
	}
	return nil
}

// Start boots worker goroutines
func (p *Pool) Start() {
	if !p.cfg.Enabled {
		log.Println("[WORKER] Worker pool dinonaktifkan oleh konfigurasi.")
		return
	}
	if p.running.CompareAndSwap(false, true) {
		for i := 0; i < p.cfg.Concurrency; i++ {
			p.wg.Add(1)
			go p.workerLoop()
		}
		log.Printf("[WORKER] Worker pool aktif dengan %d goroutine pekerja (antrean: %d)",
			p.cfg.Concurrency, p.cfg.QueueSize)
	}
}

func (p *Pool) workerLoop() {
	defer p.wg.Done()
	for {
		select {
		case <-p.quitCh:
			return
		case job, ok := <-p.taskCh:
			if !ok {
				return
			}
			p.inflight.Add(1)
			p.executeJob(job.task)
			p.inflight.Add(-1)
			p.pending.Add(-1)
		}
	}
}

func (p *Pool) executeJob(t Task) {
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
				case <-p.quitCh:
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
	return p.DispatchTask(task)
}

// DispatchTask submits a Task to the worker pool.
func (p *Pool) DispatchTask(t Task) error {
	if t.Fn == nil {
		return errors.New("task function is nil")
	}
	if p.draining.Load() {
		p.failed.Add(1)
		return fmt.Errorf("failed to dispatch task %q: %w", t.Name, ErrDraining)
	}
	if !p.cfg.Enabled || !p.running.Load() {
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

	pressure := false
	if limit > 0 && int64(m.HeapAlloc) >= int64(thresh) {
		pressure = true
	}

	return PoolStats{
		Enabled:        p.cfg.Enabled,
		Running:        p.running.Load(),
		Concurrency:    p.cfg.Concurrency,
		Dispatched:     p.dispatched.Load(),
		Completed:      p.completed.Load(),
		Failed:         p.failed.Load(),
		Retries:        p.retries.Load(),
		InFlight:       p.inflight.Load(),
		Queued:         len(p.taskCh),
		Draining:       p.draining.Load(),
		MemoryPressure: pressure,
		HeapAllocBytes: m.HeapAlloc,
		HeapAllocHuman: fmtBytes(int64(m.HeapAlloc)),
		LimitBytes:     limit,
		LimitHuman:     fmtBytes(limit),
	}
}

// InFlight returns the number of tasks currently being executed.
func (p *Pool) InFlight() int64 { return p.inflight.Load() }

// Queued returns the number of tasks waiting in the queue.
func (p *Pool) Queued() int { return len(p.taskCh) }

// Draining reports whether the pool has stopped accepting new tasks.
func (p *Pool) Draining() bool { return p.draining.Load() }

// BeginDrain stops accepting new tasks (DispatchTask returns ErrDraining) without
// touching the queue or running workers. Use before WaitIdle for a graceful stop.
func (p *Pool) BeginDrain() {
	p.draining.Store(true)
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
	canceled := 0
	for {
		select {
		case job, ok := <-p.taskCh:
			if !ok {
				return canceled
			}
			canceled++
			p.failed.Add(1)
			p.pending.Add(-1)
			p.reportCanceled(job.task)
		default:
			return canceled
		}
	}
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

// Shutdown gracefully drains the queue and stops the pool within ShutdownTimeout.
// It first stops accepting new work, then closes the task channel so workers can
// drain remaining queued jobs. If drain does not finish in time, it force-signals
// workers via quitCh.
func (p *Pool) Shutdown() error {
	if p.running.CompareAndSwap(true, false) {
		log.Println("[WORKER] Menghentikan worker pool...")
		p.draining.Store(true)

		// Close the task channel: workers finish buffered jobs, then exit on !ok.
		p.taskOnce.Do(func() { close(p.taskCh) })

		done := make(chan struct{})
		go func() {
			p.wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			log.Println("[WORKER] Worker pool berhasil dihentikan secara bersih (antrean ter-drain).")
		case <-time.After(p.cfg.ShutdownTimeout):
			// Force workers out (retry backoff / long tasks).
			p.quitOnce.Do(func() { close(p.quitCh) })
			<-done
			log.Println("[WORKER] Shutdown timeout, worker pool dipaksa berhenti.")
		}
	}
	return nil
}

// WaitBeforeShutdown stops accepting new tasks and blocks until all queued and
// in-flight tasks complete, bounded by ctx. If ctx expires, remaining QUEUED
// tasks are safely canceled (in-flight work is never killed). It does not stop
// the pool; call Shutdown afterward.
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
)

// InitGlobal creates and starts a process-wide worker pool, replacing any
// previously registered global pool (which is shut down first). Safe to call
// multiple times; returns the new pool.
func InitGlobal(cfg Config) *Pool {
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

// ShutdownGlobal performs a full graceful shutdown of the global pool:
// wait-before-shutdown bounded by ctx, then cancel any leftover queued tasks,
// then stop the pool and clear the registration.
func ShutdownGlobal(ctx context.Context) error {
	p := Global()
	if p == nil {
		return ErrNoGlobalPool
	}
	_ = p.WaitBeforeShutdown(ctx)
	p.SafeCancel()
	_ = p.Shutdown()

	globalMu.Lock()
	if globalPool == p {
		globalPool = nil
	}
	globalMu.Unlock()
	return nil
}
