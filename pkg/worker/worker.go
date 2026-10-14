package worker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// ErrQueueFull is returned when the worker pool queue is full
var ErrQueueFull = errors.New("worker task queue is full")

// Config holds configuration parameters for the background worker pool
type Config struct {
	Enabled         bool          `json:"enabled"`
	Concurrency     int           `json:"concurrency"`
	QueueSize       int           `json:"queue_size"`
	ShutdownTimeout time.Duration `json:"shutdown_timeout"`
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
	}
}

// PoolStats holds real-time telemetry metrics for the worker pool
type PoolStats struct {
	Enabled     bool  `json:"enabled"`
	Running     bool  `json:"running"`
	Concurrency int   `json:"concurrency"`
	Dispatched  int64 `json:"dispatched"`
	Completed   int64 `json:"completed"`
	Failed      int64 `json:"failed"`
}

type jobItem struct {
	name string
	fn   func(ctx context.Context) error
	ctx  context.Context
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
	running    atomic.Bool
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
	return &Pool{
		cfg:    cfg,
		taskCh: make(chan jobItem, cfg.QueueSize),
		quitCh: make(chan struct{}),
	}
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
			t0 := time.Now()
			err := job.fn(job.ctx)
			if err != nil {
				p.failed.Add(1)
				log.Printf("[WORKER] Task %q GAGAL: %v (durasi: %v)", job.name, err, time.Since(t0))
			} else {
				p.completed.Add(1)
				log.Printf("[WORKER] Task %q SUKSES (durasi: %v)", job.name, time.Since(t0))
			}
		}
	}
}

// Dispatch submits an async task to the worker pool. If pool is disabled, runs synchronously.
// If queue is full, returns ErrQueueFull immediately without blocking.
func (p *Pool) Dispatch(ctx context.Context, name string, fn func(ctx context.Context) error) error {
	if !p.cfg.Enabled || !p.running.Load() {
		return fn(ctx)
	}

	p.dispatched.Add(1)

	select {
	case p.taskCh <- jobItem{name: name, fn: fn, ctx: ctx}:
		return nil
	case <-ctx.Done():
		p.failed.Add(1)
		return ctx.Err()
	default:
		p.failed.Add(1)
		return fmt.Errorf("failed to dispatch task %q: %w", name, ErrQueueFull)
	}
}

// Stats returns a telemetry snapshot of the worker pool
func (p *Pool) Stats() PoolStats {
	return PoolStats{
		Enabled:     p.cfg.Enabled,
		Running:     p.running.Load(),
		Concurrency: p.cfg.Concurrency,
		Dispatched:  p.dispatched.Load(),
		Completed:   p.completed.Load(),
		Failed:      p.failed.Load(),
	}
}

// Shutdown gracefully waits for in-flight tasks to finish within configured timeout
func (p *Pool) Shutdown() error {
	if p.running.CompareAndSwap(true, false) {
		log.Println("[WORKER] Menghentikan worker pool...")
		close(p.quitCh)

		done := make(chan struct{})
		go func() {
			p.wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			log.Println("[WORKER] Worker pool berhasil dihentikan secara bersih.")
		case <-time.After(p.cfg.ShutdownTimeout):
			log.Println("[WORKER] Shutdown timeout, worker pool dipaksa berhenti.")
		}
	}
	return nil
}
