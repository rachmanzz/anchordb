package worker

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"sync/atomic"
	"time"

	gsuite_worker "github.com/rachmanzz/gosuite/v3/worker"
)

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

// Pool manages background job execution powered by github.com/rachmanzz/gosuite/v3/worker
type Pool struct {
	cfg        Config
	engine     *gsuite_worker.Engine
	dispatched int64
	completed  int64
	failed     int64
	running    atomic.Bool
}

// NewPool initializes a worker pool instance
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

	wCfg := gsuite_worker.DefaultConfig()
	wCfg.MaxWorkers = cfg.Concurrency
	wCfg.TaskQueueSize = cfg.QueueSize
	wCfg.ShutdownTimeout = cfg.ShutdownTimeout

	engine := gsuite_worker.NewEngine(wCfg)

	return &Pool{
		cfg:    cfg,
		engine: engine,
	}
}

// Start boots the worker engine workers
func (p *Pool) Start() {
	if !p.cfg.Enabled {
		log.Println("[WORKER] Worker pool dinonaktifkan oleh konfigurasi.")
		return
	}
	if p.running.CompareAndSwap(false, true) {
		p.engine.Start()
		log.Printf("[WORKER] Worker pool aktif dengan %d goroutine pekerja (antrean: %d)",
			p.cfg.Concurrency, p.cfg.QueueSize)
	}
}

// Dispatch submits an async task to the worker pool
func (p *Pool) Dispatch(ctx context.Context, name string, fn func(ctx context.Context) error) error {
	if !p.cfg.Enabled || !p.running.Load() {
		// If worker is disabled or stopped, run synchronously in current context
		return fn(ctx)
	}

	atomic.AddInt64(&p.dispatched, 1)

	task := gsuite_worker.NewTaskFunc(name, func(taskCtx context.Context, pool gsuite_worker.SubWorkerPool) error {
		t0 := time.Now()
		err := fn(taskCtx)
		if err != nil {
			atomic.AddInt64(&p.failed, 1)
			log.Printf("[WORKER] Task %q GAGAL: %v (durasi: %v)", name, err, time.Since(t0))
		} else {
			atomic.AddInt64(&p.completed, 1)
			log.Printf("[WORKER] Task %q SUKSES (durasi: %v)", name, time.Since(t0))
		}
		return err
	})

	if err := p.engine.Dispatch(ctx, task); err != nil {
		atomic.AddInt64(&p.failed, 1)
		return fmt.Errorf("failed to dispatch task %q to worker pool: %w", name, err)
	}

	return nil
}

// Dispatcher returns the underlying gosuite worker.Dispatcher interface
func (p *Pool) Dispatcher() gsuite_worker.Dispatcher {
	return p.engine
}

// Stats returns a telemetry snapshot of the worker pool
func (p *Pool) Stats() PoolStats {
	return PoolStats{
		Enabled:     p.cfg.Enabled,
		Running:     p.running.Load(),
		Concurrency: p.cfg.Concurrency,
		Dispatched:  atomic.LoadInt64(&p.dispatched),
		Completed:   atomic.LoadInt64(&p.completed),
		Failed:      atomic.LoadInt64(&p.failed),
	}
}

// Shutdown gracefully waits for in-flight tasks to finish
func (p *Pool) Shutdown() error {
	if p.running.CompareAndSwap(true, false) {
		log.Println("[WORKER] Menghentikan worker pool...")
		err := p.engine.Shutdown()
		log.Println("[WORKER] Worker pool berhasil dihentikan.")
		return err
	}
	return nil
}
