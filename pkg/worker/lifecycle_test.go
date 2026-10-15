package worker

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func TestRetrySucceedsEventually(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Concurrency = 2
	pool := NewPool(cfg)
	pool.Start()
	defer func() { _ = pool.Shutdown() }()

	var attempts int32
	err := pool.Dispatch(context.Background(), "flaky", func(ctx context.Context) error {
		if atomic.AddInt32(&attempts, 1) < 3 {
			return errors.New("transient")
		}
		return nil
	}, WithRetry(5, time.Millisecond))
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := pool.WaitIdle(ctx); err != nil {
		t.Fatalf("wait idle: %v", err)
	}

	st := pool.Stats()
	if st.Completed != 1 {
		t.Fatalf("expected Completed=1, got %d", st.Completed)
	}
	if st.Retries < 2 {
		t.Fatalf("expected >=2 retries, got %d", st.Retries)
	}
}

func TestFallbackRecoversTask(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Concurrency = 2
	pool := NewPool(cfg)
	pool.Start()
	defer func() { _ = pool.Shutdown() }()

	var success, fallbackHit int32
	err := pool.Dispatch(context.Background(), "always-fail", func(ctx context.Context) error {
		return errors.New("boom")
	},
		WithRetry(1, time.Millisecond),
		WithFallback(func(ctx context.Context, err error) error {
			atomic.AddInt32(&fallbackHit, 1)
			return nil // recover
		}),
		WithOnSuccess(func(ctx context.Context) { atomic.AddInt32(&success, 1) }),
	)
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = pool.WaitIdle(ctx)

	if atomic.LoadInt32(&fallbackHit) != 1 {
		t.Fatalf("expected fallback hit once, got %d", atomic.LoadInt32(&fallbackHit))
	}
	if atomic.LoadInt32(&success) != 1 {
		t.Fatalf("expected OnSuccess once (via fallback), got %d", atomic.LoadInt32(&success))
	}
	if pool.Stats().Completed != 1 {
		t.Fatalf("fallback-recovered task should count as completed")
	}
}

func TestOnErrorCalledOnFinalFailure(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Concurrency = 2
	pool := NewPool(cfg)
	pool.Start()
	defer func() { _ = pool.Shutdown() }()

	var onErr int32
	_ = pool.Dispatch(context.Background(), "fail", func(ctx context.Context) error {
		return errors.New("nope")
	}, WithOnError(func(ctx context.Context, err error) {
		if err != nil {
			atomic.AddInt32(&onErr, 1)
		}
	}))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = pool.WaitIdle(ctx)

	if atomic.LoadInt32(&onErr) != 1 {
		t.Fatalf("expected OnError once, got %d", atomic.LoadInt32(&onErr))
	}
	if pool.Stats().Failed != 1 {
		t.Fatalf("expected Failed=1, got %d", pool.Stats().Failed)
	}
}

func TestPanicRecoveryKeepsPoolAlive(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Concurrency = 2
	pool := NewPool(cfg)
	pool.Start()
	defer func() { _ = pool.Shutdown() }()

	_ = pool.Dispatch(context.Background(), "panicker", func(ctx context.Context) error {
		panic("explode")
	})

	// Pool must survive and still process work.
	var ok int32
	_ = pool.Dispatch(context.Background(), "ok", func(ctx context.Context) error {
		atomic.AddInt32(&ok, 1)
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = pool.WaitIdle(ctx)

	st := pool.Stats()
	if st.Failed < 1 {
		t.Fatalf("panicking task should be counted as failed, got %d", st.Failed)
	}
	if atomic.LoadInt32(&ok) != 1 {
		t.Fatalf("pool should still process tasks after a panic")
	}
}

func TestMemoryAdmissionRejectsWhenFull(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Concurrency = 1
	cfg.EnforceMemory = true
	cfg.MaxMemoryBytes = 1 // 1 byte -> always over threshold
	cfg.MemoryThreshold = 0.9

	pool := NewPool(cfg)
	pool.Start()
	defer func() { _ = pool.Shutdown() }()

	err := pool.Dispatch(context.Background(), "big", func(ctx context.Context) error { return nil })
	if !errors.Is(err, ErrMemoryFull) {
		t.Fatalf("expected ErrMemoryFull, got %v", err)
	}
	if st := pool.Stats(); !st.MemoryPressure {
		t.Fatalf("expected MemoryPressure=true")
	}
}

func TestWaitBeforeShutdownAndDraining(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Concurrency = 4
	cfg.QueueSize = 64
	pool := NewPool(cfg)
	pool.Start()
	defer func() { _ = pool.Shutdown() }()

	var done int32
	for i := 0; i < 5; i++ {
		if err := pool.Dispatch(context.Background(), fmt.Sprintf("t%d", i), func(ctx context.Context) error {
			atomic.AddInt32(&done, 1)
			return nil
		}); err != nil {
			t.Fatalf("dispatch %d: %v", i, err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := pool.WaitBeforeShutdown(ctx); err != nil {
		t.Fatalf("WaitBeforeShutdown: %v", err)
	}
	if atomic.LoadInt32(&done) != 5 {
		t.Fatalf("expected 5 tasks done, got %d", atomic.LoadInt32(&done))
	}
	if !pool.Draining() {
		t.Fatalf("pool should be draining after WaitBeforeShutdown")
	}
	if err := pool.Dispatch(context.Background(), "after", func(ctx context.Context) error { return nil }); !errors.Is(err, ErrDraining) {
		t.Fatalf("expected ErrDraining after drain, got %v", err)
	}
}

func TestSafeCancelQueuedTasks(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Concurrency = 1
	cfg.QueueSize = 16
	pool := NewPool(cfg)
	pool.Start()
	defer func() { _ = pool.Shutdown() }()

	release := make(chan struct{})
	started := make(chan struct{})
	_ = pool.Dispatch(context.Background(), "blocker", func(ctx context.Context) error {
		close(started)
		<-release
		return nil
	})
	<-started // single worker is now busy

	var canceled int32
	for i := 0; i < 3; i++ {
		_ = pool.Dispatch(context.Background(), fmt.Sprintf("queued%d", i),
			func(ctx context.Context) error { return nil },
			WithOnError(func(ctx context.Context, err error) {
				if errors.Is(err, ErrCanceled) {
					atomic.AddInt32(&canceled, 1)
				}
			}),
		)
	}
	if pool.Queued() != 3 {
		t.Fatalf("expected 3 queued, got %d", pool.Queued())
	}

	n := pool.SafeCancel()
	close(release)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = pool.WaitIdle(ctx)

	if n != 3 {
		t.Fatalf("expected 3 canceled, got %d", n)
	}
	if atomic.LoadInt32(&canceled) != 3 {
		t.Fatalf("expected 3 OnError(ErrCanceled), got %d", atomic.LoadInt32(&canceled))
	}
	// Canceled tasks are tracked separately from execution failures.
	if st := pool.Stats(); st.Canceled != 3 {
		t.Fatalf("expected Stats().Canceled=3, got %d", st.Canceled)
	}
}

func TestCancelByModelAndID(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Concurrency = 1
	cfg.QueueSize = 16
	pool := NewPool(cfg)
	pool.Start()
	defer func() { _ = pool.Shutdown() }()

	release := make(chan struct{})
	started := make(chan struct{})
	_ = pool.Dispatch(context.Background(), "blocker", func(ctx context.Context) error {
		close(started)
		<-release
		return nil
	})
	<-started

	var modelACanceled int32
	var modelBExecuted int32
	var taskCExecuted int32

	// Queue 2 tasks for tenant/model "modelA"
	for i := 0; i < 2; i++ {
		_ = pool.Dispatch(context.Background(), fmt.Sprintf("jobA_%d", i),
			func(ctx context.Context) error { return nil },
			WithModel("modelA"),
			WithOnError(func(ctx context.Context, err error) {
				if errors.Is(err, ErrCanceled) {
					atomic.AddInt32(&modelACanceled, 1)
				}
			}),
		)
	}

	// Queue 1 task for "modelB"
	_ = pool.Dispatch(context.Background(), "jobB_1",
		func(ctx context.Context) error {
			atomic.AddInt32(&modelBExecuted, 1)
			return nil
		},
		WithModel("modelB"),
	)

	// Queue 1 task with specific ID "target-task-id"
	_ = pool.Dispatch(context.Background(), "jobC_specific",
		func(ctx context.Context) error {
			atomic.AddInt32(&taskCExecuted, 1)
			return nil
		},
		WithID("target-task-id"),
		WithModel("modelC"),
	)

	if pool.Queued() != 4 {
		t.Fatalf("expected 4 queued tasks, got %d", pool.Queued())
	}

	// Cancel only modelA tasks
	nA := pool.CancelByModel("modelA")
	if nA != 2 {
		t.Fatalf("expected 2 tasks canceled for modelA, got %d", nA)
	}
	if atomic.LoadInt32(&modelACanceled) != 2 {
		t.Fatalf("expected 2 OnError(ErrCanceled) for modelA, got %d", atomic.LoadInt32(&modelACanceled))
	}

	// Unblock worker and let remaining modelB and modelC tasks execute
	close(release)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = pool.WaitIdle(ctx)

	if atomic.LoadInt32(&modelBExecuted) != 1 {
		t.Fatalf("expected modelB job to execute successfully, got %d", atomic.LoadInt32(&modelBExecuted))
	}
	if atomic.LoadInt32(&taskCExecuted) != 1 {
		t.Fatalf("expected modelC job to execute successfully, got %d", atomic.LoadInt32(&taskCExecuted))
	}
}

func TestResumeAfterWaitBeforeShutdown(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Concurrency = 2
	pool := NewPool(cfg)
	pool.Start()
	defer func() { _ = pool.Shutdown() }()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := pool.WaitBeforeShutdown(ctx); err != nil {
		t.Fatalf("WaitBeforeShutdown: %v", err)
	}
	if err := pool.Dispatch(context.Background(), "blocked", func(ctx context.Context) error { return nil }); !errors.Is(err, ErrDraining) {
		t.Fatalf("expected ErrDraining while draining, got %v", err)
	}

	// Resume must re-open intake (the pool must not stay bricked forever).
	pool.Resume()
	if pool.Draining() {
		t.Fatalf("pool should not be draining after Resume")
	}
	var ran int32
	if err := pool.Dispatch(context.Background(), "after-resume", func(ctx context.Context) error {
		atomic.AddInt32(&ran, 1)
		return nil
	}); err != nil {
		t.Fatalf("dispatch after Resume: %v", err)
	}
	_ = pool.WaitIdle(ctx)
	if atomic.LoadInt32(&ran) != 1 {
		t.Fatalf("expected task to run after Resume, ran=%d", atomic.LoadInt32(&ran))
	}
}

func TestRestartAfterCleanShutdown(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Concurrency = 2
	pool := NewPool(cfg)
	pool.Start()

	if err := pool.Shutdown(); err != nil {
		t.Fatalf("clean shutdown: %v", err)
	}

	// A clean shutdown must be restartable.
	pool.Start()
	var ran int32
	if err := pool.Dispatch(context.Background(), "second-life", func(ctx context.Context) error {
		atomic.AddInt32(&ran, 1)
		return nil
	}); err != nil {
		t.Fatalf("dispatch after restart: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = pool.WaitIdle(ctx)
	if atomic.LoadInt32(&ran) != 1 {
		t.Fatalf("expected task to run after restart, ran=%d", atomic.LoadInt32(&ran))
	}
	if err := pool.Shutdown(); err != nil {
		t.Fatalf("second shutdown: %v", err)
	}
}

func TestShutdownRespectsTimeout(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Concurrency = 1
	cfg.ShutdownTimeout = 100 * time.Millisecond
	pool := NewPool(cfg)
	pool.Start()

	started := make(chan struct{})
	release := make(chan struct{})
	_ = pool.Dispatch(context.Background(), "stuck", func(ctx context.Context) error {
		close(started)
		<-release // ignores ctx entirely
		return nil
	})
	<-started

	t0 := time.Now()
	err := pool.Shutdown()
	elapsed := time.Since(t0)
	close(release) // let the leaked goroutine finish

	if !errors.Is(err, ErrShutdownTimeout) {
		t.Fatalf("expected ErrShutdownTimeout, got %v", err)
	}
	// Must return in bounded time (2 * ShutdownTimeout), never block on the task.
	if elapsed > time.Second {
		t.Fatalf("Shutdown blocked too long: %v (should be ~2*ShutdownTimeout)", elapsed)
	}

	// Restart after a forced shutdown must be refused (no overlapping workers).
	pool.Start()
	if err := pool.Dispatch(context.Background(), "post-force", func(ctx context.Context) error { return nil }); !errors.Is(err, ErrDraining) {
		t.Fatalf("expected pool to stay stopped after forced shutdown, got %v", err)
	}
}

func TestMemoryUsageMetric(t *testing.T) {
	usage, source := processMemoryUsage()
	if usage == 0 {
		t.Fatalf("expected non-zero memory usage, got 0 (source=%s)", source)
	}
	if source == "" {
		t.Fatalf("expected a non-empty usage source label")
	}

	cfg := DefaultConfig()
	cfg.EnforceMemory = true
	cfg.MaxMemoryBytes = 1 << 62 // huge -> never over threshold
	pool := NewPool(cfg)
	if err := pool.checkMemory(); err != nil {
		t.Fatalf("expected no pressure with a huge limit, got %v", err)
	}
	if st := pool.Stats(); st.UsageBytes == 0 || st.UsageSource == "" {
		t.Fatalf("expected Stats to report usage + source, got %+v", st)
	}
}

func TestMemoryThresholdAllowsOne(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MemoryThreshold = 1.0 // must be accepted verbatim (W7)
	pool := NewPool(cfg)
	if pool.cfg.MemoryThreshold != 1.0 {
		t.Fatalf("expected MemoryThreshold=1.0 preserved, got %v", pool.cfg.MemoryThreshold)
	}
}

func TestGlobalRegistryFunctions(t *testing.T) {
	// Clean slate.
	SetGlobal(nil)
	if _, err := SafeCancelation(); !errors.Is(err, ErrNoGlobalPool) {
		t.Fatalf("expected ErrNoGlobalPool, got %v", err)
	}
	if err := WaitBeforeShutdown(context.Background()); !errors.Is(err, ErrNoGlobalPool) {
		t.Fatalf("expected ErrNoGlobalPool, got %v", err)
	}

	cfg := DefaultConfig()
	cfg.Concurrency = 2
	p := InitGlobal(cfg)
	if Global() != p {
		t.Fatalf("Global() should return the initialized pool")
	}

	if _, err := SafeCancelation(); err != nil {
		t.Fatalf("SafeCancelation on global: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := ShutdownGlobal(ctx); err != nil {
		t.Fatalf("ShutdownGlobal: %v", err)
	}
	if Global() != nil {
		t.Fatalf("Global() should be nil after ShutdownGlobal")
	}
}
