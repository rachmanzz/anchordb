package worker

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestWorkerPool(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Concurrency = 4
	cfg.QueueSize = 50

	pool := NewPool(cfg)
	pool.Start()
	defer func() { _ = pool.Shutdown() }()

	var executed int32

	err := pool.Dispatch(context.Background(), "test_job", func(_ context.Context) error {
		atomic.AddInt32(&executed, 1)
		return nil
	})
	if err != nil {
		t.Fatalf("Dispatch failed: %v", err)
	}

	// Wait for execution
	time.Sleep(100 * time.Millisecond)

	if atomic.LoadInt32(&executed) != 1 {
		t.Fatalf("expected executed=1, got %d", atomic.LoadInt32(&executed))
	}

	stats := pool.Stats()
	if stats.Completed != 1 {
		t.Fatalf("expected stats.Completed=1, got %d", stats.Completed)
	}

	// Test failing job
	_ = pool.Dispatch(context.Background(), "failing_job", func(_ context.Context) error {
		return errors.New("simulated error")
	})

	time.Sleep(100 * time.Millisecond)
	stats = pool.Stats()
	if stats.Failed != 1 {
		t.Fatalf("expected stats.Failed=1, got %d", stats.Failed)
	}
}

func TestFmtBytesFormatsExabyteRange(t *testing.T) {
	// 2^63-1 bytes is ~8 EiB; the unit must not be capped at PB while div keeps growing.
	got := fmtBytes(1<<63 - 1)
	if got == "" {
		t.Fatal("expected a formatted value")
	}
	if want := "8.00 EB"; got != want {
		t.Fatalf("fmtBytes(max int64) = %q, want %q", got, want)
	}
}

func TestWorkerPoolDisabled(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Enabled = false

	pool := NewPool(cfg)
	pool.Start()

	var syncExecuted int32
	err := pool.Dispatch(context.Background(), "sync_job", func(_ context.Context) error {
		atomic.AddInt32(&syncExecuted, 1)
		return nil
	})
	if !errors.Is(err, ErrWorkerUnavailable) {
		t.Fatalf("expected ErrWorkerUnavailable on disabled pool, got %v", err)
	}
	if atomic.LoadInt32(&syncExecuted) != 0 {
		t.Fatalf("expected no silent sync execution when disabled, got %d", atomic.LoadInt32(&syncExecuted))
	}

	_ = pool.Shutdown()
}

func TestWorkerPool_FallbackCounterAndCanceledDispatch(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Concurrency = 1
	cfg.QueueSize = 8
	pool := NewPool(cfg)
	pool.Start()
	defer func() { _ = pool.Shutdown() }()

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	ran := false
	err := pool.Dispatch(canceledCtx, "canceled_job", func(_ context.Context) error {
		ran = true
		return nil
	})
	if !errors.Is(err, context.Canceled) || ran {
		t.Fatalf("expected Dispatch on canceled ctx to fail before execution (ran=%v, err=%v)", ran, err)
	}

	var fallbackHookCalled int32
	if err := pool.Dispatch(context.Background(), "fallback_job", func(_ context.Context) error {
		return errors.New("primary failed")
	}, WithFallback(func(_ context.Context, _ error) error {
		return nil
	}), WithOnFallback(func(_ context.Context, primaryErr error) {
		if primaryErr != nil {
			atomic.AddInt32(&fallbackHookCalled, 1)
		}
	})); err != nil {
		t.Fatalf("expected dispatch nil, got %v", err)
	}

	if err := pool.WaitIdle(context.Background()); err != nil {
		t.Fatalf("WaitIdle: %v", err)
	}

	stats := pool.Stats()
	if stats.FallbackSuccesses != 1 || stats.Failed != 1 || stats.Completed != 0 {
		t.Fatalf("expected FallbackSuccesses=1 Failed=1 Completed=0, got %+v", stats)
	}
	if atomic.LoadInt32(&fallbackHookCalled) != 1 {
		t.Fatalf("expected OnFallback called once, got %d", atomic.LoadInt32(&fallbackHookCalled))
	}
}

func TestCancelMatching_PredicateCanAccessPoolWithoutDeadlock(t *testing.T) {
	cfg := DefaultConfig()
	if !cfg.EnforceMemory {
		t.Fatal("expected DefaultConfig().EnforceMemory to be true")
	}
	cfg.Concurrency = 1
	cfg.QueueSize = 8
	p := NewPool(cfg)
	p.Start()
	defer func() { _ = p.Shutdown() }()

	started := make(chan struct{})
	release := make(chan struct{})
	if err := p.Dispatch(context.Background(), "blocker", func(context.Context) error {
		close(started)
		<-release
		return nil
	}); err != nil {
		t.Fatalf("dispatch blocker: %v", err)
	}
	<-started

	if err := p.Dispatch(context.Background(), "target", func(context.Context) error { return nil }); err != nil {
		t.Fatalf("dispatch target: %v", err)
	}

	predDone := make(chan int, 1)
	go func() {
		predDone <- p.CancelMatching(func(task Task) bool {
			_ = p.Queued()
			if task.Name == "target" {
				_ = p.Dispatch(context.Background(), "reentrant", func(context.Context) error { return nil })
				return true
			}
			return false
		})
	}()

	select {
	case n := <-predDone:
		if n != 1 {
			t.Fatalf("expected 1 canceled task, got %d", n)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("CancelMatching deadlocked when predicate accessed Queued/Dispatch")
	}
	close(release)
	_ = p.WaitIdle(context.Background())
}
