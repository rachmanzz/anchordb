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
	if err != nil {
		t.Fatalf("Dispatch on disabled pool failed: %v", err)
	}

	if atomic.LoadInt32(&syncExecuted) != 1 {
		t.Fatalf("expected sync execution when disabled, got %d", atomic.LoadInt32(&syncExecuted))
	}

	_ = pool.Shutdown()
}

func TestWorkerPool_FallbackCounterAndSyncCtxAdmission(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Enabled = false
	pool := NewPool(cfg)

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	ran := false
	err := pool.Dispatch(canceledCtx, "canceled_sync", func(_ context.Context) error {
		ran = true
		return nil
	})
	if !errors.Is(err, context.Canceled) || ran {
		t.Fatalf("expected Dispatch on canceled ctx to fail before sync execution (ran=%v, err=%v)", ran, err)
	}

	if err := pool.Dispatch(context.Background(), "fallback_sync", func(_ context.Context) error {
		return errors.New("primary failed")
	}, WithFallback(func(_ context.Context, _ error) error {
		return nil
	})); err != nil {
		t.Fatalf("expected sync fallback dispatch nil, got %v", err)
	}

	stats := pool.Stats()
	if stats.FallbackSuccesses != 1 || stats.Failed != 1 || stats.Completed != 1 {
		t.Fatalf("expected FallbackSuccesses=1 Failed=1 Completed=1, got %+v", stats)
	}
}
