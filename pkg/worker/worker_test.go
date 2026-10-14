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

	err := pool.Dispatch(context.Background(), "test_job", func(ctx context.Context) error {
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
	_ = pool.Dispatch(context.Background(), "failing_job", func(ctx context.Context) error {
		return errors.New("simulated error")
	})

	time.Sleep(100 * time.Millisecond)
	stats = pool.Stats()
	if stats.Failed != 1 {
		t.Fatalf("expected stats.Failed=1, got %d", stats.Failed)
	}
}

func TestWorkerPoolDisabled(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Enabled = false

	pool := NewPool(cfg)
	pool.Start()

	var syncExecuted int32
	err := pool.Dispatch(context.Background(), "sync_job", func(ctx context.Context) error {
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
