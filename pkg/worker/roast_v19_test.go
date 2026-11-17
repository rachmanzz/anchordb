package worker

import (
	"context"
	"errors"
	"math"
	"runtime/debug"
	"testing"
	"time"
)

func TestRoastV19_CheckMemoryUnsetLimitDoesNotOverflow(t *testing.T) {
	prev := debug.SetMemoryLimit(math.MaxInt64)
	defer debug.SetMemoryLimit(prev)

	pool := NewPool(Config{
		Enabled:         true,
		Concurrency:     1,
		QueueSize:       4,
		EnforceMemory:   true,
		MaxMemoryBytes:  0,
		MemoryThreshold: 1.0,
	})
	if err := pool.checkMemory(); err != nil {
		t.Fatalf("checkMemory with MaxInt64 GOMEMLIMIT and threshold 1.0 returned %v, want nil", err)
	}
}

func TestRoastV19_ShutdownTimeoutDrainsQueuedPending(t *testing.T) {
	pool := NewPool(Config{
		Enabled:         true,
		Concurrency:     1,
		QueueSize:       8,
		ShutdownTimeout: 40 * time.Millisecond,
	})
	pool.Start()

	started := make(chan struct{})
	release := make(chan struct{})
	if err := pool.DispatchAsync(context.Background(), "stuck", func(_ context.Context) error {
		close(started)
		<-release
		return nil
	}); err != nil {
		t.Fatalf("DispatchAsync stuck: %v", err)
	}
	<-started

	var canceledCount int
	for i := 0; i < 3; i++ {
		if err := pool.DispatchAsync(context.Background(), "queued", func(_ context.Context) error {
			return nil
		}, WithOnError(func(_ context.Context, err error) {
			if errors.Is(err, ErrCanceled) {
				canceledCount++
			}
		})); err != nil {
			t.Fatalf("DispatchAsync queued %d: %v", i, err)
		}
	}

	err := pool.Shutdown()
	close(release)
	if !errors.Is(err, ErrShutdownTimeout) {
		t.Fatalf("expected ErrShutdownTimeout, got %v", err)
	}
	if canceledCount == 0 {
		t.Fatalf("expected queued tasks in taskCh to receive OnError(ErrCanceled) on Shutdown timeout")
	}
	if stats := pool.Stats(); stats.Queued != 0 || stats.Canceled == 0 {
		t.Fatalf("expected queued tasks drained and canceled on Shutdown timeout, got stats=%+v", stats)
	}
}
