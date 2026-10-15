package worker

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// L3: Dispatch racing with Shutdown must never panic with "send on closed
// channel"; it must either enqueue or return an error.
func TestDispatchConcurrentWithShutdownNeverPanics(t *testing.T) {
	var panics atomic.Int64

	for round := 0; round < 300; round++ {
		cfg := DefaultConfig()
		cfg.Concurrency = 2
		cfg.QueueSize = 8
		cfg.ShutdownTimeout = 200 * time.Millisecond
		p := NewPool(cfg)
		p.Start()

		stop := make(chan struct{})
		var wg sync.WaitGroup
		for g := 0; g < 4; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() {
					if r := recover(); r != nil {
						panics.Add(1)
					}
				}()
				for {
					select {
					case <-stop:
						return
					default:
						_ = p.Dispatch(context.Background(), "t", func(context.Context) error { return nil })
					}
				}
			}()
		}

		time.Sleep(time.Millisecond)
		_ = p.Shutdown()
		close(stop)
		wg.Wait()
	}

	if n := panics.Load(); n != 0 {
		t.Fatalf("Dispatch panicked %d time(s) racing with Shutdown", n)
	}
}
