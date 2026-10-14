package queue

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rachmanzz/anchordb/pkg/worker"
)

func TestQueueManagerMemory(t *testing.T) {
	// Setup worker pool as dispatcher
	wCfg := worker.DefaultConfig()
	wCfg.Concurrency = 2
	wPool := worker.NewPool(wCfg)
	wPool.Start()
	defer func() { _ = wPool.Shutdown() }()

	cfg := DefaultConfig()
	cfg.PollInterval = 10 * time.Millisecond
	cfg.BatchSize = 5
	cfg.MaxSize = 100

	qm, err := NewQueueManager(cfg, wPool.Dispatcher())
	if err != nil {
		t.Fatalf("NewQueueManager failed: %v", err)
	}
	defer func() { _ = qm.Close() }()

	var receivedCount int32
	var mu sync.Mutex
	var lastReceivedModel string

	qm.RegisterHandler(TopicRecordSaved, func(ctx context.Context, payload []byte) error {
		var evt RecordSavedEvent
		if err := json.Unmarshal(payload, &evt); err != nil {
			return err
		}
		atomic.AddInt32(&receivedCount, 1)
		mu.Lock()
		lastReceivedModel = evt.Model
		mu.Unlock()
		return nil
	})

	qm.Start()

	// Publish test events
	err = qm.Publish(context.Background(), TopicRecordSaved, RecordSavedEvent{
		Model:     "test_model",
		ID:        42,
		Count:     1,
		Timestamp: time.Now(),
	})
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	// Wait for consumer to process message
	time.Sleep(150 * time.Millisecond)

	if atomic.LoadInt32(&receivedCount) != 1 {
		t.Fatalf("expected receivedCount=1, got %d", atomic.LoadInt32(&receivedCount))
	}
	mu.Lock()
	model := lastReceivedModel
	mu.Unlock()
	if model != "test_model" {
		t.Fatalf("expected lastReceivedModel='test_model', got %q", model)
	}

	stats := qm.Stats()
	if stats.Published != 1 {
		t.Fatalf("expected published=1, got %d", stats.Published)
	}
	if stats.Processed != 1 {
		t.Fatalf("expected processed=1, got %d", stats.Processed)
	}
}

func TestQueueManagerBoundedCapacity(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ConsumerEnabled = false // No consumer pulling, queue will fill up
	cfg.MaxSize = 3

	qm, err := NewQueueManager(cfg, nil)
	if err != nil {
		t.Fatalf("NewQueueManager failed: %v", err)
	}
	defer func() { _ = qm.Close() }()

	// Publish up to capacity limit (3 items)
	for i := 0; i < 3; i++ {
		if err := qm.Publish(context.Background(), TopicRecordSaved, "item"); err != nil {
			t.Fatalf("unexpected error at item %d: %v", i, err)
		}
	}

	// 4th item should hit bounded limit and return ErrQueueFull
	err = qm.Publish(context.Background(), TopicRecordSaved, "item_overflow")
	if err != ErrQueueFull {
		t.Fatalf("expected ErrQueueFull, got %v", err)
	}

	stats := qm.Stats()
	if stats.Capacity != 3 {
		t.Fatalf("expected capacity 3, got %d", stats.Capacity)
	}
	if stats.Pending != 3 {
		t.Fatalf("expected pending 3, got %d", stats.Pending)
	}
	if stats.Dropped != 1 {
		t.Fatalf("expected dropped 1, got %d", stats.Dropped)
	}
}

func TestQueueManagerDisabled(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Enabled = false

	qm, err := NewQueueManager(cfg, nil)
	if err != nil {
		t.Fatalf("NewQueueManager failed: %v", err)
	}

	// Should not error when disabled
	err = qm.Publish(context.Background(), TopicRecordSaved, "some_data")
	if err != nil {
		t.Fatalf("expected nil error on disabled publish, got %v", err)
	}

	stats := qm.Stats()
	if stats.Enabled {
		t.Fatalf("expected stats.Enabled=false")
	}

	_ = qm.Close()
}
