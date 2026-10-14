package queue

import (
	"context"
	"encoding/json"
	"fmt"
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

func TestQueueManagerWaitingAndRejection(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ConsumerEnabled = false // No consumer pulling, queue will fill up
	cfg.MaxSize = 2
	cfg.EnqueueTimeout = 50 * time.Millisecond // Short timeout for rejection test

	qm, err := NewQueueManager(cfg, nil)
	if err != nil {
		t.Fatalf("NewQueueManager failed: %v", err)
	}
	defer func() { _ = qm.Close() }()

	// Publish up to capacity limit (2 items)
	for i := 0; i < 2; i++ {
		if err := qm.Publish(context.Background(), TopicRecordSaved, "item"); err != nil {
			t.Fatalf("unexpected error at item %d: %v", i, err)
		}
	}

	// 3rd item should wait for memory space and then be rejected after 50ms
	t0 := time.Now()
	err = qm.Publish(context.Background(), TopicRecordSaved, "item_overflow")
	elapsed := time.Since(t0)

	if err != ErrQueueFull {
		t.Fatalf("expected ErrQueueFull, got %v", err)
	}
	if elapsed < 40*time.Millisecond {
		t.Fatalf("expected publisher to wait before rejection, but returned in %v", elapsed)
	}

	stats := qm.Stats()
	if stats.Capacity != 2 {
		t.Fatalf("expected capacity 2, got %d", stats.Capacity)
	}
	if stats.Pending != 2 {
		t.Fatalf("expected pending 2, got %d", stats.Pending)
	}
	if stats.Rejected != 1 {
		t.Fatalf("expected rejected 1, got %d", stats.Rejected)
	}
}

func TestQueueManagerWaitingFlowIntoMemory(t *testing.T) {
	// Setup worker pool as dispatcher
	wCfg := worker.DefaultConfig()
	wCfg.Concurrency = 2
	wPool := worker.NewPool(wCfg)
	wPool.Start()
	defer func() { _ = wPool.Shutdown() }()

	cfg := DefaultConfig()
	cfg.PollInterval = 5 * time.Millisecond
	cfg.BatchSize = 1
	cfg.MaxSize = 1                           // Max 1 item in memory
	cfg.EnqueueTimeout = 500 * time.Millisecond // Wait up to 500ms

	qm, err := NewQueueManager(cfg, wPool.Dispatcher())
	if err != nil {
		t.Fatalf("NewQueueManager failed: %v", err)
	}
	defer func() { _ = qm.Close() }()

	var processedCount int32
	qm.RegisterHandler(TopicRecordSaved, func(ctx context.Context, payload []byte) error {
		time.Sleep(20 * time.Millisecond) // Simulate work
		atomic.AddInt32(&processedCount, 1)
		return nil
	})
	qm.Start()

	// Publish item 1
	if err := qm.Publish(context.Background(), TopicRecordSaved, "item_1"); err != nil {
		t.Fatalf("item 1 publish failed: %v", err)
	}

	// Item 2 should wait for item 1 to finish, and successfully enter memory once item 1 finishes!
	err = qm.Publish(context.Background(), TopicRecordSaved, "item_2")
	if err != nil {
		t.Fatalf("expected item 2 to wait and enter memory, got error: %v", err)
	}

	time.Sleep(100 * time.Millisecond)
	if atomic.LoadInt32(&processedCount) != 2 {
		t.Fatalf("expected both items processed, got %d", atomic.LoadInt32(&processedCount))
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

func TestQueueManager_ConsumerOn_NoHandler_NoLeak(t *testing.T) {
	// Setup worker pool as dispatcher
	wCfg := worker.DefaultConfig()
	wCfg.Concurrency = 2
	wPool := worker.NewPool(wCfg)
	wPool.Start()
	defer func() { _ = wPool.Shutdown() }()

	cfg := DefaultConfig()
	cfg.PollInterval = 10 * time.Millisecond
	cfg.BatchSize = 5
	cfg.MaxSize = 3
	cfg.EnqueueTimeout = 150 * time.Millisecond
	cfg.ConsumerEnabled = true

	// Create manager with consumer ON, but DO NOT register any handler!
	qm, err := NewQueueManager(cfg, wPool.Dispatcher())
	if err != nil {
		t.Fatalf("NewQueueManager failed: %v", err)
	}
	defer func() { _ = qm.Close() }()

	qm.Start()

	// Publish 5 items (more than MaxSize=3)
	for i := 0; i < 5; i++ {
		err := qm.Publish(context.Background(), "unhandled_topic", fmt.Sprintf("payload_%d", i))
		if err != nil {
			t.Fatalf("publish %d failed unexpectedly: %v (possible slot leak)", i, err)
		}
	}

	// Give time for consumer to drain and settle items
	time.Sleep(200 * time.Millisecond)

	stats := qm.Stats()
	if stats.Pending != 0 {
		t.Fatalf("expected 0 pending slots after consumer drain, got %d (slot leak detected!)", stats.Pending)
	}
	if stats.Rejected != 0 {
		t.Fatalf("expected 0 rejections, got %d", stats.Rejected)
	}
}

