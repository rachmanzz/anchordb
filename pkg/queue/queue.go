package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	gsuite_queue "github.com/rachmanzz/gosuite/v3/queue"
	gsuite_worker "github.com/rachmanzz/gosuite/v3/worker"
)

// ErrQueueFull is returned when the in-memory queue exceeds its capacity and waiting timeout expires
var ErrQueueFull = errors.New("queue: in-memory queue capacity limit reached, process rejected")

// Standard Event Topics for AnchorDB mutations
const (
	TopicRecordSaved       = "anchordb.record.saved"
	TopicRecordUpdated     = "anchordb.record.updated"
	TopicPartitionArchived = "anchordb.partition.archived"
)

// Config defines bounded queue broker and consumer configuration
type Config struct {
	Enabled         bool          `json:"enabled"`
	ConsumerEnabled bool          `json:"consumer_enabled"`
	PollInterval    time.Duration `json:"poll_interval"`
	BatchSize       int           `json:"batch_size"`
	MaxSize         int           `json:"max_size"`        // Maximum pending in-memory items (RAM capacity bound)
	EnqueueTimeout  time.Duration `json:"enqueue_timeout"` // Timeout waiting for queue slot before rejecting
}

// DefaultConfig provides high-throughput in-memory queue settings with bounded capacity & backpressure
func DefaultConfig() Config {
	return Config{
		Enabled:         true,
		ConsumerEnabled: true,
		PollInterval:    20 * time.Millisecond,
		BatchSize:       20,
		MaxSize:         10000,                  // Maximum 10,000 items in memory
		EnqueueTimeout:  200 * time.Millisecond, // Default fast non-blocking timeout
	}
}

// RecordSavedEvent represents an entity insertion/creation event
type RecordSavedEvent struct {
	Model     string    `json:"model"`
	Name      string    `json:"name"`
	ID        uint32    `json:"id,omitempty"`
	Count     int       `json:"count"`
	Timestamp time.Time `json:"timestamp"`
}

// RecordUpdatedEvent represents an entity modification event
type RecordUpdatedEvent struct {
	Model         string    `json:"model"`
	Names         []string  `json:"names,omitempty"`
	ModifiedCount int       `json:"modified_count"`
	Timestamp     time.Time `json:"timestamp"`
}

// PartitionArchivedEvent represents an historical partition archival event
type PartitionArchivedEvent struct {
	Model          string    `json:"model"`
	ArchivedCount  int       `json:"archived_count"`
	TargetLocation string    `json:"target_location"`
	CutoffDate     string    `json:"cutoff_date"`
	Timestamp      time.Time `json:"timestamp"`
}

// QueueStats contains operational telemetry snapshot for the queue manager
type QueueStats struct {
	Enabled   bool   `json:"enabled"`
	Driver    string `json:"driver"`
	Consumer  bool   `json:"consumer"`
	Capacity  int    `json:"capacity"`
	Pending   int    `json:"pending"`
	Published int64  `json:"published"`
	Processed int64  `json:"processed"`
	Rejected  int64  `json:"rejected"`
	Failed    int64  `json:"failed"`
}

// BoundedMemoryAdapter wraps gsuite_queue.MemoryDriver and enforces capacity limits via slot semaphore.
// It implements gsuite_queue.Adapter natively and frees memory slots when deliveries are Acked or Nacked.
type BoundedMemoryAdapter struct {
	inner          *gsuite_queue.MemoryDriver
	slotCh         chan struct{}
	enqueueTimeout time.Duration
	maxSize        int

	published atomic.Int64
	processed atomic.Int64
	rejected  atomic.Int64
	failed    atomic.Int64
}

// NewBoundedMemoryAdapter creates a bounded in-memory adapter
func NewBoundedMemoryAdapter(name string, maxSize int, timeout time.Duration) *BoundedMemoryAdapter {
	if maxSize <= 0 {
		maxSize = 10000
	}
	if timeout <= 0 {
		timeout = 200 * time.Millisecond
	}
	return &BoundedMemoryAdapter{
		inner:          gsuite_queue.NewNamedMemoryDriver(name),
		slotCh:         make(chan struct{}, maxSize),
		enqueueTimeout: timeout,
		maxSize:        maxSize,
	}
}

func (b *BoundedMemoryAdapter) Name() string {
	return b.inner.Name()
}

func (b *BoundedMemoryAdapter) Capabilities() gsuite_queue.Capabilities {
	return b.inner.Capabilities()
}

func (b *BoundedMemoryAdapter) Enqueue(ctx context.Context, msg *gsuite_queue.Message) error {
	select {
	case b.slotCh <- struct{}{}:
		// Slot acquired
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(b.enqueueTimeout):
		b.rejected.Add(1)
		log.Printf("[WARN] In-memory queue penuh (%d item), proses ditolak setelah menunggu slot memori", b.maxSize)
		return ErrQueueFull
	}

	b.published.Add(1)
	if err := b.inner.Enqueue(ctx, msg); err != nil {
		select {
		case <-b.slotCh:
		default:
		}
		b.failed.Add(1)
		return err
	}
	return nil
}

func (b *BoundedMemoryAdapter) Dequeue(ctx context.Context, req gsuite_queue.DequeueRequest) ([]*gsuite_queue.Delivery, error) {
	deliveries, err := b.inner.Dequeue(ctx, req)
	if err != nil {
		return nil, err
	}
	for range deliveries {
		select {
		case <-b.slotCh:
		default:
		}
	}
	return deliveries, nil
}

func (b *BoundedMemoryAdapter) Ack(ctx context.Context, d *gsuite_queue.Delivery) error {
	b.processed.Add(1)
	return b.inner.Ack(ctx, d)
}

func (b *BoundedMemoryAdapter) Nack(ctx context.Context, d *gsuite_queue.Delivery, reason error) error {
	b.failed.Add(1)
	return b.inner.Nack(ctx, d, reason)
}

func (b *BoundedMemoryAdapter) Close() error {
	return b.inner.Close()
}

// QueueManager orchestrates bounded async messaging powered by github.com/rachmanzz/gosuite/v3/queue
type QueueManager struct {
	cfg        Config
	adapter    *BoundedMemoryAdapter
	router     *gsuite_queue.Router
	consumer   *gsuite_queue.Consumer
	dispatcher gsuite_worker.Dispatcher
	running    atomic.Bool
}

// NewQueueManager instantiates the bounded in-memory queue broker using isolated instance API
func NewQueueManager(cfg Config, disp gsuite_worker.Dispatcher) (*QueueManager, error) {
	if !cfg.Enabled {
		return &QueueManager{
			cfg:        cfg,
			dispatcher: disp,
		}, nil
	}

	if cfg.MaxSize <= 0 {
		cfg.MaxSize = 10000
	}
	if cfg.EnqueueTimeout <= 0 {
		cfg.EnqueueTimeout = 200 * time.Millisecond
	}

	adapter := NewBoundedMemoryAdapter("anchordb-memory", cfg.MaxSize, cfg.EnqueueTimeout)
	router := gsuite_queue.NewRouter()

	log.Printf("[QUEUE] In-Memory Queue Adapter aktif (Zero External Dependency, Kapasitas Memori: %d item, Timeout Tunggu: %v).",
		cfg.MaxSize, cfg.EnqueueTimeout)

	return &QueueManager{
		cfg:        cfg,
		adapter:    adapter,
		router:     router,
		dispatcher: disp,
	}, nil
}

// Start launches the consumer if enabled
func (m *QueueManager) Start() {
	if !m.cfg.Enabled || m.adapter == nil {
		return
	}

	if !m.cfg.ConsumerEnabled {
		log.Println("[QUEUE] Queue consumer dinonaktifkan (mode producer murni).")
		return
	}

	if m.running.CompareAndSwap(false, true) {
		pollInterval := m.cfg.PollInterval
		if pollInterval <= 0 {
			pollInterval = 20 * time.Millisecond
		}
		batchSize := m.cfg.BatchSize
		if batchSize <= 0 {
			batchSize = 20
		}

		opts := []gsuite_queue.ConsumerOption{
			gsuite_queue.WithPollInterval(pollInterval),
			gsuite_queue.WithBatchSize(batchSize),
		}

		m.consumer = gsuite_queue.NewConsumer(m.adapter, m.router, m.dispatcher, opts...)
		m.consumer.Start()
		log.Printf("[QUEUE] Consumer aktif mendengarkan antrean (poll: %v, batch: %d, limit: %d)",
			pollInterval, batchSize, m.cfg.MaxSize)
	}
}

// RegisterHandler registers a subscriber callback for a topic
func (m *QueueManager) RegisterHandler(topic string, handler func(ctx context.Context, payload []byte) error) {
	if m.router == nil {
		return
	}
	m.router.Register(topic, func(ctx context.Context, msg *gsuite_queue.Message, pool gsuite_worker.SubWorkerPool) error {
		return handler(ctx, msg.Payload)
	}, gsuite_queue.WithAllowOverwrite())
	log.Printf("[QUEUE] Handler terdaftar untuk topik %q", topic)
}

// Publish enqueues an event into the bounded adapter. If queue is full, it waits up to EnqueueTimeout.
func (m *QueueManager) Publish(ctx context.Context, topic string, payload any, opts ...gsuite_queue.DispatchOption) error {
	if !m.cfg.Enabled || m.adapter == nil {
		return nil
	}

	var bytes []byte
	switch v := payload.(type) {
	case []byte:
		bytes = v
	case string:
		bytes = []byte(v)
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return fmt.Errorf("queue: failed to marshal payload: %w", err)
		}
		bytes = b
	}

	msg := gsuite_queue.NewMessage(topic, bytes, opts...)
	return m.adapter.Enqueue(ctx, msg)
}

// Stats returns runtime metrics for the queue system
func (m *QueueManager) Stats() QueueStats {
	driverName := "disabled"
	pending := 0
	var pub, proc, rej, fail int64

	if m.cfg.Enabled && m.adapter != nil {
		driverName = m.adapter.Name()
		pending = len(m.adapter.slotCh)
		pub = m.adapter.published.Load()
		proc = m.adapter.processed.Load()
		rej = m.adapter.rejected.Load()
		fail = m.adapter.failed.Load()
	}

	return QueueStats{
		Enabled:   m.cfg.Enabled,
		Driver:    driverName,
		Consumer:  m.running.Load(),
		Capacity:  m.cfg.MaxSize,
		Pending:   pending,
		Published: pub,
		Processed: proc,
		Rejected:  rej,
		Failed:    fail,
	}
}

// Close gracefully stops the consumer and closes the adapter
func (m *QueueManager) Close() error {
	if m.running.CompareAndSwap(true, false) {
		log.Println("[QUEUE] Menghentikan queue consumer...")
		if m.consumer != nil {
			m.consumer.Stop()
		}
	}
	if m.adapter != nil {
		_ = m.adapter.Close()
	}
	log.Println("[QUEUE] Queue manager berhasil ditutup.")
	return nil
}
