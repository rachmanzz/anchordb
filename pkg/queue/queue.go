package queue

import (
	"context"
	"log"
	"sync/atomic"
	"time"

	gsuite_queue "github.com/rachmanzz/gosuite/v3/queue"
	gsuite_worker "github.com/rachmanzz/gosuite/v3/worker"
)

// Standard Event Topics for AnchorDB mutations
const (
	TopicRecordSaved       = "anchordb.record.saved"
	TopicRecordUpdated     = "anchordb.record.updated"
	TopicPartitionArchived = "anchordb.partition.archived"
)

// Config defines queue broker and consumer configuration
type Config struct {
	Enabled         bool          `json:"enabled"`
	ConsumerEnabled bool          `json:"consumer_enabled"`
	PollInterval    time.Duration `json:"poll_interval"`
	BatchSize       int           `json:"batch_size"`
}

// DefaultConfig provides high-throughput in-memory queue settings
func DefaultConfig() Config {
	return Config{
		Enabled:         true,
		ConsumerEnabled: true,
		PollInterval:    20 * time.Millisecond,
		BatchSize:       20,
	}
}

// RecordSavedEvent represents an entity insertion/creation event
type RecordSavedEvent struct {
	Model     string    `json:"model"`
	ID        uint32    `json:"id"`
	Count     int       `json:"count"`
	Timestamp time.Time `json:"timestamp"`
}

// RecordUpdatedEvent represents an entity modification event
type RecordUpdatedEvent struct {
	Model     string    `json:"model"`
	ID        uint32    `json:"id"`
	Timestamp time.Time `json:"timestamp"`
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
	Published int64  `json:"published"`
	Processed int64  `json:"processed"`
	Failed    int64  `json:"failed"`
}

// QueueManager orchestrates async messaging powered by github.com/rachmanzz/gosuite/v3/queue (In-Memory)
type QueueManager struct {
	cfg        Config
	driver     gsuite_queue.Driver
	consumer   *gsuite_queue.Consumer
	dispatcher gsuite_worker.Dispatcher

	published int64
	processed int64
	failed    int64
	running   atomic.Bool
}

// NewQueueManager instantiates the in-memory queue broker
func NewQueueManager(cfg Config, disp gsuite_worker.Dispatcher) (*QueueManager, error) {
	if !cfg.Enabled {
		return &QueueManager{
			cfg:        cfg,
			dispatcher: disp,
		}, nil
	}

	driver := gsuite_queue.NewMemoryDriver()
	gsuite_queue.SetDriver(driver)
	log.Println("[QUEUE] In-Memory Queue Adapter aktif (Zero External Dependency, CPU-native).")

	return &QueueManager{
		cfg:        cfg,
		driver:     driver,
		dispatcher: disp,
	}, nil
}

// Start launches the consumer if enabled
func (m *QueueManager) Start() {
	if !m.cfg.Enabled || m.driver == nil {
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

		m.consumer = gsuite_queue.NewConsumer(m.driver, gsuite_queue.GetRouter(), m.dispatcher, opts...)
		m.consumer.Start()
		log.Printf("[QUEUE] Consumer aktif mendengarkan antrean (poll: %v, batch: %d)",
			pollInterval, batchSize)
	}
}

// Publish enqueues an event topic with automatic JSON serialization
func (m *QueueManager) Publish(ctx context.Context, topic string, payload any, opts ...gsuite_queue.DispatchOption) error {
	if !m.cfg.Enabled || m.driver == nil {
		return nil // Graceful no-op when queue is disabled
	}

	atomic.AddInt64(&m.published, 1)
	err := gsuite_queue.Dispatch(ctx, topic, payload, opts...)
	if err != nil {
		atomic.AddInt64(&m.failed, 1)
		log.Printf("[QUEUE] Gagal mempublikasikan pesan ke topik %q: %v", topic, err)
		return err
	}
	return nil
}

// RegisterHandler registers a subscriber callback for a topic
func (m *QueueManager) RegisterHandler(topic string, handler func(ctx context.Context, payload []byte) error) {
	gsuite_queue.RegisterHandler(topic, func(ctx context.Context, msg *gsuite_queue.Message, pool gsuite_worker.SubWorkerPool) error {
		err := handler(ctx, msg.Payload)
		if err != nil {
			atomic.AddInt64(&m.failed, 1)
			return err
		}
		atomic.AddInt64(&m.processed, 1)
		return nil
	})
	log.Printf("[QUEUE] Handler terdaftar untuk topik %q", topic)
}

// Stats returns runtime metrics for the queue system
func (m *QueueManager) Stats() QueueStats {
	driverName := "disabled"
	if m.cfg.Enabled && m.driver != nil {
		driverName = m.driver.Name()
	}
	return QueueStats{
		Enabled:   m.cfg.Enabled,
		Driver:    driverName,
		Consumer:  m.running.Load(),
		Published: atomic.LoadInt64(&m.published),
		Processed: atomic.LoadInt64(&m.processed),
		Failed:    atomic.LoadInt64(&m.failed),
	}
}

// Close gracefully stops the consumer
func (m *QueueManager) Close() error {
	if m.running.CompareAndSwap(true, false) {
		log.Println("[QUEUE] Menghentikan queue consumer...")
		if m.consumer != nil {
			m.consumer.Stop()
		}
	}
	log.Println("[QUEUE] Queue manager berhasil ditutup.")
	return nil
}
