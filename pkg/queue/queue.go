package queue

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	nats "github.com/nats-io/nats.go"
	gsuite_queue "github.com/rachmanzz/gosuite/v3/queue"
	queuenats "github.com/rachmanzz/gosuite/v3/queue-nats"
	queueredis "github.com/rachmanzz/gosuite/v3/queue-redis"
	gsuite_worker "github.com/rachmanzz/gosuite/v3/worker"
	redis "github.com/redis/go-redis/v9"
)

// Standard Event Topics for AnchorDB mutations
const (
	TopicRecordSaved       = "anchordb.record.saved"
	TopicRecordUpdated     = "anchordb.record.updated"
	TopicPartitionArchived = "anchordb.partition.archived"
)

// DriverType defines the underlying broker adapter
type DriverType string

const (
	DriverMemory DriverType = "memory"
	DriverRedis  DriverType = "redis"
	DriverNATS   DriverType = "nats"
)

// Config defines queue broker and consumer configuration
type Config struct {
	Enabled         bool          `json:"enabled"`
	Driver          DriverType    `json:"driver"`
	ConsumerEnabled bool          `json:"consumer_enabled"`
	PollInterval    time.Duration `json:"poll_interval"`
	BatchSize       int           `json:"batch_size"`

	// Redis adapter options (optional)
	RedisAddr     string `json:"redis_addr,omitempty"`
	RedisPassword string `json:"redis_password,omitempty"`
	RedisDB       int    `json:"redis_db,omitempty"`
	RedisPrefix   string `json:"redis_prefix,omitempty"`

	// NATS JetStream options (optional)
	NATSURL    string `json:"nats_url,omitempty"`
	NATSStream string `json:"nats_stream,omitempty"`
}

// DefaultConfig provides default in-memory queue settings
func DefaultConfig() Config {
	return Config{
		Enabled:         true,
		Driver:          DriverMemory,
		ConsumerEnabled: true,
		PollInterval:    50 * time.Millisecond,
		BatchSize:       10,
		RedisPrefix:     "anchordb:queue:",
		NATSStream:      "anchordb_stream",
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

// QueueManager orchestrates async messaging powered by github.com/rachmanzz/gosuite/v3/queue
type QueueManager struct {
	cfg        Config
	driver     gsuite_queue.Driver
	consumer   *gsuite_queue.Consumer
	dispatcher gsuite_worker.Dispatcher

	// Connections to close cleanly
	redisClient *redis.Client
	natsConn    *nats.Conn

	published int64
	processed int64
	failed    int64
	mu        sync.RWMutex
	running   atomic.Bool
}

// NewQueueManager instantiates and connects the specified broker driver
func NewQueueManager(cfg Config, disp gsuite_worker.Dispatcher) (*QueueManager, error) {
	if !cfg.Enabled {
		return &QueueManager{
			cfg:        cfg,
			dispatcher: disp,
		}, nil
	}

	// Auto-detect driver if not explicitly specified
	if cfg.Driver == "" {
		if cfg.RedisAddr != "" {
			cfg.Driver = DriverRedis
		} else if cfg.NATSURL != "" {
			cfg.Driver = DriverNATS
		} else {
			cfg.Driver = DriverMemory
		}
	}

	mgr := &QueueManager{
		cfg:        cfg,
		dispatcher: disp,
	}

	switch cfg.Driver {
	case DriverRedis:
		if cfg.RedisAddr == "" {
			return nil, fmt.Errorf("queue: redis driver requested but redis_addr is empty")
		}
		rdb := redis.NewClient(&redis.Options{
			Addr:     cfg.RedisAddr,
			Password: cfg.RedisPassword,
			DB:       cfg.RedisDB,
		})

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := rdb.Ping(ctx).Err(); err != nil {
			_ = rdb.Close()
			return nil, fmt.Errorf("queue: failed to connect to redis at %s: %w", cfg.RedisAddr, err)
		}

		prefix := cfg.RedisPrefix
		if prefix == "" {
			prefix = "anchordb:queue:"
		}

		redisAdapter := queueredis.New(rdb, queueredis.WithPrefix(prefix))
		mgr.driver = redisAdapter
		mgr.redisClient = rdb
		log.Printf("[QUEUE] Terhubung ke Redis Queue di %s (prefix: %s)", cfg.RedisAddr, prefix)

	case DriverNATS:
		if cfg.NATSURL == "" {
			return nil, fmt.Errorf("queue: nats driver requested but nats_url is empty")
		}
		nc, err := nats.Connect(cfg.NATSURL)
		if err != nil {
			return nil, fmt.Errorf("queue: failed to connect to nats at %s: %w", cfg.NATSURL, err)
		}

		stream := cfg.NATSStream
		if stream == "" {
			stream = "anchordb_stream"
		}

		natsAdapter, err := queuenats.New(nc, queuenats.WithStream(stream))
		if err != nil {
			nc.Close()
			return nil, fmt.Errorf("queue: failed to initialize nats queue adapter: %w", err)
		}

		mgr.driver = natsAdapter
		mgr.natsConn = nc
		log.Printf("[QUEUE] Terhubung ke NATS JetStream Queue di %s (stream: %s)", cfg.NATSURL, stream)

	case DriverMemory:
		fallthrough
	default:
		mgr.driver = gsuite_queue.NewMemoryDriver()
		log.Println("[QUEUE] Menggunakan In-Memory Queue Adapter (Zero External Dependency).")
	}

	// Register with global gsuite/v3/queue manager
	gsuite_queue.SetDriver(mgr.driver)

	return mgr, nil
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
			pollInterval = 50 * time.Millisecond
		}
		batchSize := m.cfg.BatchSize
		if batchSize <= 0 {
			batchSize = 10
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

// Close gracefully stops the consumer and terminates external network connections
func (m *QueueManager) Close() error {
	if m.running.CompareAndSwap(true, false) {
		log.Println("[QUEUE] Menghentikan queue consumer...")
		if m.consumer != nil {
			m.consumer.Stop()
		}
	}

	var errs []string
	if m.redisClient != nil {
		if err := m.redisClient.Close(); err != nil {
			errs = append(errs, fmt.Sprintf("redis: %v", err))
		}
		m.redisClient = nil
	}
	if m.natsConn != nil {
		m.natsConn.Close()
		m.natsConn = nil
	}

	if len(errs) > 0 {
		return fmt.Errorf("queue shutdown errors: %s", strings.Join(errs, ", "))
	}
	log.Println("[QUEUE] Queue manager berhasil ditutup.")
	return nil
}
