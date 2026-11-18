// Package main runs the AnchorDB server engine daemon.
package main

import (
	"container/list"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/rachmanzz/anchordb/pkg/driver"
	"github.com/rachmanzz/anchordb/pkg/partition"
	"github.com/rachmanzz/anchordb/pkg/scheduler"
	"github.com/rachmanzz/anchordb/pkg/security"
	"github.com/rachmanzz/anchordb/pkg/spatial"
	"github.com/rachmanzz/anchordb/pkg/storage"
	"github.com/rachmanzz/anchordb/pkg/system"
	"github.com/rachmanzz/anchordb/pkg/wire"
	"github.com/rachmanzz/anchordb/pkg/worker"
)

const (
	maxRequestBodyBytes = 10 << 20
	// maxQueryLimit caps how many records a single /v1/list call can return,
	// preventing a client from requesting the entire dataset in one request.
	maxQueryLimit = 1000
)

// ServerConfig holds complete runtime configuration parsed from CLI flags and ENV
type ServerConfig struct {
	Port          string
	DataDir       string
	AuthToken     string
	MemoryLimit   string
	IPWhitelist   string
	TrustProxy    bool
	MemorySummary string

	// Background subsystems (Minimal Dependencies: pure Go, zero external broker)
	CronEnabled       bool
	PartitionSchedule string
	RetentionDays     float64
	SyncInterval      string
	CronTaskTimeout   time.Duration
	CronJitter        time.Duration
	ShutdownTimeout   time.Duration // total graceful shutdown budget (default 25s)

	WorkerConfig worker.Config

	// Database durability options passed to every opened model.
	DBOptions storage.Options
	// Per-client abuse controls. RateLimitPerSec <= 0 disables rate limiting.
	RateLimitPerSec float64
	RateLimitBurst  int
	WirePort        string

	// Gateway & Cluster Join options
	GatewayMode        bool
	JoinMode           string
	PairingTimeout     time.Duration
	PairingAllowedCIDR string
	SessionSecret      string

	// Cold Storage & S3 defaults
	S3DefaultEndpoint  string
	S3DefaultBucket    string
	S3DefaultPrefix    string
	S3DefaultRegion    string
	S3DefaultAccessKey string
	S3DefaultSecretKey string
}

// Server holds active AnchorDB database instances and background subsystems
type Server struct {
	mu            sync.RWMutex
	dataDir       string
	authToken     string
	port          string
	wirePort      string
	wireServer    *wire.Server
	models        map[string]*storage.AnchorDB
	modelPins     map[string]*atomic.Int64
	modelDraining map[string]chan struct{}
	lruMu         sync.Mutex
	modelLRU      *list.List
	modelElems    map[string]*list.Element
	scheduler     *scheduler.InternalScheduler
	cronEnabled   bool
	worker        *worker.Pool
	ipFilter      *security.IPFilter
	startTime     time.Time

	dbOptions storage.Options
	limiter   *rateLimiter

	memStatsMu     sync.Mutex
	memStatsCached runtime.MemStats
	memStatsAt     time.Time

	// Gateway & Cluster Join state
	gatewayMode bool
	joinManager *wire.JoinManager

	// Cold storage & partition catalog
	catalog            *partition.Catalog
	s3Cache            *partition.S3PartitionCache
	s3DefaultEndpoint  string
	s3DefaultBucket    string
	s3DefaultPrefix    string
	s3DefaultRegion    string
	s3DefaultAccessKey string
	s3DefaultSecretKey string

	// Lifecycle state.
	draining        atomic.Bool        // true once Shutdown began (readiness = 503)
	closed          bool               // guarded by mu; registry sealed after Shutdown
	shutdownTimeout time.Duration      // total graceful-shutdown budget
	opCtx           context.Context    // canceled when async worker drain times out
	opCancel        context.CancelFunc // cancels opCtx
}

func (s *Server) asyncBaseContext() context.Context {
	if s.opCtx != nil {
		return s.opCtx
	}
	return context.Background()
}

func NewServer(cfg ServerConfig) (*Server, error) {
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("cannot create data dir %q: %w", cfg.DataDir, err)
	}
	probe, err := os.CreateTemp(cfg.DataDir, ".anchordb_write_check_*")
	if err != nil {
		return nil, fmt.Errorf("data dir %q is not writable: %w", cfg.DataDir, err)
	}
	if err := probe.Close(); err != nil {
		log.Printf("[WARN] Closing data dir write probe failed: %v", err)
	}
	if err := os.Remove(probe.Name()); err != nil && !errors.Is(err, os.ErrNotExist) {
		log.Printf("[WARN] Removing data dir write probe failed: %v", err)
	}
	ipFilter, err := security.NewIPFilter(cfg.IPWhitelist, cfg.TrustProxy)
	if err != nil {
		return nil, fmt.Errorf("invalid IP whitelist %q: %w", cfg.IPWhitelist, err)
	}

	// Initialize Partition Catalog and S3 Cache
	catalogPath := filepath.Join(cfg.DataDir, "partitions", "catalog.json")
	cat, err := partition.OpenCatalog(catalogPath)
	if err != nil {
		return nil, fmt.Errorf("cannot open partition catalog: %w", err)
	}

	s3CacheDir := filepath.Join(cfg.DataDir, ".s3_cache")
	s3Cache, err := partition.NewS3PartitionCache(s3CacheDir)
	if err != nil {
		return nil, fmt.Errorf("cannot initialize s3 partition cache: %w", err)
	}

	// 0. Validate cron schedules before starting any subsystem: a typo must
	// fail startup with a clear error, never a silently unregistered task.
	if cfg.CronEnabled {
		if cfg.PartitionSchedule != "" {
			if err := scheduler.ValidateSchedule(cfg.PartitionSchedule); err != nil {
				return nil, fmt.Errorf("invalid partition schedule %q: %w", cfg.PartitionSchedule, err)
			}
		}
		if cfg.SyncInterval != "" && cfg.SyncInterval != "0" && cfg.SyncInterval != "off" {
			if err := scheduler.ValidateSchedule(cfg.SyncInterval); err != nil {
				return nil, fmt.Errorf("invalid sync interval %q: %w", cfg.SyncInterval, err)
			}
		}
	}

	// 1. Initialize Worker Pool
	workerPool := worker.NewPool(cfg.WorkerConfig)
	workerPool.Start()

	// 2. Initialize Cron Scheduler
	sched := scheduler.NewInternalScheduler()
	if cfg.CronTaskTimeout > 0 {
		sched.SetTaskTimeout(cfg.CronTaskTimeout)
	}
	if cfg.CronJitter > 0 {
		sched.SetJitter(cfg.CronJitter)
	}

	joinMgr := wire.NewJoinManager(wire.JoinConfig{
		GatewayMode:         cfg.GatewayMode,
		JoinMode:            cfg.JoinMode,
		PairingTimeout:      cfg.PairingTimeout,
		PairingAllowedCIDR:  cfg.PairingAllowedCIDR,
		SessionSecret:       cfg.SessionSecret,
		AuthToken:           cfg.AuthToken,
		ClusterID:           "cluster-01",
		CommitmentStorePath: filepath.Join(cfg.DataDir, "auth", "commitments.bin"),
	})
	if initErr := joinMgr.InitError(); initErr != nil {
		sched.Stop()
		if wErr := workerPool.Shutdown(); wErr != nil {
			log.Printf("[WARN] Worker pool shutdown during init abort failed: %v", wErr)
		}
		return nil, initErr
	}

	opCtx, opCancel := context.WithCancel(context.Background())
	s := &Server{
		dataDir:            cfg.DataDir,
		authToken:          cfg.AuthToken,
		port:               cfg.Port,
		wirePort:           cfg.WirePort,
		gatewayMode:        cfg.GatewayMode,
		joinManager:        joinMgr,
		catalog:            cat,
		s3Cache:            s3Cache,
		s3DefaultEndpoint:  cfg.S3DefaultEndpoint,
		s3DefaultBucket:    cfg.S3DefaultBucket,
		s3DefaultPrefix:    cfg.S3DefaultPrefix,
		s3DefaultRegion:    cfg.S3DefaultRegion,
		s3DefaultAccessKey: cfg.S3DefaultAccessKey,
		s3DefaultSecretKey: cfg.S3DefaultSecretKey,
		models:             make(map[string]*storage.AnchorDB),
		modelPins:          make(map[string]*atomic.Int64),
		modelDraining:      make(map[string]chan struct{}),
		scheduler:          sched,
		cronEnabled:        cfg.CronEnabled,
		worker:             workerPool,
		ipFilter:           ipFilter,
		startTime:          time.Now(),

		dbOptions: cfg.DBOptions,

		shutdownTimeout: cfg.ShutdownTimeout,
		opCtx:           opCtx,
		opCancel:        opCancel,
	}
	s.wireServer = wire.NewServerWithJoinManager(s, joinMgr)

	if cfg.RateLimitPerSec > 0 {
		s.limiter = newRateLimiter(cfg.RateLimitPerSec, cfg.RateLimitBurst)
		s.wireServer.SetRateLimiter(s.limiter)
	}

	// Register top-global scheduled tasks if cron is enabled. Schedules were
	// validated before subsystem start, so a failure here is unexpected: stop
	// everything and fail startup rather than running without retention/sync.
	failStartup := func(task string, err error) (*Server, error) {
		s.scheduler.Stop()
		if wErr := workerPool.Shutdown(); wErr != nil {
			log.Printf("[WARN] Worker pool shutdown during startup abort failed: %v", wErr)
		}
		return nil, fmt.Errorf("failed to register cron task %s: %w", task, err)
	}
	if cfg.CronEnabled {
		if cfg.PartitionSchedule != "" {
			if err := s.scheduler.RegisterTask("auto_partition_maintenance", NewAutoPartitionCronTask(s, cfg.PartitionSchedule, cfg.RetentionDays)); err != nil {
				return failStartup("auto_partition_maintenance", err)
			}
		}

		if cfg.SyncInterval != "" && cfg.SyncInterval != "0" && cfg.SyncInterval != "off" {
			if err := s.scheduler.RegisterTask("periodic_background_sync", NewPeriodicSyncCronTask(s, cfg.SyncInterval)); err != nil {
				return failStartup("periodic_background_sync", err)
			}
		}

		s.scheduler.Start()
	}

	return s, nil
}

// Routes returns the configured HTTP handler with all endpoints and middlewares registered
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/metrics", s.handleMetrics)
	mux.HandleFunc("/v1/save", s.authMiddleware(s.handleSave))
	mux.HandleFunc("/v1/save_batch", s.authMiddleware(s.handleSaveBatch))
	mux.HandleFunc("/v1/get", s.authMiddleware(s.handleGet))
	mux.HandleFunc("/v1/list", s.authMiddleware(s.handleList))
	mux.HandleFunc("/v1/update", s.authMiddleware(s.handleUpdate))
	mux.HandleFunc("/v1/graph", s.authMiddleware(s.handleGraph))
	mux.HandleFunc("/v1/edge", s.authMiddleware(s.handleEdge))
	mux.HandleFunc("/v1/causal", s.authMiddleware(s.handleCausal))
	mux.HandleFunc("/v1/math", s.authMiddleware(s.handleMath))
	mux.HandleFunc("/v1/sync", s.authMiddleware(s.handleSync))
	mux.HandleFunc("/v1/record", s.authMiddleware(s.handleGet))
	mux.HandleFunc("/v1/partition/run", s.authMiddleware(s.handlePartitionRun))
	mux.HandleFunc("/v1/partition/list", s.authMiddleware(s.handlePartitionList))
	mux.HandleFunc("/v1/partition/inspect", s.authMiddleware(s.handlePartitionInspect))
	mux.HandleFunc("/v1/partition/catalog", s.authMiddleware(s.handlePartitionCatalog))
	mux.HandleFunc("/v1/partition/catalog/record", s.authMiddleware(s.handlePartitionCatalogRecord))
	mux.HandleFunc("/v1/partition/s3/list", s.authMiddleware(s.handlePartitionS3List))
	mux.HandleFunc("/v1/partition/s3/inspect", s.authMiddleware(s.handlePartitionS3Inspect))
	mux.HandleFunc("/v1/collector/run", s.authMiddleware(s.handleCollectorRun))
	mux.HandleFunc("/v1/scheduler/status", s.authMiddleware(s.handleSchedulerStatus))
	mux.HandleFunc("/v1/scheduler/run", s.authMiddleware(s.handleSchedulerRun))
	mux.HandleFunc("/v1/worker/status", s.authMiddleware(s.handleWorkerStatus))
	mux.HandleFunc("/v1/worker/cancel", s.authMiddleware(s.handleWorkerCancel))
	mux.HandleFunc("/v1/model/attach", s.authMiddleware(s.handleModelAttach))
	mux.HandleFunc("/v1/model/schema", s.authMiddleware(s.handleModelSchema))
	mux.HandleFunc("/v1/model/reset", s.authMiddleware(s.handleModelReset))
	// recoverMiddleware is outermost so a panic in any layer yields a clean 500
	// instead of dropping the connection with a stack trace. ipFilter runs outside
	// rateLimitMiddleware so disallowed IPs are rejected before touching rate-limit buckets.
	return recoverMiddleware(s.ipFilter.Middleware(s.rateLimitMiddleware(mux)))
}

// recoverMiddleware catches panics from downstream handlers, logs them, and
// returns a generic 500 without leaking internal details or a stack trace.
func recoverMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				if rec == http.ErrAbortHandler {
					panic(rec)
				}
				log.Printf("[ERROR] panic recovered serving %s %s: %v", r.Method, r.URL.Path, rec)
				writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal server error"})
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func limitRequestBody(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)
		next.ServeHTTP(w, r)
	})
}

func decodeJSONBody(w http.ResponseWriter, r *http.Request, dst any, allowEOF bool) bool {
	err := json.NewDecoder(r.Body).Decode(dst)
	if err == nil {
		return true
	}
	var maxErr *http.MaxBytesError
	if errors.As(err, &maxErr) {
		writeJSONError(w, http.StatusRequestEntityTooLarge, "request body too large")
		return false
	}
	if allowEOF && errors.Is(err, io.EOF) {
		return true
	}
	writeJSONError(w, http.StatusBadRequest, "invalid json body")
	return false
}

func dispatchErrorStatus(err error) int {
	switch {
	case errors.Is(err, storage.ErrRecordNotFound):
		return http.StatusNotFound
	case errors.Is(err, storage.ErrComputeFailed):
		return http.StatusUnprocessableEntity
	case errors.Is(err, worker.ErrWorkerUnavailable), errors.Is(err, storage.ErrDatabaseClosed):
		return http.StatusServiceUnavailable
	default:
		return http.StatusInternalServerError
	}
}

// requireMethod writes a 405 response and reports false when r.Method is not one
// of the allowed methods.
func requireMethod(w http.ResponseWriter, r *http.Request, allowed ...string) bool {
	for _, m := range allowed {
		if r.Method == m {
			return true
		}
	}
	writeJSON(w, http.StatusMethodNotAllowed, map[string]string{
		"error": fmt.Sprintf("method not allowed; use %s", strings.Join(allowed, " or ")),
	})
	return false
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	data, err := json.Marshal(payload)
	if err != nil {
		log.Printf("[ERROR] Failed marshaling HTTP JSON response: %v", err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		if _, wErr := io.WriteString(w, `{"error":"internal serialization error"}`); wErr != nil {
			log.Printf("[WARN] Failed writing serialization error response: %v", wErr)
		}
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if _, err := w.Write(data); err != nil {
		log.Printf("[WARN] Failed writing HTTP JSON response: %v", err)
	}
}

func writeJSONError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func checkMemoryWriteAdmission(w http.ResponseWriter, db *storage.AnchorDB) bool {
	headroom := system.GetMemoryHeadroom()
	if headroom.LimitKnown && headroom.UsedPercent >= 88 {
		db.TriggerAutoSync()
	}
	if headroom.LimitKnown && headroom.UsedPercent >= 93 {
		writeJSONError(w, http.StatusInsufficientStorage, fmt.Sprintf("storage admission control: container memory pressure %d%%", headroom.UsedPercent))
		return false
	}
	return true
}

// validateS3Endpoint rejects endpoints that would let an authenticated caller
// use the server as an SSRF proxy (loopback, private, link-local and metadata
// addresses). Set ANCHORDB_ALLOW_S3_PRIVATE_ENDPOINT=true to permit them for a
// trusted local MinIO-style deployment.
func validateS3Endpoint(endpoint string, allowPrivate bool) error {
	if endpoint == "" {
		return nil
	}
	u, err := url.Parse(endpoint)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("invalid s3_endpoint %q", endpoint)
	}
	if allowPrivate {
		return nil
	}
	host := u.Hostname()
	ips, err := net.LookupIP(host)
	if err != nil {
		return fmt.Errorf("cannot resolve s3_endpoint host %q", host)
	}
	for _, ip := range ips {
		if partition.IsBlockedS3IP(ip) {
			return fmt.Errorf("s3_endpoint %q resolves to a private/loopback address; refusing (set ANCHORDB_ALLOW_S3_PRIVATE_ENDPOINT=true to allow)", host)
		}
	}
	return nil
}

// resolvePathWithExistingSymlinks resolves the existing prefix of path and
// appends any not-yet-created suffix. This catches symlinks in the destination
// while still allowing the sink to create a new directory tree.
func resolvePathWithExistingSymlinks(path string) (string, error) {
	path, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	var suffix []string
	for candidate := path; ; candidate = filepath.Dir(candidate) {
		resolved, evalErr := filepath.EvalSymlinks(candidate)
		if evalErr == nil {
			for i := len(suffix) - 1; i >= 0; i-- {
				resolved = filepath.Join(resolved, suffix[i])
			}
			return filepath.Clean(resolved), nil
		}
		if !os.IsNotExist(evalErr) {
			return "", evalErr
		}
		parent := filepath.Dir(candidate)
		if parent == candidate {
			return "", evalErr
		}
		suffix = append(suffix, filepath.Base(candidate))
	}
}

// ErrServerClosed is returned by getModel once Shutdown has sealed the model registry.
var ErrServerClosed = errors.New("server is shut down")

// Shutdown performs the full graceful stop: mark draining (readiness) → drain
// HTTP → stop scheduler → stop worker → Sync (+Close) every model. It is bounded
// by ctx and splits the budget between phases (HTTP 1/3, scheduler 1/6, worker
// up to 1/3 via Config.ShutdownTimeout, rest for Sync).
//
// Safety rule (L1/L2): the databases are Closed ONLY if every phase before them
// finished cleanly. If HTTP handlers, cron tasks or worker tasks may still be
// running, closing the DB (munmap + WAL close) under them would make their
// writes vanish silently. In that case we still Sync (safe concurrently with
// mutations) and leave the DB open; durability then rests on the WAL, which is
// replayed on the next start. The returned error reports what did not stop.
func (s *Server) Shutdown(ctx context.Context, httpSrv *http.Server) error {
	var errs []error
	safeToClose := true

	budget := s.shutdownTimeout
	if budget <= 0 {
		budget = 25 * time.Second
	}

	// 0. Readiness: /health reports shutting_down so traffic is routed away.
	s.draining.Store(true)

	// 1. Drain in-flight HTTP requests (25% of budget).
	if httpSrv != nil {
		hctx, cancel := context.WithTimeout(ctx, budget/4)
		if err := httpSrv.Shutdown(hctx); err != nil {
			errs = append(errs, fmt.Errorf("http drain: %w", err))
			// Shutdown timed out: force-close listeners and connections so no new
			// work arrives (handlers already running cannot be killed in Go).
			_ = httpSrv.Close()
			safeToClose = false
		}
		cancel()
	}

	// 1.5 Stop AnchorWire binary TCP daemon within shutdown budget (20% of budget).
	if s.wireServer != nil {
		if wErr := s.wireServer.CloseWithTimeout(budget / 5); wErr != nil {
			errs = append(errs, fmt.Errorf("wire server stop: %w", wErr))
			safeToClose = false
		}
	}

	// 2. Stop the cron scheduler (16.7% of budget; cancels task contexts, waits for loops).
	if s.scheduler != nil {
		sctx, cancel := context.WithTimeout(ctx, budget/6)
		if err := s.scheduler.Shutdown(sctx); err != nil {
			errs = append(errs, fmt.Errorf("scheduler stop: %w", err))
			safeToClose = false
		}
		cancel()
	}

	// 3. Stop the worker pool, honoring ctx (Pool.Shutdown uses up to 2 * ShutdownTimeout).
	// Total phase maxima stay within ~87% of budget, leaving margin for model sync/close.
	if s.worker != nil {
		done := make(chan error, 1)
		go func() { done <- s.worker.Shutdown() }()
		select {
		case err := <-done:
			if err != nil {
				errs = append(errs, fmt.Errorf("worker stop: %w", err))
				safeToClose = false
			}
		case <-ctx.Done():
			if s.opCancel != nil {
				s.opCancel()
			}
			errs = append(errs, fmt.Errorf("worker stop: %w", ctx.Err()))
			safeToClose = false
		}
	}
	if s.opCancel != nil {
		s.opCancel()
	}

	// 4. Flush every model. Seal registry first and wait for any in-flight evictions.
	s.mu.Lock()
	s.closed = true // seal the registry: no model may be opened after this point
	for len(s.modelDraining) > 0 {
		var waitCh chan struct{}
		for _, ch := range s.modelDraining {
			waitCh = ch
			break
		}
		s.mu.Unlock()
		<-waitCh
		s.mu.Lock()
	}
	defer s.mu.Unlock()
	for name, db := range s.models {
		if err := db.Sync(); err != nil {
			errs = append(errs, fmt.Errorf("sync model %s: %w", name, err))
		}
		if safeToClose {
			if err := db.Close(); err != nil {
				errs = append(errs, fmt.Errorf("close model %s: %w", name, err))
			}
			delete(s.models, name)
			delete(s.modelPins, name)
		}
	}
	if safeToClose {
		s.lruMu.Lock()
		s.modelLRU = nil
		s.modelElems = nil
		s.lruMu.Unlock()
	}
	if !safeToClose {
		if s.dbOptions.WAL.SyncMode == storage.SyncAlways {
			log.Printf("[WARN] Databases not closed because background work may still be running; durability of subsequent writes relies on WAL (sync=always, replayed on restart).")
		} else {
			log.Printf("[WARN] Databases not closed because background work may still be running; subsequent writes after this point are not guaranteed durable under non-sync WAL mode.")
		}
	}

	if s.s3Cache != nil && safeToClose {
		if err := s.s3Cache.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close s3 cache: %w", err))
		}
	} else if s.s3Cache != nil {
		log.Printf("[WARN] S3 partition cache not closed because background handlers may still be active.")
	}

	return errors.Join(errs...)
}

// WireServer returns the internal AnchorWire binary protocol server.
func (s *Server) WireServer() *wire.Server {
	return s.wireServer
}

// GetModel implements wire.Backend.
func (s *Server) GetModel(name string) (*storage.AnchorDB, error) {
	return s.getModel(name)
}

// AcquireModel implements wire.LeaseBackend, pinning the model handle until
// release is invoked so LRU eviction cannot Close/Munmap it while in flight.
func (s *Server) AcquireModel(name string) (*storage.AnchorDB, func(), error) {
	return s.acquireModel(name)
}

// Authenticate implements wire.Backend.
func (s *Server) Authenticate(token string) bool {
	if s.gatewayMode {
		return false
	}
	if s.authToken == "" {
		return true
	}
	return subtle.ConstantTimeCompare([]byte(token), []byte(s.authToken)) == 1
}

// RequiresAuth implements wire.Backend.
func (s *Server) RequiresAuth() bool {
	return s.authToken != "" || s.gatewayMode
}

// --- TOP-GLOBAL CRON TASK HANDLERS -------------------------------------------

// AutoPartitionCronTask is the top-global handler for automated database partitioning.
type AutoPartitionCronTask struct {
	server        *Server
	schedule      string
	retentionDays float64
}

func NewAutoPartitionCronTask(s *Server, schedule string, retentionDays float64) *AutoPartitionCronTask {
	return &AutoPartitionCronTask{
		server:        s,
		schedule:      schedule,
		retentionDays: retentionDays,
	}
}

func (t *AutoPartitionCronTask) Schedule() string {
	return t.schedule
}

func dispatchAndWait(ctx context.Context, pool *worker.Pool, name string, fn func(context.Context) error, opts ...worker.Option) error {
	done := make(chan error, 1)
	wrappedOpts := append([]worker.Option{}, opts...)
	wrappedOpts = append(wrappedOpts,
		worker.WithOnSuccess(func(_ context.Context) { done <- nil }),
		worker.WithOnError(func(_ context.Context, err error) { done <- err }),
	)
	if err := pool.Dispatch(ctx, name, fn, wrappedOpts...); err != nil {
		return err
	}
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (t *AutoPartitionCronTask) Execute(ctx context.Context) error {
	log.Printf("[SCHEDULER] Running automatic partition maintenance (archiving data > %.0f days)...", t.retentionDays)
	models := t.server.allModelNames()

	var errs []error
	for _, m := range models {
		modelName := m
		err := dispatchAndWait(
			ctx,
			t.server.worker,
			"cron_partition_"+modelName,
			func(taskCtx context.Context) error {
				if err := taskCtx.Err(); err != nil {
					return err
				}
				db, release, err := t.server.acquireModel(modelName)
				if err != nil {
					return err
				}
				defer release()
				partRes, err := partition.ExecutePartition(db, partition.PartitionPolicy{
					ModelName:     modelName,
					DateField:     "date",
					RetentionDays: t.retentionDays,
					Sink:          partition.NewLocalDiskSink(filepath.Join(t.server.dataDir, "partitions")),
					FileFormat:    "{date}_{uuid}.anchordb",
					PruneHotDB:    true,
					Catalog:       t.server.catalog,
				})
				if err == nil && partRes.ArchivedCount > 0 {
					log.Printf("[WORKER] Model %q partitioned: %d records archived to %s",
						modelName, partRes.ArchivedCount, partRes.TargetLocation)
				}
				return err
			},
			worker.WithModel(modelName),
			worker.WithRetry(2, 500*time.Millisecond),
			worker.WithRetryJitter(100*time.Millisecond),
		)
		if err != nil {
			log.Printf("[WARN] AutoPartition failed for model %q: %v", modelName, err)
			errs = append(errs, fmt.Errorf("model %s: %w", modelName, err))
		}
	}
	return errors.Join(errs...)
}

func (t *AutoPartitionCronTask) OnError(_ context.Context, err error) {
	log.Printf("[SCHEDULER] AutoPartitionCronTask recoverable error: %v", err)
}

func (t *AutoPartitionCronTask) OnFail(_ context.Context, err error) {
	log.Printf("[SCHEDULER] AutoPartitionCronTask fatal failure: %v", err)
}

// PeriodicSyncCronTask is the top-global handler for periodic background disk flushing.
type PeriodicSyncCronTask struct {
	server   *Server
	schedule string
}

func NewPeriodicSyncCronTask(s *Server, schedule string) *PeriodicSyncCronTask {
	return &PeriodicSyncCronTask{
		server:   s,
		schedule: schedule,
	}
}

func (t *PeriodicSyncCronTask) Schedule() string {
	return t.schedule
}

func (t *PeriodicSyncCronTask) Execute(ctx context.Context) error {
	return dispatchAndWait(
		ctx,
		t.server.worker,
		"periodic_disk_sync",
		func(taskCtx context.Context) error {
			names := t.server.allModelNames()

			var synced int
			var purged int
			var syncErrs []error
			for _, name := range names {
				if err := taskCtx.Err(); err != nil {
					syncErrs = append(syncErrs, err)
					break
				}
				d, release, err := t.server.acquireExistingModel(name)
				if err != nil {
					if !errors.Is(err, errModelNotFound) && !errors.Is(err, ErrServerClosed) {
						syncErrs = append(syncErrs, err)
					}
					continue
				}
				collected, err := d.CollectTombstones(5000)
				purged += collected
				if err != nil {
					syncErrs = append(syncErrs, err)
				}
				if err := d.Sync(); err != nil {
					syncErrs = append(syncErrs, err)
				} else {
					synced++
				}
				release()
			}
			if purged > 0 || synced > 0 {
				log.Printf("[WORKER] Periodic background sync succeeded: %d models synced to disk, %d tombstones collected", synced, purged)
			}
			return errors.Join(syncErrs...)
		},
		worker.WithRetry(1, 200*time.Millisecond),
	)
}

func (t *PeriodicSyncCronTask) OnError(_ context.Context, err error) {
	log.Printf("[SCHEDULER] PeriodicSyncCronTask recoverable error: %v", err)
}

func (t *PeriodicSyncCronTask) OnFail(_ context.Context, err error) {
	log.Printf("[SCHEDULER] PeriodicSyncCronTask fatal failure: %v", err)
}

// sanitizeModelName enforces alphanumeric model names, max length, and prevents path traversal
func sanitizeModelName(name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "default", nil
	}
	if len(name) > 64 {
		return "", fmt.Errorf("model name too long (max 64 characters): %q", name)
	}
	cleanName := filepath.Base(name)
	if cleanName != name || strings.Contains(name, "..") || strings.Contains(name, "/") || strings.Contains(name, "\\") {
		return "", fmt.Errorf("invalid model name (path traversal rejected): %q", name)
	}
	for _, ch := range name {
		if !isAllowedNameChar(ch, false) {
			return "", fmt.Errorf("invalid character %q in model name: %q", ch, name)
		}
	}
	return name, nil
}

// isAllowedNameChar reports whether ch is a safe character for a model name or
// identifier: ASCII letters, digits, underscore and hyphen, plus dot when
// allowDot is set.
func isAllowedNameChar(ch rune, allowDot bool) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') ||
		ch == '_' || ch == '-' || (allowDot && ch == '.')
}

// isValidIdentifier checks that an identifier consists only of safe characters (letters, digits, underscore, dot, hyphen)
func isValidIdentifier(token string) bool {
	if len(token) == 0 || len(token) > 64 {
		return false
	}
	for _, ch := range token {
		if !isAllowedNameChar(ch, true) {
			return false
		}
	}
	return true
}

var errModelNotFound = errors.New("model does not exist")

func (s *Server) touchModelOrder(modelName string) {
	s.lruMu.Lock()
	defer s.lruMu.Unlock()
	if s.modelLRU == nil {
		s.modelLRU = list.New()
	}
	if s.modelElems == nil {
		s.modelElems = make(map[string]*list.Element)
	}
	if elem, ok := s.modelElems[modelName]; ok {
		s.modelLRU.MoveToFront(elem)
		return
	}
	s.modelElems[modelName] = s.modelLRU.PushFront(modelName)
}

func makeLeaseRelease(pin *atomic.Int64) func() {
	if pin == nil {
		return func() {}
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			pin.Add(-1)
		})
	}
}

func (s *Server) ensureModelPinLocked(modelName string) *atomic.Int64 {
	if s.modelPins == nil {
		s.modelPins = make(map[string]*atomic.Int64)
	}
	pin, ok := s.modelPins[modelName]
	if !ok {
		pin = &atomic.Int64{}
		s.modelPins[modelName] = pin
	}
	return pin
}

func (s *Server) pickIdleVictimLocked(skip map[string]bool) (string, *storage.AnchorDB) {
	s.lruMu.Lock()
	if s.modelLRU != nil {
		for elem := s.modelLRU.Back(); elem != nil; elem = elem.Prev() {
			candidate, _ := elem.Value.(string)
			if skip[candidate] {
				continue
			}
			vdb, ok := s.models[candidate]
			if !ok {
				continue
			}
			if pin := s.modelPins[candidate]; pin != nil && pin.Load() > 0 {
				continue
			}
			s.modelLRU.Remove(elem)
			delete(s.modelElems, candidate)
			s.lruMu.Unlock()
			return candidate, vdb
		}
	}
	s.lruMu.Unlock()

	// Defensive fallback if modelLRU drifted out of sync with s.models.
	var victim string
	for k := range s.models {
		if skip[k] {
			continue
		}
		if pin := s.modelPins[k]; pin != nil && pin.Load() > 0 {
			continue
		}
		if victim == "" || k < victim {
			victim = k
		}
	}
	if victim == "" {
		return "", nil
	}
	return victim, s.models[victim]
}

// evictIdleModelWithUnlockLocked selects one idle (unpinned) LRU model, removes it
// from the active registry, and runs Sync+Close outside s.mu so other requests are
// not stalled during disk flush. Caller must hold s.mu.Lock() and continues holding
// s.mu.Lock() upon return.
func (s *Server) evictIdleModelWithUnlockLocked(skip map[string]bool) bool {
	victim, vdb := s.pickIdleVictimLocked(skip)
	if victim == "" || vdb == nil {
		return false
	}

	delete(s.models, victim)
	delete(s.modelPins, victim)
	if s.modelDraining == nil {
		s.modelDraining = make(map[string]chan struct{})
	}
	done := make(chan struct{})
	s.modelDraining[victim] = done
	s.mu.Unlock()

	syncErr := vdb.Sync()
	if syncErr != nil {
		log.Printf("[WARN] Failed to sync evicted LRU model %q (keeping handle open): %v", victim, syncErr)
		s.mu.Lock()
		delete(s.modelDraining, victim)
		close(done)
		if !s.closed && s.models[victim] == nil {
			s.models[victim] = vdb
			s.ensureModelPinLocked(victim)
			s.touchModelOrder(victim)
		}
		skip[victim] = true
		return true
	}

	if err := vdb.Close(); err != nil {
		log.Printf("[WARN] Failed to close evicted LRU model %q: %v", victim, err)
	}

	s.mu.Lock()
	delete(s.modelDraining, victim)
	close(done)
	return true
}

func (s *Server) acquireExistingModel(modelName string) (*storage.AnchorDB, func(), error) {
	return s.acquireExistingModelInternal(modelName, true)
}

func (s *Server) acquireExistingModelInternal(modelName string, pinned bool) (*storage.AnchorDB, func(), error) {
	cleanName, err := sanitizeModelName(modelName)
	if err != nil {
		return nil, nil, err
	}
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return nil, nil, ErrServerClosed
	}
	db, exists := s.models[cleanName]
	var pin *atomic.Int64
	if exists && pinned {
		pin = s.modelPins[cleanName]
		if pin != nil {
			pin.Add(1)
		}
	}
	s.mu.RUnlock()
	if exists && (!pinned || pin != nil) {
		s.touchModelOrder(cleanName)
		if !pinned {
			return db, func() {}, nil
		}
		return db, makeLeaseRelease(pin), nil
	}
	dbPath := filepath.Join(s.dataDir, cleanName+".anchordb")
	if _, statErr := os.Stat(dbPath); errors.Is(statErr, os.ErrNotExist) {
		return nil, nil, errModelNotFound
	}
	return s.acquireModelInternal(cleanName, pinned)
}

func closePartitionDB(pdb *storage.AnchorDB, label string) {
	if err := pdb.Close(); err != nil {
		log.Printf("[WARN] Failed closing partition DB (%s): %v", label, err)
	}
}

func (s *Server) allModelNames() []string {
	seen := make(map[string]struct{})
	s.mu.RLock()
	for m := range s.models {
		seen[m] = struct{}{}
	}
	dataDir := s.dataDir
	s.mu.RUnlock()

	if dataDir != "" {
		entries, err := os.ReadDir(dataDir)
		if err != nil {
			log.Printf("[WARN] Failed scanning data directory %s for models: %v", dataDir, err)
		} else {
			for _, e := range entries {
				if e.IsDir() || !strings.HasSuffix(e.Name(), ".anchordb") {
					continue
				}
				name := strings.TrimSuffix(e.Name(), ".anchordb")
				if name != "" {
					seen[name] = struct{}{}
				}
			}
		}
	}
	names := make([]string, 0, len(seen))
	for m := range seen {
		names = append(names, m)
	}
	sort.Strings(names)
	return names
}

func (s *Server) acquireModel(modelName string) (*storage.AnchorDB, func(), error) {
	return s.acquireModelInternal(modelName, true)
}

// getModel retrieves or dynamically opens an AnchorDB instance ("One DB = One Model")
func (s *Server) getModel(modelName string) (*storage.AnchorDB, error) {
	db, _, err := s.acquireModelInternal(modelName, false)
	return db, err
}

type redactedPathError struct {
	msg string
	err error
}

func (e *redactedPathError) Error() string { return e.msg }
func (e *redactedPathError) Unwrap() error { return e.err }

func (s *Server) acquireModelInternal(modelName string, pinned bool) (*storage.AnchorDB, func(), error) {
	cleanName, err := sanitizeModelName(modelName)
	if err != nil {
		return nil, nil, err
	}
	modelName = cleanName

	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return nil, nil, ErrServerClosed
	}
	db, exists := s.models[modelName]
	var pin *atomic.Int64
	if exists && pinned {
		pin = s.modelPins[modelName]
		if pin != nil {
			pin.Add(1)
		}
	}
	s.mu.RUnlock()
	if exists && (!pinned || pin != nil) {
		s.touchModelOrder(modelName)
		if !pinned {
			return db, func() {}, nil
		}
		return db, makeLeaseRelease(pin), nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	skip := make(map[string]bool)
	for {
		// After Shutdown closed every model the registry is sealed: a late handler
		// must not open a new DB that would never be Synced/Closed (L11).
		if s.closed {
			return nil, nil, ErrServerClosed
		}
		if waitCh, draining := s.modelDraining[modelName]; draining {
			s.mu.Unlock()
			<-waitCh
			s.mu.Lock()
			continue
		}
		if db, exists := s.models[modelName]; exists {
			pin := s.ensureModelPinLocked(modelName)
			if pinned {
				pin.Add(1)
			}
			s.touchModelOrder(modelName)
			if !pinned {
				return db, func() {}, nil
			}
			return db, makeLeaseRelease(pin), nil
		}

		const maxLoadedModels = 500
		if len(s.models) >= maxLoadedModels {
			if s.evictIdleModelWithUnlockLocked(skip) {
				continue
			}
		}
		break
	}

	dbPath := filepath.Join(s.dataDir, modelName+".anchordb")
	newDB, err := storage.OpenWithOptions(dbPath, s.dbOptions)
	if err != nil {
		// Log full detail (including the on-disk path) server-side only; preserve
		// sentinel error identity (e.g. ErrWALCorrupt) without disclosing dbPath.
		log.Printf("[ERROR] Failed to open model %q at %s: %v", modelName, dbPath, err)
		redacted := strings.ReplaceAll(err.Error(), dbPath, modelName)
		return nil, nil, &redactedPathError{
			msg: fmt.Sprintf("failed to open model %q: %s", modelName, redacted),
			err: err,
		}
	}

	s.models[modelName] = newDB
	pin = s.ensureModelPinLocked(modelName)
	if pinned {
		pin.Add(1)
	}
	s.touchModelOrder(modelName)
	log.Printf("[INFO] Model %q loaded from disk: %s", modelName, dbPath)
	if !pinned {
		return newDB, func() {}, nil
	}
	return newDB, makeLeaseRelease(pin), nil
}

// isAuthorized reports whether the request carries a valid bearer token. When no
// token is configured (explicit insecure mode) every request is authorized.
// In gateway mode, direct HTTP access is always unauthorized.
func (s *Server) isAuthorized(r *http.Request) bool {
	if s.gatewayMode {
		return false
	}
	if s.authToken == "" {
		return true
	}
	authHeader := r.Header.Get("Authorization")
	expected := "Bearer " + s.authToken
	return subtle.ConstantTimeCompare([]byte(authHeader), []byte(expected)) == 1
}

func (s *Server) authMiddleware(next http.HandlerFunc) http.HandlerFunc {
	// The request body is only buffered *after* authentication and rate limiting
	// succeed, so unauthenticated clients cannot force large allocations.
	limited := limitRequestBody(next)
	return func(w http.ResponseWriter, r *http.Request) {
		if s.gatewayMode {
			writeJSON(w, http.StatusForbidden, map[string]interface{}{
				"success": false,
				"error":   "gateway_mode_enforced",
				"message": "direct HTTP queries are disabled in gateway mode; all requests must route through anchor-connector",
			})
			return
		}
		if !s.isAuthorized(r) {
			writeJSONError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		limited.ServeHTTP(w, r)
	}
}

// Health check endpoint
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	headroom := system.GetMemoryHeadroom()
	status, code := "ok", http.StatusOK
	if s.draining.Load() {
		// Readiness: tell load balancers / K8s to stop routing new traffic.
		status, code = "shutting_down", http.StatusServiceUnavailable
	} else if s.worker.IsBlocked() || s.scheduler.IsBlocked() || (s.wireServer != nil && s.wireServer.IsClosed()) || (headroom.LimitKnown && headroom.UsedPercent >= 93) {
		status, code = "degraded", http.StatusServiceUnavailable
	}

	// Unauthenticated callers only get liveness/readiness. Internal state
	// (loaded model names, memory, scheduler/worker stats) requires auth.
	if !s.isAuthorized(r) {
		writeJSON(w, code, map[string]interface{}{
			"status": status,
			"engine": "AnchorDB",
		})
		return
	}

	s.mu.RLock()
	var loadedModels []string
	for k := range s.models {
		loadedModels = append(loadedModels, k)
	}
	s.mu.RUnlock()

	memSample := s.readMemStatsCached()

	resp := map[string]interface{}{
		"status":         status,
		"engine":         "AnchorDB",
		"uptime_seconds": time.Since(s.startTime).Seconds(),
		"port":           s.port,
		"loaded_models":  loadedModels,
		"memory":         system.GetMemoryStatsFrom(memSample),
		"background": map[string]interface{}{
			"scheduler": map[string]interface{}{
				"enabled": s.cronEnabled,
				"blocked": s.scheduler.IsBlocked(),
				"tasks":   len(s.scheduler.Tasks()),
			},
			"worker":         s.worker.StatsFromMemStats(memSample),
			"worker_blocked": s.worker.IsBlocked(),
		},
	}
	writeJSON(w, code, resp)
}

const memStatsCacheTTL = 2 * time.Second

func (s *Server) readMemStatsCached() runtime.MemStats {
	s.memStatsMu.Lock()
	defer s.memStatsMu.Unlock()
	if !s.memStatsAt.IsZero() && time.Since(s.memStatsAt) < memStatsCacheTTL {
		return s.memStatsCached
	}
	var sample runtime.MemStats
	runtime.ReadMemStats(&sample)
	s.memStatsCached = sample
	s.memStatsAt = time.Now()
	return sample
}

// handleMetrics exposes dependency-free Prometheus text-format metrics for
// scraping using a cached MemStats snapshot to avoid STW pauses on scrape floods.
func (s *Server) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	s.mu.RLock()
	models := len(s.models)
	s.mu.RUnlock()

	memSample := s.readMemStatsCached()
	mem := system.GetMemoryStatsFrom(memSample)
	ws := s.worker.StatsFromMemStats(memSample)

	draining := 0
	if s.draining.Load() {
		draining = 1
	}

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.WriteHeader(http.StatusOK)

	var b strings.Builder
	fmt.Fprintf(&b, "# HELP anchordb_up Whether the server process is running.\n")
	fmt.Fprintf(&b, "# TYPE anchordb_up gauge\nanchordb_up 1\n")
	fmt.Fprintf(&b, "# HELP anchordb_uptime_seconds Seconds since process start.\n")
	fmt.Fprintf(&b, "# TYPE anchordb_uptime_seconds gauge\nanchordb_uptime_seconds %.3f\n", time.Since(s.startTime).Seconds())
	fmt.Fprintf(&b, "# HELP anchordb_models_loaded Number of currently open model databases.\n")
	fmt.Fprintf(&b, "# TYPE anchordb_models_loaded gauge\nanchordb_models_loaded %d\n", models)
	fmt.Fprintf(&b, "# HELP anchordb_draining Whether the server is draining (1) or serving (0).\n")
	fmt.Fprintf(&b, "# TYPE anchordb_draining gauge\nanchordb_draining %d\n", draining)
	fmt.Fprintf(&b, "# HELP anchordb_go_goroutines Number of live goroutines.\n")
	fmt.Fprintf(&b, "# TYPE anchordb_go_goroutines gauge\nanchordb_go_goroutines %d\n", runtime.NumGoroutine())
	fmt.Fprintf(&b, "# HELP anchordb_heap_alloc_bytes Heap bytes currently allocated.\n")
	fmt.Fprintf(&b, "# TYPE anchordb_heap_alloc_bytes gauge\nanchordb_heap_alloc_bytes %d\n", mem.HeapAllocBytes)
	fmt.Fprintf(&b, "# HELP anchordb_heap_sys_bytes Total heap bytes obtained from the OS.\n")
	fmt.Fprintf(&b, "# TYPE anchordb_heap_sys_bytes gauge\nanchordb_heap_sys_bytes %d\n", mem.HeapSysBytes)
	fmt.Fprintf(&b, "# HELP anchordb_gc_cycles_total Completed GC cycles.\n")
	fmt.Fprintf(&b, "# TYPE anchordb_gc_cycles_total counter\nanchordb_gc_cycles_total %d\n", mem.NumGC)
	fmt.Fprintf(&b, "# HELP anchordb_memory_limit_bytes Soft memory limit in bytes (0 = unlimited).\n")
	fmt.Fprintf(&b, "# TYPE anchordb_memory_limit_bytes gauge\nanchordb_memory_limit_bytes %d\n", mem.LimitBytes)
	fmt.Fprintf(&b, "# HELP anchordb_worker_jobs_dispatched_total Worker jobs dispatched.\n")
	fmt.Fprintf(&b, "# TYPE anchordb_worker_jobs_dispatched_total counter\nanchordb_worker_jobs_dispatched_total %d\n", ws.Dispatched)
	fmt.Fprintf(&b, "# HELP anchordb_worker_jobs_completed_total Worker jobs completed.\n")
	fmt.Fprintf(&b, "# TYPE anchordb_worker_jobs_completed_total counter\nanchordb_worker_jobs_completed_total %d\n", ws.Completed)
	fmt.Fprintf(&b, "# HELP anchordb_worker_jobs_failed_total Worker jobs failed.\n")
	fmt.Fprintf(&b, "# TYPE anchordb_worker_jobs_failed_total counter\nanchordb_worker_jobs_failed_total %d\n", ws.Failed)
	fmt.Fprintf(&b, "# HELP anchordb_worker_jobs_in_flight Worker jobs currently in flight.\n")
	fmt.Fprintf(&b, "# TYPE anchordb_worker_jobs_in_flight gauge\nanchordb_worker_jobs_in_flight %d\n", ws.InFlight)

	if _, err := io.WriteString(w, b.String()); err != nil {
		log.Printf("[WARN] Failed writing /metrics response: %v", err)
	}
}

// Request / Response types for REST API
type SaveRequest struct {
	Model     string              `json:"model"`
	Name      string              `json:"name"`
	Operation *uint8              `json:"operation,omitempty"`
	Props     map[string]float32  `json:"props"`
	Attrs     []storage.AttrEntry `json:"attrs,omitempty"`
	Coord     *spatial.Point2D    `json:"coord,omitempty"`
	Roles     *[4]float32         `json:"roles,omitempty"`
}

type SaveBatchRequest struct {
	Model   string                    `json:"model"`
	Records []storage.BatchRecordItem `json:"records"`
}

type GetRequest struct {
	Model        string            `json:"model"`
	Name         string            `json:"name"`
	Select       []string          `json:"select"`
	Computations map[string]string `json:"computations"` // e.g. {"speed": "distance / duration"}
}

type FilterDTO struct {
	Field     string `json:"field"`
	Operator  string `json:"operator"` // "=", "!=", ">", ">=", "<", "<=", "BETWEEN"
	Value     string `json:"value"`
	ValueHigh string `json:"value_high"` // For "BETWEEN"
}

type ListRequest struct {
	Model                string            `json:"model"`
	Select               []string          `json:"select"`
	Filters              []FilterDTO       `json:"filters"`
	Computations         map[string]string `json:"computations"`
	Limit                int               `json:"limit"`
	Offset               int               `json:"offset"`
	Page                 int               `json:"page"`
	IncludeAllOperations bool              `json:"include_all_operations,omitempty"`
	Operation            *uint8            `json:"operation,omitempty"`
	AllowedOperations    []uint8           `json:"allowed_operations,omitempty"`
}

type UpdateRequest struct {
	Model     string            `json:"model"`
	In        []string          `json:"in"`
	Filters   []FilterDTO       `json:"filters"`
	Mutations map[string]string `json:"mutations"` // e.g. {"distance": "distance + 10"}
	Limit     int               `json:"limit"`
	Offset    int               `json:"offset"`
	Async     bool              `json:"async,omitempty"`
}

type GraphRequest struct {
	Model     string              `json:"model"`
	Inputs    []string            `json:"inputs"`
	Relation  string              `json:"relation"`
	Relations []string            `json:"relations,omitempty"`
	BeamWidth float32             `json:"beam_width,omitempty"`
	LimitK    int                 `json:"limit_k"`
	OrderBy   storage.OrderByMode `json:"order_by,omitempty"`
}

// EdgeRequest registers or removes an explicit directed edge
// (source --relation--> target) used for cause-and-effect links,
// or configures directional vector relations ("set_relation").
type EdgeRequest struct {
	Model     string           `json:"model"`
	Action    string           `json:"action"` // "add", "remove", "set_relation"
	Source    string           `json:"source"`
	Relation  string           `json:"relation"`
	Target    string           `json:"target"`
	Weight    float32          `json:"weight"`
	Direction *spatial.Point2D `json:"direction,omitempty"`
}

// CausalRequest traverses explicit edges. A single relation performs one hop;
// a relations list performs a weighted multi-hop chain.
type CausalRequest struct {
	Model     string   `json:"model"`
	Seeds     []string `json:"seeds"`
	Relation  string   `json:"relation"`
	Relations []string `json:"relations"`
	LimitK    int      `json:"limit_k"`
}

// MathRequest triggers direct numerical and geometric operations on the graph.
type MathRequest struct {
	Model  string   `json:"model"`
	Op     string   `json:"op"` // "distance", "centroid", "cosine_similarity", "nearest", "haversine", "stats"
	Words  []string `json:"words"`
	Field  string   `json:"field,omitempty"`
	LimitK int      `json:"limit_k,omitempty"`
}

// parseComputeOp converts a string expression like "distance / duration", "distance + 10", "0", or "-5" to ComputeOp
func parseComputeOp(expr string) (storage.ComputeOp, error) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil, fmt.Errorf("empty compute expression")
	}

	// 1. Check for single literal assignment (e.g. "0", "-5", "100.5") or direct field alias
	if _, err := strconv.ParseFloat(expr, 64); err == nil {
		return storage.OpAssign{Value: expr}, nil
	}
	if isValidIdentifier(expr) {
		return storage.OpAssign{Value: expr}, nil
	}

	// 2. Binary operations: check for binary operators not at index 0 (allowing unary negative fieldA)
	var opIdx = -1
	var opChar = ""
	for _, op := range []string{"/", "*", "+", "-"} {
		idx := strings.Index(expr, op)
		if idx > 0 {
			opIdx = idx
			opChar = op
			break
		}
	}

	if opIdx != -1 {
		fieldA := strings.TrimSpace(expr[:opIdx])
		fieldB := strings.TrimSpace(expr[opIdx+1:])
		if !isValidIdentifier(fieldA) {
			return nil, fmt.Errorf("invalid identifier in field A: %q", fieldA)
		}
		if !isValidIdentifier(fieldB) {
			return nil, fmt.Errorf("invalid identifier in field B: %q", fieldB)
		}
		switch opChar {
		case "/":
			return driver.Divide(fieldA, fieldB), nil
		case "*":
			return driver.Multiply(fieldA, fieldB), nil
		case "+":
			return driver.Add(fieldA, fieldB), nil
		case "-":
			return driver.Subtract(fieldA, fieldB), nil
		}
	}

	return nil, fmt.Errorf("unsupported or malformed expression %q", expr)
}

func parseFilterVal(str string) (v32 float32, v64 float64, vInt64 int64, vUint64 uint64, valType storage.AttrType, has64 bool, err error) {
	if str == "" {
		return 0, 0, 0, 0, storage.AttrTypeFloat32, false, nil
	}
	if d, err := storage.ParseDateInt(str); err == nil {
		var u uint64
		if d >= 0 {
			u = uint64(d)
		}
		return float32(d), float64(d), d, u, storage.AttrTypeInt64, true, nil
	}
	if i, err := strconv.ParseInt(str, 10, 64); err == nil {
		var u uint64
		if i >= 0 {
			u = uint64(i)
		}
		return float32(i), float64(i), i, u, storage.AttrTypeInt64, true, nil
	}
	if u, err := strconv.ParseUint(str, 10, 64); err == nil {
		var i int64
		if u <= math.MaxInt64 {
			i = int64(u)
		}
		return float32(u), float64(u), i, u, storage.AttrTypeUint64, true, nil
	}
	if f, err := strconv.ParseFloat(str, 64); err == nil {
		return float32(f), f, 0, 0, storage.AttrTypeFloat64, true, nil
	}
	return 0, 0, 0, 0, storage.AttrTypeFloat32, false, fmt.Errorf("invalid numeric value %q", str)
}

// buildFilters converts API filter DTOs into storage filter conditions,
// rejecting any value that is not a valid number or date.
func buildFilters(filters []FilterDTO) ([]storage.FilterCondition, error) {
	out := make([]storage.FilterCondition, 0, len(filters))
	for _, f := range filters {
		v32, v64, vInt64, vUint64, valType, has64, err := parseFilterVal(f.Value)
		if err != nil {
			return nil, fmt.Errorf("filter field %q: %w", f.Field, err)
		}
		vh32, vh64, vhInt64, vhUint64, vhType, hasHigh64, err := parseFilterVal(f.ValueHigh)
		if err != nil {
			return nil, fmt.Errorf("filter field %q (value_high): %w", f.Field, err)
		}
		condType := valType
		condHighType := vhType
		hasHighType := false
		if f.Operator == "BETWEEN" && f.ValueHigh != "" {
			lowIsInt := valType == storage.AttrTypeInt64 || valType == storage.AttrTypeUint64
			highIsInt := vhType == storage.AttrTypeInt64 || vhType == storage.AttrTypeUint64
			if lowIsInt && highIsInt {
				if valType != vhType {
					if valType == storage.AttrTypeInt64 && vInt64 >= 0 {
						vUint64 = uint64(vInt64)
						condType = storage.AttrTypeUint64
						condHighType = storage.AttrTypeUint64
					} else if vhType == storage.AttrTypeInt64 && vhInt64 >= 0 {
						vhUint64 = uint64(vhInt64)
						condType = storage.AttrTypeUint64
						condHighType = storage.AttrTypeUint64
					} else if valType == storage.AttrTypeUint64 && vUint64 <= math.MaxInt64 {
						vInt64 = int64(vUint64)
						condType = storage.AttrTypeInt64
						condHighType = storage.AttrTypeInt64
					} else if vhType == storage.AttrTypeUint64 && vhUint64 <= math.MaxInt64 {
						vhInt64 = int64(vhUint64)
						condType = storage.AttrTypeInt64
						condHighType = storage.AttrTypeInt64
					} else {
						condType = valType
						condHighType = vhType
						hasHighType = true
					}
				}
			} else if valType == storage.AttrTypeFloat64 || vhType == storage.AttrTypeFloat64 || (lowIsInt != highIsInt) {
				condType = storage.AttrTypeFloat64
				condHighType = storage.AttrTypeFloat64
				has64 = true
			}
		} else if condType == storage.AttrTypeFloat32 && vhType != storage.AttrTypeFloat32 {
			condType = vhType
		}
		out = append(out, storage.FilterCondition{
			Field:           f.Field,
			Operator:        f.Operator,
			Value:           v32,
			ValueHigh:       vh32,
			Value64:         v64,
			ValueHigh64:     vh64,
			HasFloat64:      has64 || hasHigh64,
			ValueInt64:      vInt64,
			ValueHighInt64:  vhInt64,
			ValueUint64:     vUint64,
			ValueHighUint64: vhUint64,
			ValType:         condType,
			ValHighType:     condHighType,
			HasHighType:     hasHighType,
		})
	}
	return out, nil
}

func (s *Server) handleSave(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	var req SaveRequest
	if !decodeJSONBody(w, r, &req, false) {
		return
	}

	db, release, err := s.acquireModel(req.Model)
	if err != nil {
		writeJSONError(w, dispatchErrorStatus(err), err.Error())
		return
	}
	defer release()

	if !checkMemoryWriteAdmission(w, db) {
		return
	}

	opStatus := storage.OpStatusActive
	if req.Operation != nil {
		opStatus = *req.Operation
	}

	effectiveName := req.Name
	if req.Coord != nil || req.Roles != nil {
		coord := spatial.Point2D{}
		if req.Coord != nil {
			coord = *req.Coord
		}
		var roles [4]float32
		if req.Roles != nil {
			roles = *req.Roles
		}
		if len(req.Attrs) > 0 {
			name, err := db.SaveRecordTypedWithNode(effectiveName, opStatus, req.Attrs, coord, roles)
			if err != nil {
				if errors.Is(err, storage.ErrMissingRequiredField) ||
					errors.Is(err, storage.ErrUnknownField) ||
					errors.Is(err, storage.ErrInvalidFieldType) ||
					errors.Is(err, storage.ErrSchemaMismatch) ||
					errors.Is(err, storage.ErrRecordLocked) {
					writeJSONError(w, http.StatusBadRequest, err.Error())
					return
				}
				writeJSONError(w, dispatchErrorStatus(err), err.Error())
				return
			}
			effectiveName = name
		} else {
			name, err := db.SaveRecordWithNode(effectiveName, opStatus, req.Props, coord, roles)
			if err != nil {
				if errors.Is(err, storage.ErrMissingRequiredField) ||
					errors.Is(err, storage.ErrUnknownField) ||
					errors.Is(err, storage.ErrInvalidFieldType) ||
					errors.Is(err, storage.ErrSchemaMismatch) ||
					errors.Is(err, storage.ErrRecordLocked) {
					writeJSONError(w, http.StatusBadRequest, err.Error())
					return
				}
				writeJSONError(w, dispatchErrorStatus(err), err.Error())
				return
			}
			effectiveName = name
		}
	} else {
		if len(req.Attrs) > 0 {
			name, err := db.SaveRecordTypedWithIDAndOp(req.Name, opStatus, req.Attrs)
			if err != nil {
				if errors.Is(err, storage.ErrMissingRequiredField) ||
					errors.Is(err, storage.ErrUnknownField) ||
					errors.Is(err, storage.ErrInvalidFieldType) ||
					errors.Is(err, storage.ErrSchemaMismatch) ||
					errors.Is(err, storage.ErrRecordLocked) {
					writeJSONError(w, http.StatusBadRequest, err.Error())
					return
				}
				writeJSONError(w, dispatchErrorStatus(err), err.Error())
				return
			}
			effectiveName = name
		} else {
			name, err := db.SaveRecordWithIDAndOp(req.Name, opStatus, req.Props)
			if err != nil {
				if errors.Is(err, storage.ErrMissingRequiredField) ||
					errors.Is(err, storage.ErrUnknownField) ||
					errors.Is(err, storage.ErrInvalidFieldType) ||
					errors.Is(err, storage.ErrSchemaMismatch) ||
					errors.Is(err, storage.ErrRecordLocked) {
					writeJSONError(w, http.StatusBadRequest, err.Error())
					return
				}
				writeJSONError(w, dispatchErrorStatus(err), err.Error())
				return
			}
			effectiveName = name
		}
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success":   true,
		"name":      effectiveName,
		"operation": opStatus,
		"message":   "record saved",
	})
}

func (s *Server) handleSaveBatch(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	var req SaveBatchRequest
	if !decodeJSONBody(w, r, &req, false) {
		return
	}
	if len(req.Records) == 0 {
		writeJSONError(w, http.StatusBadRequest, "records array cannot be empty")
		return
	}

	db, release, err := s.acquireModel(req.Model)
	if err != nil {
		writeJSONError(w, dispatchErrorStatus(err), err.Error())
		return
	}
	defer release()

	if !checkMemoryWriteAdmission(w, db) {
		return
	}

	names, err := db.SaveRecordsBatch(req.Records)
	if err != nil {
		if errors.Is(err, storage.ErrMissingRequiredField) ||
			errors.Is(err, storage.ErrUnknownField) ||
			errors.Is(err, storage.ErrInvalidFieldType) ||
			errors.Is(err, storage.ErrSchemaMismatch) ||
			errors.Is(err, storage.ErrRecordLocked) {
			writeJSONError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeJSONError(w, dispatchErrorStatus(err), err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"count":   len(names),
		"names":   names,
		"message": "batch records saved",
	})
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost, http.MethodGet) {
		return
	}
	var req GetRequest
	if r.Method == http.MethodGet {
		req.Model = r.URL.Query().Get("model")
		if req.Model == "" {
			req.Model = "default"
		}
		req.Name = r.URL.Query().Get("name")
	} else {
		if !decodeJSONBody(w, r, &req, false) {
			return
		}
	}

	if req.Name == "" {
		writeJSONError(w, http.StatusBadRequest, "name is required")
		return
	}

	computations := make(map[string]storage.ComputeOp)
	for outField, expr := range req.Computations {
		opCompute, err := parseComputeOp(expr)
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("invalid computation %q for field %q: %s", expr, outField, err.Error()))
			return
		}
		computations[outField] = opCompute
	}

	// 1. Try Hot DB first (read-only lookup avoids creating empty models on disk)
	db, release, err := s.acquireExistingModel(req.Model)
	if err != nil && !errors.Is(err, errModelNotFound) {
		writeJSONError(w, dispatchErrorStatus(err), err.Error())
		return
	}
	if err == nil {
		cr, gErr := db.GetRecordTypedWithCompute(req.Name, computations, req.Select...)
		release()
		if gErr == nil {
			props, pErr := cr.ToMap()
			if pErr != nil {
				writeJSONError(w, dispatchErrorStatus(pErr), pErr.Error())
				return
			}
			attrs, aErr := cr.ExpandAttrs()
			if aErr != nil {
				writeJSONError(w, dispatchErrorStatus(aErr), aErr.Error())
				return
			}
			writeJSON(w, http.StatusOK, map[string]interface{}{
				"success":   true,
				"name":      req.Name,
				"operation": cr.Operation,
				"props":     props,
				"attrs":     attrs,
				"tier":      "hot",
			})
			return
		}
		// Only a missing record may fall through to cold tier. Any other hot
		// failure keeps its own status instead of masquerading as 404.
		if !errors.Is(gErr, storage.ErrRecordNotFound) {
			writeJSONError(w, dispatchErrorStatus(gErr), gErr.Error())
			return
		}
	}

	// 2. Transparent Cold Storage Fallback (not-found only)
	cr, coldErr := s.GetColdRecordTyped(req.Model, req.Name, computations, req.Select)
	if coldErr != nil && !errors.Is(coldErr, storage.ErrRecordNotFound) {
		writeJSONError(w, dispatchErrorStatus(coldErr), coldErr.Error())
		return
	}
	if coldErr == nil {
		props, pErr := cr.ToMap()
		if pErr != nil {
			writeJSONError(w, dispatchErrorStatus(pErr), pErr.Error())
			return
		}
		attrs, aErr := cr.ExpandAttrs()
		if aErr != nil {
			writeJSONError(w, dispatchErrorStatus(aErr), aErr.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"success":   true,
			"name":      req.Name,
			"operation": cr.Operation,
			"props":     props,
			"attrs":     attrs,
			"tier":      "cold",
		})
		return
	}

	if errors.Is(coldErr, storage.ErrRecordNotFound) {
		writeJSONError(w, http.StatusNotFound, fmt.Sprintf("record %q not found in model %q (hot or cold tiers)", req.Name, req.Model))
		return
	}
	writeJSONError(w, dispatchErrorStatus(coldErr), coldErr.Error())
}

func (s *Server) handleList(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	var req ListRequest
	if !decodeJSONBody(w, r, &req, false) {
		return
	}

	db, release, err := s.acquireModel(req.Model)
	if err != nil {
		writeJSONError(w, dispatchErrorStatus(err), err.Error())
		return
	}
	defer release()

	computations := make(map[string]storage.ComputeOp)
	for outField, expr := range req.Computations {
		op, err := parseComputeOp(expr)
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("invalid computation %q for field %q: %s", expr, outField, err.Error()))
			return
		}
		computations[outField] = op
	}

	filters, err := buildFilters(req.Filters)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	if req.Limit < 0 {
		writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("limit %d cannot be negative", req.Limit))
		return
	}
	if req.Offset < 0 {
		writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("offset %d cannot be negative", req.Offset))
		return
	}
	if req.Page < 0 {
		writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("page %d cannot be negative", req.Page))
		return
	}

	limit := req.Limit
	if limit == 0 {
		limit = 10
	}
	if limit > maxQueryLimit {
		limit = maxQueryLimit
	}
	offset := req.Offset
	if req.Page > 1 {
		offset = (req.Page - 1) * limit
	}

	queryOpts := storage.QueryOptions{
		Context:              r.Context(),
		Select:               req.Select,
		Filters:              filters,
		Limit:                limit,
		Offset:               offset,
		Computations:         computations,
		IncludeAllOperations: req.IncludeAllOperations,
		OperationFilter:      req.Operation,
		AllowedOperations:    req.AllowedOperations,
	}
	records, hasMore, err := db.ListRecordsWithPaging(queryOpts)
	if err != nil {
		writeJSONError(w, dispatchErrorStatus(err), err.Error())
		return
	}
	tier := "hot"
	if len(records) == 0 {
		coldRecs, coldHasMore, coldErr := s.ListColdRecords(req.Model, queryOpts)
		if coldErr != nil {
			writeJSONError(w, dispatchErrorStatus(coldErr), coldErr.Error())
			return
		}
		if len(coldRecs) > 0 {
			records = coldRecs
			hasMore = coldHasMore
			tier = "cold"
		}
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success":  true,
		"items":    records,
		"has_more": hasMore,
		"count":    len(records),
		"tier":     tier,
	})
}

func (s *Server) handleUpdate(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	var req UpdateRequest
	if !decodeJSONBody(w, r, &req, false) {
		return
	}
	if req.Limit < 0 {
		writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("limit %d cannot be negative", req.Limit))
		return
	}
	if req.Offset < 0 {
		writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("offset %d cannot be negative", req.Offset))
		return
	}

	mutations := make(map[string]storage.ComputeOp)
	for field, expr := range req.Mutations {
		op, err := parseComputeOp(expr)
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("invalid mutation %q for field %q: %s", expr, field, err.Error()))
			return
		}
		mutations[field] = op
	}

	filters, err := buildFilters(req.Filters)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	isAsync := req.Async || r.URL.Query().Get("async") == "true"
	if isAsync {
		if _, err := sanitizeModelName(req.Model); err != nil {
			writeJSONError(w, dispatchErrorStatus(err), err.Error())
			return
		}
		jobID := uuid.New().String()
		err := s.worker.DispatchAsync(
			s.asyncBaseContext(),
			"update_"+req.Model,
			func(taskCtx context.Context) error {
				asyncDB, asyncRelease, err := s.acquireModel(req.Model)
				if err != nil {
					return err
				}
				defer asyncRelease()
				count, err := asyncDB.UpdateRecords(storage.UpdateOptions{
					Context:        taskCtx,
					TargetEntities: req.In,
					Filters:        filters,
					Mutations:      mutations,
					Limit:          req.Limit,
					Offset:         req.Offset,
				})
				if err == nil {
					log.Printf("[WORKER] Async batch update succeeded for model %q: %d records updated", req.Model, count)
				}
				return err
			},
			worker.WithID(jobID),
			worker.WithModel(req.Model),
			// Note: No retry on arithmetic mutations (+ - * /) to prevent duplicate compounding of non-idempotent updates.
		)
		if err != nil {
			writeJSONError(w, dispatchErrorStatus(err), fmt.Sprintf("failed to dispatch update to worker: %s", err.Error()))
			return
		}
		writeJSON(w, http.StatusAccepted, map[string]interface{}{
			"status":  "accepted",
			"job_id":  jobID,
			"model":   req.Model,
			"async":   true,
			"message": fmt.Sprintf("Bulk update for model %q accepted by worker pool", req.Model),
		})
		return
	}

	db, release, err := s.acquireModel(req.Model)
	if err != nil {
		writeJSONError(w, dispatchErrorStatus(err), err.Error())
		return
	}
	defer release()

	if !checkMemoryWriteAdmission(w, db) {
		return
	}

	updateOpts := storage.UpdateOptions{
		Context:        r.Context(),
		TargetEntities: req.In,
		Filters:        filters,
		Mutations:      mutations,
		Limit:          req.Limit,
		Offset:         req.Offset,
	}
	count, err := db.UpdateRecords(updateOpts)
	if err == nil && count == 0 {
		coldCount, coldErr := s.UpdateColdRecords(req.Model, updateOpts)
		if coldErr != nil {
			err = coldErr
		} else {
			count = coldCount
		}
	}
	if err != nil {
		if errors.Is(err, storage.ErrUnknownField) ||
			errors.Is(err, storage.ErrInvalidFieldType) ||
			errors.Is(err, storage.ErrMissingRequiredField) {
			writeJSONError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeJSONError(w, dispatchErrorStatus(err), err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success":       true,
		"updated_count": count,
	})
}

func (s *Server) handleGraph(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	var req GraphRequest
	if !decodeJSONBody(w, r, &req, false) {
		return
	}

	db, release, err := s.acquireModel(req.Model)
	if err != nil {
		writeJSONError(w, dispatchErrorStatus(err), err.Error())
		return
	}
	defer release()

	if req.LimitK < 0 {
		writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("limit_k %d cannot be negative", req.LimitK))
		return
	}
	if req.BeamWidth < 0 {
		writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("beam_width %g cannot be negative", req.BeamWidth))
		return
	}
	limit := req.LimitK
	if limit == 0 {
		limit = 5
	}
	beamWidth := req.BeamWidth
	if beamWidth == 0 {
		beamWidth = 0.05
	}

	if len(req.Relations) > 0 {
		steps, err := db.ExecuteGraphChainWithContext(r.Context(), req.Inputs, req.Relations, limit, beamWidth)
		if (err == nil && len(steps) == 0) || (err != nil && errors.Is(err, storage.ErrRecordNotFound)) {
			coldSteps, coldErr := s.ExecuteColdGraphChain(r.Context(), req.Model, wire.GraphChainRequest{
				Inputs:        req.Inputs,
				RelationChain: req.Relations,
				LimitK:        limit,
				BeamWidth:     beamWidth,
			})
			if coldErr == nil && len(coldSteps) > 0 {
				steps = coldSteps
				err = nil
			} else if coldErr != nil && !errors.Is(coldErr, storage.ErrRecordNotFound) {
				err = coldErr
			}
		}
		if err != nil {
			writeJSONError(w, dispatchErrorStatus(err), err.Error())
			return
		}
		seq := make([]string, len(steps))
		for i, s := range steps {
			seq[i] = s.Word
		}
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"success":  true,
			"sequence": seq,
			"steps":    steps,
		})
		return
	}

	orderMode := req.OrderBy
	if orderMode == 0 {
		orderMode = storage.OrderByCompositeDesc
	}
	results, anchor, err := db.ExecuteGraphStepWithContext(r.Context(), req.Inputs, req.Relation, limit, orderMode, beamWidth)
	if (err == nil && len(results) == 0) || (err != nil && errors.Is(err, storage.ErrRecordNotFound)) {
		coldRes, coldAnchor, coldErr := s.ExecuteColdGraph(r.Context(), req.Model, wire.GraphRequest{
			Inputs:    req.Inputs,
			Relation:  req.Relation,
			LimitK:    limit,
			OrderMode: uint8(orderMode),
			BeamWidth: beamWidth,
		})
		if coldErr == nil && len(coldRes) > 0 {
			results = coldRes
			anchor = coldAnchor
			err = nil
		} else if coldErr != nil && !errors.Is(coldErr, storage.ErrRecordNotFound) {
			err = coldErr
		}
	}
	if err != nil {
		writeJSONError(w, dispatchErrorStatus(err), err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"anchor":  anchor,
		"results": results,
	})
}

// handleEdge adds or removes an explicit causal edge or sets a directional relation vector.
func (s *Server) handleEdge(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	var req EdgeRequest
	if !decodeJSONBody(w, r, &req, false) {
		return
	}

	db, release, err := s.acquireModel(req.Model)
	if err != nil {
		writeJSONError(w, dispatchErrorStatus(err), err.Error())
		return
	}
	defer release()

	switch strings.ToLower(strings.TrimSpace(req.Action)) {
	case "", "add", "link":
		if !checkMemoryWriteAdmission(w, db) {
			return
		}
		if err := db.AddEdge(req.Source, req.Relation, req.Target, req.Weight); err != nil {
			writeJSONError(w, http.StatusBadRequest, err.Error())
			return
		}
	case "remove", "delete", "unlink":
		if err := db.RemoveEdge(req.Source, req.Relation, req.Target); err != nil {
			writeJSONError(w, http.StatusBadRequest, err.Error())
			return
		}
	case "set_relation", "relation":
		if !checkMemoryWriteAdmission(w, db) {
			return
		}
		if req.Direction == nil {
			writeJSONError(w, http.StatusBadRequest, "direction is required for set_relation")
			return
		}
		if err := db.SetRelation(req.Relation, *req.Direction); err != nil {
			writeJSONError(w, http.StatusBadRequest, err.Error())
			return
		}
	default:
		writeJSONError(w, http.StatusBadRequest, "action must be add, remove, or set_relation")
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success":    true,
		"edge_count": db.EdgeCount(),
	})
}

// handleCausal traverses explicit edges by relation, single-hop or multi-hop.
func (s *Server) handleCausal(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	var req CausalRequest
	if !decodeJSONBody(w, r, &req, false) {
		return
	}
	if req.LimitK < 0 {
		writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("limit_k %d cannot be negative", req.LimitK))
		return
	}

	db, release, err := s.acquireModel(req.Model)
	if err != nil {
		writeJSONError(w, dispatchErrorStatus(err), err.Error())
		return
	}
	defer release()

	limit := req.LimitK
	if limit == 0 {
		limit = 5
	}

	var results []storage.CausalResult
	if len(req.Relations) > 0 {
		results, err = db.ExecuteCausalChain(r.Context(), req.Seeds, req.Relations, limit)
		if (err == nil && len(results) == 0) || (err != nil && errors.Is(err, storage.ErrRecordNotFound)) {
			coldRes, coldErr := s.ExecuteColdCausalChain(r.Context(), req.Model, req.Seeds, req.Relations, limit)
			if coldErr == nil && len(coldRes) > 0 {
				results = coldRes
				err = nil
			} else if coldErr != nil && !errors.Is(coldErr, storage.ErrRecordNotFound) {
				err = coldErr
			}
		}
	} else {
		results, err = db.ExecuteCausalStep(r.Context(), req.Seeds, req.Relation, limit)
		if (err == nil && len(results) == 0) || (err != nil && errors.Is(err, storage.ErrRecordNotFound)) {
			coldRes, coldErr := s.ExecuteColdCausal(r.Context(), req.Model, req.Seeds, req.Relation, limit)
			if coldErr == nil && len(coldRes) > 0 {
				results = coldRes
				err = nil
			} else if coldErr != nil && !errors.Is(coldErr, storage.ErrRecordNotFound) {
				err = coldErr
			}
		}
	}
	if err != nil {
		writeDispatchOrBadRequestError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success":    true,
		"results":    results,
		"edge_count": db.EdgeCount(),
	})
}

func writeDispatchOrBadRequestError(w http.ResponseWriter, err error) {
	status := http.StatusBadRequest
	if errors.Is(err, storage.ErrRecordNotFound) ||
		errors.Is(err, storage.ErrComputeFailed) ||
		errors.Is(err, storage.ErrDatabaseClosed) ||
		errors.Is(err, worker.ErrWorkerUnavailable) {
		status = dispatchErrorStatus(err)
	}
	writeJSONError(w, status, err.Error())
}

func (s *Server) handleMath(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	var req MathRequest
	if !decodeJSONBody(w, r, &req, false) {
		return
	}

	db, release, err := s.acquireModel(req.Model)
	if err != nil {
		writeJSONError(w, dispatchErrorStatus(err), err.Error())
		return
	}
	defer release()

	switch strings.ToLower(strings.TrimSpace(req.Op)) {
	case "distance":
		if len(req.Words) < 2 {
			writeJSONError(w, http.StatusBadRequest, "distance requires at least 2 words")
			return
		}
		dist, err := db.CalculateDistance(req.Words[0], req.Words[1])
		if err != nil && errors.Is(err, storage.ErrRecordNotFound) {
			coldRes, coldErr := s.ExecuteColdMath(req.Model, wire.MathRequest{Op: "distance", Words: req.Words})
			if coldErr == nil {
				dist, err = coldRes.Value, nil
			} else if !errors.Is(coldErr, storage.ErrRecordNotFound) {
				err = coldErr
			}
		}
		if err != nil {
			writeDispatchOrBadRequestError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"success":  true,
			"distance": dist,
		})
	case "centroid":
		if len(req.Words) < 1 {
			writeJSONError(w, http.StatusBadRequest, "centroid requires at least 1 word")
			return
		}
		coord, roles, err := db.CalculateCentroid(req.Words)
		if err != nil && errors.Is(err, storage.ErrRecordNotFound) {
			coldRes, coldErr := s.ExecuteColdMath(req.Model, wire.MathRequest{Op: "centroid", Words: req.Words})
			if coldErr == nil {
				coord, roles, err = coldRes.Coord, coldRes.Roles, nil
			} else if !errors.Is(coldErr, storage.ErrRecordNotFound) {
				err = coldErr
			}
		}
		if err != nil {
			writeDispatchOrBadRequestError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"success": true,
			"coord":   coord,
			"roles":   roles,
		})
	case "cosine_similarity", "similarity":
		if len(req.Words) < 2 {
			writeJSONError(w, http.StatusBadRequest, "cosine_similarity requires at least 2 words")
			return
		}
		sim, err := db.CalculateCosineSimilarity(req.Words[0], req.Words[1])
		if err != nil && errors.Is(err, storage.ErrRecordNotFound) {
			coldRes, coldErr := s.ExecuteColdMath(req.Model, wire.MathRequest{Op: "cosine_similarity", Words: req.Words})
			if coldErr == nil {
				sim, err = coldRes.Value, nil
			} else if !errors.Is(coldErr, storage.ErrRecordNotFound) {
				err = coldErr
			}
		}
		if err != nil {
			writeDispatchOrBadRequestError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"success":    true,
			"similarity": sim,
		})
	case "nearest":
		if len(req.Words) < 1 {
			writeJSONError(w, http.StatusBadRequest, "nearest requires at least 1 word")
			return
		}
		if req.LimitK < 0 {
			writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("limit_k %d cannot be negative", req.LimitK))
			return
		}
		limit := req.LimitK
		if limit == 0 {
			limit = 5
		}
		neighbors, err := db.FindNearestNeighbors(req.Words[0], limit)
		if err != nil && errors.Is(err, storage.ErrRecordNotFound) {
			coldRes, coldErr := s.ExecuteColdMath(req.Model, wire.MathRequest{Op: "nearest", Words: req.Words, LimitK: limit})
			if coldErr == nil {
				neighbors, err = coldRes.Neighbors, nil
			} else if !errors.Is(coldErr, storage.ErrRecordNotFound) {
				err = coldErr
			}
		}
		if err != nil {
			writeDispatchOrBadRequestError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"success":   true,
			"neighbors": neighbors,
		})
	case "haversine":
		if len(req.Words) < 2 {
			writeJSONError(w, http.StatusBadRequest, "haversine requires at least 2 words")
			return
		}
		dist, err := db.CalculateHaversineDistance(req.Words[0], req.Words[1])
		if err != nil && errors.Is(err, storage.ErrRecordNotFound) {
			coldDist, coldErr := s.executeColdHaversine(req.Model, req.Words[0], req.Words[1])
			if coldErr == nil {
				dist, err = coldDist, nil
			} else if !errors.Is(coldErr, storage.ErrRecordNotFound) {
				err = coldErr
			}
		}
		if err != nil {
			writeDispatchOrBadRequestError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"success":     true,
			"distance_km": dist,
		})
	case "stats", "statistics":
		field := strings.TrimSpace(req.Field)
		if field == "" && len(req.Words) > 0 {
			field = req.Words[0]
		}
		if field == "" {
			writeJSONError(w, http.StatusBadRequest, "stats requires field parameter")
			return
		}
		stats, err := db.ComputeStats(field)
		if err == nil && stats.Count == 0 {
			coldStats, coldErr := s.executeColdStats(req.Model, field)
			if coldErr == nil && coldStats.Count > 0 {
				stats = coldStats
			} else if coldErr != nil && !errors.Is(coldErr, storage.ErrRecordNotFound) {
				err = coldErr
			}
		}
		if err != nil {
			writeDispatchOrBadRequestError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"success": true,
			"stats":   stats,
		})
	default:
		writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("unsupported math op %q", req.Op))
	}
}

func (s *Server) handleSync(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost, http.MethodGet) {
		return
	}
	var syncReq struct {
		Model string `json:"model"`
		Async bool   `json:"async"`
	}
	if r.Method == http.MethodPost {
		if !decodeJSONBody(w, r, &syncReq, true) {
			return
		}
	}
	modelName := r.URL.Query().Get("model")
	if modelName == "" {
		modelName = strings.TrimSpace(syncReq.Model)
	}
	if modelName == "" {
		modelName = "default"
	}

	isAsync := r.URL.Query().Get("async") == "true" || syncReq.Async

	// If async requested, offload to worker pool and return immediately
	if isAsync {
		if _, err := sanitizeModelName(modelName); err != nil {
			writeJSONError(w, dispatchErrorStatus(err), err.Error())
			return
		}
		jobID := uuid.New().String()
		err := s.worker.DispatchAsync(
			s.asyncBaseContext(),
			"sync_"+modelName,
			func(taskCtx context.Context) error {
				if err := taskCtx.Err(); err != nil {
					return err
				}
				asyncDB, asyncRelease, err := s.acquireModel(modelName)
				if err != nil {
					return err
				}
				defer asyncRelease()
				return asyncDB.Sync()
			},
			worker.WithID(jobID),
			worker.WithModel(modelName),
			worker.WithRetry(1, 200*time.Millisecond),
		)
		if err != nil {
			writeJSONError(w, dispatchErrorStatus(err), fmt.Sprintf("failed to dispatch sync task to worker: %s", err.Error()))
			return
		}
		writeJSON(w, http.StatusAccepted, map[string]interface{}{
			"status":  "accepted",
			"job_id":  jobID,
			"model":   modelName,
			"async":   true,
			"message": fmt.Sprintf("Sync for model %q queued to background worker engine", modelName),
		})
		return
	}

	if modelName == "all" || modelName == "*" {
		for _, m := range s.allModelNames() {
			db, release, err := s.acquireExistingModel(m)
			if err != nil {
				if errors.Is(err, errModelNotFound) {
					continue
				}
				writeJSONError(w, dispatchErrorStatus(err), err.Error())
				return
			}
			syncErr := db.Sync()
			release()
			if syncErr != nil {
				writeJSONError(w, dispatchErrorStatus(syncErr), syncErr.Error())
				return
			}
		}
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"success": true,
			"message": "sync complete",
		})
		return
	}

	db, release, err := s.acquireModel(modelName)
	if err != nil {
		writeJSONError(w, dispatchErrorStatus(err), err.Error())
		return
	}
	defer release()

	if err := db.Sync(); err != nil {
		writeJSONError(w, dispatchErrorStatus(err), err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": "sync complete",
	})
}

type PartitionRunRequest struct {
	Model         string  `json:"model"`
	DateField     string  `json:"date_field"`
	RetentionDays float64 `json:"retention_days"`
	SinkType      string  `json:"sink_type"` // "local" or "s3"
	TargetPath    string  `json:"target_path"`
	S3Endpoint    string  `json:"s3_endpoint"`
	S3Bucket      string  `json:"s3_bucket"`
	S3Prefix      string  `json:"s3_prefix"`
	S3Region      string  `json:"s3_region"`
	S3AccessKey   string  `json:"s3_access_key"`
	S3SecretKey   string  `json:"s3_secret_key"`
	PruneHotDB    *bool   `json:"prune_hot_db"`
	Async         bool    `json:"async"` // If true, offloads to worker pool (returns HTTP 202)
}

type s3RequestParams struct {
	Endpoint  string
	Bucket    string
	Prefix    string
	Region    string
	AccessKey string
	SecretKey string
}

// buildRequestS3Sink resolves S3 sink configuration from request overrides and server defaults.
func (s *Server) buildRequestS3Sink(params s3RequestParams, defaultBucket, defaultPrefix string) (*partition.S3Sink, int, error) {
	endpoint := params.Endpoint
	if endpoint == "" {
		endpoint = s.s3DefaultEndpoint
	}
	allowPrivate, err := getEnvBool("ANCHORDB_ALLOW_S3_PRIVATE_ENDPOINT", false)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	if err := validateS3Endpoint(endpoint, allowPrivate); err != nil {
		return nil, http.StatusBadRequest, fmt.Errorf("invalid s3_endpoint: %s", err.Error())
	}

	bucket := params.Bucket
	if bucket == "" {
		bucket = s.s3DefaultBucket
	}
	if bucket == "" {
		if defaultBucket == "" {
			return nil, http.StatusBadRequest, errors.New("s3_bucket is required")
		}
		bucket = defaultBucket
	}

	prefix := params.Prefix
	if prefix == "" {
		prefix = s.s3DefaultPrefix
	}
	if prefix == "" && defaultPrefix != "" {
		prefix = defaultPrefix
	}

	accessKey := params.AccessKey
	if accessKey == "" {
		accessKey = s.s3DefaultAccessKey
	}
	secretKey := params.SecretKey
	if secretKey == "" {
		secretKey = s.s3DefaultSecretKey
	}

	sink := partition.NewS3Sink(endpoint, bucket, prefix, accessKey, secretKey)
	if region := params.Region; region != "" {
		sink.Region = region
	} else if s.s3DefaultRegion != "" {
		sink.Region = s.s3DefaultRegion
	}

	allowInsecure, err := getEnvBool("ANCHORDB_ALLOW_INSECURE_S3_HTTP", false)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	sink.AllowInsecureHTTP = allowInsecure
	sink.HTTPClient = partition.NewS3HTTPClient(allowPrivate)
	return sink, 0, nil
}

func (s *Server) resolveTargetModels(model string) ([]string, error) {
	if model != "" && model != "all" && model != "*" {
		cleanModel, err := sanitizeModelName(model)
		if err != nil {
			return nil, err
		}
		return []string{cleanModel}, nil
	}
	return s.allModelNames(), nil
}

func (s *Server) handlePartitionRun(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "Method not allowed, use POST")
		return
	}

	var req PartitionRunRequest
	if !decodeJSONBody(w, r, &req, true) {
		return
	}

	dateField := req.DateField
	if dateField == "" {
		dateField = "date"
	}

	if req.RetentionDays < 0 || math.IsNaN(req.RetentionDays) || math.IsInf(req.RetentionDays, 0) {
		writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("invalid retention_days %v", req.RetentionDays))
		return
	}
	retentionDays := req.RetentionDays
	if retentionDays == 0 {
		retentionDays = 180 // Default 6 months
	}

	prune := true
	if req.PruneHotDB != nil {
		prune = *req.PruneHotDB
	}

	var sink partition.PartitionSink
	if strings.ToLower(req.SinkType) == "s3" {
		s3sink, code, err := s.buildRequestS3Sink(s3RequestParams{
			Endpoint:  req.S3Endpoint,
			Bucket:    req.S3Bucket,
			Prefix:    req.S3Prefix,
			Region:    req.S3Region,
			AccessKey: req.S3AccessKey,
			SecretKey: req.S3SecretKey,
		}, "anchordb-archive", "partitions")
		if err != nil {
			writeJSONError(w, code, err.Error())
			return
		}
		sink = s3sink
	} else {
		targetDir := req.TargetPath
		if targetDir == "" {
			targetDir = filepath.Join(s.dataDir, "partitions")
		} else {
			absDataDir, err := filepath.Abs(s.dataDir)
			if err != nil {
				writeJSONError(w, http.StatusInternalServerError, "invalid data directory")
				return
			}
			absTarget, err := filepath.Abs(targetDir)
			if err != nil {
				writeJSONError(w, http.StatusBadRequest, "invalid target_path")
				return
			}
			resolvedRoot, err := filepath.EvalSymlinks(absDataDir)
			if err != nil {
				writeJSONError(w, http.StatusInternalServerError, "cannot resolve data directory")
				return
			}
			resolvedTarget, err := resolvePathWithExistingSymlinks(absTarget)
			if err != nil {
				writeJSONError(w, http.StatusBadRequest, "cannot resolve target_path")
				return
			}
			relTarget, relErr := filepath.Rel(resolvedRoot, resolvedTarget)
			if relErr != nil || relTarget == ".." || strings.HasPrefix(relTarget, ".."+string(filepath.Separator)) || filepath.IsAbs(relTarget) {
				writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("Invalid target_path: must reside inside data directory %s", s.dataDir))
				return
			}
			targetDir = resolvedTarget
		}
		sink = partition.NewLocalDiskSink(targetDir)
	}

	targetModels, err := s.resolveTargetModels(req.Model)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	isAsync := req.Async || r.URL.Query().Get("async") == "true"

	// 1. Asynchronous execution via Worker Engine
	if isAsync {
		jobID := uuid.New().String()
		modelTag := req.Model
		if modelTag == "" {
			modelTag = "all"
		}
		completedModels := make(map[string]bool, len(targetModels))
		err := s.worker.DispatchAsync(
			s.asyncBaseContext(),
			"partition_run_async",
			func(taskCtx context.Context) error {
				var errs []error
				for _, m := range targetModels {
					if completedModels[m] {
						continue
					}
					if err := taskCtx.Err(); err != nil {
						errs = append(errs, err)
						break
					}
					db, release, err := s.acquireModel(m)
					if err != nil {
						errs = append(errs, fmt.Errorf("model %s: %w", m, err))
						continue
					}
					res, err := partition.ExecutePartition(db, partition.PartitionPolicy{
						ModelName:     m,
						DateField:     dateField,
						RetentionDays: retentionDays,
						Sink:          sink,
						FileFormat:    "{date}_{uuid}.anchordb",
						PruneHotDB:    prune,
						Catalog:       s.catalog,
					})
					release()
					if err != nil {
						errs = append(errs, fmt.Errorf("partition %s: %w", m, err))
						continue
					}
					completedModels[m] = true
					if res != nil && res.ArchivedCount > 0 {
						log.Printf("[WORKER] Async partition succeeded for model %q: %d records archived to %s",
							m, res.ArchivedCount, res.TargetLocation)
					}
				}
				return errors.Join(errs...)
			},
			worker.WithID(jobID),
			worker.WithModel(modelTag),
			worker.WithRetry(2, 500*time.Millisecond),
			worker.WithRetryJitter(100*time.Millisecond),
		)

		if err != nil {
			writeJSONError(w, dispatchErrorStatus(err), fmt.Sprintf("Failed to dispatch partition to worker: %s", err.Error()))
			return
		}

		writeJSON(w, http.StatusAccepted, map[string]interface{}{
			"status":  "accepted",
			"job_id":  jobID,
			"async":   true,
			"message": "Partition task accepted by background worker pool",
			"targets": targetModels,
		})
		return
	}

	// 2. Synchronous execution
	var results []*partition.PartitionResult
	for _, m := range targetModels {
		db, release, err := s.acquireModel(m)
		if err != nil {
			writeJSONError(w, dispatchErrorStatus(err), fmt.Sprintf("Partition failed opening model %s: %s", m, err.Error()))
			return
		}
		res, err := partition.ExecutePartition(db, partition.PartitionPolicy{
			ModelName:     m,
			DateField:     dateField,
			RetentionDays: retentionDays,
			Sink:          sink,
			FileFormat:    "{date}_{uuid}.anchordb",
			PruneHotDB:    prune,
			Catalog:       s.catalog,
		})
		release()
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("Partition failed for model %s: %s", m, err.Error()))
			return
		}

		results = append(results, res)
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"results": results,
	})
}

// handlePartitionList lists available partition archives in s.dataDir/partitions (optionally filtered by ?model=).
func (s *Server) handlePartitionList(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet, http.MethodPost) {
		return
	}
	modelFilter := strings.TrimSpace(r.URL.Query().Get("model"))
	if modelFilter == "" && r.Method == http.MethodPost {
		var body struct {
			Model string `json:"model"`
		}
		if !decodeJSONBody(w, r, &body, true) {
			return
		}
		modelFilter = strings.TrimSpace(body.Model)
	}

	type PartitionMetaDTO struct {
		Name      string    `json:"name"`
		SizeBytes int64     `json:"size_bytes"`
		ModTime   time.Time `json:"mod_time"`
	}

	if modelFilter != "" && s.catalog != nil {
		entries := s.catalog.ListEntries(modelFilter)
		if len(entries) > 0 {
			partitions := make([]PartitionMetaDTO, 0, len(entries))
			for _, entry := range entries {
				partitions = append(partitions, PartitionMetaDTO{
					Name:      entry.PartitionFile,
					SizeBytes: entry.SizeBytes,
					ModTime:   entry.CreatedAt,
				})
			}
			writeJSON(w, http.StatusOK, map[string]interface{}{
				"success":    true,
				"partitions": partitions,
			})
			return
		}
	}

	partDir := filepath.Join(s.dataDir, "partitions")
	entries, err := os.ReadDir(partDir)
	if err != nil {
		if os.IsNotExist(err) {
			writeJSON(w, http.StatusOK, map[string]interface{}{
				"success":    true,
				"partitions": []interface{}{},
			})
			return
		}
		writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("failed to read partitions directory: %s", err.Error()))
		return
	}

	partitions := make([]PartitionMetaDTO, 0)
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".anchordb") {
			continue
		}
		if modelFilter != "" && !strings.HasPrefix(e.Name(), modelFilter+"_") {
			continue
		}
		info, err := e.Info()
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("failed to stat partition %s: %s", e.Name(), err.Error()))
			return
		}
		partitions = append(partitions, PartitionMetaDTO{
			Name:      e.Name(),
			SizeBytes: info.Size(),
			ModTime:   info.ModTime(),
		})
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success":    true,
		"partitions": partitions,
	})
}

type PartitionInspectRequest struct {
	PartitionFile string            `json:"partition_file"`
	Name          string            `json:"name,omitempty"`
	Limit         int               `json:"limit,omitempty"`
	Offset        int               `json:"offset,omitempty"`
	Filters       []FilterDTO       `json:"filters,omitempty"`
	Select        []string          `json:"select,omitempty"`
	Computations  map[string]string `json:"computations,omitempty"`
}

// handlePartitionInspect safely reads records from an archived partition file via the engine.
// It transparently handles local partition files as well as remote S3 partition archives.
func (s *Server) handlePartitionInspect(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	var req PartitionInspectRequest
	if !decodeJSONBody(w, r, &req, false) {
		return
	}
	if req.PartitionFile == "" {
		writeJSONError(w, http.StatusBadRequest, "partition_file is required")
		return
	}
	if req.Limit < 0 {
		writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("limit %d cannot be negative", req.Limit))
		return
	}
	if req.Offset < 0 {
		writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("offset %d cannot be negative", req.Offset))
		return
	}

	filters, err := buildFilters(req.Filters)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	computations := make(map[string]storage.ComputeOp, len(req.Computations))
	for outField, expr := range req.Computations {
		op, cErr := parseComputeOp(expr)
		if cErr != nil {
			writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("invalid computation %q for field %q: %s", expr, outField, cErr.Error()))
			return
		}
		computations[outField] = op
	}

	limit := req.Limit
	if limit == 0 {
		limit = 10
	}
	if limit > 100 {
		limit = 100
	}

	// 1. Check if S3 partition (either starts with s3:// or registered in catalog with SinkType == "s3")
	var entry *partition.CatalogEntry
	if s.catalog != nil {
		entry, _ = s.catalog.GetEntry(req.PartitionFile)
	}

	if strings.HasPrefix(req.PartitionFile, "s3://") || (entry != nil && entry.SinkType == "s3") {
		sink, sErr := s.buildS3SinkForEntry(entry, req.PartitionFile)
		if sErr != nil {
			writeJSONError(w, http.StatusInternalServerError, sErr.Error())
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
		defer cancel()

		records, hasMore, specificProps, specificAttrs, err := s.s3Cache.InspectPartitionQuery(ctx, sink, req.PartitionFile, partition.PartitionInspectQuery{
			RecordName:   req.Name,
			Limit:        limit,
			Offset:       req.Offset,
			Filters:      filters,
			Select:       req.Select,
			Computations: computations,
		})
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("failed inspecting s3 partition: %s", err.Error()))
			return
		}
		if req.Name != "" {
			writeJSON(w, http.StatusOK, map[string]interface{}{
				"success":        true,
				"partition_file": req.PartitionFile,
				"name":           req.Name,
				"props":          specificProps,
				"attrs":          specificAttrs,
				"source":         "s3",
			})
			return
		}
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"success":        true,
			"partition_file": req.PartitionFile,
			"records":        records,
			"count":          len(records),
			"has_more":       hasMore,
			"source":         "s3",
		})
		return
	}

	// 2. Local disk partition inspection
	cleanFilename := filepath.Base(req.PartitionFile)
	fullPath := filepath.Join(s.dataDir, "partitions", cleanFilename)
	if _, err := os.Stat(fullPath); err != nil {
		writeJSONError(w, http.StatusNotFound, fmt.Sprintf("partition file not found: %s", cleanFilename))
		return
	}

	pdb, err := storage.Open(fullPath)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("failed to open partition: %s", err.Error()))
		return
	}
	defer closePartitionDB(pdb, cleanFilename)

	if req.Name != "" {
		var cr storage.CompactRecord
		if len(computations) > 0 || len(req.Select) > 0 {
			cr, err = pdb.GetRecordTypedWithCompute(req.Name, computations, req.Select...)
		} else {
			cr, err = pdb.GetRecordTyped(req.Name)
		}
		if err != nil {
			writeJSONError(w, http.StatusNotFound, fmt.Sprintf("record %q not found in partition", req.Name))
			return
		}
		props, pErr := cr.ToMap()
		if pErr != nil {
			writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("corrupt partition record %q: %v", req.Name, pErr))
			return
		}
		attrs, aErr := cr.ExpandAttrs()
		if aErr != nil {
			writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("corrupt partition record %q: %v", req.Name, aErr))
			return
		}
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"success":        true,
			"partition_file": cleanFilename,
			"name":           req.Name,
			"props":          props,
			"attrs":          attrs,
		})
		return
	}

	records, hasMore, err := pdb.ListRecordsWithPaging(storage.QueryOptions{
		Context:      r.Context(),
		Limit:        limit,
		Offset:       req.Offset,
		Filters:      filters,
		Select:       req.Select,
		Computations: computations,
	})
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("failed to list records in partition: %s", err.Error()))
		return
	}

	type RecordDTO struct {
		Name  string              `json:"name"`
		Props map[string]float32  `json:"props"`
		Attrs []storage.AttrEntry `json:"attrs,omitempty"`
	}
	out := make([]RecordDTO, len(records))
	for i, rec := range records {
		out[i] = RecordDTO{Name: rec.Name, Props: rec.Props, Attrs: rec.Attrs}
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success":        true,
		"partition_file": cleanFilename,
		"records":        out,
		"count":          len(out),
		"has_more":       hasMore,
	})
}

// handlePartitionCatalog returns all registered partition manifest entries from the persistent catalog.
func (s *Server) handlePartitionCatalog(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet, http.MethodPost) {
		return
	}
	model := r.URL.Query().Get("model")
	var entries []*partition.CatalogEntry
	if s.catalog != nil {
		entries = s.catalog.ListEntries(model)
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success":    true,
		"count":      len(entries),
		"partitions": entries,
	})
}

// PartitionCatalogRecordRequest represents payload to query which partition file contains a record.
type PartitionCatalogRecordRequest struct {
	Model string `json:"model"`
	Name  string `json:"name"`
}

// handlePartitionCatalogRecord returns the partition manifest entry for a specific record.
func (s *Server) handlePartitionCatalogRecord(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	var req PartitionCatalogRecordRequest
	if !decodeJSONBody(w, r, &req, false) {
		return
	}
	if req.Model == "" || req.Name == "" {
		writeJSONError(w, http.StatusBadRequest, "model and name are required")
		return
	}
	if s.catalog == nil {
		writeJSONError(w, http.StatusNotFound, "catalog is not initialized")
		return
	}

	entry, found := s.catalog.FindRecord(req.Model, req.Name)
	if !found {
		writeJSONError(w, http.StatusNotFound, fmt.Sprintf("record %q not found in partition catalog for model %q", req.Name, req.Model))
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success":   true,
		"model":     req.Model,
		"name":      req.Name,
		"partition": entry,
	})
}

// PartitionS3ListRequest represents parameters to list partition files directly in an S3 bucket.
type PartitionS3ListRequest struct {
	S3Endpoint  string `json:"s3_endpoint"`
	S3Bucket    string `json:"s3_bucket"`
	S3Prefix    string `json:"s3_prefix,omitempty"`
	S3AccessKey string `json:"s3_access_key,omitempty"`
	S3SecretKey string `json:"s3_secret_key,omitempty"`
	S3Region    string `json:"s3_region,omitempty"`
}

// handlePartitionS3List queries an S3 bucket via SigV4 and lists all available .anchordb files.
func (s *Server) handlePartitionS3List(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	var req PartitionS3ListRequest
	if !decodeJSONBody(w, r, &req, false) {
		return
	}

	sink, code, err := s.buildRequestS3Sink(s3RequestParams{
		Endpoint:  req.S3Endpoint,
		Bucket:    req.S3Bucket,
		Prefix:    req.S3Prefix,
		Region:    req.S3Region,
		AccessKey: req.S3AccessKey,
		SecretKey: req.S3SecretKey,
	}, "", "")
	if err != nil {
		writeJSONError(w, code, err.Error())
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	partitions, err := sink.ListPartitions(ctx)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("failed listing s3 partitions: %s", err.Error()))
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success":    true,
		"endpoint":   sink.Endpoint,
		"bucket":     sink.Bucket,
		"prefix":     sink.Prefix,
		"count":      len(partitions),
		"partitions": partitions,
	})
}

// PartitionS3InspectRequest represents parameters to directly inspect an S3 partition file.
type PartitionS3InspectRequest struct {
	S3Endpoint    string            `json:"s3_endpoint"`
	S3Bucket      string            `json:"s3_bucket"`
	S3Prefix      string            `json:"s3_prefix,omitempty"`
	S3Key         string            `json:"s3_key"`
	PartitionFile string            `json:"partition_file,omitempty"`
	S3AccessKey   string            `json:"s3_access_key,omitempty"`
	S3SecretKey   string            `json:"s3_secret_key,omitempty"`
	S3Region      string            `json:"s3_region,omitempty"`
	Name          string            `json:"name,omitempty"`
	Limit         int               `json:"limit,omitempty"`
	Offset        int               `json:"offset,omitempty"`
	Filters       []FilterDTO       `json:"filters,omitempty"`
	Select        []string          `json:"select,omitempty"`
	Computations  map[string]string `json:"computations,omitempty"`
}

// handlePartitionS3Inspect reads records from an S3 partition file without requiring manual pre-downloading.
func (s *Server) handlePartitionS3Inspect(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	var req PartitionS3InspectRequest
	if !decodeJSONBody(w, r, &req, false) {
		return
	}

	targetKey := req.S3Key
	if targetKey == "" {
		targetKey = req.PartitionFile
	}
	if targetKey == "" {
		writeJSONError(w, http.StatusBadRequest, "s3_key or partition_file is required")
		return
	}
	if req.Limit < 0 {
		writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("limit %d cannot be negative", req.Limit))
		return
	}
	if req.Offset < 0 {
		writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("offset %d cannot be negative", req.Offset))
		return
	}
	filters, err := buildFilters(req.Filters)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	computations := make(map[string]storage.ComputeOp, len(req.Computations))
	for outField, expr := range req.Computations {
		op, cErr := parseComputeOp(expr)
		if cErr != nil {
			writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("invalid computation %q for field %q: %s", expr, outField, cErr.Error()))
			return
		}
		computations[outField] = op
	}

	sink, code, err := s.buildRequestS3Sink(s3RequestParams{
		Endpoint:  req.S3Endpoint,
		Bucket:    req.S3Bucket,
		Prefix:    req.S3Prefix,
		Region:    req.S3Region,
		AccessKey: req.S3AccessKey,
		SecretKey: req.S3SecretKey,
	}, "", "")
	if err != nil {
		writeJSONError(w, code, err.Error())
		return
	}

	limit := req.Limit
	if limit == 0 {
		limit = 10
	}
	if limit > 100 {
		limit = 100
	}

	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()

	records, hasMore, specificProps, specificAttrs, err := s.s3Cache.InspectPartitionQuery(ctx, sink, targetKey, partition.PartitionInspectQuery{
		RecordName:   req.Name,
		Limit:        limit,
		Offset:       req.Offset,
		Filters:      filters,
		Select:       req.Select,
		Computations: computations,
	})
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("failed inspecting s3 partition: %s", err.Error()))
		return
	}

	if req.Name != "" {
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"success": true,
			"target":  targetKey,
			"name":    req.Name,
			"props":   specificProps,
			"attrs":   specificAttrs,
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success":  true,
		"target":   targetKey,
		"count":    len(records),
		"has_more": hasMore,
		"records":  records,
	})
}

func (s *Server) buildS3SinkForEntry(entry *partition.CatalogEntry, keyOrLoc string) (*partition.S3Sink, error) {
	endpoint := s.s3DefaultEndpoint
	bucket := s.s3DefaultBucket
	prefix := s.s3DefaultPrefix
	accessKey := s.s3DefaultAccessKey
	secretKey := s.s3DefaultSecretKey
	region := s.s3DefaultRegion

	if entry != nil && entry.S3Metadata != nil {
		if entry.S3Metadata.Endpoint != "" {
			endpoint = entry.S3Metadata.Endpoint
		}
		if entry.S3Metadata.Bucket != "" {
			bucket = entry.S3Metadata.Bucket
		}
		if entry.S3Metadata.Prefix != "" {
			prefix = entry.S3Metadata.Prefix
		}
		if entry.S3Metadata.Region != "" {
			region = entry.S3Metadata.Region
		}
		if entry.S3Metadata.AccessKey != "" {
			accessKey = entry.S3Metadata.AccessKey
		}
	} else if strings.HasPrefix(keyOrLoc, "s3://") {
		trimmed := strings.TrimPrefix(keyOrLoc, "s3://")
		parts := strings.SplitN(trimmed, "/", 2)
		if len(parts) >= 1 && parts[0] != "" {
			bucket = parts[0]
		}
	}

	if endpoint == "" {
		endpoint = os.Getenv("AWS_S3_ENDPOINT")
		if endpoint == "" {
			endpoint = os.Getenv("MINIO_ENDPOINT")
		}
	}
	if accessKey == "" {
		accessKey = os.Getenv("AWS_ACCESS_KEY_ID")
		if accessKey == "" {
			accessKey = os.Getenv("MINIO_ROOT_USER")
		}
	}
	if secretKey == "" {
		secretKey = os.Getenv("AWS_SECRET_ACCESS_KEY")
		if secretKey == "" {
			secretKey = os.Getenv("MINIO_ROOT_PASSWORD")
		}
	}

	allowInsecure, err := getEnvBool("ANCHORDB_ALLOW_INSECURE_S3_HTTP", false)
	if err != nil {
		return nil, err
	}
	allowPrivate, err := getEnvBool("ANCHORDB_ALLOW_S3_PRIVATE_ENDPOINT", false)
	if err != nil {
		return nil, err
	}

	sink := partition.NewS3Sink(endpoint, bucket, prefix, accessKey, secretKey)
	if region != "" {
		sink.Region = region
	}
	sink.AllowInsecureHTTP = allowInsecure
	sink.HTTPClient = partition.NewS3HTTPClient(allowPrivate)
	return sink, nil
}

func (s *Server) fetchRecordTypedFromCatalogEntry(entry *partition.CatalogEntry, name string, computations map[string]storage.ComputeOp, selectFields []string) (storage.CompactRecord, error) {
	if entry.SinkType == "s3" || strings.HasPrefix(entry.TargetLocation, "s3://") {
		sink, err := s.buildS3SinkForEntry(entry, entry.PartitionFile)
		if err != nil {
			return storage.CompactRecord{}, err
		}
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		return s.s3Cache.GetRecordTyped(ctx, sink, entry.PartitionFile, name, computations, selectFields)
	}

	localFile := filepath.Join(s.dataDir, "partitions", entry.PartitionFile)
	pdb, err := storage.Open(localFile)
	if err != nil {
		return storage.CompactRecord{}, err
	}
	defer closePartitionDB(pdb, entry.PartitionFile)

	if len(computations) > 0 || len(selectFields) > 0 {
		return pdb.GetRecordTypedWithCompute(name, computations, selectFields...)
	}
	return pdb.GetRecordTyped(name)
}

// GetColdRecordTyped looks up a record only in the cold tier via the partition
// catalog. A catalog hit whose fetch fails returns that failure (never a
// masked "not found"); a catalog miss returns ErrRecordNotFound. It implements
// wire.ColdTierBackend so wire callers whose own hot lookup already failed can
// consult cold storage without repeating the hot read.
func (s *Server) GetColdRecordTyped(model, name string, computations map[string]storage.ComputeOp, selectFields []string) (storage.CompactRecord, error) {
	if s.catalog != nil {
		if entry, found := s.catalog.FindRecord(model, name); found {
			return s.fetchRecordTypedFromCatalogEntry(entry, name, computations, selectFields)
		}
	}
	return storage.CompactRecord{}, fmt.Errorf("record %q not found in model %q: %w", name, model, storage.ErrRecordNotFound)
}

// GetRecordTypedWithComputeTiered implements wire.TypedTieredBackend.
func (s *Server) GetRecordTypedWithComputeTiered(model, name string, computations map[string]storage.ComputeOp, selectFields []string) (storage.CompactRecord, error) {
	// 1. Check Hot DB
	db, release, err := s.acquireModel(model)
	if err != nil {
		return storage.CompactRecord{}, err
	}
	var cr storage.CompactRecord
	var hotErr error
	if len(computations) > 0 || len(selectFields) > 0 {
		cr, hotErr = db.GetRecordTypedWithCompute(name, computations, selectFields...)
	} else {
		cr, hotErr = db.GetRecordTyped(name)
	}
	release()
	if hotErr == nil {
		return cr, nil
	}
	// Only a missing record may fall through to cold tier. Compute, IO,
	// closed-DB and other failures keep their identity so callers never
	// mistake them for "not found".
	if !errors.Is(hotErr, storage.ErrRecordNotFound) {
		return storage.CompactRecord{}, hotErr
	}

	// 2. Check Cold Tier via Partition Catalog
	return s.GetColdRecordTyped(model, name, computations, selectFields)
}

// GetRecordTiered implements wire.TieredBackend.
func (s *Server) GetRecordTiered(model, name string) (map[string]float32, uint8, error) {
	return s.GetRecordWithComputeTiered(model, name, nil, nil)
}

// GetRecordWithComputeTiered implements wire.TieredBackend.
func (s *Server) GetRecordWithComputeTiered(model, name string, computations map[string]storage.ComputeOp, selectFields []string) (map[string]float32, uint8, error) {
	cr, err := s.GetRecordTypedWithComputeTiered(model, name, computations, selectFields)
	if err != nil {
		return nil, 0, err
	}
	props, err := cr.ToMap()
	if err != nil {
		return nil, 0, err
	}
	return props, cr.Operation, nil
}

func (s *Server) forEachColdPartition(model string, fn func(entry *partition.CatalogEntry, pdb *storage.AnchorDB) (bool, error)) error {
	if s.catalog == nil {
		return nil
	}
	entries := s.catalog.ListEntries(model)
	for _, entry := range entries {
		var stop bool
		var err error
		if entry.SinkType == "s3" || strings.HasPrefix(entry.TargetLocation, "s3://") {
			sink, sErr := s.buildS3SinkForEntry(entry, entry.PartitionFile)
			if sErr != nil {
				return sErr
			}
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			err = s.s3Cache.RunOnPartition(ctx, sink, entry.PartitionFile, func(pdb *storage.AnchorDB) error {
				var cbErr error
				stop, cbErr = fn(entry, pdb)
				return cbErr
			})
			cancel()
		} else {
			localFile := filepath.Join(s.dataDir, "partitions", filepath.Base(entry.PartitionFile))
			pdb, openErr := storage.Open(localFile)
			if openErr != nil {
				return openErr
			}
			stop, err = fn(entry, pdb)
			closePartitionDB(pdb, entry.PartitionFile)
		}
		if err != nil {
			return err
		}
		if stop {
			break
		}
	}
	return nil
}

// ListColdRecords implements wire.ColdOperationsBackend across archived partitions for a model.
func (s *Server) ListColdRecords(model string, opts storage.QueryOptions) ([]storage.EntityRecord, bool, error) {
	limit := opts.Limit
	if limit <= 0 {
		limit = 10
	}
	offset := opts.Offset
	if offset < 0 {
		return nil, false, fmt.Errorf("negative offset %d", offset)
	}
	seen := make(map[string]struct{})
	var all []storage.EntityRecord
	var hasMore bool
	err := s.forEachColdPartition(model, func(_ *partition.CatalogEntry, pdb *storage.AnchorDB) (bool, error) {
		partOpts := opts
		partOpts.Offset = 0
		partOpts.Limit = offset + limit + 1 - len(all)
		if partOpts.Limit < 1 {
			partOpts.Limit = 1
		}
		recs, partMore, lErr := pdb.ListRecordsWithPaging(partOpts)
		if lErr != nil {
			return false, lErr
		}
		for _, rec := range recs {
			if _, dup := seen[rec.Name]; dup {
				continue
			}
			seen[rec.Name] = struct{}{}
			all = append(all, rec)
		}
		if len(all) > offset+limit {
			hasMore = true
			return true, nil
		}
		if partMore {
			hasMore = true
		}
		return false, nil
	})
	if err != nil {
		return nil, false, err
	}
	if offset >= len(all) {
		return nil, false, nil
	}
	end := offset + limit
	if end > len(all) {
		end = len(all)
	} else if len(all) > end {
		hasMore = true
	}
	return all[offset:end], hasMore, nil
}

// UpdateColdRecords implements wire.ColdOperationsBackend for writable local partition archives.
func (s *Server) UpdateColdRecords(model string, opts storage.UpdateOptions) (int, error) {
	if s.catalog == nil {
		return 0, nil
	}
	remaining := opts.Limit
	if remaining < 0 {
		return 0, fmt.Errorf("negative update limit %d", remaining)
	}
	totalUpdated := 0
	entries := s.catalog.ListEntries(model)
	for _, entry := range entries {
		if entry.SinkType == "s3" || strings.HasPrefix(entry.TargetLocation, "s3://") {
			for _, target := range opts.TargetEntities {
				if slices.Contains(entry.RecordNames, target) {
					return totalUpdated, fmt.Errorf("cannot update immutable s3 partition record %q (%s)", target, entry.PartitionFile)
				}
			}
			continue
		}
		localFile := filepath.Join(s.dataDir, "partitions", filepath.Base(entry.PartitionFile))
		pdb, err := storage.Open(localFile)
		if err != nil {
			return totalUpdated, err
		}
		partOpts := opts
		if opts.Limit > 0 {
			partOpts.Limit = remaining - totalUpdated
			if partOpts.Limit <= 0 {
				closePartitionDB(pdb, entry.PartitionFile)
				break
			}
		}
		updated, uErr := pdb.UpdateRecords(partOpts)
		if uErr == nil && updated > 0 {
			uErr = pdb.Sync()
		}
		closePartitionDB(pdb, entry.PartitionFile)
		if uErr != nil {
			return totalUpdated, uErr
		}
		totalUpdated += updated
		if opts.Limit > 0 && totalUpdated >= opts.Limit {
			break
		}
	}
	return totalUpdated, nil
}

func (s *Server) hotModelRelations(model string) (map[string]spatial.Point2D, error) {
	db, release, err := s.acquireExistingModel(model)
	if err != nil {
		if errors.Is(err, errModelNotFound) {
			return nil, nil
		}
		return nil, err
	}
	defer release()
	return db.ExportRelations()
}

func applyRelationsToColdDB(pdb *storage.AnchorDB, rels map[string]spatial.Point2D) error {
	for relName, vec := range rels {
		if _, ok := pdb.Relation(relName); ok {
			continue
		}
		if err := pdb.SetRelation(relName, vec); err != nil {
			return err
		}
	}
	return nil
}

// ExecuteColdGraph implements wire.ColdOperationsBackend across archived partitions.
func (s *Server) ExecuteColdGraph(ctx context.Context, model string, req wire.GraphRequest) ([]storage.GraphQueryResult, spatial.Point2D, error) {
	hotRels, err := s.hotModelRelations(model)
	if err != nil {
		return nil, spatial.Point2D{}, err
	}
	var bestRes []storage.GraphQueryResult
	var bestAnchor spatial.Point2D
	err = s.forEachColdPartition(model, func(_ *partition.CatalogEntry, pdb *storage.AnchorDB) (bool, error) {
		if rErr := applyRelationsToColdDB(pdb, hotRels); rErr != nil {
			return false, rErr
		}
		res, anchor, gErr := pdb.ExecuteGraphStepWithContext(ctx, req.Inputs, req.Relation, req.LimitK, storage.OrderByMode(req.OrderMode), req.BeamWidth)
		if gErr != nil {
			if errors.Is(gErr, storage.ErrRecordNotFound) {
				return false, nil
			}
			return false, gErr
		}
		if len(res) > 0 {
			bestRes = res
			bestAnchor = anchor
			return true, nil
		}
		return false, nil
	})
	if err != nil {
		return nil, spatial.Point2D{}, err
	}
	if len(bestRes) == 0 {
		return nil, spatial.Point2D{}, storage.ErrRecordNotFound
	}
	return bestRes, bestAnchor, nil
}

// ExecuteColdGraphChain implements wire.ColdOperationsBackend across archived partitions.
func (s *Server) ExecuteColdGraphChain(ctx context.Context, model string, req wire.GraphChainRequest) ([]storage.GraphQueryResult, error) {
	hotRels, err := s.hotModelRelations(model)
	if err != nil {
		return nil, err
	}
	var bestRes []storage.GraphQueryResult
	err = s.forEachColdPartition(model, func(_ *partition.CatalogEntry, pdb *storage.AnchorDB) (bool, error) {
		if rErr := applyRelationsToColdDB(pdb, hotRels); rErr != nil {
			return false, rErr
		}
		res, gErr := pdb.ExecuteGraphChainWithContext(ctx, req.Inputs, req.RelationChain, req.LimitK, req.BeamWidth)
		if gErr != nil {
			if errors.Is(gErr, storage.ErrRecordNotFound) {
				return false, nil
			}
			return false, gErr
		}
		if len(res) > 0 {
			bestRes = res
			return true, nil
		}
		return false, nil
	})
	if err != nil {
		return nil, err
	}
	if len(bestRes) == 0 {
		return nil, storage.ErrRecordNotFound
	}
	return bestRes, nil
}

// ExecuteColdCausal implements wire.ColdOperationsBackend across archived partitions.
func (s *Server) ExecuteColdCausal(ctx context.Context, model string, seeds []string, relation string, limitK int) ([]storage.CausalResult, error) {
	var combined []storage.CausalResult
	err := s.forEachColdPartition(model, func(_ *partition.CatalogEntry, pdb *storage.AnchorDB) (bool, error) {
		res, cErr := pdb.ExecuteCausalStep(ctx, seeds, relation, limitK)
		if cErr != nil {
			if errors.Is(cErr, storage.ErrRecordNotFound) {
				return false, nil
			}
			return false, cErr
		}
		combined = append(combined, res...)
		return len(combined) >= limitK && limitK > 0, nil
	})
	if err != nil {
		return nil, err
	}
	if limitK > 0 && len(combined) > limitK {
		combined = combined[:limitK]
	}
	return combined, nil
}

// ExecuteColdCausalChain implements wire.ColdOperationsBackend across archived partitions.
func (s *Server) ExecuteColdCausalChain(ctx context.Context, model string, seeds []string, relations []string, limitK int) ([]storage.CausalResult, error) {
	var combined []storage.CausalResult
	err := s.forEachColdPartition(model, func(_ *partition.CatalogEntry, pdb *storage.AnchorDB) (bool, error) {
		res, cErr := pdb.ExecuteCausalChain(ctx, seeds, relations, limitK)
		if cErr != nil {
			if errors.Is(cErr, storage.ErrRecordNotFound) {
				return false, nil
			}
			return false, cErr
		}
		combined = append(combined, res...)
		return len(combined) >= limitK && limitK > 0, nil
	})
	if err != nil {
		return nil, err
	}
	if limitK > 0 && len(combined) > limitK {
		combined = combined[:limitK]
	}
	return combined, nil
}

// ExecuteColdMath implements wire.ColdOperationsBackend across archived partitions.
func (s *Server) ExecuteColdMath(model string, req wire.MathRequest) (wire.MathResponse, error) {
	var out wire.MathResponse
	var found bool
	err := s.forEachColdPartition(model, func(_ *partition.CatalogEntry, pdb *storage.AnchorDB) (bool, error) {
		var mErr error
		switch req.Op {
		case "distance":
			if len(req.Words) >= 2 {
				out.Value, mErr = pdb.CalculateDistance(req.Words[0], req.Words[1])
			}
		case "centroid":
			out.Coord, out.Roles, mErr = pdb.CalculateCentroid(req.Words)
		case "cosine_similarity", "similarity":
			if len(req.Words) >= 2 {
				out.Value, mErr = pdb.CalculateCosineSimilarity(req.Words[0], req.Words[1])
			}
		case "nearest":
			if len(req.Words) >= 1 {
				out.Neighbors, mErr = pdb.FindNearestNeighbors(req.Words[0], req.LimitK)
			}
		default:
			return true, fmt.Errorf("unsupported math op: %s", req.Op)
		}
		if mErr != nil {
			if errors.Is(mErr, storage.ErrRecordNotFound) {
				return false, nil
			}
			return false, mErr
		}
		found = true
		return true, nil
	})
	if err != nil {
		return wire.MathResponse{}, err
	}
	if !found {
		return wire.MathResponse{}, storage.ErrRecordNotFound
	}
	return out, nil
}

func (s *Server) executeColdHaversine(model, wordA, wordB string) (float32, error) {
	var dist float32
	var found bool
	err := s.forEachColdPartition(model, func(_ *partition.CatalogEntry, pdb *storage.AnchorDB) (bool, error) {
		d, err := pdb.CalculateHaversineDistance(wordA, wordB)
		if err != nil {
			if errors.Is(err, storage.ErrRecordNotFound) {
				return false, nil
			}
			return false, err
		}
		dist = d
		found = true
		return true, nil
	})
	if err != nil {
		return 0, err
	}
	if !found {
		return 0, storage.ErrRecordNotFound
	}
	return dist, nil
}

func (s *Server) executeColdStats(model, field string) (storage.StatsResult, error) {
	var out storage.StatsResult
	var found bool
	err := s.forEachColdPartition(model, func(_ *partition.CatalogEntry, pdb *storage.AnchorDB) (bool, error) {
		st, err := pdb.ComputeStats(field)
		if err != nil {
			return false, err
		}
		if st.Count > 0 {
			out = st
			found = true
			return true, nil
		}
		return false, nil
	})
	if err != nil {
		return storage.StatsResult{}, err
	}
	if !found {
		return storage.StatsResult{}, nil
	}
	return out, nil
}

// GetColdSchema implements wire.ColdOperationsBackend across archived partitions.
func (s *Server) GetColdSchema(model string) (storage.ModelSchema, bool, error) {
	var out storage.ModelSchema
	var found bool
	err := s.forEachColdPartition(model, func(_ *partition.CatalogEntry, pdb *storage.AnchorDB) (bool, error) {
		if ms, ok := pdb.GetSchema(); ok {
			out = ms
			found = true
			return true, nil
		}
		return false, nil
	})
	return out, found, err
}

// GetPartitionCatalog implements wire.PartitionBackend.
func (s *Server) GetPartitionCatalog(model string) ([]wire.PartitionCatalogItemDTO, error) {
	if s.catalog == nil {
		return nil, nil
	}
	entries := s.catalog.ListEntries(model)
	out := make([]wire.PartitionCatalogItemDTO, len(entries))
	for i, e := range entries {
		out[i] = wire.PartitionCatalogItemDTO{
			ID:             e.ID,
			Model:          e.Model,
			PartitionFile:  e.PartitionFile,
			TargetLocation: e.TargetLocation,
			SinkType:       e.SinkType,
			RecordCount:    e.RecordCount,
			CutoffDate:     e.CutoffDate,
			MinDate:        e.MinDate,
			MaxDate:        e.MaxDate,
			CreatedAt:      e.CreatedAt,
			SizeBytes:      e.SizeBytes,
		}
	}
	return out, nil
}

// InspectPartition implements wire.PartitionBackend.
func (s *Server) InspectPartition(req wire.PartitionInspectWireRequest) (wire.PartitionInspectWireResponse, error) {
	if req.Limit < 0 {
		return wire.PartitionInspectWireResponse{}, fmt.Errorf("limit %d cannot be negative", req.Limit)
	}
	if req.Offset < 0 {
		return wire.PartitionInspectWireResponse{}, fmt.Errorf("offset %d cannot be negative", req.Offset)
	}
	fileOrLoc := req.PartitionFile
	recordName := req.RecordName
	limit := req.Limit

	var entry *partition.CatalogEntry
	if s.catalog != nil {
		entry, _ = s.catalog.GetEntry(fileOrLoc)
	}

	if (entry != nil && entry.SinkType == "s3") || strings.HasPrefix(fileOrLoc, "s3://") {
		sink, sErr := s.buildS3SinkForEntry(entry, fileOrLoc)
		if sErr != nil {
			return wire.PartitionInspectWireResponse{}, sErr
		}
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		records, hasMore, props, attrs, err := s.s3Cache.InspectPartitionQuery(ctx, sink, fileOrLoc, partition.PartitionInspectQuery{
			RecordName:   recordName,
			Limit:        limit,
			Offset:       req.Offset,
			Filters:      req.Filters,
			Select:       req.Select,
			Computations: req.Computations,
		})
		if err != nil {
			return wire.PartitionInspectWireResponse{}, err
		}
		var wireItems []wire.PartitionRecordWireItem
		for _, r := range records {
			wireItems = append(wireItems, wire.PartitionRecordWireItem{Name: r.Name, Props: r.Props, Attrs: r.Attrs})
		}
		return wire.PartitionInspectWireResponse{
			PartitionFile: fileOrLoc,
			RecordName:    recordName,
			Props:         props,
			Attrs:         attrs,
			Records:       wireItems,
			HasMore:       hasMore,
		}, nil
	}

	cleanFilename := filepath.Base(fileOrLoc)
	fullPath := filepath.Join(s.dataDir, "partitions", cleanFilename)
	pdb, err := storage.Open(fullPath)
	if err != nil {
		return wire.PartitionInspectWireResponse{}, err
	}
	defer closePartitionDB(pdb, cleanFilename)

	if recordName != "" {
		var cr storage.CompactRecord
		if len(req.Computations) > 0 || len(req.Select) > 0 {
			cr, err = pdb.GetRecordTypedWithCompute(recordName, req.Computations, req.Select...)
		} else {
			cr, err = pdb.GetRecordTyped(recordName)
		}
		if err != nil {
			return wire.PartitionInspectWireResponse{}, err
		}
		props, pErr := cr.ToMap()
		if pErr != nil {
			return wire.PartitionInspectWireResponse{}, pErr
		}
		attrs, aErr := cr.ExpandAttrs()
		if aErr != nil {
			return wire.PartitionInspectWireResponse{}, aErr
		}
		return wire.PartitionInspectWireResponse{
			PartitionFile: cleanFilename,
			RecordName:    recordName,
			Props:         props,
			Attrs:         attrs,
		}, nil
	}

	if limit == 0 || limit > 1000 {
		limit = 50
	}
	recs, hasMore, err := pdb.ListRecordsWithPaging(storage.QueryOptions{
		Limit:        limit,
		Offset:       req.Offset,
		Filters:      req.Filters,
		Select:       req.Select,
		Computations: req.Computations,
	})
	if err != nil {
		return wire.PartitionInspectWireResponse{}, err
	}
	var wireItems []wire.PartitionRecordWireItem
	for _, r := range recs {
		wireItems = append(wireItems, wire.PartitionRecordWireItem{Name: r.Name, Props: r.Props, Attrs: r.Attrs})
	}
	return wire.PartitionInspectWireResponse{
		PartitionFile: cleanFilename,
		Records:       wireItems,
		HasMore:       hasMore,
	}, nil
}

func (s *Server) handleSchedulerStatus(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet, http.MethodPost) {
		return
	}
	tasks := s.scheduler.Tasks()
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success":      true,
		"enabled":      s.cronEnabled,
		"task_timeout": s.scheduler.TaskTimeout().String(),
		"jitter":       s.scheduler.Jitter().String(),
		"tasks":        tasks,
		"uptime_sec":   time.Since(s.startTime).Seconds(),
	})
}

type SchedulerRunRequest struct {
	Task string `json:"task"`
}

func (s *Server) handleSchedulerRun(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "Method not allowed, use POST")
		return
	}

	taskName := r.URL.Query().Get("task")
	if taskName == "" {
		var req SchedulerRunRequest
		if !decodeJSONBody(w, r, &req, true) {
			return
		}
		taskName = req.Task
	}
	taskName = strings.TrimSpace(taskName)
	if taskName == "" {
		writeJSONError(w, http.StatusBadRequest, "Task name is required (pass 'task' query param or JSON {'task': '...'})")
		return
	}

	if err := s.scheduler.RunNow(taskName); err != nil {
		if strings.Contains(err.Error(), "already running") {
			writeJSONError(w, http.StatusConflict, fmt.Sprintf("Task %q is currently running", taskName))
			return
		}
		if strings.Contains(err.Error(), "not found") {
			writeJSONError(w, http.StatusNotFound, fmt.Sprintf("Task %q not found in scheduler", taskName))
			return
		}
		writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to execute task %q: %s", taskName, err.Error()))
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"task":    taskName,
		"message": fmt.Sprintf("Scheduled task %q triggered and executed successfully", taskName),
	})
}

func (s *Server) handleWorkerStatus(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet, http.MethodPost) {
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"worker":  s.worker.Stats(),
	})
}

type CollectorRunRequest struct {
	Model    string `json:"model"`
	MaxBatch int    `json:"max_batch"`
	Async    bool   `json:"async,omitempty"`
}

func (s *Server) handleCollectorRun(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "Method not allowed, use POST")
		return
	}

	var req CollectorRunRequest
	if !decodeJSONBody(w, r, &req, true) {
		return
	}

	if req.MaxBatch < 0 {
		writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("max_batch %d cannot be negative", req.MaxBatch))
		return
	}
	maxBatch := req.MaxBatch
	if maxBatch == 0 {
		maxBatch = 5000
	}

	targetModels, err := s.resolveTargetModels(req.Model)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	isAsync := req.Async || r.URL.Query().Get("async") == "true"
	if isAsync {
		jobID := uuid.New().String()
		modelTag := req.Model
		if modelTag == "" {
			modelTag = "*"
		}
		completedModels := make(map[string]bool, len(targetModels))
		pendingSync := make(map[string]bool, len(targetModels))
		err := s.worker.DispatchAsync(
			s.asyncBaseContext(),
			"collector_run_async",
			func(taskCtx context.Context) error {
				total := 0
				var errs []error
				for _, m := range targetModels {
					if completedModels[m] {
						continue
					}
					if err := taskCtx.Err(); err != nil {
						errs = append(errs, err)
						break
					}
					db, release, err := s.acquireModel(m)
					if err != nil {
						errs = append(errs, fmt.Errorf("model %s: %w", m, err))
						continue
					}
					err = func() error {
						defer release()
						if !pendingSync[m] {
							purged, err := db.CollectTombstones(maxBatch)
							if err != nil {
								return fmt.Errorf("collect %s: %w", m, err)
							}
							total += purged
							if purged == 0 {
								completedModels[m] = true
								return nil
							}
							pendingSync[m] = true
						}
						if err := db.Sync(); err != nil {
							return fmt.Errorf("sync %s: %w", m, err)
						}
						pendingSync[m] = false
						completedModels[m] = true
						return nil
					}()
					if err != nil {
						errs = append(errs, err)
					}
				}
				if total > 0 {
					log.Printf("[WORKER] Async collector sweep succeeded: %d tombstones purged", total)
				}
				return errors.Join(errs...)
			},
			worker.WithID(jobID),
			worker.WithModel(modelTag),
			worker.WithRetry(1, 200*time.Millisecond),
		)
		if err != nil {
			writeJSONError(w, dispatchErrorStatus(err), fmt.Sprintf("Failed to dispatch collector to worker: %s", err.Error()))
			return
		}

		writeJSON(w, http.StatusAccepted, map[string]interface{}{
			"status":  "accepted",
			"job_id":  jobID,
			"async":   true,
			"message": "Tombstone collection task accepted by worker pool",
			"models":  targetModels,
		})
		return
	}

	totalPurged := 0
	for _, m := range targetModels {
		db, release, err := s.acquireModel(m)
		if err != nil {
			writeJSONError(w, dispatchErrorStatus(err), fmt.Sprintf("collector failed opening model %s: %s", m, err.Error()))
			return
		}
		purged, err := db.CollectTombstones(maxBatch)
		if err != nil {
			release()
			writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("collector failed for model %s: %s", m, err.Error()))
			return
		}
		if purged > 0 {
			if err := db.Sync(); err != nil {
				release()
				writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("collector failed to sync model %s: %v", m, err))
				return
			}
		}
		release()
		totalPurged += purged
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success":        true,
		"purged_records": totalPurged,
		"models":         targetModels,
	})
}

type WorkerCancelRequest struct {
	JobID string `json:"job_id"`
	Model string `json:"model"`
	All   bool   `json:"all"`
}

func (s *Server) handleWorkerCancel(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "Method not allowed, use POST")
		return
	}

	var req WorkerCancelRequest
	if !decodeJSONBody(w, r, &req, true) {
		return
	}

	if req.JobID != "" {
		ok := s.worker.CancelByID(req.JobID)
		if !ok {
			writeJSONError(w, http.StatusNotFound, fmt.Sprintf("Task with job_id %q not found in queue", req.JobID))
			return
		}
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"success":        true,
			"job_id":         req.JobID,
			"canceled_tasks": 1,
			"message":        fmt.Sprintf("Task %q canceled from queue", req.JobID),
		})
		return
	}

	if req.Model != "" && req.Model != "*" {
		canceled := s.worker.CancelByModel(req.Model)
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"success":        true,
			"model":          req.Model,
			"canceled_tasks": canceled,
			"message":        fmt.Sprintf("%d queued tasks for model %q canceled", canceled, req.Model),
		})
		return
	}

	if req.All || req.Model == "*" {
		canceled := s.worker.SafeCancel()
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"success":        true,
			"scope":          "global",
			"canceled_tasks": canceled,
			"message":        fmt.Sprintf("%d queued tasks safely canceled globally", canceled),
		})
		return
	}

	writeJSONError(w, http.StatusBadRequest, "Scoped cancellation required. Provide 'job_id', 'model', or 'all: true' for global cancellation")
}

type ModelAttachRequest struct {
	Model  string              `json:"model"`
	Schema storage.ModelSchema `json:"schema"`
}

func (s *Server) handleModelAttach(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	var req ModelAttachRequest
	if !decodeJSONBody(w, r, &req, false) {
		return
	}
	modelName := strings.TrimSpace(req.Model)
	if modelName == "" {
		modelName = "default"
	}
	if len(req.Schema) == 0 {
		writeJSONError(w, http.StatusBadRequest, "schema is required and cannot be empty")
		return
	}

	db, release, err := s.acquireModel(modelName)
	if err != nil {
		writeJSONError(w, dispatchErrorStatus(err), err.Error())
		return
	}
	defer release()

	if err := db.AttachSchema(req.Schema); err != nil {
		if errors.Is(err, storage.ErrSchemaMismatch) {
			writeJSON(w, http.StatusConflict, map[string]interface{}{
				"success": false,
				"error":   "schema_mismatch",
				"model":   modelName,
				"message": err.Error(),
			})
			return
		}
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"model":   modelName,
		"status":  "LOCKED",
		"message": "model schema attached and locked successfully",
	})
}

func (s *Server) handleModelSchema(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}
	modelName := strings.TrimSpace(r.URL.Query().Get("model"))
	if modelName == "" {
		modelName = "default"
	}
	db, release, err := s.acquireExistingModel(modelName)
	if err != nil {
		if errors.Is(err, errModelNotFound) {
			if coldSchema, ok, coldErr := s.GetColdSchema(modelName); coldErr != nil {
				writeJSONError(w, dispatchErrorStatus(coldErr), coldErr.Error())
				return
			} else if ok {
				writeJSON(w, http.StatusOK, map[string]interface{}{
					"success": true,
					"model":   modelName,
					"locked":  true,
					"schema":  coldSchema,
					"tier":    "cold",
				})
				return
			}
			writeJSON(w, http.StatusNotFound, map[string]interface{}{
				"success": false,
				"error":   "no schema attached to model",
				"model":   modelName,
			})
			return
		}
		writeJSONError(w, dispatchErrorStatus(err), err.Error())
		return
	}
	defer release()

	schema, ok := db.GetSchema()
	if !ok {
		if coldSchema, coldOK, coldErr := s.GetColdSchema(modelName); coldErr != nil {
			writeJSONError(w, dispatchErrorStatus(coldErr), coldErr.Error())
			return
		} else if coldOK {
			writeJSON(w, http.StatusOK, map[string]interface{}{
				"success": true,
				"model":   modelName,
				"locked":  true,
				"schema":  coldSchema,
				"tier":    "cold",
			})
			return
		}
		writeJSON(w, http.StatusNotFound, map[string]interface{}{
			"success": false,
			"error":   "no schema attached to model",
			"model":   modelName,
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"model":   modelName,
		"locked":  true,
		"schema":  schema,
	})
}

type ModelResetRequest struct {
	Model   string `json:"model"`
	Confirm bool   `json:"confirm"`
}

func (s *Server) handleModelReset(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	var req ModelResetRequest
	if !decodeJSONBody(w, r, &req, false) {
		return
	}
	if !req.Confirm {
		writeJSONError(w, http.StatusBadRequest, "confirm must be true to reset model schema")
		return
	}
	modelName := strings.TrimSpace(req.Model)
	if modelName == "" {
		modelName = "default"
	}

	db, release, err := s.acquireModel(modelName)
	if err != nil {
		writeJSONError(w, dispatchErrorStatus(err), err.Error())
		return
	}
	defer release()

	if err := db.ResetSchema(); err != nil {
		writeJSONError(w, dispatchErrorStatus(err), err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"model":   modelName,
		"message": "model schema reset successfully",
	})
}

// Environment / flag resolution helpers
func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}

func getEnvBool(key string, fallback bool) (bool, error) {
	val := os.Getenv(key)
	if val == "" {
		return fallback, nil
	}
	switch strings.ToLower(strings.TrimSpace(val)) {
	case "true", "1", "yes":
		return true, nil
	case "false", "0", "no":
		return false, nil
	default:
		return false, fmt.Errorf("invalid boolean env %s=%q", key, val)
	}
}

func getEnvInt(key string, fallback int) (int, error) {
	val := strings.TrimSpace(os.Getenv(key))
	if val == "" {
		return fallback, nil
	}
	i, err := strconv.Atoi(val)
	if err != nil {
		return 0, fmt.Errorf("invalid integer env %s=%q: %w", key, val, err)
	}
	return i, nil
}

func getEnvFloat(key string, fallback float64) (float64, error) {
	val := strings.TrimSpace(os.Getenv(key))
	if val == "" {
		return fallback, nil
	}
	f, err := strconv.ParseFloat(val, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid float env %s=%q: %w", key, val, err)
	}
	return f, nil
}

// resolveDBOptions parses the WAL durability configuration with precedence
// Flag > ENV > default. Defaults to the safe SyncAlways mode.
func resolveDBOptions(flagSyncMode, flagSyncInterval string) (storage.Options, error) {
	modeStr := flagSyncMode
	if modeStr == "" {
		modeStr = getEnv("ANCHORDB_WAL_SYNC", "always")
	}
	opts := storage.DefaultOptions()
	switch strings.ToLower(strings.TrimSpace(modeStr)) {
	case "", "always", "sync", "fsync":
		opts.WAL.SyncMode = storage.SyncAlways
	case "interval", "periodic":
		opts.WAL.SyncMode = storage.SyncInterval
	case "none", "off", "false":
		opts.WAL.SyncMode = storage.SyncNone
	default:
		return opts, fmt.Errorf("invalid wal sync mode %q (want always|interval|none)", modeStr)
	}

	intervalStr := flagSyncInterval
	if intervalStr == "" {
		intervalStr = getEnv("ANCHORDB_WAL_SYNC_INTERVAL", "")
	}
	if intervalStr != "" {
		d, err := time.ParseDuration(intervalStr)
		if err != nil || d <= 0 {
			return opts, fmt.Errorf("invalid wal sync interval %q", intervalStr)
		}
		opts.WAL.SyncInterval = d
	} else if opts.WAL.SyncMode == storage.SyncInterval {
		opts.WAL.SyncInterval = 200 * time.Millisecond
	}
	return opts, nil
}

func main() {
	// 1. Core Server Flags
	flagPort := flag.String("port", "", "Port to listen on (overrides ANCHORDB_PORT, default 7447)")
	flagWirePort := flag.String("wire-port", "", "AnchorWire binary TCP port to listen on (overrides ANCHORDB_WIRE_PORT, default 7448; 'none' or 'off' disables)")
	flagDataDir := flag.String("data-dir", "", "Data directory path (overrides ANCHORDB_DATA_DIR)")
	flagToken := flag.String("auth-token", "", "Bearer auth token (overrides ANCHORDB_AUTH_TOKEN)")
	flagMemLimit := flag.String("memory-limit", "", "RAM allocation limit e.g. 500MB, 1GB, 5GB (overrides ANCHORDB_MEMORY_LIMIT)")
	flag.StringVar(flagMemLimit, "mem-limit", "", "Alias for -memory-limit")
	flag.StringVar(flagMemLimit, "m", "", "Short alias for -memory-limit")

	flagIPWhitelist := flag.String("ip-whitelist", "", "Allowed IP CIDR rules (overrides ANCHORDB_IP_WHITELIST)")
	flagTrustProxy := flag.Bool("trust-proxy", false, "Trust X-Forwarded-For headers (overrides ANCHORDB_TRUST_PROXY)")

	// 2. Cron Scheduler Flags
	flagCronEnabled := flag.Bool("cron-enabled", true, "Enable background cron scheduler (overrides ANCHORDB_CRON_ENABLED)")
	flagPartitionSchedule := flag.String("partition-schedule", "", "Partition cron schedule or HH:MM e.g. 23:00 (overrides ANCHORDB_PARTITION_SCHEDULE)")
	flagRetentionDays := flag.Float64("retention-days", 0, "Default partition archival retention days (overrides ANCHORDB_PARTITION_RETENTION_DAYS)")
	flagSyncInterval := flag.String("sync-interval", "", "Interval for periodic background disk sync e.g. 5m (overrides ANCHORDB_SYNC_INTERVAL)")
	flagCronTaskTimeout := flag.String("cron-task-timeout", "", "Timeout for scheduled cron task execution e.g. 15m (overrides ANCHORDB_CRON_TASK_TIMEOUT)")
	flagCronJitter := flag.String("cron-jitter", "", "Random jitter added to scheduled fire time e.g. 5s (overrides ANCHORDB_CRON_JITTER)")
	flagShutdownTimeout := flag.String("shutdown-timeout", "", "Total graceful shutdown budget e.g. 25s; keep below the orchestrator grace period (ANCHORDB_SHUTDOWN_TIMEOUT, default 25s)")

	// 3. Worker Pool Flags
	flagWorkerEnabled := flag.Bool("worker-enabled", true, "Enable background worker pool (overrides ANCHORDB_WORKER_ENABLED)")
	flagWorkerConcurrency := flag.Int("worker-concurrency", 0, "Worker pool concurrency (overrides ANCHORDB_WORKER_CONCURRENCY)")
	flagWorkerQueueSize := flag.Int("worker-queue-size", 0, "Worker pool task queue size (overrides ANCHORDB_WORKER_QUEUE_SIZE)")
	flagWorkerEnforceMemory := flag.Bool("worker-enforce-memory", true, "Enable worker memory admission control (overrides ANCHORDB_WORKER_ENFORCE_MEMORY)")
	flagWorkerMemoryThreshold := flag.Float64("worker-memory-threshold", 0.85, "Worker memory threshold (overrides ANCHORDB_WORKER_MEMORY_THRESHOLD)")

	// 4. Durability & Abuse-Control Flags
	flagWALSync := flag.String("wal-sync", "", "WAL durability: always|interval|none (overrides ANCHORDB_WAL_SYNC, default always)")
	flagWALSyncInterval := flag.String("wal-sync-interval", "", "WAL fsync interval for wal-sync=interval e.g. 200ms (overrides ANCHORDB_WAL_SYNC_INTERVAL)")
	flagRateLimit := flag.Float64("rate-limit", 0, "Per-client request rate limit in req/s; 0 disables (overrides ANCHORDB_RATE_LIMIT)")
	flagRateLimitBurst := flag.Int("rate-limit-burst", 0, "Per-client burst capacity; defaults to the rate (overrides ANCHORDB_RATE_LIMIT_BURST)")

	// 5. Gateway & Cluster Join Flags
	flagGatewayMode := flag.Bool("gateway-mode", false, "Enable Gateway Mode: requires anchor-connector proxy, disables direct client hits (overrides ANCHORDB_GATEWAY_MODE)")
	flagJoinMode := flag.String("join-mode", "pairing", "Cluster join mode: pairing|otk|session|closed (overrides ANCHORDB_JOIN_MODE)")
	flagPairingTimeout := flag.String("pairing-timeout", "120s", "Pairing window duration e.g. 120s (overrides ANCHORDB_PAIRING_TIMEOUT)")
	flagPairingCIDR := flag.String("pairing-cidr", "", "Allowed CIDR subnet for pairing requests (overrides ANCHORDB_PAIRING_ALLOWED_CIDR)")
	flagSessionSecret := flag.String("session-secret", "", "HMAC secret for connector session tokens (overrides ANCHORDB_SESSION_SECRET)")

	flag.Parse()

	// Resolve Gateway & Cluster Join Parameters (Flag > ENV > Default)
	gatewayMode, err := resolveBoolFlag("gateway-mode", *flagGatewayMode, "ANCHORDB_GATEWAY_MODE", false)
	if err != nil {
		log.Fatalf("[FATAL] Invalid gateway-mode configuration: %v", err)
	}
	joinMode := resolveStringFlag("join-mode", *flagJoinMode, "ANCHORDB_JOIN_MODE", "pairing")

	pairingTimeout, err := resolveDurationFlag("pairing-timeout", *flagPairingTimeout, "ANCHORDB_PAIRING_TIMEOUT", 120*time.Second, false)
	if err != nil {
		log.Fatalf("[FATAL] Invalid pairing-timeout configuration: %v", err)
	}

	pairingCIDR := resolveStringFlag("pairing-cidr", *flagPairingCIDR, "ANCHORDB_PAIRING_ALLOWED_CIDR", "")
	sessionSecret := resolveStringFlag("session-secret", *flagSessionSecret, "ANCHORDB_SESSION_SECRET", "")

	// Resolve Core Parameters with precedence: Flag > ENV > Default
	portStr := resolveStringFlag("port", *flagPort, "ANCHORDB_PORT", "7447")
	wirePortStr := resolveStringFlag("wire-port", *flagWirePort, "ANCHORDB_WIRE_PORT", "7448")
	dataDir := resolveStringFlag("data-dir", *flagDataDir, "ANCHORDB_DATA_DIR", "")
	if dataDir == "" {
		dataDir = "/data"
		if _, err := os.Stat(dataDir); os.IsNotExist(err) {
			dataDir = "./data"
		}
	}

	authToken := resolveStringFlag("auth-token", *flagToken, "ANCHORDB_AUTH_TOKEN", "")
	if *flagToken != "" {
		log.Printf("[WARN] Auth token provided via CLI flag; its value is visible in `ps`/cmdline. Use ANCHORDB_AUTH_TOKEN instead.")
	}
	allowInsecure, err := getEnvBool("ANCHORDB_ALLOW_INSECURE", false)
	if err != nil {
		log.Fatalf("[FATAL] Invalid ANCHORDB_ALLOW_INSECURE configuration: %v", err)
	}
	if authToken == "" && !gatewayMode && !allowInsecure {
		log.Fatalf("[FATAL] ANCHORDB_AUTH_TOKEN is not set. Server refuses to run without authentication. " +
			"Set a strong token (e.g. `openssl rand -hex 32`), enable ANCHORDB_GATEWAY_MODE=true, or for trusted LOCAL access only set ANCHORDB_ALLOW_INSECURE=true.")
	}
	if gatewayMode {
		log.Printf("[INFO] AnchorDB Gateway Mode ACTIVE: direct HTTP/Wire client queries disabled; routing via anchor-connector enforced (JoinMode: %s, PairingTimeout: %v)", joinMode, pairingTimeout)
	}

	memLimitStr := *flagMemLimit
	if !flagPassed("memory-limit") && !flagPassed("mem-limit") && !flagPassed("m") {
		memLimitStr = getEnv("ANCHORDB_MEMORY_LIMIT", "")
	}

	ipWhitelist := resolveStringFlag("ip-whitelist", *flagIPWhitelist, "ANCHORDB_IP_WHITELIST", "")
	trustProxy, err := resolveBoolFlag("trust-proxy", *flagTrustProxy, "ANCHORDB_TRUST_PROXY", false)
	if err != nil {
		log.Fatalf("[FATAL] Invalid trust-proxy configuration: %v", err)
	}

	// Resolve Cron Parameters
	cronEnabled, err := resolveBoolFlag("cron-enabled", *flagCronEnabled, "ANCHORDB_CRON_ENABLED", true)
	if err != nil {
		log.Fatalf("[FATAL] Invalid cron-enabled configuration: %v", err)
	}
	partitionSched := resolveStringFlag("partition-schedule", *flagPartitionSchedule, "ANCHORDB_PARTITION_SCHEDULE", "23:00")

	retentionDays := *flagRetentionDays
	if !flagPassed("retention-days") {
		retentionDays, err = getEnvFloat("ANCHORDB_PARTITION_RETENTION_DAYS", 180)
		if err != nil {
			log.Fatalf("[FATAL] Invalid retention-days configuration: %v", err)
		}
	}
	if retentionDays < 0 || math.IsNaN(retentionDays) || math.IsInf(retentionDays, 0) {
		log.Fatalf("[FATAL] Invalid retention-days value %v: must be >= 0", retentionDays)
	}

	syncInterval := resolveStringFlag("sync-interval", *flagSyncInterval, "ANCHORDB_SYNC_INTERVAL", "5m")

	cronTaskTimeout, err := resolveDurationFlag("cron-task-timeout", *flagCronTaskTimeout, "ANCHORDB_CRON_TASK_TIMEOUT", 0, true)
	if err != nil {
		log.Fatalf("[FATAL] Invalid cron-task-timeout configuration: %v", err)
	}

	cronJitter, err := resolveDurationFlag("cron-jitter", *flagCronJitter, "ANCHORDB_CRON_JITTER", 0, true)
	if err != nil {
		log.Fatalf("[FATAL] Invalid cron-jitter configuration: %v", err)
	}

	// Resolve Worker Parameters
	workerEnabled, err := resolveBoolFlag("worker-enabled", *flagWorkerEnabled, "ANCHORDB_WORKER_ENABLED", true)
	if err != nil {
		log.Fatalf("[FATAL] Invalid worker-enabled configuration: %v", err)
	}

	workerConcurrency := *flagWorkerConcurrency
	if !flagPassed("worker-concurrency") {
		workerConcurrency, err = getEnvInt("ANCHORDB_WORKER_CONCURRENCY", runtime.NumCPU()*2)
		if err != nil {
			log.Fatalf("[FATAL] Invalid worker-concurrency configuration: %v", err)
		}
		if workerConcurrency < 2 {
			workerConcurrency = 2
		}
	} else if workerConcurrency <= 0 {
		log.Fatalf("[FATAL] Invalid worker-concurrency %d: must be > 0", workerConcurrency)
	}

	workerQueueSize := *flagWorkerQueueSize
	if !flagPassed("worker-queue-size") {
		workerQueueSize, err = getEnvInt("ANCHORDB_WORKER_QUEUE_SIZE", 1000)
		if err != nil {
			log.Fatalf("[FATAL] Invalid worker-queue-size configuration: %v", err)
		}
	} else if workerQueueSize <= 0 {
		log.Fatalf("[FATAL] Invalid worker-queue-size %d: must be > 0", workerQueueSize)
	}

	workerEnforceMemory, err := resolveBoolFlag("worker-enforce-memory", *flagWorkerEnforceMemory, "ANCHORDB_WORKER_ENFORCE_MEMORY", true)
	if err != nil {
		log.Fatalf("[FATAL] Invalid worker-enforce-memory configuration: %v", err)
	}

	workerMemoryThreshold := *flagWorkerMemoryThreshold
	if !flagPassed("worker-memory-threshold") {
		workerMemoryThreshold, err = getEnvFloat("ANCHORDB_WORKER_MEMORY_THRESHOLD", 0.85)
		if err != nil {
			log.Fatalf("[FATAL] Invalid worker-memory-threshold configuration: %v", err)
		}
	}
	if workerMemoryThreshold <= 0 || workerMemoryThreshold > 1 {
		log.Fatalf("[FATAL] Invalid worker-memory-threshold %g: must be in (0, 1]", workerMemoryThreshold)
	}

	// Initialize and enforce Soft Memory Limit (GOMEMLIMIT)
	_, memSummary, err := system.ApplyMemoryLimit(memLimitStr)
	if err != nil {
		log.Fatalf("[FATAL] Invalid Memory Limit configuration: %v", err)
	}

	// Single global shutdown budget (L7). Phases get a share so the sum stays
	// within it: HTTP 1/3, scheduler 1/6, worker up to 2x(1/6) = 1/3, leaving
	// ~1/6 for the final per-model Sync.
	shutdownTimeout, err := resolveDurationFlag("shutdown-timeout", *flagShutdownTimeout, "ANCHORDB_SHUTDOWN_TIMEOUT", 25*time.Second, false)
	if err != nil {
		log.Fatalf("[FATAL] Invalid shutdown-timeout configuration: %v", err)
	}

	// Resolve WAL durability options (Flag > ENV > Default).
	dbOptions, err := resolveDBOptions(*flagWALSync, *flagWALSyncInterval)
	if err != nil {
		log.Fatalf("[FATAL] Invalid WAL configuration: %v", err)
	}

	// Resolve per-client rate limit (Flag > ENV > disabled).
	rateLimit := *flagRateLimit
	if !flagPassed("rate-limit") {
		rateLimit, err = getEnvFloat("ANCHORDB_RATE_LIMIT", 0)
		if err != nil {
			log.Fatalf("[FATAL] Invalid rate-limit configuration: %v", err)
		}
	}
	if rateLimit < 0 || math.IsNaN(rateLimit) || math.IsInf(rateLimit, 0) {
		log.Fatalf("[FATAL] Invalid rate-limit %v: must be >= 0", rateLimit)
	}
	rateLimitBurst := *flagRateLimitBurst
	if !flagPassed("rate-limit-burst") {
		rateLimitBurst, err = getEnvInt("ANCHORDB_RATE_LIMIT_BURST", 0)
		if err != nil {
			log.Fatalf("[FATAL] Invalid rate-limit-burst configuration: %v", err)
		}
	}
	if rateLimitBurst < 0 {
		log.Fatalf("[FATAL] Invalid rate-limit-burst %d: must be >= 0", rateLimitBurst)
	}

	serverConfig := ServerConfig{
		Port:               portStr,
		WirePort:           wirePortStr,
		DataDir:            dataDir,
		AuthToken:          authToken,
		MemoryLimit:        memLimitStr,
		IPWhitelist:        ipWhitelist,
		TrustProxy:         trustProxy,
		MemorySummary:      memSummary,
		CronEnabled:        cronEnabled,
		PartitionSchedule:  partitionSched,
		RetentionDays:      retentionDays,
		SyncInterval:       syncInterval,
		CronTaskTimeout:    cronTaskTimeout,
		CronJitter:         cronJitter,
		ShutdownTimeout:    shutdownTimeout,
		DBOptions:          dbOptions,
		RateLimitPerSec:    rateLimit,
		RateLimitBurst:     rateLimitBurst,
		GatewayMode:        gatewayMode,
		JoinMode:           joinMode,
		PairingTimeout:     pairingTimeout,
		PairingAllowedCIDR: pairingCIDR,
		SessionSecret:      sessionSecret,
		WorkerConfig: worker.Config{
			Enabled:         workerEnabled,
			Concurrency:     workerConcurrency,
			QueueSize:       workerQueueSize,
			ShutdownTimeout: shutdownTimeout / 8,
			EnforceMemory:   workerEnforceMemory,
			MemoryThreshold: workerMemoryThreshold,
		},
	}

	// Register signal handling BEFORE any startup work so a SIGTERM during boot
	// is not lost to the default (immediate, no cleanup) handler (L9).
	stopSignal := make(chan os.Signal, 2)
	signal.Notify(stopSignal, os.Interrupt, syscall.SIGTERM)

	server, err := NewServer(serverConfig)
	if err != nil {
		log.Fatalf("[FATAL] Failed to start AnchorDB Server: %v", err)
	}

	fmt.Println("AnchorDB Server")
	fmt.Printf("  HTTP REST   : :%s\n", portStr)
	if wirePortStr != "none" && wirePortStr != "off" && wirePortStr != "" {
		fmt.Printf("  AnchorWire  : :%s (TCP)\n", wirePortStr)
	}
	fmt.Printf("  Data Dir    : %s\n", dataDir)
	if authToken != "" {
		fmt.Println("  Auth        : Enabled")
	} else {
		fmt.Println("  Auth        : Disabled")
	}

	listenErr := make(chan error, 2)

	if wirePortStr != "none" && wirePortStr != "off" && wirePortStr != "" {
		wireAddr := wirePortStr
		if !strings.Contains(wireAddr, ":") {
			wireAddr = ":" + wireAddr
		}
		wireListener, wErr := net.Listen("tcp", wireAddr)
		if wErr != nil {
			log.Fatalf("[FATAL] Failed to bind AnchorWire TCP port %s: %v", wirePortStr, wErr)
		}
		if server.ipFilter != nil && !server.ipFilter.IsEmpty() {
			wireListener = server.ipFilter.WrapListener(wireListener)
		}
		go func() {
			log.Printf("[READY] AnchorWire TCP Binary Protocol listening on %s ...", wireAddr)
			if err := server.WireServer().Serve(wireListener); err != nil && !errors.Is(err, net.ErrClosed) {
				listenErr <- fmt.Errorf("wire server: %w", err)
			}
		}()
	}

	addr := ":" + portStr
	srv := &http.Server{
		Addr:              addr,
		Handler:           server.Routes(),
		ReadTimeout:       15 * time.Second,
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    64 << 10, // cap request headers at 64 KiB (default is 1 MiB)
	}

	go func() {
		log.Printf("[READY] AnchorDB Server listening on %s ...", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			listenErr <- err
		}
	}()

	exitCode := 0
	select {
	case <-stopSignal:
		log.Println("[SHUTDOWN] Stopping server & shutting down background pipeline...")
	case err := <-listenErr:
		// Same graceful path as a signal: never os.Exit past live data.
		log.Printf("[ERROR] Server error: %v", err)
		exitCode = 1
	}

	// A second signal means "stop waiting": force exit. Durability is preserved
	// by WAL replay on the next start.
	go func() {
		<-stopSignal
		log.Println("[SHUTDOWN] Second signal received; force exiting (WAL will be replayed on restart).")
		os.Exit(130)
	}()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), serverConfig.ShutdownTimeout)
	err = server.Shutdown(shutdownCtx, srv)
	shutdownCancel()
	if err != nil {
		log.Printf("[WARN] Shutdown was not completely clean: %v", err)
		exitCode = 1
	} else {
		log.Println("[EXIT] AnchorDB Server stopped gracefully.")
	}
	if exitCode != 0 {
		os.Exit(exitCode)
	}
}

func flagPassedIn(fs *flag.FlagSet, name string) bool {
	found := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == name {
			found = true
		}
	})
	return found
}

func flagPassed(name string) bool {
	return flagPassedIn(flag.CommandLine, name)
}

func resolveBoolFlagIn(fs *flag.FlagSet, name string, flagVal bool, envKey string, defaultVal bool) (bool, error) {
	if flagPassedIn(fs, name) {
		return flagVal, nil
	}
	return getEnvBool(envKey, defaultVal)
}

func resolveBoolFlag(name string, flagVal bool, envKey string, defaultVal bool) (bool, error) {
	return resolveBoolFlagIn(flag.CommandLine, name, flagVal, envKey, defaultVal)
}

func resolveStringFlagIn(fs *flag.FlagSet, name string, flagVal string, envKey string, defaultVal string) string {
	if flagPassedIn(fs, name) {
		return flagVal
	}
	return getEnv(envKey, defaultVal)
}

func resolveStringFlag(name string, flagVal string, envKey string, defaultVal string) string {
	return resolveStringFlagIn(flag.CommandLine, name, flagVal, envKey, defaultVal)
}

func resolveDurationFlagIn(fs *flag.FlagSet, name string, flagVal string, envKey string, defaultVal time.Duration, allowZero bool) (time.Duration, error) {
	raw := resolveStringFlagIn(fs, name, flagVal, envKey, "")
	if strings.TrimSpace(raw) == "" {
		return defaultVal, nil
	}
	d, err := time.ParseDuration(strings.TrimSpace(raw))
	if err != nil {
		return 0, fmt.Errorf("invalid duration %q for %s/%s: %w", raw, name, envKey, err)
	}
	if allowZero {
		if d < 0 {
			return 0, fmt.Errorf("negative duration %q for %s/%s", raw, name, envKey)
		}
	} else if d <= 0 {
		return 0, fmt.Errorf("non-positive duration %q for %s/%s", raw, name, envKey)
	}
	return d, nil
}

func resolveDurationFlag(name string, flagVal string, envKey string, defaultVal time.Duration, allowZero bool) (time.Duration, error) {
	return resolveDurationFlagIn(flag.CommandLine, name, flagVal, envKey, defaultVal, allowZero)
}
