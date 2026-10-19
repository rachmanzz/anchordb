package main

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
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
	RetentionDays     float32
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
}

// Server holds active AnchorDB database instances and background subsystems
type Server struct {
	mu          sync.RWMutex
	dataDir     string
	authToken   string
	port        string
	wirePort    string
	wireServer  *wire.Server
	models      map[string]*storage.AnchorDB
	scheduler   *scheduler.InternalScheduler
	cronEnabled bool
	worker      *worker.Pool
	ipFilter    *security.IPFilter
	startTime   time.Time

	dbOptions storage.Options
	limiter   *rateLimiter

	// Gateway & Cluster Join state
	gatewayMode bool
	joinManager *wire.JoinManager

	// Lifecycle state.
	draining        atomic.Bool   // true once Shutdown began (readiness = 503)
	closed          bool          // guarded by mu; registry sealed after Shutdown
	shutdownTimeout time.Duration // total graceful-shutdown budget
}

func NewServer(cfg ServerConfig) (*Server, error) {
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("cannot create data dir %q: %w", cfg.DataDir, err)
	}
	if probe, err := os.CreateTemp(cfg.DataDir, ".anchordb_write_check_*"); err != nil {
		return nil, fmt.Errorf("data dir %q is not writable: %w", cfg.DataDir, err)
	} else {
		_ = probe.Close()
		_ = os.Remove(probe.Name())
	}
	ipFilter, err := security.NewIPFilter(cfg.IPWhitelist, cfg.TrustProxy)
	if err != nil {
		return nil, fmt.Errorf("invalid IP whitelist %q: %w", cfg.IPWhitelist, err)
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

	s := &Server{
		dataDir:     cfg.DataDir,
		authToken:   cfg.AuthToken,
		port:        cfg.Port,
		wirePort:    cfg.WirePort,
		gatewayMode: cfg.GatewayMode,
		joinManager: joinMgr,
		models:      make(map[string]*storage.AnchorDB),
		scheduler:   sched,
		cronEnabled: cfg.CronEnabled,
		worker:      workerPool,
		ipFilter:    ipFilter,
		startTime:   time.Now(),

		dbOptions: cfg.DBOptions,

		shutdownTimeout: cfg.ShutdownTimeout,
	}
	s.wireServer = wire.NewServerWithJoinManager(s, joinMgr)

	if cfg.RateLimitPerSec > 0 {
		s.limiter = newRateLimiter(cfg.RateLimitPerSec, cfg.RateLimitBurst)
	}

	// Register top-global scheduled tasks if cron is enabled
	if cfg.CronEnabled {
		if cfg.PartitionSchedule != "" {
			if err := s.scheduler.RegisterTask("auto_partition_maintenance", NewAutoPartitionCronTask(s, cfg.PartitionSchedule, cfg.RetentionDays)); err != nil {
				log.Printf("[ERROR] Failed to register cron task auto_partition_maintenance: %v", err)
			}
		}

		if cfg.SyncInterval != "" && cfg.SyncInterval != "0" && cfg.SyncInterval != "off" {
			if err := s.scheduler.RegisterTask("periodic_background_sync", NewPeriodicSyncCronTask(s, cfg.SyncInterval)); err != nil {
				log.Printf("[ERROR] Failed to register cron task periodic_background_sync: %v", err)
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
	mux.HandleFunc("/v1/partition/run", s.authMiddleware(s.handlePartitionRun))
	mux.HandleFunc("/v1/partition/list", s.authMiddleware(s.handlePartitionList))
	mux.HandleFunc("/v1/partition/inspect", s.authMiddleware(s.handlePartitionInspect))
	mux.HandleFunc("/v1/collector/run", s.authMiddleware(s.handleCollectorRun))
	mux.HandleFunc("/v1/scheduler/status", s.authMiddleware(s.handleSchedulerStatus))
	mux.HandleFunc("/v1/scheduler/run", s.authMiddleware(s.handleSchedulerRun))
	mux.HandleFunc("/v1/worker/status", s.authMiddleware(s.handleWorkerStatus))
	mux.HandleFunc("/v1/worker/cancel", s.authMiddleware(s.handleWorkerCancel))
	mux.HandleFunc("/v1/model/attach", s.authMiddleware(s.handleModelAttach))
	mux.HandleFunc("/v1/model/schema", s.authMiddleware(s.handleModelSchema))
	mux.HandleFunc("/v1/model/reset", s.authMiddleware(s.handleModelReset))
	// recoverMiddleware is outermost so a panic in any layer yields a clean 500
	// instead of dropping the connection with a stack trace.
	return recoverMiddleware(s.rateLimitMiddleware(s.ipFilter.Middleware(mux)))
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
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = io.WriteString(w, `{"error":"internal server error"}`)
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func limitRequestBody(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(io.LimitReader(r.Body, maxRequestBodyBytes+1))
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, "failed to read request body")
			return
		}
		if len(body) > maxRequestBodyBytes {
			writeJSONError(w, http.StatusRequestEntityTooLarge, "request body too large")
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		next.ServeHTTP(w, r)
	})
}

func dispatchErrorStatus(err error) int {
	if errors.Is(err, worker.ErrWorkerUnavailable) {
		return http.StatusServiceUnavailable
	}
	return http.StatusInternalServerError
}

// requireMethod writes a 405 response and reports false when r.Method is not one
// of the allowed methods.
func requireMethod(w http.ResponseWriter, r *http.Request, allowed ...string) bool {
	for _, m := range allowed {
		if r.Method == m {
			return true
		}
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusMethodNotAllowed)
	_, _ = io.WriteString(w, fmt.Sprintf(`{"error":"method not allowed; use %s"}`, strings.Join(allowed, " or ")))
	return false
}

func writeJSONError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": message})
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
		if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() ||
			ip.IsLinkLocalMulticast() || ip.IsUnspecified() {
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

	// 1. Drain in-flight HTTP requests.
	if httpSrv != nil {
		hctx, cancel := context.WithTimeout(ctx, budget/3)
		if err := httpSrv.Shutdown(hctx); err != nil {
			errs = append(errs, fmt.Errorf("http drain: %w", err))
			// Shutdown timed out: force-close listeners and connections so no new
			// work arrives (handlers already running cannot be killed in Go).
			_ = httpSrv.Close()
			safeToClose = false
		}
		cancel()
	}

	// 1.5 Stop AnchorWire binary TCP daemon.
	if s.wireServer != nil {
		_ = s.wireServer.Close()
	}

	// 2. Stop the cron scheduler (cancels task contexts, waits for loops).
	if s.scheduler != nil {
		sctx, cancel := context.WithTimeout(ctx, budget/6)
		if err := s.scheduler.Shutdown(sctx); err != nil {
			errs = append(errs, fmt.Errorf("scheduler stop: %w", err))
			safeToClose = false
		}
		cancel()
	}

	// 3. Stop the worker pool, honoring ctx (Pool.Shutdown itself is bounded by
	// its ShutdownTimeout but takes no context).
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
			errs = append(errs, fmt.Errorf("worker stop: %w", ctx.Err()))
			safeToClose = false
		}
	}

	// 4. Flush every model. Close only when nothing can still be using them.
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true // seal the registry: no model may be opened after this point
	for name, db := range s.models {
		if err := db.Sync(); err != nil {
			errs = append(errs, fmt.Errorf("sync model %s: %w", name, err))
		}
		if safeToClose {
			if err := db.Close(); err != nil {
				errs = append(errs, fmt.Errorf("close model %s: %w", name, err))
			}
			delete(s.models, name)
		}
	}
	if !safeToClose {
		if s.dbOptions.WAL.SyncMode == storage.SyncAlways {
			log.Printf("[WARN] Databases not closed because background work may still be running; durability of subsequent writes relies on WAL (sync=always, replayed on restart).")
		} else {
			log.Printf("[WARN] Databases not closed because background work may still be running; subsequent writes after this point are not guaranteed durable under non-sync WAL mode.")
		}
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
	retentionDays float32
}

func NewAutoPartitionCronTask(s *Server, schedule string, retentionDays float32) *AutoPartitionCronTask {
	return &AutoPartitionCronTask{
		server:        s,
		schedule:      schedule,
		retentionDays: retentionDays,
	}
}

func (t *AutoPartitionCronTask) Schedule() string {
	return t.schedule
}

func (t *AutoPartitionCronTask) Execute(ctx context.Context) error {
	log.Printf("[SCHEDULER] Running automatic partition maintenance (archiving data > %.0f days)...", t.retentionDays)
	t.server.mu.RLock()
	var loaded []string
	for m := range t.server.models {
		loaded = append(loaded, m)
	}
	t.server.mu.RUnlock()

	var dispatchErr error
	for _, m := range loaded {
		modelName := m
		err := t.server.worker.Dispatch(
			ctx,
			"cron_partition_"+modelName,
			func(wCtx context.Context) error {
				db, err := t.server.getModel(modelName)
				if err != nil {
					return err
				}
				partRes, err := partition.ExecutePartition(db, partition.PartitionPolicy{
					ModelName:     modelName,
					DateField:     "date",
					RetentionDays: t.retentionDays,
					Sink:          partition.NewLocalDiskSink(filepath.Join(t.server.dataDir, "partitions")),
					FileFormat:    "{date}_{uuid}.anchordb",
					PruneHotDB:    true,
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
			log.Printf("[WARN] AutoPartition failed to dispatch task for model %q: %v", modelName, err)
			dispatchErr = err
		}
	}
	return dispatchErr
}

func (t *AutoPartitionCronTask) OnError(ctx context.Context, err error) {
	log.Printf("[SCHEDULER] AutoPartitionCronTask recoverable error: %v", err)
}

func (t *AutoPartitionCronTask) OnFail(ctx context.Context, err error) {
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
	return t.server.worker.Dispatch(
		ctx,
		"periodic_disk_sync",
		func(wCtx context.Context) error {
			t.server.mu.RLock()
			var dbs []*storage.AnchorDB
			for _, d := range t.server.models {
				dbs = append(dbs, d)
			}
			t.server.mu.RUnlock()

			var synced int
			var purged int
			var syncErrs []error
			for _, d := range dbs {
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
			}
			if purged > 0 || synced > 0 {
				log.Printf("[WORKER] Periodic background sync succeeded: %d models synced to disk, %d tombstones collected", synced, purged)
			}
			return errors.Join(syncErrs...)
		},
		worker.WithRetry(1, 200*time.Millisecond),
	)
}

func (t *PeriodicSyncCronTask) OnError(ctx context.Context, err error) {
	log.Printf("[SCHEDULER] PeriodicSyncCronTask recoverable error: %v", err)
}

func (t *PeriodicSyncCronTask) OnFail(ctx context.Context, err error) {
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

// getModel retrieves or dynamically opens an AnchorDB instance ("One DB = One Model")
func (s *Server) getModel(modelName string) (*storage.AnchorDB, error) {
	cleanName, err := sanitizeModelName(modelName)
	if err != nil {
		return nil, err
	}
	modelName = cleanName

	s.mu.RLock()
	db, exists := s.models[modelName]
	s.mu.RUnlock()
	if exists {
		return db, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// After Shutdown closed every model the registry is sealed: a late handler
	// must not open a new DB that would never be Synced/Closed (L11).
	if s.closed {
		return nil, ErrServerClosed
	}

	// Double check
	if db, exists := s.models[modelName]; exists {
		return db, nil
	}

	// Prevent unbounded model creation from exhausting file descriptors and memory
	const maxLoadedModels = 500
	if len(s.models) >= maxLoadedModels {
		return nil, fmt.Errorf("maximum loaded models limit reached (%d)", maxLoadedModels)
	}

	dbPath := filepath.Join(s.dataDir, modelName+".anchordb")
	newDB, err := storage.OpenWithOptions(dbPath, s.dbOptions)
	if err != nil {
		// Log full detail (including the on-disk path) server-side only; the
		// client gets a generic message so internal paths are not disclosed.
		log.Printf("[ERROR] Failed to open model %q at %s: %v", modelName, dbPath, err)
		return nil, fmt.Errorf("failed to open model %q", modelName)
	}

	s.models[modelName] = newDB
	log.Printf("[INFO] Model %q loaded from disk: %s", modelName, dbPath)
	return newDB, nil
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
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
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
	status, code := "ok", http.StatusOK
	if s.draining.Load() {
		// Readiness: tell load balancers / K8s to stop routing new traffic.
		status, code = "shutting_down", http.StatusServiceUnavailable
	}

	// Unauthenticated callers only get liveness/readiness. Internal state
	// (loaded model names, memory, scheduler/worker stats) requires auth.
	if !s.isAuthorized(r) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
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

	resp := map[string]interface{}{
		"status":         status,
		"engine":         "AnchorDB",
		"uptime_seconds": time.Since(s.startTime).Seconds(),
		"port":           s.port,
		"loaded_models":  loadedModels,
		"memory":         system.GetMemoryStats(),
		"background": map[string]interface{}{
			"scheduler": map[string]interface{}{
				"enabled": s.cronEnabled,
				"tasks":   len(s.scheduler.Tasks()),
			},
			"worker": s.worker.Stats(),
		},
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(resp)
}

// handleMetrics exposes dependency-free Prometheus text-format metrics for
// scraping. It is intentionally unauthenticated, like /health, so that standard
// scrapers work; put it behind your reverse proxy if that is not acceptable.
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	models := len(s.models)
	s.mu.RUnlock()

	mem := system.GetMemoryStats()
	ws := s.worker.Stats()

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

	_, _ = io.WriteString(w, b.String())
}

// Request / Response types for REST API
type SaveRequest struct {
	Model     string             `json:"model"`
	Name      string             `json:"name"`
	Operation *uint8             `json:"operation,omitempty"`
	Props     map[string]float32 `json:"props"`
	Coord     *spatial.Point2D   `json:"coord,omitempty"`
	Roles     *[4]float32        `json:"roles,omitempty"`
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
	if _, err := strconv.ParseFloat(expr, 32); err == nil {
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

// parseVal converts a filter value to float32. An empty string is treated as
// zero (filters that do not use ValueHigh leave it empty). A non-empty value
// that parses as neither a date nor a number is a hard error so callers cannot
// silently match/update unintended records.
func parseVal(str string) (float32, error) {
	if str == "" {
		return 0, nil
	}
	if d, err := storage.ParseDate(str); err == nil {
		return d, nil
	}
	if f, err := strconv.ParseFloat(str, 32); err == nil {
		return float32(f), nil
	}
	return 0, fmt.Errorf("invalid numeric value %q", str)
}

// buildFilters converts API filter DTOs into storage filter conditions,
// rejecting any value that is not a valid number or date.
func buildFilters(filters []FilterDTO) ([]storage.FilterCondition, error) {
	out := make([]storage.FilterCondition, 0, len(filters))
	for _, f := range filters {
		value, err := parseVal(f.Value)
		if err != nil {
			return nil, fmt.Errorf("filter field %q: %w", f.Field, err)
		}
		valueHigh, err := parseVal(f.ValueHigh)
		if err != nil {
			return nil, fmt.Errorf("filter field %q (value_high): %w", f.Field, err)
		}
		out = append(out, storage.FilterCondition{
			Field:     f.Field,
			Operator:  f.Operator,
			Value:     value,
			ValueHigh: valueHigh,
		})
	}
	return out, nil
}

func (s *Server) handleSave(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	var req SaveRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid json body")
		return
	}

	db, err := s.getModel(req.Model)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
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
		if len(req.Props) > 0 {
			name, err := db.SaveRecordWithNode(effectiveName, opStatus, req.Props, coord, roles)
			if err != nil {
				if errors.Is(err, storage.ErrMissingRequiredField) ||
					errors.Is(err, storage.ErrUnknownField) ||
					errors.Is(err, storage.ErrInvalidFieldType) {
					writeJSONError(w, http.StatusBadRequest, err.Error())
					return
				}
				writeJSONError(w, http.StatusInternalServerError, err.Error())
				return
			}
			effectiveName = name
		} else {
			if effectiveName == "" {
				effectiveName = uuid.NewString()
			}
			if _, err := db.AppendNode(effectiveName, coord, roles, uint16(opStatus)); err != nil {
				writeJSONError(w, http.StatusInternalServerError, err.Error())
				return
			}
		}
	} else {
		name, err := db.SaveRecordWithIDAndOp(req.Name, opStatus, req.Props)
		if err != nil {
			if errors.Is(err, storage.ErrMissingRequiredField) ||
				errors.Is(err, storage.ErrUnknownField) ||
				errors.Is(err, storage.ErrInvalidFieldType) {
				writeJSONError(w, http.StatusBadRequest, err.Error())
				return
			}
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
		effectiveName = name
	}

	_ = json.NewEncoder(w).Encode(map[string]interface{}{
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
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid json body")
		return
	}
	if len(req.Records) == 0 {
		writeJSONError(w, http.StatusBadRequest, "records array cannot be empty")
		return
	}

	db, err := s.getModel(req.Model)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
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
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"count":   len(names),
		"names":   names,
		"message": "batch records saved",
	})
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	var req GetRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid json body")
		return
	}

	db, err := s.getModel(req.Model)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	_, op, err := db.GetRecordWithOperation(req.Name)
	if err != nil {
		writeJSONError(w, http.StatusNotFound, err.Error())
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

	props, err := db.GetRecordWithCompute(req.Name, computations, req.Select...)
	if err != nil {
		writeJSONError(w, http.StatusNotFound, err.Error())
		return
	}

	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"success":   true,
		"name":      req.Name,
		"operation": op,
		"props":     props,
	})
}

func (s *Server) handleList(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	var req ListRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid json body")
		return
	}

	db, err := s.getModel(req.Model)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

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

	limit := req.Limit
	if limit <= 0 {
		limit = 10
	}
	if limit > maxQueryLimit {
		limit = maxQueryLimit
	}
	offset := req.Offset
	if req.Page > 1 {
		offset = (req.Page - 1) * limit
	}
	if offset < 0 {
		offset = 0
	}

	records, hasMore, err := db.ListRecordsWithPaging(storage.QueryOptions{
		Select:               req.Select,
		Filters:              filters,
		Limit:                limit,
		Offset:               offset,
		Computations:         computations,
		IncludeAllOperations: req.IncludeAllOperations,
		OperationFilter:      req.Operation,
		AllowedOperations:    req.AllowedOperations,
	})
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"success":  true,
		"items":    records,
		"has_more": hasMore,
		"count":    len(records),
	})
}

func (s *Server) handleUpdate(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	var req UpdateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid json body")
		return
	}

	db, err := s.getModel(req.Model)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
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
		jobID := uuid.New().String()
		err := s.worker.DispatchAsync(
			context.Background(),
			"update_"+req.Model,
			func(ctx context.Context) error {
				count, err := db.UpdateRecords(storage.UpdateOptions{
					TargetEntities: req.In,
					Filters:        filters,
					Mutations:      mutations,
					Limit:          req.Limit,
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
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"status":  "accepted",
			"job_id":  jobID,
			"model":   req.Model,
			"async":   true,
			"message": fmt.Sprintf("Bulk update for model %q accepted by worker pool", req.Model),
		})
		return
	}

	count, err := db.UpdateRecords(storage.UpdateOptions{
		TargetEntities: req.In,
		Filters:        filters,
		Mutations:      mutations,
		Limit:          req.Limit,
	})
	if err != nil {
		if errors.Is(err, storage.ErrUnknownField) ||
			errors.Is(err, storage.ErrInvalidFieldType) ||
			errors.Is(err, storage.ErrMissingRequiredField) {
			writeJSONError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"success":       true,
		"updated_count": count,
	})
}

func (s *Server) handleGraph(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	var req GraphRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid json body")
		return
	}

	db, err := s.getModel(req.Model)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	limit := req.LimitK
	if limit <= 0 {
		limit = 5
	}
	beamWidth := req.BeamWidth
	if beamWidth <= 0 {
		beamWidth = 0.05
	}

	if len(req.Relations) > 0 {
		steps, err := db.ExecuteGraphChainWithContext(r.Context(), req.Inputs, req.Relations, limit, beamWidth)
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
		seq := make([]string, len(steps))
		for i, s := range steps {
			seq[i] = s.Word
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
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
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	_ = json.NewEncoder(w).Encode(map[string]interface{}{
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
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid json body")
		return
	}

	db, err := s.getModel(req.Model)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	switch strings.ToLower(strings.TrimSpace(req.Action)) {
	case "", "add", "link":
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

	_ = json.NewEncoder(w).Encode(map[string]interface{}{
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
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid json body")
		return
	}

	db, err := s.getModel(req.Model)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	limit := req.LimitK
	if limit <= 0 {
		limit = 5
	}

	var results []storage.CausalResult
	if len(req.Relations) > 0 {
		results, err = db.ExecuteCausalChain(r.Context(), req.Seeds, req.Relations, limit)
	} else {
		results, err = db.ExecuteCausalStep(r.Context(), req.Seeds, req.Relation, limit)
	}
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"success":    true,
		"results":    results,
		"edge_count": db.EdgeCount(),
	})
}

func (s *Server) handleMath(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	var req MathRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid json body")
		return
	}

	db, err := s.getModel(req.Model)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	switch strings.ToLower(strings.TrimSpace(req.Op)) {
	case "distance":
		if len(req.Words) < 2 {
			writeJSONError(w, http.StatusBadRequest, "distance requires at least 2 words")
			return
		}
		dist, err := db.CalculateDistance(req.Words[0], req.Words[1])
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, err.Error())
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"success":  true,
			"distance": dist,
		})
	case "centroid":
		if len(req.Words) < 1 {
			writeJSONError(w, http.StatusBadRequest, "centroid requires at least 1 word")
			return
		}
		coord, roles, err := db.CalculateCentroid(req.Words)
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, err.Error())
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
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
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, err.Error())
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"success":    true,
			"similarity": sim,
		})
	case "nearest":
		if len(req.Words) < 1 {
			writeJSONError(w, http.StatusBadRequest, "nearest requires at least 1 word")
			return
		}
		limit := req.LimitK
		if limit <= 0 {
			limit = 5
		}
		neighbors, err := db.FindNearestNeighbors(req.Words[0], limit)
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, err.Error())
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"success":   true,
			"neighbors": neighbors,
		})
	case "haversine":
		if len(req.Words) < 2 {
			writeJSONError(w, http.StatusBadRequest, "haversine requires at least 2 words")
			return
		}
		dist, err := db.CalculateHaversineDistance(req.Words[0], req.Words[1])
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, err.Error())
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
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
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, err.Error())
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
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
	modelName := r.URL.Query().Get("model")
	if modelName == "" {
		modelName = "default"
	}

	db, err := s.getModel(modelName)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// If async requested, offload to worker pool and return immediately
	if r.URL.Query().Get("async") == "true" {
		jobID := uuid.New().String()
		err := s.worker.DispatchAsync(
			context.Background(),
			"sync_"+modelName,
			func(ctx context.Context) error {
				return db.Sync()
			},
			worker.WithID(jobID),
			worker.WithModel(modelName),
			worker.WithRetry(1, 200*time.Millisecond),
		)
		if err != nil {
			writeJSONError(w, dispatchErrorStatus(err), fmt.Sprintf("failed to dispatch sync task to worker: %s", err.Error()))
			return
		}
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"status":  "accepted",
			"job_id":  jobID,
			"model":   modelName,
			"async":   true,
			"message": fmt.Sprintf("Sync for model %q queued to background worker engine", modelName),
		})
		return
	}

	if err := db.Sync(); err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"message": "sync complete",
	})
}

type PartitionRunRequest struct {
	Model         string  `json:"model"`
	DateField     string  `json:"date_field"`
	RetentionDays float32 `json:"retention_days"`
	SinkType      string  `json:"sink_type"` // "local" or "s3"
	TargetPath    string  `json:"target_path"`
	S3Endpoint    string  `json:"s3_endpoint"`
	S3Bucket      string  `json:"s3_bucket"`
	S3Prefix      string  `json:"s3_prefix"`
	S3AccessKey   string  `json:"s3_access_key"`
	S3SecretKey   string  `json:"s3_secret_key"`
	PruneHotDB    *bool   `json:"prune_hot_db"`
	Async         bool    `json:"async"` // If true, offloads to worker pool (returns HTTP 202)
}

func (s *Server) handlePartitionRun(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "Method not allowed, use POST")
		return
	}

	var req PartitionRunRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil && err.Error() != "EOF" {
		writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("Invalid request body: %s", err.Error()))
		return
	}

	dateField := req.DateField
	if dateField == "" {
		dateField = "date"
	}

	retentionDays := req.RetentionDays
	if retentionDays <= 0 {
		retentionDays = 180 // Default 6 months
	}

	prune := true
	if req.PruneHotDB != nil {
		prune = *req.PruneHotDB
	}

	var sink partition.PartitionSink
	if strings.ToLower(req.SinkType) == "s3" {
		if err := validateS3Endpoint(req.S3Endpoint, getEnvBool("ANCHORDB_ALLOW_S3_PRIVATE_ENDPOINT", false)); err != nil {
			writeJSONError(w, http.StatusBadRequest, err.Error())
			return
		}
		endpoint := req.S3Endpoint
		bucket := req.S3Bucket
		if bucket == "" {
			bucket = "anchordb-archive"
		}
		prefix := req.S3Prefix
		if prefix == "" {
			prefix = "partitions"
		}
		s3sink := partition.NewS3Sink(endpoint, bucket, prefix, req.S3AccessKey, req.S3SecretKey)
		s3sink.AllowInsecureHTTP = getEnvBool("ANCHORDB_ALLOW_INSECURE_S3_HTTP", false)
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

	var targetModels []string
	if req.Model != "" && req.Model != "all" {
		targetModels = []string{req.Model}
	} else {
		s.mu.RLock()
		for m := range s.models {
			targetModels = append(targetModels, m)
		}
		s.mu.RUnlock()
	}

	isAsync := req.Async || r.URL.Query().Get("async") == "true"

	// 1. Asynchronous execution via Worker Engine
	if isAsync {
		jobID := uuid.New().String()
		modelTag := req.Model
		if modelTag == "" {
			modelTag = "all"
		}
		err := s.worker.DispatchAsync(
			context.Background(),
			"partition_run_async",
			func(ctx context.Context) error {
				var errs []error
				for _, m := range targetModels {
					db, err := s.getModel(m)
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
					})
					if err != nil {
						errs = append(errs, fmt.Errorf("partition %s: %w", m, err))
					} else if res != nil && res.ArchivedCount > 0 {
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

		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
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
		db, err := s.getModel(m)
		if err != nil {
			continue
		}
		res, err := partition.ExecutePartition(db, partition.PartitionPolicy{
			ModelName:     m,
			DateField:     dateField,
			RetentionDays: retentionDays,
			Sink:          sink,
			FileFormat:    "{date}_{uuid}.anchordb",
			PruneHotDB:    prune,
		})
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("Partition failed for model %s: %s", m, err.Error()))
			return
		}

		results = append(results, res)
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"results": results,
	})
}

// handlePartitionList lists available partition archives in s.dataDir/partitions
func (s *Server) handlePartitionList(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet, http.MethodPost) {
		return
	}
	partDir := filepath.Join(s.dataDir, "partitions")
	entries, err := os.ReadDir(partDir)
	if err != nil {
		if os.IsNotExist(err) {
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"success":    true,
				"partitions": []interface{}{},
			})
			return
		}
		writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("failed to read partitions directory: %s", err.Error()))
		return
	}

	type PartitionMetaDTO struct {
		Name      string    `json:"name"`
		SizeBytes int64     `json:"size_bytes"`
		ModTime   time.Time `json:"mod_time"`
	}

	var partitions []PartitionMetaDTO
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".anchordb") {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		partitions = append(partitions, PartitionMetaDTO{
			Name:      e.Name(),
			SizeBytes: info.Size(),
			ModTime:   info.ModTime(),
		})
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"success":    true,
		"partitions": partitions,
	})
}

type PartitionInspectRequest struct {
	PartitionFile string `json:"partition_file"`
	Name          string `json:"name,omitempty"`
	Limit         int    `json:"limit,omitempty"`
}

// handlePartitionInspect safely reads records from an archived partition file via the engine.
func (s *Server) handlePartitionInspect(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	var req PartitionInspectRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid json body")
		return
	}
	if req.PartitionFile == "" {
		writeJSONError(w, http.StatusBadRequest, "partition_file is required")
		return
	}

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
	defer pdb.Close()

	if req.Name != "" {
		props, err := pdb.GetRecord(req.Name)
		if err != nil {
			writeJSONError(w, http.StatusNotFound, fmt.Sprintf("record %q not found in partition", req.Name))
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"success":        true,
			"partition_file": cleanFilename,
			"name":           req.Name,
			"props":          props,
		})
		return
	}

	limit := req.Limit
	if limit <= 0 || limit > 100 {
		limit = 10
	}
	records, _, err := pdb.ListRecordsWithPaging(storage.QueryOptions{Limit: limit})
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("failed to list records in partition: %s", err.Error()))
		return
	}

	type RecordDTO struct {
		Name  string             `json:"name"`
		Props map[string]float32 `json:"props"`
	}
	out := make([]RecordDTO, len(records))
	for i, r := range records {
		out[i] = RecordDTO{Name: r.Name, Props: r.Props}
	}

	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"success":        true,
		"partition_file": cleanFilename,
		"records":        out,
		"count":          len(out),
	})
}

func (s *Server) handleSchedulerStatus(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet, http.MethodPost) {
		return
	}
	tasks := s.scheduler.Tasks()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
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
		if err := json.NewDecoder(r.Body).Decode(&req); err == nil {
			taskName = req.Task
		}
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

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"task":    taskName,
		"message": fmt.Sprintf("Scheduled task %q triggered and executed successfully", taskName),
	})
}

func (s *Server) handleWorkerStatus(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet, http.MethodPost) {
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
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
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil && err.Error() != "EOF" {
		writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("Invalid request body: %s", err.Error()))
		return
	}

	maxBatch := req.MaxBatch
	if maxBatch <= 0 {
		maxBatch = 5000
	}

	var targetModels []string
	if req.Model != "" && req.Model != "*" {
		targetModels = []string{req.Model}
	} else {
		s.mu.RLock()
		for m := range s.models {
			targetModels = append(targetModels, m)
		}
		s.mu.RUnlock()
	}

	isAsync := req.Async || r.URL.Query().Get("async") == "true"
	if isAsync {
		jobID := uuid.New().String()
		modelTag := req.Model
		if modelTag == "" {
			modelTag = "*"
		}
		err := s.worker.DispatchAsync(
			context.Background(),
			"collector_run_async",
			func(ctx context.Context) error {
				total := 0
				var errs []error
				for _, m := range targetModels {
					db, err := s.getModel(m)
					if err != nil {
						errs = append(errs, fmt.Errorf("model %s: %w", m, err))
						continue
					}
					purged, err := db.CollectTombstones(maxBatch)
					if err != nil {
						errs = append(errs, fmt.Errorf("collect %s: %w", m, err))
						continue
					}
					if purged > 0 {
						if err := db.Sync(); err != nil {
							errs = append(errs, fmt.Errorf("sync %s: %w", m, err))
						}
					}
					total += purged
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

		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
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
		db, err := s.getModel(m)
		if err != nil {
			continue
		}
		purged, err := db.CollectTombstones(maxBatch)
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("collector failed for model %s: %s", m, err.Error()))
			return
		}
		if purged > 0 {
			if err := db.Sync(); err != nil {
				writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("collector failed to sync model %s: %v", m, err))
				return
			}
		}
		totalPurged += purged
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
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
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil && err.Error() != "EOF" {
		writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("Invalid request body: %s", err.Error()))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if req.JobID != "" {
		ok := s.worker.CancelByID(req.JobID)
		if !ok {
			writeJSONError(w, http.StatusNotFound, fmt.Sprintf("Task with job_id %q not found in queue", req.JobID))
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"success":        true,
			"job_id":         req.JobID,
			"canceled_tasks": 1,
			"message":        fmt.Sprintf("Task %q canceled from queue", req.JobID),
		})
		return
	}

	if req.Model != "" && req.Model != "*" {
		canceled := s.worker.CancelByModel(req.Model)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"success":        true,
			"model":          req.Model,
			"canceled_tasks": canceled,
			"message":        fmt.Sprintf("%d queued tasks for model %q canceled", canceled, req.Model),
		})
		return
	}

	if req.All || req.Model == "*" {
		canceled := s.worker.SafeCancel()
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
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
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid json body")
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

	db, err := s.getModel(modelName)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	if err := db.AttachSchema(req.Schema); err != nil {
		if errors.Is(err, storage.ErrSchemaMismatch) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusConflict)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
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

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
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
	db, err := s.getModel(modelName)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	schema, ok := db.GetSchema()
	if !ok {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"success": false,
			"error":   "no schema attached to model",
			"model":   modelName,
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
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
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid json body")
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

	db, err := s.getModel(modelName)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	if err := db.ResetSchema(); err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
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

func getEnvBool(key string, fallback bool) bool {
	val := os.Getenv(key)
	if val == "" {
		return fallback
	}
	val = strings.ToLower(val)
	return val == "true" || val == "1" || val == "yes"
}

func getEnvInt(key string, fallback int) int {
	val := os.Getenv(key)
	if val == "" {
		return fallback
	}
	if i, err := strconv.Atoi(val); err == nil {
		return i
	}
	return fallback
}

func getEnvFloat(key string, fallback float64) float64 {
	val := os.Getenv(key)
	if val == "" {
		return fallback
	}
	if f, err := strconv.ParseFloat(val, 64); err == nil {
		return f
	}
	return fallback
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
	flagWorkerEnforceMemory := flag.Bool("worker-enforce-memory", false, "Enable worker memory admission control (overrides ANCHORDB_WORKER_ENFORCE_MEMORY)")
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

	// Resolve Gateway & Cluster Join Parameters
	gatewayMode := *flagGatewayMode
	if !gatewayMode || !flagPassed("gateway-mode") {
		gatewayMode = getEnvBool("ANCHORDB_GATEWAY_MODE", false)
	}

	joinMode := *flagJoinMode
	if joinMode == "" || !flagPassed("join-mode") {
		joinMode = getEnv("ANCHORDB_JOIN_MODE", "pairing")
	}

	pairingTimeout := 120 * time.Second
	pairingTimeoutStr := *flagPairingTimeout
	if pairingTimeoutStr == "" || !flagPassed("pairing-timeout") {
		pairingTimeoutStr = getEnv("ANCHORDB_PAIRING_TIMEOUT", "120s")
	}
	if d, perr := time.ParseDuration(pairingTimeoutStr); perr == nil && d > 0 {
		pairingTimeout = d
	}

	pairingCIDR := *flagPairingCIDR
	if pairingCIDR == "" || !flagPassed("pairing-cidr") {
		pairingCIDR = getEnv("ANCHORDB_PAIRING_ALLOWED_CIDR", "")
	}

	sessionSecret := *flagSessionSecret
	if sessionSecret == "" || !flagPassed("session-secret") {
		sessionSecret = getEnv("ANCHORDB_SESSION_SECRET", "")
	}

	// Resolve Core Parameters with precedence: Flag > ENV > Default
	portStr := *flagPort
	if portStr == "" {
		portStr = getEnv("ANCHORDB_PORT", "7447")
	}

	wirePortStr := *flagWirePort
	if wirePortStr == "" {
		wirePortStr = getEnv("ANCHORDB_WIRE_PORT", "7448")
	}

	dataDir := *flagDataDir
	if dataDir == "" {
		dataDir = getEnv("ANCHORDB_DATA_DIR", "")
	}
	if dataDir == "" {
		dataDir = "/data"
		if _, err := os.Stat(dataDir); os.IsNotExist(err) {
			dataDir = "./data"
		}
	}

	authToken := *flagToken
	if authToken == "" {
		authToken = getEnv("ANCHORDB_AUTH_TOKEN", "")
	}
	if *flagToken != "" {
		log.Printf("[WARN] Auth token provided via CLI flag; its value is visible in `ps`/cmdline. Use ANCHORDB_AUTH_TOKEN instead.")
	}
	if authToken == "" && !gatewayMode && !getEnvBool("ANCHORDB_ALLOW_INSECURE", false) {
		log.Fatalf("[FATAL] ANCHORDB_AUTH_TOKEN is not set. Server refuses to run without authentication. " +
			"Set a strong token (e.g. `openssl rand -hex 32`), enable ANCHORDB_GATEWAY_MODE=true, or for trusted LOCAL access only set ANCHORDB_ALLOW_INSECURE=true.")
	}
	if gatewayMode {
		log.Printf("[INFO] AnchorDB Gateway Mode ACTIVE: direct HTTP/Wire client queries disabled; routing via anchor-connector enforced (JoinMode: %s, PairingTimeout: %v)", joinMode, pairingTimeout)
	}

	memLimitStr := *flagMemLimit
	if memLimitStr == "" {
		memLimitStr = getEnv("ANCHORDB_MEMORY_LIMIT", "")
	}

	ipWhitelist := *flagIPWhitelist
	if ipWhitelist == "" {
		ipWhitelist = getEnv("ANCHORDB_IP_WHITELIST", "")
	}

	trustProxy := *flagTrustProxy
	if !trustProxy {
		trustProxy = getEnvBool("ANCHORDB_TRUST_PROXY", false)
	}

	// Resolve Cron Parameters
	cronEnabled := *flagCronEnabled
	if !cronEnabled || !flagPassed("cron-enabled") {
		cronEnabled = getEnvBool("ANCHORDB_CRON_ENABLED", true)
	}

	partitionSched := *flagPartitionSchedule
	if partitionSched == "" {
		partitionSched = getEnv("ANCHORDB_PARTITION_SCHEDULE", "23:00")
	}

	retentionDays := float32(*flagRetentionDays)
	if retentionDays <= 0 {
		retentionDays = float32(getEnvFloat("ANCHORDB_PARTITION_RETENTION_DAYS", 180))
	}

	syncInterval := *flagSyncInterval
	if syncInterval == "" {
		syncInterval = getEnv("ANCHORDB_SYNC_INTERVAL", "5m")
	}

	var cronTaskTimeout time.Duration
	if cronTaskTimeoutStr := getEnv("ANCHORDB_CRON_TASK_TIMEOUT", *flagCronTaskTimeout); cronTaskTimeoutStr != "" {
		if d, err := time.ParseDuration(cronTaskTimeoutStr); err == nil {
			cronTaskTimeout = d
		}
	}

	var cronJitter time.Duration
	if cronJitterStr := getEnv("ANCHORDB_CRON_JITTER", *flagCronJitter); cronJitterStr != "" {
		if d, err := time.ParseDuration(cronJitterStr); err == nil {
			cronJitter = d
		}
	}

	// Resolve Worker Parameters
	workerEnabled := *flagWorkerEnabled
	if !workerEnabled || !flagPassed("worker-enabled") {
		workerEnabled = getEnvBool("ANCHORDB_WORKER_ENABLED", true)
	}

	workerConcurrency := *flagWorkerConcurrency
	if workerConcurrency <= 0 {
		workerConcurrency = getEnvInt("ANCHORDB_WORKER_CONCURRENCY", runtime.NumCPU()*2)
		if workerConcurrency < 2 {
			workerConcurrency = 2
		}
	}

	workerQueueSize := *flagWorkerQueueSize
	if workerQueueSize <= 0 {
		workerQueueSize = getEnvInt("ANCHORDB_WORKER_QUEUE_SIZE", 1000)
	}

	workerEnforceMemory := *flagWorkerEnforceMemory
	if !workerEnforceMemory || !flagPassed("worker-enforce-memory") {
		workerEnforceMemory = getEnvBool("ANCHORDB_WORKER_ENFORCE_MEMORY", false)
	}

	workerMemoryThreshold := *flagWorkerMemoryThreshold
	if workerMemoryThreshold <= 0 || !flagPassed("worker-memory-threshold") {
		workerMemoryThreshold = getEnvFloat("ANCHORDB_WORKER_MEMORY_THRESHOLD", 0.85)
	}

	// Initialize and enforce Soft Memory Limit (GOMEMLIMIT)
	_, memSummary, err := system.ApplyMemoryLimit(memLimitStr)
	if err != nil {
		log.Fatalf("[FATAL] Invalid Memory Limit configuration: %v", err)
	}

	// Single global shutdown budget (L7). Phases get a share so the sum stays
	// within it: HTTP 1/3, scheduler 1/6, worker up to 2x(1/6) = 1/3, leaving
	// ~1/6 for the final per-model Sync.
	shutdownTimeout := 25 * time.Second
	if s := getEnv("ANCHORDB_SHUTDOWN_TIMEOUT", *flagShutdownTimeout); s != "" {
		if d, perr := time.ParseDuration(s); perr == nil && d > 0 {
			shutdownTimeout = d
		} else {
			log.Printf("[WARN] Invalid shutdown-timeout %q; using default %v", s, shutdownTimeout)
		}
	}

	// Resolve WAL durability options (Flag > ENV > Default).
	dbOptions, err := resolveDBOptions(*flagWALSync, *flagWALSyncInterval)
	if err != nil {
		log.Fatalf("[FATAL] Invalid WAL configuration: %v", err)
	}

	// Resolve per-client rate limit (Flag > ENV > disabled).
	rateLimit := *flagRateLimit
	if !flagPassed("rate-limit") {
		rateLimit = getEnvFloat("ANCHORDB_RATE_LIMIT", 0)
	}
	rateLimitBurst := *flagRateLimitBurst
	if rateLimitBurst <= 0 {
		rateLimitBurst = getEnvInt("ANCHORDB_RATE_LIMIT_BURST", 0)
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
			ShutdownTimeout: shutdownTimeout / 6,
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
				log.Printf("[WARN] AnchorWire server terminated with error: %v", err)
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

	listenErr := make(chan error, 1)
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

func flagPassed(name string) bool {
	found := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == name {
			found = true
		}
	})
	return found
}
