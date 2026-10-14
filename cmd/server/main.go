package main

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/rachmanzz/anchordb/pkg/driver"
	"github.com/rachmanzz/anchordb/pkg/partition"
	"github.com/rachmanzz/anchordb/pkg/scheduler"
	"github.com/rachmanzz/anchordb/pkg/security"
	"github.com/rachmanzz/anchordb/pkg/storage"
	"github.com/rachmanzz/anchordb/pkg/system"
)

// Server holds active AnchorDB database instances keyed by model name
type Server struct {
	mu        sync.RWMutex
	dataDir   string
	authToken string
	port      string
	models    map[string]*storage.AnchorDB
	scheduler *scheduler.InternalScheduler
	ipFilter  *security.IPFilter
	startTime time.Time
}

func NewServer(dataDir, authToken, portStr string) *Server {
	_ = os.MkdirAll(dataDir, 0755)
	whitelistRule := os.Getenv("ANCHORDB_IP_WHITELIST")
	trustProxy := os.Getenv("ANCHORDB_TRUST_PROXY") == "true" || os.Getenv("ANCHORDB_TRUST_PROXY") == "1"
	ipFilter, err := security.NewIPFilter(whitelistRule, trustProxy)
	if err != nil {
		log.Fatalf("Fatal: Invalid ANCHORDB_IP_WHITELIST %q: %v", whitelistRule, err)
	}

	s := &Server{
		dataDir:   dataDir,
		authToken: authToken,
		port:      portStr,
		models:    make(map[string]*storage.AnchorDB),
		scheduler: scheduler.NewInternalScheduler(),
		ipFilter:  ipFilter,
		startTime: time.Now(),
	}

	// Internal Scheduling: default check every day at 23:00 (or custom ANCHORDB_PARTITION_SCHEDULE)
	schedTime := os.Getenv("ANCHORDB_PARTITION_SCHEDULE")
	if schedTime == "" {
		schedTime = "23:00" // Jam 23:00 setiap hari
	}

	retentionDays := float32(180) // 6 bulan terakhir
	if retStr := os.Getenv("ANCHORDB_PARTITION_RETENTION_DAYS"); retStr != "" {
		if r, err := strconv.ParseFloat(retStr, 32); err == nil && r > 0 {
			retentionDays = float32(r)
		}
	}

	_ = s.scheduler.Register("auto_partition_6months", schedTime, func() error {
		log.Printf("[SCHEDULER] Menjalankan partisi otomatis (arsip data > %.0f hari)...", retentionDays)
		s.mu.RLock()
		var loaded []string
		for m := range s.models {
			loaded = append(loaded, m)
		}
		s.mu.RUnlock()

		for _, m := range loaded {
			db, err := s.getModel(m)
			if err != nil {
				continue
			}
			partRes, err := partition.ExecutePartition(db, partition.PartitionPolicy{
				ModelName:     m,
				DateField:     "tanggal",
				RetentionDays: retentionDays,
				Sink:          partition.NewLocalDiskSink(filepath.Join(s.dataDir, "partitions")),
				FileFormat:    "{date}_{uuid}.anchordb",
				PruneHotDB:    true,
			})
			if err == nil && partRes.ArchivedCount > 0 {
				log.Printf("[SCHEDULER] Model %q terpartisi: %d record diarsipkan ke %s",
					m, partRes.ArchivedCount, partRes.TargetLocation)
			}
		}
		return nil
	})

	s.scheduler.Start()
	return s
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
		if !((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '_' || ch == '-') {
			return "", fmt.Errorf("invalid character %q in model name: %q", ch, name)
		}
	}
	return name, nil
}

// isValidIdentifier checks that an identifier consists only of safe characters (letters, digits, underscore, dot, hyphen)
func isValidIdentifier(token string) bool {
	if len(token) == 0 || len(token) > 64 {
		return false
	}
	for _, ch := range token {
		if !((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '_' || ch == '-' || ch == '.') {
			return false
		}
	}
	return true
}

// getModel retrieves or dynamically opens an AnchorDB instance ("Satu DB = Satu Model")
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
	newDB, err := storage.Open(dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open model %q at %s: %w", modelName, dbPath, err)
	}

	s.models[modelName] = newDB
	log.Printf("[INFO] Model %q dimuat dari disk: %s", modelName, dbPath)
	return newDB, nil
}

func (s *Server) authMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Enforce maximum body size (10 MB) to protect against memory exhaustion Slowloris / DoS
		r.Body = http.MaxBytesReader(w, r.Body, 10<<20)

		if s.authToken != "" {
			authHeader := r.Header.Get("Authorization")
			token := strings.TrimPrefix(authHeader, "Bearer ")
			if subtle.ConstantTimeCompare([]byte(token), []byte(s.authToken)) != 1 {
				http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
				return
			}
		}
		w.Header().Set("Content-Type", "application/json")
		next(w, r)
	}
}

// Health check endpoint
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	var loadedModels []string
	for k := range s.models {
		loadedModels = append(loadedModels, k)
	}
	s.mu.RUnlock()

	resp := map[string]interface{}{
		"status":         "ok",
		"engine":         "AnchorDB",
		"uptime_seconds": time.Since(s.startTime).Seconds(),
		"port":           s.port,
		"loaded_models":  loadedModels,
		"memory":         system.GetMemoryStats(),
	}
	_ = json.NewEncoder(w).Encode(resp)
}

// Request / Response types for REST API
type SaveRequest struct {
	Model string             `json:"model"`
	Name  string             `json:"name"`
	Props map[string]float32 `json:"props"`
}

type GetRequest struct {
	Model        string            `json:"model"`
	Name         string            `json:"name"`
	Select       []string          `json:"select"`
	Computations map[string]string `json:"computations"` // e.g. {"kecepatan": "jarak / waktu"}
}

type FilterDTO struct {
	Field     string `json:"field"`
	Operator  string `json:"operator"` // "=", "!=", ">", ">=", "<", "<=", "BETWEEN"
	Value     string `json:"value"`
	ValueHigh string `json:"value_high"` // For "BETWEEN"
}

type ListRequest struct {
	Model        string            `json:"model"`
	Select       []string          `json:"select"`
	Filters      []FilterDTO       `json:"filters"`
	Computations map[string]string `json:"computations"`
	Limit        int               `json:"limit"`
	Offset       int               `json:"offset"`
	Page         int               `json:"page"`
}

type UpdateRequest struct {
	Model     string            `json:"model"`
	In        []string          `json:"in"`
	Filters   []FilterDTO       `json:"filters"`
	Mutations map[string]string `json:"mutations"` // e.g. {"jarak": "jarak + 10"}
	Limit     int               `json:"limit"`
}

type GraphRequest struct {
	Model    string   `json:"model"`
	Inputs   []string `json:"inputs"`
	Relation string   `json:"relation"`
	LimitK   int      `json:"limit_k"`
}

// parseComputeOp converts a string expression like "jarak / waktu", "jarak + 10", "0", or "-5" to ComputeOp
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

func parseVal(str string) float32 {
	if d, err := storage.ParseDate(str); err == nil {
		return d
	}
	if f, err := strconv.ParseFloat(str, 32); err == nil {
		return float32(f)
	}
	return 0
}

func (s *Server) handleSave(w http.ResponseWriter, r *http.Request) {
	var req SaveRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid json body"}`, http.StatusBadRequest)
		return
	}

	db, err := s.getModel(req.Model)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}

	if err := db.SaveRecord(req.Name, req.Props); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}

	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"name":    req.Name,
		"message": "record saved",
	})
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	var req GetRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid json body"}`, http.StatusBadRequest)
		return
	}

	db, err := s.getModel(req.Model)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}

	computations := make(map[string]storage.ComputeOp)
	for outField, expr := range req.Computations {
		op, err := parseComputeOp(expr)
		if err != nil {
			http.Error(w, fmt.Sprintf(`{"error":"invalid computation %q for field %q: %s"}`, expr, outField, err.Error()), http.StatusBadRequest)
			return
		}
		computations[outField] = op
	}

	props, err := db.GetRecordWithCompute(req.Name, computations, req.Select...)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusNotFound)
		return
	}

	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"name":    req.Name,
		"props":   props,
	})
}

func (s *Server) handleList(w http.ResponseWriter, r *http.Request) {
	var req ListRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid json body"}`, http.StatusBadRequest)
		return
	}

	db, err := s.getModel(req.Model)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}

	computations := make(map[string]storage.ComputeOp)
	for outField, expr := range req.Computations {
		op, err := parseComputeOp(expr)
		if err != nil {
			http.Error(w, fmt.Sprintf(`{"error":"invalid computation %q for field %q: %s"}`, expr, outField, err.Error()), http.StatusBadRequest)
			return
		}
		computations[outField] = op
	}

	var filters []storage.FilterCondition
	for _, f := range req.Filters {
		filters = append(filters, storage.FilterCondition{
			Field:     f.Field,
			Operator:  f.Operator,
			Value:     parseVal(f.Value),
			ValueHigh: parseVal(f.ValueHigh),
		})
	}

	limit := req.Limit
	if limit <= 0 {
		limit = 10
	}
	offset := req.Offset
	if req.Page > 1 {
		offset = (req.Page - 1) * limit
	}

	records, hasMore, err := db.ListRecordsWithPaging(storage.QueryOptions{
		Select:       req.Select,
		Filters:      filters,
		Computations: computations,
		Limit:        limit,
		Offset:       offset,
	})
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
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
	var req UpdateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid json body"}`, http.StatusBadRequest)
		return
	}

	db, err := s.getModel(req.Model)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}

	mutations := make(map[string]storage.ComputeOp)
	for field, expr := range req.Mutations {
		op, err := parseComputeOp(expr)
		if err != nil {
			http.Error(w, fmt.Sprintf(`{"error":"invalid mutation %q for field %q: %s"}`, expr, field, err.Error()), http.StatusBadRequest)
			return
		}
		mutations[field] = op
	}

	var filters []storage.FilterCondition
	for _, f := range req.Filters {
		filters = append(filters, storage.FilterCondition{
			Field:     f.Field,
			Operator:  f.Operator,
			Value:     parseVal(f.Value),
			ValueHigh: parseVal(f.ValueHigh),
		})
	}

	count, err := db.UpdateRecords(storage.UpdateOptions{
		TargetEntities: req.In,
		Filters:        filters,
		Mutations:      mutations,
		Limit:          req.Limit,
	})
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}

	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"success":       true,
		"updated_count": count,
	})
}

func (s *Server) handleGraph(w http.ResponseWriter, r *http.Request) {
	var req GraphRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid json body"}`, http.StatusBadRequest)
		return
	}

	db, err := s.getModel(req.Model)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}

	limit := req.LimitK
	if limit <= 0 {
		limit = 5
	}

	results, anchor, err := db.ExecuteGraphStep(req.Inputs, req.Relation, limit)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}

	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"anchor":  anchor,
		"results": results,
	})
}

func (s *Server) handleSync(w http.ResponseWriter, r *http.Request) {
	model := r.URL.Query().Get("model")
	db, err := s.getModel(model)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}

	if err := db.Sync(); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}

	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"model":   model,
		"message": "synced to disk",
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
}

func (s *Server) handlePartitionRun(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"Method not allowed, use POST"}`, http.StatusMethodNotAllowed)
		return
	}

	var req PartitionRunRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil && err.Error() != "EOF" {
		http.Error(w, fmt.Sprintf(`{"error":"Invalid request body: %s"}`, err.Error()), http.StatusBadRequest)
		return
	}

	dateField := req.DateField
	if dateField == "" {
		dateField = "tanggal"
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
		endpoint := req.S3Endpoint
		bucket := req.S3Bucket
		if bucket == "" {
			bucket = "anchordb-archive"
		}
		prefix := req.S3Prefix
		if prefix == "" {
			prefix = "partitions"
		}
		sink = partition.NewS3Sink(endpoint, bucket, prefix, req.S3AccessKey, req.S3SecretKey)
	} else {
		targetDir := req.TargetPath
		if targetDir == "" {
			targetDir = filepath.Join(s.dataDir, "partitions")
		} else {
			// Enforce path containment within s.dataDir to prevent arbitrary directory write vulnerability
			cleanTarget := filepath.Clean(targetDir)
			absDataDir, err1 := filepath.Abs(s.dataDir)
			absTarget, err2 := filepath.Abs(cleanTarget)
			if err1 != nil || err2 != nil || !strings.HasPrefix(absTarget, absDataDir) {
				http.Error(w, fmt.Sprintf(`{"error":"Invalid target_path: must reside inside data directory %s"}`, s.dataDir), http.StatusBadRequest)
				return
			}
			targetDir = absTarget
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
			http.Error(w, fmt.Sprintf(`{"error":"Partition failed for model %s: %s"}`, m, err.Error()), http.StatusInternalServerError)
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

func (s *Server) handleSchedulerStatus(w http.ResponseWriter, r *http.Request) {
	tasks := s.scheduler.Tasks()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"success":    true,
		"tasks":      tasks,
		"uptime_sec": time.Since(s.startTime).Seconds(),
	})
}

func main() {
	flagPort := flag.String("port", "", "Port to listen on (overrides ANCHORDB_PORT, default 7447)")
	flagDataDir := flag.String("data-dir", "", "Data directory path (overrides ANCHORDB_DATA_DIR)")
	flagToken := flag.String("auth-token", "", "Bearer auth token (overrides ANCHORDB_AUTH_TOKEN)")
	flagMemLimit := flag.String("memory-limit", "", "RAM allocation limit e.g. 500MB, 1GB, 5GB (overrides ANCHORDB_MEMORY_LIMIT)")
	flag.StringVar(flagMemLimit, "mem-limit", "", "Alias for -memory-limit")
	flag.StringVar(flagMemLimit, "m", "", "Short alias for -memory-limit")
	flag.Parse()

	portStr := *flagPort
	if portStr == "" {
		portStr = os.Getenv("ANCHORDB_PORT")
	}
	if portStr == "" {
		portStr = "7447" // Port Resmi AnchorDB
	}

	dataDir := *flagDataDir
	if dataDir == "" {
		dataDir = os.Getenv("ANCHORDB_DATA_DIR")
	}
	if dataDir == "" {
		dataDir = "/data"
		if _, err := os.Stat(dataDir); os.IsNotExist(err) {
			dataDir = "./data"
		}
	}

	authToken := *flagToken
	if authToken == "" {
		authToken = os.Getenv("ANCHORDB_AUTH_TOKEN")
	}

	// Initialize and enforce Soft Memory Limit (GOMEMLIMIT)
	_, memSummary, err := system.ApplyMemoryLimit(*flagMemLimit)
	if err != nil {
		log.Fatalf("[FATAL] Konfigurasi Memory Limit tidak valid: %v", err)
	}

	server := NewServer(dataDir, authToken, portStr)

	mux := http.NewServeMux()
	mux.HandleFunc("/health", server.handleHealth)
	mux.HandleFunc("/v1/save", server.authMiddleware(server.handleSave))
	mux.HandleFunc("/v1/get", server.authMiddleware(server.handleGet))
	mux.HandleFunc("/v1/list", server.authMiddleware(server.handleList))
	mux.HandleFunc("/v1/update", server.authMiddleware(server.handleUpdate))
	mux.HandleFunc("/v1/graph", server.authMiddleware(server.handleGraph))
	mux.HandleFunc("/v1/sync", server.authMiddleware(server.handleSync))
	mux.HandleFunc("/v1/partition/run", server.authMiddleware(server.handlePartitionRun))
	mux.HandleFunc("/v1/scheduler/status", server.authMiddleware(server.handleSchedulerStatus))

	fmt.Println("==========================================================")
	fmt.Println("          ANCHORDB HIGH-PERFORMANCE SERVER DAEMON         ")
	fmt.Println("      Copyright (c) 2026 MUHAMMD ABDURRAHMAN              ")
	fmt.Println("             All Commercial Rights Reserved               ")
	fmt.Printf("   Port Dedicated : %s\n", portStr)
	fmt.Printf("   Memory Limit   : %s\n", memSummary)
	if server.ipFilter.IsEmpty() {
		fmt.Println("   IP Whitelist   : OPEN (All client IPs allowed)")
	} else {
		fmt.Printf("   IP Whitelist   : RESTRICTED (%s)\n", os.Getenv("ANCHORDB_IP_WHITELIST"))
	}
	fmt.Println("   Container Ready: Docker / Podman / Cloud Host / K8s    ")
	fmt.Println("==========================================================")

	addr := ":" + portStr
	srv := &http.Server{
		Addr:              addr,
		Handler:           server.ipFilter.Middleware(mux),
		ReadTimeout:       15 * time.Second,
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	// Graceful shutdown handling
	stopSignal := make(chan os.Signal, 1)
	signal.Notify(stopSignal, os.Interrupt, syscall.SIGTERM)

	go func() {
		log.Printf("[READY] AnchorDB Server mendengarkan di %s ...", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server error: %v", err)
		}
	}()

	<-stopSignal
	log.Println("[SHUTDOWN] Menghentikan server & menyimpan semua database ke disk...")
	server.scheduler.Stop()

	// Sync all active models cleanly to disk before exit
	server.mu.Lock()
	for name, db := range server.models {
		if err := db.Sync(); err != nil {
			log.Printf("[WARN] Gagal sync model %s saat shutdown: %v", name, err)
		}
		_ = db.Close()
	}
	server.mu.Unlock()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
	log.Println("[EXIT] AnchorDB Server berhasil dihentikan secara graceful.")
}
