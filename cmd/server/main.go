package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rachmanzz/anchordb/pkg/driver"
	"github.com/rachmanzz/anchordb/pkg/partition"
	"github.com/rachmanzz/anchordb/pkg/scheduler"
	"github.com/rachmanzz/anchordb/pkg/security"
	"github.com/rachmanzz/anchordb/pkg/storage"
)

// Server holds active AnchorDB database instances keyed by model name
type Server struct {
	mu        sync.RWMutex
	dataDir   string
	authToken string
	models    map[string]*storage.AnchorDB
	scheduler *scheduler.InternalScheduler
	ipFilter  *security.IPFilter
	startTime time.Time
}

func NewServer(dataDir, authToken string) *Server {
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

// sanitizeModelName enforces alphanumeric model names and prevents path traversal
func sanitizeModelName(name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "default", nil
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
		if s.authToken != "" {
			authHeader := r.Header.Get("Authorization")
			token := strings.TrimPrefix(authHeader, "Bearer ")
			if token != s.authToken {
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
		"port":           7447,
		"data_dir":       s.dataDir,
		"loaded_models":  loadedModels,
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

type ListRequest struct {
	Model        string            `json:"model"`
	Select       []string          `json:"select"`
	Filters      []FilterDTO       `json:"filters"`
	Computations map[string]string `json:"computations"`
	Page         int               `json:"page"`
	Limit        int               `json:"limit"`
	Offset       int               `json:"offset"`
}

type FilterDTO struct {
	Field     string  `json:"field"`
	Operator  string  `json:"operator"` // "=", "!=", ">", ">=", "<", "<=", "BETWEEN"
	Value     string  `json:"value"`    // supports number or "YYYY-MM-DD" date
	ValueHigh string  `json:"value_high"`
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

// parseComputeOp converts a string like "jarak / waktu" or "jarak + 10" to ComputeOp
func parseComputeOp(expr string) (storage.ComputeOp, error) {
	expr = strings.TrimSpace(expr)
	var parts []string
	var op string

	if strings.Contains(expr, "/") {
		parts = strings.Split(expr, "/")
		op = "/"
	} else if strings.Contains(expr, "*") {
		parts = strings.Split(expr, "*")
		op = "*"
	} else if strings.Contains(expr, "+") {
		parts = strings.Split(expr, "+")
		op = "+"
	} else if strings.Contains(expr, "-") {
		parts = strings.Split(expr, "-")
		op = "-"
	} else {
		return nil, fmt.Errorf("unsupported operator in expression %q", expr)
	}

	if len(parts) != 2 {
		return nil, fmt.Errorf("malformed expression %q", expr)
	}

	fieldA := strings.TrimSpace(parts[0])
	fieldB := strings.TrimSpace(parts[1])

	if !isValidIdentifier(fieldA) {
		return nil, fmt.Errorf("invalid identifier in field A: %q", fieldA)
	}
	if !isValidIdentifier(fieldB) {
		return nil, fmt.Errorf("invalid identifier in field B: %q", fieldB)
	}

	switch op {
	case "/":
		return driver.Divide(fieldA, fieldB), nil
	case "*":
		return driver.Multiply(fieldA, fieldB), nil
	case "+":
		return driver.Add(fieldA, fieldB), nil
	case "-":
		return driver.Subtract(fieldA, fieldB), nil
	}
	return nil, fmt.Errorf("invalid op")
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
		"model":   req.Model,
		"name":    req.Name,
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
		if err == nil {
			computations[outField] = op
		}
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
		if err == nil {
			computations[outField] = op
		}
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
		if err == nil {
			mutations[field] = op
		}
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

	limitK := req.LimitK
	if limitK <= 0 {
		limitK = 5
	}

	results, anchor, err := db.ExecuteGraphStep(req.Inputs, req.Relation, limitK)
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
	portStr := os.Getenv("ANCHORDB_PORT")
	if portStr == "" {
		portStr = "7447" // Port Resmi AnchorDB
	}

	dataDir := os.Getenv("ANCHORDB_DATA_DIR")
	if dataDir == "" {
		dataDir = "/data"
		if _, err := os.Stat(dataDir); os.IsNotExist(err) {
			dataDir = "./data"
		}
	}

	authToken := os.Getenv("ANCHORDB_AUTH_TOKEN")

	server := NewServer(dataDir, authToken)

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
	fmt.Printf("   Data Directory : %s\n", dataDir)
	if server.ipFilter.IsEmpty() {
		fmt.Println("   IP Whitelist   : OPEN (All client IPs allowed)")
	} else {
		fmt.Printf("   IP Whitelist   : RESTRICTED (%s)\n", os.Getenv("ANCHORDB_IP_WHITELIST"))
	}
	fmt.Println("   Container Ready: Docker / Podman / Cloud Host / K8s    ")
	fmt.Println("==========================================================")

	addr := ":" + portStr
	log.Printf("[READY] AnchorDB Server mendengarkan di %s ...", addr)
	handler := server.ipFilter.Middleware(mux)
	if err := http.ListenAndServe(addr, handler); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}
