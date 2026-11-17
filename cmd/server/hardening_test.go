package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/rachmanzz/anchordb/pkg/storage"
	"github.com/rachmanzz/anchordb/pkg/worker"
)

// createFastTestServer builds a server whose model databases skip fsync so tests
// that write many records stay fast.
func createFastTestServer(t *testing.T) (*Server, string) {
	t.Helper()
	tmpDir, err := os.MkdirTemp("", "anchordb-server-fast-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	cfg := ServerConfig{
		DataDir:   tmpDir,
		Port:      "0",
		DBOptions: storage.Options{WAL: storage.WALOptions{SyncMode: storage.SyncNone}},
		WorkerConfig: worker.Config{
			Enabled:         false,
			Concurrency:     2,
			QueueSize:       100,
			ShutdownTimeout: 2 * time.Second,
		},
	}
	s, err := NewServer(cfg)
	if err != nil {
		_ = os.RemoveAll(tmpDir)
		t.Fatalf("NewServer failed: %v", err)
	}
	return s, tmpDir
}

func TestRateLimiterTokenBucket(t *testing.T) {
	rl := newRateLimiter(0.0001, 2) // effectively no refill within the test

	if !rl.allow("a") {
		t.Fatal("expected first request to be allowed (burst=2)")
	}
	if !rl.allow("a") {
		t.Fatal("expected second request to be allowed (burst=2)")
	}
	if rl.allow("a") {
		t.Fatal("expected third request to be rejected")
	}
	// A different key has its own bucket.
	if !rl.allow("b") {
		t.Fatal("expected independent key to be allowed")
	}

	// Force the idle bucket to be swept by backdating its last use. A bucket is
	// only swept once it has been idle long enough to refill completely
	// (idle >= burst/rate), so use a limiter with a meaningful rate here.
	sweep := newRateLimiter(1, 1)
	_ = sweep.allow("a")
	sweep.mu.Lock()
	sweep.buckets["a"].last = time.Now().Add(-2 * time.Hour)
	sweep.lastSweep = time.Now().Add(-2 * time.Minute)
	sweep.mu.Unlock()
	_ = sweep.allow("c")
	sweep.mu.Lock()
	_, exists := sweep.buckets["a"]
	sweep.mu.Unlock()
	if exists {
		t.Fatal("expected idle bucket \"a\" to be swept")
	}
}

func TestRateLimitMiddlewareRejects(t *testing.T) {
	s, dir := createTestServer(t)
	defer cleanupServer(t, s, dir)
	s.limiter = newRateLimiter(0.0001, 2)

	handler := s.Routes()
	body := []byte(`{"model":"rl","limit":1}`)

	var lastCode int
	for i := 0; i < 3; i++ {
		req := httptest.NewRequest(http.MethodPost, "/v1/list", bytes.NewReader(body))
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		lastCode = rec.Code
		if i >= 2 && rec.Code != http.StatusTooManyRequests {
			t.Fatalf("request %d: expected 429, got %d", i, rec.Code)
		}
		if i < 2 && rec.Code == http.StatusTooManyRequests {
			t.Fatalf("request %d: unexpectedly rate limited", i)
		}
	}
	if lastCode != http.StatusTooManyRequests {
		t.Fatalf("expected final request to be rate limited, got %d", lastCode)
	}

	// Health checks must never be rate limited.
	for i := 0; i < 5; i++ {
		req := httptest.NewRequest(http.MethodGet, "/health", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code == http.StatusTooManyRequests {
			t.Fatal("/health must be exempt from rate limiting")
		}
	}
}

func TestMetricsEndpoint(t *testing.T) {
	s, dir := createTestServer(t)
	defer cleanupServer(t, s, dir)

	handler := s.Routes()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("metrics status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	for _, want := range []string{
		"anchordb_up 1",
		"anchordb_models_loaded",
		"anchordb_uptime_seconds",
		"anchordb_go_goroutines",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics missing %q\n%s", want, body)
		}
	}
}

func TestResolveDBOptions(t *testing.T) {
	cases := []struct {
		mode     string
		interval string
		want     storage.SyncMode
		wantErr  bool
	}{
		{mode: "", want: storage.SyncAlways},
		{mode: "always", want: storage.SyncAlways},
		{mode: "none", want: storage.SyncNone},
		{mode: "OFF", want: storage.SyncNone},
		{mode: "interval", interval: "50ms", want: storage.SyncInterval},
		{mode: "bogus", wantErr: true},
		{mode: "interval", interval: "not-a-duration", wantErr: true},
	}
	for _, tc := range cases {
		opts, err := resolveDBOptions(tc.mode, tc.interval)
		if tc.wantErr {
			if err == nil {
				t.Fatalf("resolveDBOptions(%q,%q): expected error", tc.mode, tc.interval)
			}
			continue
		}
		if err != nil {
			t.Fatalf("resolveDBOptions(%q,%q): %v", tc.mode, tc.interval, err)
		}
		if opts.WAL.SyncMode != tc.want {
			t.Fatalf("resolveDBOptions(%q,%q) mode = %v, want %v", tc.mode, tc.interval, opts.WAL.SyncMode, tc.want)
		}
	}
}

func TestListLimitIsCapped(t *testing.T) {
	s, dir := createFastTestServer(t)
	defer cleanupServer(t, s, dir)

	handler := s.Routes()

	// Write more records than maxQueryLimit.
	const total = maxQueryLimit + 5
	for i := 0; i < total; i++ {
		payload := map[string]interface{}{
			"model": "capmodel",
			"name":  fmt.Sprintf("r%04d", i),
			"props": map[string]float32{"v": float32(i)},
		}
		b, _ := json.Marshal(payload)
		req := httptest.NewRequest(http.MethodPost, "/v1/save", bytes.NewReader(b))
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("save %d failed with %d: %s", i, rec.Code, rec.Body.String())
		}
	}

	// Request far more than the cap; the server must clamp to maxQueryLimit.
	listBody := []byte(`{"model":"capmodel","limit":1000000}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/list", bytes.NewReader(listBody))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("list failed with %d: %s", rec.Code, rec.Body.String())
	}

	var resp struct {
		Count   int  `json:"count"`
		HasMore bool `json:"has_more"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode list response: %v", err)
	}
	if resp.Count != maxQueryLimit {
		t.Fatalf("list returned %d records, want capped at %d", resp.Count, maxQueryLimit)
	}
	if !resp.HasMore {
		t.Fatal("expected has_more=true when results are capped")
	}
}

func TestRateLimiterMaxBucketsBounded(t *testing.T) {
	rl := newRateLimiter(1, 1)
	for i := 0; i < maxRateLimiterBuckets+500; i++ {
		_ = rl.allow(fmt.Sprintf("ip-%d", i))
	}
	rl.mu.Lock()
	count := len(rl.buckets)
	rl.mu.Unlock()
	if count > maxRateLimiterBuckets {
		t.Fatalf("rateLimiter buckets grew past cap: got %d, want <= %d", count, maxRateLimiterBuckets)
	}
}

func TestReadOnlyGetModelDoesNotCreateEmptyFile(t *testing.T) {
	s, dir := createFastTestServer(t)
	defer cleanupServer(t, s, dir)

	handler := s.Routes()

	// GET schema on nonexistent model must return 404 and not create ghost_schema.anchordb.
	req := httptest.NewRequest(http.MethodGet, "/v1/model/schema?model=ghost_schema", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("GET schema on nonexistent model = %d, want 404", rec.Code)
	}
	if _, err := os.Stat(dir + "/ghost_schema.anchordb"); !os.IsNotExist(err) {
		t.Fatalf("expected no model file created for read-only schema check, stat err=%v", err)
	}

	// POST /v1/get on nonexistent model must return 404 and not create ghost_get.anchordb.
	req2 := httptest.NewRequest(http.MethodPost, "/v1/get", bytes.NewReader([]byte(`{"model":"ghost_get","name":"missing"}`)))
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusNotFound {
		t.Fatalf("POST /v1/get on nonexistent model = %d, want 404", rec2.Code)
	}
	if _, err := os.Stat(dir + "/ghost_get.anchordb"); !os.IsNotExist(err) {
		t.Fatalf("expected no model file created for read-only /v1/get, stat err=%v", err)
	}
}

func TestRateLimiter_DeterministicLRUEviction(t *testing.T) {
	rl := newRateLimiter(10, 10)
	// Insert cap entries; repeatedly touch ip-0 so it moves to MRU front.
	for i := 0; i < maxRateLimiterBuckets; i++ {
		_ = rl.allow(fmt.Sprintf("ip-%d", i))
	}
	// Consume one token from ip-0 so it stays recently used at front of LRU.
	_ = rl.allow("ip-0")
	// Add new entry exceeding capacity: ip-1 (oldest LRU) must be evicted, while ip-0 remains.
	_ = rl.allow("ip-overflow")

	rl.mu.Lock()
	_, hasHot := rl.buckets["ip-0"]
	_, hasOldest := rl.buckets["ip-1"]
	rl.mu.Unlock()
	if !hasHot {
		t.Fatal("expected recently active bucket ip-0 to be preserved under LRU eviction")
	}
	if hasOldest {
		t.Fatal("expected oldest LRU bucket ip-1 to be evicted deterministically")
	}
}

func TestServer_HandleGetMaxBytes413AndAllModelNames(t *testing.T) {
	s, dir := createFastTestServer(t)
	defer cleanupServer(t, s, dir)

	// Create an unloaded .anchordb file on disk and ensure allModelNames discovers it.
	diskModelPath := dir + "/cold_unloaded.anchordb"
	db, err := storage.OpenWithOptions(diskModelPath, storage.Options{WAL: storage.WALOptions{SyncMode: storage.SyncNone}})
	if err != nil {
		t.Fatalf("storage.OpenWithOptions: %v", err)
	}
	_ = db.Close()

	names := s.allModelNames()
	foundDisk := false
	for _, n := range names {
		if n == "cold_unloaded" {
			foundDisk = true
		}
	}
	if !foundDisk {
		t.Fatalf("expected allModelNames() to include on-disk unloaded model, got %v", names)
	}

	// Send oversized JSON body through Routes() (which wraps limitRequestBody).
	handler := s.Routes()
	huge := append([]byte(`{"model":"default","name":"`), bytes.Repeat([]byte("a"), maxRequestBodyBytes+1024)...)
	huge = append(huge, []byte(`"}`)...)
	req := httptest.NewRequest(http.MethodPost, "/v1/get", bytes.NewReader(huge))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413 RequestEntityTooLarge for oversized /v1/get, got %d: %s", rec.Code, rec.Body.String())
	}
}
