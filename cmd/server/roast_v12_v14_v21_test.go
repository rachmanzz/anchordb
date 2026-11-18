package main

import (
	"bytes"
	"flag"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rachmanzz/anchordb/pkg/storage"
)

func TestRoastV12_PinnedModelNotEvictedAboveCap(t *testing.T) {
	s, dir := createFastTestServer(t)
	defer cleanupServer(t, s, dir)

	db1, release1, err := s.acquireModel("pinned_model")
	if err != nil {
		t.Fatalf("acquireModel(pinned_model): %v", err)
	}
	defer release1()

	if err := db1.SaveRecord("k1", map[string]float32{"v": 7}); err != nil {
		t.Fatalf("SaveRecord: %v", err)
	}

	// Open 505 unpinned models concurrently/sequentially to force LRU eviction passes.
	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(offset int) {
			defer wg.Done()
			for i := 0; i < 130; i++ {
				name := fmt.Sprintf("cold_%d_%03d", offset, i)
				_, rel, err := s.acquireModel(name)
				if err != nil {
					t.Errorf("acquireModel(%s): %v", name, err)
					return
				}
				rel()
			}
		}(g)
	}
	wg.Wait()

	// Pinned handle must remain open and readable without ErrDatabaseClosed or SIGSEGV.
	if err := db1.Sync(); err != nil {
		t.Fatalf("Sync on pinned db1 failed: %v", err)
	}
	cr, err := db1.GetRecordTyped("k1")
	if err != nil {
		t.Fatalf("GetRecordTyped on pinned db1 failed: %v", err)
	}
	if v, ok, err := cr.Get("v"); err != nil || !ok || v != 7 {
		t.Fatalf("unexpected value on pinned db1: (%v, %v, err=%v)", v, ok, err)
	}
}

func TestRoastV14_IPFilterRejectsBeforeRateLimiterBuckets(t *testing.T) {
	cfg := ServerConfig{
		DataDir:         t.TempDir(),
		Port:            "0",
		IPWhitelist:     "10.0.0.1",
		RateLimitPerSec: 10,
		RateLimitBurst:  10,
		DBOptions:       storage.Options{WAL: storage.WALOptions{SyncMode: storage.SyncNone}},
	}
	srv, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	defer func() {
		_ = srv.Shutdown(t.Context(), nil)
	}()

	handler := srv.Routes()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	req.RemoteAddr = "203.0.113.55:4321"
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403 Forbidden, got %d", rec.Code)
	}
	srv.limiter.mu.Lock()
	bucketsLen := len(srv.limiter.buckets)
	srv.limiter.mu.Unlock()
	if bucketsLen != 0 {
		t.Fatalf("expected 0 rate limiter buckets for IP rejected by ipFilter, got %d", bucketsLen)
	}
}

func TestRoastV21_FlagFalseOverridesEnvAndSyncJSONAndSaveSchema(t *testing.T) {
	// 1. CLI flag=false must NOT be overwritten by ENV=true or default=true.
	t.Setenv("ANCHORDB_CRON_ENABLED", "true")
	t.Setenv("ANCHORDB_GATEWAY_MODE", "true")
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	cronFlag := fs.Bool("cron-enabled", true, "")
	gwFlag := fs.Bool("gateway-mode", false, "")
	durFlag := fs.String("shutdown-timeout", "", "")
	if err := fs.Parse([]string{"-cron-enabled=false", "-gateway-mode=false", "-shutdown-timeout=5s"}); err != nil {
		t.Fatalf("fs.Parse: %v", err)
	}
	if v, err := resolveBoolFlagIn(fs, "cron-enabled", *cronFlag, "ANCHORDB_CRON_ENABLED", true); err != nil || v {
		t.Fatalf("expected -cron-enabled=false to override ANCHORDB_CRON_ENABLED=true, got (%v, %v)", v, err)
	}
	if v, err := resolveBoolFlagIn(fs, "gateway-mode", *gwFlag, "ANCHORDB_GATEWAY_MODE", false); err != nil || v {
		t.Fatalf("expected -gateway-mode=false to override ANCHORDB_GATEWAY_MODE=true, got (%v, %v)", v, err)
	}
	d, err := resolveDurationFlagIn(fs, "shutdown-timeout", *durFlag, "ANCHORDB_SHUTDOWN_TIMEOUT", 25*time.Second, false)
	if err != nil || d != 5*time.Second {
		t.Fatalf("expected 5s duration, got (%v, %v)", d, err)
	}

	// 2. POST /v1/sync honors JSON {"model":"sync_json"} and /v1/save node-only enforces schema.
	s, dir := createFastTestServer(t)
	defer cleanupServer(t, s, dir)
	handler := s.Routes()

	attachBody := `{"model":"strict_node","schema":{"required_num":"float"}}`
	recAttach := httptest.NewRecorder()
	handler.ServeHTTP(recAttach, httptest.NewRequest(http.MethodPost, "/v1/model/attach", strings.NewReader(attachBody)))
	if recAttach.Code != http.StatusOK {
		t.Fatalf("attach failed: %d %s", recAttach.Code, recAttach.Body.String())
	}

	nodeOnlyBody := `{"model":"strict_node","name":"n1","coord":{"x":1,"y":2}}`
	recSave := httptest.NewRecorder()
	handler.ServeHTTP(recSave, httptest.NewRequest(http.MethodPost, "/v1/save", strings.NewReader(nodeOnlyBody)))
	if recSave.Code != http.StatusBadRequest {
		t.Fatalf("expected node-only save missing required schema field to fail 400, got %d %s", recSave.Code, recSave.Body.String())
	}

	recSync := httptest.NewRecorder()
	handler.ServeHTTP(recSync, httptest.NewRequest(http.MethodPost, "/v1/sync", bytes.NewBufferString(`{"model":"sync_json"}`)))
	if recSync.Code != http.StatusOK {
		t.Fatalf("POST /v1/sync JSON failed: %d %s", recSync.Code, recSync.Body.String())
	}
	s.mu.RLock()
	_, loaded := s.models["sync_json"]
	s.mu.RUnlock()
	if !loaded {
		t.Fatal("expected POST /v1/sync with JSON {\"model\":\"sync_json\"} to target sync_json")
	}
}
