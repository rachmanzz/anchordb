package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rachmanzz/anchordb/pkg/worker"
)

func createTestServer(t *testing.T) (*Server, string) {
	t.Helper()
	return createTestServerWithWorkerTimeout(t, 2*time.Second)
}

func createTestServerWithWorkerTimeout(t *testing.T, workerShutdown time.Duration) (*Server, string) {
	t.Helper()
	tmpDir, err := os.MkdirTemp("", "anchordb-server-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	cfg := ServerConfig{
		DataDir: tmpDir,
		Port:    "0",
		WorkerConfig: worker.Config{
			Enabled:         true,
			Concurrency:     2,
			QueueSize:       100,
			ShutdownTimeout: workerShutdown,
		},
	}
	s, err := NewServer(cfg)
	if err != nil {
		_ = os.RemoveAll(tmpDir)
		t.Fatalf("NewServer failed: %v", err)
	}
	return s, tmpDir
}

func cleanupServer(t *testing.T, s *Server, dir string) {
	t.Helper()
	if s.scheduler != nil {
		s.scheduler.Stop()
	}
	if s.worker != nil {
		_ = s.worker.Shutdown()
	}
	s.mu.Lock()
	for _, db := range s.models {
		_ = db.Close()
	}
	s.mu.Unlock()
	_ = os.RemoveAll(dir)
}

func TestAsyncUpdateAcceptedAndExecuted(t *testing.T) {
	s, dir := createTestServer(t)
	defer cleanupServer(t, s, dir)

	handler := s.Routes()

	// 1. Save an initial record
	savePayload := map[string]interface{}{
		"model": "wallet",
		"name":  "user_1",
		"props": map[string]float32{"balance": 100},
	}
	saveBody, _ := json.Marshal(savePayload)
	req := httptest.NewRequest(http.MethodPost, "/v1/save", bytes.NewReader(saveBody))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("save failed with code %d: %s", rec.Code, rec.Body.String())
	}

	// 2. Dispatch async update: balance = balance + 50
	updatePayload := map[string]interface{}{
		"model": "wallet",
		"in":    []string{"user_1"},
		"mutations": map[string]string{
			"balance": "balance + 50",
		},
	}
	updateBody, _ := json.Marshal(updatePayload)
	req = httptest.NewRequest(http.MethodPost, "/v1/update?async=true", bytes.NewReader(updateBody))
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp["status"] != "accepted" {
		t.Fatalf("expected status 'accepted', got %v", resp["status"])
	}
	jobID, ok := resp["job_id"].(string)
	if !ok || jobID == "" {
		t.Fatalf("expected non-empty job_id in 202 response, got %v", resp["job_id"])
	}

	// Wait for worker to finish processing
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = s.worker.WaitIdle(ctx)

	// 3. Verify record was updated exactly once (balance == 150)
	getBody := []byte(`{"model":"wallet","name":"user_1"}`)
	getReq := httptest.NewRequest(http.MethodPost, "/v1/get", bytes.NewReader(getBody))
	getRec := httptest.NewRecorder()
	handler.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("get failed with code %d: %s", getRec.Code, getRec.Body.String())
	}

	var getResp map[string]interface{}
	_ = json.NewDecoder(getRec.Body).Decode(&getResp)
	props, _ := getResp["props"].(map[string]interface{})
	if props["balance"] != float64(150) {
		t.Fatalf("expected balance=150, got %v", props["balance"])
	}
}

func TestAsyncDispatchRejectedWhenWorkerDisabled(t *testing.T) {
	tmpDir := t.TempDir()
	s, err := NewServer(ServerConfig{
		DataDir: tmpDir,
		Port:    "0",
		WorkerConfig: worker.Config{
			Enabled: false,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cleanupServer(t, s, tmpDir)

	body := []byte(`{"model":"orders","retention_days":30,"async":true}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/partition/run", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	s.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 when async worker unavailable, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestRequestBodyLimit(t *testing.T) {
	s, dir := createTestServer(t)
	defer cleanupServer(t, s, dir)

	body := bytes.NewReader(bytes.Repeat([]byte(" "), maxRequestBodyBytes+1))
	req := httptest.NewRequest(http.MethodPost, "/v1/save", body)
	rec := httptest.NewRecorder()
	s.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413 for oversized request, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestPartitionTargetPathRejectsSymlinkEscape(t *testing.T) {
	s, dir := createTestServer(t)
	defer cleanupServer(t, s, dir)

	outside := t.TempDir()
	link := filepath.Join(dir, "escape")
	if err := os.Symlink(outside, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	body := []byte(`{"model":"orders","target_path":` + strconv.Quote(link) + `}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/partition/run", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	s.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for symlink escape, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestAsyncPartitionAndCollectorAccepted(t *testing.T) {
	s, dir := createTestServer(t)
	defer cleanupServer(t, s, dir)

	handler := s.Routes()

	// Async Partition Run
	partReqBody := []byte(`{"model":"orders","retention_days":30,"async":true}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/partition/run", bytes.NewReader(partReqBody))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted, got %d: %s", rec.Code, rec.Body.String())
	}

	var partResp map[string]interface{}
	_ = json.NewDecoder(rec.Body).Decode(&partResp)
	if partResp["status"] != "accepted" {
		t.Fatalf("expected status 'accepted', got %v", partResp["status"])
	}
	if partResp["job_id"] == nil || partResp["job_id"] == "" {
		t.Fatalf("expected job_id in partition async response")
	}

	// Async Collector Run
	colReqBody := []byte(`{"model":"orders","async":true}`)
	req = httptest.NewRequest(http.MethodPost, "/v1/collector/run", bytes.NewReader(colReqBody))
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted, got %d: %s", rec.Code, rec.Body.String())
	}

	var colResp map[string]interface{}
	_ = json.NewDecoder(rec.Body).Decode(&colResp)
	if colResp["status"] != "accepted" {
		t.Fatalf("expected status 'accepted', got %v", colResp["status"])
	}
	if colResp["job_id"] == nil || colResp["job_id"] == "" {
		t.Fatalf("expected job_id in collector async response")
	}
}

func TestWorkerCancelScopedEndpoint(t *testing.T) {
	s, dir := createTestServer(t)
	defer cleanupServer(t, s, dir)

	handler := s.Routes()

	// 1. Un-scoped POST /v1/worker/cancel must be rejected with 400
	req := httptest.NewRequest(http.MethodPost, "/v1/worker/cancel", bytes.NewReader([]byte(`{}`)))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 Bad Request for un-scoped cancel, got %d: %s", rec.Code, rec.Body.String())
	}

	// Block the worker concurrency so tasks queue up
	blockerDone := make(chan struct{})
	blockerStarted := make(chan struct{})
	for i := 0; i < 2; i++ {
		_ = s.worker.Dispatch(context.Background(), "blocker", func(ctx context.Context) error {
			select {
			case blockerStarted <- struct{}{}:
			default:
			}
			<-blockerDone
			return nil
		})
	}
	<-blockerStarted

	// Queue task for model A and model B
	var taskAExecuted int32
	var taskBExecuted int32

	_ = s.worker.Dispatch(context.Background(), "job_a",
		func(ctx context.Context) error {
			atomic.AddInt32(&taskAExecuted, 1)
			return nil
		},
		worker.WithModel("tenant_a"),
		worker.WithID("job-a-1"),
	)

	_ = s.worker.Dispatch(context.Background(), "job_b",
		func(ctx context.Context) error {
			atomic.AddInt32(&taskBExecuted, 1)
			return nil
		},
		worker.WithModel("tenant_b"),
		worker.WithID("job-b-1"),
	)

	// Cancel only tenant_a
	cancelPayload := []byte(`{"model":"tenant_a"}`)
	req = httptest.NewRequest(http.MethodPost, "/v1/worker/cancel", bytes.NewReader(cancelPayload))
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for scoped model cancel, got %d: %s", rec.Code, rec.Body.String())
	}

	// Release blocker
	close(blockerDone)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = s.worker.WaitIdle(ctx)

	// Tenant A should not have run (canceled), Tenant B should have run
	if atomic.LoadInt32(&taskAExecuted) != 0 {
		t.Fatalf("expected tenant_a task to be canceled, but it executed")
	}
	if atomic.LoadInt32(&taskBExecuted) != 1 {
		t.Fatalf("expected tenant_b task to execute, got %d", atomic.LoadInt32(&taskBExecuted))
	}
}

func TestSchedulerEndpoints(t *testing.T) {
	s, dir := createTestServer(t)
	defer cleanupServer(t, s, dir)

	handler := s.Routes()

	// 1. Register a test task directly in the scheduler
	var runCount int32
	err := s.scheduler.Register("test_maintenance", "@every 1h", func() error {
		atomic.AddInt32(&runCount, 1)
		return nil
	})
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	// 2. Query /v1/scheduler/status
	req := httptest.NewRequest(http.MethodGet, "/v1/scheduler/status", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var statusResp map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&statusResp); err != nil {
		t.Fatalf("decode status response failed: %v", err)
	}

	tasksList, ok := statusResp["tasks"].([]interface{})
	if !ok || len(tasksList) == 0 {
		t.Fatalf("expected non-empty tasks list, got %v", statusResp["tasks"])
	}

	// 3. Trigger task on demand via POST /v1/scheduler/run
	runBody := []byte(`{"task":"test_maintenance"}`)
	req = httptest.NewRequest(http.MethodPost, "/v1/scheduler/run", bytes.NewReader(runBody))
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 from scheduler run, got %d: %s", rec.Code, rec.Body.String())
	}

	if atomic.LoadInt32(&runCount) != 1 {
		t.Fatalf("expected task to be executed once, got %d", atomic.LoadInt32(&runCount))
	}

	// 4. Trigger nonexistent task -> 404
	badRunBody := []byte(`{"task":"nonexistent_task"}`)
	req = httptest.NewRequest(http.MethodPost, "/v1/scheduler/run", bytes.NewReader(badRunBody))
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 from nonexistent task, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestEdgeAndCausalEndpoints(t *testing.T) {
	s, dir := createTestServer(t)
	defer cleanupServer(t, s, dir)
	handler := s.Routes()

	post := func(path string, payload map[string]interface{}) *httptest.ResponseRecorder {
		t.Helper()
		body, _ := json.Marshal(payload)
		req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec
	}

	// 1. Add two causal edges
	rec := post("/v1/edge", map[string]interface{}{
		"model": "causal", "action": "add",
		"source": "cabai", "relation": "sebab", "target": "inflasi", "weight": 0.9,
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("add edge status = %d body=%s", rec.Code, rec.Body.String())
	}
	rec = post("/v1/edge", map[string]interface{}{
		"model": "causal", "source": "gagal_panen", "relation": "sebab", "target": "inflasi", "weight": 0.4,
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("add edge status = %d body=%s", rec.Code, rec.Body.String())
	}

	// 2. Single-hop causal query
	rec = post("/v1/causal", map[string]interface{}{
		"model": "causal", "seeds": []string{"cabai", "gagal_panen"}, "relation": "sebab", "limit_k": 5,
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("causal status = %d body=%s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Success bool `json:"success"`
		Results []struct {
			Word  string  `json:"word"`
			Score float32 `json:"score"`
		} `json:"results"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode causal response: %v", err)
	}
	if !resp.Success || len(resp.Results) != 1 || resp.Results[0].Word != "inflasi" || resp.Results[0].Score != 0.9 {
		t.Fatalf("unexpected causal response: %+v", resp)
	}

	// 3. Remove an edge
	rec = post("/v1/edge", map[string]interface{}{
		"model": "causal", "action": "remove",
		"source": "gagal_panen", "relation": "sebab", "target": "inflasi",
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("remove edge status = %d body=%s", rec.Code, rec.Body.String())
	}

	// 4. Invalid action is rejected
	rec = post("/v1/edge", map[string]interface{}{
		"model": "causal", "action": "explode",
		"source": "a", "relation": "r", "target": "b",
	})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("invalid action status = %d, want 400", rec.Code)
	}
}
