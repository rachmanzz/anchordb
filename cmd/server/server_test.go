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
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rachmanzz/anchordb/pkg/storage"
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
	if s.wireServer != nil {
		_ = s.wireServer.Close()
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
		_ = s.worker.Dispatch(context.Background(), "blocker", func(_ context.Context) error {
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
		func(_ context.Context) error {
			atomic.AddInt32(&taskAExecuted, 1)
			return nil
		},
		worker.WithModel("tenant_a"),
		worker.WithID("job-a-1"),
	)

	_ = s.worker.Dispatch(context.Background(), "job_b",
		func(_ context.Context) error {
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
		"source": "fuel_shortage", "relation": "causes", "target": "inflation", "weight": 0.9,
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("add edge status = %d body=%s", rec.Code, rec.Body.String())
	}
	rec = post("/v1/edge", map[string]interface{}{
		"model": "causal", "source": "crop_failure", "relation": "causes", "target": "inflation", "weight": 0.4,
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("add edge status = %d body=%s", rec.Code, rec.Body.String())
	}

	// 2. Single-hop causal query
	rec = post("/v1/causal", map[string]interface{}{
		"model": "causal", "seeds": []string{"fuel_shortage", "crop_failure"}, "relation": "causes", "limit_k": 5,
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
	if !resp.Success || len(resp.Results) != 1 || resp.Results[0].Word != "inflation" || resp.Results[0].Score != 0.9 {
		t.Fatalf("unexpected causal response: %+v", resp)
	}

	// 3. Remove an edge
	rec = post("/v1/edge", map[string]interface{}{
		"model": "causal", "action": "remove",
		"source": "crop_failure", "relation": "causes", "target": "inflation",
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

func TestServer_SaveBatch(t *testing.T) {
	s, dir := createTestServer(t)
	defer cleanupServer(t, s, dir)

	handler := s.Routes()
	post := func(path string, payload interface{}) *httptest.ResponseRecorder {
		body, _ := json.Marshal(payload)
		req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec
	}

	// 1. Batch save 3 records
	batchReq := map[string]interface{}{
		"model": "batch_test",
		"records": []map[string]interface{}{
			{
				"name":  "item_1",
				"props": map[string]float32{"price": 10.5, "stock": 100},
			},
			{
				"name":  "item_2",
				"props": map[string]float32{"price": 20.0, "stock": 50},
			},
			{
				"name":  "item_3",
				"props": map[string]float32{"price": 5.25, "stock": 500},
			},
		},
	}

	rec := post("/v1/save_batch", batchReq)
	if rec.Code != http.StatusOK {
		t.Fatalf("save_batch status = %d body = %s", rec.Code, rec.Body.String())
	}

	var batchResp struct {
		Success bool     `json:"success"`
		Count   int      `json:"count"`
		Names   []string `json:"names"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &batchResp); err != nil {
		t.Fatalf("decode save_batch response: %v", err)
	}
	if !batchResp.Success || batchResp.Count != 3 || len(batchResp.Names) != 3 {
		t.Fatalf("unexpected save_batch response: %+v", batchResp)
	}

	// 2. Verify with /v1/get
	rec = post("/v1/get", map[string]interface{}{
		"model": "batch_test",
		"name":  "item_2",
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("get item_2 status = %d body = %s", rec.Code, rec.Body.String())
	}
	var getResp struct {
		Success bool               `json:"success"`
		Props   map[string]float32 `json:"props"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &getResp); err != nil {
		t.Fatalf("decode get response: %v", err)
	}
	if getResp.Props["price"] != 20.0 || getResp.Props["stock"] != 50 {
		t.Fatalf("unexpected props for item_2: %+v", getResp.Props)
	}

	// 3. Empty batch should be rejected
	rec = post("/v1/save_batch", map[string]interface{}{
		"model":   "batch_test",
		"records": []map[string]interface{}{},
	})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for empty batch, got %d", rec.Code)
	}
}

func TestServer_SaveAndGetTyped(t *testing.T) {
	s, dir := createTestServer(t)
	defer cleanupServer(t, s, dir)

	handler := s.Routes()
	post := func(target string, body interface{}) *httptest.ResponseRecorder {
		data, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal error: %v", err)
		}
		req := httptest.NewRequest(http.MethodPost, target, bytes.NewReader(data))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec
	}

	largeInt := int64(9007199254740993)
	preciseFloat := 123456.789012345

	// 1. Save typed attributes
	savePayload := map[string]interface{}{
		"model": "typed_model",
		"name":  "account_1",
		"attrs": []storage.AttrEntry{
			storage.NewInt64Attr("balance", largeInt),
			storage.NewFloat64Attr("interest_rate", preciseFloat),
			storage.NewFloat32Attr("level", 5.0),
		},
	}
	rec := post("/v1/save", savePayload)
	if rec.Code != http.StatusOK {
		t.Fatalf("save typed record status = %d body = %s", rec.Code, rec.Body.String())
	}

	// 2. Get typed attributes
	rec = post("/v1/get", map[string]interface{}{
		"model": "typed_model",
		"name":  "account_1",
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("get typed record status = %d body = %s", rec.Code, rec.Body.String())
	}
	var getResp struct {
		Success bool                `json:"success"`
		Props   map[string]float32  `json:"props"`
		Attrs   []storage.AttrEntry `json:"attrs"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &getResp); err != nil {
		t.Fatalf("decode get response: %v", err)
	}
	if !getResp.Success || len(getResp.Attrs) != 3 {
		t.Fatalf("unexpected get response: %+v", getResp)
	}
	var foundInt int64
	var foundFloat float64
	for _, a := range getResp.Attrs {
		if a.Key == "balance" {
			foundInt = a.Int64()
		}
		if a.Key == "interest_rate" {
			foundFloat = a.Float64()
		}
	}
	if foundInt != largeInt {
		t.Fatalf("expected balance %d, got %d", largeInt, foundInt)
	}
	if foundFloat != preciseFloat {
		t.Fatalf("expected interest_rate %f, got %f", preciseFloat, foundFloat)
	}

	// 3. List with 64-bit filter
	rec = post("/v1/list", map[string]interface{}{
		"model": "typed_model",
		"filters": []FilterDTO{
			{
				Field:    "balance",
				Operator: "=",
				Value:    strconv.FormatInt(largeInt, 10),
			},
		},
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("list status = %d body = %s", rec.Code, rec.Body.String())
	}
	var listResp struct {
		Success bool                   `json:"success"`
		Items   []storage.EntityRecord `json:"items"`
		Count   int                    `json:"count"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &listResp); err != nil {
		t.Fatalf("decode list response: %v", err)
	}
	if listResp.Count != 1 || len(listResp.Items) != 1 || listResp.Items[0].Name != "account_1" {
		t.Fatalf("unexpected list response: %+v", listResp)
	}
}

func TestGetErrorMapping(t *testing.T) {
	s, dir := createTestServer(t)
	defer cleanupServer(t, s, dir)

	handler := s.Routes()
	post := func(target string, body interface{}) *httptest.ResponseRecorder {
		data, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal error: %v", err)
		}
		req := httptest.NewRequest(http.MethodPost, target, bytes.NewReader(data))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec
	}

	// Seed a record whose computation overflows int64.
	rec := post("/v1/save", map[string]interface{}{
		"model": "errmap_model",
		"name":  "bigrec",
		"attrs": []storage.AttrEntry{storage.NewInt64Attr("big", 9223372036854775807)},
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("save status = %d body = %s", rec.Code, rec.Body.String())
	}

	// 1. Overflow on GET must be 422, never 404.
	rec = post("/v1/get", map[string]interface{}{
		"model":        "errmap_model",
		"name":         "bigrec",
		"computations": map[string]string{"out": "big + 1"},
	})
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422 for compute overflow, got %d body = %s", rec.Code, rec.Body.String())
	}

	// 2. A truly missing record must stay 404.
	rec = post("/v1/get", map[string]interface{}{
		"model": "errmap_model",
		"name":  "ghost",
	})
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for missing record, got %d body = %s", rec.Code, rec.Body.String())
	}

	// 3. Overflow on List must be 422 with the record identified, not an empty 200.
	rec = post("/v1/list", map[string]interface{}{
		"model":        "errmap_model",
		"computations": map[string]string{"out": "big + 1"},
	})
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422 for list compute overflow, got %d body = %s", rec.Code, rec.Body.String())
	}
	if !bytes.Contains(rec.Body.Bytes(), []byte("bigrec")) {
		t.Fatalf("expected list error to identify record bigrec, body = %s", rec.Body.String())
	}
}

func TestInvalidScheduleFailsStartup(t *testing.T) {
	newCfg := func() ServerConfig {
		dir, err := os.MkdirTemp("", "anchordb-sched-test-*")
		if err != nil {
			t.Fatalf("temp dir failed: %v", err)
		}
		t.Cleanup(func() { _ = os.RemoveAll(dir) })
		return ServerConfig{
			DataDir: dir,
			Port:    "0",
			WorkerConfig: worker.Config{
				Enabled:         true,
				Concurrency:     1,
				QueueSize:       10,
				ShutdownTimeout: time.Second,
			},
		}
	}

	// 1. Invalid partition schedule must fail startup, not log-and-continue.
	cfg := newCfg()
	cfg.CronEnabled = true
	cfg.PartitionSchedule = "not-a-schedule"
	if _, err := NewServer(cfg); err == nil || !strings.Contains(err.Error(), "invalid partition schedule") {
		t.Fatalf("expected invalid partition schedule error, got %v", err)
	}

	// 2. Invalid sync interval must fail startup too.
	cfg = newCfg()
	cfg.CronEnabled = true
	cfg.PartitionSchedule = "02:00"
	cfg.SyncInterval = "every now and then"
	if _, err := NewServer(cfg); err == nil || !strings.Contains(err.Error(), "invalid sync interval") {
		t.Fatalf("expected invalid sync interval error, got %v", err)
	}

	// 3. Valid schedules still start.
	cfg = newCfg()
	cfg.CronEnabled = true
	cfg.PartitionSchedule = "02:00"
	cfg.SyncInterval = "5m"
	s, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("valid schedules must start, got %v", err)
	}
	if s.scheduler != nil {
		s.scheduler.Stop()
	}
	_ = s.worker.Shutdown()
}
