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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rachmanzz/anchordb/pkg/partition"
	"github.com/rachmanzz/anchordb/pkg/spatial"
	"github.com/rachmanzz/anchordb/pkg/storage"
	"github.com/rachmanzz/anchordb/pkg/worker"
)

// When a token is configured, every /v1 endpoint must reject unauthenticated
// requests and accept a correctly formed bearer token.
func TestAuthMiddlewareRequiresToken(t *testing.T) {
	s, dir := createTestServer(t)
	defer cleanupServer(t, s, dir)
	s.authToken = "s3cr3t"

	handler := s.Routes()
	body := []byte(`{"model":"m","name":"n","props":{"v":1}}`)

	req := httptest.NewRequest(http.MethodPost, "/v1/save", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 without token, got %d: %s", rec.Code, rec.Body.String())
	}

	req = httptest.NewRequest(http.MethodPost, "/v1/save", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer s3cr3t")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 with valid token, got %d: %s", rec.Code, rec.Body.String())
	}
}

// Unauthenticated /health must not disclose internal state (model names, memory
// stats, background subsystem state) when a token is configured.
func TestHealthHidesDetailsWhenUnauthenticated(t *testing.T) {
	s, dir := createTestServer(t)
	defer cleanupServer(t, s, dir)
	s.authToken = "s3cr3t"

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	s.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("health status = %d, want 200", rec.Code)
	}
	for _, leaked := range []string{"loaded_models", "memory", "background", "uptime_seconds"} {
		if strings.Contains(rec.Body.String(), leaked) {
			t.Fatalf("unauthenticated /health leaked %q: %s", leaked, rec.Body.String())
		}
	}
}

// Mutation endpoints must reject non-POST methods with 405.
func TestMutationEndpointsRejectWrongMethod(t *testing.T) {
	s, dir := createTestServer(t)
	defer cleanupServer(t, s, dir)

	req := httptest.NewRequest(http.MethodGet, "/v1/list", nil)
	rec := httptest.NewRecorder()
	s.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 for GET /v1/list, got %d: %s", rec.Code, rec.Body.String())
	}
}

// Filter values that are neither numeric nor dates must be rejected instead of
// being silently coerced to zero (which could mutate/match unintended records).
func TestInvalidFilterValueRejected(t *testing.T) {
	s, dir := createFastTestServer(t)
	defer cleanupServer(t, s, dir)

	body := []byte(`{"model":"m","filters":[{"field":"x","operator":"=","value":"not-a-number"}]}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/list", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	s.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid filter value, got %d: %s", rec.Code, rec.Body.String())
	}
}

// An s3_endpoint that resolves to a loopback/private address must be rejected to
// prevent server-side request forgery from an authenticated partition request.
func TestPartitionRunRejectsPrivateS3Endpoint(t *testing.T) {
	s, dir := createTestServer(t)
	defer cleanupServer(t, s, dir)

	body := []byte(`{"model":"orders","sink_type":"s3","s3_endpoint":"http://127.0.0.1:9000","s3_bucket":"b"}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/partition/run", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	s.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for private s3 endpoint, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestSaveSchemaRejectionDoesNotLeaveNode(t *testing.T) {
	s, dir := createTestServer(t)
	defer cleanupServer(t, s, dir)
	db, err := s.getModel("m")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.AttachSchema(storage.ModelSchema{"required": "float"}); err != nil {
		t.Fatal(err)
	}
	request := []byte(`{"model":"m","name":"orphan","props":{"wrong":1},"coord":{"X":0.2,"Y":0.3}}`)
	rec := httptest.NewRecorder()
	s.Routes().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/save", bytes.NewReader(request)))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("save status=%d body=%s", rec.Code, rec.Body.String())
	}
	if _, exists := db.Lexicon["orphan"]; exists {
		t.Fatal("schema rejection left an orphan node")
	}
}

func TestDynamicAPIErrorIsJSON(t *testing.T) {
	s, dir := createTestServer(t)
	defer cleanupServer(t, s, dir)
	db, err := s.getModel("m")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.SaveRecord("existing", map[string]float32{"x": 1}); err != nil {
		t.Fatal(err)
	}
	expr := "bad\"line\n"
	request, err := json.Marshal(GetRequest{Model: "m", Name: "existing", Computations: map[string]string{"out": expr}})
	if err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	s.Routes().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/get", bytes.NewReader(request)))
	if rec.Code != http.StatusBadRequest || rec.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("error status=%d content-type=%q body=%s", rec.Code, rec.Header().Get("Content-Type"), rec.Body.String())
	}
	var payload map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("invalid JSON error: %v body=%s", err, rec.Body.String())
	}
	if !strings.Contains(payload["error"], strconv.Quote(expr)) {
		t.Fatalf("error omitted expression: %q", payload["error"])
	}
}

func TestCollectorReportsSyncFailure(t *testing.T) {
	s, dir := createTestServer(t)
	defer cleanupServer(t, s, dir)
	db, err := s.getModel("m")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.SaveRecordWithIDAndOp("expired", storage.OpStatusTombstone, map[string]float32{"x": 1}); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "m.anchordb")
	held := path + ".held"
	if err := os.Rename(path, held); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = os.RemoveAll(path)
		_ = os.Rename(held, path)
	}()
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "keep"), []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	s.Routes().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/collector/run", strings.NewReader(`{"model":"m"}`)))
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("collector falsely succeeded: status=%d body=%s", rec.Code, rec.Body.String())
	}
	var payload map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil || !strings.Contains(payload["error"], "sync model m") {
		t.Fatalf("collector error: payload=%v err=%v", payload, err)
	}
}

func TestAudit_StrictEnvNegativeBoundsHealthAndColdList(t *testing.T) {
	t.Setenv("TEST_BAD_BOOL", "maybe")
	if _, err := getEnvBool("TEST_BAD_BOOL", false); err == nil {
		t.Fatal("expected getEnvBool to fail on invalid bool")
	}
	t.Setenv("TEST_BAD_INT", "abc")
	if _, err := getEnvInt("TEST_BAD_INT", 1); err == nil {
		t.Fatal("expected getEnvInt to fail on invalid int")
	}
	t.Setenv("TEST_BAD_FLOAT", "nope")
	if _, err := getEnvFloat("TEST_BAD_FLOAT", 1.0); err == nil {
		t.Fatal("expected getEnvFloat to fail on invalid float")
	}

	s, dir := createFastTestServer(t)
	defer cleanupServer(t, s, dir)
	handler := s.Routes()

	// Negative /v1/list bounds must fail 400.
	recNeg := httptest.NewRecorder()
	handler.ServeHTTP(recNeg, httptest.NewRequest(http.MethodPost, "/v1/list", strings.NewReader(`{"model":"m","limit":-1}`)))
	if recNeg.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 on negative limit, got %d: %s", recNeg.Code, recNeg.Body.String())
	}

	// Archive a cold record and verify /v1/list falls back to cold tier.
	db, release, err := s.acquireModel("cold_m")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.SaveRecordWithIDAndOp("old_rec", storage.OpStatusActive, map[string]float32{"date": 1, "val": 42}); err != nil {
		release()
		t.Fatal(err)
	}
	partRes, err := partition.ExecutePartition(db, partition.PartitionPolicy{
		ModelName:     "cold_m",
		DateField:     "date",
		RetentionDays: 1,
		Sink:          partition.NewLocalDiskSink(filepath.Join(dir, "partitions")),
		FileFormat:    "{date}_{uuid}.anchordb",
		PruneHotDB:    true,
		Catalog:       s.catalog,
	})
	release()
	if err != nil || partRes.ArchivedCount != 1 {
		t.Fatalf("ExecutePartition failed: res=%+v err=%v", partRes, err)
	}

	recList := httptest.NewRecorder()
	handler.ServeHTTP(recList, httptest.NewRequest(http.MethodPost, "/v1/list", strings.NewReader(`{"model":"cold_m","limit":10}`)))
	if recList.Code != http.StatusOK {
		t.Fatalf("cold list status=%d body=%s", recList.Code, recList.Body.String())
	}
	var listResp struct {
		Success bool `json:"success"`
		Count   int  `json:"count"`
		Items   []struct {
			Name string `json:"name"`
		} `json:"items"`
		Tier string `json:"tier"`
	}
	if err := json.Unmarshal(recList.Body.Bytes(), &listResp); err != nil {
		t.Fatalf("unmarshal cold list: %v", err)
	}
	if listResp.Tier != "cold" || listResp.Count != 1 || listResp.Items[0].Name != "old_rec" {
		t.Fatalf("expected cold fallback item 'old_rec', got %+v", listResp)
	}

	// Closing wireServer before draining must degrade /health to 503.
	if err := s.wireServer.Close(); err != nil {
		t.Fatalf("wireServer.Close: %v", err)
	}
	recHealth := httptest.NewRecorder()
	handler.ServeHTTP(recHealth, httptest.NewRequest(http.MethodGet, "/health", nil))
	if recHealth.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 on closed wireServer, got %d: %s", recHealth.Code, recHealth.Body.String())
	}
}

func TestPartitionRun_AsyncPartialRetryIdempotencyAndS3Region(t *testing.T) {
	t.Setenv("ANCHORDB_ALLOW_S3_PRIVATE_ENDPOINT", "true")
	t.Setenv("ANCHORDB_ALLOW_INSECURE_S3_HTTP", "true")

	var mu sync.Mutex
	putPaths := make([]string, 0, 4)
	authHeaders := make([]string, 0, 4)
	var putCount atomic.Int32

	s3Srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		n := putCount.Add(1)
		mu.Lock()
		putPaths = append(putPaths, r.URL.Path)
		authHeaders = append(authHeaders, r.Header.Get("Authorization"))
		mu.Unlock()
		// Fail the second model's first upload attempt, succeed on retry.
		if n == 2 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer s3Srv.Close()

	dir := t.TempDir()
	s, err := NewServer(ServerConfig{
		DataDir:           dir,
		Port:              "0",
		S3DefaultBucket:   "default-bucket",
		S3DefaultPrefix:   "archive",
		S3DefaultRegion:   "ap-southeast-1",
		S3DefaultEndpoint: s3Srv.URL,
		WorkerConfig: worker.Config{
			Enabled:         true,
			Concurrency:     2,
			QueueSize:       16,
			ShutdownTimeout: 2 * time.Second,
		},
		DBOptions: storage.Options{WAL: storage.WALOptions{SyncMode: storage.SyncNone}},
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	defer cleanupServer(t, s, dir)

	for _, m := range []string{"m1", "m2"} {
		db, release, err := s.acquireModel(m)
		if err != nil {
			t.Fatalf("acquireModel(%s): %v", m, err)
		}
		if _, err := db.SaveRecordWithIDAndOp(m+"_old", storage.OpStatusActive, map[string]float32{"date": 10, "val": 1}); err != nil {
			release()
			t.Fatalf("save(%s): %v", m, err)
		}
		release()
	}

	body := `{"model":"all","sink_type":"s3","retention_days":30,"s3_access_key":"ak","s3_secret_key":"sk","async":true}`
	req := httptest.NewRequest(http.MethodPost, "/v1/partition/run", strings.NewReader(body))
	rec := httptest.NewRecorder()
	s.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted, got %d: %s", rec.Code, rec.Body.String())
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.worker.WaitIdle(waitCtx); err != nil {
		t.Fatalf("WaitIdle: %v", err)
	}

	mu.Lock()
	// m1 succeeds once + m2 fails once and succeeds on retry => 3 PUTs total (never duplicates m1).
	if len(putPaths) != 3 {
		mu.Unlock()
		t.Fatalf("expected 3 total S3 PUTs (m1 once, m2 retried once), got %d (%v)", len(putPaths), putPaths)
	}
	for i, p := range putPaths {
		if !strings.HasPrefix(p, "/default-bucket/archive/") {
			mu.Unlock()
			t.Fatalf("PUT #%d missing default bucket/prefix '/default-bucket/archive/': %q", i, p)
		}
	}
	for i, auth := range authHeaders {
		if !strings.Contains(auth, "/ap-southeast-1/s3/aws4_request") {
			mu.Unlock()
			t.Fatalf("PUT #%d Authorization missing s3DefaultRegion 'ap-southeast-1': %q", i, auth)
		}
	}
	mu.Unlock()

	if entries := s.catalog.ListEntries("m1"); len(entries) != 1 {
		t.Fatalf("expected exactly 1 catalog entry for m1 (no duplicate on retry), got %d", len(entries))
	} else if entries[0].S3Metadata == nil || entries[0].S3Metadata.Region != "ap-southeast-1" {
		t.Fatalf("expected catalog entry S3Metadata.Region='ap-southeast-1', got %+v", entries[0].S3Metadata)
	}
	if entries := s.catalog.ListEntries("m2"); len(entries) != 1 {
		t.Fatalf("expected exactly 1 catalog entry for m2 after retry, got %d", len(entries))
	}
}

func TestServer_ColdMathGraphFallbackAndAsyncModelSanitization(t *testing.T) {
	s, dir := createTestServer(t)
	defer cleanupServer(t, s, dir)
	handler := s.Routes()

	// 1. Empty partitions directory must return [] (not JSON null) from /v1/partition/list.
	if err := os.MkdirAll(filepath.Join(dir, "partitions"), 0o750); err != nil {
		t.Fatalf("MkdirAll partitions: %v", err)
	}
	recListEmpty := httptest.NewRecorder()
	handler.ServeHTTP(recListEmpty, httptest.NewRequest(http.MethodGet, "/v1/partition/list", nil))
	if recListEmpty.Code != http.StatusOK {
		t.Fatalf("partition list status = %d: %s", recListEmpty.Code, recListEmpty.Body.String())
	}
	if !strings.Contains(recListEmpty.Body.String(), `"partitions":[]`) {
		t.Fatalf("expected non-null empty partitions array, got %s", recListEmpty.Body.String())
	}

	// 2. Async /v1/partition/run and /v1/collector/run must reject invalid model names before 202 Accepted.
	for _, ep := range []string{"/v1/partition/run", "/v1/collector/run"} {
		recBad := httptest.NewRecorder()
		handler.ServeHTTP(recBad, httptest.NewRequest(http.MethodPost, ep, strings.NewReader(`{"model":"../bad","async":true}`)))
		if recBad.Code != http.StatusBadRequest {
			t.Fatalf("%s invalid model status = %d, want 400: %s", ep, recBad.Code, recBad.Body.String())
		}
	}

	// 3. Save records with coords/roles, archive & prune hot DB, then verify cold /v1/math and /v1/graph fallback + 404.
	for _, payload := range []string{
		`{"model":"cold_math","name":"n1","props":{"date":1},"coord":{"X":0.1,"Y":0.1},"roles":[1,0,0,0]}`,
		`{"model":"cold_math","name":"n2","props":{"date":1},"coord":{"X":0.4,"Y":0.5},"roles":[0.8,0.6,0,0]}`,
	} {
		recSave := httptest.NewRecorder()
		handler.ServeHTTP(recSave, httptest.NewRequest(http.MethodPost, "/v1/save", strings.NewReader(payload)))
		if recSave.Code != http.StatusOK {
			t.Fatalf("save failed: %d %s", recSave.Code, recSave.Body.String())
		}
	}
	db, release, err := s.acquireModel("cold_math")
	if err != nil {
		t.Fatalf("acquireModel: %v", err)
	}
	if err := db.SetRelation("link", spatial.Point2D{X: 0.3, Y: 0.4}); err != nil {
		release()
		t.Fatalf("SetRelation: %v", err)
	}
	if err := db.AddEdge("n1", "n2", "link", 1.0); err != nil {
		release()
		t.Fatalf("AddEdge: %v", err)
	}
	partRes, err := partition.ExecutePartition(db, partition.PartitionPolicy{
		ModelName:     "cold_math",
		DateField:     "date",
		RetentionDays: 1,
		Sink:          partition.NewLocalDiskSink(filepath.Join(dir, "partitions")),
		FileFormat:    "{date}_{uuid}.anchordb",
		PruneHotDB:    true,
		Catalog:       s.catalog,
	})
	release()
	if err != nil || partRes.ArchivedCount != 2 {
		t.Fatalf("ExecutePartition: res=%+v err=%v", partRes, err)
	}

	for _, tc := range []struct {
		endpoint string
		body     string
		wantCode int
	}{
		{"/v1/math", `{"model":"cold_math","op":"distance","words":["n1","n2"]}`, http.StatusOK},
		{"/v1/math", `{"model":"cold_math","op":"haversine","words":["n1","n2"]}`, http.StatusOK},
		{"/v1/math", `{"model":"cold_math","op":"centroid","words":["n1","n2"]}`, http.StatusOK},
		{"/v1/math", `{"model":"cold_math","op":"cosine_similarity","words":["n1","n2"]}`, http.StatusOK},
		{"/v1/math", `{"model":"cold_math","op":"nearest","words":["n1"],"limit_k":2}`, http.StatusOK},
		{"/v1/math", `{"model":"cold_math","op":"distance","words":["n1","no_such_word"]}`, http.StatusNotFound},
		{"/v1/graph", `{"model":"cold_math","inputs":["n1"],"relation":"link","limit_k":5}`, http.StatusOK},
		{"/v1/graph", `{"model":"cold_math","inputs":["no_such_word"],"relation":"link","limit_k":5}`, http.StatusNotFound},
	} {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, tc.endpoint, strings.NewReader(tc.body)))
		if rec.Code != tc.wantCode {
			t.Fatalf("%s %s status = %d, want %d: %s", tc.endpoint, tc.body, rec.Code, tc.wantCode, rec.Body.String())
		}
	}
}

func TestServer_NegativeVsZeroContractBoundaries(t *testing.T) {
	s, dir := createTestServer(t)
	defer cleanupServer(t, s, dir)
	handler := s.Routes()

	cases := []struct {
		endpoint string
		body     string
		wantCode int
	}{
		{"/v1/list", `{"model":"m","limit":0,"offset":0}`, http.StatusOK},
		{"/v1/list", `{"model":"m","limit":-1}`, http.StatusBadRequest},
		{"/v1/list", `{"model":"m","offset":-1}`, http.StatusBadRequest},
		{"/v1/partition/run", `{"model":"m","retention_days":0}`, http.StatusOK},
		{"/v1/partition/run", `{"model":"m","retention_days":-5}`, http.StatusBadRequest},
		{"/v1/collector/run", `{"model":"m","max_batch":0}`, http.StatusOK},
		{"/v1/collector/run", `{"model":"m","max_batch":-1}`, http.StatusBadRequest},
		{"/v1/graph", `{"model":"m","inputs":["a"],"relation":"r","limit_k":-1}`, http.StatusBadRequest},
		{"/v1/graph", `{"model":"m","inputs":["a"],"relation":"r","beam_width":-0.1}`, http.StatusBadRequest},
		{"/v1/math", `{"model":"m","op":"nearest","words":["a"],"limit_k":-1}`, http.StatusBadRequest},
	}

	for _, tc := range cases {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, tc.endpoint, strings.NewReader(tc.body)))
		if rec.Code != tc.wantCode {
			t.Fatalf("%s %s status = %d, want %d: %s", tc.endpoint, tc.body, rec.Code, tc.wantCode, rec.Body.String())
		}
	}
}
