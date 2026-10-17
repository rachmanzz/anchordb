package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/rachmanzz/anchordb/pkg/storage"
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
