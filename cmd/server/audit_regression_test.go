package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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
