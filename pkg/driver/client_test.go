package driver

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"
)

func TestDriver_ConnectDSNParsing(t *testing.T) {
	tmpDir := t.TempDir()
	localFile1 := filepath.Join(tmpDir, "local_test_1.anchordb")
	localFile2 := filepath.Join(tmpDir, "local_test_2.anchordb")

	// 1. Open local with legacy ?mode=mmap (should strip parameter and open successfully)
	c1, err := Connect("anchordb://" + localFile1 + "?mode=mmap")
	if err != nil {
		t.Fatalf("Connect local with ?mode=mmap failed: %v", err)
	}
	defer c1.Close()

	if !c1.IsLocal() || c1.IsRemote() {
		t.Errorf("expected c1 to be local")
	}
	if c1.DB() == nil {
		t.Errorf("expected c1.DB() to be non-nil for local client")
	}

	// 2. OpenLocal directly
	c2, err := OpenLocal(localFile2)
	if err != nil {
		t.Fatalf("OpenLocal failed: %v", err)
	}
	defer c2.Close()

	if !c2.IsLocal() {
		t.Errorf("expected c2 to be local")
	}

	// 3. Connect remote with anchordb:// scheme and credentials
	c3, err := Connect("anchordb://myuser:mypassword@localhost:7447/iot_network?token=override_token&model=override_model")
	if err != nil {
		t.Fatalf("Connect remote DSN failed: %v", err)
	}
	defer c3.Close()

	if !c3.IsRemote() || c3.IsLocal() {
		t.Errorf("expected c3 to be remote")
	}
	if c3.ModelName() != "override_model" {
		t.Errorf("expected model 'override_model', got %q", c3.ModelName())
	}

	// 4. Connect remote with http:// scheme
	c4, err := Connect("http://127.0.0.1:7447?model=plants")
	if err != nil {
		t.Fatalf("Connect http DSN failed: %v", err)
	}
	defer c4.Close()

	if !c4.IsRemote() {
		t.Errorf("expected c4 to be remote")
	}
	if c4.ModelName() != "plants" {
		t.Errorf("expected model 'plants', got %q", c4.ModelName())
	}
}

func TestDriver_RemoteBackendErrorHandling(t *testing.T) {
	// Mock server that returns errors
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/save":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte(`{"error":"record key conflict"}`))
		case "/v1/get":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"record not found"}`))
		case "/v1/timeout":
			time.Sleep(100 * time.Millisecond)
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`internal failure`))
		}
	}))
	defer ts.Close()

	client, err := ConnectRemote(ts.URL, WithTimeout(50*time.Millisecond))
	if err != nil {
		t.Fatalf("ConnectRemote failed: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	// 1. Save returns error from JSON
	err = client.Save("test_conflict", map[string]float32{"x": 1})
	if err == nil || err.Error() != "engine error (status 409): record key conflict" {
		t.Errorf("expected engine error 409, got: %v", err)
	}

	// 2. Get returns not found
	_, err = client.Get("nonexistent").Execute(ctx)
	if err == nil || err.Error() != "engine error (status 404): record not found" {
		t.Errorf("expected engine error 404, got: %v", err)
	}
}
