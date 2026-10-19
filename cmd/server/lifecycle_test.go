package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/rachmanzz/anchordb/pkg/worker"
)

// Clean path: everything stops in time, so models are synced, closed and the
// registry is sealed (L6, L11), and /health reports draining (L8).
func TestShutdownCleanClosesAndSeals(t *testing.T) {
	s, dir := createTestServer(t)
	defer cleanupServer(t, s, dir)

	db, err := s.getModel("m")
	if err != nil {
		t.Fatalf("getModel: %v", err)
	}
	if _, err := db.SaveRecordWithID("a", map[string]float32{"x": 1}); err != nil {
		t.Fatalf("save: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.Shutdown(ctx, nil); err != nil {
		t.Fatalf("clean Shutdown returned error: %v", err)
	}

	s.mu.RLock()
	remaining := len(s.models)
	s.mu.RUnlock()
	if remaining != 0 {
		t.Fatalf("expected models map emptied after clean shutdown, have %d", remaining)
	}
	if _, err := s.getModel("m"); !errors.Is(err, ErrServerClosed) {
		t.Fatalf("getModel after Shutdown: want ErrServerClosed, got %v", err)
	}
	if _, err := s.getModel("never_loaded"); !errors.Is(err, ErrServerClosed) {
		t.Fatalf("opening a new model after Shutdown must be refused, got %v", err)
	}

	rec := httptest.NewRecorder()
	s.handleHealth(rec, httptest.NewRequest(http.MethodGet, "/health", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("health while shut down: want 503, got %d", rec.Code)
	}
}

// L1: a worker task that ignores cancellation makes Shutdown time out. The DB
// must then NOT be closed under it (that would silently drop its writes).
func TestShutdownWorkerTimeoutDoesNotCloseDB(t *testing.T) {
	s, dir := createTestServerWithWorkerTimeout(t, 50*time.Millisecond)
	defer cleanupServer(t, s, dir)

	db, err := s.getModel("m")
	if err != nil {
		t.Fatalf("getModel: %v", err)
	}

	release := make(chan struct{})
	started := make(chan struct{})
	if err := s.worker.Dispatch(context.Background(), "stuck", func(context.Context) error {
		close(started)
		<-release // ignores ctx, like a hung task
		return nil
	}); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	<-started
	defer close(release)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = s.Shutdown(ctx, nil)
	if !errors.Is(err, worker.ErrShutdownTimeout) {
		t.Fatalf("want ErrShutdownTimeout surfaced, got %v", err)
	}

	// The DB is still open and usable: a late task's write is not lost.
	if _, err := db.SaveRecordWithID("late", map[string]float32{"x": 1}); err != nil {
		t.Fatalf("DB was closed under a still-running task: %v", err)
	}
	// ...but the registry is sealed so nothing new is opened.
	if _, err := s.getModel("another"); !errors.Is(err, ErrServerClosed) {
		t.Fatalf("registry should be sealed, got %v", err)
	}
}

// L2: when the HTTP drain times out, Shutdown must force-close the server and
// keep the DBs open because a handler may still be running.
func TestShutdownHTTPTimeoutForcesCloseAndKeepsDB(t *testing.T) {
	s, dir := createTestServer(t)
	defer cleanupServer(t, s, dir)

	db, err := s.getModel("m")
	if err != nil {
		t.Fatalf("getModel: %v", err)
	}

	release := make(chan struct{})
	entered := make(chan struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("/slow", func(_ http.ResponseWriter, _ *http.Request) {
		close(entered)
		<-release
	})
	ts := httptest.NewUnstartedServer(mux)
	ts.Start()
	defer ts.Close()
	defer close(release)

	go func() { _, _ = http.Get(ts.URL + "/slow") }()
	<-entered

	s.shutdownTimeout = 300 * time.Millisecond // HTTP phase gets budget/3 = 100ms
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = s.Shutdown(ctx, ts.Config)
	if err == nil {
		t.Fatalf("expected HTTP drain timeout to be reported")
	}
	if _, err := db.SaveRecordWithID("late", map[string]float32{"x": 1}); err != nil {
		t.Fatalf("DB was closed while a handler may still be running: %v", err)
	}
}

// L10: an unusable data dir fails fast at startup instead of per request.
func TestNewServerRejectsUnwritableDataDir(t *testing.T) {
	_, err := NewServer(ServerConfig{
		DataDir:      "/proc/anchordb-cannot-write-here",
		WorkerConfig: worker.Config{Enabled: false},
	})
	if err == nil {
		t.Fatal("expected NewServer to fail for an unwritable data dir")
	}
}
