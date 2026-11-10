package wire

import (
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rachmanzz/anchordb/pkg/storage"
)

// TestWire_SelectModel_SequentialIsolation is the regression test for audit
// finding W7 (anchorwire_bench_v1): sharing one connection across models wrote
// records to the wrong model. The client-side data race on Client.model is
// closed with a mutex; this test pins the remaining contract — sequential
// SelectModel calls address exactly the selected model, verified on both
// sides of the switch.
//
// Concurrent multi-model use of a single Client remains unsupported by design
// (see the Client doc comment); concurrent workloads must use one Client per
// model, as the driver pool in pkg/driver does.
func TestWire_SelectModel_SequentialIsolation(t *testing.T) {
	tmpDir := t.TempDir()
	backend := &mockBackend{
		dataDir: tmpDir,
		models:  make(map[string]*storage.AnchorDB),
	}
	defer backend.CloseAll()

	server := NewServer(backend)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen on dynamic port: %v", err)
	}
	addr := listener.Addr().String()

	go func() {
		_ = server.Serve(listener)
	}()
	defer server.Close()

	client, err := Connect(addr,
		WithClientModel("m1"),
		WithClientTimeout(5*time.Second),
	)
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer client.Close()

	save := func(name string, voltage float32) {
		t.Helper()
		if _, err := client.Save(storage.BatchRecordItem{
			Name:      name,
			Operation: storage.OpStatusActive,
			Props:     map[string]float32{"voltage": voltage},
		}); err != nil {
			t.Fatalf("Save %s failed: %v", name, err)
		}
	}
	getVoltage := func(name string) float32 {
		t.Helper()
		props, _, err := client.Get(name)
		if err != nil {
			t.Fatalf("Get %s failed: %v", name, err)
		}
		return props["voltage"]
	}

	// Same record name in two models must hold independent values.
	save("shared_rec", 1.5)
	if got := getVoltage("shared_rec"); got != 1.5 {
		t.Fatalf("expected m1 voltage 1.5, got %v", got)
	}
	if err := client.SelectModel("m2"); err != nil {
		t.Fatalf("SelectModel m2 failed: %v", err)
	}
	if got := client.ModelName(); got != "m2" {
		t.Fatalf("expected active model m2, got %q", got)
	}
	save("shared_rec", 9.5)
	if got := getVoltage("shared_rec"); got != 9.5 {
		t.Fatalf("expected m2 voltage 9.5, got %v", got)
	}

	// Switch back: m1 must be untouched by the m2 writes.
	if err := client.SelectModel("m1"); err != nil {
		t.Fatalf("SelectModel m1 failed: %v", err)
	}
	if got := getVoltage("shared_rec"); got != 1.5 {
		t.Fatalf("m2 write leaked into m1: expected 1.5, got %v", got)
	}

	// Independence must also hold on disk: sync both models, release every
	// handle (the backend holds an flock per model file), then reopen the
	// files directly and verify each kept its own value.
	if err := client.Sync(); err != nil {
		t.Fatalf("Sync m1 failed: %v", err)
	}
	if err := client.SelectModel("m2"); err != nil {
		t.Fatalf("SelectModel m2 failed: %v", err)
	}
	if err := client.Sync(); err != nil {
		t.Fatalf("Sync m2 failed: %v", err)
	}
	_ = client.Close()
	server.Close()
	backend.CloseAll()
	for model, want := range map[string]float32{"m1": 1.5, "m2": 9.5} {
		db, err := storage.Open(filepath.Join(tmpDir, model+".anchordb"))
		if err != nil {
			t.Fatalf("Open %s failed: %v", model, err)
		}
		props, err := db.GetRecord("shared_rec")
		if err != nil {
			_ = db.Close()
			t.Fatalf("GetRecord %s failed: %v", model, err)
		}
		if props["voltage"] != want {
			_ = db.Close()
			t.Fatalf("expected %s voltage %v on disk, got %v", model, want, props["voltage"])
		}
		if err := db.Close(); err != nil {
			t.Fatalf("Close %s failed: %v", model, err)
		}
	}
}

// TestWire_GetErrorCodes pins the TN2-02 wire mapping: a failed computation
// surfaces code 422 while a truly missing record stays 404.
func TestWire_GetErrorCodes(t *testing.T) {
	tmpDir := t.TempDir()
	backend := &mockBackend{
		dataDir: tmpDir,
		models:  make(map[string]*storage.AnchorDB),
	}
	defer backend.CloseAll()

	server := NewServer(backend)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen on dynamic port: %v", err)
	}
	addr := listener.Addr().String()

	go func() {
		_ = server.Serve(listener)
	}()
	defer server.Close()

	client, err := Connect(addr,
		WithClientModel("errmap"),
		WithClientTimeout(5*time.Second),
	)
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer client.Close()

	if _, err := client.SaveTyped("bigrec", storage.OpStatusActive, []storage.AttrEntry{
		storage.NewInt64Attr("big", 9223372036854775807),
	}); err != nil {
		t.Fatalf("SaveTyped failed: %v", err)
	}

	_, err = client.GetWithOptionsTyped(GetRequest{
		Name:         "bigrec",
		Computations: map[string]storage.ComputeOp{"out": storage.OpAdd{FieldA: "big", FieldB: "1"}},
	})
	if err == nil || !strings.Contains(err.Error(), "code 422") {
		t.Fatalf("expected wire code 422 for compute overflow, got %v", err)
	}

	_, err = client.GetWithOptionsTyped(GetRequest{Name: "ghost"})
	if err == nil || !strings.Contains(err.Error(), "code 404") {
		t.Fatalf("expected wire code 404 for missing record, got %v", err)
	}
}

// legacyTieredBackend simulates a third-party backend predating ColdTierBackend:
// its tiered fallback repeats the hot read before going cold.
type legacyTieredBackend struct {
	mu            sync.Mutex
	dataDir       string
	models        map[string]*storage.AnchorDB
	getModelCalls int64
}

func (l *legacyTieredBackend) GetModel(name string) (*storage.AnchorDB, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.getModelCalls++
	if db, ok := l.models[name]; ok {
		return db, nil
	}
	db, err := storage.Open(filepath.Join(l.dataDir, name+".anchordb"))
	if err != nil {
		return nil, err
	}
	l.models[name] = db
	return db, nil
}

func (l *legacyTieredBackend) Authenticate(string) bool { return true }
func (l *legacyTieredBackend) RequiresAuth() bool       { return false }

func (l *legacyTieredBackend) GetRecordTypedWithComputeTiered(model, name string, _ map[string]storage.ComputeOp, _ []string) (storage.CompactRecord, error) {
	db, err := l.GetModel(model)
	if err != nil {
		return storage.CompactRecord{}, err
	}
	if _, err := db.GetRecordTyped(name); err != nil {
		return storage.CompactRecord{}, fmt.Errorf("record %q not found in model %q: %w", name, model, storage.ErrRecordNotFound)
	}
	return storage.CompactRecord{}, fmt.Errorf("unreachable legacy cold path")
}

func (l *legacyTieredBackend) CloseAll() {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, db := range l.models {
		_ = db.Close()
	}
}

// TestWire_ColdPathSkipsRedundantHotRead pins the hot-ganda fix: a GET miss
// resolves the model exactly twice (one Save + one Get), proving the cold-only
// branch does not repeat the hot lookup the request already performed.
func TestWire_ColdPathSkipsRedundantHotRead(t *testing.T) {
	tmpDir := t.TempDir()
	backend := &mockBackend{
		dataDir: tmpDir,
		models:  make(map[string]*storage.AnchorDB),
	}
	defer backend.CloseAll()

	server := NewServer(backend)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen on dynamic port: %v", err)
	}
	addr := listener.Addr().String()

	go func() {
		_ = server.Serve(listener)
	}()
	defer server.Close()

	client, err := Connect(addr,
		WithClientModel("probe"),
		WithClientTimeout(5*time.Second),
	)
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer client.Close()

	if _, err := client.Save(storage.BatchRecordItem{Name: "r1"}); err != nil {
		t.Fatalf("Save failed: %v", err)
	}
	backend.mu.Lock()
	before := backend.getModelCalls
	backend.mu.Unlock()
	if _, _, err := client.Get("ghost"); err == nil {
		t.Fatalf("expected miss for ghost, got nil error")
	}
	backend.mu.Lock()
	delta := backend.getModelCalls - before
	backend.mu.Unlock()
	// One resolution at request dispatch; the cold-only branch must not
	// resolve the model a second time for the repeated hot read.
	if delta != 1 {
		t.Fatalf("expected exactly 1 model resolution for a GET miss, got %d (hot lookup repeated)", delta)
	}
}

// TestWire_LegacyTieredFallbackPreserved proves the pre-ColdTierBackend path
// still works: a backend without the new method keeps the old behavior
// (404 on miss) at the old cost (one repeated hot read: delta == 2).
func TestWire_LegacyTieredFallbackPreserved(t *testing.T) {
	tmpDir := t.TempDir()
	backend := &legacyTieredBackend{
		dataDir: tmpDir,
		models:  make(map[string]*storage.AnchorDB),
	}
	defer backend.CloseAll()

	server := NewServer(backend)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen on dynamic port: %v", err)
	}
	addr := listener.Addr().String()

	go func() {
		_ = server.Serve(listener)
	}()
	defer server.Close()

	client, err := Connect(addr,
		WithClientModel("probe"),
		WithClientTimeout(5*time.Second),
	)
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer client.Close()

	if _, err := client.Save(storage.BatchRecordItem{Name: "r1"}); err != nil {
		t.Fatalf("Save failed: %v", err)
	}
	backend.mu.Lock()
	before := backend.getModelCalls
	backend.mu.Unlock()
	if _, _, err := client.Get("ghost"); err == nil || !strings.Contains(err.Error(), "code 404") {
		t.Fatalf("expected wire code 404 for missing record, got %v", err)
	}
	backend.mu.Lock()
	delta := backend.getModelCalls - before
	backend.mu.Unlock()
	if delta != 2 {
		t.Fatalf("expected legacy fallback to resolve the model twice, got %d", delta)
	}
}
