package wire

import (
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/rachmanzz/anchordb/pkg/spatial"
	"github.com/rachmanzz/anchordb/pkg/storage"
)

type mockBackend struct {
	mu        sync.Mutex
	dataDir   string
	models    map[string]*storage.AnchorDB
	authToken string
}

func (m *mockBackend) GetModel(name string) (*storage.AnchorDB, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if db, ok := m.models[name]; ok {
		return db, nil
	}
	dbPath := filepath.Join(m.dataDir, name+".anchordb")
	db, err := storage.Open(dbPath)
	if err != nil {
		return nil, err
	}
	m.models[name] = db
	return db, nil
}

func (m *mockBackend) Authenticate(token string) bool {
	if m.authToken == "" {
		return true
	}
	return token == m.authToken
}

func (m *mockBackend) RequiresAuth() bool {
	return m.authToken != ""
}

func (m *mockBackend) CloseAll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, db := range m.models {
		_ = db.Close()
	}
}

func TestWire_ServerAndClient_EndToEnd(t *testing.T) {
	tmpDir := t.TempDir()
	backend := &mockBackend{
		dataDir:   tmpDir,
		models:    make(map[string]*storage.AnchorDB),
		authToken: "secret123",
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

	// 1. Connection without token should fail or fail on command
	unauthClient, err := Connect(addr, WithClientTimeout(2*time.Second))
	if err != nil {
		t.Fatalf("Connect without auth failed: %v", err)
	}
	if _, err := unauthClient.Save(storage.BatchRecordItem{Name: "unauth_item"}); err == nil {
		t.Fatalf("expected unauthenticated save to be rejected")
	}
	_ = unauthClient.Close()

	// 2. Connect with valid token
	client, err := Connect(addr,
		WithClientToken("secret123"),
		WithClientModel("wire_bench"),
		WithClientTimeout(5*time.Second),
	)
	if err != nil {
		t.Fatalf("Connect with token failed: %v", err)
	}
	defer client.Close()

	// 3. Ping
	if err := client.Ping(); err != nil {
		t.Fatalf("Ping failed: %v", err)
	}

	// 4. Save single record with geometry
	item := storage.BatchRecordItem{
		Name:      "sensor_hub_01",
		Operation: storage.OpStatusActive,
		Props: map[string]float32{
			"voltage":     12.4,
			"temperature": 45.2,
		},
		Coord: &spatial.Point2D{X: 10.0, Y: 20.0},
		Roles: &[4]float32{0.8, 0.2, 0.1, 0.1},
	}
	name, err := client.Save(item)
	if err != nil {
		t.Fatalf("Save failed: %v", err)
	}
	if name != "sensor_hub_01" {
		t.Fatalf("expected name sensor_hub_01, got %s", name)
	}

	// 5. Get record
	props, op, err := client.Get("sensor_hub_01")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if op != storage.OpStatusActive {
		t.Errorf("expected op %d, got %d", storage.OpStatusActive, op)
	}
	if props["voltage"] != 12.4 {
		t.Errorf("expected voltage 12.4, got %f", props["voltage"])
	}

	// 6. SaveBatch
	const batchSize = 100
	records := make([]storage.BatchRecordItem, batchSize)
	for i := 0; i < batchSize; i++ {
		records[i] = storage.BatchRecordItem{
			Name: fmt.Sprintf("sku_batch_%04d", i),
			Props: map[string]float32{
				"inventory": float32(i * 10),
				"price":     float32(i) * 1.5,
			},
		}
	}
	names, err := client.SaveBatch(records)
	if err != nil {
		t.Fatalf("SaveBatch failed: %v", err)
	}
	if len(names) != batchSize {
		t.Fatalf("expected %d names, got %d", batchSize, len(names))
	}

	// 7. Graph Edges & Causal Traversal
	if err := client.Edge("add", "hub_alpha", "powers", "sensor_hub_01", 0.95); err != nil {
		t.Fatalf("Edge add failed: %v", err)
	}
	results, err := client.Causal([]string{"hub_alpha"}, "powers", 5)
	if err != nil {
		t.Fatalf("Causal traversal failed: %v", err)
	}
	if len(results) != 1 || results[0].Word != "sensor_hub_01" || results[0].Score != 0.95 {
		t.Fatalf("unexpected causal results: %+v", results)
	}

	// 8. Sync
	if err := client.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}
}
