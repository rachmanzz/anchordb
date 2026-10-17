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

func TestWire_AdaptiveMemoryNegotiation_EndToEnd(t *testing.T) {
	tmpDir := t.TempDir()
	backend := &mockBackend{
		dataDir:   tmpDir,
		models:    make(map[string]*storage.AnchorDB),
		authToken: "pace_secret",
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
		WithClientToken("pace_secret"),
		WithClientModel("pace_model"),
		WithClientTimeout(5*time.Second),
		WithClientAdaptivePacing(50000),
	)
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer client.Close()

	// 1. Verify initial negotiation occurred
	nego := client.GetNegotiation()
	if nego == nil {
		t.Fatalf("expected non-nil negotiation")
	}
	if nego.AllocatedBatchSize == 0 {
		t.Errorf("expected AllocatedBatchSize > 0, got %d", nego.AllocatedBatchSize)
	}

	// 2. Explicit negotiation call
	negoResp, err := client.NegotiateMemory(100000, 512, 1000)
	if err != nil {
		t.Fatalf("NegotiateMemory failed: %v", err)
	}
	if negoResp.AllocatedBatchSize == 0 {
		t.Errorf("expected non-zero AllocatedBatchSize")
	}

	// 3. SaveBatch with pacing active
	const totalItems = 120
	items := make([]storage.BatchRecordItem, totalItems)
	for i := 0; i < totalItems; i++ {
		items[i] = storage.BatchRecordItem{
			Name: fmt.Sprintf("item_paced_%d", i),
			Props: map[string]float32{
				"val": float32(i),
			},
		}
	}

	// Force a small batch allocation to test chunking & progressive release
	client.negoMu.Lock()
	client.negotiation.AllocatedBatchSize = 40
	client.negotiation.ReleaseIntervalMs = 5
	client.negotiation.Status = MemoryStatusThrottled
	client.negoMu.Unlock()

	start := time.Now()
	names, err := client.SaveBatch(items)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("SaveBatch failed under adaptive pacing: %v", err)
	}
	if len(names) != totalItems {
		t.Fatalf("expected %d items saved, got %d", totalItems, len(names))
	}

	// With 120 items split into chunks of 40 (3 chunks) and 5ms interval between chunks,
	// elapsed should be >= 10ms
	if elapsed < 8*time.Millisecond {
		t.Logf("paced save finished in %v (expected ~10ms delay between 3 chunks)", elapsed)
	}

	// Verify all items were actually saved in the DB
	for _, it := range items {
		_, _, err := client.Get(it.Name)
		if err != nil {
			t.Errorf("expected item %s to exist: %v", it.Name, err)
		}
	}
}

func TestWire_FullFeatureParity_EndToEnd(t *testing.T) {
	tmpDir := t.TempDir()
	backend := &mockBackend{
		dataDir:   tmpDir,
		models:    make(map[string]*storage.AnchorDB),
		authToken: "parity_secret",
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
		WithClientToken("parity_secret"),
		WithClientModel("logistics"),
		WithClientTimeout(5*time.Second),
	)
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer client.Close()

	// 1. Schema attachment and retrieval
	schemaJSON := `{"distance":"float","duration":"float"}`
	if err := client.AttachSchema(schemaJSON); err != nil {
		t.Fatalf("AttachSchema failed: %v", err)
	}
	gotSchema, err := client.GetSchema()
	if err != nil {
		t.Fatalf("GetSchema failed: %v", err)
	}
	if gotSchema == "" {
		t.Fatalf("expected non-empty schema")
	}

	// 2. SetRelation (directional spatial ray)
	downDir := spatial.Point2D{X: 0, Y: -1}
	if err := client.SetRelation("downlink", downDir); err != nil {
		t.Fatalf("SetRelation failed: %v", err)
	}

	// 3. Save records with geometry (AppendNode equivalent)
	truck1 := storage.BatchRecordItem{
		Name:      "truck_alpha",
		Operation: storage.OpStatusActive,
		Props: map[string]float32{
			"distance": 120.0,
			"duration": 2.0,
		},
		Coord: &spatial.Point2D{X: 0.0, Y: 1.0},
		Roles: &[4]float32{1.0, 0.0, 0.0, 0.0},
	}
	if _, err := client.Save(truck1); err != nil {
		t.Fatalf("Save truck1 failed: %v", err)
	}

	truck2 := storage.BatchRecordItem{
		Name:      "truck_beta",
		Operation: storage.OpStatusActive,
		Props: map[string]float32{
			"distance": 200.0,
			"duration": 4.0,
		},
		Coord: &spatial.Point2D{X: 0.0, Y: 0.0},
		Roles: &[4]float32{1.0, 0.0, 0.0, 0.0},
	}
	if _, err := client.Save(truck2); err != nil {
		t.Fatalf("Save truck2 failed: %v", err)
	}

	// 4. GetWithOptions with calculations and projections
	getReq := GetRequest{
		Name:   "truck_alpha",
		Select: []string{"distance", "duration", "speed"},
		Computations: map[string]storage.ComputeOp{
			"speed": storage.OpDivide{FieldA: "distance", FieldB: "duration"},
		},
	}
	props, _, err := client.GetWithOptions(getReq)
	if err != nil {
		t.Fatalf("GetWithOptions failed: %v", err)
	}
	if props["speed"] != 60.0 {
		t.Fatalf("expected speed 60, got %f", props["speed"])
	}

	// 5. List with filtering, computations, select, and paging
	listReq := ListRequest{
		Limit:  10,
		Offset: 0,
		Select: []string{"distance", "duration", "speed"},
		Filters: []storage.FilterCondition{
			{Field: "distance", Operator: ">=", Value: 100.0},
		},
		Computations: map[string]storage.ComputeOp{
			"speed": storage.OpDivide{FieldA: "distance", FieldB: "duration"},
		},
	}
	listResp, err := client.List(listReq)
	if err != nil {
		t.Fatalf("List query failed: %v", err)
	}
	if len(listResp.Items) != 2 {
		t.Fatalf("expected 2 items from List, got %d", len(listResp.Items))
	}

	// 6. Bulk Update with mutations
	upReq := UpdateRequest{
		Limit: 10,
		Filters: []storage.FilterCondition{
			{Field: "distance", Operator: "=", Value: 120.0},
		},
		Mutations: map[string]storage.ComputeOp{
			"distance": storage.OpAdd{FieldA: "distance", FieldB: "duration"},
		},
	}
	updatedCount, err := client.Update(upReq)
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}
	if updatedCount != 1 {
		t.Fatalf("expected 1 record updated, got %d", updatedCount)
	}
	// Verify updated value (120 + 2 = 122)
	updatedProps, _, _ := client.Get("truck_alpha")
	if updatedProps["distance"] != 122.0 {
		t.Fatalf("expected distance 122, got %f", updatedProps["distance"])
	}

	// 7. Math operations: distance, centroid, cosine similarity, nearest
	distResp, err := client.Math(MathRequest{
		Op:    "distance",
		Words: []string{"truck_alpha", "truck_beta"},
	})
	if err != nil {
		t.Fatalf("Math distance failed: %v", err)
	}
	if distResp.Value <= 0 {
		t.Fatalf("expected distance > 0, got %f", distResp.Value)
	}

	centroidResp, err := client.Math(MathRequest{
		Op:    "centroid",
		Words: []string{"truck_alpha", "truck_beta"},
	})
	if err != nil {
		t.Fatalf("Math centroid failed: %v", err)
	}
	if centroidResp.Coord.Y != 0.5 {
		t.Fatalf("expected centroid Y=0.5, got %f", centroidResp.Coord.Y)
	}

	simResp, err := client.Math(MathRequest{
		Op:    "cosine_similarity",
		Words: []string{"truck_alpha", "truck_beta"},
	})
	if err != nil {
		t.Fatalf("Math cosine similarity failed: %v", err)
	}
	if simResp.Value != 1.0 {
		t.Fatalf("expected cosine similarity 1.0, got %f", simResp.Value)
	}

	nearestResp, err := client.Math(MathRequest{
		Op:     "nearest",
		Words:  []string{"truck_alpha"},
		LimitK: 2,
	})
	if err != nil {
		t.Fatalf("Math nearest failed: %v", err)
	}
	if len(nearestResp.Neighbors) == 0 {
		t.Fatalf("expected nearest neighbors, got 0")
	}

	// 8. Graph Step along configured relation
	graphResp, err := client.Graph(GraphRequest{
		Inputs:    []string{"truck_alpha"},
		Relation:  "downlink",
		LimitK:    5,
		OrderMode: uint8(storage.OrderByDistanceAsc),
		BeamWidth: 2.0,
	})
	if err != nil {
		t.Fatalf("Graph beam step failed: %v", err)
	}
	if len(graphResp.Results) == 0 {
		t.Fatalf("expected graph step results along downlink relation")
	}

	// 9. ResetModel
	if err := client.ResetModel(true); err != nil {
		t.Fatalf("ResetModel failed: %v", err)
	}
	// After reset, schema should no longer exist
	if _, err := client.GetSchema(); err == nil {
		t.Fatalf("expected GetSchema to error after reset, got nil")
	}
}
