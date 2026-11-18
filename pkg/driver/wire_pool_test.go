package driver

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"testing"

	"github.com/rachmanzz/anchordb/pkg/storage"
	"github.com/rachmanzz/anchordb/pkg/wire"
)

type wireTestBackend struct {
	mu      sync.Mutex
	dataDir string
	models  map[string]*storage.AnchorDB
}

func (m *wireTestBackend) GetModel(name string) (*storage.AnchorDB, error) {
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

func (m *wireTestBackend) Authenticate(_ string) bool {
	return true
}

func (m *wireTestBackend) RequiresAuth() bool {
	return false
}

func (m *wireTestBackend) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, db := range m.models {
		_ = db.Close()
	}
}

func TestDriver_WireConnectionPool(t *testing.T) {
	tmpDir := t.TempDir()
	backend := &wireTestBackend{
		dataDir: tmpDir,
		models:  make(map[string]*storage.AnchorDB),
	}
	defer backend.Close()

	srv := wire.NewServer(backend)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer l.Close()

	go func() {
		_ = srv.Serve(l)
	}()
	defer func() {
		_ = srv.Close()
	}()

	addr := l.Addr().String()

	// 1. Connect with WithPoolSize(8)
	client, err := ConnectWire("wire://"+addr, WithPoolSize(8))
	if err != nil {
		t.Fatalf("ConnectWire failed: %v", err)
	}
	defer client.Close()

	// 2. Concurrently save 800 records across 8 worker goroutines in parallel
	const workers = 8
	const recordsPerWorker = 100
	var wg sync.WaitGroup
	var errCount sync.Mutex
	var errs []error

	for w := 0; w < workers; w++ {
		wg.Add(1)
		workerID := w
		go func() {
			defer wg.Done()
			for i := 0; i < recordsPerWorker; i++ {
				name := fmt.Sprintf("worker_%d_rec_%d", workerID, i)
				props := map[string]float32{
					"worker_id": float32(workerID),
					"seq":       float32(i),
					"val":       float32(i * 10),
				}
				if err := client.Save(name, props); err != nil {
					errCount.Lock()
					errs = append(errs, err)
					errCount.Unlock()
					return
				}
			}
		}()
	}
	wg.Wait()

	if len(errs) > 0 {
		t.Fatalf("encountered %d errors during parallel pool writes: %v", len(errs), errs[0])
	}

	// 3. Verify records were saved properly
	res, err := client.Get("worker_0_rec_50").Execute(context.Background())
	if err != nil {
		t.Fatalf("failed to get record: %v", err)
	}
	if res.Get("val") != 500 {
		t.Errorf("expected val = 500, got %f", res.Get("val"))
	}

	// 4. Test URL parameter ?pool=4
	client2, err := Connect("wire://" + addr + "?pool=4")
	if err != nil {
		t.Fatalf("Connect with ?pool=4 failed: %v", err)
	}
	defer client2.Close()

	if err := client2.Save("pool4_entity", map[string]float32{"x": 42}); err != nil {
		t.Fatalf("failed to save with pool=4 client: %v", err)
	}
}

func TestDriver_WirePoolDiscardsBrokenConnAndRetainsProtocolErrorConn(t *testing.T) {
	tmpDir := t.TempDir()
	backend := &wireTestBackend{
		dataDir: tmpDir,
		models:  make(map[string]*storage.AnchorDB),
	}
	defer backend.Close()

	srv := wire.NewServer(backend)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer l.Close()
	go func() { _ = srv.Serve(l) }()
	defer func() { _ = srv.Close() }()

	client, err := ConnectWire("wire://"+l.Addr().String(), WithPoolSize(1))
	if err != nil {
		t.Fatalf("ConnectWire: %v", err)
	}
	defer client.Close()

	wb := client.backend.(*wireBackend)

	// 1. Protocol error (WireError, e.g., missing record) keeps the healthy connection in pool.
	if _, err := client.Get("does_not_exist").Execute(context.Background()); err == nil {
		t.Fatalf("expected error on missing record")
	}
	if len(wb.idle) != 1 {
		t.Fatalf("expected protocol error to return healthy conn to idle pool, got len=%d", len(wb.idle))
	}

	// 2. Simulate transport error by closing the single underlying connection, then invoking a call.
	wb.mu.Lock()
	var onlyConn *wire.Client
	for wc := range wb.clients {
		onlyConn = wc
	}
	wb.mu.Unlock()
	if onlyConn == nil {
		t.Fatalf("expected 1 tracked wire client")
	}
	_ = onlyConn.Close()

	if err := client.Save("fails_on_closed", map[string]float32{"x": 1}); err == nil {
		t.Fatalf("expected Save on closed conn to fail")
	}

	// The broken connection must be discarded (not left in idle), and the next call must redial cleanly.
	if err := client.Save("recovers_after_redial", map[string]float32{"x": 99}); err != nil {
		t.Fatalf("expected pool to redial after discarding broken conn, got: %v", err)
	}
	res, err := client.Get("recovers_after_redial").Execute(context.Background())
	if err != nil {
		t.Fatalf("Get after redial: %v", err)
	}
	if res.Get("x") != 99 {
		t.Fatalf("expected x=99 after redial, got %v", res.Get("x"))
	}
}

type wirePartitionTestBackend struct {
	*wireTestBackend
}

func (m *wirePartitionTestBackend) GetPartitionCatalog(_ string) ([]wire.PartitionCatalogItemDTO, error) {
	return nil, nil
}

func (m *wirePartitionTestBackend) InspectPartition(req wire.PartitionInspectWireRequest) (wire.PartitionInspectWireResponse, error) {
	all := []wire.PartitionRecordWireItem{
		{Name: "r1"},
		{Name: "r2"},
		{Name: "r3"},
	}
	hasMore := false
	if req.Limit > 0 && req.Limit < len(all) {
		hasMore = true
		all = all[:req.Limit]
	}
	return wire.PartitionInspectWireResponse{
		PartitionFile: req.PartitionFile,
		Records:       all,
		HasMore:       hasMore,
	}, nil
}

func TestWireBackend_InspectPartitionHasMore(t *testing.T) {
	backend := &wirePartitionTestBackend{
		wireTestBackend: &wireTestBackend{
			dataDir: t.TempDir(),
			models:  make(map[string]*storage.AnchorDB),
		},
	}
	defer backend.Close()

	srv := wire.NewServer(backend)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = l.Close() }()
	go func() {
		_ = srv.Serve(l)
	}()

	client, err := ConnectWire(fmt.Sprintf("wire://%s", l.Addr().String()))
	if err != nil {
		t.Fatalf("ConnectWire: %v", err)
	}
	defer func() { _ = client.Close() }()

	res, err := client.InspectPartition(context.Background(), "part1.anchordb", "", 2)
	if err != nil {
		t.Fatalf("InspectPartition: %v", err)
	}
	if len(res.Records) != 2 || !res.HasMore {
		t.Fatalf("expected len=2 and HasMore=true over wire, got len=%d HasMore=%v", len(res.Records), res.HasMore)
	}

	resAll, err := client.InspectPartition(context.Background(), "part1.anchordb", "", 5)
	if err != nil {
		t.Fatalf("InspectPartition all: %v", err)
	}
	if len(resAll.Records) != 3 || resAll.HasMore {
		t.Fatalf("expected len=3 and HasMore=false over wire, got len=%d HasMore=%v", len(resAll.Records), resAll.HasMore)
	}
}

func TestConnectWire_RejectsInvalidQueryParams(t *testing.T) {
	for _, dsn := range []string{
		"wire://127.0.0.1:7448?target=notanumber",
		"wire://127.0.0.1:7448?pool=0",
		"wire://127.0.0.1:7448?pool=abc",
		"wire://127.0.0.1:7448?pacing=maybe",
		"wire://127.0.0.1:7448?unknown=1",
		"wire://?pool=1",
	} {
		if _, err := ConnectWire(dsn); err == nil {
			t.Fatalf("expected ConnectWire(%q) to fail on invalid DSN", dsn)
		}
	}
	if _, err := ConnectWire("wire://127.0.0.1:7448", WithPoolSize(0)); err == nil {
		t.Fatal("expected ConnectWire with WithPoolSize(0) to return config error")
	}
}
