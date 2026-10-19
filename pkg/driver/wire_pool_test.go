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
