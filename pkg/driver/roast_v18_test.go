package driver

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rachmanzz/anchordb/pkg/storage"
	"github.com/rachmanzz/anchordb/pkg/wire"
)

func TestRoastV18_BackendModelIsolationAndSuccessFalseAndWireModelPartitions(t *testing.T) {
	// 1. localBackend rejects foreign Model() calls (including ListPartitions/InspectPartition).
	localPath := filepath.Join(t.TempDir(), "orders.anchordb")
	localClient, err := OpenLocal(localPath)
	if err != nil {
		t.Fatalf("OpenLocal: %v", err)
	}
	defer localClient.Close()

	if err := localClient.Model("users").Save("u1", map[string]float32{"v": 1}); err == nil {
		t.Fatal("expected localBackend to reject foreign Model(\"users\")")
	}
	if _, err := localClient.Model("users").ListPartitions(context.Background()); err == nil {
		t.Fatal("expected localBackend.ListPartitions to reject foreign Model(\"users\")")
	}
	if _, err := localClient.Model("users").InspectPartition(context.Background(), "p.anchordb", "", 1); err == nil {
		t.Fatal("expected localBackend.InspectPartition to reject foreign Model(\"users\")")
	}

	// 2. embeddedBackend rejects path-traversal model names.
	embClient, err := OpenEmbedded(t.TempDir())
	if err != nil {
		t.Fatalf("OpenEmbedded: %v", err)
	}
	defer embClient.Close()

	if err := embClient.Model("tenantA/users").Save("u1", map[string]float32{"v": 1}); err == nil {
		t.Fatal("expected embeddedBackend to reject path separators in model name")
	}

	// 3. remoteBackend returns an error on HTTP 200 {"success": false}.
	httpSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"success": false, "error": "logical failure"}`))
	}))
	defer httpSrv.Close()

	remClient, err := Connect(httpSrv.URL)
	if err != nil {
		t.Fatalf("Connect remote: %v", err)
	}
	defer remClient.Close()
	if err := remClient.Save("k1", map[string]float32{"v": 1}); err == nil || !strings.Contains(err.Error(), "logical failure") {
		t.Fatalf("expected HTTP 200 success=false to return error, got %v", err)
	}

	// 4. wireBackend passes active Client.Model(...) to ListPartitions.
	backend := &wireCatalogModelBackend{
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
	defer l.Close()
	go func() { _ = srv.Serve(l) }()
	defer srv.Close()

	wClient, err := ConnectWire("wire://"+l.Addr().String(), WithModel("default"))
	if err != nil {
		t.Fatalf("ConnectWire: %v", err)
	}
	defer wClient.Close()

	parts, err := wClient.Model("tenant_b").ListPartitions(context.Background())
	if err != nil {
		t.Fatalf("ListPartitions: %v", err)
	}
	if len(parts) != 1 || parts[0].Name != "tenant_b.anchordb" {
		t.Fatalf("expected tenant_b partition catalog, got %+v", parts)
	}
}

type wireCatalogModelBackend struct {
	*wireTestBackend
}

func (m *wireCatalogModelBackend) GetPartitionCatalog(model string) ([]wire.PartitionCatalogItemDTO, error) {
	return []wire.PartitionCatalogItemDTO{
		{PartitionFile: model + ".anchordb"},
	}, nil
}

func (m *wireCatalogModelBackend) InspectPartition(_ wire.PartitionInspectWireRequest) (wire.PartitionInspectWireResponse, error) {
	return wire.PartitionInspectWireResponse{}, nil
}
