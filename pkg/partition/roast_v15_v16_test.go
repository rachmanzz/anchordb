package partition

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rachmanzz/anchordb/pkg/storage"
)

func TestRoastV15_CatalogMultiModelIsolationAndNamedTombstones(t *testing.T) {
	dir := t.TempDir()
	cat, err := OpenCatalog(filepath.Join(dir, "catalog.json"))
	if err != nil {
		t.Fatalf("OpenCatalog: %v", err)
	}
	sink := NewLocalDiskSink(filepath.Join(dir, "partitions"))

	makeModelDB := func(name, recName string) *storage.AnchorDB {
		db, err := storage.OpenWithOptions(filepath.Join(dir, name+".anchordb"), storage.Options{
			WAL: storage.WALOptions{SyncMode: storage.SyncNone},
		})
		if err != nil {
			t.Fatalf("Open(%s): %v", name, err)
		}
		// Pre-existing tombstone that must NOT steal ExecutePartition's CollectNamedTombstones budget.
		if _, err := db.SaveRecordWithIDAndOp("old_tomb", storage.OpStatusTombstone, map[string]float32{"date": 1}); err != nil {
			t.Fatalf("SaveRecordWithIDAndOp(old_tomb): %v", err)
		}
		if err := db.SaveRecord(recName, map[string]float32{"date": 100}); err != nil {
			t.Fatalf("SaveRecord(%s): %v", recName, err)
		}
		return db
	}

	dbA := makeModelDB("modelA", "recA")
	defer dbA.Close()
	dbB := makeModelDB("modelB", "recB")
	defer dbB.Close()

	resA, err := ExecutePartition(dbA, PartitionPolicy{
		ModelName:     "modelA",
		DateField:     "date",
		RetentionDays: 1,
		Sink:          sink,
		FileFormat:    "{date}.anchordb",
		PruneHotDB:    true,
		Catalog:       cat,
	})
	if err != nil {
		t.Fatalf("ExecutePartition(modelA): %v", err)
	}
	resB, err := ExecutePartition(dbB, PartitionPolicy{
		ModelName:     "modelB",
		DateField:     "date",
		RetentionDays: 1,
		Sink:          sink,
		FileFormat:    "{date}.anchordb",
		PruneHotDB:    true,
		Catalog:       cat,
	})
	if err != nil {
		t.Fatalf("ExecutePartition(modelB): %v", err)
	}
	if resA.PartitionFile == resB.PartitionFile {
		t.Fatalf("expected distinct disambiguated PartitionFile names on collision, both got %q", resA.PartitionFile)
	}

	if _, ok := cat.FindRecord("modelA", "recA"); !ok {
		t.Fatal("expected modelA/recA in catalog after modelB registration")
	}
	if _, ok := cat.FindRecord("modelB", "recB"); !ok {
		t.Fatal("expected modelB/recB in catalog")
	}
	// Newly archived recA must be swept from hot DB despite pre-existing old_tomb.
	if _, err := dbA.GetRecordTyped("recA"); err == nil {
		t.Fatal("expected recA to be pruned from hot dbA via CollectNamedTombstones")
	}
}

func TestRoastV16_S3RedirectBlockedAndTraversalRejectedAndPrivateDialBlocked(t *testing.T) {
	// 1. Redirect 302 -> 127.0.0.1 must be rejected.
	redirSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "http://127.0.0.1:9999/secret", http.StatusFound)
	}))
	defer redirSrv.Close()

	sink := NewS3Sink(redirSrv.URL, "bucket", "pfx", "", "")
	if err := sink.FetchPartition(context.Background(), "p.anchordb", filepath.Join(t.TempDir(), "out.anchordb")); err == nil || !strings.Contains(err.Error(), "redirects are disallowed") {
		t.Fatalf("expected redirect error, got %v", err)
	}

	// 2. Path traversal in FetchPartition must be rejected.
	if err := sink.FetchPartition(context.Background(), "../../other-bucket/secret", filepath.Join(t.TempDir(), "out2.anchordb")); err == nil {
		t.Fatal("expected FetchPartition to reject '..' traversal")
	}

	// 3. Restricted HTTP client must block loopback and CGNAT 100.64.0.1 connections.
	restricted := NewS3HTTPClient(false)
	if _, err := restricted.Get(redirSrv.URL); err == nil {
		t.Fatal("expected NewS3HTTPClient(false) to refuse loopback connection")
	}
	if !IsBlockedS3IP(net.ParseIP("100.64.0.1")) || !IsBlockedS3IP(net.ParseIP("224.0.0.1")) {
		t.Fatal("expected CGNAT and multicast IPs to be blocked")
	}
}
