package partition

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rachmanzz/anchordb/pkg/storage"
)

func buildTestPartitionBytes(t *testing.T, recName string, val float64) []byte {
	t.Helper()
	p := filepath.Join(t.TempDir(), "src.anchordb")
	db, err := storage.OpenWithOptions(p, storage.Options{WAL: storage.WALOptions{SyncMode: storage.SyncNone}})
	if err != nil {
		t.Fatalf("Open partition: %v", err)
	}
	if _, err := db.SaveRecordTyped(recName, []storage.AttrEntry{storage.NewFloat64Attr("val", val)}); err != nil {
		_ = db.Close()
		t.Fatalf("SaveRecordTyped: %v", err)
	}
	if err := db.Sync(); err != nil {
		_ = db.Close()
		t.Fatalf("Sync: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	data, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	return data
}

func TestS3PartitionCache_SingleflightConcurrentHandlesAndEviction(t *testing.T) {
	partA := buildTestPartitionBytes(t, "rec_a", 123.4567890123)
	partB := buildTestPartitionBytes(t, "rec_b", 987.6543210987)

	var getsA atomic.Int32
	var getsB atomic.Int32
	gate := make(chan struct{})

	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/bucket/prefix_a/part.anchordb":
			getsA.Add(1)
			<-gate
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(partA)
		case "/bucket/prefix_b/part.anchordb":
			getsB.Add(1)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(partB)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	cacheDir := filepath.Join(t.TempDir(), "s3cache")
	// Size cap allows only one of the two partition files at a time.
	maxBytes := int64(len(partA) + len(partB) - 1)
	if maxBytes < int64(len(partA)) {
		maxBytes = int64(len(partA))
	}
	cache, err := NewS3PartitionCacheWithOptions(cacheDir, S3CacheOptions{
		MaxBytes:       maxBytes,
		TTL:            200 * time.Millisecond,
		MaxOpenHandles: 4,
	})
	if err != nil {
		t.Fatalf("NewS3PartitionCacheWithOptions: %v", err)
	}
	defer cache.Close()

	sinkA := &S3Sink{Endpoint: srv.URL, Bucket: "bucket", Prefix: "prefix_a", AccessKey: "ak", SecretKey: "sk", HTTPClient: srv.Client()}
	sinkB := &S3Sink{Endpoint: srv.URL, Bucket: "bucket", Prefix: "prefix_b", AccessKey: "ak", SecretKey: "sk", HTTPClient: srv.Client()}

	// 1. Concurrent InspectPartitionTyped on the same key must coalesce S3 download (singleflight)
	// and never fail with flock contention.
	const readers = 6
	var wg sync.WaitGroup
	errs := make(chan error, readers)
	go func() {
		time.Sleep(30 * time.Millisecond)
		close(gate)
	}()

	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, attrs, err := cache.InspectPartitionTyped(context.Background(), sinkA, "part.anchordb", "rec_a", 10)
			if err != nil {
				errs <- err
				return
			}
			if len(attrs) != 1 || attrs[0].Float64() != 123.4567890123 {
				errs <- fmt.Errorf("unexpected attrs: %+v", attrs)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent InspectPartitionTyped error: %v", err)
	}
	if getsA.Load() != 1 {
		t.Fatalf("expected singleflight to trigger exactly 1 S3 GET for partA, got %d", getsA.Load())
	}

	// 2. Concurrent GetRecordTyped also reuses handle without flock errors.
	cr, err := cache.GetRecordTyped(context.Background(), sinkA, "part.anchordb", "rec_a", nil, nil)
	if err != nil {
		t.Fatalf("GetRecordTyped: %v", err)
	}
	if v, ok := cr.GetFloat64("val"); !ok || v != 123.4567890123 {
		t.Fatalf("unexpected GetRecordTyped value: %v (%v)", v, ok)
	}

	// 3. Different prefix with same basename ("part.anchordb") does not collide and triggers LRU eviction of partA.
	_, _, attrsB, err := cache.InspectPartitionTyped(context.Background(), sinkB, "part.anchordb", "rec_b", 10)
	if err != nil {
		t.Fatalf("InspectPartitionTyped sinkB: %v", err)
	}
	if len(attrsB) != 1 || attrsB[0].Float64() != 987.6543210987 {
		t.Fatalf("unexpected attrsB (prefix collision?): %+v", attrsB)
	}
	if getsB.Load() != 1 {
		t.Fatalf("expected 1 S3 GET for partB, got %d", getsB.Load())
	}
	cache.mu.Lock()
	totalAfterB := cache.totalBytes
	entryCount := len(cache.entries)
	cache.mu.Unlock()
	if totalAfterB > maxBytes || entryCount != 1 {
		t.Fatalf("expected LRU eviction to keep totalBytes <= %d and 1 entry, got total=%d entries=%d", maxBytes, totalAfterB, entryCount)
	}

	// 4. TTL expiration re-fetches partB after 200ms.
	time.Sleep(220 * time.Millisecond)
	if _, _, _, err := cache.InspectPartitionTyped(context.Background(), sinkB, "part.anchordb", "rec_b", 10); err != nil {
		t.Fatalf("InspectPartitionTyped after TTL: %v", err)
	}
	if getsB.Load() != 2 {
		t.Fatalf("expected TTL expiry to trigger 2nd S3 GET for partB, got %d", getsB.Load())
	}
}

func TestS3Sink_ListPartitionsPagination(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := r.URL.Query().Get("continuation-token")
		w.Header().Set("Content-Type", "application/xml")
		if token == "" {
			_, _ = w.Write([]byte(`<ListBucketResult>
				<IsTruncated>true</IsTruncated>
				<NextContinuationToken>page2</NextContinuationToken>
				<Contents><Key>p/part1.anchordb</Key><Size>100</Size></Contents>
				<Contents><Key>p/ignore.txt</Key><Size>10</Size></Contents>
			</ListBucketResult>`))
			return
		}
		if token == "page2" {
			_, _ = w.Write([]byte(`<ListBucketResult>
				<IsTruncated>false</IsTruncated>
				<Contents><Key>p/part2.anchordb</Key><Size>200</Size></Contents>
			</ListBucketResult>`))
			return
		}
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()

	sink := &S3Sink{
		Endpoint:   srv.URL,
		Bucket:     "bucket",
		Prefix:     "p",
		HTTPClient: srv.Client(),
	}
	objs, err := sink.ListPartitions(context.Background())
	if err != nil {
		t.Fatalf("ListPartitions failed: %v", err)
	}
	if len(objs) != 2 || objs[0].Key != "p/part1.anchordb" || objs[1].Key != "p/part2.anchordb" {
		t.Fatalf("expected 2 paginated .anchordb objects, got %+v", objs)
	}
}

func TestS3PartitionCache_CancelOneWaiterAndActiveTTLRefresh(t *testing.T) {
	partV1 := buildTestPartitionBytes(t, "rec_v", 1.0)
	partV2 := buildTestPartitionBytes(t, "rec_v", 2.0)

	var gets atomic.Int32
	gate := make(chan struct{})
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, ".schema") {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		n := gets.Add(1)
		if n == 1 {
			<-gate
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(partV1)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(partV2)
	}))
	defer srv.Close()

	cache, err := NewS3PartitionCacheWithOptions(t.TempDir(), S3CacheOptions{
		MaxBytes:       1 << 20,
		TTL:            40 * time.Millisecond,
		MaxOpenHandles: 4,
	})
	if err != nil {
		t.Fatalf("NewS3PartitionCacheWithOptions: %v", err)
	}
	defer func() { _ = cache.Close() }()

	sink := &S3Sink{Endpoint: srv.URL, Bucket: "b", Prefix: "p", AccessKey: "a", SecretKey: "s", HTTPClient: srv.Client()}

	ctx1, cancel1 := context.WithCancel(context.Background())
	errCh1 := make(chan error, 1)
	go func() {
		_, err := cache.EnsurePartition(ctx1, sink, "item.anchordb")
		errCh1 <- err
	}()
	time.Sleep(15 * time.Millisecond)

	errCh2 := make(chan error, 1)
	go func() {
		_, err := cache.EnsurePartition(context.Background(), sink, "item.anchordb")
		errCh2 <- err
	}()
	time.Sleep(15 * time.Millisecond)

	cancel1()
	if err := <-errCh1; err == nil {
		t.Fatal("expected canceled waiter 1 to fail promptly")
	}

	close(gate)
	if err := <-errCh2; err != nil {
		t.Fatalf("waiter 2 must not be poisoned by leader cancel: %v", err)
	}

	// Hold an active handle across TTL expiry and run concurrent refresh without flock contention.
	entry1, h1, err := cache.acquireHandle(context.Background(), sink, "item.anchordb")
	if err != nil {
		t.Fatalf("acquireHandle v1: %v", err)
	}
	time.Sleep(60 * time.Millisecond)

	cr2, err := cache.GetRecordTyped(context.Background(), sink, "item.anchordb", "rec_v", nil, nil)
	if err != nil {
		cache.releaseHandle(entry1, h1)
		t.Fatalf("GetRecordTyped across active TTL refresh failed: %v", err)
	}
	if v, ok := cr2.GetFloat64("val"); !ok || v != 2.0 {
		cache.releaseHandle(entry1, h1)
		t.Fatalf("expected refreshed v2 value 2.0, got %v (%v)", v, ok)
	}
	cache.releaseHandle(entry1, h1)
}

func TestCatalog_RegisterPartitionFailureRollsBackMemory(t *testing.T) {
	dir := t.TempDir()
	cat, err := OpenCatalog(filepath.Join(dir, "catalog.json"))
	if err != nil {
		t.Fatalf("OpenCatalog: %v", err)
	}
	cat.mu.Lock()
	cat.filePath = filepath.Join(dir, "nonexistent", "catalog.json")
	cat.mu.Unlock()

	if err := cat.RegisterPartition(&CatalogEntry{
		ID:            "p1",
		Model:         "m1",
		PartitionFile: "p1.anchordb",
		RecordNames:   []string{"r1"},
	}); err == nil {
		t.Fatal("expected RegisterPartition failure on missing parent dir")
	}
	if entries := cat.ListEntries(""); len(entries) != 0 {
		t.Fatalf("expected catalog memory rolled back on save error, got %d entries", len(entries))
	}
}

func TestS3Sink_ListPartitionsTruncatedWithoutTokenErrors(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(`<ListBucketResult><IsTruncated>true</IsTruncated><Contents><Key>p/part1.anchordb</Key><Size>100</Size></Contents></ListBucketResult>`))
	}))
	defer srv.Close()

	sink := &S3Sink{Endpoint: srv.URL, Bucket: "b", Prefix: "p", HTTPClient: srv.Client()}
	if _, err := sink.ListPartitions(context.Background()); err == nil {
		t.Fatal("expected ListPartitions to error when IsTruncated=true without NextContinuationToken")
	}
}

func TestS3Sink_RejectsPartialCredentials(t *testing.T) {
	sink := NewS3Sink("https://example.invalid", "bucket", "p", "", "secret-only")
	if _, err := sink.ListPartitions(context.Background()); err == nil {
		t.Fatal("expected S3Sink with SecretKey and empty AccessKey to error")
	}
	if _, err := sink.SavePartition("part1.anchordb", []byte("data")); err == nil {
		t.Fatal("expected SavePartition with partial credentials to error")
	}
}

func TestExecutePartition_PreservesSchemaAndHasMore(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "main.anchordb")
	db, err := storage.OpenWithOptions(dbPath, storage.Options{WAL: storage.WALOptions{SyncMode: storage.SyncNone}})
	if err != nil {
		t.Fatalf("OpenWithOptions: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.AttachSchema(storage.ModelSchema{"created": "int", "v": "int"}); err != nil {
		t.Fatalf("AttachSchema: %v", err)
	}
	// Epoch day 1000 is far older than 30-day retention.
	for i := 0; i < 3; i++ {
		if _, err := db.SaveRecordTyped(fmt.Sprintf("old-%d", i), []storage.AttrEntry{
			storage.NewInt64Attr("created", 1000),
			storage.NewInt64Attr("v", int64(i)),
		}); err != nil {
			t.Fatalf("SaveRecordTyped: %v", err)
		}
	}

	sinkDir := filepath.Join(dir, "partitions")
	res, err := ExecutePartition(db, PartitionPolicy{
		ModelName:     "m",
		RetentionDays: 30,
		DateField:     "created",
		Sink:          NewLocalDiskSink(sinkDir),
	})
	if err != nil {
		t.Fatalf("ExecutePartition: %v", err)
	}
	if res.ArchivedCount != 3 {
		t.Fatalf("expected ArchivedCount=3, got %d", res.ArchivedCount)
	}

	partDB, err := storage.Open(res.TargetLocation)
	if err != nil {
		t.Fatalf("open archived partition: %v", err)
	}
	defer func() { _ = partDB.Close() }()

	gotSchema, hasSchema := partDB.GetSchema()
	if !hasSchema || gotSchema["v"] != "int" || gotSchema["created"] != "int" {
		t.Fatalf("expected archived partition to preserve schema, got hasSchema=%v schema=%v", hasSchema, gotSchema)
	}
	recs, hasMore, err := partDB.ListRecordsWithPaging(storage.QueryOptions{Limit: 2})
	if err != nil || len(recs) != 2 || !hasMore {
		t.Fatalf("expected len=2 and hasMore=true, got len=%d hasMore=%v err=%v", len(recs), hasMore, err)
	}
}

func TestS3Sink_AndCache_SchemaSidecarOrderFetchAndEviction(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "src_schema.anchordb")
	db, err := storage.OpenWithOptions(dbPath, storage.Options{WAL: storage.WALOptions{SyncMode: storage.SyncNone}})
	if err != nil {
		t.Fatalf("OpenWithOptions: %v", err)
	}
	if err := db.AttachSchema(storage.ModelSchema{"k": "int", "score": "float"}); err != nil {
		_ = db.Close()
		t.Fatalf("AttachSchema: %v", err)
	}
	if _, err := db.SaveRecordTyped("r1", []storage.AttrEntry{
		storage.NewInt64Attr("k", 42),
		storage.NewFloat64Attr("score", 9.5),
	}); err != nil {
		_ = db.Close()
		t.Fatalf("SaveRecordTyped: %v", err)
	}
	if err := db.Sync(); err != nil {
		_ = db.Close()
		t.Fatalf("Sync: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	var mu sync.Mutex
	objects := make(map[string][]byte)
	var putOrder []string
	var failSchemaGet atomic.Bool

	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.URL.Path, "/b/")
		switch r.Method {
		case http.MethodPut:
			body, _ := io.ReadAll(r.Body)
			mu.Lock()
			objects[key] = body
			putOrder = append(putOrder, key)
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			if strings.HasSuffix(key, ".schema") && failSchemaGet.Load() {
				http.Error(w, "upstream error", http.StatusInternalServerError)
				return
			}
			mu.Lock()
			data, ok := objects[key]
			mu.Unlock()
			if !ok {
				http.Error(w, "NotFound", http.StatusNotFound)
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(data)
		default:
			http.Error(w, "unsupported", http.StatusMethodNotAllowed)
		}
	}))
	defer srv.Close()

	sink := &S3Sink{Endpoint: srv.URL, Bucket: "b", Prefix: "pfx", HTTPClient: srv.Client()}
	if _, err := sink.SavePartitionFromFile("p_schema.anchordb", dbPath); err != nil {
		t.Fatalf("SavePartitionFromFile: %v", err)
	}
	if len(putOrder) != 2 || putOrder[0] != "pfx/p_schema.anchordb" || putOrder[1] != "pfx/p_schema.anchordb.schema" {
		t.Fatalf("expected main archive uploaded before .schema sidecar, got putOrder=%v", putOrder)
	}

	// Main upload failure must not upload an orphaned .schema object.
	var orphanPuts []string
	failMainSrv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.URL.Path, "/b/")
		orphanPuts = append(orphanPuts, key)
		http.Error(w, "fail main", http.StatusInternalServerError)
	}))
	defer failMainSrv.Close()
	failSink := &S3Sink{Endpoint: failMainSrv.URL, Bucket: "b", Prefix: "pfx", HTTPClient: failMainSrv.Client()}
	if _, err := failSink.SavePartitionFromFile("fail_main.anchordb", dbPath); err == nil {
		t.Fatal("expected SavePartitionFromFile to fail when main upload returns 500")
	}
	if len(orphanPuts) != 1 || orphanPuts[0] != "pfx/fail_main.anchordb" {
		t.Fatalf("expected only main archive PUT attempted on failure, got %v", orphanPuts)
	}

	// Transient sidecar GET failure must fail EnsurePartition and roll back the local archive file.
	cacheDir := filepath.Join(dir, "cache")
	cache, err := NewS3PartitionCacheWithOptions(cacheDir, S3CacheOptions{MaxBytes: 1024 * 1024})
	if err != nil {
		t.Fatalf("NewS3PartitionCacheWithOptions: %v", err)
	}
	defer func() { _ = cache.Close() }()

	failSchemaGet.Store(true)
	if _, err := cache.EnsurePartition(context.Background(), sink, "p_schema.anchordb"); err == nil {
		t.Fatal("expected EnsurePartition error on 500 .schema sidecar fetch")
	}
	failSchemaGet.Store(false)

	cachedPath, err := cache.EnsurePartition(context.Background(), sink, "p_schema.anchordb")
	if err != nil {
		t.Fatalf("EnsurePartition: %v", err)
	}
	opened, err := storage.OpenWithOptions(cachedPath, storage.Options{WAL: storage.WALOptions{SyncMode: storage.SyncNone}})
	if err != nil {
		t.Fatalf("Open cached partition: %v", err)
	}
	gotSchema, hasSchema := opened.GetSchema()
	_ = opened.Close()
	if !hasSchema || gotSchema["k"] != "int" || gotSchema["score"] != "float" {
		t.Fatalf("expected cached S3 partition to preserve schema, got hasSchema=%v schema=%v", hasSchema, gotSchema)
	}

	// Legacy partition without .schema (404) must succeed, and evicting p_schema must remove its .schema file.
	legacyBytes := buildTestPartitionBytes(t, "legacy_rec", 3.14)
	mu.Lock()
	objects["pfx/p_legacy.anchordb"] = legacyBytes
	mu.Unlock()
	if _, err := cache.EnsurePartition(context.Background(), sink, "p_legacy.anchordb"); err != nil {
		t.Fatalf("EnsurePartition legacy 404 .schema: %v", err)
	}
	cache.mu.Lock()
	cache.maxBytes = 1
	cache.mu.Unlock()
	if _, err := cache.EnsurePartition(context.Background(), sink, "p_legacy.anchordb"); err != nil {
		t.Fatalf("EnsurePartition trigger eviction: %v", err)
	}
	// Eviction removes p_schema files once EnsurePartition runs for a new key.
	mu.Lock()
	objects["pfx/p_trigger.anchordb"] = legacyBytes
	mu.Unlock()
	if _, err := cache.EnsurePartition(context.Background(), sink, "p_trigger.anchordb"); err != nil {
		t.Fatalf("EnsurePartition p_trigger: %v", err)
	}
	if _, statErr := os.Stat(cachedPath + ".schema"); !os.IsNotExist(statErr) {
		t.Fatalf("expected evicted .schema sidecar %s.schema removed, statErr=%v", cachedPath, statErr)
	}
}
