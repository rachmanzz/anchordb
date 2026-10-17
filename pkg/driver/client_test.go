package driver

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rachmanzz/anchordb/pkg/storage"
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

func TestDriver_OpenEmbeddedMultiModel(t *testing.T) {
	dataDir := t.TempDir()
	ctx := context.Background()

	client, err := OpenEmbedded(dataDir, WithModel("sensors"), WithSyncInterval(50*time.Millisecond))
	if err != nil {
		t.Fatalf("OpenEmbedded failed: %v", err)
	}
	defer client.Close()

	if client.IsRemote() || !client.IsLocal() {
		t.Errorf("expected embedded client to report IsLocal=true, IsRemote=false")
	}

	// 1. Save and Get on default model ("sensors")
	err = client.Save("temp_sensor_1", map[string]float32{"celsius": 24.5})
	if err != nil {
		t.Fatalf("Save on sensors failed: %v", err)
	}
	rec, err := client.Get("temp_sensor_1").Execute(ctx)
	if err != nil {
		t.Fatalf("Get on sensors failed: %v", err)
	}
	if rec.Get("celsius") != 24.5 {
		t.Errorf("expected celsius=24.5, got %f", rec.Get("celsius"))
	}

	// 2. Multi-Model routing: switch to "telemetry"
	teleClient := client.Model("telemetry")
	if teleClient.ModelName() != "telemetry" {
		t.Errorf("expected model name 'telemetry', got %q", teleClient.ModelName())
	}
	err = teleClient.Save("packet_100", map[string]float32{"bytes": 1024})
	if err != nil {
		t.Fatalf("Save on telemetry failed: %v", err)
	}
	recTele, err := teleClient.Get("packet_100").Execute(ctx)
	if err != nil {
		t.Fatalf("Get on telemetry failed: %v", err)
	}
	if recTele.Get("bytes") != 1024 {
		t.Errorf("expected bytes=1024, got %f", recTele.Get("bytes"))
	}

	// 3. Verify physical file isolation: sensors.anchordb and telemetry.anchordb
	sensorsFile := filepath.Join(dataDir, "sensors.anchordb")
	telemetryFile := filepath.Join(dataDir, "telemetry.anchordb")

	// Ensure files exist on disk and close client to release flock
	if err := client.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	pdbSensors, err := storage.Open(sensorsFile)
	if err != nil {
		t.Fatalf("Failed to open physical sensors.anchordb: %v", err)
	}
	defer pdbSensors.Close()

	// packet_100 must NOT exist in sensors model
	if _, err := pdbSensors.GetRecord("packet_100"); err == nil {
		t.Errorf("Cross-model contamination! packet_100 found in sensors.anchordb")
	}

	pdbTele, err := storage.Open(telemetryFile)
	if err != nil {
		t.Fatalf("Failed to open physical telemetry.anchordb: %v", err)
	}
	defer pdbTele.Close()
	// temp_sensor_1 must NOT exist in telemetry model
	if _, err := pdbTele.GetRecord("temp_sensor_1"); err == nil {
		t.Errorf("Cross-model contamination! temp_sensor_1 found in telemetry.anchordb")
	}
}

func TestDriver_PartitionInspection(t *testing.T) {
	dataDir := t.TempDir()
	partDir := filepath.Join(dataDir, "partitions")
	if err := os.MkdirAll(partDir, 0755); err != nil {
		t.Fatalf("failed to create partition dir: %v", err)
	}

	// Create a dummy archived partition file
	partFilePath := filepath.Join(partDir, "sensors_2026_05.anchordb")
	pdb, err := storage.Open(partFilePath)
	if err != nil {
		t.Fatalf("failed to create partition archive: %v", err)
	}
	_, _ = pdb.SaveRecordWithIDAndOp("archive_sensor_1", storage.OpStatusActive, map[string]float32{"humidity": 45.0})
	_, _ = pdb.SaveRecordWithIDAndOp("archive_sensor_2", storage.OpStatusActive, map[string]float32{"humidity": 55.0})
	_ = pdb.Sync()
	_ = pdb.Close()

	ctx := context.Background()
	client, err := OpenEmbedded(dataDir)
	if err != nil {
		t.Fatalf("OpenEmbedded failed: %v", err)
	}
	defer client.Close()

	// 1. ListPartitions
	parts, err := client.ListPartitions(ctx)
	if err != nil {
		t.Fatalf("ListPartitions failed: %v", err)
	}
	if len(parts) != 1 || parts[0].Name != "sensors_2026_05.anchordb" {
		t.Fatalf("expected 1 partition 'sensors_2026_05.anchordb', got: %+v", parts)
	}

	// 2. InspectPartition single record
	res, err := client.InspectPartition(ctx, "sensors_2026_05.anchordb", "archive_sensor_1", 0)
	if err != nil {
		t.Fatalf("InspectPartition single record failed: %v", err)
	}
	if res.Props["humidity"] != 45.0 {
		t.Errorf("expected humidity=45.0, got %f", res.Props["humidity"])
	}

	// 3. InspectPartitionRecord helper
	rec, err := client.InspectPartitionRecord(ctx, "sensors_2026_05.anchordb", "archive_sensor_2")
	if err != nil {
		t.Fatalf("InspectPartitionRecord failed: %v", err)
	}
	if rec.Get("humidity") != 55.0 {
		t.Errorf("expected humidity=55.0, got %f", rec.Get("humidity"))
	}

	// 4. InspectPartition listing records
	listRes, err := client.InspectPartition(ctx, "sensors_2026_05.anchordb", "", 10)
	if err != nil {
		t.Fatalf("InspectPartition list failed: %v", err)
	}
	if len(listRes.Records) != 2 {
		t.Errorf("expected 2 records in partition, got %d", len(listRes.Records))
	}
}

func TestDriver_RemoteTransport_Tuning(t *testing.T) {
	client, err := ConnectRemote("http://127.0.0.1:7447")
	if err != nil {
		t.Fatalf("ConnectRemote failed: %v", err)
	}
	defer client.Close()

	rem, ok := client.backend.(*remoteBackend)
	if !ok {
		t.Fatalf("expected remoteBackend, got %T", client.backend)
	}
	tr, ok := rem.httpClient.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("expected *http.Transport, got %T", rem.httpClient.Transport)
	}
	if tr.MaxIdleConnsPerHost != 256 {
		t.Errorf("expected MaxIdleConnsPerHost=256, got %d", tr.MaxIdleConnsPerHost)
	}
	if tr.MaxIdleConns != 1024 {
		t.Errorf("expected MaxIdleConns=1024, got %d", tr.MaxIdleConns)
	}
	if tr.IdleConnTimeout != 90*time.Second {
		t.Errorf("expected IdleConnTimeout=90s, got %v", tr.IdleConnTimeout)
	}
}

func TestDriver_SaveBatch_Embedded(t *testing.T) {
	tmpDir := t.TempDir()
	client, err := OpenEmbedded(tmpDir, WithModel("batch_embedded"))
	if err != nil {
		t.Fatalf("OpenEmbedded failed: %v", err)
	}
	defer client.Close()

	batch := []BatchRecord{
		{
			Name: "sku_001",
			Props: map[string]float32{
				"inventory": 500,
				"price":     25.5,
			},
		},
		{
			Name: "sku_002",
			Props: map[string]float32{
				"inventory": 1200,
				"price":     10.0,
			},
		},
	}

	names, err := client.SaveBatch(batch)
	if err != nil {
		t.Fatalf("SaveBatch failed: %v", err)
	}
	if len(names) != 2 || names[0] != "sku_001" || names[1] != "sku_002" {
		t.Fatalf("unexpected names returned: %+v", names)
	}

	rec1, err := client.Get("sku_001").Execute(context.Background())
	if err != nil {
		t.Fatalf("Get sku_001 failed: %v", err)
	}
	if rec1.Get("inventory") != 500 {
		t.Errorf("expected inventory=500, got %f", rec1.Get("inventory"))
	}
}

func TestDriver_SaveBatch_RemoteMock(t *testing.T) {
	var receivedBatch bool
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/save_batch" {
			receivedBatch = true
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"success":true,"count":2,"names":["sku_1","sku_2"],"message":"batch records saved"}`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer ts.Close()

	client, err := ConnectRemote(ts.URL)
	if err != nil {
		t.Fatalf("ConnectRemote failed: %v", err)
	}
	defer client.Close()

	names, err := client.SaveBatch([]BatchRecord{
		{Name: "sku_1", Props: map[string]float32{"p": 1}},
		{Name: "sku_2", Props: map[string]float32{"p": 2}},
	})
	if err != nil {
		t.Fatalf("SaveBatch remote failed: %v", err)
	}
	if !receivedBatch {
		t.Errorf("server never received /v1/save_batch request")
	}
	if len(names) != 2 || names[0] != "sku_1" {
		t.Errorf("unexpected names: %+v", names)
	}
}
