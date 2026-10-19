package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/rachmanzz/anchordb/pkg/driver"
	"github.com/rachmanzz/anchordb/pkg/wire"
	"github.com/rachmanzz/anchordb/pkg/worker"
)

// TestS3MinIO_EndToEndArchitectureGaps verifies all three architectural gaps:
// 1. Direct S3 Inspection (POST /v1/partition/s3/list & POST /v1/partition/s3/inspect)
// 2. Partition Catalog / Manifest (GET /v1/partition/catalog & POST /v1/partition/catalog/record)
// 3. Transparent Tiered Query (GET /v1/record, POST /v1/get, and AnchorWire OpGet fallback)
func TestS3MinIO_EndToEndArchitectureGaps(t *testing.T) {
	minioEndpoint := "http://localhost:9000"
	if envEndpoint := os.Getenv("MINIO_ENDPOINT"); envEndpoint != "" {
		minioEndpoint = envEndpoint
	}

	// Check if MinIO is alive
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(minioEndpoint + "/minio/health/live")
	if err != nil || resp.StatusCode != http.StatusOK {
		if resp != nil {
			_ = resp.Body.Close()
		}
		t.Skipf("MinIO is not responding on %s. Skipping server S3 integration test.", minioEndpoint)
	}
	_ = resp.Body.Close()

	// Enable private endpoint & insecure plaintext HTTP for local MinIO testing
	t.Setenv("ANCHORDB_ALLOW_S3_PRIVATE_ENDPOINT", "true")
	t.Setenv("ANCHORDB_ALLOW_INSECURE_S3_HTTP", "true")

	tmpDir, err := os.MkdirTemp("", "anchordb-server-s3-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	authToken := "test-s3-secret-token"
	cfg := ServerConfig{
		DataDir:            tmpDir,
		Port:               "0",
		WirePort:           "0",
		AuthToken:          authToken,
		S3DefaultEndpoint:  minioEndpoint,
		S3DefaultBucket:    "anchordb-test-partitions",
		S3DefaultAccessKey: "minioadmin",
		S3DefaultSecretKey: "minioadmin",
		WorkerConfig: worker.Config{
			Enabled:         true,
			Concurrency:     2,
			QueueSize:       50,
			ShutdownTimeout: 500 * time.Millisecond,
		},
	}

	s, err := NewServer(cfg)
	if err != nil {
		_ = os.RemoveAll(tmpDir)
		t.Fatalf("NewServer failed: %v", err)
	}
	defer cleanupServer(t, s, tmpDir)

	handler := s.Routes()

	// 1. Ingest records into model "financial_logs"
	modelName := "financial_logs"
	db, err := s.getModel(modelName)
	if err != nil {
		t.Fatalf("getModel failed: %v", err)
	}

	nowDays := float32(time.Now().Unix() / 86400)
	_ = db.SaveRecord("tx_archived_1", map[string]float32{"date": nowDays - 250, "val": 100})
	_ = db.SaveRecord("tx_archived_2", map[string]float32{"date": nowDays - 200, "val": 200})
	_ = db.SaveRecord("tx_active_recent", map[string]float32{"date": nowDays - 10, "val": 300})
	if err := db.Sync(); err != nil {
		t.Fatalf("db.Sync failed: %v", err)
	}

	// 2. Call POST /v1/partition/run with S3 sink targeting MinIO
	pruneTrue := true
	prefix := fmt.Sprintf("partitions/http_test_%d", time.Now().UnixNano())
	partReq := PartitionRunRequest{
		Model:         modelName,
		DateField:     "date",
		RetentionDays: 180,
		SinkType:      "s3",
		S3Endpoint:    minioEndpoint,
		S3Bucket:      "anchordb-test-partitions",
		S3Prefix:      prefix,
		S3AccessKey:   "minioadmin",
		S3SecretKey:   "minioadmin",
		PruneHotDB:    &pruneTrue,
		Async:         false,
	}

	reqBody, _ := json.Marshal(partReq)
	req := httptest.NewRequest(http.MethodPost, "/v1/partition/run", bytes.NewReader(reqBody))
	req.Header.Set("Authorization", "Bearer "+authToken)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("POST /v1/partition/run returned status %d: %s", w.Code, w.Body.String())
	}

	var respBody struct {
		Success bool `json:"success"`
		Results []struct {
			ModelName      string  `json:"model_name"`
			ArchivedCount  int     `json:"archived_count"`
			TargetLocation string  `json:"target_location"`
			DurationMs     float64 `json:"duration_ms"`
		} `json:"results"`
	}

	if err := json.Unmarshal(w.Body.Bytes(), &respBody); err != nil {
		t.Fatalf("failed decoding partition response JSON: %v, raw: %s", err, w.Body.String())
	}

	if !respBody.Success || len(respBody.Results) != 1 {
		t.Fatalf("unexpected partition response: %+v", respBody)
	}

	res := respBody.Results[0]
	t.Logf("Server S3 Partition Result: Model=%s ArchivedCount=%d TargetLocation=%s Duration=%.2fms",
		res.ModelName, res.ArchivedCount, res.TargetLocation, res.DurationMs)

	if res.ArchivedCount != 2 {
		t.Fatalf("expected 2 archived records, got %d", res.ArchivedCount)
	}

	// Hot DB check: Old records must be pruned, recent record must exist
	if _, err := db.GetRecord("tx_archived_1"); err == nil {
		t.Fatalf("tx_archived_1 should be pruned from hot db")
	}
	if _, err := db.GetRecord("tx_active_recent"); err != nil {
		t.Fatalf("tx_active_recent must still exist in hot db, got err: %v", err)
	}

	// -------------------------------------------------------------------------
	// GAP 2 VERIFICATION: Partition Catalog / Manifest
	// -------------------------------------------------------------------------
	t.Run("Gap2_PartitionCatalog", func(t *testing.T) {
		// A. Query full catalog for model
		catReq := httptest.NewRequest(http.MethodGet, "/v1/partition/catalog?model="+modelName, nil)
		catReq.Header.Set("Authorization", "Bearer "+authToken)
		catW := httptest.NewRecorder()
		handler.ServeHTTP(catW, catReq)

		if catW.Code != http.StatusOK {
			t.Fatalf("GET /v1/partition/catalog returned %d: %s", catW.Code, catW.Body.String())
		}
		var catResp struct {
			Success    bool `json:"success"`
			Count      int  `json:"count"`
			Partitions []struct {
				PartitionFile  string   `json:"partition_file"`
				TargetLocation string   `json:"target_location"`
				SinkType       string   `json:"sink_type"`
				RecordCount    int      `json:"record_count"`
				RecordNames    []string `json:"record_names"`
			} `json:"partitions"`
		}
		if err := json.Unmarshal(catW.Body.Bytes(), &catResp); err != nil {
			t.Fatalf("unmarshal catalog response failed: %v", err)
		}
		if catResp.Count != 1 || catResp.Partitions[0].RecordCount != 2 {
			t.Fatalf("unexpected catalog partitions: %+v", catResp)
		}
		t.Logf("Catalog registered 1 partition with %d records at %s",
			catResp.Partitions[0].RecordCount, catResp.Partitions[0].TargetLocation)

		// B. Query record location in catalog
		recCatBody, _ := json.Marshal(map[string]string{
			"model": modelName,
			"name":  "tx_archived_1",
		})
		recCatReq := httptest.NewRequest(http.MethodPost, "/v1/partition/catalog/record", bytes.NewReader(recCatBody))
		recCatReq.Header.Set("Authorization", "Bearer "+authToken)
		recCatReq.Header.Set("Content-Type", "application/json")
		recCatW := httptest.NewRecorder()
		handler.ServeHTTP(recCatW, recCatReq)

		if recCatW.Code != http.StatusOK {
			t.Fatalf("POST /v1/partition/catalog/record returned %d: %s", recCatW.Code, recCatW.Body.String())
		}
		var recCatResp struct {
			Success   bool `json:"success"`
			Partition struct {
				PartitionFile string `json:"partition_file"`
			} `json:"partition"`
		}
		if err := json.Unmarshal(recCatW.Body.Bytes(), &recCatResp); err != nil {
			t.Fatalf("unmarshal catalog record response failed: %v", err)
		}
		if recCatResp.Partition.PartitionFile == "" {
			t.Fatalf("catalog failed to locate partition file for record tx_archived_1")
		}
		t.Logf("Catalog confirmed tx_archived_1 is located in partition: %s", recCatResp.Partition.PartitionFile)
	})

	// -------------------------------------------------------------------------
	// GAP 1 VERIFICATION: Direct S3 Inspection & Listing
	// -------------------------------------------------------------------------
	t.Run("Gap1_DirectS3InspectAndList", func(t *testing.T) {
		// A. Direct S3 Listing
		listReqBody, _ := json.Marshal(map[string]string{
			"s3_endpoint":   minioEndpoint,
			"s3_bucket":     "anchordb-test-partitions",
			"s3_prefix":     prefix,
			"s3_access_key": "minioadmin",
			"s3_secret_key": "minioadmin",
		})
		listReq := httptest.NewRequest(http.MethodPost, "/v1/partition/s3/list", bytes.NewReader(listReqBody))
		listReq.Header.Set("Authorization", "Bearer "+authToken)
		listReq.Header.Set("Content-Type", "application/json")
		listW := httptest.NewRecorder()
		handler.ServeHTTP(listW, listReq)

		if listW.Code != http.StatusOK {
			t.Fatalf("POST /v1/partition/s3/list returned %d: %s", listW.Code, listW.Body.String())
		}
		var listResp struct {
			Success    bool `json:"success"`
			Count      int  `json:"count"`
			Partitions []struct {
				Key  string `json:"key"`
				Size int64  `json:"size"`
			} `json:"partitions"`
		}
		if err := json.Unmarshal(listW.Body.Bytes(), &listResp); err != nil {
			t.Fatalf("unmarshal s3 list response failed: %v", err)
		}
		if listResp.Count < 1 {
			t.Fatalf("expected at least 1 S3 partition file in %s, got %d", prefix, listResp.Count)
		}
		s3Key := listResp.Partitions[0].Key
		t.Logf("Direct S3 List found partition key: %s (size %d bytes)", s3Key, listResp.Partitions[0].Size)

		// B. Direct S3 Inspect (reading record properties directly from S3 partition)
		inspectReqBody, _ := json.Marshal(map[string]interface{}{
			"s3_endpoint":   minioEndpoint,
			"s3_bucket":     "anchordb-test-partitions",
			"s3_key":        s3Key,
			"s3_access_key": "minioadmin",
			"s3_secret_key": "minioadmin",
			"name":          "tx_archived_1",
		})
		inspectReq := httptest.NewRequest(http.MethodPost, "/v1/partition/s3/inspect", bytes.NewReader(inspectReqBody))
		inspectReq.Header.Set("Authorization", "Bearer "+authToken)
		inspectReq.Header.Set("Content-Type", "application/json")
		inspectW := httptest.NewRecorder()
		handler.ServeHTTP(inspectW, inspectReq)

		if inspectW.Code != http.StatusOK {
			t.Fatalf("POST /v1/partition/s3/inspect returned %d: %s", inspectW.Code, inspectW.Body.String())
		}
		var inspectResp struct {
			Success bool               `json:"success"`
			Props   map[string]float32 `json:"props"`
		}
		if err := json.Unmarshal(inspectW.Body.Bytes(), &inspectResp); err != nil {
			t.Fatalf("unmarshal s3 inspect response failed: %v", err)
		}
		if inspectResp.Props["val"] != 100 {
			t.Fatalf("expected val=100 from S3 partition inspection, got %v", inspectResp.Props)
		}
		t.Logf("Direct S3 Inspect retrieved props for tx_archived_1: %+v", inspectResp.Props)

		// C. Transparent S3 Inspect via /v1/partition/inspect
		transInspectBody, _ := json.Marshal(map[string]interface{}{
			"partition_file": res.TargetLocation, // s3://anchordb-test-partitions/...
			"name":           "tx_archived_2",
		})
		transReq := httptest.NewRequest(http.MethodPost, "/v1/partition/inspect", bytes.NewReader(transInspectBody))
		transReq.Header.Set("Authorization", "Bearer "+authToken)
		transReq.Header.Set("Content-Type", "application/json")
		transW := httptest.NewRecorder()
		handler.ServeHTTP(transW, transReq)

		if transW.Code != http.StatusOK {
			t.Fatalf("POST /v1/partition/inspect on S3 URI returned %d: %s", transW.Code, transW.Body.String())
		}
		var transResp struct {
			Success bool               `json:"success"`
			Props   map[string]float32 `json:"props"`
		}
		if err := json.Unmarshal(transW.Body.Bytes(), &transResp); err != nil {
			t.Fatalf("unmarshal partition inspect response failed: %v", err)
		}
		if transResp.Props["val"] != 200 {
			t.Fatalf("expected val=200 for tx_archived_2, got %v", transResp.Props)
		}
		t.Logf("Transparent /v1/partition/inspect retrieved tx_archived_2: %+v", transResp.Props)
	})

	// -------------------------------------------------------------------------
	// GAP 3 VERIFICATION: Transparent Tiered Query via HTTP
	// -------------------------------------------------------------------------
	t.Run("Gap3_TransparentTieredQuery_HTTP", func(t *testing.T) {
		// A. GET /v1/record?model=financial_logs&name=tx_archived_1
		// Record was pruned from hot DB; server must automatically fetch from S3 cold tier!
		getReq := httptest.NewRequest(http.MethodGet, "/v1/record?model="+modelName+"&name=tx_archived_1", nil)
		getReq.Header.Set("Authorization", "Bearer "+authToken)
		getW := httptest.NewRecorder()
		handler.ServeHTTP(getW, getReq)

		if getW.Code != http.StatusOK {
			t.Fatalf("GET /v1/record for cold record returned %d: %s", getW.Code, getW.Body.String())
		}
		var getResp struct {
			Success bool               `json:"success"`
			Name    string             `json:"name"`
			Tier    string             `json:"tier"`
			Props   map[string]float32 `json:"props"`
		}
		if err := json.Unmarshal(getW.Body.Bytes(), &getResp); err != nil {
			t.Fatalf("unmarshal GET /v1/record response failed: %v", err)
		}
		if getResp.Tier != "cold" || getResp.Props["val"] != 100 {
			t.Fatalf("expected cold tier val=100 for tx_archived_1, got tier=%s, props=%+v", getResp.Tier, getResp.Props)
		}
		t.Logf("Transparent GET /v1/record resolved cold record tx_archived_1: tier=%s, val=%.0f", getResp.Tier, getResp.Props["val"])

		// B. POST /v1/get for active hot record
		hotBody, _ := json.Marshal(map[string]string{
			"model": modelName,
			"name":  "tx_active_recent",
		})
		hotReq := httptest.NewRequest(http.MethodPost, "/v1/get", bytes.NewReader(hotBody))
		hotReq.Header.Set("Authorization", "Bearer "+authToken)
		hotReq.Header.Set("Content-Type", "application/json")
		hotW := httptest.NewRecorder()
		handler.ServeHTTP(hotW, hotReq)

		if hotW.Code != http.StatusOK {
			t.Fatalf("POST /v1/get for hot record returned %d: %s", hotW.Code, hotW.Body.String())
		}
		var hotResp struct {
			Success bool               `json:"success"`
			Tier    string             `json:"tier"`
			Props   map[string]float32 `json:"props"`
		}
		if err := json.Unmarshal(hotW.Body.Bytes(), &hotResp); err != nil {
			t.Fatalf("unmarshal hot record response failed: %v", err)
		}
		if hotResp.Tier != "hot" || hotResp.Props["val"] != 300 {
			t.Fatalf("expected hot tier val=300, got tier=%s, props=%+v", hotResp.Tier, hotResp.Props)
		}
		t.Logf("Hot record tx_active_recent resolved: tier=%s, val=%.0f", hotResp.Tier, hotResp.Props["val"])
	})

	// -------------------------------------------------------------------------
	// ANCHORWIRE PROTOCOL VERIFICATION: Transparent Cold Tier & Wire Commands
	// -------------------------------------------------------------------------
	t.Run("AnchorWire_TransparentTieredQueryAndCommands", func(t *testing.T) {
		// Listen on dynamic TCP port
		wireListener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("failed to bind dynamic wire port: %v", err)
		}
		wireAddr := wireListener.Addr().String()

		go func() {
			_ = s.WireServer().Serve(wireListener)
		}()

		// Connect client using driver.ConnectWire
		driverClient, err := driver.ConnectWire(wireAddr,
			driver.WithModel(modelName),
			driver.WithToken(authToken),
			driver.WithTimeout(5*time.Second),
		)
		if err != nil {
			t.Fatalf("driver.ConnectWire failed: %v", err)
		}
		defer driverClient.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// A. Transparent query of cold record via AnchorWire!
		coldRec, err := driverClient.Get("tx_archived_1").Execute(ctx)
		if err != nil {
			t.Fatalf("Get cold record via AnchorWire failed: %v", err)
		}
		if coldRec.Get("val") != 100 {
			t.Fatalf("expected val=100 from AnchorWire for tx_archived_1, got %.2f", coldRec.Get("val"))
		}
		t.Logf("AnchorWire successfully resolved cold pruned record tx_archived_1: val=%.0f", coldRec.Get("val"))

		// B. Query hot record via AnchorWire
		hotRec, err := driverClient.Get("tx_active_recent").Execute(ctx)
		if err != nil {
			t.Fatalf("Get hot record via AnchorWire failed: %v", err)
		}
		if hotRec.Get("val") != 300 {
			t.Fatalf("expected val=300 from AnchorWire for tx_active_recent, got %.2f", hotRec.Get("val"))
		}
		t.Logf("AnchorWire successfully resolved hot record tx_active_recent: val=%.0f", hotRec.Get("val"))

		// C. Native AnchorWire Client Catalog & Inspect commands
		wc, err := wire.Connect(wireAddr,
			wire.WithClientToken(authToken),
			wire.WithClientModel(modelName),
			wire.WithClientTimeout(5*time.Second),
		)
		if err != nil {
			t.Fatalf("wire.Connect failed: %v", err)
		}
		defer wc.Close()

		// OpPartitionCatalog over wire
		catItems, err := wc.PartitionCatalog(modelName)
		if err != nil {
			t.Fatalf("wc.PartitionCatalog failed: %v", err)
		}
		if len(catItems) != 1 || catItems[0].RecordCount != 2 {
			t.Fatalf("unexpected catItems over wire: %+v", catItems)
		}
		t.Logf("AnchorWire OpPartitionCatalog returned %d items, target=%s", len(catItems), catItems[0].TargetLocation)

		// OpPartitionInspect over wire
		inspectResp, err := wc.PartitionInspect(wire.PartitionInspectWireRequest{
			PartitionFile: catItems[0].PartitionFile,
			RecordName:    "tx_archived_2",
		})
		if err != nil {
			t.Fatalf("wc.PartitionInspect failed: %v", err)
		}
		if inspectResp.Props["val"] != 200 {
			t.Fatalf("expected val=200 for tx_archived_2 via wire inspect, got %+v", inspectResp)
		}
		t.Logf("AnchorWire OpPartitionInspect retrieved record: %+v", inspectResp.Props)
	})

	t.Log("All 3 Architectural Gaps + AnchorWire Support: 100% VERIFIED & PASSING!")
}
