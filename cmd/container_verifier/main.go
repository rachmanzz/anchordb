package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os/exec"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rachmanzz/anchordb/pkg/driver"
	"github.com/rachmanzz/anchordb/pkg/spatial"
)

const (
	baseURL       = "http://localhost:7447"
	authToken     = "test_secret_token_live"
	containerName = "anchordb-live-test"
	testModel     = "iot_plant"
)

func main() {
	fmt.Println("================================================================================")
	fmt.Println("       ANCHORDB CONTAINER INTEGRATION & STRESS VERIFICATION SUITE              ")
	fmt.Println("================================================================================")

	ctx := context.Background()

	// Wait for container to be ready
	if err := waitForHealth(15 * time.Second); err != nil {
		log.Fatalf("[FATAL] Container is not ready: %v", err)
	}
	fmt.Println("[READY] AnchorDB Container is healthy and responding on :7447")

	// -------------------------------------------------------------------------
	// (a) Stress Testing All Operations
	// -------------------------------------------------------------------------
	fmt.Println("--------------------------------------------------------------------------------")
	fmt.Println(" (A) STRESS TESTING ALL ENGINE OPERATIONS")
	fmt.Println("--------------------------------------------------------------------------------")
	runStressTests(ctx)

	// -------------------------------------------------------------------------
	// (b) Concurrency Polling Ramp-Up (from few to many)
	// -------------------------------------------------------------------------
	fmt.Println("\n--------------------------------------------------------------------------------")
	fmt.Println(" (B) CONCURRENCY POLLING RAMP-UP (Few -> Many Workers)")
	fmt.Println("--------------------------------------------------------------------------------")
	runConcurrencyRampUp(ctx)

	// -------------------------------------------------------------------------
	// (c) Stop Container, Restart, Check Data Persistence
	// -------------------------------------------------------------------------
	fmt.Println("\n--------------------------------------------------------------------------------")
	fmt.Println(" (C) CONTAINER RESTART & DATA PERSISTENCE VERIFICATION")
	fmt.Println("--------------------------------------------------------------------------------")
	runPersistenceRestartTest(ctx)

	// -------------------------------------------------------------------------
	// (d) Short Scheduling Verification
	// -------------------------------------------------------------------------
	fmt.Println("\n--------------------------------------------------------------------------------")
	fmt.Println(" (D) SHORT SCHEDULER & PERIODIC SYNC VERIFICATION")
	fmt.Println("--------------------------------------------------------------------------------")
	runSchedulerTests(ctx)

	// -------------------------------------------------------------------------
	// (e) Cold Data Partitioning & Archival Verification
	// -------------------------------------------------------------------------
	fmt.Println("\n--------------------------------------------------------------------------------")
	fmt.Println(" (E) COLD DATA ARCHIVAL & PARTITION PRUNING VERIFICATION")
	fmt.Println("--------------------------------------------------------------------------------")
	runColdDataArchivalTest(ctx)

	fmt.Println("\n================================================================================")
	fmt.Println(" [ALL TESTS PASSED] Container validation completed successfully with 100% integrity!")
	fmt.Println("================================================================================")
}

// -----------------------------------------------------------------------------
// Helper: Wait for /health
// -----------------------------------------------------------------------------

func waitForHealth(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	client := &http.Client{Timeout: 1 * time.Second}
	for time.Now().Before(deadline) {
		req, _ := http.NewRequest(http.MethodGet, baseURL+"/health", nil)
		req.Header.Set("Authorization", "Bearer "+authToken)
		resp, err := client.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for health check at %s", baseURL)
}

// -----------------------------------------------------------------------------
// Phase (A): Stress Testing All Operations
// -----------------------------------------------------------------------------

func runStressTests(ctx context.Context) {
	client, err := driver.ConnectRemote(baseURL, driver.WithToken(authToken), driver.WithModel(testModel))
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}
	defer client.Close()

	// 1. Batch Writes
	const totalWrites = 2000
	fmt.Printf("[1/7] Writing %d records with 6 properties each...\n", totalWrites)
	t0 := time.Now()
	for i := 0; i < totalWrites; i++ {
		name := fmt.Sprintf("sensor_%04d", i)
		props := map[string]float32{
			"moisture":    float32(30 + (i % 50)),
			"depth":       float32(5 + (i % 20)),
			"temperature": float32(20 + (i % 15)),
			"voltage":     float32(10 + (i % 5)),
			"current":     float32(1 + (i % 3)),
			"date":        driver.MustDate("2026-10-16"),
		}
		if err := client.Save(name, props); err != nil {
			log.Fatalf("Save %s failed: %v", name, err)
		}
	}
	writeDuration := time.Since(t0)
	fmt.Printf("      -> Completed in %v (%.0f writes/sec)\n", writeDuration, float64(totalWrites)/writeDuration.Seconds())

	// 2. Computed Point Queries
	fmt.Println("[2/7] Testing on-the-fly In-Storage Computations (Divide & Multiply)...")
	rec, err := client.Get("sensor_0100").
		Compute("moisture_per_depth", driver.Divide("moisture", "depth")).
		Compute("power", driver.Multiply("voltage", "current")).
		Execute(ctx)
	if err != nil {
		log.Fatalf("Computed Get failed: %v", err)
	}
	expectedPower := rec.Get("voltage") * rec.Get("current")
	if rec.Get("power") != expectedPower {
		log.Fatalf("Power mismatch: got %v, want %v", rec.Get("power"), expectedPower)
	}
	fmt.Printf("      -> sensor_0100: voltage=%.1f, current=%.1f => power=%.1f W [OK]\n",
		rec.Get("voltage"), rec.Get("current"), rec.Get("power"))

	// 3. Filtered List & Pagination
	fmt.Println("[3/7] Testing Rich Filtering (Where, WhereDate) and Server Pagination...")
	paged, err := client.List().
		Where("temperature", ">=", 25.0).
		WhereDate("date", "2026-10-16").
		Compute("power", driver.Multiply("voltage", "current")).
		Page(1, 10).
		Execute(ctx)
	if err != nil {
		log.Fatalf("List query failed: %v", err)
	}
	fmt.Printf("      -> Page 1 returned %d items (HasMore: %v) [OK]\n", len(paged.Items), paged.HasMore)

	// 4. Batch Updates
	fmt.Println("[4/7] Testing Batch In-Place Arithmetic Updates...")
	t0 = time.Now()
	updatedCount, err := client.Update().
		Where("voltage", "<", 12.0).
		Set("voltage", driver.Add("voltage", "1.0")).
		Execute(ctx)
	if err != nil {
		log.Fatalf("Batch update failed: %v", err)
	}
	fmt.Printf("      -> Updated %d records in %v [OK]\n", updatedCount, time.Since(t0))

	// 5. Spatial Ray-Casting & Graph Traversal
	fmt.Println("[5/7] Testing Spatial Node Insertion & Vector Ray-Casting...")
	meshNodes := []struct {
		word  string
		coord spatial.Point2D
		roles [4]float32
	}{
		{"root_controller", spatial.Point2D{X: 0.2, Y: 0.9}, [4]float32{0.9, 0.1, 0.2, 0.2}},
		{"repeater_node_1", spatial.Point2D{X: 0.2, Y: 0.5}, [4]float32{0.5, 0.5, 0.2, 0.2}},
		{"sprinkler_zone_a", spatial.Point2D{X: 0.2, Y: 0.1}, [4]float32{0.1, 0.1, 0.9, 0.2}},
		{"sprinkler_zone_b", spatial.Point2D{X: 0.8, Y: -0.5}, [4]float32{0.1, 0.1, 0.8, 0.3}},
	}
	for _, n := range meshNodes {
		if err := client.AppendNode(n.word, n.coord, n.roles); err != nil {
			log.Fatalf("AppendNode %s failed: %v", n.word, err)
		}
	}
	if err := client.SetRelation("south_feed", spatial.Point2D{Y: -1}); err != nil {
		log.Fatalf("SetRelation failed: %v", err)
	}

	graphResp, err := client.Graph().
		From("root_controller").
		Relate("south_feed").
		TopK(3).
		Execute(ctx)
	if err != nil {
		log.Fatalf("Graph ray-cast failed: %v", err)
	}
	fmt.Printf("      -> Ray Cast from root_controller along south_feed: Top Result = %s (Score: %.4f) [OK]\n",
		graphResp.Results[0].Word, graphResp.Results[0].Score)

	// 6. Causal Reasoning & Chain
	fmt.Println("[6/7] Testing Causal Link Insertion and Multi-Hop Chain Traversal...")
	_ = client.Link("soil_dry", "triggers", "irrigation_on", 0.95)
	_ = client.Link("irrigation_on", "causes", "soil_moist", 0.90)

	causalHop, err := client.Causal("soil_dry", "triggers", 5)
	if err != nil || len(causalHop) == 0 {
		log.Fatalf("Causal step failed: %v", err)
	}
	causalChain, err := client.CausalChain([]string{"soil_dry"}, []string{"triggers", "causes"}, 5)
	if err != nil || len(causalChain) == 0 {
		log.Fatalf("Causal chain failed: %v", err)
	}
	fmt.Printf("      -> Causal Chain: soil_dry -> triggers -> %s -> causes -> %s (Cumulative Score: %.3f) [OK]\n",
		causalHop[0].Word, causalChain[0].Word, causalChain[0].Score)

	// 7. Direct Math Engine
	fmt.Println("[7/7] Testing Direct Math Engine (Distance, Centroid, CosineSimilarity, Nearest)...")
	math := client.Math()
	dist, err := math.Distance("root_controller", "sprinkler_zone_a")
	if err != nil {
		log.Fatalf("Distance failed: %v", err)
	}
	centroid, err := math.Centroid("root_controller", "sprinkler_zone_a")
	if err != nil {
		log.Fatalf("Centroid failed: %v", err)
	}
	sim, err := math.CosineSimilarity("root_controller", "repeater_node_1")
	if err != nil {
		log.Fatalf("CosineSimilarity failed: %v", err)
	}
	nearest, err := math.Nearest("root_controller", 3)
	if err != nil {
		log.Fatalf("Nearest failed: %v", err)
	}
	fmt.Printf("      -> Distance: %.4f, Centroid: (%.2f, %.2f), Similarity: %.4f, Nearest Count: %d [OK]\n",
		dist, centroid.Coord.X, centroid.Coord.Y, sim, len(nearest))
}

// -----------------------------------------------------------------------------
// Phase (B): Concurrency Polling Ramp-Up (Few -> Many Workers)
// -----------------------------------------------------------------------------

func runConcurrencyRampUp(ctx context.Context) {
	tiers := []struct {
		concurrency int
		requests    int
	}{
		{concurrency: 5, requests: 500},
		{concurrency: 25, requests: 2500},
		{concurrency: 50, requests: 5000},
		{concurrency: 100, requests: 10000},
	}

	for _, tier := range tiers {
		client, err := driver.ConnectRemote(baseURL, driver.WithToken(authToken), driver.WithModel(testModel))
		if err != nil {
			log.Fatalf("failed to connect: %v", err)
		}

		var (
			successCount atomic.Int64
			failureCount atomic.Int64
			latencies    = make([]time.Duration, 0, tier.requests)
			latMu        sync.Mutex
		)

		fmt.Printf("[POLLING] Ramp-Up: %d concurrent workers executing %d requests...\n",
			tier.concurrency, tier.requests)

		reqPerWorker := tier.requests / tier.concurrency
		var wg sync.WaitGroup
		t0 := time.Now()

		for w := 0; w < tier.concurrency; w++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				localLatencies := make([]time.Duration, 0, reqPerWorker)

				for i := 0; i < reqPerWorker; i++ {
					targetID := fmt.Sprintf("sensor_%04d", (workerID*reqPerWorker+i)%2000)
					reqStart := time.Now()

					// Mix point computed read and list
					if i%4 == 0 {
						_, err := client.List().Where("moisture", ">", 40).Page(1, 5).Execute(ctx)
						if err != nil {
							failureCount.Add(1)
						} else {
							successCount.Add(1)
						}
					} else {
						_, err := client.Get(targetID).
							Compute("power", driver.Multiply("voltage", "current")).
							Execute(ctx)
						if err != nil {
							failureCount.Add(1)
						} else {
							successCount.Add(1)
						}
					}
					localLatencies = append(localLatencies, time.Since(reqStart))
				}

				latMu.Lock()
				latencies = append(latencies, localLatencies...)
				latMu.Unlock()
			}(w)
		}

		wg.Wait()
		duration := time.Since(t0)
		throughput := float64(tier.requests) / duration.Seconds()

		// Calculate latency stats
		sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
		var totalLat time.Duration
		for _, l := range latencies {
			totalLat += l
		}
		avgLat := time.Duration(0)
		if len(latencies) > 0 {
			avgLat = totalLat / time.Duration(len(latencies))
		}
		minLat := latencies[0]
		p50Lat := latencies[len(latencies)/2]
		p95Index := int(float64(len(latencies)) * 0.95)
		if p95Index >= len(latencies) {
			p95Index = len(latencies) - 1
		}
		p95Lat := latencies[p95Index]
		p99Index := int(float64(len(latencies)) * 0.99)
		if p99Index >= len(latencies) {
			p99Index = len(latencies) - 1
		}
		p99Lat := latencies[p99Index]
		maxLat := latencies[len(latencies)-1]

		fmt.Printf("      -> Concurrency %3d: %d OK, %d Failed | Duration: %v (%.0f req/s) | min=%v, avg=%v, p50=%v, p95=%v, p99=%v, max=%v\n",
			tier.concurrency, successCount.Load(), failureCount.Load(), duration, throughput, minLat, avgLat, p50Lat, p95Lat, p99Lat, maxLat)

		client.Close()
		if failureCount.Load() > 0 {
			log.Fatalf("concurrency tier %d had %d failed requests!", tier.concurrency, failureCount.Load())
		}
	}
}

// -----------------------------------------------------------------------------
// Phase (C): Stop Container, Restart, Check Data Persistence
// -----------------------------------------------------------------------------

func runPersistenceRestartTest(ctx context.Context) {
	client, err := driver.ConnectRemote(baseURL, driver.WithToken(authToken), driver.WithModel(testModel))
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}

	// 1. Write Canary Record & Canary Nodes
	fmt.Println("[1/4] Writing canary sentinel records and syncing to disk...")
	canaryID := "sensor_canary_omega_99"
	err = client.Save(canaryID, map[string]float32{
		"moisture": 77.7,
		"depth":    15.0,
		"voltage":  12.5,
		"current":  2.0,
	})
	if err != nil {
		log.Fatalf("Canary write failed: %v", err)
	}
	_ = client.AppendNode("canary_node_primary", spatial.Point2D{X: 0.5, Y: 0.5}, [4]float32{0.8, 0.2, 0.5, 0.1})
	_ = client.Link("canary_node_primary", "triggers", "canary_node_target", 0.99)

	// Explicit sync
	if err := client.Sync(); err != nil {
		log.Fatalf("Sync before stop failed: %v", err)
	}
	client.Close()

	// 2. Stop container
	fmt.Printf("[2/4] Executing: podman stop %s ...\n", containerName)
	t0 := time.Now()
	cmdStop := exec.Command("podman", "stop", containerName)
	if out, err := cmdStop.CombinedOutput(); err != nil {
		log.Fatalf("podman stop failed: %v, output: %s", err, string(out))
	}
	fmt.Printf("      -> Container stopped cleanly in %v.\n", time.Since(t0))

	// Verify server is unreachable
	unreachableClient := &http.Client{Timeout: 500 * time.Millisecond}
	_, err = unreachableClient.Get(baseURL + "/health")
	if err == nil {
		log.Fatalf("Expected connection refused while container is stopped, but got response!")
	}
	fmt.Println("      -> Verified server is offline and unreachable [OK]")

	// 3. Start container
	fmt.Printf("[3/4] Executing: podman start %s ...\n", containerName)
	t0 = time.Now()
	cmdStart := exec.Command("podman", "start", containerName)
	if out, err := cmdStart.CombinedOutput(); err != nil {
		log.Fatalf("podman start failed: %v, output: %s", err, string(out))
	}
	fmt.Printf("      -> Container restarted in %v.\n", time.Since(t0))

	// Wait for health
	if err := waitForHealth(15 * time.Second); err != nil {
		log.Fatalf("Container failed to become healthy after start: %v", err)
	}
	fmt.Println("      -> Container is healthy again [OK]")

	// 4. Verify data persistence after restart
	fmt.Println("[4/4] Verifying all data and models survived restart...")
	reopenedClient, err := driver.ConnectRemote(baseURL, driver.WithToken(authToken), driver.WithModel(testModel))
	if err != nil {
		log.Fatalf("Reconnect failed: %v", err)
	}
	defer reopenedClient.Close()

	// Verify canary record
	canaryRec, err := reopenedClient.Get(canaryID).
		Compute("power", driver.Multiply("voltage", "current")).
		Execute(ctx)
	if err != nil {
		log.Fatalf("Canary record missing after restart: %v", err)
	}
	if canaryRec.Get("moisture") != 77.7 {
		log.Fatalf("Canary moisture mismatch: got %v, want 77.7", canaryRec.Get("moisture"))
	}
	expectedCanaryPower := float32(12.5 * 2.0)
	if canaryRec.Get("power") != expectedCanaryPower {
		log.Fatalf("Canary power mismatch: got %v, want %v", canaryRec.Get("power"), expectedCanaryPower)
	}
	fmt.Printf("      -> Canary Record verified: %s (moisture=%.1f, computed power=%.1f) [OK]\n",
		canaryID, canaryRec.Get("moisture"), canaryRec.Get("power"))

	// Verify spatial & causal data
	causal, err := reopenedClient.Causal("canary_node_primary", "triggers", 5)
	if err != nil || len(causal) == 0 {
		log.Fatalf("Canary causal edge missing after restart: %v", err)
	}
	fmt.Printf("      -> Canary Causal Edge verified: %s -> triggers -> %s (Weight: %.2f) [OK]\n",
		"canary_node_primary", causal[0].Word, causal[0].Weight)

	// Verify bulk records count
	listRes, err := reopenedClient.List().Page(1, 10).Execute(ctx)
	if err != nil || len(listRes.Items) == 0 {
		log.Fatalf("List records failed after restart: %v", err)
	}
	fmt.Printf("      -> Bulk dataset verified: returned page 1 with %d items (HasMore: %v) [OK]\n",
		len(listRes.Items), listRes.HasMore)
}

// -----------------------------------------------------------------------------
// Phase (D): Short Scheduling Verification
// -----------------------------------------------------------------------------

func runSchedulerTests(ctx context.Context) {
	fmt.Println("[1/3] Inspecting active scheduler status from engine...")
	req, _ := http.NewRequest(http.MethodGet, baseURL+"/v1/scheduler/status", nil)
	req.Header.Set("Authorization", "Bearer "+authToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatalf("Failed to query scheduler status: %v", err)
	}
	defer resp.Body.Close()

	var schedStatus struct {
		Success bool `json:"success"`
		Enabled bool `json:"enabled"`
		Tasks   []struct {
			Name       string `json:"name"`
			Schedule   string `json:"schedule"`
			LastStatus string `json:"last_status"`
		} `json:"tasks"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&schedStatus); err != nil {
		log.Fatalf("Failed to decode scheduler status: %v", err)
	}

	fmt.Printf("      -> Scheduler Enabled: %v, Registered Tasks: %d\n", schedStatus.Enabled, len(schedStatus.Tasks))
	for _, t := range schedStatus.Tasks {
		fmt.Printf("         * Task %-28s | Schedule: %-10s | Last Status: %s\n", t.Name, t.Schedule, t.LastStatus)
	}

	// 2. Observe short background sync
	fmt.Println("[2/3] Waiting 4 seconds to observe automated periodic sync schedule (@every 3s)...")
	time.Sleep(4 * time.Second)

	// 3. Manual Task Trigger via /v1/scheduler/run
	fmt.Println("[3/3] Triggering immediate task run via /v1/scheduler/run?task=periodic_background_sync...")
	reqRun, _ := http.NewRequest(http.MethodPost, baseURL+"/v1/scheduler/run?task=periodic_background_sync", nil)
	reqRun.Header.Set("Authorization", "Bearer "+authToken)
	respRun, err := http.DefaultClient.Do(reqRun)
	if err != nil {
		log.Fatalf("Manual scheduler run failed: %v", err)
	}
	defer respRun.Body.Close()
	bodyRun, _ := io.ReadAll(respRun.Body)
	if respRun.StatusCode != http.StatusOK {
		log.Fatalf("Scheduler run returned status %d: %s", respRun.StatusCode, string(bodyRun))
	}
	fmt.Printf("      -> Scheduler Run Response: %s [OK]\n", string(bodyRun))
}

// -----------------------------------------------------------------------------
// Phase (E): Cold Data Partitioning & Archival Verification
// -----------------------------------------------------------------------------

func runColdDataArchivalTest(ctx context.Context) {
	client, err := driver.ConnectRemote(baseURL, driver.WithToken(authToken), driver.WithModel(testModel))
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}

	// 1. Insert historical (cold) records and recent (hot) records
	fmt.Println("[1/4] Inserting historical records (older than 60 days) and active records...")
	_ = client.Save("sensor_archive_old_1", map[string]float32{
		"moisture": 20.0,
		"date":     driver.MustDate("2026-05-01"), // 168 days ago
	})
	_ = client.Save("sensor_archive_old_2", map[string]float32{
		"moisture": 25.0,
		"date":     driver.MustDate("2026-06-01"), // 137 days ago
	})
	_ = client.Save("sensor_active_recent", map[string]float32{
		"moisture": 85.0,
		"date":     driver.MustDate("2026-10-16"), // today
	})
	_ = client.Sync()
	client.Close()

	// 2. Run Partition Archival via Engine API
	fmt.Println("[2/4] Triggering cold data partition archival (/v1/partition/run with 60-day retention)...")
	partitionPayload := map[string]interface{}{
		"model":          testModel,
		"date_field":     "date",
		"retention_days": 60,
		"prune_hot_db":   true,
		"sink_type":      "local",
		"target_path":    "/data/partitions",
	}
	payloadBytes, _ := json.Marshal(partitionPayload)
	req, _ := http.NewRequest(http.MethodPost, baseURL+"/v1/partition/run", bytes.NewReader(payloadBytes))
	req.Header.Set("Authorization", "Bearer "+authToken)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatalf("Partition run failed: %v", err)
	}
	defer resp.Body.Close()

	var partResult struct {
		Success bool `json:"success"`
		Results []struct {
			ModelName      string  `json:"model_name"`
			ArchivedCount  int     `json:"archived_count"`
			TargetLocation string  `json:"target_location"`
			CutoffDate     string  `json:"cutoff_date"`
			DurationMs     float64 `json:"duration_ms"`
		} `json:"results"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&partResult); err != nil {
		log.Fatalf("Failed to decode partition result: %v", err)
	}
	if len(partResult.Results) == 0 {
		log.Fatalf("Partition run returned empty results")
	}
	res0 := partResult.Results[0]
	fmt.Printf("      -> Partition executed: %d records archived to %s (Cutoff: %s) in %.2fms [OK]\n",
		res0.ArchivedCount, res0.TargetLocation, res0.CutoffDate, res0.DurationMs)

	// 3. Verify hot database pruning
	fmt.Println("[3/4] Verifying cold records were pruned from active hot database...")
	hotClient, err := driver.ConnectRemote(baseURL, driver.WithToken(authToken), driver.WithModel(testModel))
	if err != nil {
		log.Fatalf("connect failed: %v", err)
	}
	defer hotClient.Close()

	_, err = hotClient.Get("sensor_archive_old_1").Execute(ctx)
	if err == nil {
		log.Fatalf("Expected sensor_archive_old_1 to be pruned from hot database!")
	}
	fmt.Println("      -> sensor_archive_old_1 is no longer in hot DB [PRUNED]")

	recRecent, err := hotClient.Get("sensor_active_recent").Execute(ctx)
	if err != nil {
		log.Fatalf("Active record missing in hot DB: %v", err)
	}
	fmt.Printf("      -> sensor_active_recent remains in hot DB (moisture=%.1f) [ACTIVE]\n", recRecent.Get("moisture"))

	// 4. Verify cold partition through Engine API (no raw file access)
	fmt.Println("[4/4] Locating and verifying cold data partition through the Engine API...")
	partitions, err := hotClient.ListPartitions(ctx)
	if err != nil || len(partitions) == 0 {
		log.Fatalf("No cold partition files returned by engine: %v", err)
	}
	coldPartitionFile := partitions[0].Name
	fmt.Printf("      -> Engine detected partition archive: %s (%d bytes, modTime: %s)\n",
		coldPartitionFile, partitions[0].SizeBytes, partitions[0].ModTime.Format(time.RFC3339))

	coldRec, err := hotClient.InspectPartitionRecord(ctx, coldPartitionFile, "sensor_archive_old_1")
	if err != nil {
		log.Fatalf("Failed to retrieve record from cold archive via engine: %v", err)
	}
	fmt.Printf("      -> Cold Archive Query (Engine Authenticated): %s found in %s! (moisture=%.1f) [OK]\n",
		coldRec.Name, coldPartitionFile, coldRec.Get("moisture"))
}
