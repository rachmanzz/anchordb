package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
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

var (
	baseURL       = "http://localhost:7447"
	authToken     = "test_secret_token_live"
	containerName = "anchordb-live-test"
	testModel     = "iot_plant"
)

func main() {
	flag.StringVar(&baseURL, "url", baseURL, "AnchorDB server base URL")
	flag.StringVar(&authToken, "token", authToken, "AnchorDB bearer auth token")
	flag.StringVar(&containerName, "container", containerName, "Container name in podman/docker")
	flag.StringVar(&testModel, "model", testModel, "Test model database name")
	flag.Parse()

	fmt.Println("================================================================================")
	fmt.Println("       ANCHORDB CONTAINER INTEGRATION & STRESS VERIFICATION SUITE              ")
	fmt.Println("================================================================================")

	ctx := context.Background()

	// Wait for container to be ready
	if err := waitForHealth(15 * time.Second); err != nil {
		log.Fatalf("[FATAL] Container is not ready: %v", err)
	}
	fmt.Printf("[READY] AnchorDB Container (%s) is healthy on %s\n", containerName, baseURL)

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

	// -------------------------------------------------------------------------
	// (f) Schema Attachment & Strict Immutability Stress Verification
	// -------------------------------------------------------------------------
	fmt.Println("\n--------------------------------------------------------------------------------")
	fmt.Println(" (F) SCHEMA ATTACHMENT & STRICT IMMUTABILITY STRESS VERIFICATION")
	fmt.Println("--------------------------------------------------------------------------------")
	runSchemaStressTest(ctx)

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

	// 6. Causal Reasoning & Multi-Branching Chain (Mesh DAG)
	fmt.Println("[6/7] Testing Causal Link Insertion & Multi-Branching DAG Traversal...")
	_ = client.Link("soil_dry", "triggers", "irrigation_pump_1", 0.95)
	_ = client.Link("soil_dry", "triggers", "irrigation_pump_2", 0.90)
	_ = client.Link("irrigation_pump_1", "causes", "sprinkler_zone_a", 0.95)
	_ = client.Link("irrigation_pump_2", "causes", "sprinkler_zone_a", 0.88) // convergence
	_ = client.Link("sprinkler_zone_a", "propagates", "soil_moist", 0.95)

	causalHop, err := client.Causal("soil_dry", "triggers", 5)
	if err != nil || len(causalHop) < 2 {
		log.Fatalf("Causal step failed: %v", err)
	}
	causalChain, err := client.CausalChain([]string{"soil_dry"}, []string{"triggers", "causes", "propagates"}, 5)
	if err != nil || len(causalChain) == 0 {
		log.Fatalf("Causal chain failed: %v", err)
	}
	fmt.Printf("      -> Multi-Branching Mesh Traversal: soil_dry -> triggers (%d branches) -> causes -> sprinkler_zone_a -> propagates -> %s (Score: %.3f) [OK]\n",
		len(causalHop), causalChain[0].Word, causalChain[0].Score)

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
	_ = client.Link("canary_node_primary", "triggers", "canary_branch_a", 0.95)
	_ = client.Link("canary_node_primary", "triggers", "canary_branch_b", 0.90)
	_ = client.Link("canary_branch_a", "propagates", "canary_node_target", 0.95)
	_ = client.Link("canary_branch_b", "propagates", "canary_node_target", 0.85) // convergence

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
	if err != nil || len(causal) < 2 {
		log.Fatalf("Canary causal branches missing after restart: %v", err)
	}
	chain, err := reopenedClient.CausalChain([]string{"canary_node_primary"}, []string{"triggers", "propagates"}, 5)
	if err != nil || len(chain) == 0 {
		log.Fatalf("Canary causal chain missing after restart: %v", err)
	}
	fmt.Printf("      -> Canary Multi-Branch Causal Chain verified: %s -> triggers (%d branches) -> propagates -> %s (Score: %.4f) [OK]\n",
		"canary_node_primary", len(causal), chain[0].Word, chain[0].Score)

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

// -----------------------------------------------------------------------------
// Phase (F): Schema Attachment & Strict Immutability Stress Verification
// -----------------------------------------------------------------------------

func runSchemaStressTest(ctx context.Context) {
	schemaModel := "plant_schema_stress"

	// 1. Initial Attachment
	fmt.Println("[1/6] Attaching strict schema with primitives & 1D array to model...")
	client, err := driver.ConnectRemote(baseURL,
		driver.WithToken(authToken),
		driver.WithModel(schemaModel),
	)
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}
	defer client.Close()

	schema := driver.Schema{
		"voltage":     "float",
		"current":     "float",
		"status":      "int",
		"temperature": "float?",
		"tags":        "string[]?",
	}
	if err := client.Attach(schema); err != nil {
		log.Fatalf("client.Attach failed: %v", err)
	}
	fmt.Println("      -> Schema attached and permanently locked into container engine [OK]")

	// 2. High-Concurrency Valid Writes (5,000 writes across 50 goroutines)
	const (
		numWorkers       = 50
		writesPerWorker  = 100
		totalValidWrites = numWorkers * writesPerWorker
	)
	fmt.Printf("[2/6] Executing %d valid concurrent writes across %d parallel goroutines...\n", totalValidWrites, numWorkers)
	t0 := time.Now()
	var wg sync.WaitGroup
	var validSuccess atomic.Int64
	var validFailures atomic.Int64

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			wClient, wErr := driver.ConnectRemote(baseURL,
				driver.WithToken(authToken),
				driver.WithModel(schemaModel),
			)
			if wErr != nil {
				validFailures.Add(int64(writesPerWorker))
				return
			}
			defer wClient.Close()

			for i := 0; i < writesPerWorker; i++ {
				recName := fmt.Sprintf("meter_valid_%02d_%04d", workerID, i)
				props := map[string]float32{
					"voltage": 220.0 + float32(i%10),
					"current": 5.0 + float32(i%5),
					"status":  1,
				}
				if i%2 == 0 {
					props["temperature"] = 28.5
				}
				if err := wClient.Save(recName, props); err != nil {
					validFailures.Add(1)
				} else {
					validSuccess.Add(1)
				}
			}
		}(w)
	}
	wg.Wait()
	dur := time.Since(t0)
	throughput := float64(totalValidWrites) / dur.Seconds()
	fmt.Printf("      -> Completed in %v (%.0f writes/sec) | Success: %d, Failures: %d [OK]\n",
		dur, throughput, validSuccess.Load(), validFailures.Load())
	if validFailures.Load() > 0 {
		log.Fatalf("unexpected failures during valid schema writes: %d", validFailures.Load())
	}

	// 3. High-Concurrency Invalid Writes Rejection (Missing required fields & Unknown fields)
	const (
		invalidWorkers    = 20
		attemptsPerWorker = 50
		totalInvalid      = invalidWorkers * attemptsPerWorker
	)
	fmt.Printf("[3/6] Stressing fail-fast rejection: sending %d invalid payloads across %d goroutines...\n", totalInvalid, invalidWorkers)
	var rejectedCount atomic.Int64
	var leakedWrites atomic.Int64

	for w := 0; w < invalidWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			wClient, wErr := driver.ConnectRemote(baseURL,
				driver.WithToken(authToken),
				driver.WithModel(schemaModel),
			)
			if wErr != nil {
				return
			}
			defer wClient.Close()

			for i := 0; i < attemptsPerWorker; i++ {
				recName := fmt.Sprintf("meter_invalid_%02d_%04d", workerID, i)
				var props map[string]float32
				if i%2 == 0 {
					// Missing required "voltage"
					props = map[string]float32{
						"current": 5.0,
						"status":  1,
					}
				} else {
					// Unknown extra field "hacker_val"
					props = map[string]float32{
						"voltage":    220.0,
						"current":    5.0,
						"status":     1,
						"hacker_val": 99.9,
					}
				}
				if err := wClient.Save(recName, props); err != nil {
					rejectedCount.Add(1)
				} else {
					leakedWrites.Add(1)
				}
			}
		}(w)
	}
	wg.Wait()
	fmt.Printf("      -> %d out of %d invalid writes rejected immediately (0 leaks) [VERIFIED]\n",
		rejectedCount.Load(), totalInvalid)
	if leakedWrites.Load() > 0 {
		log.Fatalf("SECURITY VIOLATION: %d invalid payloads bypassed schema validator!", leakedWrites.Load())
	}

	// 4. Incompatible Schema Re-Attachment Rejection Under Load
	fmt.Println("[4/6] Verifying schema immutability: re-attaching mismatched schema must fail...")
	conflictingSchema := driver.Schema{
		"voltage": "int", // changed from float
		"current": "float",
		"status":  "int",
	}
	if err := client.Attach(conflictingSchema); err == nil {
		log.Fatalf("Expected schema mismatch rejection, but Attach succeeded!")
	} else {
		fmt.Printf("      -> Rejected as expected: %v [IMMUTABLE]\n", err)
	}

	// 5. ConnectRemote with Matching vs Mismatched Schema
	fmt.Println("[5/6] Testing ConnectRemote WithAttachSchema contract enforcement...")
	// 5a. Matching schema connects cleanly
	goodClient, err := driver.ConnectRemote(baseURL,
		driver.WithToken(authToken),
		driver.WithModel(schemaModel),
		driver.WithAttachSchema(schema),
	)
	if err != nil {
		log.Fatalf("ConnectRemote with matching schema failed: %v", err)
	}
	goodClient.Close()
	fmt.Println("      -> Connect with matching schema succeeded [OK]")

	// 5b. Mismatched schema rejected on connect
	_, err = driver.ConnectRemote(baseURL,
		driver.WithToken(authToken),
		driver.WithModel(schemaModel),
		driver.WithAttachSchema(driver.Schema{"random_field": "string"}),
	)
	if err == nil {
		log.Fatalf("Expected connection rejection on mismatched schema, but succeeded!")
	}
	fmt.Printf("      -> Connect with mismatched schema rejected at handshake: %v [OK]\n", err)

	// 6. Administrative Reset & Re-Attach Under Load
	fmt.Println("[6/6] Testing administrative hard reset (confirm: true)...")
	if err := client.ResetModel(ctx, true); err != nil {
		log.Fatalf("client.ResetModel failed: %v", err)
	}
	// After reset, schema is unmounted
	_, err = client.GetSchema(ctx)
	if err == nil {
		log.Fatalf("Expected GetSchema to return error after reset!")
	}
	// Attach brand new schema
	replacementSchema := driver.Schema{
		"new_metric": "float",
	}
	if err := client.Attach(replacementSchema); err != nil {
		log.Fatalf("Attaching new schema after reset failed: %v", err)
	}
	fmt.Println("      -> Model schema successfully reset and re-attached with new contract [OK]")
}
