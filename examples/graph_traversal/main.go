package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rachmanzz/anchordb/pkg/driver"
)

// GraphNode defines an infrastructure node in our failure-propagation scenario.
type GraphNode struct {
	Name       string
	Severity   float32
	ImpactUSD  float32
	LatencyMs  float32
	IsCritical float32
}

func main() {
	mode := flag.String("mode", "embedded", "Execution mode: 'embedded' (in-process engine), 'wire' (AnchorWire binary TCP protocol on port 7448), or 'remote' (HTTP engine on port 7447)")
	serverURL := flag.String("url", "http://localhost:7447", "AnchorDB server URL (used in wire/remote mode): wire://host:7448 or http(s)://host:7447")
	flag.Parse()

	fmt.Println("==================================================================================")
	fmt.Println("       AnchorDB Graph Traversal vs Schema: Multi-Thread (Mesh) Stress Test        ")
	fmt.Println("==================================================================================")
	fmt.Printf("OS/Arch:       %s/%s\n", runtime.GOOS, runtime.GOARCH)
	fmt.Printf("Logical CPUs:  %d cores\n", runtime.NumCPU())
	fmt.Println("----------------------------------------------------------------------------------")
	fmt.Println()

	var client *driver.Client
	var cleanup func()

	if *mode == "wire" {
		if *serverURL == "http://localhost:7447" || *serverURL == "" {
			*serverURL = "wire://localhost:7448"
		}
		*mode = "remote"
	}

	if *mode == "remote" {
		c, err := driver.Connect(*serverURL, driver.WithModel("infra_causal"))
		if err != nil {
			fmt.Printf("Failed to connect to %s: %v\n", *serverURL, err)
			return
		}
		client = c
		cleanup = func() { _ = client.Close() }
	} else {
		tmpDir, err := os.MkdirTemp("", "anchordb_graph_stress_*")
		if err != nil {
			fmt.Printf("Failed to create temp dir: %v\n", err)
			return
		}
		c, err := driver.OpenEmbedded(tmpDir, driver.WithModel("infra_causal"))
		if err != nil {
			fmt.Printf("Failed to open embedded engine: %v\n", err)
			_ = os.RemoveAll(tmpDir)
			return
		}
		client = c
		cleanup = func() {
			_ = client.Close()
			_ = os.RemoveAll(tmpDir)
		}
	}
	defer cleanup()

	ctx := context.Background()

	// -------------------------------------------------------------------------
	// 1. Schema Registration (Contract Enforcement)
	// -------------------------------------------------------------------------
	fmt.Println(">>> [PHASE 1] Schema Registration & Contract Locking")
	schema := driver.Schema{
		"severity":    "int",   // Required integer: 1 (info) to 5 (critical)
		"impact_usd":  "float", // Required float: economic impact
		"latency_ms":  "float", // Required float: latency impact
		"is_critical": "bool",  // Required bool: 1.0 (true) or 0.0 (false)
		"tier":        "int?",  // Optional int tag
	}

	if err := client.Attach(schema); err != nil {
		fmt.Printf("Schema attach failed: %v\n", err)
		return
	}
	fmt.Println("    ✓ Schema successfully locked to model 'infra_causal'")
	gotSchema, _ := client.GetSchema(ctx)
	fmt.Printf("    ✓ Active Schema Contract: %+v\n\n", gotSchema)

	// -------------------------------------------------------------------------
	// 2. Multi-Branching Nodes Generation & Schema Validation
	// -------------------------------------------------------------------------
	fmt.Println(">>> [PHASE 2] Ingesting Graph Nodes (Validated against Schema)")
	nodes := []GraphNode{
		// Seed (Hop 0)
		{"dc_power_trip", 5, 250000.0, 1500.0, 1.0},

		// Hop 1 (Subsystems - 4 threads)
		{"ups_alpha_failure", 5, 120000.0, 850.0, 1.0},
		{"ups_beta_failure", 4, 90000.0, 600.0, 1.0},
		{"hvac_cooling_shutdown", 4, 75000.0, 450.0, 1.0},
		{"aux_bus_overload", 3, 40000.0, 300.0, 0.0},

		// Hop 2 (Infrastructure Components - Multi-branching & Convergence)
		{"rack_alpha_blackout", 5, 80000.0, 950.0, 1.0},
		{"rack_beta_blackout", 4, 65000.0, 700.0, 1.0},
		{"spine_switch_restart", 4, 50000.0, 800.0, 1.0},
		{"thermal_throttling_core", 3, 35000.0, 500.0, 0.0},
		{"cooling_loop_failure", 4, 45000.0, 400.0, 1.0},
		{"san_storage_bus_fault", 5, 95000.0, 1200.0, 1.0},
		{"pdu_subpanel_blown", 3, 30000.0, 250.0, 0.0},

		// Hop 3 (Core Software Services)
		{"db_primary_crash", 5, 180000.0, 2500.0, 1.0},
		{"db_replica_desync", 4, 60000.0, 800.0, 1.0},
		{"kafka_broker_loss", 5, 110000.0, 1400.0, 1.0},
		{"redis_cluster_eviction", 3, 25000.0, 350.0, 0.0},
		{"san_io_lockup", 5, 140000.0, 3000.0, 1.0},
		{"ingress_api_timeout", 4, 55000.0, 900.0, 1.0},

		// Hop 4 (Customer-Facing Applications)
		{"checkout_api_down", 5, 300000.0, 5000.0, 1.0},
		{"payment_gateway_v2_fail", 5, 450000.0, 6000.0, 1.0},
		{"inventory_sync_hang", 3, 40000.0, 1200.0, 0.0},
		{"auth_session_invalidation", 4, 85000.0, 1800.0, 1.0},
		{"streaming_telemetry_loss", 2, 15000.0, 400.0, 0.0},

		// Hop 5 (Business & Regulatory Outcomes)
		{"sla_penalty_breach", 5, 750000.0, 0.0, 1.0},
		{"direct_revenue_loss", 5, 980000.0, 0.0, 1.0},
		{"customer_churn_escalation", 4, 320000.0, 0.0, 1.0},
		{"regulatory_audit_alert", 5, 500000.0, 0.0, 1.0},
		{"brand_reputation_damage", 4, 250000.0, 0.0, 0.0},
	}

	for _, n := range nodes {
		props := map[string]float32{
			"severity":    n.Severity,
			"impact_usd":  n.ImpactUSD,
			"latency_ms":  n.LatencyMs,
			"is_critical": n.IsCritical,
		}
		if err := client.Save(n.Name, props); err != nil {
			fmt.Printf("Save failed for %s: %v\n", n.Name, err)
			return
		}
	}
	fmt.Printf("    ✓ Ingested %d nodes with strict schema verification.\n", len(nodes))

	// Verify Schema Enforcement: attempt invalid write
	invalidProps := map[string]float32{
		"severity":   2.5, // Invalid: int schema rejects non-integer!
		"impact_usd": 100.0,
	}
	if err := client.Save("invalid_test_node", invalidProps); err != nil {
		fmt.Printf("    ✓ Schema Guard Check Passed: Rejection of invalid int property: %v\n\n", err)
	}

	// -------------------------------------------------------------------------
	// 3. Multi-Threaded Causal Edge Network (Multi-Branching Mesh DAG)
	// -------------------------------------------------------------------------
	fmt.Println(">>> [PHASE 3] Building Multi-Branching Causal Edge Network (Mesh DAG)")
	type Edge struct {
		Src, Rel, Dst string
		W             float32
	}

	edges := []Edge{
		// Hop 1: triggers (Fan-out into 4 concurrent threads)
		{"dc_power_trip", "triggers", "ups_alpha_failure", 0.95},
		{"dc_power_trip", "triggers", "ups_beta_failure", 0.90},
		{"dc_power_trip", "triggers", "hvac_cooling_shutdown", 0.85},
		{"dc_power_trip", "triggers", "aux_bus_overload", 0.80},

		// Hop 2: propagates (Branching + Re-convergent mesh)
		{"ups_alpha_failure", "propagates", "rack_alpha_blackout", 0.95},
		{"ups_alpha_failure", "propagates", "spine_switch_restart", 0.85},
		{"ups_beta_failure", "propagates", "rack_beta_blackout", 0.90},
		{"ups_beta_failure", "propagates", "san_storage_bus_fault", 0.85},
		{"hvac_cooling_shutdown", "propagates", "thermal_throttling_core", 0.90},
		{"hvac_cooling_shutdown", "propagates", "cooling_loop_failure", 0.80},
		{"aux_bus_overload", "propagates", "pdu_subpanel_blown", 0.90},
		// Convergence / Cross-edge: aux_bus also threatens rack_beta
		{"aux_bus_overload", "propagates", "rack_beta_blackout", 0.75},

		// Hop 3: degrades (Convergence on critical shared databases & buses)
		{"rack_alpha_blackout", "degrades", "db_primary_crash", 0.95},
		{"rack_alpha_blackout", "degrades", "kafka_broker_loss", 0.90},
		{"rack_beta_blackout", "degrades", "db_replica_desync", 0.90},
		{"rack_beta_blackout", "degrades", "redis_cluster_eviction", 0.85},
		{"spine_switch_restart", "degrades", "ingress_api_timeout", 0.92},
		{"san_storage_bus_fault", "degrades", "san_io_lockup", 0.95},
		// Convergence: storage fault also causes db_primary_crash
		{"san_storage_bus_fault", "degrades", "db_primary_crash", 0.88},
		{"thermal_throttling_core", "degrades", "ingress_api_timeout", 0.78},

		// Hop 4: disrupts (Convergence on core business flows)
		{"db_primary_crash", "disrupts", "checkout_api_down", 0.98},
		{"db_primary_crash", "disrupts", "payment_gateway_v2_fail", 0.95},
		{"san_io_lockup", "disrupts", "payment_gateway_v2_fail", 0.92},
		{"kafka_broker_loss", "disrupts", "inventory_sync_hang", 0.90},
		{"kafka_broker_loss", "disrupts", "streaming_telemetry_loss", 0.85},
		{"ingress_api_timeout", "disrupts", "checkout_api_down", 0.88},
		{"redis_cluster_eviction", "disrupts", "auth_session_invalidation", 0.90},
		{"db_replica_desync", "disrupts", "inventory_sync_hang", 0.80},

		// Hop 5: escalates (Final enterprise impact outcomes)
		{"payment_gateway_v2_fail", "escalates", "direct_revenue_loss", 0.98},
		{"payment_gateway_v2_fail", "escalates", "sla_penalty_breach", 0.95},
		{"checkout_api_down", "escalates", "direct_revenue_loss", 0.95},
		{"checkout_api_down", "escalates", "customer_churn_escalation", 0.90},
		{"auth_session_invalidation", "escalates", "customer_churn_escalation", 0.85},
		{"payment_gateway_v2_fail", "escalates", "regulatory_audit_alert", 0.90},
		{"direct_revenue_loss", "escalates", "brand_reputation_damage", 0.75},
	}

	for _, e := range edges {
		if err := client.Link(e.Src, e.Rel, e.Dst, e.W); err != nil {
			fmt.Printf("Link error (%s->%s): %v\n", e.Src, e.Dst, err)
			return
		}
	}
	fmt.Printf("    ✓ Successfully connected %d directed causal edges across 5 relational tiers.\n\n", len(edges))

	// -------------------------------------------------------------------------
	// 4. Multi-Hop Traversal Verification (1-Hop to 5-Hops)
	// -------------------------------------------------------------------------
	fmt.Println(">>> [PHASE 4] Executing Graph Traversal from 1-Hop to 5-Hops")
	seed := []string{"dc_power_trip"}

	hopRels := [][]string{
		{"triggers"},                                                    // 1-Hop
		{"triggers", "propagates"},                                      // 2-Hops
		{"triggers", "propagates", "degrades"},                          // 3-Hops
		{"triggers", "propagates", "degrades", "disrupts"},              // 4-Hops
		{"triggers", "propagates", "degrades", "disrupts", "escalates"}, // 5-Hops
	}

	for i, rels := range hopRels {
		hopNum := i + 1
		t0 := time.Now()
		res, err := client.CausalChain(seed, rels, 10)
		lat := time.Since(t0)
		if err != nil {
			fmt.Printf("Error at hop %d: %v\n", hopNum, err)
			continue
		}

		fmt.Printf("    ▶ Hop Depth %d (%s) -> Reached %d nodes [Latency: %v]:\n",
			hopNum, fmt.Sprintf("%v", rels), len(res), lat)

		for rank, r := range res {
			fmt.Printf("       [%d] Node: %-28s | Final Score: %.4f\n", rank+1, r.Word, r.Score)
		}
		fmt.Println()
	}

	// -------------------------------------------------------------------------
	// 5. Stress Test: High-Concurrency Multi-Threaded Traversal
	// -------------------------------------------------------------------------
	fmt.Println(">>> [PHASE 5] Stress Testing Graph Traversal Under Concurrent Multi-Thread Load")
	concurrencyLevels := []int{1, 4, 16, 64}
	const totalQueriesPerLevel = 100_000

	for _, workers := range concurrencyLevels {
		var wg sync.WaitGroup
		var completed atomic.Int64
		queriesPerWorker := totalQueriesPerLevel / workers

		t0 := time.Now()
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				// Each worker executes traversals across random hop depths (1 to 5)
				for q := 0; q < queriesPerWorker; q++ {
					depth := (q % 5) + 1
					rels := hopRels[depth-1]
					_, err := client.CausalChain(seed, rels, 5)
					if err != nil {
						fmt.Printf("Worker error: %v\n", err)
						return
					}
					completed.Add(1)
				}
			}()
		}
		wg.Wait()
		duration := time.Since(t0)
		opsPerSec := float64(completed.Load()) / duration.Seconds()
		nsPerOp := float64(duration.Nanoseconds()) / float64(completed.Load())

		fmt.Printf("    * Workers: %2d | Completed: %d queries | Total Time: %8v | Latency: %6.2f µs/op | Throughput: %8.0f ops/sec\n",
			workers, completed.Load(), duration, nsPerOp/1000.0, opsPerSec)
	}
	fmt.Println()

	// -------------------------------------------------------------------------
	// 6. Schema-Enforced Entity Retrieval (Get & List)
	// -------------------------------------------------------------------------
	fmt.Println(">>> [PHASE 6] Schema-Enforced Entity Resolution ('Get' & 'List')")
	fmt.Println("    Demonstrating that nodes discovered via Graph Traversal are strictly validated by Schema.")
	fmt.Println()

	// A. Individual GET resolution on Top Impacted Nodes
	fmt.Println("    --- [A] Individual `client.Get(node)` on Top Discovered Causal Targets ---")
	topNodes := []string{"payment_gateway_v2_fail", "db_primary_crash", "direct_revenue_loss", "sla_penalty_breach"}

	fmt.Printf("    %-28s | %-8s | %-12s | %-12s | %-11s\n", "Entity Name", "Severity", "Impact (USD)", "Latency (ms)", "Is Critical")
	fmt.Printf("    %s\n", "---------------------------------------------------------------------------------")
	for _, name := range topNodes {
		rec, err := client.Get(name).Execute(ctx)
		if err != nil {
			fmt.Printf("    Error getting %s: %v\n", name, err)
			continue
		}
		fmt.Printf("    %-28s | %-8.0f | $%-11.2f | %-12.1f | %-11.0f\n",
			rec.Name,
			rec.Get("severity"),
			rec.Get("impact_usd"),
			rec.Get("latency_ms"),
			rec.Get("is_critical"),
		)
	}
	fmt.Println()

	// B. Filtered LIST resolution with In-Storage Math
	fmt.Println("    --- [B] `client.List()` with Filter & In-Storage Arithmetic Computation ---")
	fmt.Println("    Query: List all nodes where severity >= 4, computing risk_index = (impact_usd * severity)")

	listRes, err := client.List().
		Where("severity", ">=", 4.0).
		Compute("risk_index", driver.Multiply("impact_usd", "severity")).
		Page(1, 15).
		Execute(ctx)

	if err != nil {
		fmt.Printf("    List query error: %v\n", err)
		return
	}

	fmt.Printf("    Matching Entities Count: %d (HasMore: %v)\n\n", len(listRes.Items), listRes.HasMore)
	fmt.Printf("    %-28s | %-8s | %-12s | %-14s | %-11s\n", "Entity Name", "Severity", "Impact (USD)", "Risk Index (Math)", "Is Critical")
	fmt.Printf("    %s\n", "----------------------------------------------------------------------------------------")
	for _, item := range listRes.Items {
		fmt.Printf("    %-28s | %-8.0f | $%-11.2f | $%-13.2f | %-11.0f\n",
			item.Name,
			item.Get("severity"),
			item.Get("impact_usd"),
			item.Get("risk_index"),
			item.Get("is_critical"),
		)
	}

	fmt.Println("\n==================================================================================")
	fmt.Println("               Graph Traversal & Schema Stress Test Completed!                    ")
	fmt.Println("==================================================================================")
}
