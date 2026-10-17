package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/rachmanzz/anchordb/pkg/driver"
)

type GraphNode struct {
	Name       string
	Severity   float32
	ImpactUSD  float32
	LatencyMs  float32
	IsCritical float32
}

func main() {
	phase := flag.String("phase", "populate", "Execution phase: 'populate' or 'verify'")
	url := flag.String("url", "http://localhost:7447", "AnchorDB server URL")
	flag.Parse()

	ctx := context.Background()

	client, err := driver.ConnectRemote(*url, driver.WithModel("infra_causal"))
	if err != nil {
		fmt.Printf("Connection error to %s: %v\n", *url, err)
		os.Exit(1)
	}
	defer client.Close()

	seed := []string{"dc_power_trip"}
	hopRels := [][]string{
		{"triggers"},
		{"triggers", "propagates"},
		{"triggers", "propagates", "degrades"},
		{"triggers", "propagates", "degrades", "disrupts"},
		{"triggers", "propagates", "degrades", "disrupts", "escalates"},
	}

	switch *phase {
	case "populate":
		fmt.Println("==========================================================================")
		fmt.Println("   PODMAN STEP 1: Populating Schema, Multi-Branch Graph, and Initial Run   ")
		fmt.Println("==========================================================================")

		// 1. Attach Schema Contract
		schema := driver.Schema{
			"severity":    "int",
			"impact_usd":  "float",
			"latency_ms":  "float",
			"is_critical": "bool",
			"tier":        "int?",
		}
		if err := client.Attach(schema); err != nil {
			fmt.Printf("client.Attach error: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("    [✓] Schema attached and locked to model 'infra_causal' via REST API")

		// 2. Ingest 28 Nodes
		nodes := []GraphNode{
			{"dc_power_trip", 5, 250000.0, 1500.0, 1.0},
			{"ups_alpha_failure", 5, 120000.0, 850.0, 1.0},
			{"ups_beta_failure", 4, 90000.0, 600.0, 1.0},
			{"hvac_cooling_shutdown", 4, 75000.0, 450.0, 1.0},
			{"aux_bus_overload", 3, 40000.0, 300.0, 0.0},
			{"rack_alpha_blackout", 5, 80000.0, 950.0, 1.0},
			{"rack_beta_blackout", 4, 65000.0, 700.0, 1.0},
			{"spine_switch_restart", 4, 50000.0, 800.0, 1.0},
			{"thermal_throttling_core", 3, 35000.0, 500.0, 0.0},
			{"cooling_loop_failure", 4, 45000.0, 400.0, 1.0},
			{"san_storage_bus_fault", 5, 95000.0, 1200.0, 1.0},
			{"pdu_subpanel_blown", 3, 30000.0, 250.0, 0.0},
			{"db_primary_crash", 5, 180000.0, 2500.0, 1.0},
			{"db_replica_desync", 4, 60000.0, 800.0, 1.0},
			{"kafka_broker_loss", 5, 110000.0, 1400.0, 1.0},
			{"redis_cluster_eviction", 3, 25000.0, 350.0, 0.0},
			{"san_io_lockup", 5, 140000.0, 3000.0, 1.0},
			{"ingress_api_timeout", 4, 55000.0, 900.0, 1.0},
			{"checkout_api_down", 5, 300000.0, 5000.0, 1.0},
			{"payment_gateway_v2_fail", 5, 450000.0, 6000.0, 1.0},
			{"inventory_sync_hang", 3, 40000.0, 1200.0, 0.0},
			{"auth_session_invalidation", 4, 85000.0, 1800.0, 1.0},
			{"streaming_telemetry_loss", 2, 15000.0, 400.0, 0.0},
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
				os.Exit(1)
			}
		}
		fmt.Printf("    [✓] Ingested %d nodes into Podman container with schema validation\n", len(nodes))

		// 3. Connect 35 Directed Edges (Multi-Branching DAG)
		type Edge struct {
			Src, Rel, Dst string
			W             float32
		}
		edges := []Edge{
			{"dc_power_trip", "triggers", "ups_alpha_failure", 0.95},
			{"dc_power_trip", "triggers", "ups_beta_failure", 0.90},
			{"dc_power_trip", "triggers", "hvac_cooling_shutdown", 0.85},
			{"dc_power_trip", "triggers", "aux_bus_overload", 0.80},

			{"ups_alpha_failure", "propagates", "rack_alpha_blackout", 0.95},
			{"ups_alpha_failure", "propagates", "spine_switch_restart", 0.85},
			{"ups_beta_failure", "propagates", "rack_beta_blackout", 0.90},
			{"ups_beta_failure", "propagates", "san_storage_bus_fault", 0.85},
			{"hvac_cooling_shutdown", "propagates", "thermal_throttling_core", 0.90},
			{"hvac_cooling_shutdown", "propagates", "cooling_loop_failure", 0.80},
			{"aux_bus_overload", "propagates", "pdu_subpanel_blown", 0.90},
			{"aux_bus_overload", "propagates", "rack_beta_blackout", 0.75},

			{"rack_alpha_blackout", "degrades", "db_primary_crash", 0.95},
			{"rack_alpha_blackout", "degrades", "kafka_broker_loss", 0.90},
			{"rack_beta_blackout", "degrades", "db_replica_desync", 0.90},
			{"rack_beta_blackout", "degrades", "redis_cluster_eviction", 0.85},
			{"spine_switch_restart", "degrades", "ingress_api_timeout", 0.92},
			{"san_storage_bus_fault", "degrades", "san_io_lockup", 0.95},
			{"san_storage_bus_fault", "degrades", "db_primary_crash", 0.88},
			{"thermal_throttling_core", "degrades", "ingress_api_timeout", 0.78},

			{"db_primary_crash", "disrupts", "checkout_api_down", 0.98},
			{"db_primary_crash", "disrupts", "payment_gateway_v2_fail", 0.95},
			{"san_io_lockup", "disrupts", "payment_gateway_v2_fail", 0.92},
			{"kafka_broker_loss", "disrupts", "inventory_sync_hang", 0.90},
			{"kafka_broker_loss", "disrupts", "streaming_telemetry_loss", 0.85},
			{"ingress_api_timeout", "disrupts", "checkout_api_down", 0.88},
			{"redis_cluster_eviction", "disrupts", "auth_session_invalidation", 0.90},
			{"db_replica_desync", "disrupts", "inventory_sync_hang", 0.80},

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
				fmt.Printf("Link error %s->%s: %v\n", e.Src, e.Dst, err)
				os.Exit(1)
			}
		}
		fmt.Printf("    [✓] Linked %d directed causal edges across 5 relational tiers\n\n", len(edges))

		// 4. Initial 1-5 Hop Traversal
		fmt.Println("    --- Initial Traversal Verification (Pre-Shutdown) ---")
		for i, rels := range hopRels {
			hopNum := i + 1
			res, err := client.CausalChain(seed, rels, 5)
			if err != nil {
				fmt.Printf("CausalChain hop %d error: %v\n", hopNum, err)
				os.Exit(1)
			}
			if len(res) == 0 {
				fmt.Printf("    [X] CausalChain hop %d (%s) returned 0 nodes before restart\n", hopNum, rels[len(rels)-1])
				os.Exit(1)
			}
			fmt.Printf("    Hop %d (%s) -> Reached %d nodes | Top: %-26s (Score: %.4f)\n",
				hopNum, rels[len(rels)-1], len(res), res[0].Word, res[0].Score)
		}
		fmt.Println()

	case "verify":
		fmt.Println("==========================================================================")
		fmt.Println("   PODMAN STEP 3: Verifying Schema, Traversal, Get & List After Restart   ")
		fmt.Println("==========================================================================")

		// 1. Verify Schema Persistence
		gotSchema, err := client.GetSchema(ctx)
		if err != nil {
			fmt.Printf("    [X] GetSchema failed after restart: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("    [✓] Schema Contract Restored from disk: %+v\n\n", gotSchema)

		// 2. Verify 1-Hop to 5-Hop Causal Chain Traversal
		fmt.Println("    --- [A] Causal Traversal Verification Across All Hops (Post-Restart) ---")
		for i, rels := range hopRels {
			hopNum := i + 1
			res, err := client.CausalChain(seed, rels, 5)
			if err != nil {
				fmt.Printf("    [X] CausalChain hop %d error after restart: %v\n", hopNum, err)
				os.Exit(1)
			}
			if len(res) == 0 {
				fmt.Printf("    [X] CausalChain hop %d returned 0 nodes after restart\n", hopNum)
				os.Exit(1)
			}
			fmt.Printf("    ▶ Hop %d (%-10s) -> %d nodes reached. Top Target: %-26s | Score: %.4f\n",
				hopNum, rels[len(rels)-1], len(res), res[0].Word, res[0].Score)
			for rIdx, r := range res {
				fmt.Printf("       [%d] %-28s (Score: %.4f)\n", rIdx+1, r.Word, r.Score)
			}
			fmt.Println()
		}

		// 3. Verify Schema-Enforced GET Resolution
		fmt.Println("    --- [B] Schema-Enforced GET Resolution on Top Discovered Nodes ---")
		topNodes := []string{"payment_gateway_v2_fail", "db_primary_crash", "direct_revenue_loss", "sla_penalty_breach"}
		fmt.Printf("    %-28s | %-8s | %-12s | %-12s | %-11s\n", "Entity Name", "Severity", "Impact (USD)", "Latency (ms)", "Is Critical")
		fmt.Printf("    %s\n", "---------------------------------------------------------------------------------")
		for _, name := range topNodes {
			rec, err := client.Get(name).Execute(ctx)
			if err != nil {
				fmt.Printf("    [X] Get(%s) failed after restart: %v\n", name, err)
				os.Exit(1)
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

		// 4. Verify Schema-Enforced LIST Resolution with In-Storage Compute
		fmt.Println("    --- [C] Schema-Enforced LIST Query with In-Storage Math (Post-Restart) ---")
		listRes, err := client.List().
			Where("severity", ">=", 4.0).
			Compute("risk_index", driver.Multiply("impact_usd", "severity")).
			Page(1, 10).
			Execute(ctx)
		if err != nil {
			fmt.Printf("    List query error: %v\n", err)
			os.Exit(1)
		}
		if len(listRes.Items) == 0 {
			fmt.Printf("    [X] List query returned 0 items after restart\n")
			os.Exit(1)
		}

		fmt.Printf("    Total matching items returned: %d (HasMore: %v)\n", len(listRes.Items), listRes.HasMore)
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
		fmt.Println("\n==========================================================================")
		fmt.Println("   PODMAN CONTAINER DURABILITY VERIFICATION: 100% PASSED!                  ")
		fmt.Println("==========================================================================")
	default:
		fmt.Printf("Unknown phase %q; expected populate or verify\n", *phase)
		os.Exit(1)
	}
}
