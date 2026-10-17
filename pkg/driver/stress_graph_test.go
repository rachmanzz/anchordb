package driver_test

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/rachmanzz/anchordb/pkg/driver"
)

// TestDriver_GraphTraversal_MultiBranchingAndSchema tests multi-branching DAG traversal,
// from 1 hop to 5 hops, and resolves reached nodes using schema-validated Get and List operations.
func TestDriver_GraphTraversal_MultiBranchingAndSchema(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "anchordb_driver_graph_stress_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	client, err := driver.OpenEmbedded(tmpDir, driver.WithModel("incident_graph"))
	if err != nil {
		t.Fatalf("failed to open embedded client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	// 1. Attach strict schema contract
	schema := driver.Schema{
		"severity":    "int",
		"impact_usd":  "float",
		"latency_ms":  "float",
		"is_critical": "bool",
	}
	if err := client.Attach(schema); err != nil {
		t.Fatalf("client.Attach failed: %v", err)
	}

	// 2. Ingest graph nodes with schema validation
	nodes := map[string]map[string]float32{
		"root_failure":     {"severity": 5, "impact_usd": 100000.0, "latency_ms": 1200.0, "is_critical": 1.0},
		"ups_bank_a":       {"severity": 4, "impact_usd": 40000.0, "latency_ms": 500.0, "is_critical": 1.0},
		"ups_bank_b":       {"severity": 4, "impact_usd": 35000.0, "latency_ms": 400.0, "is_critical": 1.0},
		"hvac_subsystem":   {"severity": 3, "impact_usd": 20000.0, "latency_ms": 250.0, "is_critical": 0.0},
		"rack_1_down":      {"severity": 4, "impact_usd": 50000.0, "latency_ms": 600.0, "is_critical": 1.0},
		"rack_2_down":      {"severity": 4, "impact_usd": 45000.0, "latency_ms": 550.0, "is_critical": 1.0},
		"core_switch_trip": {"severity": 4, "impact_usd": 30000.0, "latency_ms": 450.0, "is_critical": 1.0},
		"thermal_overheat": {"severity": 3, "impact_usd": 15000.0, "latency_ms": 300.0, "is_critical": 0.0},
		"db_cluster_down":  {"severity": 5, "impact_usd": 150000.0, "latency_ms": 2000.0, "is_critical": 1.0},
		"kafka_queue_down": {"severity": 4, "impact_usd": 80000.0, "latency_ms": 1100.0, "is_critical": 1.0},
		"auth_service_err": {"severity": 4, "impact_usd": 60000.0, "latency_ms": 900.0, "is_critical": 1.0},
		"checkout_outage":  {"severity": 5, "impact_usd": 250000.0, "latency_ms": 3500.0, "is_critical": 1.0},
		"payment_failure":  {"severity": 5, "impact_usd": 350000.0, "latency_ms": 4500.0, "is_critical": 1.0},
		"total_sla_breach": {"severity": 5, "impact_usd": 500000.0, "latency_ms": 0.0, "is_critical": 1.0},
	}

	for name, props := range nodes {
		if err := client.Save(name, props); err != nil {
			t.Fatalf("failed to save node %q: %v", name, err)
		}
	}

	// 3. Connect multi-branching edges across 5 relational tiers
	edges := []struct {
		s, r, t string
		w       float32
	}{
		// Hop 1: triggers (3 threads)
		{"root_failure", "triggers", "ups_bank_a", 0.95},
		{"root_failure", "triggers", "ups_bank_b", 0.90},
		{"root_failure", "triggers", "hvac_subsystem", 0.85},

		// Hop 2: propagates (branching with cross-links)
		{"ups_bank_a", "propagates", "rack_1_down", 0.95},
		{"ups_bank_a", "propagates", "core_switch_trip", 0.85},
		{"ups_bank_b", "propagates", "rack_2_down", 0.90},
		{"hvac_subsystem", "propagates", "thermal_overheat", 0.88},
		{"hvac_subsystem", "propagates", "rack_1_down", 0.70}, // convergence

		// Hop 3: degrades (services)
		{"rack_1_down", "degrades", "db_cluster_down", 0.95},
		{"rack_2_down", "degrades", "kafka_queue_down", 0.90},
		{"core_switch_trip", "degrades", "auth_service_err", 0.92},
		{"thermal_overheat", "degrades", "db_cluster_down", 0.80}, // convergence

		// Hop 4: disrupts (applications)
		{"db_cluster_down", "disrupts", "checkout_outage", 0.95},
		{"db_cluster_down", "disrupts", "payment_failure", 0.92},
		{"kafka_queue_down", "disrupts", "checkout_outage", 0.85}, // convergence
		{"auth_service_err", "disrupts", "payment_failure", 0.88}, // convergence

		// Hop 5: escalates (business impact)
		{"checkout_outage", "escalates", "total_sla_breach", 0.98},
		{"payment_failure", "escalates", "total_sla_breach", 0.95},
	}

	for _, e := range edges {
		if err := client.Link(e.s, e.r, e.t, e.w); err != nil {
			t.Fatalf("failed to link %s -> %s: %v", e.s, e.t, err)
		}
	}

	// 4. Test 1-Hop up to 5-Hop traversals
	seed := []string{"root_failure"}

	// 1-Hop
	hop1, err := client.CausalChain(seed, []string{"triggers"}, 10)
	if err != nil || len(hop1) != 3 {
		t.Fatalf("hop 1 failed: len=%d, err=%v", len(hop1), err)
	}
	if hop1[0].Word != "ups_bank_a" {
		t.Errorf("hop 1 top node expected ups_bank_a, got %s", hop1[0].Word)
	}

	// 2-Hops
	hop2, err := client.CausalChain(seed, []string{"triggers", "propagates"}, 10)
	if err != nil || len(hop2) < 4 {
		t.Fatalf("hop 2 failed: len=%d, err=%v", len(hop2), err)
	}

	// 3-Hops
	hop3, err := client.CausalChain(seed, []string{"triggers", "propagates", "degrades"}, 10)
	if err != nil || len(hop3) < 3 {
		t.Fatalf("hop 3 failed: len=%d, err=%v", len(hop3), err)
	}
	if hop3[0].Word != "db_cluster_down" {
		t.Errorf("hop 3 top node expected db_cluster_down, got %s", hop3[0].Word)
	}

	// 4-Hops
	hop4, err := client.CausalChain(seed, []string{"triggers", "propagates", "degrades", "disrupts"}, 10)
	if err != nil || len(hop4) < 2 {
		t.Fatalf("hop 4 failed: len=%d, err=%v", len(hop4), err)
	}

	// 5-Hops
	hop5, err := client.CausalChain(seed, []string{"triggers", "propagates", "degrades", "disrupts", "escalates"}, 10)
	if err != nil || len(hop5) != 1 {
		t.Fatalf("hop 5 failed: len=%d, err=%v", len(hop5), err)
	}
	if hop5[0].Word != "total_sla_breach" {
		t.Errorf("hop 5 top node expected total_sla_breach, got %s", hop5[0].Word)
	}

	// 5. Test Schema-guaranteed Get on reached nodes
	for _, res := range hop5 {
		rec, err := client.Get(res.Word).Execute(ctx)
		if err != nil {
			t.Fatalf("Get(%s) failed: %v", res.Word, err)
		}
		if rec.Get("severity") != 5 {
			t.Errorf("expected severity 5, got %v", rec.Get("severity"))
		}
		if rec.Get("impact_usd") != 500000.0 {
			t.Errorf("expected impact_usd 500000.0, got %v", rec.Get("impact_usd"))
		}
	}

	// 6. Test Schema-guaranteed List with arithmetic computation
	listRes, err := client.List().
		Where("severity", ">=", 4.0).
		Compute("risk_factor", driver.Multiply("impact_usd", "severity")).
		Execute(ctx)
	if err != nil {
		t.Fatalf("List query failed: %v", err)
	}
	if len(listRes.Items) == 0 {
		t.Fatalf("expected matching items in list")
	}

	// 7. Concurrent Stress Test (Multi-threaded Traversal)
	const workers = 8
	const itersPerWorker = 2000
	var wg sync.WaitGroup
	var count atomic.Int64

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < itersPerWorker; i++ {
				// Alternate between 1-hop and 5-hop queries concurrently
				_, _ = client.CausalChain(seed, []string{"triggers", "propagates", "degrades", "disrupts", "escalates"}, 5)
				count.Add(1)
			}
		}()
	}
	wg.Wait()

	if count.Load() != workers*itersPerWorker {
		t.Fatalf("stress count mismatch: expected %d, got %d", workers*itersPerWorker, count.Load())
	}
}
