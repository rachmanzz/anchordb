package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rachmanzz/anchordb/pkg/driver"
)

// SupplyChainEntity represents a node in the multi-tier supply network.
type SupplyChainEntity struct {
	Name           string
	Tier           float32 // 0: Factory, 1: Tier1, 2: Sub-Assembly, 3: Assembly, 4: Logistics, 5: Retail, 6: Product SKU
	InventoryUnits float32 // Current buffer inventory on-hand
	DailyBurnRate  float32 // Depletion rate (units per day)
	LeadTimeDays   float32 // Re-supply lead time (days)
	IsCritical     float32 // 1.0 = single-source bottleneck, 0.0 = multi-source
}

// CausalLink represents a directed edge with historical disruption transmission score.
type CausalLink struct {
	Source   string
	Relation string
	Target   string
	Weight   float32 // Probability / transmission coefficient (0.0 - 1.0)
}

// DisruptionAnalysisResult combines causal graph reachability with in-storage math.
type DisruptionAnalysisResult struct {
	SKU            string
	Store          string
	DisruptionProb float32
	CurrentStock   float32
	DailyBurn      float32
	DaysRemaining  float32
	IsBottleneck   bool
	StockoutIn30   bool
	RiskScore      float32
}

func main() {
	wireURL := flag.String("url", "wire://localhost:7448", "AnchorWire TCP server URL: wire://host:7448 or localhost:7448")
	authToken := flag.String("token", "", "Bearer auth token for AnchorWire (if required)")
	modelName := flag.String("model", "supply_chain", "Model name to isolate supply chain data")
	scenario := flag.String("scenario", "analysis", "Scenario: 'analysis' (6-hop Vietnam flood query), 'crash-test' (selected restart checks), or 'benchmark' (up to 5M stress test)")
	scale := flag.Int("scale", 100000, "Dataset scale for benchmark scenario (e.g. 100000, 1000000, 5000000)")
	phase := flag.String("phase", "all", "For crash-test: 'populate', 'verify', or 'all' (session reconnect check; no restart)")
	flag.Parse()
	if *scenario == "benchmark" && (*scale < 50000 || *scale > 5000000) {
		fmt.Fprintf(os.Stderr, "benchmark scale must be between 50,000 and 5,000,000 entities (got %d)\n", *scale)
		os.Exit(2)
	}

	fmt.Println("==========================================================================================")
	fmt.Println("       AnchorDB Real-World Case Study: Multi-Tier Supply Chain Disruption Analysis        ")
	fmt.Println("                             (AnchorWire Binary TCP Protocol)                             ")
	fmt.Println("==========================================================================================")
	fmt.Printf("OS / Arch:       %s / %s\n", runtime.GOOS, runtime.GOARCH)
	fmt.Printf("Logical CPUs:    %d cores\n", runtime.NumCPU())
	fmt.Printf("Wire Server:     %s (Model: %s)\n", *wireURL, *modelName)
	fmt.Printf("Active Scenario: %s\n", strings.ToUpper(*scenario))
	fmt.Println("------------------------------------------------------------------------------------------")
	fmt.Println()

	switch *scenario {
	case "analysis":
		runSupplyChainAnalysis(*wireURL, *authToken, *modelName)
	case "crash-test":
		runCrashDurabilityTest(*wireURL, *authToken, *modelName, *phase)
	case "benchmark":
		if err := runLargeScaleBenchmark(*wireURL, *authToken, *modelName, *scale); err != nil {
			fmt.Fprintf(os.Stderr, "FATAL: benchmark failed: %v\n", err)
			os.Exit(1)
		}
	default:
		fmt.Printf("Unknown scenario %q. Available: 'analysis', 'crash-test', 'benchmark'\n", *scenario)
		os.Exit(1)
	}
}

// ------------------------------------------------------------------------------------------------
// SCENARIO 1: Real-World 6-Hop Supply Chain Disruption Analysis
// "If the factory in Vietnam floods, which products run out of stock within 30 days?"
// ------------------------------------------------------------------------------------------------

func runSupplyChainAnalysis(serverURL, authToken, modelName string) {
	ctx := context.Background()
	client, cleanup := setupClient(serverURL, authToken, modelName)
	defer cleanup()

	// 1. Schema Registration & Contract Locking
	fmt.Println(">>> [PHASE 1] Schema Contract Registration & Immutability Enforcement")
	schema := driver.Schema{
		"tier":            "int",   // 0: Factory, 1: T1, 2: T2, 3: Assembly, 4: Logistics, 5: Retail, 6: SKU
		"inventory_units": "float", // Buffer stock on-hand
		"daily_burn_rate": "float", // Daily sales/consumption velocity
		"lead_time_days":  "float", // Re-supply replenishment lead time in days
		"is_critical":     "bool",  // 1.0 (True) = Sole-source bottleneck component
	}

	if err := client.Attach(schema); err != nil {
		fmt.Printf("FATAL: Failed to attach model schema: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("    [✓] Schema attached & locked into AnchorDB engine:")
	for k, v := range schema {
		fmt.Printf("        - %-18s : %s\n", k, v)
	}
	fmt.Println()

	// 2. Multi-Tier Supply Network Entities (Hop 0 to Hop 6)
	fmt.Println(">>> [PHASE 2] Ingesting Multi-Tier Supply Chain Entities (Hop 0 to Hop 6)")
	entities := []SupplyChainEntity{
		// Hop 0: Epicenter (Bac Ninh Factory, Vietnam)
		{"factory_vietnam_bac_ninh", 0, 0.0, 0.0, 90.0, 1.0},

		// Hop 1: Tier-1 Specialized Component Suppliers
		{"supplier_t1_soc_processor", 1, 15000.0, 1200.0, 45.0, 1.0},  // Single-source 3nm Wafer
		{"supplier_t1_oled_display", 1, 28000.0, 1400.0, 30.0, 0.0},   // Dual-source flexible OLED
		{"supplier_t1_periscope_lens", 1, 12000.0, 900.0, 40.0, 1.0},  // High-precision prism optic
		{"supplier_t1_rf_transceiver", 1, 35000.0, 1500.0, 25.0, 0.0}, // 5G Millimeter Wave IC

		// Hop 2: Sub-Assembly Manufacturing Units
		{"subassembly_main_logic_board", 2, 8500.0, 800.0, 21.0, 1.0},  // Assembled PCB with SoC + RF
		{"subassembly_camera_module", 2, 9200.0, 750.0, 18.0, 1.0},     // Optical stabilization module
		{"subassembly_display_chassis", 2, 14000.0, 1100.0, 14.0, 0.0}, // OLED lamination unit

		// Hop 3: Final Production Assembly Plants
		{"assembly_plant_shenzhen", 3, 22000.0, 2500.0, 14.0, 1.0}, // Flagship Smartphone Mega-Factory
		{"assembly_plant_penang", 3, 14000.0, 1200.0, 14.0, 0.0},   // Ultrabook & Laptop Assembly
		{"assembly_plant_chennai", 3, 18000.0, 1500.0, 10.0, 0.0},  // Wearables & Smartwatches

		// Hop 4: Regional Logistics & Shipping Hubs
		{"hub_logistics_apac_singapore", 4, 35000.0, 3000.0, 7.0, 0.0},
		{"hub_logistics_na_los_angeles", 4, 45000.0, 3500.0, 12.0, 1.0},
		{"hub_logistics_eu_rotterdam", 4, 30000.0, 2500.0, 10.0, 0.0},

		// Hop 5: Retail Flagship Stores & Regional Megastores
		{"megastore_tokyo_ginza", 5, 2500.0, 180.0, 5.0, 0.0},
		{"megastore_seattle_flagship", 5, 3200.0, 220.0, 5.0, 0.0},
		{"megastore_frankfurt_center", 5, 2100.0, 150.0, 4.0, 0.0},
		{"megastore_jakarta_grand_indo", 5, 2800.0, 190.0, 5.0, 0.0},
		{"megastore_sydney_central", 5, 1800.0, 130.0, 4.0, 0.0},

		// Hop 6: Final Consumer Product SKUs on Retail Shelves
		// - sku_smartphone_pro_1tb: 240 units / 14 per day = 17.1 days (< 30 days! CRITICAL)
		{"sku_smartphone_pro_1tb", 6, 240.0, 14.0, 35.0, 1.0},
		// - sku_smartphone_base_128gb: 520 units / 20 per day = 26.0 days (< 30 days! CRITICAL)
		{"sku_smartphone_base_128gb", 6, 520.0, 20.0, 25.0, 0.0},
		// - sku_ultrabook_pro_16in: 150 units / 12 per day = 12.5 days (< 30 days! VERY CRITICAL)
		{"sku_ultrabook_pro_16in", 6, 150.0, 12.0, 45.0, 1.0},
		// - sku_tablet_air_11in: 480 units / 10 per day = 48.0 days (> 30 days, SAFE)
		{"sku_tablet_air_11in", 6, 480.0, 10.0, 20.0, 0.0},
		// - sku_smartwatch_ultra_49mm: 650 units / 11 per day = 59.1 days (> 30 days, SAFE)
		{"sku_smartwatch_ultra_49mm", 6, 650.0, 11.0, 15.0, 0.0},
		// - sku_smartwatch_fit_band: 190 units / 9 per day = 21.1 days (< 30 days! CRITICAL)
		{"sku_smartwatch_fit_band", 6, 190.0, 9.0, 28.0, 0.0},
	}

	for _, e := range entities {
		err := client.Save(e.Name, map[string]float32{
			"tier":            e.Tier,
			"inventory_units": e.InventoryUnits,
			"daily_burn_rate": e.DailyBurnRate,
			"lead_time_days":  e.LeadTimeDays,
			"is_critical":     e.IsCritical,
		})
		if err != nil {
			fmt.Printf("FATAL: Failed to save entity %s: %v\n", e.Name, err)
			os.Exit(1)
		}
	}
	fmt.Printf("    [✓] Ingested %d supply chain entities across 7 hierarchical tiers.\n\n", len(entities))

	// 3. Multi-Hop Causal Disruption Links
	fmt.Println(">>> [PHASE 3] Building 6-Hop Causal Disruption Propagation Network")
	fmt.Println("    Disruption Path: Factory (VN) -> Tier1 -> SubAssembly -> Assembly -> Hub -> Store -> SKU")

	links := []CausalLink{
		// Hop 1: supplies_t1 (Factory Flooded -> Tier 1 Component Shortage)
		{"factory_vietnam_bac_ninh", "supplies_t1", "supplier_t1_soc_processor", 0.96},  // Sole factory for 3nm wafer
		{"factory_vietnam_bac_ninh", "supplies_t1", "supplier_t1_oled_display", 0.85},   // Dual source exists
		{"factory_vietnam_bac_ninh", "supplies_t1", "supplier_t1_periscope_lens", 0.92}, // Proprietary coating facility
		{"factory_vietnam_bac_ninh", "supplies_t1", "supplier_t1_rf_transceiver", 0.88}, // High dependency

		// Hop 2: subassembles (Tier 1 Shortage -> Sub-Assembly Disruption)
		{"supplier_t1_soc_processor", "subassembles", "subassembly_main_logic_board", 0.95},
		{"supplier_t1_rf_transceiver", "subassembles", "subassembly_main_logic_board", 0.90},
		{"supplier_t1_periscope_lens", "subassembles", "subassembly_camera_module", 0.92},
		{"supplier_t1_oled_display", "subassembles", "subassembly_display_chassis", 0.88},

		// Hop 3: fabricates (Sub-Assembly Stoppage -> Assembly Plant Halt)
		{"subassembly_main_logic_board", "fabricates", "assembly_plant_shenzhen", 0.98}, // Cannot assemble phone without logic board
		{"subassembly_camera_module", "fabricates", "assembly_plant_shenzhen", 0.90},
		{"subassembly_display_chassis", "fabricates", "assembly_plant_shenzhen", 0.85},
		{"subassembly_main_logic_board", "fabricates", "assembly_plant_penang", 0.92}, // Laptop motherboard dependency
		{"subassembly_camera_module", "fabricates", "assembly_plant_chennai", 0.70},   // Minor dependency for smartwatches

		// Hop 4: transports (Assembly Halt -> Regional Logistics Hub Starvation)
		{"assembly_plant_shenzhen", "transports", "hub_logistics_na_los_angeles", 0.95},
		{"assembly_plant_shenzhen", "transports", "hub_logistics_apac_singapore", 0.92},
		{"assembly_plant_shenzhen", "transports", "hub_logistics_eu_rotterdam", 0.90},
		{"assembly_plant_penang", "transports", "hub_logistics_na_los_angeles", 0.92},
		{"assembly_plant_penang", "transports", "hub_logistics_eu_rotterdam", 0.88},
		{"assembly_plant_chennai", "transports", "hub_logistics_apac_singapore", 0.85},

		// Hop 5: distributes (Logistics Starvation -> Retail Megastore Out-of-Stock)
		{"hub_logistics_na_los_angeles", "distributes", "megastore_seattle_flagship", 0.95},
		{"hub_logistics_apac_singapore", "distributes", "megastore_tokyo_ginza", 0.95},
		{"hub_logistics_apac_singapore", "distributes", "megastore_jakarta_grand_indo", 0.92},
		{"hub_logistics_apac_singapore", "distributes", "megastore_sydney_central", 0.88},
		{"hub_logistics_eu_rotterdam", "distributes", "megastore_frankfurt_center", 0.92},

		// Hop 6: stocks_sku (Retail Megastore -> Final Consumer Product SKUs)
		{"megastore_seattle_flagship", "stocks_sku", "sku_smartphone_pro_1tb", 0.98},
		{"megastore_seattle_flagship", "stocks_sku", "sku_ultrabook_pro_16in", 0.95},
		{"megastore_seattle_flagship", "stocks_sku", "sku_tablet_air_11in", 0.90},

		{"megastore_tokyo_ginza", "stocks_sku", "sku_smartphone_pro_1tb", 0.98},
		{"megastore_tokyo_ginza", "stocks_sku", "sku_smartphone_base_128gb", 0.96},
		{"megastore_tokyo_ginza", "stocks_sku", "sku_smartwatch_ultra_49mm", 0.90},

		{"megastore_jakarta_grand_indo", "stocks_sku", "sku_smartphone_base_128gb", 0.96},
		{"megastore_jakarta_grand_indo", "stocks_sku", "sku_smartwatch_fit_band", 0.94},

		{"megastore_frankfurt_center", "stocks_sku", "sku_smartphone_pro_1tb", 0.95},
		{"megastore_frankfurt_center", "stocks_sku", "sku_ultrabook_pro_16in", 0.92},

		{"megastore_sydney_central", "stocks_sku", "sku_smartwatch_fit_band", 0.90},
		{"megastore_sydney_central", "stocks_sku", "sku_tablet_air_11in", 0.88},
	}

	for _, l := range links {
		if err := client.Link(l.Source, l.Relation, l.Target, l.Weight); err != nil {
			fmt.Printf("FATAL: Failed to link edge (%s -> %s): %v\n", l.Source, l.Target, err)
			os.Exit(1)
		}
	}
	fmt.Printf("    [✓] Connected %d directed weighted edges across 6 relational hops.\n\n", len(links))

	// 4. Executing Multi-Hop Graph Traversal (Hop 1 to Hop 6)
	fmt.Println(">>> [PHASE 4] Executing Multi-Hop Causal Disruption Traversal (Hop 1 to Hop 6)")
	seed := []string{"factory_vietnam_bac_ninh"}
	relationsSequence := []string{
		"supplies_t1",  // Hop 1: Factory -> Tier 1
		"subassembles", // Hop 2: Tier 1 -> SubAssembly
		"fabricates",   // Hop 3: SubAssembly -> Final Assembly
		"transports",   // Hop 4: Assembly -> Logistics Hub
		"distributes",  // Hop 5: Logistics Hub -> Retail Megastore
		"stocks_sku",   // Hop 6: Retail Store -> Product SKU
	}

	// Traverse progressively from 1-hop all the way to 6-hops
	for h := 1; h <= len(relationsSequence); h++ {
		curPath := relationsSequence[:h]
		t0 := time.Now()
		res, err := client.CausalChain(seed, curPath, 20)
		lat := time.Since(t0)
		if err != nil {
			fmt.Printf("Error at hop %d: %v\n", h, err)
			os.Exit(1)
		}
		if len(res) == 0 {
			fmt.Printf("Traversal at hop %d returned no downstream targets\n", h)
			os.Exit(1)
		}

		fmt.Printf("    ▶ Hop %d (%s) -> Reached %d downstream targets [Latency: %v]:\n",
			h, strings.Join(curPath, " → "), len(res), lat)
		for rank, r := range res {
			if rank >= 4 && len(res) > 5 {
				fmt.Printf("       ... and %d more targets\n", len(res)-4)
				break
			}
			fmt.Printf("       [%d] %-30s | Disruption Transmission Probability: %.4f (%.1f%%)\n",
				rank+1, r.Word, r.Score, r.Score*100)
		}
		fmt.Println()
	}

	// 5. Combining Graph Scoring with In-Storage Math:
	// "Which products run out of stock within 30 days?"
	fmt.Println(">>> [PHASE 5] Answering Business Question via Graph Scoring + In-Storage Math")
	fmt.Println("    Question: \"If the factory in Vietnam floods, which products run out of stock within 30 days?\"")
	fmt.Println("    Method: In-Storage Division (inventory_units / daily_burn_rate) on all results returned for the full relation sequence.")
	fmt.Println()

	// Get all entities reached by the full relation sequence with their scores
	skuResults, err := client.CausalChain(seed, relationsSequence, 50)
	if err != nil {
		fmt.Printf("FATAL: Failed to query Hop-6 causal chain: %v\n", err)
		os.Exit(1)
	}
	if len(skuResults) == 0 {
		fmt.Println("FATAL: Hop-6 causal chain returned no entities")
		os.Exit(1)
	}

	var analysisTable []DisruptionAnalysisResult

	for _, skuItem := range skuResults {
		skuName := skuItem.Word
		disruptionProb := skuItem.Score

		// In-Storage Math calculation:
		// Execute Get query evaluating days_remaining = inventory_units / daily_burn_rate
		entityRes, err := client.Get(skuName).
			Select("inventory_units", "daily_burn_rate", "lead_time_days", "is_critical", "days_remaining").
			Compute("days_remaining", driver.Divide("inventory_units", "daily_burn_rate")).
			Execute(ctx)
		if err != nil {
			fmt.Printf("Error evaluating entity %s: %v\n", skuName, err)
			os.Exit(1)
		}

		curStock := entityRes.Get("inventory_units")
		dailyBurn := entityRes.Get("daily_burn_rate")
		daysRem := entityRes.Get("days_remaining")
		isCrit := entityRes.Get("is_critical") == 1.0
		isStockout30 := daysRem <= 30.0

		// Risk Priority Score = (Disruption Probability) * (1 / (days_remaining + 1)) * 100
		riskScore := disruptionProb * (100.0 / (daysRem + 1.0))
		if isCrit {
			riskScore *= 1.5
		}

		analysisTable = append(analysisTable, DisruptionAnalysisResult{
			SKU:            skuName,
			DisruptionProb: disruptionProb,
			CurrentStock:   curStock,
			DailyBurn:      dailyBurn,
			DaysRemaining:  daysRem,
			IsBottleneck:   isCrit,
			StockoutIn30:   isStockout30,
			RiskScore:      riskScore,
		})
	}

	// Sort by Risk Score descending (highest urgency first)
	sort.Slice(analysisTable, func(i, j int) bool {
		return analysisTable[i].RiskScore > analysisTable[j].RiskScore
	})

	// Print executive summary table
	fmt.Println("┌───────────────────────────┬──────────────┬────────────┬──────────────┬─────────────┬──────────────┬──────────────────────────────────┐")
	fmt.Println("│ Reached Entity Name       │ Current Stock│ Burn Rate  │ Days to Empty│ Prob. Impact│ Status       │ Action Recommendation            │")
	fmt.Println("├───────────────────────────┼──────────────┼────────────┼──────────────┼─────────────┼──────────────┼──────────────────────────────────┤")

	for _, item := range analysisTable {
		statusStr := "SAFE (>=30d)"
		actionStr := "Monitor normal replenishment"
		if item.StockoutIn30 {
			statusStr = "ALERT (<=30d)"
			if item.IsBottleneck {
				actionStr = "EMERGENCY: Reroute air freight"
			} else {
				actionStr = "Allocate backup warehouse stock"
			}
		}

		fmt.Printf("│ %-25s │ %10.0f u │ %8.1f/d │ %10.1f d │ %10.1f%% │ %-12s │ %-32s │\n",
			item.SKU,
			item.CurrentStock,
			item.DailyBurn,
			item.DaysRemaining,
			item.DisruptionProb*100,
			statusStr,
			actionStr,
		)
	}
	fmt.Println("└───────────────────────────┴──────────────┴────────────┴──────────────┴─────────────┴──────────────┴──────────────────────────────────┘")
	fmt.Println()

	// Highlight actionable answer to the exact business prompt
	var criticalCount int
	for _, it := range analysisTable {
		if it.StockoutIn30 {
			criticalCount++
		}
	}

	fmt.Println("==========================================================================================")
	fmt.Printf(">>> ANALYSIS FINDINGS: Detected %d reached entities with stockout estimates within 30 days due to Vietnam flood!\n", criticalCount)
	fmt.Println("==========================================================================================")
	for idx, it := range analysisTable {
		if it.StockoutIn30 {
			fmt.Printf(" [%d] %s:\n", idx+1, it.SKU)
			fmt.Printf("     - Days of Stock Remaining: %.1f DAYS (Stock: %.0f units, Burn Rate: %.1f units/day)\n",
				it.DaysRemaining, it.CurrentStock, it.DailyBurn)
			fmt.Printf("     - Supply Chain Disruption Probability: %.2f%%\n", it.DisruptionProb*100)
			fmt.Printf("     - Component Criticality: %v (Composite Risk Score: %.2f)\n",
				map[bool]string{true: "SINGLE SOURCE BOTTLENECK", false: "DUAL SOURCE"}[it.IsBottleneck], it.RiskScore)
			fmt.Println()
		}
	}
}

// ------------------------------------------------------------------------------------------------
// SCENARIO 2: Selected Restart Checks
// ------------------------------------------------------------------------------------------------

func runCrashDurabilityTest(serverURL, authToken, modelName, phase string) {
	fmt.Println("==========================================================================================")
	fmt.Println("   SCENARIO: Selected Post-Restart Checks on Multi-Hop Supply Network                       ")
	fmt.Println("==========================================================================================")

	canaryKey := "canary_supply_chain_sentinel"
	canaryProps := map[string]float32{
		"tier":            0.0,
		"inventory_units": 999999.0,
		"daily_burn_rate": 1.0,
		"lead_time_days":  1.0,
		"is_critical":     1.0,
	}

	switch phase {
	case "populate":
		client, cleanup := setupClient(serverURL, authToken, modelName)
		defer cleanup()

		fmt.Println("[STEP 1/3] Populating Schema, Selected Entities, One Causal Link, and Sentinel Canary via AnchorWire...")
		schema := driver.Schema{
			"tier":            "int",
			"inventory_units": "float",
			"daily_burn_rate": "float",
			"lead_time_days":  "float",
			"is_critical":     "bool",
		}
		if err := client.Attach(schema); err != nil {
			fmt.Printf("Attach error: %v\n", err)
			os.Exit(1)
		}
		if err := client.Save("factory_vietnam_bac_ninh", map[string]float32{
			"tier": 0, "inventory_units": 100000, "daily_burn_rate": 500,
			"lead_time_days": 14, "is_critical": 1,
		}); err != nil {
			fmt.Printf("Save factory node error: %v\n", err)
			os.Exit(1)
		}
		if err := client.Save("supplier_t1_soc_processor", map[string]float32{
			"tier": 1, "inventory_units": 50000, "daily_burn_rate": 250,
			"lead_time_days": 10, "is_critical": 1,
		}); err != nil {
			fmt.Printf("Save supplier node error: %v\n", err)
			os.Exit(1)
		}
		if err := client.Save(canaryKey, canaryProps); err != nil {
			fmt.Printf("Save canary error: %v\n", err)
			os.Exit(1)
		}
		if err := client.Link("factory_vietnam_bac_ninh", "supplies_t1", "supplier_t1_soc_processor", 0.96); err != nil {
			fmt.Printf("Link error: %v\n", err)
			os.Exit(1)
		}
		if err := client.Sync(); err != nil {
			fmt.Printf("Sync error: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("    [✓] Selected sample data written and synced via AnchorWire. You may now stop and restart the container normally.")

	case "verify":
		client, cleanup := setupClient(serverURL, authToken, modelName)
		defer cleanup()

		fmt.Println("[STEP 2/3] Verifying Selected Data & Graph Integrity After Restart via AnchorWire...")
		// 1. Schema must remain locked
		sch, err := client.GetSchema(context.Background())
		expectedSchema := driver.Schema{
			"tier": "int", "inventory_units": "float", "daily_burn_rate": "float",
			"lead_time_days": "float", "is_critical": "bool",
		}
		if err != nil || len(sch) != len(expectedSchema) {
			fmt.Printf("FATAL: Schema missing or incomplete after restart: %v\n", err)
			os.Exit(1)
		}
		for field, want := range expectedSchema {
			if sch[field] != want {
				fmt.Printf("FATAL: Schema mismatch for %q: got %q, want %q\n", field, sch[field], want)
				os.Exit(1)
			}
		}
		fmt.Printf("    [✓] Expected schema fields present: %d\n", len(sch))

		// 2. Canary entity must be intact
		res, err := client.Get(canaryKey).Execute(context.Background())
		if err != nil || res.Get("inventory_units") != 999999.0 {
			fmt.Printf("FATAL: Canary record corrupt or missing: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("    [✓] Canary record verified intact: inventory_units = %.0f\n", res.Get("inventory_units"))

		// 3. Multi-hop edge must be traversable
		causalRes, err := client.Causal("factory_vietnam_bac_ninh", "supplies_t1", 5)
		if err != nil || len(causalRes) == 0 {
			fmt.Printf("FATAL: Expected causal edge missing after restart: %v\n", err)
			os.Exit(1)
		}
		foundExpectedEdge := false
		for _, result := range causalRes {
			if result.Word == "supplier_t1_soc_processor" && result.Score == 0.96 {
				foundExpectedEdge = true
				break
			}
		}
		if !foundExpectedEdge {
			fmt.Println("FATAL: Expected supplier_t1_soc_processor edge and score were not found")
			os.Exit(1)
		}
		fmt.Printf("    [✓] Causal graph verified: %s -> %s (Score: %.4f)\n",
			"factory_vietnam_bac_ninh", "supplier_t1_soc_processor", float32(0.96))

		fmt.Println("\n==========================================================================================")
		fmt.Println("    >>> POST-RESTART SAMPLE CHECKS PASSED (no full-dataset comparison performed)")
		fmt.Println("==========================================================================================")

	case "all":
		fmt.Println("[STEP 1/3] Connecting via AnchorWire & populating data...")
		c1, cleanup1 := setupClient(serverURL, authToken, modelName)

		schema := driver.Schema{
			"tier":            "int",
			"inventory_units": "float",
			"daily_burn_rate": "float",
			"lead_time_days":  "float",
			"is_critical":     "bool",
		}
		if err := c1.Attach(schema); err != nil {
			cleanup1()
			fmt.Printf("Attach error: %v\n", err)
			os.Exit(1)
		}
		if err := c1.Save("factory_vietnam_bac_ninh", map[string]float32{
			"tier": 0, "inventory_units": 100000, "daily_burn_rate": 500,
			"lead_time_days": 14, "is_critical": 1,
		}); err != nil {
			cleanup1()
			fmt.Printf("Save factory node error: %v\n", err)
			os.Exit(1)
		}
		if err := c1.Save("supplier_t1_soc_processor", map[string]float32{
			"tier": 1, "inventory_units": 50000, "daily_burn_rate": 250,
			"lead_time_days": 10, "is_critical": 1,
		}); err != nil {
			cleanup1()
			fmt.Printf("Save supplier node error: %v\n", err)
			os.Exit(1)
		}
		if err := c1.Save(canaryKey, canaryProps); err != nil {
			cleanup1()
			fmt.Printf("Save canary error: %v\n", err)
			os.Exit(1)
		}
		if err := c1.Link("factory_vietnam_bac_ninh", "supplies_t1", "supplier_t1_soc_processor", 0.96); err != nil {
			cleanup1()
			fmt.Printf("Link error: %v\n", err)
			os.Exit(1)
		}
		if err := c1.Sync(); err != nil {
			cleanup1()
			fmt.Printf("Sync error: %v\n", err)
			os.Exit(1)
		}

		// Disconnect client to test durability across sessions
		fmt.Println("[STEP 2/3] Closing AnchorWire connection (session reconnect check; no server restart)...")
		cleanup1()

		fmt.Println("[STEP 3/3] Reconnecting via AnchorWire & verifying data integrity...")
		c2, cleanup2 := setupClient(serverURL, authToken, modelName)
		defer cleanup2()

		sch, err := c2.GetSchema(context.Background())
		expectedSchema := driver.Schema{
			"tier": "int", "inventory_units": "float", "daily_burn_rate": "float",
			"lead_time_days": "float", "is_critical": "bool",
		}
		if err != nil || len(sch) != len(expectedSchema) {
			fmt.Printf("FATAL: Schema lost after session reconnect: %v\n", err)
			os.Exit(1)
		}
		for field, want := range expectedSchema {
			if sch[field] != want {
				fmt.Printf("FATAL: Schema mismatch for %q after session reconnect: got %q, want %q\n", field, sch[field], want)
				os.Exit(1)
			}
		}
		cRes, err := c2.Get(canaryKey).Execute(context.Background())
		if err != nil || cRes.Get("inventory_units") != 999999.0 {
			fmt.Printf("FATAL: Canary entity corrupted: %v\n", err)
			os.Exit(1)
		}
		graphRes, err := c2.Causal("factory_vietnam_bac_ninh", "supplies_t1", 5)
		if err != nil || len(graphRes) == 0 {
			fmt.Printf("FATAL: Graph edges lost: %v\n", err)
			os.Exit(1)
		}
		foundExpectedEdge := false
		for _, result := range graphRes {
			if result.Word == "supplier_t1_soc_processor" && result.Score == float32(0.96) {
				foundExpectedEdge = true
				break
			}
		}
		if !foundExpectedEdge {
			fmt.Println("FATAL: Expected supplier_t1_soc_processor edge and score were not found after session reconnect")
			os.Exit(1)
		}

		fmt.Println("    [✓] Schema matched expected fields after session reconnect")
		fmt.Println("    [✓] Canary record matched expected value after session reconnect")
		fmt.Println("    [✓] Causal edge was traversable after session reconnect")
		fmt.Println("\n==========================================================================================")
		fmt.Println("    >>> SESSION RECONNECT CHECKS PASSED (no server restart performed)")
		fmt.Println("==========================================================================================")
	default:
		fmt.Printf("Unknown crash-test phase %q; expected populate, verify, or all\n", phase)
		os.Exit(2)
	}
}

// ------------------------------------------------------------------------------------------------
// SCENARIO 3: Large-Scale Benchmark Stress Test (Up to 5,000,000 Entities/Edges)
// ------------------------------------------------------------------------------------------------

func runLargeScaleBenchmark(serverURL, authToken, modelName string, totalRecords int) error {
	fmt.Println("==========================================================================================")
	fmt.Printf("   SCENARIO: Large-Scale Benchmark Stress Test (%s entities)\n", formatNumber(totalRecords))
	fmt.Println("==========================================================================================")

	client, cleanup := setupClient(serverURL, authToken, modelName)
	defer cleanup()

	// 1. Attach Schema
	schema := driver.Schema{
		"tier":            "int",
		"inventory_units": "float",
		"daily_burn_rate": "float",
		"lead_time_days":  "float",
		"is_critical":     "bool",
	}
	if err := client.Attach(schema); err != nil {
		return fmt.Errorf("attach benchmark schema: %w", err)
	}

	// 2. High-Throughput Streaming Ingestion
	fmt.Printf("[1/3] Streaming %s entities in high-speed batches...\n", formatNumber(totalRecords))
	t0 := time.Now()

	var counter atomic.Int64
	numWorkers := runtime.NumCPU() * 2
	if numWorkers < 4 {
		numWorkers = 4
	}

	recordsPerWorker := totalRecords / numWorkers
	remainder := totalRecords % numWorkers
	var wg sync.WaitGroup
	var errorCount atomic.Int64

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		workerID := w
		go func() {
			defer wg.Done()
			startIdx := workerID * recordsPerWorker
			if workerID < remainder {
				startIdx += workerID
			} else {
				startIdx += remainder
			}
			count := recordsPerWorker
			if workerID < remainder {
				count++
			}
			endIdx := startIdx + count

			for i := startIdx; i < endIdx; i++ {
				entityName := fmt.Sprintf("sku_auto_%08d", i)
				props := map[string]float32{
					"tier":            6.0,
					"inventory_units": float32(100 + (i % 500)),
					"daily_burn_rate": float32(5 + (i % 20)),
					"lead_time_days":  float32(10 + (i % 30)),
					"is_critical":     float32(i % 2),
				}
				if err := client.Save(entityName, props); err != nil {
					errs := errorCount.Add(1)
					if errs <= 5 {
						fmt.Printf("Worker %d error saving %s: %v\n", workerID, entityName, err)
					}
					continue
				}
				done := counter.Add(1)
				if done%100000 == 0 || done == int64(totalRecords) {
					elapsed := time.Since(t0).Seconds()
					fmt.Printf("      -> Progress: %s / %s records (%.0f records/sec) [Elapsed: %v]\n",
						formatNumber(int(done)), formatNumber(totalRecords), float64(done)/elapsed, time.Since(t0).Truncate(time.Second))
				}
			}
		}()
	}
	wg.Wait()
	durIngest := time.Since(t0)
	savedCount := counter.Load()
	totalErrors := errorCount.Load()
	throughput := float64(savedCount) / durIngest.Seconds()
	if totalErrors > 0 {
		fmt.Printf("    [!] Ingestion Completed with %d ERRORS: %s / %s records saved in %v (%.0f writes/sec)\n\n",
			totalErrors, formatNumber(int(savedCount)), formatNumber(totalRecords), durIngest, throughput)
		return fmt.Errorf("ingestion failed: %d of %d writes failed; %d saved", totalErrors, totalRecords, savedCount)
	}
	fmt.Printf("    [✓] Ingestion Completed: %s records in %v (%.0f writes/sec)\n\n",
		formatNumber(int(savedCount)), durIngest, throughput)

	// 3. Connect Mesh Causal Chains (Multi-tier edges)
	const numEdges = 50000
	fmt.Printf("[2/3] Constructing %s multi-hop causal mesh edges...\n", formatNumber(numEdges))
	tEdge := time.Now()

	// Pre-register the 100 regional logistics hub nodes so that graph edges connect valid entity records
	fmt.Println("    -> Pre-registering 100 regional logistics hub nodes...")
	for h := 0; h < 100; h++ {
		hubName := fmt.Sprintf("hub_logistics_%04d", h)
		hubProps := map[string]float32{
			"tier":            4.0, // Logistics tier
			"inventory_units": 50000.0,
			"daily_burn_rate": 3500.0,
			"lead_time_days":  7.0,
			"is_critical":     float32(h % 2),
		}
		if err := client.Save(hubName, hubProps); err != nil {
			fmt.Printf("Warning: Failed to create hub %s: %v\n", hubName, err)
			return fmt.Errorf("save logistics hub %s: %w", hubName, err)
		}
	}

	var edgeErrors atomic.Int64
	var edgeWg sync.WaitGroup
	edgesPerWorker := numEdges / numWorkers
	edgeRemainder := numEdges % numWorkers

	for w := 0; w < numWorkers; w++ {
		edgeWg.Add(1)
		workerID := w
		go func() {
			defer edgeWg.Done()
			startIdx := workerID * edgesPerWorker
			if workerID < edgeRemainder {
				startIdx += workerID
			} else {
				startIdx += edgeRemainder
			}
			count := edgesPerWorker
			if workerID < edgeRemainder {
				count++
			}
			endIdx := startIdx + count

			for i := startIdx; i < endIdx; i++ {
				src := fmt.Sprintf("hub_logistics_%04d", i%100)
				target := fmt.Sprintf("sku_auto_%08d", i)
				if err := client.Link(src, "stocks_sku", target, 0.85+float32(i%15)*0.01); err != nil {
					errCount := edgeErrors.Add(1)
					if errCount <= 5 {
						fmt.Printf("Worker %d error linking %s -> %s: %v\n", workerID, src, target, err)
					}
				}
			}
		}()
	}
	edgeWg.Wait()
	errs := edgeErrors.Load()
	linked := int64(numEdges) - errs
	fmt.Printf("    Linked %s / %s edges (%d errors) in %v (%.0f successful edges/sec)\n\n",
		formatNumber(int(linked)), formatNumber(numEdges), errs, time.Since(tEdge), float64(linked)/time.Since(tEdge).Seconds())
	if errs > 0 {
		return fmt.Errorf("%d of %d causal edge writes failed", errs, numEdges)
	}

	// 4. Measure Query Latency at the configured entity scale
	fmt.Println("[3/3] Measuring Multi-Hop Query Latency Under Scale...")
	const benchmarkQueries = 20000
	tQuery := time.Now()
	ctx := context.Background()

	var querySuccess atomic.Int64
	var queryErrors atomic.Int64
	for i := 0; i < benchmarkQueries; i++ {
		src := fmt.Sprintf("hub_logistics_%04d", i%100)
		res, err := client.Causal(src, "stocks_sku", 5)
		if err != nil {
			queryErrors.Add(1)
		} else if len(res) > 0 {
			querySuccess.Add(1)
		}
	}
	durQuery := time.Since(tQuery)
	qps := float64(benchmarkQueries) / durQuery.Seconds()
	avgLat := (durQuery.Seconds() * 1e6) / float64(benchmarkQueries)
	successCount := int(querySuccess.Load())

	emptyResults := benchmarkQueries - successCount - int(queryErrors.Load())
	fmt.Printf("    Executed %s causal traversals (%s non-empty, %d empty, %d errors) in %v\n",
		formatNumber(benchmarkQueries), formatNumber(successCount), emptyResults, queryErrors.Load(), durQuery)
	fmt.Printf("    Traversal Throughput: %.0f queries/sec\n", qps)
	fmt.Printf("    [✓] Average Latency:      %.2f µs / query\n", avgLat)

	// 5. In-Storage Math at Scale
	tMath := time.Now()
	_, err := client.Get("sku_auto_00000001").
		Compute("days_remaining", driver.Divide("inventory_units", "daily_burn_rate")).
		Execute(ctx)
	if err != nil {
		return fmt.Errorf("point compute query: %w", err)
	}
	fmt.Printf("    Point Compute Latency: %v\n", time.Since(tMath))
	if queryErrors.Load() > 0 || successCount != benchmarkQueries {
		return fmt.Errorf("causal traversal incomplete: %d non-empty, %d empty, %d errors out of %d", successCount, emptyResults, queryErrors.Load(), benchmarkQueries)
	}
	fmt.Printf("    Query check passed: %d / %d causal queries returned results with no errors\n", successCount, benchmarkQueries)

	fmt.Println("\n==========================================================================================")
	fmt.Println("    >>> LARGE-SCALE WORKLOAD COMPLETED; all configured writes/queries reported success")
	fmt.Println("==========================================================================================")
	return nil
}

// ------------------------------------------------------------------------------------------------
// HELPER FUNCTIONS
// ------------------------------------------------------------------------------------------------

func setupClient(serverURL, authToken, modelName string) (*driver.Client, func()) {
	target := serverURL
	if strings.HasPrefix(target, "http://") || strings.HasPrefix(target, "https://") {
		fmt.Printf("FATAL: HTTP is disabled. Only AnchorWire binary TCP (wire://localhost:7448) is supported.\n")
		os.Exit(1)
	}
	if !strings.HasPrefix(target, "wire://") && !strings.HasPrefix(target, "tcp://") {
		target = "wire://" + target
	}

	opts := []driver.Option{
		driver.WithModel(modelName),
	}
	if authToken != "" {
		opts = append(opts, driver.WithToken(authToken))
	}

	client, err := driver.ConnectWire(target, opts...)
	if err != nil {
		fmt.Printf("FATAL: Failed to connect to AnchorWire server (%s): %v\nMake sure AnchorDB is running with AnchorWire listening on port 7448.\n", target, err)
		os.Exit(1)
	}
	return client, func() {
		if err := client.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "AnchorWire client close failed: %v\n", err)
		}
	}
}

func formatNumber(n int) string {
	in := fmt.Sprintf("%d", n)
	out := ""
	for i := len(in) - 1; i >= 0; i-- {
		out = string(in[i]) + out
		if (len(in)-i)%3 == 0 && i != 0 {
			out = "." + out
		}
	}
	return out
}
