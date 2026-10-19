// Package main demonstrates real-time banking fraud analysis with AnchorDB.
package main

import (
	"context"
	"flag"
	"fmt"
	"math"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rachmanzz/anchordb/pkg/driver"
	"github.com/rachmanzz/anchordb/pkg/spatial"
)

// BankAccountEntity models a retail or commercial bank account in AnchorDB.
type BankAccountEntity struct {
	AccountNumber   string
	RiskScore       float32 // Continuous rating: 0.0 (safe) to 100.0 (high risk)
	LastTxTimestamp float32 // Seconds since the sample epoch (float32 precision-safe)
	DailyTxVolume   float32 // Cumulative transfer volume today in USD
	IsBlacklisted   float32 // 1.0 = on financial crimes / mule watchlist, 0.0 = clean
	KYCTier         float32 // 1: basic digital, 2: biometric verified, 3: enterprise KYC
}

// TransactionLink represents a fund transfer between accounts with anomaly risk weight.
type TransactionLink struct {
	SourceAccount string
	Relation      string
	TargetAccount string
	RiskWeight    float32 // Historical anomaly probability (0.0 to 1.0)
}

// FraudDecision contains the real-time pre-authorization evaluation result.
type FraudDecision struct {
	TargetAccount string
	PathScore     float32
	IsBlacklisted bool
	RiskScore     float32
	Decision      string
	Latency       time.Duration
}

func main() {
	wireURL := flag.String("url", "wire://localhost:7448", "AnchorWire TCP server URL: wire://host:7448 or localhost:7448")
	authToken := flag.String("token", "", "Bearer auth token for AnchorWire (if required)")
	modelName := flag.String("model", "banking_fraud", "Model name to isolate banking fraud data")
	scenario := flag.String("scenario", "all", "Scenario: 'all', 'mule-tracking', 'impossible-travel', or 'scale-benchmark'")
	scale := flag.Int("scale", 1000000, "Dataset scale for benchmark (minimum 1,000,000 accounts)")
	flag.Parse()

	if (*scenario == "scale-benchmark" || *scenario == "all") && (*scale < 1000000 || *scale > 5000000) {
		if *scale > 0 && *scale < 1000000 {
			fmt.Printf("Notice: Enforcing minimum dataset size of 1,000,000 records (specified: %d)\n", *scale)
			*scale = 1000000
		} else {
			fmt.Fprintf(os.Stderr, "scale must be between 1,000,000 and 5,000,000 accounts (got %d)\n", *scale)
			os.Exit(2)
		}
	}
	if *scenario != "all" && *scenario != "mule-tracking" && *scenario != "impossible-travel" && *scenario != "scale-benchmark" {
		fmt.Fprintf(os.Stderr, "unknown scenario %q; expected all, mule-tracking, impossible-travel, or scale-benchmark\n", *scenario)
		os.Exit(2)
	}

	fmt.Println("==========================================================================================")
	fmt.Println("       AnchorDB Banking Fraud Detection & Real-Time AML Prevention Engine                 ")
	fmt.Println("                             (AnchorWire Binary TCP Protocol)                             ")
	fmt.Println("==========================================================================================")
	fmt.Printf("OS / Arch:       %s / %s\n", runtime.GOOS, runtime.GOARCH)
	fmt.Printf("Logical CPUs:    %d cores\n", runtime.NumCPU())
	fmt.Printf("Wire Server:     %s (Model: %s)\n", *wireURL, *modelName)
	fmt.Printf("Target Scenario: %s\n", strings.ToUpper(*scenario))
	if *scenario == "all" || *scenario == "scale-benchmark" {
		fmt.Printf("Configured Scale: %s accounts (used by scale-benchmark scenario)\n", formatNumber(*scale))
	}
	fmt.Println("------------------------------------------------------------------------------------------")
	fmt.Println()

	client, cleanup := setupClient(*wireURL, *authToken, *modelName)
	defer cleanup()

	// 1. Schema Registration & Strict Contract Locking
	fmt.Println(">>> [PHASE 1] Registering & Locking Bank Account Risk Schema")
	schema := driver.Schema{
		"risk_score":        "float", // Continuous risk rating: 0.0 (safe) - 100.0 (critical)
		"last_tx_timestamp": "float", // Seconds since sample epoch (float32 precision-safe)
		"daily_tx_volume":   "float", // Cumulative daily transfer total in USD
		"is_blacklisted":    "bool",  // 1.0 (True) = in financial crimes watchlist
		"kyc_tier":          "int",   // 1 (basic) to 3 (fully verified)
	}

	if err := client.Attach(schema); err != nil {
		fmt.Printf("FATAL: Failed to attach banking schema: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("    [✓] Schema attached and locked into model %q:\n", *modelName)
	for k, v := range schema {
		fmt.Printf("        - %-20s : %s\n", k, v)
	}
	fmt.Println()

	ctx := context.Background()

	// Execute Scenarios
	if *scenario == "all" || *scenario == "mule-tracking" {
		if err := runMuleTrackingScenario(ctx, client); err != nil {
			fmt.Printf("FATAL: mule-tracking scenario failed: %v\n", err)
			os.Exit(1)
		}
	}

	if *scenario == "all" || *scenario == "impossible-travel" {
		if err := runImpossibleTravelScenario(ctx, client); err != nil {
			fmt.Printf("FATAL: impossible-travel scenario failed: %v\n", err)
			os.Exit(1)
		}
	}

	if *scenario == "all" || *scenario == "scale-benchmark" {
		if err := runScaleBenchmarkScenario(ctx, client, *scale); err != nil {
			fmt.Printf("FATAL: scale-benchmark scenario failed: %v\n", err)
			os.Exit(1)
		}
	}
}

// ------------------------------------------------------------------------------------------------
// SCENARIO 1: Multi-Hop Money Mule & Layered Smurfing Detection
// ------------------------------------------------------------------------------------------------

func runMuleTrackingScenario(ctx context.Context, client *driver.Client) error {
	fmt.Println("==========================================================================================")
	fmt.Println(">>> [SCENARIO 1] Real-Time Multi-Hop Mule Account & Smurfing Layering Detection")
	fmt.Println("    Goal: Trace stolen funds flowing across 5 hops from victim to offshore cashout.")
	fmt.Println("    Example target: Pre-Authorization decision in < 1 millisecond (< 1,000 µs).")
	fmt.Println("==========================================================================================")

	// Ingest representative network accounts
	accounts := []BankAccountEntity{
		// Compromised Victim Account (Hop 0)
		{"acc_victim_retail_1092", 5.0, 0.0, 15000.0, 0.0, 2.0},

		// Tier-1 Rapid Mule Relay (Hop 1)
		{"acc_mule_t1_relay_alpha", 65.0, 60.0, 14800.0, 0.0, 1.0},
		{"acc_mule_t1_relay_beta", 60.0, 75.0, 12000.0, 0.0, 1.0},

		// Tier-2 Aggregation Accounts (Hop 2)
		{"acc_mule_t2_aggregator_east", 78.0, 120.0, 45000.0, 0.0, 1.0},
		{"acc_mule_t2_aggregator_west", 72.0, 150.0, 38000.0, 0.0, 1.0},

		// Tier-3 Structured Smurfing Nodes (Hop 3)
		{"acc_mule_t3_smurf_node_01", 85.0, 200.0, 9500.0, 0.0, 1.0},
		{"acc_mule_t3_smurf_node_02", 82.0, 210.0, 9200.0, 0.0, 1.0},
		{"acc_mule_t3_smurf_node_03", 80.0, 220.0, 8900.0, 0.0, 1.0},

		// Tier-4 P2P Crypto Exchange Escrow Mules (Hop 4)
		{"acc_mule_t4_p2p_escrow_a", 92.0, 300.0, 85000.0, 0.0, 1.0},
		{"acc_mule_t4_p2p_escrow_b", 89.0, 320.0, 62000.0, 0.0, 1.0},

		// Tier-5 Blacklisted Cash-Out Destination (Hop 5)
		{"acc_offshore_crypto_drain", 99.5, 400.0, 500000.0, 1.0, 1.0}, // BLACKLISTED
		{"acc_legitimate_merchant_safe", 2.0, 450.0, 4200.0, 0.0, 3.0}, // Clean merchant
	}

	for _, acc := range accounts {
		err := client.Save(acc.AccountNumber, map[string]float32{
			"risk_score":        acc.RiskScore,
			"last_tx_timestamp": acc.LastTxTimestamp,
			"daily_tx_volume":   acc.DailyTxVolume,
			"is_blacklisted":    acc.IsBlacklisted,
			"kyc_tier":          acc.KYCTier,
		})
		if err != nil {
			fmt.Printf("Error saving account %s: %v\n", acc.AccountNumber, err)
			return fmt.Errorf("save account %s: %w", acc.AccountNumber, err)
		}
	}
	fmt.Printf("    [✓] Ingested %d bank accounts across 6 laundering layers.\n", len(accounts))

	// Connect fund transfer transactions with risk transmission weights
	transfers := []TransactionLink{
		// Hop 1: Rapid Fan-Out from Victim
		{"acc_victim_retail_1092", "transfers_to", "acc_mule_t1_relay_alpha", 0.98},
		{"acc_victim_retail_1092", "transfers_to", "acc_mule_t1_relay_beta", 0.92},

		// Hop 2: Aggregation
		{"acc_mule_t1_relay_alpha", "transfers_to", "acc_mule_t2_aggregator_east", 0.95},
		{"acc_mule_t1_relay_beta", "transfers_to", "acc_mule_t2_aggregator_west", 0.90},

		// Hop 3: Smurfing Split (< $10,000 reporting threshold)
		{"acc_mule_t2_aggregator_east", "transfers_to", "acc_mule_t3_smurf_node_01", 0.94},
		{"acc_mule_t2_aggregator_east", "transfers_to", "acc_mule_t3_smurf_node_02", 0.91},
		{"acc_mule_t2_aggregator_west", "transfers_to", "acc_mule_t3_smurf_node_03", 0.89},

		// Hop 4: Re-converge into P2P Crypto Escrow Mules
		{"acc_mule_t3_smurf_node_01", "transfers_to", "acc_mule_t4_p2p_escrow_a", 0.96},
		{"acc_mule_t3_smurf_node_02", "transfers_to", "acc_mule_t4_p2p_escrow_a", 0.93},
		{"acc_mule_t3_smurf_node_03", "transfers_to", "acc_mule_t4_p2p_escrow_b", 0.90},

		// Hop 5: Offshore Cashout Destination
		{"acc_mule_t4_p2p_escrow_a", "transfers_to", "acc_offshore_crypto_drain", 0.99}, // High-risk drain
		{"acc_mule_t4_p2p_escrow_b", "transfers_to", "acc_legitimate_merchant_safe", 0.40},
	}

	for _, tx := range transfers {
		if err := client.Link(tx.SourceAccount, tx.Relation, tx.TargetAccount, tx.RiskWeight); err != nil {
			return fmt.Errorf("link transfer %s -> %s: %w", tx.SourceAccount, tx.TargetAccount, err)
		}
	}
	fmt.Printf("    [✓] Linked %d directed fund transfer edges across 5 relational hops.\n\n", len(transfers))

	// Real-Time Pre-Authorization Traversal
	fmt.Println(">>> Executing Real-Time Pre-Authorization Multi-Hop Causal Inspection:")
	seed := []string{"acc_victim_retail_1092"}
	hopSequence := []string{
		"transfers_to", // Hop 1: Victim -> T1 Relay
		"transfers_to", // Hop 2: T1 -> T2 Aggregator
		"transfers_to", // Hop 3: T2 -> T3 Smurf
		"transfers_to", // Hop 4: T3 -> T4 P2P Escrow
		"transfers_to", // Hop 5: T4 -> Terminal Cashout
	}

	t0 := time.Now()
	chainResults, err := client.CausalChain(seed, hopSequence, 10)
	traversalLat := time.Since(t0)
	if err != nil {
		fmt.Printf("FATAL: CausalChain failed: %v\n", err)
		return fmt.Errorf("causal-chain query: %w", err)
	}
	if len(chainResults) == 0 {
		return fmt.Errorf("causal-chain query returned no downstream accounts")
	}

	fmt.Printf("    [✓] 5-Hop Causal Chain Completed in %v\n\n", traversalLat)

	// Decision Evaluation Table
	var decisions []FraudDecision
	var lookupErrors int
	for _, res := range chainResults {
		targetAcc := res.Word
		pathScore := res.Score

		tDec := time.Now()
		accProps, err := client.Get(targetAcc).
			Select("risk_score", "daily_tx_volume", "is_blacklisted", "kyc_tier").
			Execute(ctx)
		decLat := time.Since(tDec)

		if err != nil {
			lookupErrors++
			fmt.Printf("Account lookup failed for %s: %v\n", targetAcc, err)
			continue
		}

		isBlacklisted := accProps.Get("is_blacklisted") == 1.0
		riskScore := accProps.Get("risk_score")

		decisionStr := "APPROVE"
		if isBlacklisted || (pathScore > 0.60 && riskScore > 80.0) {
			decisionStr = "BLOCK & FREEZE (AML VIOLATION)"
		} else if pathScore > 0.40 {
			decisionStr = "FLAG FOR MANUAL REVIEW"
		}

		decisions = append(decisions, FraudDecision{
			TargetAccount: targetAcc,
			PathScore:     pathScore,
			IsBlacklisted: isBlacklisted,
			RiskScore:     riskScore,
			Decision:      decisionStr,
			Latency:       traversalLat + decLat,
		})
	}
	if lookupErrors > 0 || len(decisions) != len(chainResults) {
		return fmt.Errorf("evaluated %d of %d downstream accounts (%d lookup errors)", len(decisions), len(chainResults), lookupErrors)
	}

	fmt.Println("┌──────────────────────────────┬──────────────┬────────────┬────────────┬──────────────────────────────┬──────────────┐")
	fmt.Println("│ Downstream Account (Hop 5)   │ Path Prob.   │ Risk Score │ Watchlist? │ Pre-Authorization Decision   │ Decision Time│")
	fmt.Println("├──────────────────────────────┼──────────────┼────────────┼────────────┼──────────────────────────────┼──────────────┤")
	for _, d := range decisions {
		isWatchlist := "NO"
		if d.IsBlacklisted {
			isWatchlist = "YES (ALERT)"
		}
		fmt.Printf("│ %-28s │ %10.2f%% │ %10.1f │ %-10s │ %-28s │ %12v │\n",
			d.TargetAccount, d.PathScore*100, d.RiskScore, isWatchlist, d.Decision, d.Latency)
	}
	fmt.Println("└──────────────────────────────┴──────────────┴────────────┴────────────┴──────────────────────────────┴──────────────┘")
	fmt.Println()
	return nil
}

// ------------------------------------------------------------------------------------------------
// SCENARIO 2: Impossible Travel & Spatial Velocity Anomaly
// ------------------------------------------------------------------------------------------------

func runImpossibleTravelScenario(_ context.Context, client *driver.Client) error {
	fmt.Println("==========================================================================================")
	fmt.Println(">>> [SCENARIO 2] Impossible Travel & Geolocation Anomaly Detection (ATM / POS Fraud)")
	fmt.Println("    Method: 2D spatial lookup plus great-circle distance estimate.")
	fmt.Println("==========================================================================================")

	// ATM / POS Terminal Coordinates in Indonesia (X: Longitude, Y: Latitude)
	// - Jakarta Monas ATM:  (106.8272, -6.1751)
	// - Surabaya Gubeng ATM: (112.7508, -7.2656) -> ~660 km distance
	// - Bandung Dago POS:    (107.6186, -6.8905) -> ~120 km from Jakarta
	type TerminalNode struct {
		Name  string
		Coord spatial.Point2D
	}

	terminals := []TerminalNode{
		{"atm_jkt_monas_01", spatial.Point2D{X: 106.8272, Y: -6.1751}},
		{"atm_sby_gubeng_02", spatial.Point2D{X: 112.7508, Y: -7.2656}},
		{"pos_bdg_dago_03", spatial.Point2D{X: 107.6186, Y: -6.8905}},
	}

	for _, t := range terminals {
		if err := client.AppendNode(t.Name, t.Coord, [4]float32{1.0, 0.0, 0.0, 0.0}); err != nil {
			return fmt.Errorf("register terminal %s: %w", t.Name, err)
		}
	}
	fmt.Println("    [✓] Registered 3 ATM/POS physical coordinate nodes in 2D Spatial Index.")

	// Test Case 1: Impossible Travel Fraud Event
	// Card cloned! Used at Jakarta at 14:00:00, then used at Surabaya at 14:15:00 (15 minutes later).
	t0Jakarta := int64(1729173600)  // 14:00:00 UTC
	t1Surabaya := int64(1729174500) // 14:15:00 UTC (Delta: 900 seconds)

	tSpatial := time.Now()
	rawDistDeg, err := client.Math().Distance("atm_jkt_monas_01", "atm_sby_gubeng_02")
	spatialLat := time.Since(tSpatial)
	if err != nil {
		fmt.Printf("Distance error: %v\n", err)
		return fmt.Errorf("spatial distance query: %w", err)
	}

	// The spatial index returns Euclidean degrees. Use a great-circle distance
	// for the physical travel estimate instead of scaling degrees uniformly.
	distKm := haversineKm(terminals[0].Coord, terminals[1].Coord)
	timeDeltaHours := float64(t1Surabaya-t0Jakarta) / 3600.0
	travelVelocityKmH := distKm / timeDeltaHours

	fmt.Printf("    ▶ Great-circle distance:         %.1f km [Spatial query: %.3f degrees, %v]\n", distKm, rawDistDeg, spatialLat)
	fmt.Printf("    ▶ Time Elapsed between swipes:   %.0f minutes (%.2f hours)\n", float64(t1Surabaya-t0Jakarta)/60.0, timeDeltaHours)
	fmt.Printf("    ▶ Required Travel Speed:         %.1f km/h\n", travelVelocityKmH)

	if travelVelocityKmH > 250.0 {
		fmt.Println("    🚨 [FRAUD ALERT TRIGGERED]: IMPOSSIBLE TRAVEL ANOMALY (CARD CLONING SUSPECTED)")
		fmt.Println("       Example recommendation: reject this transaction and freeze the card.")
	} else {
		fmt.Println("    [✓] Physical travel velocity within plausible bounds.")
	}
	fmt.Println()
	return nil
}

func haversineKm(a, b spatial.Point2D) float64 {
	const earthRadiusKm = 6371.0088
	toRadians := func(degrees float32) float64 { return float64(degrees) * math.Pi / 180 }
	lat1, lat2 := toRadians(a.Y), toRadians(b.Y)
	dLat := lat2 - lat1
	dLon := toRadians(b.X - a.X)
	h := math.Sin(dLat/2)*math.Sin(dLat/2) + math.Cos(lat1)*math.Cos(lat2)*math.Sin(dLon/2)*math.Sin(dLon/2)
	if h > 1 {
		h = 1
	}
	return 2 * earthRadiusKm * math.Asin(math.Sqrt(h))
}

// ------------------------------------------------------------------------------------------------
// SCENARIO 3: Massive 1,000,000+ Scale Stress Test
// ------------------------------------------------------------------------------------------------

func runScaleBenchmarkScenario(_ context.Context, client *driver.Client, totalRecords int) error {
	fmt.Println("==========================================================================================")
	fmt.Printf(">>> [SCENARIO 3] Enterprise High-Throughput Scale Benchmark (%s Accounts)\n", formatNumber(totalRecords))
	fmt.Println("    Measuring AnchorWire ingestion throughput and causal-query latency at scale.")
	fmt.Println("==========================================================================================")

	numWorkers := runtime.NumCPU() * 2
	if numWorkers < 4 {
		numWorkers = 4
	}

	recordsPerWorker := totalRecords / numWorkers
	remainder := totalRecords % numWorkers
	var counter atomic.Int64
	var errorCount atomic.Int64
	var wg sync.WaitGroup

	fmt.Printf("[1/3] Streaming %s bank account records across %d parallel workers...\n",
		formatNumber(totalRecords), numWorkers)
	t0 := time.Now()

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
				accName := fmt.Sprintf("acc_enterprise_%08d", i)
				var isBlacklisted float32
				if i%50000 == 0 {
					isBlacklisted = 1.0
				}
				props := map[string]float32{
					"risk_score": float32(i % 100),
					// Store seconds relative to the sample epoch so float32 retains second-level precision.
					"last_tx_timestamp": float32(i % 86400),
					"daily_tx_volume":   float32(500 + (i % 50000)),
					"is_blacklisted":    isBlacklisted,
					"kyc_tier":          float32(1 + (i % 3)),
				}
				if err := client.Save(accName, props); err != nil {
					errs := errorCount.Add(1)
					if errs <= 5 {
						fmt.Printf("Worker %d error saving %s: %v\n", workerID, accName, err)
					}
					continue
				}
				done := counter.Add(1)
				if done%250000 == 0 || done == int64(totalRecords) {
					elapsed := time.Since(t0).Seconds()
					fmt.Printf("      -> Ingested %s / %s accounts (%.0f writes/sec) [Elapsed: %v]\n",
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

	// Connect Dense Transfer Chains (100,000 edges)
	const numEdges = 100000
	fmt.Printf("[2/3] Linking %s transactional transfer edges across accounts...\n", formatNumber(numEdges))
	tLink := time.Now()
	for i := 0; i < numEdges; i++ {
		src := fmt.Sprintf("acc_enterprise_%08d", i)
		dst := fmt.Sprintf("acc_enterprise_%08d", (i+1)%totalRecords)
		if err := client.Link(src, "transfers_to", dst, 0.90+float32(i%10)*0.01); err != nil {
			errorCount.Add(1)
			if errorCount.Load() <= 5 {
				fmt.Printf("Link error %s -> %s: %v\n", src, dst, err)
			}
		}
	}
	durLink := time.Since(tLink)
	linkErrors := errorCount.Load()
	fmt.Printf("    Linked %s / %s edges (%d errors) in %v (%.0f successful edges/sec)\n\n",
		formatNumber(numEdges-int(linkErrors)), formatNumber(numEdges), linkErrors, durLink, float64(numEdges-int(linkErrors))/durLink.Seconds())
	if linkErrors > 0 {
		return fmt.Errorf("%d of %d transfer links failed", linkErrors, numEdges)
	}

	// Measure Pre-Authorization Fraud Latency at 1M Scale
	const fraudInspectionQueries = 50000
	fmt.Printf("[3/3] Benchmarking %s Pre-Authorization Real-Time Fraud Inspections...\n", formatNumber(fraudInspectionQueries))

	latencies := make([]time.Duration, fraudInspectionQueries)
	tQueries := time.Now()
	var queryErrors atomic.Int64

	for i := 0; i < fraudInspectionQueries; i++ {
		src := fmt.Sprintf("acc_enterprise_%08d", (i*13)%numEdges)
		qStart := time.Now()
		results, err := client.Causal(src, "transfers_to", 5)
		if err != nil || len(results) == 0 {
			queryErrors.Add(1)
		}
		latencies[i] = time.Since(qStart)
	}
	durQueries := time.Since(tQueries)
	qps := float64(fraudInspectionQueries) / durQueries.Seconds()

	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})

	p50 := latencies[len(latencies)*50/100]
	p95 := latencies[len(latencies)*95/100]
	p99 := latencies[len(latencies)*99/100]
	avgLat := durQueries / time.Duration(fraudInspectionQueries)

	fmt.Println("------------------------------------------------------------------------------------------")
	fmt.Printf("    Inspections: %s queries (%d errors or empty results)\n", formatNumber(fraudInspectionQueries), queryErrors.Load())
	fmt.Printf("    Inspection Throughput:          %.0f checks / sec\n", qps)
	fmt.Printf("    Average Latency:                %v\n", avgLat)
	fmt.Printf("    p50 Latency:                    %v\n", p50)
	fmt.Printf("    p95 Latency:                    %v\n", p95)
	fmt.Printf("    p99 Latency:                    %v\n", p99)
	fmt.Println("------------------------------------------------------------------------------------------")
	if queryErrors.Load() > 0 {
		return fmt.Errorf("%d of %d fraud inspection queries failed", queryErrors.Load(), fraudInspectionQueries)
	}
	const authorizationBudget = time.Millisecond
	if p99 >= authorizationBudget {
		return fmt.Errorf("p99 latency %v exceeds the stated %v example budget", p99, authorizationBudget)
	}
	fmt.Printf(">>> Sample result: p99 is below the %v example budget; this benchmark alone is not an SLA guarantee.\n", authorizationBudget)
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
			out = "," + out
		}
	}
	return out
}
