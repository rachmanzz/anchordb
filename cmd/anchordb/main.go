package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/rachmanzz/anchordb/pkg/driver"
	"github.com/rachmanzz/anchordb/pkg/spatial"
)

func main() {
	fmt.Println("==========================================================")
	fmt.Println("    ANCHORDB: COMPUTATIONAL GRAPH STORAGE ENGINE (CPU)    ")
	fmt.Println("==========================================================")

	_ = os.MkdirAll("data", 0755)
	dbFile := filepath.Join("data", "world.anchordb")
	_ = os.Remove(dbFile) // Start fresh for demo

	// 1. Open connection via Dedicated Driver SDK
	t0 := time.Now()
	client, err := driver.Connect("anchordb://" + dbFile + "?mode=mmap")
	if err != nil {
		log.Fatalf("Failed to connect to AnchorDB: %v", err)
	}
	defer client.Close()
	fmt.Printf("[OK] Connected to AnchorDB via Driver in %v (mmap zero-copy)\n", time.Since(t0))

	// 2. Populate initial nodes into binary storage (Fixed-size 40 bytes / node)
	db := client.DB()
	nodes := []struct {
		word  string
		coord spatial.Point2D
		roles [4]float32
	}{
		{"primary_cluster", spatial.Point2D{X: 0.2, Y: 0.9}, [4]float32{0.9, 0.1, 0.2, 0.2}},
		{"backup_cluster", spatial.Point2D{X: 0.2, Y: 0.9}, [4]float32{0.9, 0.1, 0.2, 0.2}},
		{"load_balancer", spatial.Point2D{X: 0.2, Y: 0.1}, [4]float32{0.2, 0.1, 0.95, 0.2}},
		{"service_mesh", spatial.Point2D{X: 0.2, Y: -0.5}, [4]float32{0.1, 0.1, 0.9, 0.5}},
		{"database_cluster", spatial.Point2D{X: 0.2, Y: -0.7}, [4]float32{0.2, 0.9, 0.5, 0.1}},
		{"cache_layer", spatial.Point2D{X: 0.6, Y: -0.7}, [4]float32{0.1, 0.1, 0.9, 0.2}},
		{"analytics_engine", spatial.Point2D{X: 0.8, Y: -0.9}, [4]float32{0.2, 0.1, 0.95, 0.4}},
		{"edge_gateway", spatial.Point2D{X: -0.5, Y: 0.5}, [4]float32{0.8, 0.1, 0.2, 0.1}},
		{"waf_service", spatial.Point2D{X: -0.5, Y: 0.5}, [4]float32{0.1, 0.1, 0.2, 0.9}},
		{"dns_resolver", spatial.Point2D{X: -0.5, Y: -0.2}, [4]float32{0.2, 0.1, 0.95, 0.3}},
	}

	for _, n := range nodes {
		if _, err := db.AppendNode(n.word, n.coord, n.roles, 1); err != nil {
			log.Fatalf("failed to append node %q: %v", n.word, err)
		}
	}
	if err := db.SetRelation("route-down", spatial.Point2D{Y: -1}); err != nil {
		log.Fatalf("failed to configure demo relation: %v", err)
	}
	_ = db.Sync()

	fi, _ := os.Stat(dbFile)
	fmt.Printf("[OK] Binary .anchordb file saved: %d bytes (Ultra-dense format!)\n\n", fi.Size())

	ctx := context.Background()

	// 3. Execute Graph Query using Fluent Builder Driver
	fmt.Println("----------------------------------------------------------")
	fmt.Println(">>> QUERY 1: In-Storage Pushdown Graph Step")
	fmt.Println("Query: client.Graph().From('primary_cluster', 'backup_cluster').Relate('route-down').TopK(5).Execute()")

	qStart := time.Now()
	resp, err := client.Graph().
		From("primary_cluster", "backup_cluster").
		Relate("route-down").
		BeamWidth(0.05).
		TopK(5).
		Execute(ctx)
	qDuration := time.Since(qStart)

	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}

	fmt.Printf("Barycenter Anchor Point: (%.2f, %.2f)\n", resp.Anchor.X, resp.Anchor.Y)
	fmt.Printf("Internal DB Computation Time: %v (Sub-Microsecond!)\n", qDuration)
	fmt.Printf("Top Candidates Passing DDA Ray & Pythagoras:\n")
	for i, r := range resp.Results {
		fmt.Printf("   [%d] '%s' | Axis Distance t: %.2f | Perp Deviation d_perp: %.4f | Score: %.4f\n",
			i+1, r.Word, r.T, r.DistSqPerp, r.Score)
	}

	// 4. Query 2: In-Storage Multi-Hop Chaining
	fmt.Println("\n----------------------------------------------------------")
	fmt.Println(">>> QUERY 2: In-Storage Multi-Hop Chain Execution")
	fmt.Println("Query: client.Graph().From('primary_cluster', 'backup_cluster').Chain('route-down').ExecuteChain()")

	chainStart := time.Now()
	chainResp, err := client.Graph().
		From("primary_cluster", "backup_cluster").
		Chain("route-down").
		ExecuteChain(ctx)
	chainDuration := time.Since(chainStart)

	if err != nil {
		log.Fatalf("Chain query failed: %v", err)
	}

	fmt.Printf("Chained Traversal Result: %v (Duration: %v)\n", chainResp.Sequence, chainDuration)

	// 5. Query 3: Direct In-Storage Mathematical Calculations (Math Engine)
	fmt.Println("\n----------------------------------------------------------")
	fmt.Println(">>> QUERY 3: In-Storage Direct Mathematical Calculation (Math Engine)")

	mathEng := client.Math()

	// A. Direct Euclidean Distance
	dist, _ := mathEng.Distance("primary_cluster", "load_balancer")
	fmt.Printf("1. Direct Spatial Distance 'primary_cluster' <-> 'load_balancer': %.4f\n", dist)

	// B. Direct Centroid Calculation
	centroid, _ := mathEng.Centroid("primary_cluster", "backup_cluster", "load_balancer")
	fmt.Printf("2. Center of Gravity (Centroid) ['primary_cluster', 'backup_cluster', 'load_balancer']: (%.2f, %.2f)\n",
		centroid.Coord.X, centroid.Coord.Y)

	// C. Semantic Role Cosine Similarity
	sim, _ := mathEng.CosineSimilarity("primary_cluster", "backup_cluster")
	fmt.Printf("3. Role Cosine Similarity 'primary_cluster' & 'backup_cluster': %.4f (Identical)\n", sim)

	// D. Nearest Spatial Neighbors
	nearest, _ := mathEng.Nearest("primary_cluster", 3)
	fmt.Println("4. Nearest Spatial Neighbors of 'primary_cluster':")
	for i, n := range nearest {
		fmt.Printf("   [%d] '%s' (Distance: %.4f)\n", i+1, n.Word, n.T)
	}

	// 6. Query 4: SAVING, GETTING, and CALCULATING (Example: Distance, Duration -> Speed)
	fmt.Println("\n----------------------------------------------------------")
	fmt.Println(">>> QUERY 4: CORE DATABASE ENGINE (SAVING & GETTING + CALCULATING)")
	fmt.Println("Example: Save distance & duration, retrieve and calculate speed = distance / duration")

	// 1. SAVING: Save Raw Data
	_ = client.Save("vehicle_trip", map[string]float32{
		"distance": 100.0, // km
		"duration": 2.0,   // hours
	})
	fmt.Println("1. SAVING: Data saved -> 'vehicle_trip' {distance: 100.0, duration: 2.0}")

	// 2. GETTING: Retrieve Pure Data
	rawEntity, _ := client.Get("vehicle_trip").Execute(ctx)
	fmt.Printf("2. GETTING: Pure retrieved data -> %v\n", rawEntity.All())

	// 3. CALCULATING: Retrieve + Compute Speed internally during retrieval
	calcEntity, _ := client.Get("vehicle_trip").
		Compute("speed", driver.Divide("distance", "duration")).
		Execute(ctx)

	fmt.Printf("3. CALCULATING (In-DB Compute): Distance = %.1f km, Duration = %.1f hours ==> Speed = %.1f km/h\n",
		calcEntity.Get("distance"), calcEntity.Get("duration"), calcEntity.Get("speed"))
	fmt.Printf("   Full Return from DB: %v\n", calcEntity.All())

	// 7. Query 5: LIST, WHERE, COMPUTE, and PAGING (CONSTANT TIME)
	fmt.Println("\n----------------------------------------------------------")
	fmt.Println(">>> QUERY 5: LIST + WHERE + COMPUTE + PAGING (CONSTANT O(Limit))")
	fmt.Println("Filter distance >= 50, compute speed, filter speed > 50, Limit 2 per page")

	_ = client.Save("route_express_1", map[string]float32{"distance": 150, "duration": 2})    // v = 75
	_ = client.Save("route_regional_2", map[string]float32{"distance": 90, "duration": 1.5})  // v = 60
	_ = client.Save("route_local_loop", map[string]float32{"distance": 10, "duration": 1})    // v = 10 (filtered out)
	_ = client.Save("route_interstate_4", map[string]float32{"distance": 400, "duration": 4}) // v = 100

	// Page 1
	p1, _ := client.List().
		Where("distance", ">=", 50).
		Compute("speed", driver.Divide("distance", "duration")).
		Where("speed", ">", 50).
		Page(1, 2).
		Execute(ctx)

	fmt.Println("--- PAGE 1 (Limit: 2) ---")
	for i, item := range p1.Items {
		fmt.Printf("   [%d] %-22s | Distance: %5.0f km | Duration: %3.1f h | Speed: %5.0f km/h\n",
			i+1, item.Name, item.Get("distance"), item.Get("duration"), item.Get("speed"))
	}
	fmt.Printf("   HasMore: %v\n", p1.HasMore)

	// Page 2
	p2, _ := client.List().
		Where("distance", ">=", 50).
		Compute("speed", driver.Divide("distance", "duration")).
		Where("speed", ">", 50).
		Page(2, 2).
		Execute(ctx)

	fmt.Println("--- PAGE 2 (Next Page) ---")
	for i, item := range p2.Items {
		fmt.Printf("   [%d] %-22s | Distance: %5.0f km | Duration: %3.1f h | Speed: %5.0f km/h\n",
			i+1, item.Name, item.Get("distance"), item.Get("duration"), item.Get("speed"))
	}
	fmt.Printf("   HasMore: %v (Paging Complete!)\n", p2.HasMore)

	fmt.Println("==========================================================")
}
