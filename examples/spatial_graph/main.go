// Package main demonstrates spatial graph reasoning and path traversal with AnchorDB.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/rachmanzz/anchordb/pkg/driver"
	"github.com/rachmanzz/anchordb/pkg/spatial"
)

func main() {
	wireURL := flag.String("url", "wire://localhost:7448", "AnchorWire TCP server URL: wire://host:7448 or localhost:7448")
	authToken := flag.String("token", "", "Optional auth token")
	flag.Parse()

	fmt.Println("==========================================================")
	fmt.Println("    AnchorDB Example: 2D Spatial Grid & Raycasting DDA    ")
	fmt.Println("             (AnchorWire Binary TCP Protocol)             ")
	fmt.Println("==========================================================")

	target := *wireURL
	if strings.HasPrefix(target, "http://") || strings.HasPrefix(target, "https://") {
		log.Fatalf("[FATAL] HTTP is disabled in this example. Only AnchorWire binary TCP (wire://localhost:7448) is supported.")
	}
	if !strings.HasPrefix(target, "wire://") && !strings.HasPrefix(target, "tcp://") {
		target = "wire://" + target
	}

	opts := []driver.Option{driver.WithModel("world")}
	if *authToken != "" {
		opts = append(opts, driver.WithToken(*authToken))
	}

	t0 := time.Now()
	client, err := driver.ConnectWire(target, opts...)
	if err != nil {
		log.Fatalf("[FATAL] Failed to connect to AnchorWire server (%s): %v\nMake sure AnchorDB is running with AnchorWire listening on port 7448.", target, err)
	}
	defer client.Close()
	fmt.Printf("[OK] Connected via AnchorWire Binary TCP (%s) in %v\n", target, time.Since(t0))

	// 2. Populate spatial cluster nodes
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
		if err := client.AppendNode(n.word, n.coord, n.roles); err != nil {
			log.Fatalf("failed to append node %q: %v", n.word, err)
		}
	}
	if err := client.SetRelation("route-down", spatial.Point2D{Y: -1}); err != nil {
		log.Fatalf("failed to configure spatial relation: %v", err)
	}

	ctx := context.Background()

	// 3. Execute Spatial Ray-Cast Query
	fmt.Println("----------------------------------------------------------")
	fmt.Println(">>> QUERY 1: In-Storage Pushdown Spatial Graph Step")
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
	if len(resp.Results) == 0 {
		log.Fatal("spatial graph query returned no candidates")
	}

	fmt.Printf("Barycenter Anchor Point: (%.2f, %.2f)\n", resp.Anchor.X, resp.Anchor.Y)
	fmt.Printf("Internal DB Computation Time: %v\n", qDuration)
	fmt.Printf("Top Candidates Passing DDA Ray & Pythagoras:\n")
	for i, r := range resp.Results {
		fmt.Printf("   [%d] '%s' | Axis Distance t: %.2f | Perp Deviation d_perp: %.4f | Score: %.4f\n",
			i+1, r.Word, r.T, r.DistSqPerp, r.Score)
	}

	// 4. Query 2: Direct Math Engine Operations
	fmt.Println("\n----------------------------------------------------------")
	fmt.Println(">>> QUERY 2: Direct Mathematical Calculation (Math Engine)")

	mathEng := client.Math()

	// A. Direct Euclidean Distance
	dist, err := mathEng.Distance("primary_cluster", "load_balancer")
	if err != nil {
		log.Fatalf("distance calculation failed: %v", err)
	}
	fmt.Printf("1. Direct Spatial Distance 'primary_cluster' <-> 'load_balancer': %.4f\n", dist)

	// B. Direct Centroid Calculation
	centroid, err := mathEng.Centroid("primary_cluster", "backup_cluster", "load_balancer")
	if err != nil {
		log.Fatalf("centroid calculation failed: %v", err)
	}
	fmt.Printf("2. Center of Gravity (Centroid) ['primary_cluster', 'backup_cluster', 'load_balancer']: (%.2f, %.2f)\n",
		centroid.Coord.X, centroid.Coord.Y)

	// C. Semantic Role Cosine Similarity
	sim, err := mathEng.CosineSimilarity("primary_cluster", "backup_cluster")
	if err != nil {
		log.Fatalf("cosine similarity calculation failed: %v", err)
	}
	fmt.Printf("3. Role Cosine Similarity 'primary_cluster' & 'backup_cluster': %.4f\n", sim)

	// D. Nearest Spatial Neighbors
	nearest, err := mathEng.Nearest("primary_cluster", 3)
	if err != nil {
		log.Fatalf("nearest-neighbor query failed: %v", err)
	}
	if len(nearest) == 0 {
		log.Fatal("nearest-neighbor query returned no nodes")
	}
	fmt.Println("4. Nearest Spatial Neighbors of 'primary_cluster':")
	for i, n := range nearest {
		fmt.Printf("   [%d] '%s' (Distance: %.4f)\n", i+1, n.Word, n.T)
	}

	// 5. Query 3: Entity Storage & On-The-Fly Arithmetic
	fmt.Println("\n----------------------------------------------------------")
	fmt.Println(">>> QUERY 3: Entity Storage & In-Storage Division (Math Compute)")

	if err := client.Save("vehicle_trip", map[string]float32{
		"distance": 120.0,
		"duration": 2.0,
	}); err != nil {
		log.Fatalf("failed to save vehicle trip: %v", err)
	}

	record, err := client.Get("vehicle_trip").
		Compute("speed", driver.Divide("distance", "duration")).
		Execute(ctx)
	if err != nil {
		log.Fatalf("vehicle trip query failed: %v", err)
	}

	fmt.Printf("Saved: distance=%.0f km, duration=%.0f hours\n", record.Get("distance"), record.Get("duration"))
	fmt.Printf("Calculated Speed (distance / duration): %.1f km/h\n", record.Get("speed"))
}
