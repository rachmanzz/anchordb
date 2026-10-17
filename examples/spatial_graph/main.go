package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/rachmanzz/anchordb/pkg/driver"
	"github.com/rachmanzz/anchordb/pkg/spatial"
)

func main() {
	mode := flag.String("mode", "embedded", "Execution mode: 'embedded' (in-process engine), 'wire' (AnchorWire binary TCP protocol on port 7448), or 'remote' (HTTP engine on port 7447)")
	serverURL := flag.String("url", "http://localhost:7447", "AnchorDB server URL (used in wire/remote mode): wire://host:7448 or http(s)://host:7447")
	flag.Parse()

	fmt.Println("==========================================================")
	fmt.Println("    AnchorDB Example: 2D Spatial Grid & Raycasting DDA    ")
	fmt.Println("==========================================================")

	var client *driver.Client
	var cleanup func()

	if *mode == "wire" {
		if *serverURL == "http://localhost:7447" || *serverURL == "" {
			*serverURL = "wire://localhost:7448"
		}
		*mode = "remote"
	}

	if *mode == "remote" {
		t0 := time.Now()
		c, err := driver.Connect(*serverURL, driver.WithModel("world"))
		if err != nil {
			log.Fatalf("Failed to connect to AnchorDB (%s): %v", *serverURL, err)
		}
		client = c
		cleanup = func() { _ = client.Close() }
		fmt.Printf("[OK] Connected to AnchorDB Remote Engine (%s) in %v\n", *serverURL, time.Since(t0))
	} else {
		tmpDir, err := os.MkdirTemp("", "anchordb_spatial_*")
		if err != nil {
			log.Fatalf("Failed to create temp dir: %v", err)
		}
		t0 := time.Now()
		c, err := driver.OpenEmbedded(tmpDir, driver.WithModel("world"))
		if err != nil {
			log.Fatalf("Failed to open embedded AnchorDB: %v", err)
		}
		client = c
		cleanup = func() {
			_ = client.Close()
			_ = os.RemoveAll(tmpDir)
		}
		fmt.Printf("[OK] Connected to AnchorDB Embedded Engine in %v (mmap zero-copy)\n", time.Since(t0))
	}
	defer cleanup()

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
	dist, _ := mathEng.Distance("primary_cluster", "load_balancer")
	fmt.Printf("1. Direct Spatial Distance 'primary_cluster' <-> 'load_balancer': %.4f\n", dist)

	// B. Direct Centroid Calculation
	centroid, _ := mathEng.Centroid("primary_cluster", "backup_cluster", "load_balancer")
	fmt.Printf("2. Center of Gravity (Centroid) ['primary_cluster', 'backup_cluster', 'load_balancer']: (%.2f, %.2f)\n",
		centroid.Coord.X, centroid.Coord.Y)

	// C. Semantic Role Cosine Similarity
	sim, _ := mathEng.CosineSimilarity("primary_cluster", "backup_cluster")
	fmt.Printf("3. Role Cosine Similarity 'primary_cluster' & 'backup_cluster': %.4f\n", sim)

	// D. Nearest Spatial Neighbors
	nearest, _ := mathEng.Nearest("primary_cluster", 3)
	fmt.Println("4. Nearest Spatial Neighbors of 'primary_cluster':")
	for i, n := range nearest {
		fmt.Printf("   [%d] '%s' (Distance: %.4f)\n", i+1, n.Word, n.T)
	}

	// 5. Query 3: Entity Storage & On-The-Fly Arithmetic
	fmt.Println("\n----------------------------------------------------------")
	fmt.Println(">>> QUERY 3: Entity Storage & In-Storage Division (Math Compute)")

	_ = client.Save("vehicle_trip", map[string]float32{
		"distance": 120.0,
		"duration": 2.0,
	})

	record, _ := client.Get("vehicle_trip").
		Compute("speed", driver.Divide("distance", "duration")).
		Execute(ctx)

	fmt.Printf("Saved: distance=%.0f km, duration=%.0f hours\n", record.Get("distance"), record.Get("duration"))
	fmt.Printf("Calculated Speed (distance / duration): %.1f km/h\n", record.Get("speed"))
}
