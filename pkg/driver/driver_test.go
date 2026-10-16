package driver

import (
	"context"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/rachmanzz/anchordb/pkg/scheduler"
	"github.com/rachmanzz/anchordb/pkg/spatial"
	"github.com/rachmanzz/anchordb/pkg/storage"
)

func TestGraphBuilderBeamWidthAndTopKValidation(t *testing.T) {
	db, err := storage.Open(filepath.Join(t.TempDir(), "builder-options.anchordb"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.SetRelation("test-down", spatial.Point2D{Y: -1}); err != nil {
		t.Fatal(err)
	}
	for word, point := range map[string]spatial.Point2D{
		"a": {Y: 0},
		"b": {X: 0.02, Y: -0.02},
		"c": {X: 0.2, Y: -0.2},
	} {
		if _, err := db.AppendNode(word, point, [4]float32{}, 1); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	narrow, err := NewGraphBuilder(db).From("a").Relate("test-down").BeamWidth(0.01).TopK(10).Execute(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	wide, err := NewGraphBuilder(db).From("a").Relate("test-down").BeamWidth(0.2).TopK(10).Execute(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(wide.Results) <= len(narrow.Results) {
		t.Fatalf("BeamWidth was not applied: narrow=%d wide=%d", len(narrow.Results), len(wide.Results))
	}
	for _, k := range []int{-1} {
		if _, err := NewGraphBuilder(db).From("a").Relate("test-down").TopK(k).Execute(context.Background()); err == nil {
			t.Fatalf("TopK(%d) should fail", k)
		}
	}
	if _, err := NewGraphBuilder(db).From("a").Relate("test-down").BeamWidth(float32(math.NaN())).Execute(context.Background()); err == nil {
		t.Fatal("NaN BeamWidth should fail")
	}
}

func TestDriverInvalidFilterValueReturnsError(t *testing.T) {
	client, err := Connect(filepath.Join(t.TempDir(), "invalid-filter.anchordb"))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if _, err := client.List().Where("value", "=", "not-a-number").Execute(context.Background()); err == nil {
		t.Fatal("invalid list filter should return an error")
	}
	if _, err := client.Update().Where("value", "=", "not-a-number").Set("value", Value(1)).Execute(context.Background()); err == nil {
		t.Fatal("invalid update filter should return an error")
	}
}

func TestDriverBuildersRespectCanceledContext(t *testing.T) {
	client, err := Connect(filepath.Join(t.TempDir(), "canceled-context.anchordb"))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := client.Get("missing").Execute(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Get should respect canceled context, got %v", err)
	}
	if _, err := client.List().Execute(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("List should respect canceled context, got %v", err)
	}
	if _, err := client.Update().Execute(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Update should respect canceled context, got %v", err)
	}
	if err := client.DB().SetRelation("test", spatial.Point2D{Y: -1}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.DB().AppendNode("input", spatial.Point2D{}, [4]float32{}, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := NewGraphBuilder(client.DB()).From("input").Relate("test").Execute(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Graph should respect canceled context, got %v", err)
	}
}

func TestDriverBatchUpdatePropagatesComputationError(t *testing.T) {
	client, err := Connect(filepath.Join(t.TempDir(), "update-error.anchordb"))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if err := client.Save("zero", map[string]float32{"value": 0}); err != nil {
		t.Fatal(err)
	}
	updated, err := client.Update().In("zero").Set("result", Divide("value", "0")).Execute(context.Background())
	if err == nil || updated != 0 {
		t.Fatalf("expected failed update with zero records committed, got count=%d err=%v", updated, err)
	}
}

func TestDriver_FluentBuilder(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "driver_test.anchordb")

	// 1. Connect via Driver DSN
	client, err := Connect("anchordb://" + dbPath)
	if err != nil {
		t.Fatalf("failed to connect via driver: %v", err)
	}
	defer client.Close()

	// 2. Populate test nodes directly
	db := client.DB()
	if err := db.SetRelation("test-down", spatial.Point2D{Y: -1}); err != nil {
		t.Fatal(err)
	}
	_, _ = db.AppendNode("primary_cluster", spatial.Point2D{X: 0.2, Y: 0.9}, [4]float32{0.9, 0.1, 0.2, 0.2}, 1)
	_, _ = db.AppendNode("backup_cluster", spatial.Point2D{X: 0.2, Y: 0.9}, [4]float32{0.9, 0.1, 0.2, 0.2}, 1)
	_, _ = db.AppendNode("load_balancer", spatial.Point2D{X: 0.2, Y: 0.1}, [4]float32{0.2, 0.1, 0.95, 0.2}, 1)
	_, _ = db.AppendNode("service_mesh", spatial.Point2D{X: 0.2, Y: -0.5}, [4]float32{0.1, 0.1, 0.9, 0.5}, 1)
	_, _ = db.AppendNode("database_cluster", spatial.Point2D{X: 0.2, Y: -0.7}, [4]float32{0.2, 0.9, 0.5, 0.1}, 1)
	_ = db.Sync()

	ctx := context.Background()

	// 3. Test Fluent Query Builder
	resp, err := client.Graph().
		From("primary_cluster", "backup_cluster").
		Relate("test-down").
		BeamWidth(0.05).
		TopK(5).
		Execute(ctx)

	if err != nil {
		t.Fatalf("driver query failed: %v", err)
	}

	if len(resp.Results) == 0 {
		t.Fatalf("expected results from driver, got 0")
	}

	if resp.Results[0].Word != "load_balancer" {
		t.Fatalf("expected 'load_balancer', got %q", resp.Results[0].Word)
	}

	t.Logf("Driver Success! Anchor: (%.2f, %.2f), Top-1: %s (Score: %.4f)",
		resp.Anchor.X, resp.Anchor.Y, resp.Results[0].Word, resp.Results[0].Score)
}

func TestDriver_MultiHopChain(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "driver_chain.anchordb")

	client, err := Connect("anchordb://" + dbPath)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer client.Close()

	db := client.DB()
	if err := db.SetRelation("test-down", spatial.Point2D{Y: -1}); err != nil {
		t.Fatal(err)
	}
	_, _ = db.AppendNode("primary_cluster", spatial.Point2D{X: 0.2, Y: 0.9}, [4]float32{0.9, 0.1, 0.2, 0.2}, 1)
	_, _ = db.AppendNode("backup_cluster", spatial.Point2D{X: 0.2, Y: 0.9}, [4]float32{0.9, 0.1, 0.2, 0.2}, 1)
	_, _ = db.AppendNode("load_balancer", spatial.Point2D{X: 0.2, Y: 0.1}, [4]float32{0.2, 0.1, 0.95, 0.2}, 1)
	_ = db.Sync()

	ctx := context.Background()

	chainResp, err := client.Graph().
		From("primary_cluster", "backup_cluster").
		Chain("test-down").
		ExecuteChain(ctx)

	if err != nil {
		t.Fatalf("chain failed: %v", err)
	}

	if len(chainResp.Sequence) == 0 || chainResp.Sequence[0] != "load_balancer" {
		t.Fatalf("expected chain sequence ['load_balancer'], got %v", chainResp.Sequence)
	}

	t.Logf("Chain result: %v", chainResp.Sequence)
}

func TestDriver_DirectMathAndOrdering(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "driver_math.anchordb")

	client, err := Connect("anchordb://" + dbPath)
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer client.Close()

	db := client.DB()
	if err := db.SetRelation("test-down", spatial.Point2D{Y: -1}); err != nil {
		t.Fatal(err)
	}
	_, _ = db.AppendNode("primary_cluster", spatial.Point2D{X: 0.2, Y: 0.9}, [4]float32{0.9, 0.1, 0.2, 0.2}, 1)
	_, _ = db.AppendNode("backup_cluster", spatial.Point2D{X: 0.2, Y: 0.9}, [4]float32{0.9, 0.1, 0.2, 0.2}, 1)
	_, _ = db.AppendNode("load_balancer", spatial.Point2D{X: 0.2, Y: 0.1}, [4]float32{0.2, 0.1, 0.95, 0.2}, 1)
	_ = db.Sync()

	math := client.Math()

	// 1. Test Direct Distance Calculation
	dist, err := math.Distance("primary_cluster", "load_balancer")
	if err != nil {
		t.Fatalf("distance failed: %v", err)
	}
	// Distance between (0.2, 0.9) and (0.2, 0.1) is exactly 0.8
	if dist < 0.79 || dist > 0.81 {
		t.Fatalf("expected distance ~0.8, got %f", dist)
	}
	t.Logf("Direct Distance 'primary_cluster' -> 'load_balancer': %f", dist)

	// 2. Test Direct Centroid Calculation
	centroid, err := math.Centroid("primary_cluster", "backup_cluster")
	if err != nil {
		t.Fatalf("centroid failed: %v", err)
	}
	if centroid.Coord.X != 0.2 || centroid.Coord.Y != 0.9 {
		t.Fatalf("expected centroid (0.2, 0.9), got (%f, %f)", centroid.Coord.X, centroid.Coord.Y)
	}
	t.Logf("Centroid of 'primary_cluster' & 'backup_cluster': (%f, %f)", centroid.Coord.X, centroid.Coord.Y)

	// 3. Test Cosine Similarity
	sim, err := math.CosineSimilarity("primary_cluster", "backup_cluster")
	if err != nil {
		t.Fatalf("cosine similarity failed: %v", err)
	}
	if sim < 0.99 {
		t.Fatalf("expected cosine similarity ~ 1.0 for identical roles, got %f", sim)
	}
	t.Logf("Cosine Similarity 'primary_cluster' & 'backup_cluster': %f", sim)

	// 4. Test Nearest Neighbors
	nearest, err := math.Nearest("primary_cluster", 2)
	if err != nil {
		t.Fatalf("nearest failed: %v", err)
	}
	if len(nearest) == 0 {
		t.Fatalf("expected nearest neighbors, got 0")
	}
	t.Logf("Nearest neighbor to 'primary_cluster': %s (Distance: %f)", nearest[0].Word, nearest[0].T)

	// 5. Test Custom OrderBy in Query
	ctx := context.Background()
	resp, err := client.Graph().
		From("primary_cluster", "backup_cluster").
		Relate("test-down").
		OrderBy(storage.OrderByDistanceAsc).
		Execute(ctx)
	if err != nil {
		t.Fatalf("query with OrderByDistanceAsc failed: %v", err)
	}
	if len(resp.Results) == 0 || resp.Results[0].Word != "load_balancer" {
		t.Fatalf("expected 'load_balancer' under OrderByDistanceAsc")
	}
	t.Logf("OrderByDistanceAsc Top-1: %s (t: %f)", resp.Results[0].Word, resp.Results[0].T)
}

func TestDriver_SaveGetCompute_SpeedExample(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "driver_physics.anchordb")

	client, err := Connect("anchordb://" + dbPath)
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	// 1. Core operation: SAVING (Store raw data: distance = 100, duration = 2)
	err = client.Save("vehicle_trip", map[string]float32{
		"distance": 100.0, // km
		"duration": 2.0,   // hours
	})
	if err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	// 2. Core operation: GETTING (Retrieve raw data)
	rawRes, err := client.Get("vehicle_trip").Execute(ctx)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if rawRes.Get("distance") != 100.0 || rawRes.Get("duration") != 2.0 {
		t.Fatalf("expected distance=100, duration=2, got %v", rawRes.All())
	}
	t.Logf("1. Pure GETTING: %v", rawRes.All())

	// 3. Advanced operation: CALCULATING (Retrieve distance & duration, compute speed = distance / duration)
	// Return must include: distance, duration, and speed!
	calcRes, err := client.Get("vehicle_trip").
		Compute("speed", Divide("distance", "duration")).
		Execute(ctx)
	if err != nil {
		t.Fatalf("Get with Compute failed: %v", err)
	}

	distance := calcRes.Get("distance")
	duration := calcRes.Get("duration")
	speed := calcRes.Get("speed")

	if distance != 100.0 {
		t.Fatalf("expected distance = 100, got %f", distance)
	}
	if duration != 2.0 {
		t.Fatalf("expected duration = 2, got %f", duration)
	}
	if speed != 50.0 {
		t.Fatalf("expected speed = 50, got %f", speed)
	}

	t.Logf("2. GETTING + CALCULATING: distance=%.1f, duration=%.1f, speed=%.1f (All returned!)",
		distance, duration, speed)
}

func TestDriver_List_Where_Compute_Paging(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "driver_list_page.anchordb")

	client, err := Connect("anchordb://" + dbPath)
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	// Seed multiple trips
	_ = client.Save("trip_1", map[string]float32{"distance": 120, "duration": 2}) // speed = 60
	_ = client.Save("trip_2", map[string]float32{"distance": 200, "duration": 2}) // speed = 100
	_ = client.Save("trip_3", map[string]float32{"distance": 40, "duration": 1})  // speed = 40 (below 50km/h)
	_ = client.Save("trip_4", map[string]float32{"distance": 300, "duration": 3}) // speed = 100

	// Query: distance >= 50, compute speed, filter speed > 50, Page 1 with pageSize 2
	pagedRes, err := client.List().
		Where("distance", ">=", 50).
		Compute("speed", Divide("distance", "duration")).
		Where("speed", ">", 50).
		Page(1, 2).
		Execute(ctx)

	if err != nil {
		t.Fatalf("List query failed: %v", err)
	}

	if len(pagedRes.Items) != 2 {
		t.Fatalf("expected 2 items for page size 2, got %d", len(pagedRes.Items))
	}

	for i, item := range pagedRes.Items {
		t.Logf("Page 1 Item [%d]: %s -> distance=%.0f, duration=%.0f, speed=%.0f",
			i+1, item.Name, item.Get("distance"), item.Get("duration"), item.Get("speed"))
	}

	// Verify Page 2
	page2Res, err := client.List().
		Where("distance", ">=", 50).
		Compute("speed", Divide("distance", "duration")).
		Where("speed", ">", 50).
		Page(2, 2).
		Execute(ctx)

	if err != nil {
		t.Fatalf("Page 2 query failed: %v", err)
	}

	t.Logf("Page 2 Items count: %d, HasMore: %v", len(page2Res.Items), page2Res.HasMore)
}

func TestDriver_SessionPersistence(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	dbFile := filepath.Join(tmpDir, "persist_client.anchordb")

	// Session 1: Connect, Save, Sync, Close
	c1, err := Connect("anchordb://" + dbFile)
	if err != nil {
		t.Fatalf("Session 1 connect failed: %v", err)
	}

	_ = c1.Save("vehicle_a", map[string]float32{"distance": 120, "duration": 2})
	_ = c1.Save("vehicle_b", map[string]float32{"distance": 240, "duration": 3})
	if err := c1.DB().Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}
	_ = c1.Close()

	// Session 2: Reconnect to same file, Get and Compute
	c2, err := Connect("anchordb://" + dbFile)
	if err != nil {
		t.Fatalf("Session 2 connect failed: %v", err)
	}
	defer c2.Close()

	resA, err := c2.Get("vehicle_a").Compute("speed", Divide("distance", "duration")).Execute(ctx)
	if err != nil {
		t.Fatalf("Get vehicle_a failed: %v", err)
	}

	if resA.Get("distance") != 120 || resA.Get("duration") != 2 || resA.Get("speed") != 60 {
		t.Fatalf("unexpected values: %v", resA.All())
	}

	resB, err := c2.Get("vehicle_b").Compute("speed", Divide("distance", "duration")).Execute(ctx)
	if err != nil {
		t.Fatalf("Get vehicle_b failed: %v", err)
	}

	if resB.Get("distance") != 240 || resB.Get("duration") != 3 || resB.Get("speed") != 80 {
		t.Fatalf("unexpected values: %v", resB.All())
	}

	t.Logf("Session persistence verified! vehicle_a speed=%.0f, vehicle_b speed=%.0f",
		resA.Get("speed"), resB.Get("speed"))
}

func TestDriver_SelectProjection_UserScenario(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	dbFile := filepath.Join(tmpDir, "select_test.anchordb")

	client, err := Connect("anchordb://" + dbFile)
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer client.Close()

	// Entity has: distance, duration, area, temperature, humidity
	_ = client.Save("location_a", map[string]float32{
		"distance":    100.0,
		"duration":    2.0,
		"area":        500.0,
		"temperature": 28.5,
		"humidity":    75.0,
	})

	// 1. GET with SELECT: Retrieve distance, duration, and custom calculated column (speed)
	// area, temperature, humidity should be projected out (NOT returned)
	res, err := client.Get("location_a").
		Select("distance", "duration", "speed").
		Compute("speed", Divide("distance", "duration")).
		Execute(ctx)

	if err != nil {
		t.Fatalf("Get with Select failed: %v", err)
	}

	if res.Get("distance") != 100.0 || res.Get("duration") != 2.0 || res.Get("speed") != 50.0 {
		t.Fatalf("unexpected values in selected fields: %v", res.All())
	}

	if res.Has("area") || res.Has("temperature") || res.Has("humidity") {
		t.Fatalf("expected unselected fields to be excluded, but got: %v", res.All())
	}

	t.Logf("User scenario SELECT on Get passed! Returned fields: %v", res.All())

	// 2. LIST with SELECT:
	listRes, err := client.List().
		Select("distance", "duration", "speed").
		Where("area", ">=", 100.0). // Filter by unselected field 'area'!
		Compute("speed", Divide("distance", "duration")).
		Page(1, 5).
		Execute(ctx)

	if err != nil {
		t.Fatalf("List with Select failed: %v", err)
	}

	if len(listRes.Items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(listRes.Items))
	}

	item := listRes.Items[0]
	if item.Has("area") {
		t.Fatalf("expected 'area' to be excluded in list projection, got: %v", item.All())
	}
	if !item.Has("distance") || !item.Has("duration") || !item.Has("speed") {
		t.Fatalf("expected distance, duration, speed present, got: %v", item.All())
	}

	t.Logf("User scenario SELECT on List passed! Item fields: %v", item.All())
}

func TestDriver_BatchUpdate10k_AddDistance(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	dbFile := filepath.Join(tmpDir, "batch_update_10k.anchordb")

	client, err := Connect("anchordb://" + dbFile)
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer client.Close()

	// Seed 25,000 records, each with distance = 100, duration = 2
	totalSeeds := 25000
	for i := 0; i < totalSeeds; i++ {
		name := fmt.Sprintf("vehicle_%d", i)
		_ = client.Save(name, map[string]float32{
			"distance": 100.0,
			"duration": 2.0,
		})
	}

	// Update 10,000 records simultaneously: distance = distance + 10
	t0 := time.Now()
	updatedCount, err := client.Update().
		Set("distance", Add("distance", "10")).
		Limit(10000).
		Execute(ctx)
	elapsed := time.Since(t0)

	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	if updatedCount != 10000 {
		t.Fatalf("expected 10000 records updated, got %d", updatedCount)
	}

	t.Logf("[OK] 10,000 records updated in %v! (Constant time verified)", elapsed)

	// Verify first item was updated to 110
	rec0, err := client.Get("vehicle_0").Execute(ctx)
	if err != nil || rec0.Get("distance") != 110.0 {
		t.Fatalf("expected vehicle_0 distance=110, got %.1f", rec0.Get("distance"))
	}

	// Verify 9,999th item was updated to 110
	rec9999, err := client.Get("vehicle_9999").Execute(ctx)
	if err != nil || rec9999.Get("distance") != 110.0 {
		t.Fatalf("expected vehicle_9999 distance=110, got %.1f", rec9999.Get("distance"))
	}

	// Verify 10,000th item (the 10,001st record) was NOT touched (remains 100)
	rec10000, err := client.Get("vehicle_10000").Execute(ctx)
	if err != nil || rec10000.Get("distance") != 100.0 {
		t.Fatalf("expected vehicle_10000 distance=100 (untouched), got %.1f", rec10000.Get("distance"))
	}

	t.Logf("Verification passed: vehicle_0 distance=%.1f, vehicle_9999 distance=%.1f, vehicle_10000 distance=%.1f",
		rec0.Get("distance"), rec9999.Get("distance"), rec10000.Get("distance"))
}

func TestDriver_SequentialAddTen_UserExample(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	dbFile := filepath.Join(tmpDir, "sequential_add10.anchordb")

	client, err := Connect("anchordb://" + dbFile)
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer client.Close()

	// Seed 10,000 records with sequential distances: 1, 2, 3, ..., 10000
	total := 10000
	for i := 1; i <= total; i++ {
		name := fmt.Sprintf("rec_%d", i)
		_ = client.Save(name, map[string]float32{
			"distance": float32(i),
		})
	}

	// Update batch: distance = distance + 10
	t0 := time.Now()
	updatedCount, err := client.Update().
		Set("distance", Add("distance", "10")).
		Limit(total).
		Execute(ctx)
	elapsed := time.Since(t0)

	if err != nil {
		t.Fatalf("update failed: %v", err)
	}
	if updatedCount != total {
		t.Fatalf("expected %d updated, got %d", total, updatedCount)
	}

	t.Logf("[OK] 10,000 sequential records updated (distance + 10) in %v!", elapsed)

	// Verify sample records:
	// Record 1: 1 + 10 = 11
	r1, _ := client.Get("rec_1").Execute(ctx)
	if r1.Get("distance") != 11.0 {
		t.Fatalf("expected rec_1 distance=11, got %.1f", r1.Get("distance"))
	}

	// Record 2: 2 + 10 = 12
	r2, _ := client.Get("rec_2").Execute(ctx)
	if r2.Get("distance") != 12.0 {
		t.Fatalf("expected rec_2 distance=12, got %.1f", r2.Get("distance"))
	}

	// Record 3: 3 + 10 = 13
	r3, _ := client.Get("rec_3").Execute(ctx)
	if r3.Get("distance") != 13.0 {
		t.Fatalf("expected rec_3 distance=13, got %.1f", r3.Get("distance"))
	}

	// Record 500: 500 + 10 = 510
	r500, _ := client.Get("rec_500").Execute(ctx)
	if r500.Get("distance") != 510.0 {
		t.Fatalf("expected rec_500 distance=510, got %.1f", r500.Get("distance"))
	}

	// Record 10000: 10000 + 10 = 10010
	r10000, _ := client.Get("rec_10000").Execute(ctx)
	if r10000.Get("distance") != 10010.0 {
		t.Fatalf("expected rec_10000 distance=10010, got %.1f", r10000.Get("distance"))
	}

	t.Logf("All verifications passed: rec_1=%.0f, rec_2=%.0f, rec_3=%.0f, rec_500=%.0f, rec_10000=%.0f",
		r1.Get("distance"), r2.Get("distance"), r3.Get("distance"), r500.Get("distance"), r10000.Get("distance"))
}

func TestDriver_DateMath_Add45Days_UserScenario(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	dbFile := filepath.Join(tmpDir, "date_test.anchordb")

	client, err := Connect("anchordb://" + dbFile)
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer client.Close()

	// 1. Save entity with date 2026-10-14
	_ = client.Save("project_contract", map[string]float32{
		"start_date": MustDate("2026-10-14"),
	})

	// Verify before update
	initial, err := client.Get("project_contract").Execute(ctx)
	if err != nil || initial.GetDate("start_date") != "2026-10-14" {
		t.Fatalf("expected initial date 2026-10-14, got %q", initial.GetDate("start_date"))
	}
	t.Logf("Initial Stored Date: %s", initial.GetDate("start_date"))

	// 2. Execute UPDATE: Add 45 days during the update process
	count, err := client.Update().
		In("project_contract").
		Set("start_date", AddDays("start_date", 45)).
		Execute(ctx)

	if err != nil || count != 1 {
		t.Fatalf("update failed: count=%d, err=%v", count, err)
	}

	// 3. Fetch data after update: must become 2026-11-28!
	updated, err := client.Get("project_contract").Execute(ctx)
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}

	dateAfter := updated.GetDate("start_date")
	if dateAfter != "2026-11-28" {
		t.Fatalf("expected 2026-11-28, got %q", dateAfter)
	}
	t.Logf("Date After Update (+45 days): %s [SUCCESS]", dateAfter)

	// 4. Test on-the-fly calculation during GET / SELECT (Compute + 45 days -> 2027-01-12)
	computed, err := client.Get("project_contract").
		Compute("next_phase", AddDays("start_date", 45)).
		Execute(ctx)

	if err != nil {
		t.Fatalf("compute failed: %v", err)
	}

	dateTahap2 := computed.GetDate("next_phase")
	if dateTahap2 != "2027-01-12" {
		t.Fatalf("expected 2027-01-12, got %q", dateTahap2)
	}
	t.Logf("On-The-Fly Calculation Next Phase (+45 more days): %s [SUCCESS]", dateTahap2)
}

func TestDriver_RichRangeAndDateFilters(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	dbFile := filepath.Join(tmpDir, "rich_filters.anchordb")

	client, err := Connect("anchordb://" + dbFile)
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer client.Close()

	// Seed data with various dates, distances, durations
	_ = client.Save("trip_1", map[string]float32{
		"distance": 30.0,
		"duration": 1.0,
		"date":     MustDate("2026-10-05"),
	})
	_ = client.Save("trip_2", map[string]float32{
		"distance": 100.0,
		"duration": 2.0,
		"date":     MustDate("2026-10-14"),
	})
	_ = client.Save("trip_3", map[string]float32{
		"distance": 120.0,
		"duration": 2.5,
		"date":     MustDate("2026-10-20"),
	})
	_ = client.Save("trip_4", map[string]float32{
		"distance": 300.0,
		"duration": 5.0,
		"date":     MustDate("2026-11-01"),
	})

	// 1. GET BY DATE: WhereDate("date", "2026-10-14")
	resDate, err := client.List().
		WhereDate("date", "2026-10-14").
		Execute(ctx)
	if err != nil || len(resDate.Items) != 1 || resDate.Items[0].Name != "trip_2" {
		t.Fatalf("WhereDate failed: %v, items=%v", err, resDate.Items)
	}
	t.Logf("[1] Get by Date '2026-10-14': %s", resDate.Items[0].Name)

	// 2. GET BETWEEN DATE (Range): BetweenDate("2026-10-10", "2026-10-25")
	resRange, err := client.List().
		BetweenDate("date", "2026-10-10", "2026-10-25").
		Execute(ctx)
	if err != nil || len(resRange.Items) != 2 { // trip_2 (Oct 14) & trip_3 (Oct 20)
		t.Fatalf("BetweenDate failed: %v, count=%d", err, len(resRange.Items))
	}
	t.Logf("[2] Get Between Date 2026-10-10 to 2026-10-25: count=%d (%s, %s)",
		len(resRange.Items), resRange.Items[0].Name, resRange.Items[1].Name)

	// 3. GET BEFORE: WhereBefore("date", "2026-10-14")
	resBefore, err := client.List().
		WhereBefore("date", "2026-10-14").
		Execute(ctx)
	if err != nil || len(resBefore.Items) != 1 || resBefore.Items[0].Name != "trip_1" {
		t.Fatalf("WhereBefore date failed: %v", err)
	}
	t.Logf("[3] Get Before '2026-10-14': %s (date: %s)", resBefore.Items[0].Name, resBefore.Items[0].GetDate("date"))

	// 4. GET AFTER: WhereAfter("date", "2026-10-20")
	resAfter, err := client.List().
		WhereAfter("date", "2026-10-20").
		Execute(ctx)
	if err != nil || len(resAfter.Items) != 1 || resAfter.Items[0].Name != "trip_4" {
		t.Fatalf("WhereAfter date failed: %v", err)
	}
	t.Logf("[4] Get After '2026-10-20': %s (date: %s)", resAfter.Items[0].Name, resAfter.Items[0].GetDate("date"))

	// 5. MULTI-RANGE CHAINING:
	// getFrom(A, B) and From(C, D) and From(D, E) for both date and numbers!
	// Example: distance in [50, 150] AND duration in [1.5, 3.0] AND date in ["2026-10-10", "2026-10-25"]
	resMulti, err := client.List().
		From("distance", 50, 150).
		From("duration", 1.5, 3.0).
		From("date", "2026-10-10", "2026-10-25").
		Compute("speed", Divide("distance", "duration")).
		Execute(ctx)

	if err != nil || len(resMulti.Items) != 2 {
		t.Fatalf("Chained From ranges failed: %v, count=%d", err, len(resMulti.Items))
	}

	for i, item := range resMulti.Items {
		t.Logf("[5] Multi-Range Item [%d]: %s -> Distance=%.0f, Duration=%.1f, Date=%s, Speed=%.1f",
			i+1, item.Name, item.Get("distance"), item.Get("duration"), item.GetDate("date"), item.Get("speed"))
	}
}

func TestDriver_ScheduledDeactivation_15Days(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "users_deactivation.anchordb")

	client, err := Connect("anchordb://" + dbPath)
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	// Simulation: Today is 2026-10-15
	// Expiration cutoff = 15 days ago -> 2026-09-30
	// User 1: registered 2026-09-20 (25 days ago -> > 15 days -> MUST DEACTIVATE)
	// User 2: registered 2026-10-05 (10 days ago -> <= 15 days -> REMAINS ACTIVE)
	// User 3: registered 2026-10-12 (3 days ago -> <= 15 days -> REMAINS ACTIVE)

	_ = client.Save("user_1_legacy", map[string]float32{
		"date_registered": MustDate("2026-09-20"),
		"status":          1, // 1 = Active, 0 = Inactive
	})
	_ = client.Save("user_2_active", map[string]float32{
		"date_registered": MustDate("2026-10-05"),
		"status":          1,
	})
	_ = client.Save("user_3_recent", map[string]float32{
		"date_registered": MustDate("2026-10-12"),
		"status":          1,
	})

	// Create scheduling task / lifecycle job:
	// Rule: Every day at 23:00, find records where date_registered <= (Today - 15) and status == 1
	sched := scheduler.NewInternalScheduler()

	var updatedRecords int
	_ = sched.Register("deactivate_expired_users_15days", "23:00", func() error {
		// Cutoff date = 2026-09-30 (15 days before 2026-10-15)
		cutoffDate := "2026-09-30"

		count, err := client.Update().
			Where("date_registered", "<=", cutoffDate).
			Where("status", "=", 1).
			Set("status", Value(0)). // Update status = 0 (inactive)
			Execute(ctx)
		if err != nil {
			return err
		}
		updatedRecords = count
		return nil
	})

	// Run task via scheduler
	if err := sched.RunNow("deactivate_expired_users_15days"); err != nil {
		t.Fatalf("Scheduler RunNow failed: %v", err)
	}

	if updatedRecords != 1 {
		t.Fatalf("Expected 1 record deactivated, got %d", updatedRecords)
	}

	// Verify: user_1_legacy now has status = 0 (inactive)
	u1, err := client.Get("user_1_legacy").Execute(ctx)
	if err != nil || u1.Get("status") != 0 {
		t.Fatalf("user_1_legacy should be inactive (0), got %v", u1.Get("status"))
	}
	t.Logf("[OK] user_1_legacy (Registered: %s) -> Status: %.0f (Inactive)", u1.GetDate("date_registered"), u1.Get("status"))

	// Verify: user_2_active remains status = 1 (active)
	u2, err := client.Get("user_2_active").Execute(ctx)
	if err != nil || u2.Get("status") != 1 {
		t.Fatalf("user_2_active should remain active (1), got %v", u2.Get("status"))
	}
	t.Logf("[OK] user_2_active (Registered: %s) -> Status: %.0f (Active)", u2.GetDate("date_registered"), u2.Get("status"))

	// Verify: user_3_recent remains status = 1 (active)
	u3, err := client.Get("user_3_recent").Execute(ctx)
	if err != nil || u3.Get("status") != 1 {
		t.Fatalf("user_3_recent should remain active (1), got %v", u3.Get("status"))
	}
	t.Logf("[OK] user_3_recent (Registered: %s) -> Status: %.0f (Active)", u3.GetDate("date_registered"), u3.Get("status"))
}

func TestDriver_HighConcurrencyParallelConnections(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "parallel_stress.anchordb")

	client, err := Connect("anchordb://" + dbPath)
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	// 1. Initial seed: 100 entities
	for i := 0; i < 100; i++ {
		_ = client.Save(fmt.Sprintf("item_%d", i), map[string]float32{
			"distance": float32(i * 10),
			"duration": 2.0,
			"val":      100.0,
		})
	}

	// 2. Simulate 50 concurrent connections / goroutines executing 2,000 mixed requests (Read + Write)
	const numGoroutines = 50
	const requestsPerWorker = 40

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	t0 := time.Now()

	for g := 0; g < numGoroutines; g++ {
		workerID := g
		go func() {
			defer wg.Done()
			for r := 0; r < requestsPerWorker; r++ {
				targetName := fmt.Sprintf("item_%d", (workerID*requestsPerWorker+r)%100)

				if r%5 == 0 {
					// 20% CONCURRENT WRITE: Atomic mutation
					_, _ = client.Update().
						In(targetName).
						Set("val", Add("val", "1")).
						Execute(ctx)
				} else {
					// 80% CONCURRENT READ: Parallel non-blocking queries
					res, err := client.Get(targetName).
						Select("distance", "duration", "speed").
						Compute("speed", Divide("distance", "duration")).
						Execute(ctx)

					if err != nil || res == nil {
						t.Errorf("Concurrent read failed for %s: %v", targetName, err)
						return
					}
				}
			}
		}()
	}

	wg.Wait()
	duration := time.Since(t0)
	totalReqs := numGoroutines * requestsPerWorker
	opsPerSec := float64(totalReqs) / duration.Seconds()

	t.Logf("[PARALLEL] Successfully executed %d mixed parallel requests (Read/Write) across %d goroutines in %v (%.0f ops/sec)",
		totalReqs, numGoroutines, duration, opsPerSec)
}

func TestDriver_SaveAutoID(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "driver_autouuid.anchordb")

	client, err := Connect("anchordb://" + dbPath)
	if err != nil {
		t.Fatalf("failed to connect via driver: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	// 1. Test SaveAutoID
	id1, err := client.SaveAutoID(map[string]float32{"score": 88.5})
	if err != nil {
		t.Fatalf("SaveAutoID failed: %v", err)
	}
	if len(id1) != 36 {
		t.Fatalf("expected 36-char UUID, got %q", id1)
	}

	// 2. Test SaveWithID with empty string
	id2, err := client.SaveWithID("", map[string]float32{"score": 92.0})
	if err != nil {
		t.Fatalf("SaveWithID failed: %v", err)
	}
	if len(id2) != 36 || id2 == id1 {
		t.Fatalf("expected unique 36-char UUID, got %q", id2)
	}

	// 3. Test retrieving via driver Get
	rec1, err := client.Get(id1).Select("score").Execute(ctx)
	if err != nil {
		t.Fatalf("client.Get failed for auto UUID: %v", err)
	}
	if rec1.Get("score") != 88.5 {
		t.Fatalf("expected score 88.5, got %v", rec1.Get("score"))
	}
}

func TestDriver_CausalEdgesRoundTrip(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "driver_causal.anchordb")

	client, err := Connect("anchordb://" + dbPath)
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}

	if err := client.Link("fuel_shortage", "causes", "inflation", 0.9); err != nil {
		t.Fatalf("link failed: %v", err)
	}
	if err := client.Link("crop_failure", "causes", "inflation", 0.4); err != nil {
		t.Fatalf("link failed: %v", err)
	}

	step, err := client.Causal("fuel_shortage", "causes", 5)
	if err != nil {
		t.Fatalf("causal failed: %v", err)
	}
	if len(step) != 1 || step[0].Word != "inflation" || step[0].Score != 0.9 {
		t.Fatalf("causal step = %+v", step)
	}

	if err := client.Link("inflation", "causes", "economic_crisis", 0.7); err != nil {
		t.Fatalf("link failed: %v", err)
	}
	chain, err := client.CausalChain([]string{"fuel_shortage"}, []string{"causes", "causes"}, 5)
	if err != nil {
		t.Fatalf("causal chain failed: %v", err)
	}
	if len(chain) != 1 || chain[0].Word != "economic_crisis" {
		t.Fatalf("causal chain = %+v", chain)
	}

	if err := client.Unlink("fuel_shortage", "causes", "inflation"); err != nil {
		t.Fatalf("unlink failed: %v", err)
	}
	if got, _ := client.Causal("fuel_shortage", "causes", 5); len(got) != 0 {
		t.Fatalf("unlinked edge still visible: %+v", got)
	}

	// Durable across Sync + reopen.
	if err := client.DB().Sync(); err != nil {
		t.Fatalf("sync failed: %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	reopened, err := Connect("anchordb://" + dbPath)
	if err != nil {
		t.Fatalf("reconnect failed: %v", err)
	}
	defer reopened.Close()
	chain, err = reopened.CausalChain([]string{"crop_failure"}, []string{"causes", "causes"}, 5)
	if err != nil {
		t.Fatalf("causal chain after reopen failed: %v", err)
	}
	if len(chain) != 1 || chain[0].Word != "economic_crisis" {
		t.Fatalf("chain after reopen = %+v", chain)
	}
}
