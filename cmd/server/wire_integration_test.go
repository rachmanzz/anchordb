package main

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/rachmanzz/anchordb/pkg/driver"
	"github.com/rachmanzz/anchordb/pkg/spatial"
	"github.com/rachmanzz/anchordb/pkg/storage"
)

func TestServer_AnchorWire_Integration(t *testing.T) {
	s, dir := createTestServer(t)
	defer cleanupServer(t, s, dir)

	// Listen on dynamic TCP port for AnchorWire
	wireListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to bind dynamic wire port: %v", err)
	}
	wireAddr := wireListener.Addr().String()

	go func() {
		_ = s.WireServer().Serve(wireListener)
	}()

	// Connect client using driver.ConnectWire
	client, err := driver.ConnectWire(wireAddr,
		driver.WithModel("wire_integration"),
		driver.WithTimeout(5*time.Second),
	)
	if err != nil {
		t.Fatalf("ConnectWire failed: %v", err)
	}
	defer client.Close()

	// 1. Save single entity
	props := map[string]float32{
		"voltage":     24.0,
		"temperature": 32.5,
	}
	if err := client.Save("machine_01", props); err != nil {
		t.Fatalf("Save via wire failed: %v", err)
	}

	// 2. Get entity
	ctx := context.Background()
	res, err := client.Get("machine_01").Execute(ctx)
	if err != nil {
		t.Fatalf("Get via wire failed: %v", err)
	}
	if res.Get("voltage") != 24.0 || res.Get("temperature") != 32.5 {
		t.Fatalf("unexpected properties retrieved: %+v", res)
	}

	// 3. Batch Save via wire
	const batchCount = 50
	batch := make([]driver.BatchRecord, batchCount)
	for i := 0; i < batchCount; i++ {
		batch[i] = driver.BatchRecord{
			Name:      fmt.Sprintf("part_auto_%04d", i),
			Operation: storage.OpStatusActive,
			Props: map[string]float32{
				"stock": float32(100 + i),
				"price": float32(10 + i*2),
			},
		}
	}
	names, err := client.SaveBatch(batch)
	if err != nil {
		t.Fatalf("SaveBatch via wire failed: %v", err)
	}
	if len(names) != batchCount {
		t.Fatalf("expected %d names, got %d", batchCount, len(names))
	}

	// 4. Graph Edge linking and Causal Traversal via wire
	if err := client.Link("machine_01", "assembles", "part_auto_0005", 0.92); err != nil {
		t.Fatalf("Link via wire failed: %v", err)
	}

	causalRes, err := client.Causal("machine_01", "assembles", 5)
	if err != nil {
		t.Fatalf("Causal via wire failed: %v", err)
	}
	if len(causalRes) != 1 || causalRes[0].Word != "part_auto_0005" || causalRes[0].Score != 0.92 {
		t.Fatalf("unexpected causal traversal result: %+v", causalRes)
	}

	// 5. Connect via Connect() with wire:// scheme
	client2, err := driver.Connect("wire://" + wireAddr + "?model=wire_integration")
	if err != nil {
		t.Fatalf("Connect with wire:// scheme failed: %v", err)
	}
	defer client2.Close()

	res2, err := client2.Get("part_auto_0005").Execute(ctx)
	if err != nil {
		t.Fatalf("Get part_auto_0005 via client2 failed: %v", err)
	}
	if res2.Get("stock") != 105.0 {
		t.Fatalf("expected stock=105.0, got %f", res2.Get("stock"))
	}

	// 6. Connect with Adaptive Pacing enabled
	pacedClient, err := driver.ConnectWire(wireAddr,
		driver.WithModel("wire_integration"),
		driver.WithAdaptivePacing(10000),
		driver.WithTimeout(5*time.Second),
	)
	if err != nil {
		t.Fatalf("ConnectWire with adaptive pacing failed: %v", err)
	}
	defer pacedClient.Close()

	pacedBatch := make([]driver.BatchRecord, 25)
	for i := 0; i < 25; i++ {
		pacedBatch[i] = driver.BatchRecord{
			Name: fmt.Sprintf("paced_item_%04d", i),
			Props: map[string]float32{
				"score": float32(i) * 1.2,
			},
		}
	}
	pacedNames, err := pacedClient.SaveBatch(pacedBatch)
	if err != nil {
		t.Fatalf("SaveBatch under adaptive pacing failed: %v", err)
	}
	if len(pacedNames) != 25 {
		t.Fatalf("expected 25 paced items, got %d", len(pacedNames))
	}
}

func TestServer_AnchorWire_FullFeatureParity(t *testing.T) {
	s, dir := createTestServer(t)
	defer cleanupServer(t, s, dir)

	wireListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to bind dynamic wire port: %v", err)
	}
	wireAddr := wireListener.Addr().String()

	go func() {
		_ = s.WireServer().Serve(wireListener)
	}()

	client, err := driver.Connect("wire://"+wireAddr+"?model=ecommerce",
		driver.WithTimeout(5*time.Second),
	)
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	// 1. SetRelation directional vector
	if err := client.SetRelation("supply_flow", spatial.Point2D{X: 1, Y: 0}); err != nil {
		t.Fatalf("client.SetRelation via wire failed: %v", err)
	}

	// 2. AppendNode
	if err := client.AppendNode("warehouse_a", spatial.Point2D{X: 0, Y: 0}, [4]float32{1, 0, 0, 0}); err != nil {
		t.Fatalf("AppendNode warehouse_a failed: %v", err)
	}
	if err := client.AppendNode("store_b", spatial.Point2D{X: 10, Y: 0}, [4]float32{1, 0, 0, 0}); err != nil {
		t.Fatalf("AppendNode store_b failed: %v", err)
	}

	// Populate inventory properties
	if err := client.Save("warehouse_a", map[string]float32{"quantity": 50, "price": 10.0, "bonus": 5.0}); err != nil {
		t.Fatalf("Save warehouse_a failed: %v", err)
	}
	if err := client.Save("store_b", map[string]float32{"quantity": 10, "price": 15.0, "bonus": 2.0}); err != nil {
		t.Fatalf("Save store_b failed: %v", err)
	}

	// 3. Schema Attach & Get
	schema := storage.ModelSchema{
		"quantity": "int",
		"price":    "float",
		"bonus":    "float",
	}
	if err := client.Attach(schema); err != nil {
		t.Fatalf("client.Attach via wire failed: %v", err)
	}
	gotSchema, err := client.GetSchema(ctx)
	if err != nil {
		t.Fatalf("client.GetSchema via wire failed: %v", err)
	}
	if gotSchema["quantity"] != "int" || gotSchema["price"] != "float" {
		t.Fatalf("retrieved schema mismatch: %v", gotSchema)
	}

	// 4. Get with Select projection and on-the-fly Computation
	getRes, err := client.Get("warehouse_a").
		Select("quantity", "price", "subtotal").
		Compute("subtotal", driver.Multiply("quantity", "price")).
		Execute(ctx)
	if err != nil {
		t.Fatalf("Get with compute via wire failed: %v", err)
	}
	if getRes.Get("subtotal") != 500.0 {
		t.Fatalf("expected subtotal=500.0, got %f", getRes.Get("subtotal"))
	}

	// 5. List with Filtering, Projections, Computations, and Paging
	listRes, err := client.List().
		Where("quantity", ">=", 10.0).
		Compute("subtotal", driver.Multiply("quantity", "price")).
		Select("quantity", "price", "subtotal").
		Execute(ctx)
	if err != nil {
		t.Fatalf("List query via wire failed: %v", err)
	}
	if len(listRes.Items) != 2 {
		t.Fatalf("expected 2 items from List via wire, got %d", len(listRes.Items))
	}

	// 6. Update with filtering and mutation
	updatedCount, err := client.Update().
		Where("quantity", "=", 10.0).
		Set("quantity", driver.Add("quantity", "bonus")).
		Execute(ctx)
	if err != nil {
		t.Fatalf("Update via wire failed: %v", err)
	}
	if updatedCount != 1 {
		t.Fatalf("expected 1 record updated via wire, got %d", updatedCount)
	}
	// Verify store_b quantity was updated from 10 to 12 (10 + 2)
	storeRes, err := client.Get("store_b").Execute(ctx)
	if err != nil || storeRes.Get("quantity") != 12.0 {
		t.Fatalf("expected store_b quantity 12.0, got %f, err=%v", storeRes.Get("quantity"), err)
	}

	// 7. Spatial Math computations over wire
	dist, err := client.Math().Distance("warehouse_a", "store_b")
	if err != nil {
		t.Fatalf("Distance via wire failed: %v", err)
	}
	if dist != 10.0 {
		t.Fatalf("expected distance 10.0, got %f", dist)
	}

	centroid, err := client.Math().Centroid("warehouse_a", "store_b")
	if err != nil {
		t.Fatalf("Centroid via wire failed: %v", err)
	}
	if centroid.Coord.X != 5.0 {
		t.Fatalf("expected centroid X=5.0, got %f", centroid.Coord.X)
	}

	sim, err := client.Math().CosineSimilarity("warehouse_a", "store_b")
	if err != nil {
		t.Fatalf("CosineSimilarity via wire failed: %v", err)
	}
	if sim != 1.0 {
		t.Fatalf("expected similarity 1.0, got %f", sim)
	}

	neighbors, err := client.Math().Nearest("warehouse_a", 2)
	if err != nil {
		t.Fatalf("Nearest via wire failed: %v", err)
	}
	if len(neighbors) == 0 {
		t.Fatalf("expected nearest neighbors via wire, got 0")
	}

	// 8. Graph beam step along supply_flow relation
	graphRes, err := client.Graph().
		From("warehouse_a").
		Relate("supply_flow").
		TopK(5).
		BeamWidth(2.0).
		Execute(ctx)
	if err != nil {
		t.Fatalf("Graph step via wire failed: %v", err)
	}
	if len(graphRes.Results) == 0 {
		t.Fatalf("expected graph results along supply_flow, got 0")
	}

	// 9. ResetModel via wire
	if err := client.ResetModel(ctx, true); err != nil {
		t.Fatalf("ResetModel via wire failed: %v", err)
	}
	if _, err := client.GetSchema(ctx); err == nil {
		t.Fatalf("expected error from GetSchema after ResetModel, got nil")
	}
}

func TestServer_AnchorWire_MultiModel_Isolation(t *testing.T) {
	s, dir := createTestServer(t)
	defer cleanupServer(t, s, dir)

	wireListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to bind dynamic wire port: %v", err)
	}
	wireAddr := wireListener.Addr().String()

	go func() {
		_ = s.WireServer().Serve(wireListener)
	}()

	client, err := driver.Connect("wire://" + wireAddr + "?model=default")
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer client.Close()

	clientM1 := client.Model("m1")
	clientM2 := client.Model("m2")

	const n = 100
	var wg sync.WaitGroup
	wg.Add(2)

	// Goroutine 1 writes to m1
	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			name := fmt.Sprintf("rec_m1_%03d", i)
			_ = clientM1.Save(name, map[string]float32{"tag": 1.0})
		}
	}()

	// Goroutine 2 writes to m2
	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			name := fmt.Sprintf("rec_m2_%03d", i)
			_ = clientM2.Save(name, map[string]float32{"tag": 2.0})
		}
	}()

	wg.Wait()

	// Verify all m1 records are in m1 and NONE in m2
	ctx := context.Background()
	for i := 0; i < n; i++ {
		name1 := fmt.Sprintf("rec_m1_%03d", i)
		if _, err := clientM1.Get(name1).Execute(ctx); err != nil {
			t.Errorf("record %s should be in m1, got err: %v", name1, err)
		}
		if _, err := clientM2.Get(name1).Execute(ctx); err == nil {
			t.Errorf("record %s should NOT be in m2, but was found!", name1)
		}

		name2 := fmt.Sprintf("rec_m2_%03d", i)
		if _, err := clientM2.Get(name2).Execute(ctx); err != nil {
			t.Errorf("record %s should be in m2, got err: %v", name2, err)
		}
		if _, err := clientM1.Get(name2).Execute(ctx); err == nil {
			t.Errorf("record %s should NOT be in m1, but was found!", name2)
		}
	}
}

