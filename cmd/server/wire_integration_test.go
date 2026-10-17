package main

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/rachmanzz/anchordb/pkg/driver"
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
}
