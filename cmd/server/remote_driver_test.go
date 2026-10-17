package main

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"

	"github.com/rachmanzz/anchordb/pkg/driver"
	"github.com/rachmanzz/anchordb/pkg/spatial"
)

func TestRemoteDriver_ConnectAndSaveGetCompute(t *testing.T) {
	s, dir := createTestServer(t)
	defer cleanupServer(t, s, dir)

	s.authToken = "secret-token-123"
	ts := httptest.NewServer(s.Routes())
	defer ts.Close()

	ctx := context.Background()

	// 1. Connect over HTTP using ConnectRemote
	client, err := driver.ConnectRemote(ts.URL,
		driver.WithToken("secret-token-123"),
		driver.WithModel("plant_iot"),
	)
	if err != nil {
		t.Fatalf("ConnectRemote failed: %v", err)
	}
	defer client.Close()

	if !client.IsRemote() {
		t.Errorf("expected IsRemote() to be true")
	}
	if client.IsLocal() {
		t.Errorf("expected IsLocal() to be false")
	}
	if client.ModelName() != "plant_iot" {
		t.Errorf("expected model 'plant_iot', got %q", client.ModelName())
	}
	if client.DB() != nil {
		t.Errorf("expected DB() to be nil for remote client")
	}

	// 2. Save record
	err = client.Save("sensor_soil_1", map[string]float32{
		"moisture": 45.5,
		"depth":    10.0,
	})
	if err != nil {
		t.Fatalf("client.Save failed: %v", err)
	}

	// 3. Get plain record
	res, err := client.Get("sensor_soil_1").Execute(ctx)
	if err != nil {
		t.Fatalf("client.Get failed: %v", err)
	}
	if res.Name != "sensor_soil_1" {
		t.Errorf("expected name 'sensor_soil_1', got %q", res.Name)
	}
	if res.Props()["moisture"] != 45.5 {
		t.Errorf("expected moisture 45.5, got %v", res.Props()["moisture"])
	}

	// 4. Get with Compute
	resComputed, err := client.Get("sensor_soil_1").
		Compute("moisture_per_depth", driver.Divide("moisture", "depth")).
		Execute(ctx)
	if err != nil {
		t.Fatalf("client.Get with Compute failed: %v", err)
	}
	expectedRatio := float32(45.5 / 10.0)
	if resComputed.Props()["moisture_per_depth"] != expectedRatio {
		t.Errorf("expected computed ratio %v, got %v", expectedRatio, resComputed.Props()["moisture_per_depth"])
	}

	// 5. Get with Select projection
	resProj, err := client.Get("sensor_soil_1").
		Compute("ratio", driver.Divide("moisture", "depth")).
		Select("ratio").
		Execute(ctx)
	if err != nil {
		t.Fatalf("client.Get with Select failed: %v", err)
	}
	if _, ok := resProj.Props()["moisture"]; ok {
		t.Errorf("expected moisture to be excluded by Select projection")
	}
	if resProj.Props()["ratio"] != expectedRatio {
		t.Errorf("expected ratio %v, got %v", expectedRatio, resProj.Props()["ratio"])
	}
}

func TestRemoteDriver_AutoIDListAndBatchUpdate(t *testing.T) {
	s, dir := createTestServer(t)
	defer cleanupServer(t, s, dir)

	s.authToken = "test-token"
	ts := httptest.NewServer(s.Routes())
	defer ts.Close()

	ctx := context.Background()

	client, err := driver.Connect(ts.URL + "?token=test-token&model=greenhouse")
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer client.Close()

	// 1. SaveAutoID
	id1, err := client.SaveAutoID(map[string]float32{"temp": 22.0, "humidity": 60.0})
	if err != nil {
		t.Fatalf("SaveAutoID 1 failed: %v", err)
	}
	id2, err := client.SaveAutoID(map[string]float32{"temp": 28.5, "humidity": 80.0})
	if err != nil {
		t.Fatalf("SaveAutoID 2 failed: %v", err)
	}
	if id1 == "" || id2 == "" || id1 == id2 {
		t.Fatalf("invalid generated IDs: %q, %q", id1, id2)
	}

	// 2. List with Where, Compute, and Paging
	paged, err := client.List().
		Where("humidity", ">=", 50.0).
		Compute("heat_index", driver.Add("temp", "humidity")).
		Page(1, 10).
		Execute(ctx)
	if err != nil {
		t.Fatalf("client.List failed: %v", err)
	}
	if len(paged.Items) < 2 {
		t.Fatalf("expected at least 2 items, got %d", len(paged.Items))
	}

	// 3. Update records with compute mutation
	updatedCount, err := client.Update().
		Where("humidity", ">=", 70.0).
		Set("temp", driver.Add("temp", "2.0")).
		Execute(ctx)
	if err != nil {
		t.Fatalf("client.Update failed: %v", err)
	}
	if updatedCount != 1 {
		t.Errorf("expected 1 record updated, got %d", updatedCount)
	}

	// 4. Verify updated record
	res2, err := client.Get(id2).Execute(ctx)
	if err != nil {
		t.Fatalf("client.Get id2 failed: %v", err)
	}
	if res2.Props()["temp"] != 30.5 {
		t.Errorf("expected temp to be updated to 30.5, got %v", res2.Props()["temp"])
	}
}

func TestRemoteDriver_GraphCausalAndMath(t *testing.T) {
	s, dir := createTestServer(t)
	defer cleanupServer(t, s, dir)

	ts := httptest.NewServer(s.Routes())
	defer ts.Close()

	ctx := context.Background()

	client, err := driver.ConnectRemote(ts.URL, driver.WithModel("plant_mesh"))
	if err != nil {
		t.Fatalf("ConnectRemote failed: %v", err)
	}
	defer client.Close()

	// 1. Append nodes
	err = client.AppendNode("sensor_hub", spatial.Point2D{X: 0.2, Y: 0.9}, [4]float32{0.9, 0.1, 0.2, 0.2})
	if err != nil {
		t.Fatalf("AppendNode sensor_hub failed: %v", err)
	}
	err = client.AppendNode("sprinkler_valve", spatial.Point2D{X: 0.2, Y: 0.1}, [4]float32{0.2, 0.1, 0.95, 0.2})
	if err != nil {
		t.Fatalf("AppendNode sprinkler_valve failed: %v", err)
	}

	// 2. SetRelation and Graph Query
	err = client.SetRelation("controls", spatial.Point2D{Y: -1})
	if err != nil {
		t.Fatalf("SetRelation failed: %v", err)
	}

	graphResp, err := client.Graph().
		From("sensor_hub").
		Relate("controls").
		TopK(5).
		Execute(ctx)
	if err != nil {
		t.Fatalf("Graph.Execute failed: %v", err)
	}
	if len(graphResp.Results) == 0 {
		t.Fatalf("expected graph results, got 0")
	}
	if graphResp.Results[0].Word != "sprinkler_valve" {
		t.Errorf("expected top result 'sprinkler_valve', got %q", graphResp.Results[0].Word)
	}

	// 3. Link Causal Edge and Query
	err = client.Link("sensor_hub", "triggers", "sprinkler_valve", 0.95)
	if err != nil {
		t.Fatalf("Link failed: %v", err)
	}

	causalResp, err := client.Causal("sensor_hub", "triggers", 5)
	if err != nil {
		t.Fatalf("Causal query failed: %v", err)
	}
	if len(causalResp) == 0 || causalResp[0].Word != "sprinkler_valve" {
		t.Fatalf("expected causal target 'sprinkler_valve', got %+v", causalResp)
	}

	// 4. Direct Math: Distance, Centroid, CosineSimilarity, Nearest
	mathEngine := client.Math()
	dist, err := mathEngine.Distance("sensor_hub", "sprinkler_valve")
	if err != nil {
		t.Fatalf("Math.Distance failed: %v", err)
	}
	if dist <= 0 {
		t.Errorf("expected positive distance, got %v", dist)
	}

	centroid, err := mathEngine.Centroid("sensor_hub", "sprinkler_valve")
	if err != nil {
		t.Fatalf("Math.Centroid failed: %v", err)
	}
	if centroid.Coord.X != 0.2 {
		t.Errorf("expected centroid X=0.2, got %v", centroid.Coord.X)
	}

	sim, err := mathEngine.CosineSimilarity("sensor_hub", "sprinkler_valve")
	if err != nil {
		t.Fatalf("Math.CosineSimilarity failed: %v", err)
	}
	if sim <= 0 {
		t.Errorf("expected positive similarity, got %v", sim)
	}

	nearest, err := mathEngine.Nearest("sensor_hub", 5)
	if err != nil {
		t.Fatalf("Math.Nearest failed: %v", err)
	}
	if len(nearest) == 0 {
		t.Fatalf("expected nearest results, got 0")
	}

	// 5. Sync
	if err := client.Sync(); err != nil {
		t.Fatalf("client.Sync failed: %v", err)
	}
}

func TestRemoteDriver_ModelIsolationAndAuth(t *testing.T) {
	s, dir := createTestServer(t)
	defer cleanupServer(t, s, dir)

	s.authToken = "secure-key"
	ts := httptest.NewServer(s.Routes())
	defer ts.Close()

	ctx := context.Background()

	// 1. Attempt connection without token -> expect auth failure
	unauthClient, err := driver.ConnectRemote(ts.URL)
	if err != nil {
		t.Fatalf("ConnectRemote failed: %v", err)
	}
	defer unauthClient.Close()

	err = unauthClient.Save("forbidden_node", map[string]float32{"val": 1})
	if err == nil {
		t.Fatalf("expected error due to missing auth token, got nil")
	}

	// 2. Connect with token
	authClient, err := driver.ConnectRemote(ts.URL, driver.WithToken("secure-key"), driver.WithModel("model_alpha"))
	if err != nil {
		t.Fatalf("ConnectRemote with token failed: %v", err)
	}
	defer authClient.Close()

	err = authClient.Save("node_in_alpha", map[string]float32{"val": 42})
	if err != nil {
		t.Fatalf("Save in model_alpha failed: %v", err)
	}

	// 3. Switch model to model_beta
	betaClient := authClient.Model("model_beta")
	if betaClient.ModelName() != "model_beta" {
		t.Fatalf("expected model_beta, got %s", betaClient.ModelName())
	}

	// Verify node_in_alpha does not exist in model_beta
	_, err = betaClient.Get("node_in_alpha").Execute(ctx)
	if err == nil {
		t.Fatalf("expected node_in_alpha not to exist in model_beta")
	}
}

func TestRemoteDriver_AttachSchemaLifecycle(t *testing.T) {
	s, dir := createTestServer(t)
	defer cleanupServer(t, s, dir)

	s.authToken = "secret-token-schema"
	ts := httptest.NewServer(s.Routes())
	defer ts.Close()

	ctx := context.Background()

	// 1. Connect without schema
	client, err := driver.ConnectRemote(ts.URL,
		driver.WithToken("secret-token-schema"),
		driver.WithModel("inventory"),
	)
	if err != nil {
		t.Fatalf("ConnectRemote failed: %v", err)
	}
	defer client.Close()

	// 2. Attach schema
	schema := driver.Schema{
		"quantity":  "int",
		"price":     "float",
		"sku":       "int",
		"is_active": "bool?",
	}
	if err := client.Attach(schema); err != nil {
		t.Fatalf("client.Attach failed: %v", err)
	}

	// 3. GetSchema should return the attached schema
	gotSchema, err := client.GetSchema(ctx)
	if err != nil {
		t.Fatalf("client.GetSchema failed: %v", err)
	}
	if gotSchema["quantity"] != "int" || gotSchema["price"] != "float" {
		t.Fatalf("retrieved schema mismatch: %v", gotSchema)
	}

	// 4. Attach identical schema -> success
	identical := driver.Schema{
		"quantity":  "int",
		"price":     "float",
		"sku":       "int",
		"is_active": "bool?",
	}
	if err := client.Attach(identical); err != nil {
		t.Fatalf("re-attaching identical schema failed: %v", err)
	}

	// 5. Attach mismatched schema -> fails with ErrSchemaMismatch
	mismatched := driver.Schema{
		"quantity": "float", // mismatched type
		"price":    "float",
		"sku":      "int",
	}
	err = client.Attach(mismatched)
	if err == nil {
		t.Fatalf("expected ErrSchemaMismatch, got nil")
	}
	if !errors.Is(err, driver.ErrSchemaMismatch) {
		t.Fatalf("expected ErrSchemaMismatch, got %v", err)
	}

	// 6. Save valid record obeying schema
	err = client.Save("item_1", map[string]float32{
		"quantity": 10,
		"price":    99.95,
		"sku":      1,
	})
	if err != nil {
		t.Fatalf("client.Save valid record failed: %v", err)
	}

	// 6b. Update record violating schema type (fractional value on int field) -> fails
	_, err = client.Update().
		In("item_1").
		Set("quantity", driver.Add("0", "0.5")).
		Execute(ctx)
	if err == nil {
		t.Fatalf("expected update error when setting int field quantity to 0.5, got nil")
	}

	// 7. Save record missing required field -> fails
	err = client.Save("item_bad", map[string]float32{
		"price": 99.95, // missing quantity and sku
	})
	if err == nil {
		t.Fatalf("expected save error due to missing fields, got nil")
	}

	// 8. Connect with WithAttachSchema option:
	// 8a. Matching schema -> success
	clientMatching, err := driver.ConnectRemote(ts.URL,
		driver.WithToken("secret-token-schema"),
		driver.WithModel("inventory"),
		driver.WithAttachSchema(schema),
	)
	if err != nil {
		t.Fatalf("ConnectRemote with matching schema failed: %v", err)
	}
	clientMatching.Close()

	// 8b. Mismatched schema -> rejected on connect
	_, err = driver.ConnectRemote(ts.URL,
		driver.WithToken("secret-token-schema"),
		driver.WithModel("inventory"),
		driver.WithAttachSchema(driver.Schema{"category": "int"}),
	)
	if err == nil {
		t.Fatalf("expected ConnectRemote rejection on mismatched schema, got nil")
	}

	// 9. Reset model schema
	err = client.ResetModel(ctx, false)
	if err == nil {
		t.Fatalf("expected error without confirm=true, got nil")
	}

	err = client.ResetModel(ctx, true)
	if err != nil {
		t.Fatalf("client.ResetModel failed: %v", err)
	}

	// 10. After reset, GetSchema returns error / 404
	_, err = client.GetSchema(ctx)
	if err == nil {
		t.Fatalf("expected error after reset when getting schema, got nil")
	}
}
