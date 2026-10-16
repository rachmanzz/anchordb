# Go Driver SDK Guide

The official AnchorDB Go driver is located in `pkg/driver`. It provides an idiomatic, fluent client API for interacting with AnchorDB instances either over the network against the daemon server engine, or locally via embedded storage.

---

## 1. Architecture: Remote Engine vs Embedded Local Mode

AnchorDB driver supports two modes of operation:

### Why Use the Background Engine Server?
Connecting directly to local `.anchordb` files in an application process acquires an exclusive advisory file lock (`flock`). This means:
- Background schedulers (e.g., cron retention policies, expiration jobs) cannot run concurrently on that file.
- Multiple web services or worker processes cannot access the file simultaneously.
- Worker pools, rate limiting, and token authentication are bypassed.

**Production Best Practice**: Run `anchordb-server` as a persistent background daemon, and connect client applications to it over the network.

---

## 2. Installation & Connecting

```go
import "github.com/rachmanzz/anchordb/pkg/driver"
```

### Option A: Remote Engine Client (Production / Recommended)

```go
// Connect to a running AnchorDB engine server
client, err := driver.ConnectRemote("http://localhost:7447",
    driver.WithToken("your_auth_token"),
    driver.WithModel("plant_iot"),
    driver.WithTimeout(5*time.Second),
)
if err != nil {
    log.Fatalf("failed to connect to engine: %v", err)
}
defer client.Close()
```

You can also connect using standard URI DSN strings:
```go
// Connect with URI string
client, err := driver.Connect("anchordb://user:secret@localhost:7447/plant_iot")
```

### Option B: Local Embedded Client (Unit Testing / Single-Process Tools)

```go
// Open an embedded local database file directly
client, err := driver.OpenLocal("./data/plant_iot.anchordb")
if err != nil {
    log.Fatalf("failed to open local database: %v", err)
}
defer client.Close()
```

> **Note on `?mode=mmap`**: AnchorDB core storage is always memory-mapped via zero-copy `mmap(MAP_SHARED)`. Redundant query flags like `?mode=mmap` are unnecessary and stripped automatically for backward compatibility.

---

## 3. Multi-Model Routing

AnchorDB supports multi-tenancy and multiple named models under a single engine. Switching models on the client is instantaneous:

```go
// Switch active model target
greenhouseClient := client.Model("greenhouse_zone_1")
fmt.Println("Active model:", greenhouseClient.ModelName()) // "greenhouse_zone_1"
```

---

## 4. Reading & Writing Entities

### Saving Records
```go
// 1. Save with a named key identifier
err := client.Save("sensor_soil_1", map[string]float32{
    "moisture": 45.2,
    "depth":    10.0,
})

// 2. Save with an automatically generated UUIDv4
autoID, err := client.SaveAutoID(map[string]float32{
    "temperature": 24.5,
    "humidity":    65.0,
})
```

### In-Storage Computations (`GetBuilder`)
AnchorDB computes derived attributes on-the-fly inside the database without copying or parsing entire records:

```go
ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
defer cancel()

record, err := client.Get("trip_route_101").
    Select("distance", "duration", "speed").
    Compute("speed", driver.Divide("distance", "duration")).
    Execute(ctx)
if err != nil {
    log.Fatal(err)
}

fmt.Printf("Speed: %.2f km/h\n", record.Get("speed"))
```

---

## 5. Filtering & Pagination (`ListBuilder`)

The `List()` builder supports numeric ranges, date comparisons, and server-side pagination.

```go
results, err := client.List().
    Where("status", "=", 1.0).
    Where("distance", ">=", 50.0).
    WhereDate("date", "2026-10-14").
    BetweenDate("date", "2026-10-01", "2026-10-31").
    Compute("speed", driver.Divide("distance", "duration")).
    Page(1, 25).
    Execute(ctx)
if err != nil {
    log.Fatal(err)
}

for _, item := range results.Items {
    fmt.Println(item.Name, item.Get("speed"))
}

if results.HasMore {
    fmt.Println("Next page available")
}
```

---

## 6. Batch Updates (`UpdateBuilder`)

Apply atomic in-place arithmetic updates matching filter criteria:

```go
// Atomic in-place increment matching filtered records
count, err := client.Update().
    Where("status", "=", 1.0).
    Set("reconnect_attempts", driver.Add("reconnect_attempts", "1")).
    Limit(1000).
    Execute(ctx)
if err != nil {
    log.Fatal(err)
}

fmt.Printf("Updated %d records\n", count)
```

---

## 7. Spatial Graph & Ray-Casting

Register concept nodes and directional vector relations, then cast rays through 2D space:

```go
// 1. Append concept nodes with spatial coordinates and semantic roles
_ = client.AppendNode("sensor_hub", spatial.Point2D{X: 0.2, Y: 0.9}, [4]float32{0.9, 0.1, 0.2, 0.2})
_ = client.AppendNode("sprinkler_valve", spatial.Point2D{X: 0.2, Y: 0.1}, [4]float32{0.2, 0.1, 0.95, 0.2})

// 2. Define directional vector relation
_ = client.SetRelation("controls", spatial.Point2D{Y: -1})

// 3. Ray casting query
graphResp, err := client.Graph().
    From("sensor_hub").
    Relate("controls").
    TopK(5).
    Execute(ctx)

for _, res := range graphResp.Results {
    fmt.Printf("Target: %s (Score: %.4f)\n", res.Word, res.Score)
}
```

---

## 8. Direct Math Engine

Perform high-performance geometric and vector math across nodes:

```go
math := client.Math()

// Exact Euclidean distance
dist, _ := math.Distance("sensor_hub", "sprinkler_valve")

// Spatial and semantic centroid
center, _ := math.Centroid("sensor_hub", "sprinkler_valve")

// Cosine angle between semantic roles
similarity, _ := math.CosineSimilarity("sensor_hub", "sprinkler_valve")

// K-nearest neighbors
neighbors, _ := math.Nearest("sensor_hub", 5)
```

---

## 9. Causal Graph Traversal

Link directed, weighted edges for cause-and-effect reasoning:

```go
// Add causal links
_ = client.Link("soil_dry", "triggers", "irrigation", 0.95)
_ = client.Link("irrigation", "causes", "soil_moist", 0.90)

// Single-hop causal lookup
steps, _ := client.Causal("soil_dry", "triggers", 5)

// Multi-hop causal chain
chain, _ := client.CausalChain([]string{"soil_dry"}, []string{"triggers", "causes"}, 5)
for _, step := range chain {
    fmt.Printf("Outcome: %s (Cumulative Weight: %.3f)\n", step.Word, step.Weight)
}
```

---

## 10. Context & Timeouts

All builder execution methods (`.Execute(ctx)`) strictly honor `context.Context` cancellation and deadlines. If a request is canceled or times out, execution halts immediately and returns `ctx.Err()`.
