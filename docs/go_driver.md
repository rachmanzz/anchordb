# Go Driver SDK Guide

The official AnchorDB Go driver is located in `pkg/driver`. It provides an idiomatic, fluent client API for interacting directly with AnchorDB instances.

---

## 1. Installation & Connection

To import the driver:
```go
import "github.com/rachmanzz/anchordb/pkg/driver"
```

### Establishing a Connection
The driver connects using an `anchordb://` URI:

```go
// Connect to a local database file using memory mapping
client, err := driver.Connect("anchordb://./data/app.anchordb?mode=mmap")
if err != nil {
    log.Fatalf("failed to connect: %v", err)
}
defer client.Close()
```

---

## 2. Reading & Writing Entities

### Saving Records
```go
// 1. Save with a named key
err := client.Save("server_node_1", map[string]float32{
    "cpu_usage": 45.2,
    "ram_usage": 68.0,
})

// 2. Save with an automatically generated UUIDv4
id, err := client.SaveAutoID(map[string]float32{
    "temperature": 24.5,
})
```

### Querying with Computations (`GetBuilder`)
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

## 3. Querying & Filtering Records (`ListBuilder`)

The `List()` builder supports property comparisons, date filters, and pagination.

```go
results, err := client.List().
    Where("status", "=", 1.0).
    Where("distance", ">=", 50.0).
    WhereDate("date", "2026-10-14").
    BetweenDate("date", "2026-10-01", "2026-10-31").
    Compute("speed", driver.Divide("distance", "duration")).
    Page(1, 25).
    Execute(ctx)

for _, item := range results.Items {
    fmt.Println(item.Name, item.Get("speed"))
}

if results.HasMore {
    fmt.Println("Next page available")
}
```

---

## 4. Batch Updates (`UpdateBuilder`)

```go
// Atomic in-place increment
count, err := client.Update().
    Where("status", "=", 1.0).
    Set("reconnect_attempts", driver.Add("reconnect_attempts", "1")).
    Limit(1000).
    Execute(ctx)

fmt.Printf("Updated %d records\n", count)
```

---

## 5. Spatial & Causal Operations

### Spatial Math Engine
```go
math := client.Math()

// Compute spatial distance between two nodes
dist, _ := math.Distance("primary_cluster", "load_balancer")

// Compute centroid of cluster nodes
center, _ := math.Centroid("primary_cluster", "backup_cluster")

// Role cosine similarity
similarity, _ := math.CosineSimilarity("primary_cluster", "backup_cluster")
```

### Causal Graph
```go
// Create causal link
_ = client.Link("fuel_shortage", "causes", "inflation", 0.9)
_ = client.Link("inflation", "causes", "economic_crisis", 0.7)

// 1-Hop query
step, _ := client.Causal("fuel_shortage", "causes", 5)

// Multi-hop path traversal
chain, _ := client.CausalChain([]string{"fuel_shortage"}, []string{"causes", "causes"}, 5)
```

---

## 6. Context & Cancellation

All builder executions (`.Execute(ctx)`) respect `context.Context` deadlines and cancellations. If a query is canceled or exceeds its deadline, the builder terminates early and returns `ctx.Err()`.
