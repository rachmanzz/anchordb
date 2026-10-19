# Go Driver SDK Guide

The official AnchorDB Go driver is located in `pkg/driver`. For remote applications, use the AnchorWire binary TCP transport with `driver.ConnectWire`; embedded applications can run the engine in-process. Start with the [AnchorWire guide](anchorwire.md) for a complete runnable example.

---

## 1. Architecture: AnchorWire Remote vs Embedded Local Mode

AnchorDB driver supports two modes of operation:

### Why Use the Background Engine Server?
Connecting directly to local `.anchordb` files in an application process acquires an exclusive advisory file lock (`flock`). This means:
- Background schedulers (e.g., cron retention policies, expiration jobs) cannot run concurrently on that file.
- Multiple web services or worker processes cannot access the file simultaneously.
- Worker pools, rate limiting, and token authentication are bypassed.

**Production Best Practice**: Run `anchordb-server` as a persistent background daemon, and connect client applications over AnchorWire (default TCP port `7448`).

---

## 2. Installation & Connecting

Add the driver to your Go project using `go get`:

```bash
go get github.com/rachmanzz/anchordb/pkg/driver@v0.1.2
```

Then import it in your Go code:

```go
import "github.com/rachmanzz/anchordb/pkg/driver"
```

### Option A: AnchorWire Client (Production / Recommended)

```go
// Connect to a running AnchorDB engine server over binary TCP
client, err := driver.ConnectWire("wire://localhost:7448",
    driver.WithToken("your_auth_token"),
    driver.WithModel("plant_iot"),
)
if err != nil {
    log.Fatalf("failed to connect to engine: %v", err)
}
defer client.Close()
```

You can also connect using standard URI DSN strings:
```go
// Connect selects AnchorWire from the wire:// scheme
client, err := driver.Connect("wire://localhost:7448", driver.WithToken("your_auth_token"), driver.WithModel("plant_iot"))
```

> [!IMPORTANT]
> **Network Security Advisory**: AnchorWire communicates over raw TCP without built-in TLS for raw computational throughput. For remote connections across untrusted networks, always run AnchorWire within a private VPC, behind a VPN/overlay network (such as Tailscale or WireGuard), or through a TLS-terminating reverse proxy (such as Envoy, HAProxy, or stunnel).

### Option B: Embedded Engine Mode (In-Process Runtime, Zero Network Overhead)

For embedded applications, command-line utilities, or local unit tests that do not want to run a separate network server:

```go
// Open an in-process AnchorDB engine targeting a local data directory.
// Runs the full engine runtime (WAL, file locking, multi-model routing, worker pool,
// and optional background sync) directly in-process with zero network overhead.
client, err := driver.OpenEmbedded("./data",
    driver.WithModel("plant_iot"),
    driver.WithSyncInterval(5 * time.Second),
    driver.WithWorkerConcurrency(4),
)
if err != nil {
    log.Fatalf("failed to initialize embedded engine: %v", err)
}
defer client.Close()
```

> **Direct File Access**: `OpenLocal(filePath)` is deprecated in favor of `OpenEmbedded(dataDir)`. It still opens storage through `storage.Open`, including WAL recovery and an exclusive file lock, but does not provide the embedded engine's multi-model routing, worker pool, or background scheduling. Do not open the same model concurrently from another process.
>
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

## 4. Schema Attachment & Contract Enforcement (`client.Attach`)

AnchorDB supports strict schema immutability to prevent schema drift, accidental property typos, and invalid data types across distributed microservices.

### Defining a Schema
Schemas are defined using `driver.Schema` (a mapping of field names to type strings):

```go
schema := driver.Schema{
    "voltage":     "float", // Required float32
    "current":     "float", // Required float32
    "status":      "int",   // Required integer
    "is_active":   "bool?", // Optional boolean (trailing ?)
    "calibrated":  "date?", // Optional calendar date (epoch day)
}
```

#### Supported Field Types & Modifiers:
- **Primitives**: `int`, `float`, `bool`, `date` (YYYY-MM-DD epoch day)
- **Required (Default)**: Omission of `?` makes the field strictly mandatory during writes.
- **Optional (`?`)**: Suffix `?` marks the field as optional.

### Attaching and Locking a Schema
Once attached, the schema is locked in a `{model}.anchordb.schema` sidecar alongside the model file:

```go
// 1. Attach to active model
err := client.Attach(schema)
if err != nil {
    // Fails with driver.ErrSchemaMismatch if an incompatible schema was already locked
    log.Fatalf("Attach rejected: %v", err)
}

// 2. Or attach when spawning a new model client:
deviceClient, err := client.AttachModel("devices_zone_1", schema)

// 3. Or enforce schema attachment during initial connection:
client, err := driver.ConnectWire("wire://localhost:7448",
    driver.WithToken("token"),
    driver.WithModel("devices_zone_1"),
    driver.WithAttachSchema(schema),
)
```

### Inspecting & Resetting Schemas

```go
// Retrieve active locked schema
lockedSchema, err := client.GetSchema(ctx)
fmt.Println("Locked fields:", lockedSchema)

// Administrative hard reset (requires confirm: true)
err = client.ResetModel(ctx, true)
```

### Fail-Fast Error Guarantees
- `driver.ErrSchemaMismatch`: Returned on `Attach` or `ConnectWire` if incoming schema conflicts with the model's locked contract.
- `driver.ErrMissingRequiredField`: Returned on `Save` or `Update` if a required field is omitted.
- `driver.ErrUnknownField`: Returned on `Save` or `Update` if an undeclared property is passed.
- `driver.ErrInvalidFieldType`: Returned if a field value does not conform to its type specification.

---

## 5. Reading & Writing Entities

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

## 6. Filtering & Pagination (`ListBuilder`)

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

## 7. Batch Updates (`UpdateBuilder`)

Apply in-place arithmetic updates matching filter criteria. A batch is not transactional: if a later record fails, earlier updates remain committed.

```go
// In-place increment matching filtered records
count, err := client.Update().
    Where("status", "=", 1.0).
    Set("reconnect_attempts", driver.AddConst("reconnect_attempts", 1.0)).
    Limit(1000).
    Execute(ctx)
if err != nil {
    log.Fatal(err)
}

fmt.Printf("Updated %d records\n", count)
```

---

## 8. Spatial Graph & Ray-Casting

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

## 9. Direct Math Engine

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

## 10. Causal Graph Traversal

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
    fmt.Printf("Outcome: %s (Cumulative Score: %.3f)\n", step.Word, step.Score)
}
```

> **Field Deprecation Notice**: Query results canonically return `step.Score`. The legacy `step.Weight` field is deprecated and scheduled for removal in a future release. All new applications must use `step.Score`.

---

## 11. Context & Timeouts

All builder execution methods (`.Execute(ctx)`) strictly honor `context.Context` cancellation and deadlines. If a request is canceled or times out, execution halts immediately and returns `ctx.Err()`.

---

## 12. Cold Partition Inspection & Catalog

The Go driver supports querying archived partition files and inspecting cold records across Embedded, HTTP, and AnchorWire transports:

```go
// 1. List all available archived partitions in the engine
partitions, err := client.ListPartitions(ctx)
if err != nil {
    log.Fatal(err)
}
for _, p := range partitions {
    fmt.Printf("Partition: %s, Size: %d bytes, Created: %s\n", p.Name, p.SizeBytes, p.ModTime)
}

// 2. Inspect a specific record in an archived partition file
result, err := client.InspectPartitionRecord(ctx, "2026-10-16_part1.anchordb", "sensor_archive_old_1")
if err != nil {
    log.Fatal(err)
}
fmt.Printf("Record %s props: %+v\n", result.Name, result.Props)
```

Additionally, records pruned from the active database during partition archival are automatically and transparently resolved via `client.Get("old_record").Execute(ctx)` if the partition catalog contains them.

---

## 13. Design Boundaries & Limitations

1. **One Model = One File**: Each named model (`sensors`, `telemetry`) is an isolated binary file with its own lock and WAL. AnchorDB does not support cross-model relational JOINs.
2. **Numeric Property Vectors**: Record attributes are optimized for computational floating-point metrics (`float32`) and Epoch Days dates (`MustDate("YYYY-MM-DD")`).
3. **Engine-Only Invariant Access**: Never attempt to open, copy, or mutate an active `.anchordb` file outside the engine. Access data via `driver.ConnectWire(...)` or `driver.OpenEmbedded(...)`.

