# In-Storage Computations & Date Math

AnchorDB features an in-storage compute engine that evaluates arithmetic formulas and date manipulations directly during record retrieval and batch mutation passes. Performing computations inside the storage engine avoids the latency, serialization, and memory overhead of fetching raw fields into application memory for client-side processing.

---

## 1. Supported Computation Operators

AnchorDB provides arithmetic and assignment operators defined in `pkg/storage` and exposed via `pkg/driver`.

| Operator | Driver Function | Description | Example |
| :--- | :--- | :--- | :--- |
| **Divide** | `driver.Divide(fieldA, fieldB)` | Divides `fieldA` by `fieldB`. Returns an error if `fieldB` is 0 (aborts and rolls back batch mutations). | `driver.Divide("distance", "duration")` |
| **Multiply** | `driver.Multiply(fieldA, fieldB)` | Multiplies `fieldA` by `fieldB`. | `driver.Multiply("unit_price", "quantity")` |
| **Add** | `driver.Add(fieldA, fieldB)` | Adds two attribute fields. | `driver.Add("base_score", "bonus_score")` |
| **Add Constant** | `driver.AddConst(field, val)` | Adds a numeric float constant to an attribute. | `driver.AddConst("base_score", 10.0)` |
| **Subtract** | `driver.Subtract(fieldA, fieldB)` | Subtracts `fieldB` from `fieldA`. | `driver.Subtract("gross_revenue", "tax")` |
| **Subtract Constant** | `driver.SubtractConst(field, val)` | Subtracts a numeric float constant from an attribute. | `driver.SubtractConst("balance", 25.0)` |
| **Assign** | `driver.Value(val)` | Directly assigns a constant value to a field. | `driver.Value(100.0)` |

> **Token Resolution Note**: Under the hood, operators resolve parameters via `resolveValue(token, props)`. A parameter token can be an attribute key name (e.g. `"base_score"`), a stringified numeric literal (e.g. `"10"`), or an ISO date string (e.g. `"2026-10-14"`). Typed helpers like `driver.AddConst(field, floatVal)` allow passing native numeric constants directly without manual stringification.


---

## 2. Date Mathematics

Dates in AnchorDB represent **Epoch Days** (whole days elapsed since January 1, 1970 UTC) in two physical forms. The legacy path (`driver.Date`/`MustDate`, `map[string]float32` APIs) stores them as IEEE 754 `float32`; the typed path (schema `date` type with typed writes) stores integer epoch days (`int64`, negative before 1970) with exact integer date arithmetic that returns `ErrIntegerOverflow` instead of wrapping. Both forms execute calendar arithmetic in storage; exactness requires the typed form (see the Precision Boundary in `docs/model_schema_specification.md`).

> **Date Timezone & Precision**:
> All date parsing (`ParseDate`) assumes UTC midnight (`00:00:00 UTC`) without timezone offset ambiguity. Epoch days represent whole integer days from the 1970-01-01 UTC epoch. When formatting via `FormatDate`, values are rounded to the nearest integer day (`math.Round`) before converting back to `"YYYY-MM-DD"`.

### Date Functions

* **`driver.MustDate(dateStr string) float32`**: Converts an ISO-8601 date string (`"YYYY-MM-DD"`) into float32 epoch days. Panics on malformed format; suitable for static literals.
* **`driver.Date(dateStr string) (float32, error)`**: Converts a date string into epoch days with explicit error handling for user inputs.
* **`driver.FormatDate(epochDays float32) string`**: Formats an epoch days value back into `"YYYY-MM-DD"`.
* **`driver.AddDays(field string, days float32)`**: Adds an integer number of days to a stored date field.
* **`driver.SubtractDays(field string, days float32)`**: Subtracts days from a stored date field.
* **`driver.DaysBetween(fieldA, fieldB string)`**: Calculates the elapsed duration in days between two date fields (`fieldA - fieldB`).

### Example: Date Arithmetic
```go
// Store an entity with a start date (2026-10-14)
_ = client.Save("project_contract", map[string]float32{
    "start_date": driver.MustDate("2026-10-14"),
})

// Add 45 days during a batch update
_, err := client.Update().
    In("project_contract").
    Set("start_date", driver.AddDays("start_date", 45)).
    Execute(ctx)

// Result is 2026-11-28
updated, _ := client.Get("project_contract").Execute(ctx)
fmt.Println("New Date:", updated.GetDate("start_date"))
```

---

## 3. Read-Time Projection & Computation (`Compute` & `Select`)

When reading entities using `Get` or `List`, computed values can be generated dynamically without persisting them to disk.

### Selective Attribute Projection
Unselected attributes can be excluded from the returned payload by calling `.Select(...)`:

```go
// Entity has: distance, duration, temperature, humidity
// We only want: distance, duration, and calculated speed
record, err := client.Get("vehicle_route_1").
    Select("distance", "duration", "speed").
    Compute("speed", driver.Divide("distance", "duration")).
    Execute(ctx)

// 'temperature' and 'humidity' are omitted from the result
fmt.Println(record.All()) // map[distance:100 duration:2 speed:50]
```

### Filtering on Computed Attributes (`List`)
Computed fields can also be used in query predicates:


```go
// Filter distance >= 50, compute speed, and filter speed > 50
result, err := client.List().
    Where("distance", ">=", 50.0).
    Compute("speed", driver.Divide("distance", "duration")).
    Where("speed", ">", 50.0).
    Page(1, 10).
    Execute(ctx)

for _, item := range result.Items {
    fmt.Printf("%s -> distance: %.0f, speed: %.1f\n", item.Name, item.Get("distance"), item.Get("speed"))
}
```

---

## 4. In-Place Batch Updates (`Update`)

The update builder modifies stored records across the dataset or within specific filtered sets:

```go
// Add 10 to distance for up to 5,000 matching vehicles
updatedCount, err := client.Update().
    Where("distance", "<", 1000.0).
    Set("distance", driver.AddConst("distance", 10.0)).
    Limit(5000).
    Execute(ctx)

fmt.Printf("Updated %d records\n", updatedCount)
```

---

## 5. In-Storage Basic Statistics (Welford Single-Pass Algorithm)

> [!NOTE]
> Single-pass in-storage statistical computations (`ComputeStats`, `ComputeStatsWithOptions`, `StatsResult`, `StatsOptions`) are implemented in `pkg/storage`. Exact integer extremes are published alongside the float accumulations via `MinInt`/`MaxInt`/`MinUint`/`MaxUint` with the `MinType`/`MaxType` domain tag (the legacy `Min64`/`Max64` remain for compatibility but are inexact for `uint64` above 2^53).

When computing summary statistics across large datasets, naive algorithms that calculate variance using standard sum-of-squares formulas suffer from catastrophic numerical cancellation when the variance is small relative to the mean. Conversely, standard two-pass formulas require either traversing storage twice or materializing large slices in heap memory.

AnchorDB implements **B.P. Welford's algorithm (1962)** directly within the storage engine (`pkg/storage`). It calculates the running count, sum, minimum, maximum, mean, variance, and standard deviation in a **single sequential streaming pass** (O(N) time, O(1) auxiliary space, 0 heap allocations per entity).

### 5.1 Mathematical Formulation

For each entity `n` with scalar property value `x_n`:
1. **Delta from previous mean**:
   `d = x_n - mean_{n-1}`
2. **Mean update**:
   `mean_n = mean_{n-1} + (d / n)`
3. **Sum of squared differences (M2) update**:
   `M2_n = M2_{n-1} + d * (x_n - mean_n)`
4. **Final population variance & standard deviation**:
   `variance = M2_N / N`, `std_dev = sqrt(variance)`

### 5.2 Go Engine API

Statistical computation can be invoked directly on any opened storage model:

```go
// 1. Basic statistics over all active records for a given field
stats, err := db.ComputeStats("temperature")
if err != nil {
    log.Fatal(err)
}

fmt.Printf("Count: %d\n", stats.Count)
fmt.Printf("Sum: %.2f\n", stats.Sum)
fmt.Printf("Min: %.2f, Max: %.2f\n", stats.Min, stats.Max)
fmt.Printf("Mean: %.4f\n", stats.Mean)
fmt.Printf("Variance: %.6f\n", stats.Variance)
fmt.Printf("StdDev: %.6f\n", stats.StdDev)
```

### 5.3 Filtered Statistical Aggregation (`StatsOptions`)

To compute metrics over specific subsets (e.g. active records matching threshold conditions), provide `StatsOptions`:

```go
// 2. Filtered statistics with conditional constraints
opts := storage.StatsOptions{
    Field: "temperature",
    Filters: []storage.FilterCondition{
        {Field: "pressure", Operator: ">=", Value: 101.3},
        {Field: "status", Operator: "==", Value: 1.0},
    },
    // AllowedOperations restricts aggregation to specific record lifecycle states
    AllowedOperations: []uint8{storage.OpStatusActive},
}

filteredStats, err := db.ComputeStatsWithOptions(opts)
```

### 5.4 Performance Characteristics

* **Time Complexity**: O(N) where N is the number of active entities in the model.
* **Space Complexity**: O(1) auxiliary heap allocation for the base aggregates. The accumulators (`count`, `sum`, `min`, `max`, `m`, `s`) reside on the stack.
* **Benchmark Profile** (measured 2026-11-10, i7-1165G7, 100k entities): base stats ~15–20 ms/op; with P50+P90+P99 ~33–35 ms/op (≈3 extra selection passes) and one materialized value slice (~800 KB, ±25 growth allocs). Without requested percentiles the slice is never allocated.

### 5.5 Percentiles (`StatsOptions.Percentiles`, `P50`/`P90`/`P99`)

```go
stats, err := db.ComputeStatsWithOptions(storage.StatsOptions{
    Field: "temperature",
    Percentiles: []float64{0.5, 0.9, 0.99}, // the only supported quantiles
})
fmt.Printf("P50: %.2f, P90: %.2f, P99: %.2f\n", stats.P50, stats.P90, stats.P99)
```

Quantiles resolve by Introselect (average O(N), worst case O(N log N)) under the locked nearest-rank definition `k = ceil(p*N)-1`. Only `0.5`, `0.9`, `0.99` are published; anything else (or values outside `[0, 1]`) is rejected before any O(N) work. NaN values are excluded from the percentile population (legacy `Sum`/`Mean` keep their existing NaN behavior). Unrequested quantiles stay zero and cost nothing. Selection runs in `float64` image space, which is order-faithful but cannot split integers that round to the same `float64` (same boundary as `Min64`).

### 5.6 Histogram (`ComputeHistogram`, `HistogramResult`)

```go
h, err := db.ComputeHistogramWithOptions(storage.HistogramOptions{
    Field: "temperature",
    Bins:  20,
    Filters: []storage.FilterCondition{
        {Field: "status", Operator: "==", Value: 1.0},
    },
})
// h.Bins has len == Bins, h.Edges has len == Bins+1, sum(h.Bins) == h.Count
```

Two passes: min/max comes from the same `ComputeStatsWithOptions` path (identical population semantics, including filters), then values are binned linearly over `[Min, Max]`. The maximum value always lands in the last bin and the last edge equals Max exactly; float dust is clamped to the edge bins, never dropped. NaN values are excluded from both bins and `Count`, so `sum(Bins) == Count` always holds. A single-valued (or otherwise degenerate) range collapses to one bin holding the full count.

### 5.7 Anomaly Filters (`FilterZScore`, `FilterIQROutlier`)

```go
// |x-μ|/σ > 3 with μ/σ from the same active population
outliers, err := db.FilterZScore("temperature", 3.0)

// Outside [Q1 - 1.5·IQR, Q3 + 1.5·IQR], quartiles by nearest rank
fenced, err := db.FilterIQROutlier("temperature", 1.5)
```

Both run two read-only passes (statistics first, selection second) and return `[]storage.EntityRecord` (`Name`, `Operation`, `Props`, `Attrs`). A zero σ (constant column) or zero IQR yields an empty result, never an error; an empty population yields an empty result. Tombstoned records never match. Invalid configs (negative threshold, `k <= 0`, NaN, empty field) are rejected up front.

### 5.8 Graph Degree Statistics and PageRank (`pkg/storage/edges.go`)

```go
deg, err := db.GraphDegreeStats(5) // Top-5 hubs by in-degree
pr, err := db.PageRank(0.85, 1e-9, 1000, 5)
```

`GraphDegreeStats` aggregates in/out-degree distribution over live edges (tombstoned slots skipped) in one O(V+E) pass: node/edge counts, average degree, population variance (Welford over sorted node order), and Top-N hubs ranked by in-degree (ties by node name). `PageRank` runs power iteration with the given damping factor: dangling nodes redistribute their rank evenly so scores always sum to 1, convergence is declared when the L1 delta drops below tolerance, and repeated runs are bit-identical. Scores below the iteration budget return the best effort with `Converged=false` rather than an error.

---

## 6. AnchorWire Usage

In-storage calculations work through the Go driver over AnchorWire:

```go
client, err := driver.ConnectWire("wire://localhost:7448",
    driver.WithToken("your_auth_token"),
    driver.WithModel("vehicles"),
)
if err != nil {
    log.Fatal(err)
}
defer client.Close()

result, err := client.Get("vehicle_route_1").
    Select("distance", "duration", "speed").
    Compute("speed", driver.Divide("distance", "duration")).
    Execute(ctx)
if err != nil {
    log.Fatal(err)
}
fmt.Println(result.Get("speed"))
```

See the [AnchorWire guide](anchorwire.md) for a complete runnable program.

---

## 7. Design Boundaries & Limitations

1. **Numeric Precision**: Record attributes use four physical numeric types (`float32`, `int64`, `uint64`, `float64`); the schema types are `int`, `uint`, `float`, `float32`, `float64`, `bool`, `date` (see `docs/model_schema_specification.md`). The legacy `float32` APIs are exact only for integers up to 16,777,216 — use the typed APIs and exact integer arithmetic (`ErrIntegerOverflow` on overflow) when values must stay exact. AnchorDB is tailored for telemetry, IoT, metrics, and spatial coordinates, rather than arbitrary-precision banking decimals.
2. **Deterministic Binary Operations**: Supported operators are limited to arithmetic (`+`, `-`, `*`, `/`) and date offsets (`+days`, `-days`, `days_between`). AnchorDB intentionally avoids embedding a general-purpose programming language (e.g. Lua or JavaScript) in storage to maintain zero-GPU, sub-microsecond latency.
3. **Strict Division-by-Zero Semantics**: Division operations strictly validate that the divisor is non-zero. If the divisor resolves to zero, the engine returns an error (`division by zero: <field> is 0`) and rolls back the active mutation or query pass, preserving mathematical and financial integrity.
4. **Single-Pass Scalar Statistics**: `ComputeStats` computes 1st and 2nd central moments (mean, variance, standard deviation) along with min, max, count, and sum. Order statistics such as Median, Percentiles, or Mode are intentionally omitted because they require O(N log N) sorting or auxiliary histogram allocations, conflicting with AnchorDB's predictable streaming design.

