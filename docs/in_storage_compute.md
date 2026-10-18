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

Dates in AnchorDB are stored internally as IEEE 754 `float32` numbers representing **Epoch Days** (the number of days elapsed since January 1, 1970 UTC). This representation allows calendar arithmetic to be executed as fast, single-cycle floating point additions and subtractions.

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
> Single-pass in-storage statistical computations (`ComputeStats`, `ComputeStatsWithOptions`, `StatsResult`, `StatsOptions`) are scheduled for the **next release**.

When computing summary statistics across large datasets, naive algorithms that calculate variance using $\sum x^2 - (\sum x)^2 / N$ suffer from catastrophic numerical cancellation when the variance is small relative to the mean. Conversely, standard two-pass formulas require either traversing storage twice or materializing large slices in heap memory.

AnchorDB implements **B.P. Welford's algorithm (1962)** directly within the storage engine (`pkg/storage`). It calculates the running count, sum, minimum, maximum, mean, variance, and standard deviation in a **single sequential streaming pass** ($O(N)$ time, $O(1)$ auxiliary space, 0 heap allocations per entity).

### 5.1 Mathematical Formulation

For each entity $n$ with scalar property value $x_n$:
1. **Delta from previous mean**:
   $$d = x_n - \bar{x}_{n-1}$$
2. **Mean update**:
   $$\bar{x}_n = \bar{x}_{n-1} + \frac{d}{n}$$
3. **Sum of squared differences ($M_2$) update**:
   $$M_{2, n} = M_{2, n-1} + d \cdot (x_n - \bar{x}_n)$$
4. **Final population variance & standard deviation**:
   $$\sigma^2 = \frac{M_{2, N}}{N}, \quad \sigma = \sqrt{\sigma^2}$$

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

* **Time Complexity**: $O(N)$ where $N$ is the number of active entities in the model.
* **Space Complexity**: $O(1)$ auxiliary heap allocation. The accumulators (`count`, `sum`, `min`, `max`, `m`, `s`) reside on the stack.
* **Benchmark Profile**: Evaluates 100,000 entities in ~7.8 ms with **0 B/op** and **0 allocs/op**, without spawning background goroutines or acquiring long-lived write locks.

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

1. **Numeric Precision**: AnchorDB optimizes attribute storage as IEEE 754 single-precision floats (`float32`). It is tailored for telemetry, IoT, metrics, and spatial coordinates, rather than arbitrary-precision banking decimals.
2. **Deterministic Binary Operations**: Supported operators are limited to arithmetic (`+`, `-`, `*`, `/`) and date offsets (`+days`, `-days`, `days_between`). AnchorDB intentionally avoids embedding a general-purpose programming language (e.g. Lua or JavaScript) in storage to maintain zero-GPU, sub-microsecond latency.
3. **Strict Division-by-Zero Semantics**: Division operations strictly validate that the divisor is non-zero. If the divisor resolves to zero, the engine returns an error (`division by zero: <field> is 0`) and rolls back the active mutation or query pass, preserving mathematical and financial integrity.
4. **Single-Pass Scalar Statistics**: `ComputeStats` computes 1st and 2nd central moments (mean, variance, standard deviation) along with min, max, count, and sum. Order statistics such as Median, Percentiles, or Mode are intentionally omitted because they require $O(N \log N)$ sorting or auxiliary histogram allocations, conflicting with AnchorDB's predictable streaming design.

