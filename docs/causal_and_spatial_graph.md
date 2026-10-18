# Causal & Spatial Graph Engine

AnchorDB includes a dual graph engine supporting both continuous 2D geometric/spatial indexing and explicit directed causal reasoning over weighted relations.

---

## 1. 2D Spatial Indexing & Geometric Reasoning

Every spatial node in AnchorDB consists of:
1. **2D Coordinates (`Point2D`)**: Continuous coordinates $(X, Y)$ mapped into a uniform spatial grid.
2. **Semantic Role Vector (`[4]float32`)**: Four normalized components representing semantic attributes or latent roles.
3. **Weight (`uint8`)**: Priority ranking used during beam search.

```
                    Y ^
                      |        [primary_cluster] (0.2, 0.9)
                      |        [backup_cluster]  (0.2, 0.9)
                      |
                      |   |
                      |   | Ray vector (Y: -1)
                      |   v
                      |        [load_balancer]   (0.2, 0.1)
                      |
                      +----------------------------> X
```

### Spatial Grid & Ray-Casting
AnchorDB arranges spatial nodes into a 2D integer cell grid. When projecting a query from a set of anchor nodes:
1. **Barycenter Calculation**: The geometric center $(\bar{X}, \bar{Y})$ of the source entities is computed.
2. **Directional Ray Casting (DDA)**: A directional ray is projected along a relation vector.
3. **Beam Search & Perpendicular Deviation**: Candidate nodes within the beam width ($d_{\perp}^2 \le 4 \cdot \text{beam\_width}^2$) are ranked by a composite score that combines exponential beam energy, a role-fit term, and forward-distance attenuation:
   $$\text{score} = e^{-d_\perp^2 / (2\,\text{beam\_width}^2)} \times \frac{1 + 1.5 \cdot \text{roleFit}}{1 + 0.5 \cdot t}$$
   where $t$ is the forward projection distance along the ray and `roleFit` is the dot product of the node's 4D role vector with the anchor role deficit (target equilibrium `[1, 1, 1, 0]`).

### Spatial Math Helpers
The Go driver provides direct geometric routines via `client.Math()`:

```go
math := client.Math()

// 1. Direct Euclidean Distance
distance, err := math.Distance("primary_cluster", "load_balancer")

// 2. Centroid of Multiple Nodes
centroid, err := math.Centroid("primary_cluster", "backup_cluster", "load_balancer")
fmt.Printf("Centroid: (%.2f, %.2f)\n", centroid.Coord.X, centroid.Coord.Y)

// 3. Cosine Similarity between 4D Semantic Roles
similarity, err := math.CosineSimilarity("primary_cluster", "backup_cluster")

// 4. Nearest Neighbor Search
neighbors, err := math.Nearest("primary_cluster", 5)
```

---

## 2. Computational Geometry & Geofencing

> [!NOTE]
> The computational geometry primitives (`Box2D`, `Polygon2D`, `WithinRadius`, `HaversineDistanceKM`, `InitialBearing`) and storage engine methods (`FindNodesInPolygon`, `FindNodesInRadius`, `CalculateHaversineDistance`) described below are scheduled for the **next release**.

AnchorDB provides high-performance 2D computational geometry primitives in `pkg/spatial` and exposes direct query operations within `pkg/storage`. These routines are implemented with zero heap allocations in inner loops to maximize throughput in high-frequency telemetry, geofencing, and proximity tracking workloads.

### 2.1 Axis-Aligned Bounding Box (`Box2D`)

`spatial.Box2D` defines a rectangular envelope bounded by minimum and maximum `Point2D` coordinates.

```go
// Construct normalized bounding box (guarantees Min <= Max)
bbox := spatial.NewBox2D(
    spatial.Point2D{X: -10.0, Y: -10.0},
    spatial.Point2D{X: 10.0, Y: 10.0},
)

// O(1) containment check
inside := bbox.Contains(spatial.Point2D{X: 5.0, Y: 5.0}) // true

// O(1) intersection check between two bounding boxes
intersects := bbox.Intersects(spatial.NewBox2D(
    spatial.Point2D{X: 8.0, Y: 8.0},
    spatial.Point2D{X: 20.0, Y: 20.0},
)) // true
```

### 2.2 Polygons & Ray-Casting Geofencing (`Polygon2D`)

`spatial.Polygon2D` represents a closed planar polygon with vertices ordered sequentially. Upon creation, `Polygon2D` precalculates and caches its tight bounding box (`BBox`) for two-stage filtering.

```go
vertices := []spatial.Point2D{
    {X: 0.0, Y: 0.0},
    {X: 10.0, Y: 0.0},
    {X: 10.0, Y: 10.0},
    {X: 0.0, Y: 10.0},
}
poly, err := spatial.NewPolygon2D(vertices)
if err != nil {
    log.Fatal(err) // Returns ErrPolygonTooFewVertices if len(vertices) < 3
}

// 1. Point-in-Polygon (Geofencing)
contains := poly.Contains(spatial.Point2D{X: 5.0, Y: 5.0}) // true

// 2. Planar Polygon Area (Shoelace Formula)
area := poly.Area() // 100.0

// 3. Perimeter Length
perimeter := poly.Perimeter() // 40.0
```

#### Two-Stage Containment Algorithm
1. **Stage 1: Broad-Phase Bounding Box Rejection ($O(1)$)**: Checks if `poly.BBox.Contains(p)` is satisfied. Points outside the envelope are rejected immediately without testing vertices.
2. **Stage 2: Narrow-Phase Ray-Casting ($O(V)$)**: Evaluates horizontal ray intersections using Jordan's curve theorem (Even-Odd rule). A point is inside if a horizontal ray cast from $(X, Y)$ toward $+\infty$ intersects an odd number of polygon boundary segments.

### 2.3 Zero-Square-Root Radius Filtering (`WithinRadius`)

To avoid the computational cost of floating-point square roots (`math.Sqrt`) in inner spatial scanning loops, `spatial.WithinRadius` evaluates squared Euclidean distance:

$$\text{DistanceSq}(p, c) = (p.X - c.X)^2 + (p.Y - c.Y)^2 \le r^2$$

```go
center := spatial.Point2D{X: 0.0, Y: 0.0}
radius := float32(10.0)

// Fast O(1) proximity test without square root
isNear := spatial.WithinRadius(spatial.Point2D{X: 6.0, Y: 8.0}, center, radius) // true (36 + 64 <= 100)
```

### 2.4 Spherical Geodesics (`HaversineDistanceKM` & `InitialBearing`)

For geographical coordinates (where $X = \text{Longitude}$ and $Y = \text{Latitude}$ in decimal degrees), AnchorDB provides spherical Earth geodesic routines:

```go
monas := spatial.Point2D{X: 106.8271, Y: -6.1754}  // Jakarta
bundaranHI := spatial.Point2D{X: 106.8227, Y: -6.1950} // Jakarta

// 1. Great-circle distance using WGS84 mean Earth radius (R = 6,371.0 km)
distKM := spatial.HaversineDistanceKM(monas, bundaranHI)
fmt.Printf("Distance: %.3f km\n", distKM) // ~2.231 km

// 2. Initial forward compass azimuth in degrees [0°, 360°)
azimuth := spatial.InitialBearing(monas, bundaranHI)
fmt.Printf("Bearing: %.1f°\n", azimuth) // ~192.5°
```

### 2.5 In-Storage Geometric Queries

The storage engine (`pkg/storage`) evaluates geometry filters directly over the memory-mapped node layout:

```go
// 1. Find up to K nodes within a Polygon boundary
nodesInZone, err := db.FindNodesInPolygon(poly, 100)

// 2. Find up to K nodes within a radius from center coordinate
nodesNearby, err := db.FindNodesInRadius(center, 50.0, 20)

// 3. Compute great-circle distance between two named spatial entities
km, err := db.CalculateHaversineDistance("airport_cgk", "airport_dps")
```

---

## 3. Explicit Causal Graph Engine

For deterministic cause-and-effect modeling, AnchorDB supports explicit, weighted directed edges stored in a dedicated causal graph structure (`EDG3` binary section).

### Edge Model
An edge connects a source node to a target node labeled by a relation name and accompanied by a numeric weight score in $(0.0, 1.0]$:

$$\text{source} \xrightarrow[\text{weight}]{\text{relation}} \text{target}$$

### Managing Causal Edges

```go
// Register causal relationships
err := client.Link("fuel_shortage", "causes", "inflation", 0.9)
err  = client.Link("crop_failure", "causes", "inflation", 0.4)
err  = client.Link("inflation", "causes", "economic_crisis", 0.7)

// Remove an edge
err = client.Unlink("crop_failure", "causes", "inflation")
```

### Single-Step Traversal (`Causal`)
Retrieve outgoing connections matching a specific relation:

```go
// Find top 5 effects caused directly by fuel_shortage
results, err := client.Causal("fuel_shortage", "causes", 5)
for _, r := range results {
    fmt.Printf("Effect: %s, Score: %.2f\n", r.Word, r.Score)
}
```

### Multi-Hop Causal Chains (`CausalChain`)
Traverse multi-step paths through the graph by specifying an ordered sequence of relations:

```go
// Traverse: fuel_shortage -> causes -> inflation -> causes -> economic_crisis
seeds := []string{"fuel_shortage"}
relations := []string{"causes", "causes"}

chain, err := client.CausalChain(seeds, relations, 5)
for _, c := range chain {
    fmt.Printf("Chain Target: %s, Aggregate Score: %.4f\n", c.Word, c.Score)
}
// Outputs: Chain Target: economic_crisis
```

During multi-hop chain execution:
* The frontier is updated at each step by multiplying incoming scores with transition edge weights.
* Results are deduplicated by target node, retaining the highest cumulative path score.
* Targets are ranked and bounded to the requested `limit`.

### Canonical API Decision: `Score` vs `Weight`

AnchorDB enforces a strict architectural boundary between graph inputs and query outputs:
* **Graph Input (`weight`)**: When defining directed edges via `client.Link(source, relation, target, weight)`, the transition parameter is named `weight` $\in (0.0, 1.0]$.
* **Query Output (`Score`)**: When executing traversals or multi-hop chains (`Causal`, `CausalChain`), the aggregated cumulative affinity is canonically exposed as **`Score`** (`result.Score`).
* **Deprecation Notice**: The struct field `CausalResult.Weight` is a legacy alias retained exclusively for backward compatibility and is scheduled for removal in a future release. All new applications must use `result.Score`.

---

## 4. Storage & Compaction

* **Memory-Mapped Persistence**: Causal edges are serialized alongside spatial nodes in the `.anchordb` binary file and recorded in the Write-Ahead Log (`WalOpEdge`, `WalOpEdgeDelete`).
* **Tombstone Tagging & Compaction**: Deleted edges are tagged with tombstones in $O(1)$ constant time without shifting array elements. When the number of tombstoned records exceeds the configured ratio threshold, an amortized $O(N)$ compaction pass reclaims fragmented slots and flushes clean contiguous storage to disk.

---

## 5. AnchorWire Usage

Use the Go driver over AnchorWire for graph writes and causal traversal:

```go
client, err := driver.ConnectWire("wire://localhost:7448",
    driver.WithToken("your_auth_token"),
    driver.WithModel("events"),
)
if err != nil {
    log.Fatal(err)
}
defer client.Close()

if err := client.Link("fuel_shortage", "causes", "inflation", 0.9); err != nil {
    log.Fatal(err)
}
results, err := client.CausalChain([]string{"fuel_shortage"}, []string{"causes"}, 5)
if err != nil {
    log.Fatal(err)
}
for _, result := range results {
    fmt.Println(result.Word, result.Score)
}
```

See the [AnchorWire guide](anchorwire.md) and the [graph traversal example](../examples/graph_traversal/README.md) for complete setups.

---

## 6. Design Boundaries & Limitations

1. **Cumulative Weights vs. Probabilities**: The aggregate score along a multi-hop causal chain is the product of transition edge weights ($\prod w_i$). It represents cumulative relational affinity, not a Bayesian conditional probability or formal causal DAG backdoor adjustment.
2. **2D Spatial Coordinates**: Spatial indexing is specifically optimized for 2D Cartesian planes (e.g. facility layouts, IoT field grids, game maps). It is not designed to replace high-dimensional dense vector embeddings (e.g. 1536-dim OpenAI embeddings).
3. **Bounded Beam Search**: Multi-hop traversals and ray casting evaluate candidates within bounded grid radius or beam width ($O(K)$ candidate evaluations), ensuring bounded latency rather than exhaustive graph exploration.
4. **Planar Geometry Scope**: `Polygon2D` geofencing and `Box2D` operate on 2D Cartesian coordinates. Self-intersecting polygons or multi-polygon holes are not modeled natively in this primitive.
5. **Spherical Earth Model**: `HaversineDistanceKM` approximates Earth using the standard mean spherical radius ($R = 6371.0\text{ km}$). This yields an accuracy within ~0.3%–0.5% for general navigation and geofencing without requiring the computation overhead of ellipsoidal geodesic integration (e.g. Vincenty or Karney). Coordinates must use decimal degrees with $X = \text{Longitude}$ and $Y = \text{Latitude}$.

