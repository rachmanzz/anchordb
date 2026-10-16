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
3. **Beam Search & Perpendicular Deviation**: Candidate nodes within the beam width ($d_{\perp}^2 \le \text{beam\_width}$) are scored using axis projection distance ($t$), perpendicular deviation, and role cosine similarity.

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

## 2. Explicit Causal Graph Engine

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

## 3. Storage & Compaction

* **Memory-Mapped Persistence**: Causal edges are serialized alongside spatial nodes in the `.anchordb` binary file and recorded in the Write-Ahead Log (`WalOpEdge`, `WalOpEdgeDelete`).
* **Tombstone Tagging & Compaction**: Deleted edges are tagged with tombstones in $O(1)$ constant time without shifting array elements. When the number of tombstoned records exceeds the configured ratio threshold, an amortized $O(N)$ compaction pass reclaims fragmented slots and flushes clean contiguous storage to disk.

---

## 4. REST API Endpoints

### Add Edge (`POST /v1/edge`)
```json
{
  "source": "fuel_shortage",
  "relation": "causes",
  "target": "inflation",
  "weight": 0.9,
  "action": "add"
}
```

### Traverse Causal Chain (`POST /v1/causal`)
```json
{
  "seeds": ["fuel_shortage"],
  "relations": ["causes", "causes"],
  "limit": 5
}
```

**Response**:
```json
{
  "results": [
    {
      "word": "economic_crisis",
      "score": 0.63
    }
  ]
}
```

---

## 5. Design Boundaries & Limitations

1. **Cumulative Weights vs. Probabilities**: The aggregate score along a multi-hop causal chain is the product of transition edge weights ($\prod w_i$). It represents cumulative relational affinity, not a Bayesian conditional probability or formal causal DAG backdoor adjustment.
2. **2D Spatial Coordinates**: Spatial indexing is specifically optimized for 2D Cartesian planes (e.g. facility layouts, IoT field grids, game maps). It is not designed to replace high-dimensional dense vector embeddings (e.g. 1536-dim OpenAI embeddings).
3. **Bounded Beam Search**: Multi-hop traversals and ray casting evaluate candidates within bounded grid radius or beam width ($O(K)$ candidate evaluations), ensuring bounded latency rather than exhaustive graph exploration.

