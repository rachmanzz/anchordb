# Example: 2D Spatial Grid & Raycasting DDA

This example demonstrates AnchorDB's **2D Spatial Indexing, Vector Ray-Casting (Fast Voxel DDA)**, and direct **In-Storage Mathematics**.

---

## What This Example Demonstrates

1. **AnchorWire Binary TCP Engine (`driver.ConnectWire`)**: Connects to AnchorDB via the high-throughput Custom Binary TCP Protocol on port `7448`.
2. **2D Spatial Grid Indexing**: Inserts spatial nodes with coordinates `(X, Y)` and 4-dimensional semantic roles.
3. **Fast Voxel DDA Raycasting**: Casts directional rays through the 256×256 discrete spatial grid and ranks candidates by a composite score (beam energy × role-fit, attenuated by forward projection distance).
4. **Direct Math Engine**: Calculates Euclidean distance, spatial centroids, cosine similarity, and nearest neighbors without leaving storage.
5. **In-Storage Arithmetic**: Evaluates arithmetic formulas (`speed = distance / duration`) on-the-fly during entity retrieval.

---

## How to Run

Ensure the AnchorDB server is running with AnchorWire listening on port `7448`:

```bash
go run ./examples/spatial_graph -url=wire://localhost:7448
```

---

## Expected Output

```
==========================================================
    AnchorDB Example: 2D Spatial Grid & Raycasting DDA    
             (AnchorWire Binary TCP Protocol)             
==========================================================
[OK] Connected via AnchorWire Binary TCP (wire://localhost:7448) in 520.4µs
----------------------------------------------------------
>>> QUERY 1: In-Storage Pushdown Spatial Graph Step
Query: client.Graph().From('primary_cluster', 'backup_cluster').Relate('route-down').TopK(5).Execute()
Barycenter Anchor Point: (0.20, 0.90)
Internal DB Computation Time: 2.749281ms
Top Candidates Passing DDA Ray & Pythagoras:
   [1] 'load_balancer' | Axis Distance t: 0.80 | Perp Deviation d_perp: 0.0000 | Score: 1.4107
   [2] 'database_cluster' | Axis Distance t: 1.60 | Perp Deviation d_perp: 0.0000 | Score: 1.4056
   [3] 'service_mesh' | Axis Distance t: 1.40 | Perp Deviation d_perp: 0.0000 | Score: 1.1353

----------------------------------------------------------
>>> QUERY 2: Direct Mathematical Calculation (Math Engine)
1. Direct Spatial Distance 'primary_cluster' <-> 'load_balancer': 0.8000
2. Center of Gravity (Centroid) ['primary_cluster', 'backup_cluster', 'load_balancer']: (0.20, 0.63)
3. Role Cosine Similarity 'primary_cluster' & 'backup_cluster': 1.0000
4. Nearest Spatial Neighbors of 'primary_cluster':
   [1] 'backup_cluster' (Distance: 0.0000)
   [2] 'load_balancer' (Distance: 0.8000)
   [3] 'edge_gateway' (Distance: 0.8062)

----------------------------------------------------------
>>> QUERY 3: Entity Storage & In-Storage Division (Math Compute)
Saved: distance=120 km, duration=2 hours
Calculated Speed (distance / duration): 60.0 km/h
```

> **Note**: Connection and computation timings depend on host hardware and OS cache state. All scores, distances, and centroids are deterministic and reproducible.

### How Candidates Are Scored

Query 1 does not order nodes by raw distance alone. For every candidate that passes the DDA beam and Pythagoras filter, the engine computes:

$$\text{score} = \text{beamEnergy}(d_\perp^2) \times \frac{1 + 1.5 \cdot \text{roleFit}}{1 + 0.5 \cdot t}$$

- `beamEnergy = exp(-d⊥² / (2·beamWidth²))` — exponential beam absorption (equal to `1.0` for on-axis nodes where `d⊥ = 0`).
- `roleFit = roles · deficit`, where `deficit` is the per-dimension shortfall of the combined anchor roles from the target equilibrium `[1, 1, 1, 0]`.
- `t` is the forward projection distance along the ray.

Because role-fit participates in the score, `database_cluster` (roleFit `1.02`) outranks `service_mesh` (roleFit `0.62`) even though its axis distance `t` is larger. All three candidates above are exactly on-axis (`d⊥ = 0`), so their scores reduce to `(1 + 1.5·roleFit) / (1 + 0.5·t)`.
