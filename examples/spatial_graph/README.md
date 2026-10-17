# Example: 2D Spatial Grid & Raycasting DDA

This example demonstrates AnchorDB's **2D Spatial Indexing, Vector Ray-Casting (Fast Voxel DDA)**, and direct **In-Storage Mathematics**.

---

## What This Example Demonstrates

1. **Embedded Engine (`driver.OpenEmbedded`)**: Initializes an in-process AnchorDB engine with memory-mapped zero-copy storage.
2. **2D Spatial Grid Indexing**: Inserts spatial nodes with coordinates `(X, Y)` and 4-dimensional semantic roles.
3. **Fast Voxel DDA Raycasting**: Casts directional rays through the 256×256 discrete spatial grid to find nearest intersecting nodes ranked by distance and perpendicular deviation.
4. **Direct Math Engine**: Calculates Euclidean distance, spatial centroids, cosine similarity, and nearest neighbors without leaving storage.
5. **In-Storage Arithmetic**: Evaluates arithmetic formulas (`speed = distance / duration`) on-the-fly during entity retrieval.

---

## How to Run

```bash
go run ./examples/spatial_graph
```

---

## Expected Output

```
==========================================================
    AnchorDB Example: 2D Spatial Grid & Raycasting DDA
==========================================================
[OK] Connected to AnchorDB Embedded Engine in 4.2ms (mmap zero-copy)
----------------------------------------------------------
>>> QUERY 1: In-Storage Pushdown Spatial Graph Step
Query: client.Graph().From('primary_cluster', 'backup_cluster').Relate('route-down').TopK(5).Execute()
Barycenter Anchor Point: (0.20, 0.90)
Internal DB Computation Time: 28.5µs
Top Candidates Passing DDA Ray & Pythagoras:
   [1] 'load_balancer' | Axis Distance t: 0.80 | Perp Deviation d_perp: 0.0000 | Score: 0.5556
   [2] 'service_mesh' | Axis Distance t: 1.40 | Perp Deviation d_perp: 0.0000 | Score: 0.4167
   [3] 'database_cluster' | Axis Distance t: 1.60 | Perp Deviation d_perp: 0.0000 | Score: 0.3846

----------------------------------------------------------
>>> QUERY 2: Direct Mathematical Calculation (Math Engine)
1. Direct Spatial Distance 'primary_cluster' <-> 'load_balancer': 0.8000
2. Center of Gravity (Centroid) ['primary_cluster', 'backup_cluster', 'load_balancer']: (0.20, 0.63)
3. Role Cosine Similarity 'primary_cluster' & 'backup_cluster': 1.0000 (Identical)
4. Nearest Spatial Neighbors of 'primary_cluster':
   [1] 'backup_cluster' (Distance: 0.0000)
   [2] 'load_balancer' (Distance: 0.8000)
   [3] 'service_mesh' (Distance: 1.4000)

----------------------------------------------------------
>>> QUERY 3: Entity Storage & In-Storage Division (Math Compute)
Saved: distance=120 km, duration=2 hours
Calculated Speed (distance / duration): 60.0 km/h
```
