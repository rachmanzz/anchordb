# AnchorDB Documentation

Welcome to the technical documentation for **AnchorDB**. Below is a directory of architectural guides, feature references, and development manuals.

---

## Documentation Index

1. **[In-Storage Computations & Date Math](in_storage_compute.md)**
   * Arithmetic operators (`+`, `-`, `*`, `/`).
   * Date arithmetic using Epoch Days (`AddDays`, `SubtractDays`, `DaysBetween`).
   * Read-time selective column projection and filtered list queries.

2. **[Causal & Spatial Graph Engine](causal_and_spatial_graph.md)**
   * 2D spatial indexing, grid mapping, and directional DDA ray casting.
   * Geometric calculations: barycenters, Euclidean distances, and role cosine similarity.
   * Directed weighted causal graph modeling (`Link`, `Unlink`, `Causal`, `CausalChain`).

3. **[Storage Engine, WAL & Durability](storage_and_durability.md)**
   * Memory-mapped file format (`.anchordb`), 64-byte headers, and 40-byte nodes.
   * Compact record layout and attribute string interning.
   * Write-Ahead Logging (WAL) frame structure, CRC32 checks, and sync modes (`always`, `interval`, `none`).
   * Advisory single-writer file locking (`.lock`) and atomic snapshots.

4. **[Data Partitioning & Record Lifecycle](partitioning_and_lifecycle.md)**
   * Four-state record machine (`ACTIVE`, `SAFE_DELETE`, `TOMBSTONE`, `RETRY`).
   * Partition policies, retention cutoffs, and streaming partition files.
   * Sinks: Local Disk and S3-compatible Object Storage (streamed SigV4, SSRF protection).
   * Tombstone garbage collection.

5. **[Background Subsystems: Scheduler & Worker Pool](background_subsystems.md)**
   * Pure-Go Cron scheduler (`pkg/scheduler`): wall-clock semantics, DST safety, overlap prevention.
   * Asynchronous worker pool (`pkg/worker`): bounded task queue, fixed concurrency, and memory admission controls.

6. **[REST API Reference](rest_api.md)**
   * Server daemon configuration, authentication, rate limiting, and IP whitelisting.
   * Endpoint catalog covering health checks, CRUD, graph queries, and maintenance operations.

7. **[Go Driver SDK Guide](go_driver.md)**
   * Client connection configuration (`anchordb://` URI).
   * Fluent query builders: `Get`, `List`, `Update`, `Graph`, and `Math`.
   * Context cancellation and timeout handling.
