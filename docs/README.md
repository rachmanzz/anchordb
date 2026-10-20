# AnchorDB Documentation

Welcome to the technical documentation for **AnchorDB**. Start with [AnchorWire: Go Driver over Binary TCP](anchorwire.md) for the primary remote application path. The REST reference covers server operations that are not available over AnchorWire.

---

## Documentation Index

**Start here:** [AnchorWire: Go Driver over Binary TCP](anchorwire.md) covers server startup, connection on port `7448`, schema attachment, saves, queries, and transport limits.

1. **[In-Storage Computations & Date Math](in_storage_compute.md)**
   * Arithmetic operators (`+`, `-`, `*`, `/`) evaluated directly inside the storage engine.
   * Date arithmetic using Epoch Days (`AddDays`, `SubtractDays`, `DaysBetween`).
   * Read-time selective attribute projection and compound filtered queries.
   * Single-pass in-storage statistics via Welford's algorithm (`Count`, `Sum`, `Min`, `Max`, `Mean`, `Variance`, `StdDev`) *(Next Release)*.

2. **[Causal & Spatial Graph Engine](causal_and_spatial_graph.md)**
   * 2D spatial indexing, grid mapping, and directional DDA ray casting.
   * Geometric calculations: barycenters, Euclidean distances, and role cosine similarity.
   * Computational geometry primitives: `Box2D` AABB, `Polygon2D` geofencing, zero-sqrt `WithinRadius`, and `HaversineDistanceKM` geodesics *(Next Release)*.
   * Directed weighted causal graph modeling (`Link`, `Unlink`, `Causal`, `CausalChain`).

3. **[Storage Engine, WAL & Durability](storage_and_durability.md)**
   * Memory-mapped file format (`.anchordb`), 64-byte headers, and 40-byte nodes.
   * Compact record layout and attribute string interning.
   * Write-Ahead Logging (WAL) frame structure, CRC32 checks, and sync modes (`always`, `interval`, `none`).
   * Advisory single-writer file locking (`.lock`) and atomic snapshots.

4. **[Multi-Model Architecture: One Model = One File](multi_model_architecture.md)**
   * File-level failure isolation (`{model}.anchordb`).
   * Independent WAL, file locks, and lifecycle policies per model.
   * AnchorWire remote vs. embedded engine execution modes.
   * Strict schema registration and model attachment contract.

5. **[Model Schema Registration & Immutability Specification](model_schema_specification.md)**
   * One-time model schema registration and permanent file locking.
   * Scalar numeric and temporal primitives (`int`, `float`, `bool`, `date`).
   * Required vs. optional (`?`) syntax.
   * Attach/connect schema verification, comparison, and fail-fast rejection (`ErrSchemaMismatch`).
   * Permanent lock lifecycle and administrative hard reset through the Go driver.

6. **[Data Partitioning & Record Lifecycle](partitioning_and_lifecycle.md)**
   * Four-state record machine (`ACTIVE`, `SAFE_DELETE`, `TOMBSTONE`, `RETRY`).
   * Partition policies, retention cutoffs, and streaming partition files.
   * Sinks: Local Disk and S3-compatible Object Storage (streamed SigV4, server-side SSRF protection).
   * Tombstone garbage collection.

7. **[Background Subsystems: Scheduler & Worker Pool](background_subsystems.md)**
   * Pure-Go Cron scheduler (`pkg/scheduler`): wall-clock semantics, DST safety, overlap prevention.
   * Asynchronous worker pool (`pkg/worker`): bounded task queue, fixed concurrency, and memory admission controls.

8. **[Go Driver SDK Guide](go_driver.md)**
   * AnchorWire connection with `ConnectWire` and embedded engine with `OpenEmbedded`.
   * Schema Attachment (`client.Attach`, `client.AttachModel`, `driver.WithAttachSchema`).
   * Fluent query builders: `Get`, `List`, `Update`, `Graph`, and `Math`.
   * Context cancellation and transport limits for cold partition inspection.

9. **[Benchmark & Integration Verification Guide](benchmark_and_verification.md)**
   * Reproducible 6-phase test suite execution (`test/container_verifier`).
   * Little's Law validation (L = lambda * W) across concurrent worker pools.
   * Container crash durability, background sync, and cold partition archival.
   * High-load schema attachment, validation, and fail-fast rejection stress testing.

10. **[Installation & Deployment Guide](installation_and_deployment.md)**
   * Native OS packages (`.deb` / `.rpm`) with background `systemd` service.
   * Multi-architecture containers (`ghcr.io/rachmanzz/anchordb:latest`).
   * Pre-compiled standalone archives for Linux and macOS.
   * Go driver client installation (`go get github.com/rachmanzz/anchordb/pkg/driver@v0.1.2`) and compiling from source.

11. **[REST API Reference](rest_api.md)**
   * HTTP health, metrics, partition inspection, and server administration.
   * Graph math operations and in-storage statistics endpoints (`POST /v1/math`).
   * Endpoint details for applications that explicitly use the HTTP transport.

12. **[Workload Suitability, Limitations & Anti-Patterns](workload_suitability.md)**
   * Optimal workload profiles: semantic word graphs, IoT telemetry, and causal inference.
   * Unsuitable workloads: rich-text dictionaries, general-purpose JSON catalogs, and multi-entity OLTP transactions.
   * Design anti-patterns ("what to avoid"): string-in-float encoding, monolithic models, and unbounded log ingestion.
   * Recommended hybrid architectures: dual-engine design pairing AnchorDB with document/relational stores.

13. **[Data Citizenship & Engine Convergence](data_citizenship.md)**
   * Structural citizenship: fixed 40-byte binary nodes, compact scalar metrics, and causal graph edges.
   * Rationale for combining spatial indexing and causal graph traversal into a single unified engine.
   * Performance analysis: why Fast Voxel DDA and contiguous flat memory prevent bloat on edge hardware.
   * Architectural flexibility: converged models (all-in-one) versus decoupled multi-models.
