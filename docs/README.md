# AnchorDB Documentation

Welcome to the technical documentation for **AnchorDB**. Below is a directory of architectural guides, feature references, development manuals, and integration benchmarks.

---

## Documentation Index

1. **[In-Storage Computations & Date Math](in_storage_compute.md)**
   * Arithmetic operators (`+`, `-`, `*`, `/`) evaluated directly inside the storage engine.
   * Date arithmetic using Epoch Days (`AddDays`, `SubtractDays`, `DaysBetween`).
   * Read-time selective attribute projection and compound filtered queries.

2. **[Causal & Spatial Graph Engine](causal_and_spatial_graph.md)**
   * 2D spatial indexing, grid mapping, and directional DDA ray casting.
   * Geometric calculations: barycenters, Euclidean distances, and role cosine similarity.
   * Directed weighted causal graph modeling (`Link`, `Unlink`, `Causal`, `CausalChain`).

3. **[Storage Engine, WAL & Durability](storage_and_durability.md)**
   * Memory-mapped file format (`.anchordb`), 64-byte headers, and 40-byte nodes.
   * Compact record layout and attribute string interning.
   * Write-Ahead Logging (WAL) frame structure, CRC32 checks, and sync modes (`always`, `interval`, `none`).
   * Advisory single-writer file locking (`.lock`) and atomic snapshots.

4. **[Multi-Model Architecture: One Model = One File](multi_model_architecture.md)**
   * File-level failure isolation (`{model}.anchordb`).
   * Independent WAL, file locks, and lifecycle policies per model.
   * Remote Engine vs. Embedded Engine execution modes.
   * Strict schema registration and model attachment contract.

5. **[Model Schema Registration & Immutability Specification](model_schema_specification.md)**
   * One-time model schema registration and permanent file locking.
   * Primitive types and 1D arrays (`string[]`, `float[]`, `int[]`, `bool[]`, `date[]`).
   * Required vs. optional (`?`) syntax.
   * Attach/connect schema verification, comparison, and fail-fast rejection (`ErrSchemaMismatch`).
   * Permanent lock lifecycle and administrative hard reset contract (`POST /v1/model/reset`).

6. **[Data Partitioning & Record Lifecycle](partitioning_and_lifecycle.md)**
   * Four-state record machine (`ACTIVE`, `SAFE_DELETE`, `TOMBSTONE`, `RETRY`).
   * Partition policies, retention cutoffs, and streaming partition files.
   * Sinks: Local Disk and S3-compatible Object Storage (streamed SigV4, SSRF protection).
   * Tombstone garbage collection.

7. **[Background Subsystems: Scheduler & Worker Pool](background_subsystems.md)**
   * Pure-Go Cron scheduler (`pkg/scheduler`): wall-clock semantics, DST safety, overlap prevention.
   * Asynchronous worker pool (`pkg/worker`): bounded task queue, fixed concurrency, and memory admission controls.

8. **[REST API Reference](rest_api.md)**
   * Server daemon configuration, authentication, rate limiting, and IP whitelisting.
   * Endpoint catalog covering health checks, telemetry, CRUD, and graph queries.
   * Model Schema Management: `POST /v1/model/attach`, `GET /v1/model/schema`, `POST /v1/model/reset`.
   * Partition archival, inspection, and maintenance operations.

9. **[Go Driver SDK Guide](go_driver.md)**
   * Client connection configuration (Remote Engine `ConnectRemote` and Embedded Engine `OpenEmbedded`).
   * Schema Attachment (`client.Attach`, `client.AttachModel`, `driver.WithAttachSchema`).
   * Fluent query builders: `Get`, `List`, `Update`, `Graph`, and `Math`.
   * Cold partition inspection and context cancellation handling.

10. **[Benchmark & Integration Verification Guide](benchmark_and_verification.md)**
   * Reproducible 6-phase test suite execution (`test/container_verifier`).
   * Little's Law validation ($L = \lambda \cdot W$) across concurrent worker pools.
   * Container crash durability, background sync, and cold partition archival.
   * High-load schema attachment, validation, and fail-fast rejection stress testing.

11. **[Installation & Deployment Guide](installation_and_deployment.md)**
   * Native OS packages (`.deb` / `.rpm`) with background `systemd` service.
   * Multi-architecture containers (`ghcr.io/rachmanzz/anchordb:latest`).
   * Pre-compiled standalone archives for Linux and macOS.
   * Go driver client installation (`go get github.com/rachmanzz/anchordb/pkg/driver@v0.1.1`) and compiling from source.
