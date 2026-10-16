# Reproducible Benchmark & Integration Verification Guide

AnchorDB includes three independent tiers of verification and performance measurement:
1. **Standalone Bare-Metal CLI Benchmark (`cmd/benchmark`)**: Instant zero-dependency evaluation of arithmetic compute, spatial grid DDA, causal graphs, and embedded engine throughput without requiring Docker.
2. **Native Go Microbenchmark Suite (`pkg/storage/bench_test.go`)**: Standard Go testing benchmarks measuring allocations and ns/op.
3. **End-to-End Container Verification Suite (`cmd/container_verifier`)**: Full 6-phase integration test verifying container lifecycles, concurrency ramp-up with Little's Law, crash durability, background cron scheduling, partition archival, and strict schema immutability.

---

## 1. Quick Start: Zero-Dependency Standalone Benchmark

To evaluate AnchorDB's performance immediately on bare metal without Docker or network configuration:

```bash
git clone https://github.com/rachmanzz/anchordb.git
cd anchordb

# Run standalone benchmark suite across all engine subsystems
go run ./cmd/benchmark
```

This runs:
* **In-Storage Arithmetic**: 5,000,000 formula operations (`Divide`, `Multiply`, `AddConst`).
* **Spatial Grid DDA**: 500,000 raycasts across the 256×256 discrete spatial index.
* **Causal Reasoning**: 200,000 multi-hop 4-step path traversals (`ExecuteCausalChain`).
* **Engine Ingestion & Reads**: 100,000 records ingested with WAL, spatial indexing, and point queries.

---

## 2. Container Verification Prerequisites

- **Go**: 1.26+ (with toolchain 1.27.2+ recommended)
- **Container Engine**: Docker or Podman
- **Git**: For cloning the repository

---

## 2. Step-by-Step Reproduction

### Step 1: Clone and Build the Container Image

```bash
git clone https://github.com/rachmanzz/anchordb.git
cd anchordb

# Build the production container image
podman build -t anchordb:latest .
# (or with Docker: docker build -t anchordb:latest .)
```

### Step 2: Launch the AnchorDB Container

```bash
# Clean any previous test container and directory
podman rm -f anchordb-live-test || true
rm -rf /tmp/anchordb_container_test_data
mkdir -p /tmp/anchordb_container_test_data

# Start the live container on port 7447 with persistent volume
podman run -d \
  --name anchordb-live-test \
  -p 7447:7447 \
  -v /tmp/anchordb_container_test_data:/data:Z \
  -e ANCHORDB_AUTH_TOKEN="test_secret_token_live" \
  -e ANCHORDB_DATA_DIR="/data" \
  anchordb:latest
```

### Step 3: Run the Verification Suite

```bash
go run ./cmd/container_verifier
```

---

## 3. What the Verification Suite Tests

The suite runs **6 rigorous phases** verifying data integrity, durability, and mathematical consistency:

### Phase (A): Comprehensive Engine Operation Stress
Executes all engine subsystems under load:
1. **Bulk Record Ingestion**: Ingests 2,000 entities with 6 properties each (~3,000 writes/sec).
2. **In-Storage Computations**: Evaluates division (`voltage / resistance`) and multiplication (`voltage * current`) on-the-fly during read queries.
3. **Rich Attribute & Date Filtering**: Filters entities with compound range and epoch-day criteria with server pagination (`has_more`).
4. **Batch In-Place Updates**: Performs in-storage arithmetic updates on 800 records simultaneously in < 10 ms.
5. **Spatial Vector Ray-Casting**: Inserts 2D spatial coordinate nodes and casts directional rays along specified headings.
6. **Causal Graph Reasoning**: Traverses multi-hop causal chains (`soil_dry -> irrigation_on -> soil_moist`) and verifies aggregate transition scores.
7. **Direct Numerical Math**: Calculates in-storage Euclidean distances, role centroids, cosine similarities, and nearest neighbors.

---

### Phase (B): Concurrency Polling Ramp-Up & Little's Law Alignment
Simulates concurrent worker pools scaling from 5 to 100 parallel workers:

* **5 concurrent workers** (500 requests)
* **25 concurrent workers** (2,500 requests)
* **50 concurrent workers** (5,000 requests)
* **100 concurrent workers** (10,000 requests)

#### Little's Law Mathematical Invariant
According to queuing theory (Little's Law, $L = \lambda \cdot W$):
$$\text{Expected Average Latency } (W) = \frac{\text{Concurrency } (L)}{\text{Throughput } (\lambda)}$$

The verifier measures complete latency distributions (`min`, `p50`, `avg`, `p95`, `p99`, `max`) and confirms that measured average latencies match theoretical expectations.

---

### Phase (C): Container Stop/Restart & Crash Durability
Tests that data, WAL recovery, and file locks survive abrupt daemon restarts:
1. Writes sentinel canary records and flushes to disk.
2. Stops the container (`podman stop`). Verifies the port is completely offline.
3. Starts the container (`podman start`). Verifies health check passes upon reboot.
4. Reads canary entities and causal links, confirming zero data loss.

---

### Phase (D): Background Scheduler & Periodic Sync
1. Inspects active tasks registered in the cron scheduler (`pkg/scheduler`).
2. Observes automated periodic background sync tasks (`@every 3s`).
3. Triggers immediate manual task runs via `/v1/scheduler/run?task=periodic_background_sync`.

---

### Phase (E): Cold Partition Archival & Pruning
1. Inserts historical records (> 60 days old) and active records (today).
2. Triggers partition archival via `/v1/partition/run` with a 60-day retention cutoff.
3. Confirms cold records were pruned from the active hot database while active records remain intact.
4. Inspects the archived partition file **strictly through the Engine API** (`/v1/partition/list` and `/v1/partition/inspect`), ensuring cold data is retrieved securely without raw file exposure.

---

### Phase (F): Schema Attachment & Strict Immutability Stress
Stresses the engine's contract enforcement and validation layers under heavy parallel load:
1. **Schema Attachment**: Locks a model schema with primitives (`float`, `int`), optional modifiers (`?`), and 1D arrays (`string[]?`).
2. **High-Concurrency Valid Ingestion**: 5,000 valid records ingested across 50 parallel goroutines (~13,900 writes/sec with 0 failures).
3. **Fail-Fast Rejection Stress**: 1,000 invalid payloads (omitted required fields and undeclared unknown fields) sent across 20 goroutines; verifies 100% rejection with 0 leaks into storage.
4. **Immutability Enforcement**: Re-attaching a conflicting schema under load fails with `ErrSchemaMismatch` (HTTP 409).
5. **Connection Handshake Enforcement**: `ConnectRemote` with mismatched schema rejected at connection handshake.
6. **Administrative Reset**: Confirms hard reset unmounts the contract and allows a clean schema replacement.

---

## 4. Teardown

After completing the tests, stop and clean up the container:

```bash
podman rm -f anchordb-live-test
rm -rf /tmp/anchordb_container_test_data
```

---

## 5. Standalone Engine Microbenchmark Suite (`pkg/storage/bench_test.go`)

In addition to the end-to-end container integration test suite, AnchorDB includes a dedicated in-repo Go benchmark file in [`pkg/storage/bench_test.go`](../pkg/storage/bench_test.go) to measure raw CPU latencies, zero-allocation properties, and storage engine throughput without network or container overhead.

### How to Run

```bash
# Run all storage benchmarks with memory allocation statistics
go test -bench=. -benchmem -run=^# ./pkg/storage
```

### Measured Engine Benchmarks

| Benchmark Name | Target Subsystem | Typical Result | Memory Allocations |
| :--- | :--- | :--- | :--- |
| `BenchmarkInStorageCompute_Arithmetic` | In-Storage Math (`Divide`, `Multiply`, `AddConst`) | ~24 ns/op (40M+ ops/sec) | **0 B/op, 0 allocs/op** |
| `BenchmarkSpatial_TraverseRayDDA` | Fast Voxel DDA Raycasting across 256×256 Grid | ~1.2 µs/op | **0 B/op, 0 allocs/op** |
| `BenchmarkGraph_ExecuteCausalChain` | Multi-Hop Causal Graph Path Traversal (4 hops) | ~2.4 µs/op | 1,096 B/op, 10 allocs/op |
| `BenchmarkWAL_SyncNone` | WAL Ingestion (OS Page Cache Durability) | ~3.0 µs/op (320k+ writes/sec) | 778 B/op, 4 allocs/op |
| `BenchmarkWAL_SyncInterval` | WAL Ingestion (Buffered Periodic fsync 50ms) | ~3.2 µs/op | 778 B/op, 4 allocs/op |
| `BenchmarkWAL_SyncAlways` | WAL Ingestion (Physical Disk fsync per commit) | Media latency (NVMe vs SSD) | 778 B/op, 4 allocs/op |
