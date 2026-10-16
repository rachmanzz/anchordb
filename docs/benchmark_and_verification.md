# Reproducible Benchmark & Integration Verification Guide

AnchorDB includes a comprehensive automated integration and stress verification suite in `cmd/container_verifier/main.go`. This guide explains how to run, reproduce, and verify all engine operations and mathematical performance metrics independently.

---

## 1. Prerequisites

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

The suite runs **5 rigorous phases** verifying data integrity, durability, and mathematical consistency:

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
