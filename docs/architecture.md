# AnchorDB Architectural Specification

AnchorDB is a high-performance computational database and spatial convergence engine designed for **100% CPU execution**, **bounded constant-time $O(K)$ retrieval**, and **minimal memory footprints**.

Copyright (c) 2026 **MUHAMMD ABDURRAHMAN**. All Commercial Rights Reserved.

---

## 1. Storage Layout & Memory-Mapping (`mmap`)

AnchorDB separates storage into clean binary structures backed by `.anchordb` files:

```
+--------------------------------------------------------------------------+
|  Header (64B)  |  Nodes Array (40B/node)  |  String Pool  |  Entity Store |
+--------------------------------------------------------------------------+
```

1. **Header (64 Bytes)**:
   - `Magic` (8B) = `"ANCHORDB"`
   - `Version` (2B) = `1`
   - `NodeCount` (8B), `NodesOffset` (8B)
   - `GridCellsX` (2B), `GridCellsY` (2B), `GridOffset` (8B)
   - `StringOffset` (8B), `EntityCount` (8B), `EntityOffset` (8B)
   - `Reserved` (10B)
2. **Nodes Array (40 Bytes / Node)**:
   - Fixed-size binary structs: `ID (8B)`, `X, Y (8B)`, `Roles [4]float32 (16B)`, `DomainID (2B)`, `WordOffset (4B)`, `WordLen (2B)`.
   - **Zero-Copy Decoding**: Read directly from the `mmapData` pointer slice without heap duplication.
3. **String Pool**:
   - Packed raw UTF-8 bytes for graph labels. Words are referenced by `(WordOffset, WordLen)`.
4. **Entity Store**:
   - Key-value numeric attributes stored in compact contiguous slices (`CompactRecord`) with String Interning (`attrPool`) to minimize memory amplification.

---

## 2. Computational Model

Unlike traditional databases that transfer raw records to the application layer for arithmetic processing, AnchorDB computes on-the-fly inside storage:

* **In-Storage Mathematics**: Arithmetic operators (`+`, `-`, `*`, `/`) execute directly in CPU registers during record retrieval.
* **Epoch Days Date Math**: Dates are converted to 32-bit float Epoch Days (days since `1970-01-01`), enabling exact arithmetic (e.g. `2026-10-14` + `45` days) with zero SQL parsing overhead.
* **Bounded $O(K)$ Paging**: Positional slicing and early termination ensure predictable 1–3 ms latency regardless of total dataset size.

---

## 3. Background Pipeline Engine (`gosuite/v3`)

AnchorDB incorporates production-grade background capabilities with zero external broker daemons (no Redis, no Kafka):

```
+--------------------+        +--------------------+
|  Cron Scheduler    | -----> |   Worker Engine    |
| (gosuite/v3/cron)  |        | (gosuite/v3/worker)|
+--------------------+        +--------------------+
          |                             |
    Auto-Partition              Async Partition
 & Periodic Sync (5m)          & Background Sync
```

1. **Cron Scheduler (`pkg/scheduler`)**:
   - Backed by `github.com/rachmanzz/gosuite/v3/cron`.
   - Built-in overlap prevention (`skip-if-still-running`), panic recovery, and standard 5-field cron or human-friendly `HH:MM` syntax.
   - Triggers automated partition archiving and periodic background disk flushes (`@every 5m`).
2. **Worker Pool (`pkg/worker`)**:
   - Backed by `github.com/rachmanzz/gosuite/v3/worker`.
   - **Async Partition**: `POST /v1/partition/run?async=true` offloads heavy serialization and cloud S3 uploads to worker goroutines, responding with HTTP 202 immediately.
   - **Background Disk Sync**: Flushes memory-mapped models to disk periodically without blocking client query latencies.

---

## 4. Memory & Resource Frugality

* **RAM Tuning**: Configurable soft limits (`500MB`, `1GB`, `5GB`) via `debug.SetMemoryLimit` and automatic cgroup ceiling detection.
* **Sparse Spatial Grid**: Cell mapping allocates only for populated cells, eliminating fixed overhead on empty models.
* **Atomic Streaming Sync**: 64 KB buffered streaming writes with atomic POSIX file renaming prevent corruption during power failures.

---

## 5. Network Firewall Gateway

Built-in IP gateway filtering supports:
* Single exact IPs (`127.0.0.1`, `::1`)
* CIDR subnet masks (`192.168.1.0/24`)
* Explicit IP ranges (`10.0.0.1-10.0.0.50`)
* Timing-safe authorization token verification via `crypto/subtle.ConstantTimeCompare`.
