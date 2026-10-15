# AnchorDB Architectural Specification

AnchorDB is a high-performance computational database and spatial convergence engine designed for **100% CPU execution**, **bounded constant-time $O(K)$ retrieval**, and **minimal memory footprints**.

Copyright (c) 2026 **MUHAMMD ABDURRAHMAN**. Licensed under the Apache License, Version 2.0.

---

## 1. Storage Layout & Memory-Mapping (`mmap`)

AnchorDB separates storage into clean binary structures backed by `.anchordb` files:

```
+-------------------------------------------------------------------------------------+
|  Header (64B)  |  Nodes Array (40B/node)  |  String Pool  |  Entity Store  |  Edges  |
+-------------------------------------------------------------------------------------+
```

1. **Header (64 Bytes)** — little-endian, field order from `pkg/storage/binary_record.go`:
   - `Magic` (4B) = `0x48434E41` (`"ANCH"` when read little-endian)
   - `Version` (4B) = `3` (`DBVersion`); readers accept versions `1..3`, so v1/v2 files stay openable
   - `NodeCount` (8B)
   - `GridCellsX` (2B), `GridCellsY` (2B)
   - `GridOffset` (8B), `NodesOffset` (8B)
   - `StringOffset` (8B), `EntityCount` (8B), `EntityOffset` (8B)
   - `Reserved` (4B)
2. **Nodes Array (40 Bytes / Node)**:
   - Fixed-size binary structs: `ID (8B)`, `X, Y (8B)`, `Roles [4]float32 (16B)`, `DomainID (2B)`, `WordOffset (4B)`, `WordLen (2B)`.
   - **Zero-Copy Decoding**: Read directly from the `mmapData` pointer slice without heap duplication.
3. **String Pool**:
   - Packed raw UTF-8 bytes for graph labels. Words are referenced by `(WordOffset, WordLen)`.
4. **Entity Store**:
   - Key-value numeric attributes stored in compact contiguous slices (`CompactRecord`) with String Interning (`attrPool`) to minimize memory amplification.
5. **Edge Section (format version ≥ 3, optional)**:
   - Explicit directed, weighted relationships (`source --relation--> target`) appended after the entity store, introduced for cause-and-effect reasoning.
   - Framed as `EDG3` magic + `uint32` count, followed by length-prefixed `(source, relation, target, weight)` records.
   - Absent in v1/v2 files; the in-memory adjacency indexes (`adjOut`/`adjIn`) are rebuilt while loading.

---

## 2. Computational Model

Unlike traditional databases that transfer raw records to the application layer for arithmetic processing, AnchorDB computes on-the-fly inside storage:

* **In-Storage Mathematics**: Arithmetic operators (`+`, `-`, `*`, `/`) execute directly in CPU registers during record retrieval.
* **Epoch Days Date Math**: Dates are converted to 32-bit float Epoch Days (days since `1970-01-01`), enabling exact arithmetic (e.g. `2026-10-14` + `45` days) with zero SQL parsing overhead.
* **Directional Relations vs. Explicit Edges**: relation names map to a global query *direction vector* for ray-projection search, while the optional edge section stores explicit per-pair links (`source --relation--> target`) for deterministic cause-and-effect traversal and multi-hop chains.
* **Bounded $O(K)$ Paging**: Positional slicing and early termination ensure predictable 1–3 ms latency regardless of total dataset size.

---

## 3. Background Pipeline Engine (pure Go, self-contained)

AnchorDB implements its background capabilities itself — pure Go standard library, zero external broker daemons (no Redis, no Kafka, no cron/queue framework):

```
+--------------------+        +--------------------+
|  Cron Scheduler    | -----> |   Worker Engine    |
|  (pkg/scheduler)   |        |   (pkg/worker)     |
+--------------------+        +--------------------+
          |                             |
    Auto-Partition              Async Partition
 & Periodic Sync (5m)          & Background Sync
```

1. **Cron Scheduler (`pkg/scheduler`)**:
   - Implemented locally on `time` + `context`; no cron library is imported.
   - Built-in overlap prevention (`skip-if-still-running`), panic recovery, per-task timeout, optional jitter, and wall-clock-aware sliced waits (DST/NTP-jump safe).
   - Accepts human-friendly `HH:MM`, `@daily`/`@hourly`/`@every <dur>`, and the daily `M H * * *` cron subset (all other forms are rejected at startup).
   - Triggers automated partition archiving and periodic background disk flushes (`@every 5m`).
2. **Worker Pool (`pkg/worker`)**:
   - Implemented locally on a bounded Go channel + goroutine pool; the only transaction is the scheduler-to-worker handoff.
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
