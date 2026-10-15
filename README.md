# AnchorDB: High-Performance Computational Database Engine (CPU)

[![Go Version](https://img.shields.io/badge/Go-1.26+-blue.svg)](https://golang.org)
[![Platform](https://img.shields.io/badge/Platform-Linux%20amd64-green.svg)]()
[![Hardware](https://img.shields.io/badge/Hardware-100%25%20CPU%20%7C%20Zero%20GPU-orange.svg)]()
[![Architecture](https://img.shields.io/badge/Architecture-Minimal%20Dependencies%20%7C%20Zero%20Bloat-brightgreen.svg)]()
[![Dedicated Port](https://img.shields.io/badge/Port-7447-red.svg)]()
[![License](https://img.shields.io/badge/License-Commercial%20Proprietary-red.svg)](LICENSE)

**AnchorDB** is an ultra-fast, zero-GPU computational database and graph convergence engine built in pure Go. It delivers bounded **constant-time $O(K)$** performance for retrieval, in-storage arithmetic, date mathematics, multi-hop reasoning, and batch mutations on massive recordsets directly in CPU cache and memory-mapped storage.

Copyright (c) 2026 **MUHAMMD ABDURRAHMAN**. All Rights Reserved.

---

## Key Highlights

- **100% CPU, Zero GPU**: Operates on standard CPU registers and memory-mapped files (`mmap(MAP_SHARED)`) without expensive CUDA/GPU hardware or CGO.
- **Minimal Dependencies Architecture**: We reject dogmatic "zero-dependencies" purism in favor of **pragmatic, minimal, strictly-vetted dependencies**. The engine maintains zero bloat, zero heavy frameworks, zero external broker daemons (no Redis, no Kafka, no RabbitMQ), and a minimal attack surface.
- **Ultra-Compact Memory & Binary Layout**:
  - **Zero-Copy Node Decoding**: Historical nodes read directly from mmap without heap duplication.
  - **CompactRecord & String Interning**: Key-value entity attributes stored in contiguous slices with shared interned key pointers, slashing RAM footprint by ~70% (~80B/entity).
  - **Binary Header**: 64-byte fixed layout; **Record**: 40-byte fixed nodes.
- **Constant-Time Bound $O(K)$**: Whether querying or updating 10,000 records out of 100,000+, AnchorDB guarantees early termination and execution within 1 to 3 milliseconds.
- **In-Storage Mathematics & Projections**:
  - Arithmetic operators (`+`, `-`, `*`, `/`) executed inside storage without SQL overhead.
  - Date arithmetic stored as float32 Epoch Days (e.g. `2026-10-14` + `45 hari` $\rightarrow$ `2026-11-28`).
  - Selective column projection (`.Select("jarak", "waktu", "kecepatan")`).
- **Resilient Background Pipeline (Pure Go, zero external broker)**:
  - **Cron Scheduler (`pkg/scheduler`)**: Pure-Go scheduler with panic recovery, overlap prevention (skip-if-still-running), and graceful shutdown.
  - **Worker Engine (`pkg/worker`)**: Non-blocking asynchronous task execution on a bounded Go channel pool:
    - **Async Partition API (`POST /v1/partition/run?async=true`)**: Instant HTTP 202 response; offloads heavy serialization and S3 uploads to the worker pool.
    - **Periodic Background Disk Sync (`db.Sync()`)**: Streaming flush to disk every 5 minutes (default) without blocking client queries.
- **Single Direct Dependency**: Binary links exactly one external module — `github.com/google/uuid` — zero brokers, zero cron/queue frameworks, zero CGO.
- **Fine-Grained Memory Allocation**: Support for explicit soft RAM limits (**500MB, 1GB, 5GB**) with auto-detection of Docker/Podman cgroup ceilings.
- **Multi-Model Routing ("Satu DB = Satu Model")**: Dynamic file separation (`/data/{model}.anchordb`) with path traversal immunity.
- **Dedicated Port**: Standardized official port **`7447`**.

---

## Architecture Overview

```
                                      +---------------------------------------------+
                                      |            AnchorDB Server Daemon           |
                                      |                  (Port 7447)                |
                                      +----------------------+----------------------+
                                                             |
                   +-----------------------------------------+-----------------------------------------+
                   |                                         |                                         |
                   v                                         v                                         v
    +------------------------------+          +------------------------------+          +------------------------------+
    |       REST HTTP Engine       |          |    Cron Scheduler Daemon     |          |     Async Worker Engine      |
    | /v1/save, /v1/get, /v1/list  |          |        (pure Go)             |          |        (pure Go)             |
    | /v1/update, /v1/graph,       |          | Daily 23:00 / Sync Every 5m  |          | Async Partitions & Sync Pool |
    | /v1/edge, /v1/causal, /sync  |          |                              |          |                              |
    +--------------+---------------+          +--------------+---------------+          +--------------+---------------+
                   |                                         |                                         |
                   +-----------------------------------------+-----------------------------------------+
                                                             |
                                                             v
                                              +------------------------------+
                                              |  AnchorDB Multi-Model Cache  |
                                              |   (sync.RWMutex Threadsafe)  |
                                              +--------------+---------------+
                                                             |
                     +---------------------------------------+---------------------------------------+
                     |                                       |                                       |
                     v                                       v                                       v
      +-----------------------------+         +-----------------------------+         +-----------------------------+
      |  mmap(MAP_SHARED) Hot DB    |         |    In-Storage Math Engine   |         |    Partition Sink Engine    |
      | 64-byte Header, 40B Records |         |  O(K) Add, Multiply, Date   |         |  {date}_{uuid}.anchordb     |
      |   /data/{model}.anchordb    |         |  Early Termination Bound    |         |  Local Disk / S3 Object     |
      +-----------------------------+         +-----------------------------+         +-----------------------------+
```

---

## Background Subsystems: Cron & Worker

### 1. Cron Scheduler (`pkg/scheduler`, pure Go)
- **Automated Archival**: Runs daily (default `23:00`) to partition historical records older than $N$ days.
- **Periodic Background Sync**: Triggers safe disk streaming syncs at regular intervals (default `5m`).
- **Overlap Prevention**: Built-in atomic `running` single-flight protection and skipped-tick tracking.
- **Wall-Clock Awareness**: DST-safe calendar math and sliced waits resistant to NTP clock jumps.
- **On-Demand Execution**: Trigger any registered cron task on demand via `POST /v1/scheduler/run?task=<name>` or JSON `{"task": "<name>"}`.
- **Inspection**: Real-time status via `GET /v1/scheduler/status` reporting task state, skipped ticks, timeout, and jitter.

**Supported schedule formats** (`ANCHORDB_PARTITION_SCHEDULE` / `ANCHORDB_SYNC_INTERVAL`):

| Format | Example | Meaning |
| :--- | :--- | :--- |
| `HH:MM` | `23:00` | Daily at 23:00 |
| `M H * * *` | `0 23 * * *` | Daily at 23:00 |
| `@daily` | `@daily` | Daily at 00:00 |
| `@hourly` | `@hourly` | Every hour |
| Duration / `@every` | `5m`, `30m`, `@every 1h` | Recurring interval |

> **Note:** only the **time-of-day** is honored for cron specs — day-of-month, month and day-of-week
> fields (e.g. `0 2 * * 1`) are ignored, and other forms (e.g. `*/5 * * * *`, `@weekly`) are rejected
> at startup. Use a duration (`5m`) for sub-daily recurring work.

### 2. Worker Engine (`pkg/worker`, pure Go)
- **Async Partition Offloading (`POST /v1/partition/run?async=true`)**: Returns HTTP 202 immediately (`{"status": "accepted", "job_id": "<uuid>"}`) when the worker pool is enabled. The pool archives data to target sink partition files in the background.
- **Continuous Background Disk Sync**: Streams memory-mapped buffers to disk without locking client queries.
- **Throttled Concurrency**: A fixed pool of worker goroutines (`ANCHORDB_WORKER_CONCURRENCY`, default `CPU × 2`) prevents CPU starvation.
- **Bounded Task Queue**: Tasks flow through a bounded Go channel (`ANCHORDB_WORKER_QUEUE_SIZE`, default `1000`); dispatch is non-blocking and fails fast with `ErrQueueFull` when saturated.
- **Scoped Task Cancellation**: Cancel specific jobs via `POST /v1/worker/cancel` with `{"job_id": ...}` or `{"model": ...}`, or require explicit `{"all": true}` for global drain.
- **Inspection**: Real-time metrics via `GET /v1/worker/status`.

---

## Configuration Reference (Docker / Podman / CLI)

All settings can be configured via environment variables or CLI flags:

| Setting | CLI Flag | Environment Variable | Default | Description |
| :--- | :--- | :--- | :--- | :--- |
| **Port** | `-port` | `ANCHORDB_PORT` | `7447` | HTTP REST API port |
| **Data Dir** | `-data-dir` | `ANCHORDB_DATA_DIR` | `/data` or `./data` | Directory storing `*.anchordb` files |
| **Auth Token** | `-auth-token` | `ANCHORDB_AUTH_TOKEN` | `""` | Bearer authorization token |
| **Memory Limit** | `-memory-limit`, `-mem-limit`, `-m` | `ANCHORDB_MEMORY_LIMIT` | `""` (auto) | Soft memory limit (e.g. `500MB`, `1GB`, `5GB`) |
| **IP Whitelist** | `-ip-whitelist` | `ANCHORDB_IP_WHITELIST` | `""` (all open) | CIDR or IP list e.g. `127.0.0.1,192.168.1.0/24` |
| **Trust Proxy** | `-trust-proxy` | `ANCHORDB_TRUST_PROXY` | `false` | Trust `X-Forwarded-For` proxy headers |
| **Cron Enabled** | `-cron-enabled` | `ANCHORDB_CRON_ENABLED` | `true` | Enable background scheduler |
| **Partition Schedule** | `-partition-schedule` | `ANCHORDB_PARTITION_SCHEDULE` | `23:00` | Archival time (`HH:MM`, `M H * * *`, `@daily`, `@hourly`, or interval; see note above) |
| **Retention Days** | `-retention-days` | `ANCHORDB_PARTITION_RETENTION_DAYS` | `180` | Days of hot data before partitioning |
| **Sync Interval** | `-sync-interval` | `ANCHORDB_SYNC_INTERVAL` | `5m` | Background disk sync interval (`5m`, `30m`, etc.) |
| **Cron Task Timeout** | `-cron-task-timeout` | `ANCHORDB_CRON_TASK_TIMEOUT` | `""` | Per-task execution context timeout (e.g. `15m`, `1h`) |
| **Cron Jitter** | `-cron-jitter` | `ANCHORDB_CRON_JITTER` | `""` | Random delay added to scheduled fire time (e.g. `5s`) |
| **Shutdown Timeout** | `-shutdown-timeout` | `ANCHORDB_SHUTDOWN_TIMEOUT` | `25s` | Total graceful-shutdown budget, split across HTTP drain / scheduler / worker / final sync. Keep below your orchestrator grace period (K8s default 30s). A second `SIGINT/SIGTERM` force-exits (WAL is replayed on next start). While draining, `GET /health` returns `503 {"status":"shutting_down"}`. If HTTP/scheduler/worker do not stop in time, databases are **not closed** (only synced) so late writes are never lost. |
| **Worker Enabled** | `-worker-enabled` | `ANCHORDB_WORKER_ENABLED` | `true` | Enable background worker pool |
| **Worker Concurrency**| `-worker-concurrency` | `ANCHORDB_WORKER_CONCURRENCY` | `CPU * 2` | Max parallel worker goroutines |
| **Worker Queue Size** | `-worker-queue-size` | `ANCHORDB_WORKER_QUEUE_SIZE` | `1000` | Task queue capacity |
| **Worker Enforce Mem** | `-worker-enforce-memory` | `ANCHORDB_WORKER_ENFORCE_MEMORY` | `false` | Memory-aware admission control |
| **Worker Mem Thresh** | `-worker-memory-threshold` | `ANCHORDB_WORKER_MEMORY_THRESHOLD` | `0.85` | Memory pressure throttling ratio (0.0-1.0) |

---

## Deployment Examples

### 1. Podman / Docker (Pure Self-Contained)
```bash
podman run -d \
  --name anchordb \
  -p 7447:7447 \
  -m 1g --memory-swap 1g \
  -e ANCHORDB_MEMORY_LIMIT=800MB \
  -e ANCHORDB_CRON_ENABLED=true \
  -e ANCHORDB_PARTITION_SCHEDULE="23:00" \
  -e ANCHORDB_SYNC_INTERVAL="5m" \
  -e ANCHORDB_WORKER_CONCURRENCY=8 \
  -v ./data:/data:Z \
  anchordb:latest
```

### 2. Docker Compose
```yaml
services:
  anchordb:
    image: anchordb:latest
    ports:
      - "7447:7447"
    environment:
      - ANCHORDB_MEMORY_LIMIT=800MB
      - ANCHORDB_CRON_ENABLED=true
      - ANCHORDB_SYNC_INTERVAL=5m
      - ANCHORDB_WORKER_CONCURRENCY=8
    volumes:
      - ./data:/data:Z
    deploy:
      resources:
        limits:
          memory: 1G
```

---

## REST API Reference

All requests and responses use JSON. When `ANCHORDB_AUTH_TOKEN` is configured, include `Authorization: Bearer <token>`.

### 1. Telemetry & Health Checks
- `GET /health`: Comprehensive system status, memory allocations, and background subsystem health.
- `GET /v1/scheduler/status`: Active cron tasks and next execution times.
- `GET /v1/worker/status`: Worker pool concurrency, in-flight/queued tasks, memory pressure, and telemetry stats.
- `POST /v1/worker/cancel`: Safely cancels queued tasks before execution without interrupting in-flight jobs.

### 2. Mutation & Query Endpoints
- `POST /v1/save`: Ingest an entity with key-value properties and an optional `operation` status (`1` = Active by default). `0` (Safe-Delete) is an internal partition-lock state and is **rejected** by `/v1/save`; records only enter Safe-Delete through the partition archival flow (lock → archive → complete), never by direct save.
- `POST /v1/get`: Query an entity by name with optional in-storage computations (returns properties and current `operation` status).
- `POST /v1/list`: Paginated constant-time retrieval with arithmetic and date filters (filters `operation = 1` active by default; supports `operation` filter or `include_all_operations: true`).
- `POST /v1/update`: Constant-time batch update with arithmetic mutations (automatically skips entities not in active `operation = 1` state; add `"async": true` or `?async=true` to offload to worker).
- `POST /v1/graph`: Graph ray-projection multi-hop reasoning.
- `POST /v1/edge`: Registers or removes an explicit directed edge `source --relation--> target` with a numeric `weight` (`action: "add"` default, or `"remove"`), enabling deterministic cause-and-effect links.
- `POST /v1/causal`: Traverses explicit edges from `seeds`; a single `relation` performs one hop, while a `relations` list performs a weighted multi-hop chain.
- `GET /v1/sync?model={name}`: Flushes hot mmap changes to disk (add `?async=true` to offload to worker).
- `POST /v1/partition/run`: Triggers partition archival with Safe-Delete rollback protection (`0` Safe-Delete during archival $\rightarrow$ `2` Tombstone on success; add `"async": true` in JSON body to offload to worker).
- `POST /v1/collector/run`: Runs garbage collection to purge records in `operation = 2` (Tombstone) state (add `"async": true` or `?async=true` to offload to worker).

### 3. Record Operation Lifecycle Machine
AnchorDB records follow a resilient four-state operational lifecycle:
- **`1` (ACTIVE)**: Normal queryable record (default for new records). Eligible for reads and updates.
- **`0` (SAFE_DELETE)**: In-flight safe delete / pending archival / partition lock. Locked against mutations; excluded from normal queries.
- **`2` (TOMBSTONE)**: Marked for deletion upon partition success. Excluded from normal lookups and swept in batches by the background collector during periodic disk sync or `/v1/collector/run`.
- **`3` (RETRY)**: Execution failed (e.g. S3 upload timeout) or interrupted by server shutdown/crash while in `0` (Safe-Delete). The startup crash reconciler automatically detects interrupted `0` records and promotes them to `3`. The next partition run automatically re-attempts records in state `3`.

### 4. Durability & Concurrency Guarantees
- **Single-writer per file**: `Open()` takes an exclusive advisory lock on a `<db>.lock` sidecar held for the instance lifetime; a second process opening the same `.anchordb` fails fast instead of corrupting state.
- **Write-ahead log with CRC framing**: every mutation is appended to `<db>.wal` (magic + version + opcode + length + CRC32) and `fsync`ed before it is applied in memory. On recovery, an incomplete trailing frame is safely truncated, while a complete but corrupt frame fails closed (writes and checkpointing are disabled until repaired).
- **Atomic sync**: `db.Sync()` streams a full snapshot to a temp file, `fsync`s it, `rename`s it over the database, and only then swaps the live `mmap` — a failure leaves the previous mapping and WAL intact rather than an unusable database.
- **Partition locks are enforced**: records in Safe-Delete (`0`) reject updates, deletes, and re-saves until the archival flow completes or the startup reconciler releases them to Retry (`3`).
- **Backward-compatible format versioning**: `DBVersion` is now `3` (adds the optional causal-edge section). Readers accept versions `1..3`, so databases written by older binaries still open; the legacy `WALManager.Replay` callback keeps its original signature (use `ReplayEvents` to observe edge operations).

---

## License & Commercial Property

AnchorDB is proprietary commercial software owned exclusively by **MUHAMMD ABDURRAHMAN**.

Copyright (c) 2026 **MUHAMMD ABDURRAHMAN**. All Rights Reserved.

For full license terms and enterprise licensing inquiries, refer to the [LICENSE](LICENSE) file.
