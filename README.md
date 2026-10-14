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
- **Resilient Background Pipeline (`gosuite/v3`)**:
  - **Production Cron (`gosuite/v3/cron`)**: Automated partition archiving with panic recovery, overlap prevention, and graceful shutdown.
  - **Worker Engine (`gosuite/v3/worker`)**: Non-blocking asynchronous task execution:
    - **Async Partition API (`POST /v1/partition/run?async=true`)**: Instant HTTP 202 response; offloads heavy serialization and S3 uploads to worker pool.
    - **Periodic Background Disk Sync (`db.Sync()`)**: Continuous streaming flush to disk every 5 minutes without query blocking.
  - **Event Broker & Queue (`gosuite/v3/queue`)**: High-throughput CPU-native in-memory queue for Change Data Capture (CDC) events without external service dependencies.
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
    | /v1/save, /v1/get, /v1/list  |          |     (gosuite/v3/cron)        |          |    (gosuite/v3/worker)       |
    | /v1/update, /v1/graph, /sync |          | Daily 23:00 / Sync Every 5m  |          | Async Partitions & Sync Pool |
    +--------------+---------------+          +--------------+---------------+          +--------------+---------------+
                   |                                         |                                         |
                   +-----------------------------------------+-----------------------------------------+
                                                             |
                                                             v
                                              +------------------------------+
                                              |     In-Memory Event Queue    |
                                              |     (gosuite/v3/queue)       |
                                              |  Zero External Dependencies  |
                                              +--------------+---------------+
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

## Background Subsystems: Cron, Worker & In-Memory Queue

### 1. Cron Scheduler (`pkg/scheduler` $\rightarrow$ `gosuite/v3/cron`)
- **Automated Archival**: Runs daily (default `23:00` or custom cron expression) to partition historical records older than $N$ days.
- **Periodic Background Sync**: Triggers safe disk streaming syncs at regular intervals (default `@every 5m`).
- **Overlap Prevention**: Built-in `skip-if-still-running` protection and panic recovery.
- **Inspection**: Real-time status via `GET /v1/scheduler/status`.

### 2. Worker Engine (`pkg/worker` $\rightarrow$ `gosuite/v3/worker`)
- **Async Partition Offloading (`POST /v1/partition/run?async=true`)**: Returns HTTP 202 immediately (`{"status": "queued"}`) within 1 millisecond. The worker pool generates partition files and uploads to S3 in the background.
- **Continuous Background Disk Sync**: Safely streams memory-mapped buffers to disk without locking client queries.
- **Throttled Concurrency**: Controlled worker goroutines prevent CPU starvation.
- **Inspection**: Real-time metrics via `GET /v1/worker/status`.

### 3. In-Memory Queue Broker (`pkg/queue` $\rightarrow$ `gosuite/v3/queue`)
- **Change Data Capture (CDC)**: Publishes database mutation events:
  - `anchordb.record.saved`
  - `anchordb.record.updated`
  - `anchordb.partition.archived`
- **Zero External Dependencies**: Operates completely in-memory using CPU-native Go channels and queues (no Redis, no NATS, no RabbitMQ required).
- **Inspection**: Real-time message counters via `GET /v1/queue/status`.

---

## Configuration Reference (Docker / Podman / CLI)

All settings can be configured via environment variables or CLI flags:

| Setting | CLI Flag | Environment Variable | Default | Description |
| :--- | :--- | :--- | :--- | :--- |
| **Port** | `-port` | `ANCHORDB_PORT` | `7447` | HTTP REST API port |
| **Data Dir** | `-data-dir` | `ANCHORDB_DATA_DIR` | `/data` or `./data` | Directory storing `*.anchordb` files |
| **Auth Token** | `-auth-token` | `ANCHORDB_AUTH_TOKEN` | `""` | Bearer authorization token |
| **Memory Limit** | `-memory-limit`, `-m` | `ANCHORDB_MEMORY_LIMIT` | `""` (auto) | Soft memory limit (e.g. `500MB`, `1GB`, `5GB`) |
| **IP Whitelist** | `-ip-whitelist` | `ANCHORDB_IP_WHITELIST` | `""` (all open) | CIDR or IP list e.g. `127.0.0.1,192.168.1.0/24` |
| **Trust Proxy** | `-trust-proxy` | `ANCHORDB_TRUST_PROXY` | `false` | Trust `X-Forwarded-For` proxy headers |
| **Cron Enabled** | `-cron-enabled` | `ANCHORDB_CRON_ENABLED` | `true` | Enable background scheduler |
| **Partition Schedule** | `-partition-schedule` | `ANCHORDB_PARTITION_SCHEDULE` | `23:00` | Archival time (`HH:MM` or cron spec `0 23 * * *`) |
| **Retention Days** | `-retention-days` | `ANCHORDB_PARTITION_RETENTION_DAYS` | `180` | Days of hot data before partitioning |
| **Sync Interval** | `-sync-interval` | `ANCHORDB_SYNC_INTERVAL` | `5m` | Background disk sync interval |
| **Worker Enabled** | `-worker-enabled` | `ANCHORDB_WORKER_ENABLED` | `true` | Enable background worker pool |
| **Worker Concurrency**| `-worker-concurrency` | `ANCHORDB_WORKER_CONCURRENCY` | `CPU * 2` | Max parallel worker goroutines |
| **Worker Queue Size** | `-worker-queue-size` | `ANCHORDB_WORKER_QUEUE_SIZE` | `1000` | Task queue capacity |
| **Queue Enabled** | `-queue-enabled` | `ANCHORDB_QUEUE_ENABLED` | `true` | Enable in-memory async event broker |
| **Queue Consumer** | `-queue-consumer` | `ANCHORDB_QUEUE_CONSUMER_ENABLED` | `true` | Enable background queue consumer |

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
- `GET /v1/worker/status`: Worker pool concurrency and task execution counters.
- `GET /v1/queue/status`: In-memory queue stats and event metrics.

### 2. Mutation & Query Endpoints
- `POST /v1/save`: Ingest an entity with key-value properties. Publishes `anchordb.record.saved`.
- `POST /v1/get`: Query an entity by name with optional in-storage computations.
- `POST /v1/list`: Paginated constant-time retrieval with arithmetic and date filters.
- `POST /v1/update`: Constant-time batch update with arithmetic mutations. Publishes `anchordb.record.updated`.
- `POST /v1/graph`: Graph ray-projection multi-hop reasoning.
- `GET /v1/sync?model={name}`: Flushes hot mmap changes to disk (add `?async=true` to offload to worker).
- `POST /v1/partition/run`: Triggers partition archival (add `"async": true` in JSON body or `?async=true` to offload to worker and return HTTP 202 immediately). Publishes `anchordb.partition.archived`.

---

## License & Commercial Property

AnchorDB is proprietary commercial software owned exclusively by **MUHAMMD ABDURRAHMAN**.

Copyright (c) 2026 **MUHAMMD ABDURRAHMAN**. All Rights Reserved.

For full license terms and enterprise licensing inquiries, refer to the [LICENSE](LICENSE) file.
