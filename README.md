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
- **Minimal Dependencies Architecture**: We reject dogmatic "zero-dependencies" purism in favor of **pragmatic, minimal, strictly-vetted dependencies**. The engine maintains zero bloat, zero heavy frameworks, zero ORMs, and a minimal attack surface while allowing targeted, best-of-breed primitives where they enhance correctness and throughput.
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
  - **Worker Engine (`gosuite/v3/worker`)**: Non-blocking asynchronous task execution and sub-worker fan-out pools.
  - **Event Broker & Queue (`gosuite/v3/queue`)**: Pluggable asynchronous messaging with high-speed in-memory driver (default, zero external dependencies) and optional **Redis** (`queue-redis`) or **NATS JetStream** (`queue-nats`) adapters.
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
    | /v1/update, /v1/graph, /sync |          | Daily 23:00 / Custom Cron    |          | Concurrency & Sub-Workers    |
    +--------------+---------------+          +--------------+---------------+          +--------------+---------------+
                   |                                         |                                         |
                   +-----------------------------------------+-----------------------------------------+
                                                             |
                                                             v
                                              +------------------------------+
                                              |     Event Queue Broker       |
                                              |     (gosuite/v3/queue)       |
                                              | Memory / Redis / NATS Adapters|
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

## Background Subsystems: Cron, Worker & Queue

AnchorDB includes production-grade background capabilities powered by `github.com/rachmanzz/gosuite/v3`:

### 1. Cron Scheduler (`pkg/scheduler` $\rightarrow$ `gosuite/v3/cron`)
- **Automated Archival**: Automatically triggers partition runs at scheduled times (default `23:00` daily or standard cron syntax like `0 23 * * *`).
- **Panic Recovery & Overlap Protection**: Jobs are guaranteed not to run concurrently if a previous iteration is still active (`skip-if-still-running`).
- **Lifecycle Status**: Real-time task inspection via `GET /v1/scheduler/status`.

### 2. Worker Engine (`pkg/worker` $\rightarrow$ `gosuite/v3/worker`)
- **Non-Blocking Task Offloading**: Heavy partition file generation, S3 archival uploads, and disk flushes are offloaded from HTTP request goroutines.
- **Configurable Concurrency**: Automatically scales to available CPU cores with configurable limits.
- **Worker Status**: Real-time stats (dispatched, completed, failed) via `GET /v1/worker/status`.

### 3. Queue & Event Broker (`pkg/queue` $\rightarrow$ `gosuite/v3/queue`)
- **Change Data Capture (CDC)**: Publishes database mutation events:
  - `anchordb.record.saved`
  - `anchordb.record.updated`
  - `anchordb.partition.archived`
- **Zero-Dependency by Default**: Operates out of the box using a high-throughput in-memory queue.
- **Pluggable External Adapters**:
  - **Redis (`queue-redis`)**: Priority ZSet and delayed scheduling over Redis.
  - **NATS JetStream (`queue-nats`)**: Streaming deduplication and distributed pull consumers.
- **Queue Status**: Live message counters via `GET /v1/queue/status`.

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
| **Worker Enabled** | `-worker-enabled` | `ANCHORDB_WORKER_ENABLED` | `true` | Enable background worker pool |
| **Worker Concurrency**| `-worker-concurrency` | `ANCHORDB_WORKER_CONCURRENCY` | `CPU * 2` | Max parallel worker goroutines |
| **Worker Queue Size** | `-worker-queue-size` | `ANCHORDB_WORKER_QUEUE_SIZE` | `1000` | Task queue capacity |
| **Queue Enabled** | `-queue-enabled` | `ANCHORDB_QUEUE_ENABLED` | `true` | Enable async queue broker |
| **Queue Driver** | `-queue-driver` | `ANCHORDB_QUEUE_DRIVER` | `memory` | Adapter: `memory`, `redis`, or `nats` |
| **Queue Consumer** | `-queue-consumer` | `ANCHORDB_QUEUE_CONSUMER_ENABLED` | `true` | Enable background queue consumer |
| **Redis Address** | `-redis-addr` | `ANCHORDB_REDIS_ADDR` | `""` | Redis host:port (e.g. `localhost:6379`) |
| **Redis Password** | `-redis-password` | `ANCHORDB_REDIS_PASSWORD` | `""` | Redis authentication password |
| **Redis Database** | `-redis-db` | `ANCHORDB_REDIS_DB` | `0` | Redis database index |
| **Redis Prefix** | `-redis-prefix` | `ANCHORDB_REDIS_PREFIX` | `anchordb:queue:` | Redis key namespace |
| **NATS URL** | `-nats-url` | `ANCHORDB_NATS_URL` | `""` | NATS URL (e.g. `nats://localhost:4222`) |
| **NATS Stream** | `-nats-stream` | `ANCHORDB_NATS_STREAM` | `anchordb_stream`| NATS JetStream stream identifier |

---

## Deployment Examples

### 1. Docker / Podman (Standalone In-Memory)
```bash
podman run -d \
  --name anchordb \
  -p 7447:7447 \
  -m 1g --memory-swap 1g \
  -e ANCHORDB_MEMORY_LIMIT=800MB \
  -e ANCHORDB_CRON_ENABLED=true \
  -e ANCHORDB_PARTITION_SCHEDULE="23:00" \
  -e ANCHORDB_WORKER_CONCURRENCY=8 \
  -e ANCHORDB_QUEUE_DRIVER=memory \
  -v ./data:/data:Z \
  anchordb:latest
```

### 2. Docker / Podman with Redis Queue Adapter
```bash
podman run -d \
  --name anchordb \
  -p 7447:7447 \
  -e ANCHORDB_QUEUE_DRIVER=redis \
  -e ANCHORDB_REDIS_ADDR=redis:6379 \
  -v ./data:/data:Z \
  anchordb:latest
```

### 3. Docker Compose (Full Stack)
```yaml
services:
  anchordb:
    image: anchordb:latest
    ports:
      - "7447:7447"
    environment:
      - ANCHORDB_MEMORY_LIMIT=800MB
      - ANCHORDB_CRON_ENABLED=true
      - ANCHORDB_WORKER_CONCURRENCY=8
      - ANCHORDB_QUEUE_DRIVER=memory # or 'redis' / 'nats'
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
- `GET /v1/queue/status`: Queue broker stats and event metrics.

### 2. Mutation & Query Endpoints
- `POST /v1/save`: Ingest an entity with key-value properties. Publishes `anchordb.record.saved`.
- `POST /v1/get`: Query an entity by name with optional in-storage computations.
- `POST /v1/list`: Paginated constant-time retrieval with arithmetic and date filters.
- `POST /v1/update`: Constant-time batch update with arithmetic mutations. Publishes `anchordb.record.updated`.
- `POST /v1/graph`: Graph ray-projection multi-hop reasoning.
- `GET /v1/sync?model={name}`: Flushes hot mmap changes to disk.
- `POST /v1/partition/run`: Manually trigger partition archival. Publishes `anchordb.partition.archived`.

---

## License & Commercial Property

AnchorDB is proprietary commercial software owned exclusively by **MUHAMMD ABDURRAHMAN**.

Copyright (c) 2026 **MUHAMMD ABDURRAHMAN**. All Rights Reserved.

For full license terms and enterprise licensing inquiries, refer to the [LICENSE](LICENSE) file.
