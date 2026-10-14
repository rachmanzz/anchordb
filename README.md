# AnchorDB: High-Performance Computational Database Engine (CPU)

[![Go Version](https://img.shields.io/badge/Go-1.27.1-blue.svg)](https://golang.org)
[![Platform](https://img.shields.io/badge/Platform-Linux%20amd64-green.svg)]()
[![Hardware](https://img.shields.io/badge/Hardware-100%25%20CPU%20%7C%20Zero%20GPU-orange.svg)]()
[![Dedicated Port](https://img.shields.io/badge/Port-7447-red.svg)]()
[![License](https://img.shields.io/badge/License-Commercial%20Proprietary-red.svg)](LICENSE)

**AnchorDB** is an ultra-fast, zero-GPU computational database and graph convergence engine built in pure Go. It delivers bounded **constant-time $O(K)$** performance for retrieval, in-storage arithmetic, date mathematics, multi-hop reasoning, and batch mutations on massive recordsets directly in CPU cache and memory-mapped storage.

Copyright (c) 2026 **MUHAMMD ABDURRAHMAN**. All Rights Reserved.

---

## Key Highlights

- **100% CPU, Zero GPU**: Operates on standard CPU registers and memory-mapped files (`mmap(MAP_SHARED)`) without expensive CUDA/GPU hardware or external CGO dependencies.
- **Ultra-Compact Binary Layout**:
  - **Header**: 64-byte fixed memory layout.
  - **Record**: 40-byte fixed binary nodes (ID, 2D coordinates, 4 semantic role floats, domain tag, word offset).
- **Constant-Time Bound $O(K)$**: Whether querying or updating 10,000 records out of 100,000+, AnchorDB guarantees early termination and execution within 1 to 3 milliseconds.
- **In-Storage Mathematics & Projections**:
  - Arithmetic operators (`+`, `-`, `*`, `/`) executed inside storage without SQL overhead.
  - Date arithmetic stored as float32 Epoch Days (e.g. `2026-10-14` + `45 hari` $\rightarrow$ `2026-11-28`).
  - Selective column projection (`.Select("jarak", "waktu", "kecepatan")`).
- **Internal Scheduling & Partitioning (Zero External Cron)**:
  - Background daemon checks data on a schedule (e.g., daily at `23:00`).
  - Automatically exports records older than $N$ months into standalone archive files formatted as `{date}_{uuid}.anchordb`.
  - Supports pluggable destinations: **Local Disk** or **Cloud Object Storage (S3 / MinIO / Cloudflare R2 / GCS)**.
  - Prunes archived records from the hot database to keep storage lean.
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
                                   +-------------------------+-------------------------+
                                   |                                                   |
                                   v                                                   v
                    +------------------------------+                    +------------------------------+
                    |       REST HTTP Engine       |                    |  Internal Scheduler Daemon   |
                    | /v1/save, /v1/get, /v1/list  |                    | Background loop (e.g. 23:00) |
                    | /v1/update, /v1/graph, /sync |                    | Lifecycle & Auto-Partition   |
                    +--------------+---------------+                    +--------------+---------------+
                                   |                                                   |
                                   +-------------------------+-------------------------+
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

## Quick Start (Go Driver SDK)

### 1. Installation

```bash
go get github.com/rachmanzz/anchordb
```

### 2. Basic Example: In-Storage Math & Date Calculation

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/rachmanzz/anchordb/pkg/driver"
)

func main() {
	ctx := context.Background()

	// 1. Connect via driver (mmap zero-copy)
	client, err := driver.Connect("anchordb:///data/kinematika.anchordb?mode=mmap")
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// 2. Save records with numeric attributes and dates
	_ = client.Save("mobil_1", map[string]float32{
		"jarak":   100,
		"waktu":   2,
		"tanggal": driver.Date("2026-10-14"),
	})

	// 3. Selective Projection & On-the-fly Calculation
	result, _ := client.Get("mobil_1").
		Select("jarak", "waktu", "kecepatan").
		Compute("kecepatan", driver.Divide("jarak", "waktu")).
		Execute(ctx)

	fmt.Printf("Kecepatan: %.1f km/jam\n", result.Get("kecepatan")) // 50.0

	// 4. Batch in-place update (add 10 to distance, add 45 days to date)
	updated, _ := client.Update().
		Where("jarak", ">=", 50).
		Set("jarak", driver.Add("jarak", "10")).
		Set("tanggal", driver.AddDays("tanggal", 45)).
		Execute(ctx)

	fmt.Printf("Updated %d records in constant time\n", updated)
}
```

---

## Server Deployment

### Run via Docker / Podman

```bash
# Run with optional IP Whitelisting & Scheduling
podman run -d \
  --name anchordb \
  -p 7447:7447 \
  -v ./data:/data:Z \
  -e ANCHORDB_PORT=7447 \
  -e ANCHORDB_IP_WHITELIST="127.0.0.1,::1,192.168.1.0/24,10.0.0.1-10.0.0.50" \
  -e ANCHORDB_PARTITION_SCHEDULE="23:00" \
  localhost/anchordb:latest
```

Or using Docker Compose:
```bash
docker compose up -d
```

### Security & IP Whitelisting

AnchorDB includes a built-in network firewall gateway. Set `ANCHORDB_IP_WHITELIST` to restrict connections:

- **Single Exact IPs**: `127.0.0.1`, `::1`, `203.0.113.15`
- **CIDR Subnet Blocks**: `192.168.1.0/24`, `10.0.0.0/16`
- **Explicit IP Ranges**: `172.16.0.10-172.16.0.50`
- **Behind Proxy / Load Balancer**: Set `ANCHORDB_TRUST_PROXY=true` to validate `X-Forwarded-For` and `X-Real-IP`.

If client IP is not in the whitelist, AnchorDB immediately rejects with `HTTP 403 Forbidden` before consuming CPU or locks.

### REST API Reference

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `GET` | `/health` | Healthcheck and server status |
| `POST` | `/v1/save` | Store entity properties in model |
| `POST` | `/v1/get` | Retrieve entity with selective columns & math |
| `POST` | `/v1/list` | Stream / paginate entities with range & date filters |
| `POST` | `/v1/update` | Batch in-storage update with constant-time early exit |
| `POST` | `/v1/graph` | Execute spatial ray-projection & hourglass convergence |
| `POST` | `/v1/partition/run` | Trigger on-demand archival partition (Local / S3) |
| `GET` | `/v1/scheduler/status` | View background scheduler tasks and next run times |

#### Example: Query with Computations via cURL
```bash
curl -X POST http://localhost:7447/v1/get \
  -H "Content-Type: application/json" \
  -d '{
    "model": "kinematika",
    "name": "mobil_1",
    "select": ["jarak", "waktu", "kecepatan"],
    "computations": {"kecepatan": "jarak / waktu"}
  }'
```

---

## Memory Allocation & Resource Tuning (RAM Limits)

AnchorDB provides fine-grained memory bounds with native Go runtime soft limit enforcement (`debug.SetMemoryLimit`). This ensures the Go runtime aggressively reclaims memory before reaching host or container ceilings, eliminating container OOM-killer crashes.

You can configure memory limits (**500MB, 1GB, 5GB**, etc.) via multiple convenient mechanisms:

### 1. Docker / Podman Container Flags
```bash
# Limit container to 1 GB RAM with 800 MB soft limit:
docker run -d \
  --name anchordb \
  -p 7447:7447 \
  -m 1g --memory-swap 1g \
  -e ANCHORDB_MEMORY_LIMIT=800MB \
  -v ./data:/data:Z \
  anchordb:latest
```
*(If `ANCHORDB_MEMORY_LIMIT` is omitted inside a container, AnchorDB automatically detects the cgroup memory ceiling and sets the soft limit to 85% of container RAM).*

### 2. Docker Compose
```yaml
services:
  anchordb:
    image: anchordb:latest
    environment:
      - ANCHORDB_MEMORY_LIMIT=800MB # 500MB, 1GB, 5GB
    deploy:
      resources:
        limits:
          memory: 1G
```

### 3. Server CLI Flags or Environment Variables
```bash
# Via CLI flag:
./anchordb-server -memory-limit 1GB
./anchordb-server -m 500MB
./anchordb-server -mem-limit 5GB

# Via Environment Variable:
export ANCHORDB_MEMORY_LIMIT=1GB
./anchordb-server
```

### 4. Real-time Memory Monitoring via `/health`
Check live memory allocations, current heap usage, and configured limits:
```bash
curl http://localhost:7447/health
```
```json
{
  "status": "ok",
  "port": "7447",
  "memory": {
    "limit_bytes": 1073741824,
    "limit_human": "1.00 GB",
    "heap_alloc_bytes": 2451000,
    "heap_alloc_human": "2.34 MB",
    "heap_sys_bytes": 12582912,
    "heap_sys_human": "12.00 MB",
    "num_gc": 4
  }
}
```

---

## Benchmark Metrics

Measured on standard CPU (Intel Core i7-1165G7 @ 2.80GHz):

| Operation | Throughput / Latency |
| :--- | :--- |
| **Ingestion Throughput** | ~842,000 records / second / core |
| **10,000 Records Retrieval** | ~1.6 ms |
| **10,000 Records Batch Math Update** | ~1.8 ms |
| **Ray Convergence Calculation** | 3.7 microseconds / op |
| **Internal Scheduler Task Execution** | 6.0 to 14.1 microseconds |
| **Container Size** | **14.4 MB** (Alpine) |

---

## License & Commercial Property

AnchorDB is proprietary commercial software owned exclusively by **MUHAMMD ABDURRAHMAN**.

Copyright (c) 2026 **MUHAMMD ABDURRAHMAN**. All Rights Reserved.

For full license terms and enterprise licensing inquiries, refer to the [LICENSE](LICENSE) file.
