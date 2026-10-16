# AnchorDB

[![Go Version](https://img.shields.io/badge/Go-1.26+-blue.svg)](https://golang.org)

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Build Status](https://img.shields.io/badge/build-passing-brightgreen.svg)]()

AnchorDB is an embedded and network-accessible database engine written in Go. It combines key-value record storage with in-storage arithmetic and date computations, a 2D spatial and directed causal graph engine, write-ahead logging (WAL), memory-mapped persistence, and automated background data partitioning.

---

## Features

- **Memory-Mapped Storage (`mmap`)**: Uses shared memory mapping for reading and writing records directly, with atomic snapshot synchronization to disk.
- **In-Storage Computations**: Evaluates arithmetic expressions (`+`, `-`, `*`, `/`) and date math (epoch day arithmetic) directly during read queries or batch updates, avoiding the round-trip overhead of external compute layers.
- **Compact Record Storage**: Key-value entity attributes are stored using contiguous binary structures and interned attribute names to reduce heap allocations and memory usage.
- **Spatial & Causal Graph Capabilities**:
  - **2D Spatial Index**: Grid-based indexing supporting directional ray-casting, barycenter computation, and nearest-neighbor search.
  - **Causal Graph**: Directed, weighted edges (`source --relation--> target`) with support for single-step lookups and multi-hop causal chain traversal.
- **Crash Durability & WAL**: Write-Ahead Logging with CRC32 frame checksums, automatic recovery on startup, and configurable sync modes (`always`, `interval`, `none`).
- **Data Lifecycle & Archival**:
  - Four-state lifecycle (`ACTIVE`, `SAFE_DELETE`, `TOMBSTONE`, `RETRY`) preventing data loss during archival.
  - Automated partition archival by retention period to local disk or S3-compatible object storage.
- **Background Subsystems**:
  - Wall-clock aware cron scheduler (`pkg/scheduler`) for scheduled syncs and archival.
  - Asynchronous worker pool (`pkg/worker`) for background jobs and non-blocking maintenance tasks.
- **Strict Schema Registration & Immutability**:
  - One-time permanent schema locking with scalar primitives and 1D arrays (`string[]`, `float[]`, `int[]`, `bool[]`, `date[]`).
  - Required vs. optional (`?`) syntax, fail-fast rejection (`ErrSchemaMismatch`), and administrative reset.
- **API & Client SDK**: RESTful HTTP API (default port `7447`) and a fluent Go driver library (`pkg/driver`).

---

## Feature Documentation

- **[Multi-Model Architecture: One Model = One File](docs/multi_model_architecture.md)**: Independent storage files, failure isolation, and memory-mapped model routing.
- **[Model Schema Registration & Immutability Specification](docs/model_schema_specification.md)**: Strict contract enforcement, primitives, 1D arrays, and fail-fast validation.
- **[In-Storage Computations & Date Math](docs/in_storage_compute.md)**: Operators, date arithmetic, attribute projections, and batch updates.
- **[Causal & Spatial Graph Engine](docs/causal_and_spatial_graph.md)**: 2D spatial indexing, ray casting, and weighted causal chains.
- **[Storage Engine, WAL & Durability](docs/storage_and_durability.md)**: Binary format, write-ahead log modes, and crash recovery.
- **[Data Partitioning & Record Lifecycle](docs/partitioning_and_lifecycle.md)**: 4-state lifecycle machine, archival sinks, and tombstone collection.
- **[Background Subsystems: Scheduler & Worker Pool](docs/background_subsystems.md)**: Wall-clock cron scheduler and bounded async worker pool.
- **[REST API Reference](docs/rest_api.md)**: HTTP daemon configuration, authentication, telemetry, and endpoints.
- **[Go Driver SDK Guide](docs/go_driver.md)**: Connection setup, query builders, and integration examples.
- **[Benchmark & Verification Guide](docs/benchmark_and_verification.md)**: Reproducible test suite, Little's Law validation, and container durability.
- **[Installation & Deployment Guide](docs/installation_and_deployment.md)**: Native packages (.deb, .rpm), systemd service management, Docker/Podman container images, and universal archives.



---

## Architecture

```
                       +---------------------------------------------+
                       |            AnchorDB Server Daemon           |
                       |                  (Port 7447)                |
                       +----------------------+----------------------+
                                              |
                   +--------------------------+--------------------------+
                   |                          |                          |
                   v                          v                          v
    +------------------------------+   +--------------------+   +---------------------+
    |       REST HTTP Engine       |   |   Cron Scheduler   |   | Async Worker Pool   |
    |  /v1/save, /v1/get, /v1/list |   |     (pkg/scheduler)|   |    (pkg/worker)     |
    |  /v1/update, /v1/edge, etc.  |   | Periodic Sync/Jobs |   | Background Tasks    |
    +--------------+---------------+   +----------+---------+   +----------+----------+
                   |                              |                        |
                   +------------------------------+------------------------+
                                                  |
                                                  v
                               +-------------------------------------+
                               |         AnchorDB Engine Core        |
                               |  Entity Store, Spatial Grid, Lexicon|
                               +------------------+------------------+
                                                  |
                     +----------------------------+----------------------------+
                     |                            |                            |
                     v                            v                            v
      +-----------------------------+   +--------------------+   +--------------------------+
      |  mmap(MAP_SHARED) Hot DB    |   |  Write-Ahead Log   |   |   Partition Sink Engine  |
      |   /data/{model}.anchordb    |   |     (.wal, CRC32)  |   |   Local Disk / S3 Object |
      +-----------------------------+   +--------------------+   +--------------------------+
```

---

## Installation & Quick Start

AnchorDB offers multiple official installation and deployment methods:

For complete operational and deployment options, see the **[Installation & Deployment Guide](docs/installation_and_deployment.md)**.

### 1. Native Linux Packages (`.deb` / `.rpm`) with Automated Background Systemd

Pre-built native packages are published on [GitHub Releases](https://github.com/rachmanzz/anchordb/releases). Installing a package automatically provisions the `anchordb:anchordb` service user, configures `/var/lib/anchordb` with `0750` permissions, and registers a background `systemd` service that starts automatically.

```bash
# Ubuntu / Debian (.deb)
wget https://github.com/rachmanzz/anchordb/releases/latest/download/anchordb_0.1.0_amd64.deb
sudo dpkg -i anchordb_0.1.0_amd64.deb

# RHEL / CentOS / Fedora (.rpm)
sudo rpm -ivh https://github.com/rachmanzz/anchordb/releases/latest/download/anchordb_0.1.0_amd64.rpm
```

Verify service status:
```bash
sudo systemctl status anchordb
```
*(Configuration is managed at `/etc/default/anchordb` or `/etc/sysconfig/anchordb`. If port `7447` is occupied by another service, the installer logs a safe warning without failing; modify `PORT` in that file and run `sudo systemctl restart anchordb`)*

### 2. Multi-Architecture Container Images (Docker & Podman)

Official multi-architecture images (`linux/amd64` and `linux/arm64`) are published to the GitHub Container Registry (`ghcr.io`):

```bash
docker run -d \
  --name anchordb \
  -p 7447:7447 \
  -v ./data:/data:Z \
  -e ANCHORDB_AUTH_TOKEN="your-secure-token" \
  -e ANCHORDB_DATA_DIR="/data" \
  --restart unless-stopped \
  ghcr.io/rachmanzz/anchordb:latest
```

Using Docker Compose:

```yaml
services:
  anchordb:
    image: ghcr.io/rachmanzz/anchordb:latest
    container_name: anchordb
    restart: unless-stopped
    ports:
      - "7447:7447"
    environment:
      - ANCHORDB_AUTH_TOKEN=your-secure-token
      - ANCHORDB_DATA_DIR=/data
    volumes:
      - ./data:/data:Z
```

### 3. Go Driver Client SDK

To connect your Go application to AnchorDB or embed the engine:

```bash
go get github.com/rachmanzz/anchordb/pkg/driver@v0.1.1
```

### 4. Standalone Binaries (Linux & macOS)

Download universal pre-compiled archives (`.tar.gz`) for Linux or macOS from [GitHub Releases](https://github.com/rachmanzz/anchordb/releases):

```bash
wget https://github.com/rachmanzz/anchordb/releases/latest/download/anchordb_0.1.0_linux_amd64.tar.gz
tar -xzf anchordb_0.1.0_linux_amd64.tar.gz
./anchordb-server -port 7447 -data-dir ./data
```

### 5. Building from Source

Prerequisites: Go 1.26 or newer (toolchain `go1.27.2+` recommended).

```bash
git clone https://github.com/rachmanzz/anchordb.git
cd anchordb

# Build server, CLI, and benchmark binaries
go build -o bin/anchordb-server ./cmd/server
go build -o bin/anchordb ./cmd/anchordb
go build -o bin/anchordb-benchmark ./cmd/benchmark
```

---

## Using the Go Driver SDK

AnchorDB includes an official client driver in `pkg/driver`. It operates in two modes:
1. **Remote Engine Mode (`driver.ConnectRemote`)**: Connects to a running AnchorDB engine server over HTTP/REST, leveraging background schedulers, worker pools, central file locking, token authentication, and multi-model routing.
2. **Embedded Engine Mode (`driver.OpenEmbedded`)**: Runs the full in-process AnchorDB engine runtime (WAL, file locking, multi-model routing, worker pool, and optional background sync) directly in RAM with zero network overhead.

> **Raw File Exposure Notice**: The `.anchordb` binary file is strictly an internal storage engine artifact. Never open or manipulate `.anchordb` files directly outside the engine.

### 1. Connecting to the Engine

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/rachmanzz/anchordb/pkg/driver"
)

func main() {
	// Mode A: Remote Engine (Client-Server over HTTP)
	client, err := driver.ConnectRemote("http://localhost:7447",
		driver.WithToken("your_auth_token"),
		driver.WithModel("plant_iot"),
	)
	if err != nil {
		log.Fatalf("connection failed: %v", err)
	}
	defer client.Close()

	// Mode B: Embedded Engine (In-Process Runtime, No TCP Port)
	// embeddedClient, err := driver.OpenEmbedded("./data",
	// 	driver.WithModel("plant_iot"),
	// 	driver.WithSyncInterval(5 * time.Second),
	// )


	ctx := context.Background()

	// Save record
	err = client.Save("sensor_zone_1", map[string]float32{
		"distance": 120.0,
		"duration": 2.0,
	})
	if err != nil {
		log.Fatal(err)
	}

	// Retrieve with in-storage computation
	record, err := client.Get("sensor_zone_1").
		Select("distance", "duration", "speed").
		Compute("speed", driver.Divide("distance", "duration")).
		Execute(ctx)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Speed: %.1f km/h\n", record.Get("speed"))
}
```

### 2. Filtering & Pagination

```go
// List records with property and calculated value filters
result, err := client.List().
	Where("distance", ">=", 50.0).
	Compute("speed", driver.Divide("distance", "duration")).
	Where("speed", ">", 40.0).
	Page(1, 10).
	Execute(ctx)

for _, item := range result.Items {
	fmt.Printf("%s: distance=%.0f, speed=%.1f\n", item.Name, item.Get("distance"), item.Get("speed"))
}
```

### 3. Date Arithmetic

Date values are stored as float32 Epoch Days (days since Unix epoch).

```go
// Store date
_ = client.Save("contract_1", map[string]float32{
	"start_date": driver.MustDate("2026-10-14"),
})

// Update: add 45 days
_, _ = client.Update().
	In("contract_1").
	Set("start_date", driver.AddDays("start_date", 45)).
	Execute(ctx)

// Result is 2026-11-28
updated, _ := client.Get("contract_1").Execute(ctx)
fmt.Println("New Date:", updated.GetDate("start_date"))
```

### 4. Causal Graph Traversal

```go
// Link directed edges with weight scores
_ = client.Link("fuel_shortage", "causes", "inflation", 0.9)
_ = client.Link("inflation", "causes", "economic_crisis", 0.7)

// 1-Hop query
step, _ := client.Causal("fuel_shortage", "causes", 5)
// Returns: [{"Word": "inflation", "Score": 0.9}]

// Multi-hop chain query
chain, _ := client.CausalChain([]string{"fuel_shortage"}, []string{"causes", "causes"}, 5)
// Returns: [{"Word": "economic_crisis", "Score": ...}]
```

---

## REST API Reference

All requests and responses use JSON. When an auth token is configured, endpoints under `/v1/*` require `Authorization: Bearer <token>`.

### Health & Metrics
| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `GET` | `/health` | Server health check (detailed stats require auth) |
| `GET` | `/metrics` | Prometheus-compatible metrics endpoint |

### CRUD & Computations
| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `POST` | `/v1/save` | Store an entity with key-value properties |
| `POST` | `/v1/get` | Retrieve an entity by name, optionally projecting or computing fields |
| `POST` | `/v1/list` | Paginated query with range, date, and compute filters |
| `POST` | `/v1/update` | Batch update records matching filter conditions |

### Graph & Causal Links
| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `POST` | `/v1/graph` | Spatial ray-projection and nearest neighbor search |
| `POST` | `/v1/edge` | Add or remove directed causal edges |
| `POST` | `/v1/causal` | Query 1-hop or multi-hop causal paths from seed nodes |

### Administration & Maintenance
| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `GET` | `/v1/sync` | Flush memory-mapped changes to disk |
| `POST` | `/v1/partition/run` | Execute partition archival (supports `async: true`) |
| `POST` | `/v1/collector/run` | Run garbage collection on tombstoned records |
| `GET` | `/v1/scheduler/status`| View scheduled background jobs |
| `GET` | `/v1/worker/status` | View background worker pool status |
| `POST` | `/v1/worker/cancel` | Cancel queued background tasks |

---

## Record Lifecycle

AnchorDB maintains four operational states for records to ensure safety during archival and crash recovery:

| State | Name | Description |
| :---: | :--- | :--- |
| `1` | **ACTIVE** | Normal record state, available for reads and mutations. |
| `0` | **SAFE_DELETE** | Temporary lock state while an archival job is executing. Mutations are blocked. |
| `2` | **TOMBSTONE** | Marked for removal after successful partition archival; purged by the collector. |
| `3` | **RETRY** | Assigned by crash reconciliation if the server stops during archival; re-queued next run. |

---

## Configuration Reference

Settings can be specified through command-line flags or environment variables:

| Setting | Flag | Environment Variable | Default | Description |
| :--- | :--- | :--- | :--- | :--- |
| **Port** | `-port` | `ANCHORDB_PORT` | `7447` | HTTP listening port |
| **Data Directory** | `-data-dir` | `ANCHORDB_DATA_DIR` | `./data` | Directory for `.anchordb` files |
| **Auth Token** | `-auth-token` | `ANCHORDB_AUTH_TOKEN` | `""` | Bearer token for API authentication |
| **Allow Insecure** | — | `ANCHORDB_ALLOW_INSECURE` | `false` | Disable auth requirement for local testing |
| **IP Whitelist** | `-ip-whitelist` | `ANCHORDB_IP_WHITELIST` | `""` | Comma-separated allowed IPs/CIDRs |
| **Trust Proxy** | `-trust-proxy` | `ANCHORDB_TRUST_PROXY` | `false` | Parse client IP from `X-Forwarded-For` |
| **Memory Limit** | `-memory-limit` | `ANCHORDB_MEMORY_LIMIT` | `""` | Soft memory budget (e.g. `1GB`) |
| **Cron Enabled** | `-cron-enabled` | `ANCHORDB_CRON_ENABLED` | `true` | Enable background scheduler |
| **Partition Schedule**| `-partition-schedule` | `ANCHORDB_PARTITION_SCHEDULE` | `23:00` | Archival schedule (`HH:MM`, `@daily`, `@every 1h`) |
| **Retention Days** | `-retention-days` | `ANCHORDB_PARTITION_RETENTION_DAYS` | `180` | Days of hot data before partitioning |
| **Sync Interval** | `-sync-interval` | `ANCHORDB_SYNC_INTERVAL` | `5m` | Background disk sync interval |
| **Worker Enabled** | `-worker-enabled` | `ANCHORDB_WORKER_ENABLED` | `true` | Enable background worker pool |
| **Worker Concurrency**| `-worker-concurrency` | `ANCHORDB_WORKER_CONCURRENCY` | `CPU * 2` | Max concurrent worker goroutines |
| **Worker Queue Size** | `-worker-queue-size` | `ANCHORDB_WORKER_QUEUE_SIZE` | `1000` | In-memory task queue size |
| **WAL Sync Mode** | `-wal-sync` | `ANCHORDB_WAL_SYNC` | `always` | `always` (fsync per write), `interval`, or `none` |
| **WAL Sync Interval** | `-wal-sync-interval`| `ANCHORDB_WAL_SYNC_INTERVAL` | `200ms` | Interval when WAL sync mode is `interval` |
| **Rate Limit** | `-rate-limit` | `ANCHORDB_RATE_LIMIT` | `0` (off) | Max requests per second per client IP |

---

## Design Boundaries & Limitations

AnchorDB is engineered for high throughput, sub-microsecond in-storage math, 2D spatial indexing, and directed causal reasoning. It is designed with clear architectural trade-offs:

1. **Not a Relational SQL Database**: AnchorDB enforces the **One Model = One File** model. There are no tables, schemas, migrations, or cross-model relational `JOIN` operations.
2. **Numeric Property Vectors**: Record properties are stored as `float32` values. AnchorDB is ideal for telemetry, IoT sensors, numerical attributes, and dates (Epoch Days), rather than arbitrary text documents or binary blobs.
3. **Engine-Encapsulated Storage**: Direct access to `.anchordb` files is strictly unsupported. All reads and writes must pass through the Engine (Remote HTTP Server or Embedded In-Process Engine) to guarantee WAL recovery, single-writer file locking (`flock`), lifecycle state tracking, and background synchronization.
4. **Advisory Single-Writer Locking**: Each model file supports concurrent reads and serialized writes managed by the engine. Two separate processes cannot write directly to the same model file concurrently.

---

## Development & Testing


Run the test suite and verification tools:

```bash
# Code formatting
gofmt -l .

# Static analysis
go vet ./...
golangci-lint run ./...

# Vulnerability checks
govulncheck ./...

# Unit and integration tests with race detector
go test -race ./...

# Standalone bare-metal benchmark CLI (cmd/benchmark)
go run ./cmd/benchmark

# Storage microbenchmarks with memory allocations (pkg/storage/bench_test.go)
go test -bench=. -benchmem -run=^# ./pkg/storage
```

See [Benchmark & Verification Guide](docs/benchmark_and_verification.md) for independent 5-phase container stress and latency reproduction.

---

## License

AnchorDB is licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.
