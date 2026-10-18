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
  - One-time permanent schema locking with scalar numeric and temporal primitives (`int`, `float`, `bool`, `date`).
  - Required vs. optional (`?`) syntax, fail-fast rejection (`ErrSchemaMismatch`), and administrative reset.
- **API & Client SDK**: AnchorWire binary TCP (default port `7448`) for remote Go driver clients. HTTP on port `7447` serves health, metrics, and server operations.

---

## Feature Documentation

- **[Multi-Model Architecture: One Model = One File](docs/multi_model_architecture.md)**: Independent storage files, failure isolation, and memory-mapped model routing.
- **[Model Schema Registration & Immutability Specification](docs/model_schema_specification.md)**: Strict contract enforcement, scalar primitives, and fail-fast validation.
- **[In-Storage Computations & Date Math](docs/in_storage_compute.md)**: Operators, date arithmetic, attribute projections, and batch updates.
- **[Causal & Spatial Graph Engine](docs/causal_and_spatial_graph.md)**: 2D spatial indexing, ray casting, and weighted causal chains.
- **[Storage Engine, WAL & Durability](docs/storage_and_durability.md)**: Binary format, write-ahead log modes, and crash recovery.
- **[Data Partitioning & Record Lifecycle](docs/partitioning_and_lifecycle.md)**: 4-state lifecycle machine, archival sinks, and tombstone collection.
- **[Background Subsystems: Scheduler & Worker Pool](docs/background_subsystems.md)**: Wall-clock cron scheduler and bounded async worker pool.
- **[AnchorWire Guide](docs/anchorwire.md)**: Start the server and connect, attach a schema, save, and query over binary TCP.
- **[Go Driver SDK Guide](docs/go_driver.md)**: Connection setup, query builders, and integration examples.
- **[REST API Reference](docs/rest_api.md)**: HTTP server administration and transport-specific endpoints.
- **[Benchmark & Verification Guide](docs/benchmark_and_verification.md)**: Reproducible test suite, Little's Law validation, and container durability.
- **[Installation & Deployment Guide](docs/installation_and_deployment.md)**: Native packages (.deb, .rpm), systemd service management, Docker/Podman container images, and universal archives.



---

## Architecture

```
                       +---------------------------------------------+
                       |            AnchorDB Server Daemon           |
                       |          (AnchorWire TCP Port 7448)        |
                       +----------------------+----------------------+
                                              |
                   +--------------------------+--------------------------+
                   |                          |                          |
                   v                          v                          v
    +------------------------------+   +--------------------+   +---------------------+
    |     AnchorWire Engine        |   |   Cron Scheduler   |   | Async Worker Pool   |
    |  Save, Get, List, Update     |   |     (pkg/scheduler)|   |    (pkg/worker)     |
    |  Graph, Causal, Schema       |   | Periodic Sync/Jobs |   | Background Tasks    |
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

HTTP on port `7447` remains available for health, metrics, and administration, including partition inspection, which is not implemented by the AnchorWire driver backend.

---

## Installation & Quick Start

AnchorDB offers multiple official installation and deployment methods:

For complete operational and deployment options, see the **[Installation & Deployment Guide](docs/installation_and_deployment.md)**.

### 1. Native Linux Packages (`.deb` / `.rpm`) with Automated Background Systemd

Pre-built native packages are published on [GitHub Releases](https://github.com/rachmanzz/anchordb/releases). Installing a package provisions the `anchordb:anchordb` service user, configures `/var/lib/anchordb` with `0750` permissions, and enables a background `systemd` service. Set a strong `AUTH_TOKEN` in `/etc/default/anchordb` or `/etc/sysconfig/anchordb`, then restart the service; the server refuses to start with an empty token.

```bash
# Ubuntu / Debian (.deb)
wget https://github.com/rachmanzz/anchordb/releases/latest/download/anchordb_0.1.2_amd64.deb
sudo dpkg -i anchordb_0.1.2_amd64.deb

# RHEL / CentOS / Fedora (.rpm)
sudo rpm -ivh https://github.com/rachmanzz/anchordb/releases/latest/download/anchordb_0.1.2_amd64.rpm
```

Verify service status:
```bash
sudo systemctl status anchordb
```
*(If port `7447` is occupied, check `sudo journalctl -u anchordb`, change `PORT` in the same configuration file, and run `sudo systemctl restart anchordb`.)*

### 2. Multi-Architecture Container Images (Docker & Podman)

Official multi-architecture images (`linux/amd64` and `linux/arm64`) are published to the GitHub Container Registry (`ghcr.io`):

```bash
docker run -d \
  --name anchordb \
  -p 7447:7447 \
  -p 7448:7448 \
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
      - "7448:7448"
    environment:
      - ANCHORDB_AUTH_TOKEN=your-secure-token
      - ANCHORDB_DATA_DIR=/data
    volumes:
      - ./data:/data:Z
```

### 3. Go Driver Client SDK

To connect your Go application to AnchorDB or embed the engine:

```bash
go get github.com/rachmanzz/anchordb/pkg/driver@v0.1.2
```

### 4. Standalone Binaries (Linux & macOS)

Download universal pre-compiled archives (`.tar.gz`) for Linux or macOS from [GitHub Releases](https://github.com/rachmanzz/anchordb/releases):

```bash
wget https://github.com/rachmanzz/anchordb/releases/latest/download/anchordb_0.1.2_linux_amd64.tar.gz
tar -xzf anchordb_0.1.2_linux_amd64.tar.gz
export ANCHORDB_AUTH_TOKEN="your-secure-token"
./anchordb-server -wire-port 7448 -data-dir ./data
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
1. **AnchorWire Remote Mode (`driver.ConnectWire`)**: Connects to a running AnchorDB engine server over binary TCP, using its background schedulers, worker pools, file locking, token authentication, and multi-model routing.
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
	// 1. Declare Model Schema Contract
	// AnchorDB enforces strict data contracts per model to prevent drift and corruption.
	schema := driver.Schema{
		"distance":  "float", // Required float32 metric
		"duration":  "float", // Required float32 metric
		"sensor_id": "int?",  // Optional integer attribute (trailing ?)
		"is_active": "bool?", // Optional boolean flag
	}

	// Mode A: AnchorWire (binary TCP with schema attachment & verification)
	client, err := driver.ConnectWire("wire://localhost:7448",
		driver.WithToken("your_auth_token"),
		driver.WithModel("plant_iot"),
		driver.WithAttachSchema(schema), // Locks and validates schema contract
	)
	if err != nil {
		log.Fatalf("connection failed: %v", err)
	}
	defer client.Close()

	// Mode B: Embedded Engine (In-Process Runtime, No TCP Port)
	// embeddedClient, err := driver.OpenEmbedded("./data",
	// 	driver.WithModel("plant_iot"),
	// 	driver.WithAttachSchema(schema),
	// 	driver.WithSyncInterval(5 * time.Second),
	// )

	ctx := context.Background()

	// Save record (strictly validated against the locked model schema)
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

### 2. Model Schema Contract & Attachment

AnchorDB enforces strict schema immutability per model database to guarantee type safety and prevent silent schema drift across distributed microservices.

```go
// Define schema contract: required types and optional (?) fields
sensorSchema := driver.Schema{
	"voltage":    "float", // Required float32
	"current":    "float", // Required float32
	"status":     "int",   // Required integer
	"is_active":  "bool?", // Optional boolean
	"calibrated": "date?", // Optional calendar date (epoch day)
}

// 1. Attach to active model (fails fast if an incompatible schema was already locked)
if err := client.Attach(sensorSchema); err != nil {
	log.Fatalf("schema verification failed: %v", err)
}

// 2. Or attach when spawning a new model client:
deviceClient, err := client.AttachModel("devices_zone_1", sensorSchema)

// 3. Inspect registered schema contract
registeredSchema, err := client.GetSchema(ctx)
```

#### Supported Types & Validation Rules
- **Primitives**: `int`, `float`, `bool`, `date` (`YYYY-MM-DD` epoch day).
- **Required (Default)**: Omission of `?` makes the field strictly mandatory. Missing fields return `driver.ErrMissingRequiredField`.
- **Optional (`?` Modifier)**: Suffix `?` allows the field to be omitted on write without reserving empty space.
- **Fail-Fast Safety**: Supplying undeclared attributes returns `driver.ErrUnknownField`. Supplying incompatible values on `Save` or `Update` returns `driver.ErrInvalidFieldType`. Mismatched connection contracts return `driver.ErrSchemaMismatch`.

For comprehensive specifications, see the **[Model Schema Registration & Immutability Specification](docs/model_schema_specification.md)**.

### 3. Filtering & Pagination

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

### 4. Date Arithmetic

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

### 5. Causal Graph Traversal

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

## AnchorWire Operations

Use `driver.ConnectWire("wire://localhost:7448", ...)` for record saves and batches, `Get`/`List`/`Update` queries, spatial and causal graphs, schema management, and `Sync`. The [AnchorWire guide](docs/anchorwire.md) includes a complete runnable client and links to larger examples.

HTTP remains available for health, metrics, and server administration. The AnchorWire driver does not implement partition listing or inspection; those endpoints are documented in the [REST reference](docs/rest_api.md).

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
| **AnchorWire Port** | `-wire-port` | `ANCHORDB_WIRE_PORT` | `7448` | Binary TCP port for Go driver clients |
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

1. **Not a Relational SQL Database**: AnchorDB enforces the **One Model = One File** model. Each model maintains its own immutable schema contract, and there are no relational SQL tables, cross-model relational `JOIN` operations, or dynamic migrations.
2. **Computational Property Storage**: Attribute values are stored as contiguous `float32` scalar primitives (`float`, `int`, `bool`, `date`). Complex nested document trees (arbitrary nested JSON hierarchies) are intentionally not supported to preserve zero-copy memory-mapped speed and predictable memory layouts.
3. **Engine-Encapsulated Storage**: Direct access to `.anchordb` files is strictly unsupported. All reads and writes must pass through the Engine (AnchorWire server or embedded in-process engine) to guarantee WAL recovery, single-writer file locking (`flock`), lifecycle state tracking, and background synchronization.
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
