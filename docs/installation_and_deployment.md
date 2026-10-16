# Installation & Deployment Guide

AnchorDB can be deployed through multiple official distribution channels depending on your infrastructure requirements:

1. **[Native Linux Packages (`.deb` / `.rpm`)](#1-native-linux-packages-deb--rpm)**: Recommended for Linux servers, virtual machines, and bare metal with automated background `systemd` service management.
2. **[Multi-Architecture Container Images (`ghcr.io`)](#2-container-deployment-docker--podman)**: Recommended for Kubernetes, Docker, and containerized cloud platforms (`amd64` and `arm64`).
3. **[Universal Standalone Tarballs (`.tar.gz`)](#3-pre-compiled-standalone-binaries)**: Zero-dependency portable executables for Linux and macOS (Intel & Apple Silicon).
4. **[Go Client SDK Driver](#4-go-client-sdk-installation)**: Library for Go applications connecting remotely or embedding the engine in-process.
5. **[Building from Source](#5-compiling-from-source)**: Manual compilation using the Go toolchain.

---

## 1. Native Linux Packages (`.deb` / `.rpm`)

Official Debian/Ubuntu (`.deb`) and Red Hat/Fedora (`.rpm`) packages are published with every release on [GitHub Releases](https://github.com/rachmanzz/anchordb/releases).

### Debian / Ubuntu Installation

Download and install the `.deb` package matching your server architecture:

```bash
# For AMD64 (x86_64)
wget https://github.com/rachmanzz/anchordb/releases/latest/download/anchordb_0.1.0_amd64.deb
sudo dpkg -i anchordb_0.1.0_amd64.deb

# For ARM64 (aarch64)
wget https://github.com/rachmanzz/anchordb/releases/latest/download/anchordb_0.1.0_arm64.deb
sudo dpkg -i anchordb_0.1.0_arm64.deb
```

### RHEL / CentOS / Rocky Linux / Fedora Installation

Download and install the `.rpm` package:

```bash
# For AMD64 (x86_64)
sudo rpm -ivh https://github.com/rachmanzz/anchordb/releases/latest/download/anchordb_0.1.0_amd64.rpm

# For ARM64 (aarch64)
sudo rpm -ivh https://github.com/rachmanzz/anchordb/releases/latest/download/anchordb_0.1.0_arm64.rpm
```

### Automated Systemd Lifecycle

When the package is installed, the package manager automatically:
1. Creates a dedicated, unprivileged system service user and group: `anchordb:anchordb` with `/usr/sbin/nologin`.
2. Creates the secure persistent data directory: `/var/lib/anchordb` (permission `0750`, owned by `anchordb`).
3. Registers and enables `anchordb.service` to start automatically on system boot.
4. Starts the service in the background immediately.

#### Managing the Service

```bash
# Check service status
sudo systemctl status anchordb

# Stop, start, or restart
sudo systemctl stop anchordb
sudo systemctl start anchordb
sudo systemctl restart anchordb

# View live system logs
sudo journalctl -u anchordb -f
```

### Configuration & Safe Port Collision Handling

The daemon service reads environment configuration from:
- **Debian / Ubuntu**: `/etc/default/anchordb`
- **RHEL / CentOS / Fedora**: `/etc/sysconfig/anchordb`

Default configuration file (`/etc/default/anchordb`):

```bash
# Port for HTTP server to listen on (default: 7447)
PORT=7447

# Base data directory where database models and WAL files are stored
DATA_DIR=/var/lib/anchordb

# Optional secret bearer token for API authentication (leave empty to disable)
AUTH_TOKEN=

# Write-Ahead Log durability mode: always | interval | none (default: always)
WAL_SYNC=always

# Fsync interval when WAL_SYNC=interval (e.g. 50ms, 200ms)
WAL_SYNC_INTERVAL=200ms

# Rate limit in requests/sec per client IP (0 disables rate limiting)
RATE_LIMIT=0
```

> [!NOTE]
> **Port Conflict Handling**: If port `7447` is already bound to another application when installing the package, the post-install script detects the bind failure and logs an informative warning rather than aborting package installation. To resolve:
> 1. Edit `/etc/default/anchordb` (or `/etc/sysconfig/anchordb`) and change `PORT=7448` (or any available port).
> 2. Restart the service: `sudo systemctl restart anchordb`.

### Sandboxing & Security Hardening

The systemd unit (`/lib/systemd/system/anchordb.service`) includes OS-level sandboxing protections:
- `NoNewPrivileges=true`: Prevents privilege escalation.
- `ProtectSystem=full`: Mounts `/usr`, `/boot`, and `/etc` as read-only.
- `ProtectHome=true`: Restricts access to `/home`, `/root`, and `/run/user`.
- `ReadWritePaths=/var/lib/anchordb`: Strictly whitelists the database data directory for write operations.
- `LimitNOFILE=65536`: High open-file descriptor limit for high-concurrency connections and memory-mapped models.

---

## 2. Container Deployment (Docker & Podman)

Official multi-architecture container images (`linux/amd64` and `linux/arm64`) are published to the **GitHub Container Registry (`ghcr.io`)**.

### Pulling the Image

```bash
# Pull latest stable release
docker pull ghcr.io/rachmanzz/anchordb:latest

# Pull specific version
docker pull ghcr.io/rachmanzz/anchordb:v0.1.0
```

### Running with Docker CLI

```bash
docker run -d \
  --name anchordb \
  -p 7447:7447 \
  -v ./anchordb_data:/data:Z \
  -e ANCHORDB_AUTH_TOKEN="your-strong-secret-token" \
  -e ANCHORDB_DATA_DIR="/data" \
  --restart unless-stopped \
  ghcr.io/rachmanzz/anchordb:latest
```

*(Note: For Podman, replace `docker` with `podman`.)*

### Running with Docker Compose

Save the following as `docker-compose.yml`:

```yaml
services:
  anchordb:
    image: ghcr.io/rachmanzz/anchordb:latest
    container_name: anchordb
    restart: unless-stopped
    ports:
      - "7447:7447"
    environment:
      - ANCHORDB_PORT=7447
      - ANCHORDB_DATA_DIR=/data
      - ANCHORDB_AUTH_TOKEN=your-strong-secret-token
      - ANCHORDB_ALLOW_INSECURE=false
      - ANCHORDB_CRON_ENABLED=true
      - ANCHORDB_PARTITION_SCHEDULE=23:00
      - ANCHORDB_PARTITION_RETENTION_DAYS=180
      - ANCHORDB_SYNC_INTERVAL=5m
      - ANCHORDB_WORKER_ENABLED=true
      - ANCHORDB_WORKER_QUEUE_SIZE=1000
    volumes:
      - ./data:/data:Z
    healthcheck:
      test: ["CMD", "wget", "-qO-", "http://localhost:7447/health"]
      interval: 30s
      timeout: 3s
      retries: 3
```

Start the service:
```bash
docker compose up -d
```

### Container Environment Variables

| Variable | Default | Description |
| :--- | :--- | :--- |
| `ANCHORDB_PORT` | `7447` | TCP port the HTTP REST daemon listens on |
| `ANCHORDB_DATA_DIR` | `/data` | Path inside container where `.anchordb` files and `.wal` logs are stored |
| `ANCHORDB_AUTH_TOKEN` | `""` | Bearer token required for all authenticated endpoints |
| `ANCHORDB_ALLOW_INSECURE` | `false` | Set to `true` only for development without an auth token |
| `ANCHORDB_MEMORY_LIMIT` | `""` | Optional RAM budget (e.g. `500MB`, `1GB`, `5GB`) |
| `ANCHORDB_CRON_ENABLED` | `true` | Enables wall-clock background cron scheduling |
| `ANCHORDB_PARTITION_SCHEDULE` | `"23:00"` | Daily wall-clock time for automated cold partition rollover |
| `ANCHORDB_PARTITION_RETENTION_DAYS` | `180` | Retention days before archiving cold records |
| `ANCHORDB_SYNC_INTERVAL` | `"5m"` | Automated periodic background disk sync interval |
| `ANCHORDB_WORKER_ENABLED` | `true` | Enables asynchronous task worker pool |
| `ANCHORDB_WORKER_CONCURRENCY` | `runtime.NumCPU()` | Number of parallel worker goroutines |
| `ANCHORDB_WORKER_QUEUE_SIZE` | `1000` | Bounded memory task queue capacity before backpressure |

---

## 3. Pre-Compiled Standalone Binaries

For environments where installing system packages or running containers is not preferred, universal `.tar.gz` archives are provided on [GitHub Releases](https://github.com/rachmanzz/anchordb/releases):

| Archive File | Operating System | Architecture |
| :--- | :--- | :--- |
| `anchordb_0.1.0_linux_amd64.tar.gz` | Linux | x86_64 / AMD64 |
| `anchordb_0.1.0_linux_arm64.tar.gz` | Linux | aarch64 / ARM64 |
| `anchordb_0.1.0_darwin_amd64.tar.gz` | macOS | Intel |
| `anchordb_0.1.0_darwin_arm64.tar.gz` | macOS | Apple Silicon (M1/M2/M3/M4) |

### Installation Steps

```bash
# 1. Download archive and checksums
wget https://github.com/rachmanzz/anchordb/releases/latest/download/anchordb_0.1.0_linux_amd64.tar.gz
wget https://github.com/rachmanzz/anchordb/releases/latest/download/SHA256SUMS.txt

# 2. Verify integrity
sha256sum --check --ignore-missing SHA256SUMS.txt

# 3. Extract binaries
tar -xzf anchordb_0.1.0_linux_amd64.tar.gz

# 4. Move binaries to system PATH (optional)
sudo install -m 0755 anchordb-server /usr/local/bin/
sudo install -m 0755 anchordb /usr/local/bin/
sudo install -m 0755 anchordb-benchmark /usr/local/bin/
```

Binaries included in the archive:
- `anchordb-server`: Full database server daemon with HTTP REST API, scheduler, and worker pool.
- `anchordb`: Interactive and scripting CLI for inspecting models and queries.
- `anchordb-benchmark`: Standalone bare-metal benchmark CLI.

---

## 4. Go Client SDK Installation

To use AnchorDB from within another Go application, import the official client driver SDK:

```bash
go get github.com/rachmanzz/anchordb/pkg/driver@v0.1.0
```
*(or `go get github.com/rachmanzz/anchordb/pkg/driver` for the latest revision)*

### Basic Usage Example

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/rachmanzz/anchordb/pkg/driver"
)

func main() {
	// Connect to background engine server
	client, err := driver.ConnectRemote("http://localhost:7447",
		driver.WithToken("your-token"),
		driver.WithModel("plant_iot"),
	)
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	// Ingest entity
	_ = client.Save("sensor_zone_1", map[string]float32{
		"voltage": 12.0,
		"current": 2.5,
	})

	// Query with on-the-fly math calculation
	result, err := client.Get("sensor_zone_1").
		Compute("power", driver.Multiply("voltage", "current")).
		Execute(ctx)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Power: %.2f W\n", result.Get("power"))
}
```

For full driver details, refer to the [Go Driver SDK Guide](go_driver.md).

---

## 5. Compiling from Source

### Prerequisites
- **Go**: 1.26 or newer (toolchain `go1.27.2+` recommended for security fixes)
- **OS**: Linux or macOS (POSIX compliant with `flock` and `mmap`)
- **Git**

### Compilation Steps

```bash
# Clone the repository
git clone https://github.com/rachmanzz/anchordb.git
cd anchordb

# Build all three primary binaries
CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o bin/anchordb-server ./cmd/server
CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o bin/anchordb ./cmd/anchordb
CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o bin/anchordb-benchmark ./cmd/benchmark
```

### Running Tests and Verification

```bash
# Run complete test suite
go test -v -race ./...

# Run bare-metal benchmark suite
go run ./cmd/benchmark
```
