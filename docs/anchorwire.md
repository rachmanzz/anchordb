# AnchorWire: Go Driver over Binary TCP

AnchorWire is the primary remote transport for the Go driver. The server listens on TCP port `7448` by default, separate from its HTTP port (`7447`). The driver accepts `wire://host:port` through `driver.ConnectWire` or `driver.Connect`. The same model schema and storage rules apply over both transports.

## Start the server

Set a token and start the server from the repository root:

```bash
export ANCHORDB_AUTH_TOKEN="your-strong-secret-token"
go run ./cmd/server -data-dir ./data -wire-port 7448
```

For a container, publish both HTTP and AnchorWire ports and pass the same token:

```bash
docker run -d --name anchordb \
  -p 7447:7447 \
  -p 7448:7448 \
  -v ./data:/data:Z \
  -e ANCHORDB_AUTH_TOKEN="$ANCHORDB_AUTH_TOKEN" \
  -e ANCHORDB_DATA_DIR=/data \
  ghcr.io/rachmanzz/anchordb:latest
```

The server listens for REST administration on port `7447` and AnchorWire TCP clients on port `7448`. The Wire listener can be changed with `-wire-port` or `ANCHORDB_WIRE_PORT`.

## Connect, attach a schema, save, and query

Install the Go driver with `go get github.com/rachmanzz/anchordb/pkg/driver`, then run this program with `ANCHORDB_AUTH_TOKEN` set to the server token:

```go
package main

import (
    "context"
    "fmt"
    "log"
    "os"

    "github.com/rachmanzz/anchordb/pkg/driver"
)

func main() {
    token := os.Getenv("ANCHORDB_AUTH_TOKEN")
    if token == "" {
        log.Fatal("ANCHORDB_AUTH_TOKEN is required")
    }

    client, err := driver.ConnectWire("wire://localhost:7448",
        driver.WithToken(token),
        driver.WithModel("trips"),
        driver.WithAttachSchema(driver.Schema{
            "distance": "float",
            "duration": "float",
        }),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    if err := client.Save("trip_1", map[string]float32{
        "distance": 120,
        "duration": 2,
    }); err != nil {
        log.Fatal(err)
    }

    result, err := client.Get("trip_1").
        Compute("speed", driver.Divide("distance", "duration")).
        Execute(context.Background())
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println(result.Get("speed")) // 60
}
```

`driver.Connect("wire://localhost:7448", ...)` also selects AnchorWire. Use `driver.WithModel` or `client.Model("other_model")` to route operations to another model. `driver.WithAttachSchema` checks the model contract while connecting; incompatible schemas fail before the client is returned.

AnchorWire supports record saves and batches, `Get`/`List`/`Update` builders, graph and causal operations, schema attach/get/reset, partition catalog/inspection, and `client.Sync()`. See the [Go driver guide](go_driver.md) for these methods and the [spatial graph example](../examples/spatial_graph/README.md) for a short runnable Wire example. The [graph traversal](../examples/graph_traversal/README.md), [Podman durability](../examples/podman_durability/README.md), and [supply chain](../examples/supply_chain/README.md) examples cover more involved flows.

## Cold Storage & Partition Operations on AnchorWire

AnchorWire extends beyond active memory-mapped operations to support cold partition discovery and inspection:

* **`OpPartitionCatalog` (`0x20`)**: Retrieves the metadata manifest of archived partitions for a given model or all models, including file size, creation timestamp, and date range bounds.
* **`OpPartitionInspect` (`0x21`)**: Safely inspects a specific archived record or lists sample records directly from an archived `.anchordb` partition file.
* **Transparent Cold-Tier Query**: When executing `OpGet`, if the record has been pruned from the hot database, AnchorWire automatically searches cold tier storage. The response header carries `FlagColdTier = 0x08`, allowing the client to know whether data came from hot memory or cold archive.

## Operations that use HTTP

Server health (`/health`), Prometheus metrics (`/metrics`), scheduler/worker task cancellation (`/v1/worker/cancel`), and manual invocation of partition/collector background jobs (`/v1/partition/run`, `/v1/collector/run`) use HTTP endpoints. Keep application record mutations, queries, schemas, graphs, and partition catalog/inspection on AnchorWire.

## Security, Gateway Isolation & Staged Token Rotation

### 1. Gateway Isolation Mode (`-gateway-mode`)
When deployed behind an authenticated gateway (e.g. `anchor-connector`), pass `-gateway-mode` or set `ANCHORDB_GATEWAY_MODE=true`. This disables all direct HTTP `/v1/*` data routes (returning `403 Forbidden` with `gateway_mode_enforced`), strictly confining all data traffic through AnchorWire and the connector.

### 2. Zero-Knowledge Staged Preimage Token Rotation
For dynamic cluster topologies, AnchorWire supports one-time pairing and staged preimage rotation (`AuthTypeStagedReauth: 0x05`):
* **Zero-Knowledge Storage**: The server never stores cleartext secrets on disk. It maintains cryptographic SHA-256 commitments (`E = SHA256(V)`) inside a flat binary file (`/data/auth/commitments.bin`) protected by IEEE CRC32 checksums.
* **Commit-First Rotation**: When rotating, the new commitment `E2` is committed to durable disk as `STAGED_PENDING_CONFIRM` before the old `E1` is invalidated.
* **Piggyback Pruning**: The old commitment is safely pruned only after the client's first valid operational frame is processed, guaranteeing zero deadlock during network partition.

### 3. Network Isolation
AnchorWire communicates over raw binary TCP to maximize throughput. When deploying across public or untrusted networks:
1. **Private Subnet / VPC**: Run AnchorWire within a VPC, WireGuard/Tailscale overlay, or container network.
2. **IP Whitelisting**: Set `ANCHORDB_IP_WHITELIST` to drop unauthorized TCP clients at connection handshake.
3. **TLS Reverse Proxy**: When transiting untrusted networks, use an external TLS termination proxy (Envoy, NGINX, HAProxy).
