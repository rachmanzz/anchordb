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

AnchorWire supports record saves and batches, `Get`/`List`/`Update` builders, graph and causal operations, schema attach/get/reset, and `client.Sync()`. See the [Go driver guide](go_driver.md) for these methods and the [spatial graph example](../examples/spatial_graph/README.md) for a short runnable Wire example. The [graph traversal](../examples/graph_traversal/README.md), [Podman durability](../examples/podman_durability/README.md), and [supply chain](../examples/supply_chain/README.md) examples cover more involved flows.

## Operations that still use HTTP

The Go driver's AnchorWire backend does not implement partition listing or inspection. Use the authenticated HTTP partition endpoints in the [REST reference](rest_api.md) for those operations. Server health, metrics, scheduler/worker administration, and starting partition or collector jobs are also HTTP endpoints. Keep application record, query, schema, and graph traffic on AnchorWire; use HTTP for these server operations when needed.

## Security & Network Isolation

AnchorWire communicates over raw TCP without built-in TLS to maximize data streaming throughput and avoid encryption overhead during local and cluster computation. The authentication token is transmitted as a binary payload during connection establishment (`OpAuth`).

> [!WARNING]
> **Do not expose unencrypted AnchorWire port 7448 directly to public or untrusted networks.**
> Because tokens and records transit unencrypted, any eavesdropper on an untrusted path could capture authentication credentials and database mutations.

### Recommended Deployment Practices:
1. **Private Network / VPC**: AnchorWire should always run inside a private subnet, localhost, container network, or encrypted overlay network (e.g. WireGuard, Tailscale, IPSec).
2. **IP Whitelisting**: Set `ANCHORDB_IP_WHITELIST` on the server so unauthorized client IP addresses are dropped immediately at the TCP layer.
3. **TLS Termination Proxy**: When client traffic must cross untrusted public networks, place a TLS-terminating reverse proxy (such as Envoy, HAProxy, NGINX stream, or stunnel) in front of port `7448`.
