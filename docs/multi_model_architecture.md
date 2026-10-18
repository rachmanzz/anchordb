# Multi-Model Architecture: One Model = One Storage File

AnchorDB is designed around a strictly isolated, multi-model storage paradigm. Rather than organizing data into tables and relational schemas within a monolithic database instance, AnchorDB enforces the **One Model = One File** architecture.

---

## 1. Core Principle: One Model = One Binary File

In AnchorDB:
- Every data model (e.g. `sensors`, `vehicles`, `plants`, `telemetry`) is persisted as an independent, self-contained binary file: `{model}.anchordb`.
- There is no central catalog or shared lock across models.
- Each model file possesses its own:
  - **Memory-Mapped Region (`mmap`)**: Dedicated virtual address mapping isolated from other models.
  - **File Lock (`flock`)**: Concurrency control at the file descriptor level. Writes to Model A never block reads or writes to Model B.
  - **Write-Ahead Log (`WAL`)**: Crash recovery state managed independently per model (`{model}.anchordb.wal`).
  - **Spatial Coordinate Grid & Causal Graph**: Spatial coordinates and causal relation weights are localized per model domain.
  - **Lifecycle Policy & Archival Cutoffs**: Models can enforce distinct data retention windows (e.g., 30 days for telemetry, 365 days for financial metrics).

```
   [ Embedded Go Client ]             [ AnchorWire Go Clients ]
              │                                  │
              ▼                                  ▼
   ┌─────────────────────────────────────────────────────────────┐
   │                     AnchorDB Engine                         │
   │      (In-Process Embedded OR Standalone Container/Server)   │
   ├─────────────────────────────────────────────────────────────┤
   │  Model Router:                                              │
   │    • "sensors"   ──► data/sensors.anchordb   (+ WAL & Lock) │
   │    • "vehicles"  ──► data/vehicles.anchordb  (+ WAL & Lock) │
   │    • "plants"    ──► data/plants.anchordb    (+ WAL & Lock) │
   └─────────────────────────────────────────────────────────────┘
```

---

## 2. Why Not Tables? The Engineering Rationale

Traditional relational database engines (RDBMS) multiplex hundreds of tables into shared tablespaces or complex page-b-tree hierarchies. This introduces:
1. **Global Lock Contention**: DDL operations or high-throughput batch writes on one table can stall unrelated tables.
2. **Crash Recovery Coupling**: A WAL corrupt in one table delays startup and recovery for all tables.
3. **Backup & Archival Friction**: Archiving historical records requires complex query planning and lock-intensive row pruning.

In AnchorDB:
- **File-Level Failure Isolation**: If a process crashes or hardware corruption impacts `telemetry.anchordb`, `sensors.anchordb` remains completely intact and operational.
- **Model-Scoped Backup & Sharding**: After a successful sync, copy `{model}.anchordb` and, if the model has a locked schema, `{model}.anchordb.schema`. Coordinate the copy with writers so it captures one consistent state. Sharding across machines requires no schema rewriting.
- **Zero Schema Overhead**: Each record stores dynamic property vectors (`map[string]float32`) without table migration migrations or DDL locks.

---

## 3. Accessing Models

Raw `.anchordb` binary files are **strictly internal engine storage artifacts**. Direct filesystem access bypasses the WAL, file locking, in-memory grids, and lifecycle workers. All access MUST occur through the engine in one of two modes:

### Mode A: Remote Engine (Client-Server / Container)

When running the standalone engine server (e.g. Docker/Podman container with AnchorWire on port `7448`), models are selected dynamically:

```go
// Connect to remote engine
client, err := driver.ConnectWire("wire://localhost:7448",
    driver.WithToken("secure-token"),
    driver.WithModel("sensors"),
)
if err != nil {
    log.Fatal(err)
}
defer client.Close()

// Save record to "sensors" model
_ = client.Save("temp_sensor_1", map[string]float32{"celsius": 24.5})

// Switch to another model seamlessly
vehicles := client.Model("vehicles")
_ = vehicles.Save("truck_10", map[string]float32{"speed": 65.0, "fuel": 82.0})
```

### Mode B: Embedded Engine (In-Process Go Library)

When embedded directly within a Go binary, the engine manages model files inside a designated data directory without opening any TCP ports:

```go
// Open in-process engine runtime targeting local directory
client, err := driver.OpenEmbedded("./data",
    driver.WithModel("sensors"),
    driver.WithWorkerConcurrency(4),
    driver.WithSyncInterval(5 * time.Second),
)
if err != nil {
    log.Fatal(err)
}
defer client.Close()

// Writes automatically route to ./data/sensors.anchordb
_ = client.Save("temp_sensor_1", map[string]float32{"celsius": 24.5})

// Switching models opens ./data/telemetry.anchordb on demand
telemetry := client.Model("telemetry")
_ = telemetry.Save("pkt_99", map[string]float32{"bytes": 512})
```

---

## 4. Cold Data Archival & Partitioning

When data ages past its configured retention threshold, the partition engine creates a self-contained archive file in the `partitions/` subdirectory:

```
data/
├── sensors.anchordb
├── vehicles.anchordb
└── partitions/
    ├── 2026-10-16_sensors_part1.anchordb
    └── 2026-10-16_vehicles_part1.anchordb
```

Partition archives preserve the identical binary format. AnchorWire clients cannot list or inspect cold partitions; these server-management operations use the authenticated endpoints in the [REST reference](rest_api.md). Do not open archive files directly in an application process.

---

## 5. Design Boundaries & Limitations

1. **No Cross-Model Joins**: Because models are independent binary storage units, AnchorDB intentionally does not support relational JOIN queries across multiple models. Aggregations across models must be handled at the application layer.
2. **Single-Writer per Model File**: Each model file is protected by `flock`. While concurrent reads and internal concurrent worker pipelines are fully supported, two distinct OS processes cannot open the same model file in embedded write mode simultaneously.
3. **Property Typing**: Attributes within records are optimized for computational floating-point metrics (`float32`). AnchorDB is not intended for storing arbitrary large binary blobs or nested document trees.

---

## 6. Strict Schema Registration Contract

For applications requiring strict schema immutability and contract enforcement, AnchorDB defines the **Register Model** specification:
- **One-Time Registration**: Schema is declared once and stored in a durable `{model}.anchordb.schema` sidecar.
- **Attach Verification**: When clients attach to a model, the engine compares declared schemas; mismatches are rejected immediately, blocking all subsequent insert/update operations.
- **Lock Permanence**: The schema cannot be detached or altered dynamically until an explicit administrative reset.

For complete specifications, see the **[Model Schema Registration & Immutability Specification](model_schema_specification.md)**, the **[AnchorWire guide](anchorwire.md)**, and the **[Go Driver SDK Guide](go_driver.md#4-schema-attachment--contract-enforcement-clientattach)**.

