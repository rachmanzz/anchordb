# Storage Engine, WAL & Durability

AnchorDB utilizes memory-mapped binary files combined with a Write-Ahead Log (WAL) to provide fast record access and crash resilience.

---

## 1. Binary Storage Format (`.anchordb`)

Database files have a structured binary format designed for memory-mapped (`mmap`) access without deserialization overhead.

### File Header Layout (64 Bytes)
The file begins with a 64-byte fixed-size header:

| Offset (Bytes) | Size (Bytes) | Field | Description |
| :---: | :---: | :--- | :--- |
| `0..3` | 4 | Magic | Format identifier (`"ANCH"`) |
| `4..7` | 4 | Version | Database format version (`3`) |
| `8..15` | 8 | Node Count | Total number of spatial nodes |
| `16..17` | 2 | Grid Cells X | Spatial grid width |
| `18..19` | 2 | Grid Cells Y | Spatial grid height |
| `20..27` | 8 | Grid Offset | Byte offset of the spatial grid index |
| `28..35` | 8 | Nodes Offset | Byte offset of fixed-size node records |
| `36..43` | 8 | String Offset | Byte offset of the node string pool |
| `44..51` | 8 | Entity Count | Total entity records |
| `52..59` | 8 | Entity Offset | Byte offset of entity attributes |
| `60..63` | 4 | Reserved | Padding |

Version 3 stores the optional causal-edge section after the entity section; its offset is not a header field.

### Record Layout
* **Spatial Nodes (40 Bytes Fixed)**:
  * Node ID: 8 bytes; domain ID: 2 bytes
  * Word offset: 4 bytes; word length: 2 bytes
  * 2D Coordinates (X, Y): two `float32` values (8 bytes)
  * Semantic Role Vector: four `float32` values (16 bytes)
* **Entity Records (`CompactRecord`)**:
  * Stored in contiguous byte slices with interned string keys to avoid duplicated key strings across millions of records.
  * Attribute Pool compaction automatically purges unreferenced keys once the pool exceeds 131,072 entries (`db.CompactAttrPool()`).

---

## 2. Write-Ahead Logging (WAL)

Record and graph mutations are written to an append-only `<db>.wal` file alongside the `.anchordb` file before being applied to in-memory state.

### Frame Structure
Each WAL entry is protected by a CRC32 checksum:

```
+------------+--------------+-------------+------------------+------------+---------------+
| Magic (1B) | Version (1B) | Opcode (1B) | Payload Len (4B) | CRC32 (4B) | Payload Bytes |
+------------+--------------+-------------+------------------+------------+---------------+
```

Record identifiers, when needed, are encoded inside the opcode-specific payload.

### Durability Modes (`SyncMode`)

AnchorDB provides three explicit WAL durability modes to let operators choose their exact position on the physical durability vs write throughput frontier:

| Mode | fsync Semantics | Throughput Profile | Crash Resilience | Recommended For |
| :--- | :--- | :--- | :--- | :--- |
| **`always`** (default) | Synchronous `fsync` on every WAL write | Disk IOPS-bounded | A successful write has been synced to the WAL | Financial transactions, critical audit trails, compliance logs |
| **`interval`** | Background goroutine flushes `fsync` periodically (default `200ms`) | High throughput (buffered `fsync`) | Writes since the last successful sync can be lost on power failure; the interval is a schedule, not a hard loss bound | High-throughput telemetry, metric streams, edge devices |
| **`none`** | Never invokes `fsync`; relies on OS page cache flushing | Memory-speed (OS page cache) | Clean exits preserved; power failure loses dirty OS pages | Ephemeral testing, bulk ETL data imports, cache backends |

### Throughput & Latency Analysis

* **The Physical Limit of `SyncAlways`**: Calling `fsync()` forces the storage controller to flush volatile write buffers to non-volatile flash or magnetic platters. Sequential single-threaded writes in `always` mode are strictly bounded by hardware rotational or flash latency (~0.5–2ms on NVMe SSDs, ~10–30ms on HDDs or SD cards).
* **Batch Amortization**: When running in `always` mode, multiple records modified in a batch update pass share a single commit window, amortizing the physical `fsync` overhead across thousands of records.
* **Maximizing Throughput with `SyncInterval`**: For telemetry, sensor logging, and tracking where delayed WAL synchronization is acceptable, setting `-wal-sync=interval -wal-sync-interval=50ms` requests a 50 ms sync cadence. Scheduler delays or sync failures can extend the unsynced window.
* **Empirical Verification**: Run the in-repo benchmark suite in [`pkg/storage/bench_test.go`](../pkg/storage/bench_test.go) (`go test -bench=BenchmarkWAL -benchmem ./pkg/storage`) to measure the exact throughput profile on your local storage controller.

### Configuration Reference

Durability modes can be configured through three mechanisms:

1. **CLI Flags (Server)**:
   ```bash
   ./anchordb-server -wal-sync=interval -wal-sync-interval=100ms
   ```
2. **Environment Variables (Container & Daemon)**:
   ```bash
   ANCHORDB_WAL_SYNC="interval"
   ANCHORDB_WAL_SYNC_INTERVAL="100ms"
   ```
3. **Go SDK / Embedded Engine**:
   ```go
   db, err := storage.OpenWithOptions(path, storage.Options{
       WAL: storage.WALOptions{
           SyncMode:     storage.SyncInterval,
           SyncInterval: 50 * time.Millisecond,
       },
   })
   ```

---

## 3. Concurrency & Advisory Locking

* **Single-Writer Lock**: When opening a database, AnchorDB acquires an exclusive flock on `<db>.lock`. If another process attempts to open the same database file, it fails immediately with an error rather than risking file corruption.
* **Thread-Safe Reads & Writes**: In-memory structures are protected by a fine-grained `sync.RWMutex`, allowing multiple parallel readers without contention during batch updates.

---

## 4. Atomic Snapshot Sync (`Sync`)

AnchorDB periodically flushes active memory-mapped structures to disk using an atomic replacement strategy:
1. The database state is serialized into a temporary file (`.anchordb_sync_*.tmp`) in the database directory.
2. The temporary file is `fsync`ed to ensure physical durability.
3. The temporary file is atomically renamed over the original `.anchordb` file (`os.Rename`).
4. The live memory mapping is safely updated to the new file descriptor.
5. The WAL is checkpointed up to the snapshot offset; writes made during serialization are retained for replay.

Before the atomic rename, a crash leaves the original database file in place. After the rename, recovery uses the new snapshot together with any WAL entries not included in it.

---

## 5. Crash Recovery & Repair

When an AnchorDB instance starts:
1. It validates the base `.anchordb` file header.
2. It replays uncheckpointed records from `<db>.wal`.
3. Incomplete trailing frames (e.g., caused by an abrupt crash during write) are automatically detected and truncated.
4. If a corrupt mid-log frame is detected, `Open` fails closed with `ErrWALCorrupt`.

### Manual WAL Repair
If a corrupted frame blocks startup, the repair utility truncates the log at the last verified valid frame:

```go
import "github.com/rachmanzz/anchordb/pkg/storage"

// Truncates corrupt tail and archives bad bytes to <db>.wal.corrupt-<timestamp>
err := storage.RepairWALFile("./data/app.anchordb.wal")
if err != nil {
    log.Fatalf("repair failed: %v", err)
}
```

---

## 6. Design Boundaries & Limitations

1. **File-Level Failure Isolation**: Each model has its own `{model}.anchordb`, `{model}.anchordb.wal`, and optional `{model}.anchordb.schema` sidecar. Backups of a locked model must preserve its schema sidecar as well as the database state.
2. **Single-Process Exclusive Lock**: An active model file cannot be opened by two distinct processes at the same time. Concurrency is scaled through goroutines inside one engine process and remote AnchorWire clients connecting to that process.
3. **No Direct External Mutation**: The `.anchordb` file format relies on in-memory address translation and synchronized WAL states. Any external modification to the binary file while the engine is running results in undefined behavior or crash recovery rollbacks.

