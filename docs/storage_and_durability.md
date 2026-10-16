# Storage Engine, WAL & Durability

AnchorDB utilizes memory-mapped binary files combined with a Write-Ahead Log (WAL) to provide fast record access and crash resilience.

---

## 1. Binary Storage Format (`.anchordb`)

Database files have a structured binary format designed for memory-mapped (`mmap`) access without deserialization overhead.

### File Header Layout (64 Bytes)
The file begins with a 64-byte fixed-size header:

| Offset (Bytes) | Size (Bytes) | Field | Description |
| :---: | :---: | :--- | :--- |
| `0..3` | 4 | Magic | Format identifier (`"ANC3"`) |
| `4..7` | 4 | Version | Database format version (`3`) |
| `8..11` | 4 | Node Count | Total number of spatial nodes |
| `12..15` | 4 | Relation Count | Number of predefined geometric relations |
| `16..23` | 8 | Causal Section Offset | Byte offset to causal edge section (`EDG3`) |

| `24..63` | 40 | Reserved | Padding for future structural extensions |

### Record Layout
* **Spatial Nodes (40 Bytes Fixed)**:
  * Word / identifier string offset: 16 bytes
  * 2D Coordinates $(X, Y)$: two `float32` values (8 bytes)
  * Semantic Role Vector: four `float32` values (16 bytes)
* **Entity Records (`CompactRecord`)**:
  * Stored in contiguous byte slices with interned string keys to avoid duplicated key strings across millions of records.
  * Attribute Pool compaction automatically purges unreferenced keys once the pool exceeds 131,072 entries (`db.CompactAttrPool()`).

---

## 2. Write-Ahead Logging (WAL)

All mutations (insertions, updates, deletes, and edge links) are written to an append-only `.wal` file alongside the `.anchordb` file before being applied to in-memory state.

### Frame Structure
Each WAL entry is protected by a CRC32 checksum:

```
+------------+------------+---------------+------------+--------------------+------------+
| Magic (4B) | Opcode (1B)| Payload Len(4B)| Record ID  | Payload Bytes      | CRC32 (4B) |
+------------+------------+---------------+------------+--------------------+------------+
```

### Durability Modes (`SyncMode`)

AnchorDB provides three explicit WAL durability modes to let operators choose their exact position on the physical durability vs write throughput frontier. This mirrors the durability controls of industry-standard storage engines (such as PostgreSQL's `synchronous_commit`, Redis's `appendfsync`, and SQLite's `PRAGMA synchronous`):

| Mode | fsync Semantics | Max Write Throughput | Crash Resilience | Industry Equivalent | Recommended For |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **`always`** (default) | Synchronous `fsync` on every WAL write | ~500–2,000 writes/sec (bounded by disk IOPS) | **Zero committed writes lost** on power failure | Postgres `synchronous_commit=on`<br>Redis `appendfsync always`<br>SQLite `synchronous=FULL` | Financial transactions, critical audit trails, compliance logs |
| **`interval`** | Background goroutine flushes `fsync` periodically (default `200ms`) | ~100,000+ writes/sec | At most one interval window lost on sudden power failure | Postgres `synchronous_commit=off`<br>Redis `appendfsync everysec`<br>SQLite `synchronous=NORMAL` | High-throughput IoT telemetry, metric streams, edge devices, SD cards |
| **`none`** | Never invokes `fsync`; relies on OS page cache flushing | ~320,000+ writes/sec (pure RAM speed: ~3.05 µs/op) | Clean exits preserved; power failure loses dirty OS pages | Redis `appendfsync no`<br>SQLite `synchronous=OFF` | Ephemeral testing, bulk ETL data imports, cache backends |

### Throughput & Latency Analysis

* **The Physical Limit of `SyncAlways`**: Calling `fsync()` forces the storage controller to flush volatile write buffers to non-volatile flash or magnetic platters. Sequential single-threaded writes in `always` mode are strictly bounded by hardware rotational or flash latency (~0.5–2ms on NVMe SSDs, ~10–30ms on HDDs or SD cards).
* **Batch Amortization**: When running in `always` mode, multiple records modified in a batch update pass share a single commit window, amortizing the physical `fsync` overhead across thousands of records.
* **Maximizing Throughput with `SyncInterval`**: For telemetry, sensor logging, and tracking where sub-second durability loss on power outage is acceptable, setting `-wal-sync=interval -wal-sync-interval=50ms` unlocks **100,000+ writes/sec** while bounding maximum possible data loss to 50ms of activity.
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
1. The database state is serialized into a temporary file (`.anchordb.tmp.<pid>`).
2. The temporary file is `fsync`ed to ensure physical durability.
3. The temporary file is atomically renamed over the original `.anchordb` file (`os.Rename`).
4. The live memory mapping is safely updated to the new file descriptor.
5. The WAL file is truncated/checkpointed now that the base file is updated.

If a crash occurs mid-write, the original `.anchordb` file and the full `.wal` remain untouched.

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

1. **File-Level Failure Isolation**: Each model file `{model}.anchordb` and its corresponding `{model}.wal` are completely independent. A power failure or filesystem corruption on one model file cannot corrupt other model files.
2. **Single-Process Exclusive Lock**: An active model file cannot be opened by two distinct processes at the same time. Concurrency is scaled via multiple worker threads/goroutines inside a single engine process or through the Remote Engine HTTP server.
3. **No Direct External Mutation**: The `.anchordb` file format relies on in-memory address translation and synchronized WAL states. Any external modification to the binary file while the engine is running results in undefined behavior or crash recovery rollbacks.

