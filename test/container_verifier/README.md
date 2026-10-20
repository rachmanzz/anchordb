# AnchorDB End-to-End Container Verification Suite

This package contains the official, comprehensive 6-phase blackbox integration and durability verification test suite for AnchorDB running in Docker or Podman containers.

---

## What the Suite Tests

The suite connects over HTTP/REST to a running AnchorDB server (e.g. `http://localhost:7447`) and executes 6 rigorous phases:

1. **Phase (A): Full Engine Operations Stress**:
   - Ingests 2,000 records with 6 properties each (~3,000 writes/sec).
   - Evaluates on-the-fly in-storage division and multiplication during point queries (`Get`).
   - Executes rich filtering (`Where`, `WhereDate`) with server pagination (`Page`).
   - Executes batch in-place arithmetic updates (`Update().Set()`).
   - Tests spatial node insertion and vector ray-casting.
   - Tests causal link insertion and multi-hop chain traversal.
   - Evaluates direct math engine methods (`Distance`, `Centroid`, `CosineSimilarity`, `Nearest`).

2. **Phase (B): Concurrency Polling Ramp-Up**:
   - Ramps up concurrent workers across 4 tiers: 5, 25, 50, and 100 concurrent goroutines.
   - Measures min, avg, p50, p95, p99, and max latencies.
   - Verifies Little's Law (L = λ · W) and ensures 0% error rate under load.

3. **Phase (C): Container Restart & Data Persistence**:
   - Writes canary sentinel records and causal edges, flushing to disk.
   - Executes container stop (`podman stop anchordb-live-test`).
   - Verifies server is offline and unreachable.
   - Restarts container (`podman start anchordb-live-test`).
   - Reconnects and verifies that canary records, computed properties, causal edges, and bulk records survived with zero data loss.

4. **Phase (D): Background Scheduler & Periodic Sync**:
   - Inspects active scheduler status via `/v1/scheduler/status`.
   - Inspects worker pool concurrency, queue size, and active goroutines via `/v1/worker/status`.
   - Triggers and verifies manual memory-sync flush via `/v1/sync`.

5. **Phase (E): Cold Partition Archival & Hot Storage Pruning**:
   - Pre-seeds dated telemetry records from 200 days ago.
   - Triggers partition archival job via `POST /v1/partition/run`.
   - Polls worker status until completion.
   - Verifies hot storage tombstone pruning and cold partition file creation in the archive sink.

6. **Phase (F): Strict Schema Immutability & Fail-Fast Rejection**:
   - Locks schema contract on a dedicated model.
   - Validates concurrent writes adhering to schema (100% success).
   - Sends 1,000 invalid payloads (missing required fields and undeclared unknown fields) across 20 goroutines, verifying 100% fail-fast rejection with zero leaks.
   - Verifies that re-attaching conflicting schemas is permanently rejected (immutability).
   - Verifies client handshake rejection on mismatched schema.
   - Tests administrative hard reset (`ResetModel` with `confirm: true`) and re-attachment.

---

## How to Run

### Step 1: Start Container

```bash
# Using Podman
podman run -d \
  --name anchordb-live-test \
  -p 7447:7447 \
  -v /tmp/anchordb_test_data:/data:Z \
  -e ANCHORDB_AUTH_TOKEN="test_secret_token_live" \
  -e ANCHORDB_DATA_DIR="/data" \
  localhost/anchordb:latest

# (Or using Docker)
# docker run -d --name anchordb-live-test -p 7447:7447 -v /tmp/anchordb_test_data:/data -e ANCHORDB_AUTH_TOKEN="test_secret_token_live" -e ANCHORDB_DATA_DIR="/data" anchordb:latest
```

### Step 2: Run the Verification Suite

```bash
go run ./test/container_verifier
```

### Step 3: Cleanup

```bash
podman rm -f anchordb-live-test
rm -rf /tmp/anchordb_test_data
```
