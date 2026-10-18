# Example: Podman Container Restart Checks

This example demonstrates how to check selected AnchorDB data after a normal Podman container stop and restart, using a host-mounted storage volume. It does not by itself prove zero data loss for the complete database. The shown container command requires the host data directory to be writable by the image's `anchordb` user; `chmod -R 777` is a permissive local-demo workaround, not a production permission recommendation.

It checks that:
1. The expected schema fields are returned after restart.
2. The example's configured causal paths remain traversable.
3. Selected entities and a list-compute query return data after restart. The example does not snapshot and compare all pre-restart results.

---

## Architectural Durability Flow

```
+-----------------------------------------------------------------------------------+
|                               PODMAN HOST SYSTEM                                  |
+-----------------------------------------------------------------------------------+
| Host Storage Volume: /host/path/podman_test_data                                  |
|   • infra_causal.anchordb         (mmap hot binary storage)                       |
|   • infra_causal.anchordb.schema  (locked schema contract metadata)               |
|   • infra_causal.anchordb.wal     (write-ahead log with CRC32 checksums)          |
+-----------------------------------------------------------------------------------+
                                        |
                 Mounted into Container Volume (/data:Z)
                                        |
                                        v
+-----------------------------------------------------------------------------------+
|                        PODMAN CONTAINER: anchordb-podman-test                     |
|                               (Port 7447)                                         |
+-----------------------------------------------------------------------------------+
|  [PHASE 1: POPULATE]                                                              |
|  1. Attach Schema Contract        ---> Written to infra_causal.anchordb.schema    |
|  2. Ingest 28 Nodes               ---> Appended to WAL & mapped into RAM          |
|  3. Link 35 Directed Edges        ---> Appended to WAL & Adjacency Index          |
|  4. Run 1-5 Hop Causal Traversal  ---> Verified Pre-Shutdown                      |
|  5. Flush Memory Sync             ---> Request server sync before shutdown       |
+-----------------------------------------------------------------------------------+
                                        |
                           podman stop anchordb-podman-test
                            (Normal container shutdown; TCP stops)
                                        |
                           podman start anchordb-podman-test
                         (Server starts with persisted storage)
                                        |
                                        v
+-----------------------------------------------------------------------------------+
|  [PHASE 2: VERIFY]                                                                |
|  1. Query Schema Contract         ---> Matches expected schema fields            |
|  2. Run 1-5 Hop Traversal         ---> Example paths remain traversable          |
|  3. Resolve Points via Get()      ---> All node attributes present and typed      |
|  4. In-Storage Compute via List() ---> Computed values checked for consistency   |
+-----------------------------------------------------------------------------------+
```

---

## Prerequisites

1. **Podman** installed:
   ```bash
   podman --version
   ```
2. **Use the existing local image** `localhost/anchordb:latest`, or build it from the repository root if needed:
   ```bash
   podman build -t localhost/anchordb:latest -f Dockerfile .
   ```

---

## Step-by-Step Execution Guide

### Step 1: Create Host Directory & Launch Container

```bash
# 1. Create host directory for persistent volume mount
mkdir -p ./podman_test_data
chmod -R 777 ./podman_test_data
export ANCHORDB_AUTH_TOKEN="$(openssl rand -hex 24)"

# 2. Run AnchorDB container in the background exposing AnchorWire on port 7448
podman run -d \
  --name anchordb-podman-test \
  -p 7447:7447 \
  -p 7448:7448 \
  -v $(pwd)/podman_test_data:/data:Z \
  -e ANCHORDB_AUTH_TOKEN="$ANCHORDB_AUTH_TOKEN" \
  -e ANCHORDB_DATA_DIR=/data \
  localhost/anchordb:latest

# 3. Verify server is healthy
curl -s http://localhost:7447/health
```

### Step 2: Populate Schema, Nodes, and Graph Edges via AnchorWire

Run the population phase using the example runner via AnchorWire Binary TCP:

```bash
go run ./examples/podman_durability -phase=populate -url=wire://localhost:7448 -token="$ANCHORDB_AUTH_TOKEN"
```

**Expected Output:**
```
==========================================================================
   PODMAN STEP 1: Populating Schema, Multi-Branch Graph, and Initial Run
==========================================================================
    [✓] Schema attached and locked to model 'infra_causal' via AnchorWire TCP
    [✓] Ingested 28 nodes into Podman container with schema validation
    [✓] Linked 35 directed causal edges across 5 relational tiers

    --- Initial Traversal Verification (Pre-Shutdown) ---
    Hop 1 (triggers) -> Reached 4 nodes | Top: ups_alpha_failure          (Score: 0.9500)
     Hop 2 (propagates) -> Reached 9 nodes | Top: ups_alpha_failure          (Score: 0.9500)
     Hop 3 (degrades) -> Reached 14 nodes | Top: ups_alpha_failure          (Score: 0.9500)
     Hop 4 (disrupts) -> Reached 19 nodes | Top: ups_alpha_failure          (Score: 0.9500)
     Hop 5 (escalates) -> Reached 23 nodes | Top: ups_alpha_failure          (Score: 0.9500)
```

Request a server sync before stopping the container:
```bash
curl -fsS -X POST -H "Authorization: Bearer $ANCHORDB_AUTH_TOKEN" \
  "http://localhost:7447/v1/sync?model=infra_causal"
```

Inspect the host storage files:
```bash
ls -la ./podman_test_data
# infra_causal.anchordb
# infra_causal.anchordb.schema
# infra_causal.anchordb.wal
```

---

### Step 3: Stop the Container Normally

```bash
podman stop anchordb-podman-test
```

Verify that the container is completely stopped and the example server is unavailable:
```bash
curl -s http://localhost:7447/health || echo "Server is DOWN"
```

---

### Step 4: Restart the Container

```bash
podman start anchordb-podman-test
```

Wait 1-2 seconds for startup and check health:
```bash
curl -s http://localhost:7447/health
```

---

### Step 5: Verify Selected Data & Graph Reachability

This performs selected post-restart checks; it does not compare the entire pre-restart database and should not be interpreted as proof of zero data loss.

The example uses the `infra_causal` model. Run it against a dedicated data directory because populate replaces entity values for its fixed sample names.

Execute the verification phase:

```bash
go run ./examples/podman_durability -phase=verify -url=wire://localhost:7448 -token="$ANCHORDB_AUTH_TOKEN"
```

**Expected Output:**
```
==========================================================================
   PODMAN STEP 3: Verifying Schema, Traversal, Get & List After Restart
==========================================================================
    [✓] Schema Contract Restored from disk: map[impact_usd:float is_critical:bool latency_ms:float severity:int tier:int?]

    --- [A] Causal Traversal Verification Across All Hops (Post-Restart) ---
    ▶ Hop 1 (triggers  ) -> 4 nodes reached. Top Target: ups_alpha_failure          | Score: 0.9500
       [1] ups_alpha_failure            (Score: 0.9500)
       [2] ups_beta_failure             (Score: 0.9000)
       [3] hvac_cooling_shutdown        (Score: 0.8500)
       [4] aux_bus_overload             (Score: 0.8000)

    ▶ Hop 2 (propagates) -> 9 nodes reached. Top Target: ups_alpha_failure          | Score: 0.9500
       [1] ups_alpha_failure            (Score: 0.9500)
       [2] ups_beta_failure             (Score: 0.9000)
       [3] hvac_cooling_shutdown        (Score: 0.8500)
       [4] aux_bus_overload             (Score: 0.8000)
       [5] rack_alpha_blackout          (Score: 0.9500)

      ▶ Hop 3 (degrades  ) -> 14 nodes reached. Top Target: ups_alpha_failure          | Score: 0.9500
       [1] ups_alpha_failure            (Score: 0.9500)
       [2] ups_beta_failure             (Score: 0.9000)
       [3] hvac_cooling_shutdown        (Score: 0.8500)
       [4] aux_bus_overload             (Score: 0.8000)
       [5] rack_alpha_blackout          (Score: 0.9500)

      ▶ Hop 4 (disrupts  ) -> 19 nodes reached. Top Target: ups_alpha_failure          | Score: 0.9500
       [1] ups_alpha_failure            (Score: 0.9500)
       [2] ups_beta_failure             (Score: 0.9000)
       [3] hvac_cooling_shutdown        (Score: 0.8500)
       [4] aux_bus_overload             (Score: 0.8000)
       [5] rack_alpha_blackout          (Score: 0.9500)

      ▶ Hop 5 (escalates ) -> 23 nodes reached. Top Target: ups_alpha_failure          | Score: 0.9500
       [1] ups_alpha_failure            (Score: 0.9500)
       [2] ups_beta_failure             (Score: 0.9000)
       [3] hvac_cooling_shutdown        (Score: 0.8500)
       [4] aux_bus_overload             (Score: 0.8000)
       [5] rack_alpha_blackout          (Score: 0.9500)

    --- [B] Schema-Enforced GET Resolution on Top Discovered Nodes ---
    Entity Name                  | Severity | Impact (USD) | Latency (ms) | Is Critical
    ---------------------------------------------------------------------------------
    payment_gateway_v2_fail      | 5        | $450000.00   | 6000.0       | 1
    db_primary_crash             | 5        | $180000.00   | 2500.0       | 1
    direct_revenue_loss          | 5        | $980000.00   | 0.0          | 1
    sla_penalty_breach           | 5        | $750000.00   | 0.0          | 1

    --- [C] Schema-Enforced LIST Query with In-Storage Math (Post-Restart) ---
    Total matching items returned: 10 (HasMore: true)
    Entity Name                  | Severity | Impact (USD) | Risk Index (Math) | Is Critical
    ----------------------------------------------------------------------------------------
    dc_power_trip                | 5        | $250000.00   | $1250000.00    | 1
    ups_alpha_failure            | 5        | $120000.00   | $600000.00     | 1
    ups_beta_failure             | 4        | $90000.00    | $360000.00     | 1
    hvac_cooling_shutdown        | 4        | $75000.00    | $300000.00     | 1
    rack_alpha_blackout          | 5        | $80000.00    | $400000.00     | 1
    rack_beta_blackout           | 4        | $65000.00    | $260000.00     | 1
    spine_switch_restart         | 4        | $50000.00    | $200000.00     | 1
    cooling_loop_failure         | 4        | $45000.00    | $180000.00     | 1
    san_storage_bus_fault        | 5        | $95000.00    | $475000.00     | 1
    db_primary_crash             | 5        | $180000.00   | $900000.00     | 1

==========================================================================
    PODMAN RESTART CHECKS PASSED for sampled schema, graph, and entities.
==========================================================================
```

---

### Step 6: Cleanup Test Environment

```bash
podman rm -f anchordb-podman-test
rm -rf ./podman_test_data
```
