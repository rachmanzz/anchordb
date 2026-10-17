# Example: Podman Container Durability & Crash Recovery

This example demonstrates how AnchorDB provides **Zero-Data-Loss Durability** across container lifecycle events, sudden container stops, and restarts using **Podman** and host-mounted storage volumes.

It proves that:
1. **Model Schema Immutability**: The locked schema contract is persisted to disk and automatically restored upon restart.
2. **Causal Graph Topology Retention**: All directed causal edges across 5 relational tiers are preserved and traversed with identical scores after container recreation.
3. **In-Storage Arithmetic Durability**: Entity properties and in-storage arithmetic queries (`List().Compute()`) yield 100% consistent results pre- and post-restart.

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
|  5. Flush Memory Sync             ---> Atomic msync(MS_SYNC) via /v1/sync         |
+-----------------------------------------------------------------------------------+
                                        |
                           podman stop anchordb-podman-test
                         (Container Killed - Zero TCP Access)
                                        |
                           podman start anchordb-podman-test
                       (Engine Boots - Storage & WAL Replayed)
                                        |
                                        v
+-----------------------------------------------------------------------------------+
|  [PHASE 2: VERIFY]                                                                |
|  1. Query Schema Contract         ---> Matches locked schema identically          |
|  2. Run 1-5 Hop Traversal         ---> All 35 edges restored, scores match 100%   |
|  3. Resolve Points via Get()      ---> All node attributes present and typed      |
|  4. In-Storage Compute via List() ---> Arithmetic risk index matches pre-shutdown |
+-----------------------------------------------------------------------------------+
```

---

## Prerequisites

1. **Podman** installed:
   ```bash
   podman --version
   ```
2. **Build Local Container Image**:
   ```bash
   podman build -t localhost/anchordb:latest .
   ```

---

## Step-by-Step Execution Guide

### Step 1: Create Host Directory & Launch Container

```bash
# 1. Create host directory for persistent volume mount
mkdir -p ./podman_test_data
chmod -R 777 ./podman_test_data

# 2. Run AnchorDB container in the background
podman run -d \
  --name anchordb-podman-test \
  -p 7447:7447 \
  -v $(pwd)/podman_test_data:/data:Z \
  -e ANCHORDB_ALLOW_INSECURE=true \
  -e ANCHORDB_DATA_DIR=/data \
  localhost/anchordb:latest

# 3. Verify server is healthy
curl -s http://localhost:7447/health
```

### Step 2: Populate Schema, Nodes, and Graph Edges

Run the population phase using the example runner:

```bash
go run ./examples/podman_durability -phase=populate
```

**Expected Output:**
```
==========================================================================
   PODMAN STEP 1: Populating Schema, Multi-Branch Graph, and Initial Run
==========================================================================
    [✓] Schema attached and locked to model 'infra_causal' via REST API
    [✓] Ingested 28 nodes into Podman container with schema validation
    [✓] Linked 35 directed causal edges across 5 relational tiers

    --- Initial Traversal Verification (Pre-Shutdown) ---
    Hop 1 (triggers) -> Reached 4 nodes | Top: ups_alpha_failure          (Score: 0.9500)
    Hop 2 (propagates) -> Reached 5 nodes | Top: rack_alpha_blackout        (Score: 0.9025)
    Hop 3 (degrades) -> Reached 5 nodes | Top: db_primary_crash           (Score: 0.8574)
    Hop 4 (disrupts) -> Reached 5 nodes | Top: checkout_api_down          (Score: 0.8402)
    Hop 5 (escalates) -> Reached 4 nodes | Top: direct_revenue_loss        (Score: 0.7982)
```

Flush disk sync to ensure zero unflushed dirty pages:
```bash
curl -s http://localhost:7447/v1/sync
```

Inspect the host storage files:
```bash
ls -la ./podman_test_data
# infra_causal.anchordb
# infra_causal.anchordb.schema
# infra_causal.anchordb.wal
```

---

### Step 3: Stop the Container (Simulate Power Off / Outage)

```bash
podman stop anchordb-podman-test
```

Verify that the container is completely stopped and port 7447 is down:
```bash
curl -s http://localhost:7447/health || echo "Server is DOWN"
```

---

### Step 4: Restart the Container (Crash Recovery Boot)

```bash
podman start anchordb-podman-test
```

Wait 1-2 seconds for startup and check health:
```bash
curl -s http://localhost:7447/health
```

---

### Step 5: Verify Durability & Data Retention

Execute the verification phase:

```bash
go run ./examples/podman_durability -phase=verify
```

**Expected Output:**
```
==========================================================================
   PODMAN STEP 3: Verifying Schema, Traversal, Get & List After Restart
==========================================================================
    [✓] Schema Contract Restored from disk: map[impact_usd:float is_critical:bool latency_ms:float severity:int tier:string?]

    --- [A] Causal Traversal Verification Across All Hops (Post-Restart) ---
    ▶ Hop 1 (triggers  ) -> 4 nodes reached. Top Target: ups_alpha_failure          | Score: 0.9500
       [1] ups_alpha_failure            (Score: 0.9500)
       [2] ups_beta_failure             (Score: 0.9000)
       [3] hvac_cooling_shutdown        (Score: 0.8500)
       [4] aux_bus_overload             (Score: 0.8000)

    ▶ Hop 2 (propagates) -> 5 nodes reached. Top Target: rack_alpha_blackout        | Score: 0.9025
       [1] rack_alpha_blackout          (Score: 0.9025)
       [2] rack_beta_blackout           (Score: 0.8100)
       [3] spine_switch_restart         (Score: 0.8075)
       [4] san_storage_bus_fault        (Score: 0.7650)
       [5] thermal_throttling_core      (Score: 0.7650)

    ▶ Hop 3 (degrades  ) -> 5 nodes reached. Top Target: db_primary_crash           | Score: 0.8574
       [1] db_primary_crash             (Score: 0.8574)
       [2] kafka_broker_loss            (Score: 0.8122)
       [3] ingress_api_timeout          (Score: 0.7429)
       [4] db_replica_desync            (Score: 0.7290)
       [5] san_io_lockup                (Score: 0.7267)

    ▶ Hop 4 (disrupts  ) -> 5 nodes reached. Top Target: checkout_api_down          | Score: 0.8402
       [1] checkout_api_down            (Score: 0.8402)
       [2] payment_gateway_v2_fail      (Score: 0.8145)
       [3] inventory_sync_hang          (Score: 0.7310)
       [4] streaming_telemetry_loss     (Score: 0.6904)
       [5] auth_session_invalidation    (Score: 0.6196)

    ▶ Hop 5 (escalates ) -> 4 nodes reached. Top Target: direct_revenue_loss        | Score: 0.7982
       [1] direct_revenue_loss          (Score: 0.7982)
       [2] sla_penalty_breach           (Score: 0.7738)
       [3] customer_churn_escalation    (Score: 0.7562)
       [4] regulatory_audit_alert       (Score: 0.7331)

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
   PODMAN CONTAINER DURABILITY VERIFICATION: 100% PASSED!
==========================================================================
```

---

### Step 6: Cleanup Test Environment

```bash
podman rm -f anchordb-podman-test
rm -rf ./podman_test_data
```
