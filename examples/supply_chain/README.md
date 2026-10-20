# Multi-Tier Supply Chain Disruption Analysis with AnchorDB

Real-world implementation case study of AnchorDB for **multi-tier supply chain disruption analysis** using **Directed Causal Graphs (Multi-Hop Traversal)** and **In-Storage Arithmetic Computation**.

---

## 1. Domain Problem & Target Audience

### Who Uses This Solution?
1. **Multinational OEMs & Electronics Brands**: Managing thousands of component dependencies across worldwide suppliers (semiconductors, camera modules, batteries, display panels, chassis).
2. **Global Retail & E-Commerce Enterprises**: Monitoring real-time buffer inventory and replenishment velocity across regional distribution fulfillment centers and flagship retail stores.
3. **Logistics & Freight Operators (3PL/4PL)**: Anticipating domino effects of raw material and component delays on manufacturing plants and consumer product availability.

---

## 2. Real-World Business Question

> ### *"If our semiconductor and sensor fab in Bac Ninh, Vietnam is shut down due to severe monsoon flooding, which consumer product SKUs will run out of stock within 30 days across our retail stores?"*

In conventional database architectures (Relational SQL or Document/Key-Value stores), answering this question requires recursive multi-table queries (6-level nested joins) that suffer from high latency and lock contention, or slow overnight ETL jobs.

The example evaluates this query using AnchorWire graph traversal and entity lookups; actual latency depends on the server and dataset.

---

## 3. Why Multi-Hop (> 2 Hops) Is Essential

Real-world industrial disruptions do not stop at Tier 1 suppliers. Global manufacturing relies on a deep multi-tier propagation mesh:

```
[Hop 0: Natural Disaster Event]
   Bac Ninh Fab, Vietnam (Flooded & Offline)
               │
               ▼ (Hop 1: supplies_t1)
   Tier 1 Specialized Suppliers (3nm SoC Wafer, Periscope Prism, Flexible OLED, 5G RF)
               │
               ▼ (Hop 2: subassembles)
   Sub-Assembly Units (Main Logic Board PCB, OIS Camera Module, Display Chassis)
               │
               ▼ (Hop 3: fabricates)
   Final Assembly Mega-Plants (Shenzhen, Penang, Chennai)
               │
               ▼ (Hop 4: transports)
   Regional Logistics & Cargo Hubs (Singapore APAC, Los Angeles NA, Rotterdam EU)
               │
               ▼ (Hop 5: distributes)
   Retail Megastores (Tokyo Ginza, Seattle Flagship, Jakarta, Frankfurt, Sydney)
               │
               ▼ (Hop 6: stocks_sku)
   Final Consumer Product SKUs on Retail Shelves (Smartphone Pro, Ultrabook, Tablet, Watch)
```

If query traversal halts at **Hop 2**, decision makers only know that *"Mainboard sub-assembly is delayed"*, but **cannot identify which final products and stores will suffer stockouts**. Actionable operational decisions require complete visibility down to **Hop 6**.

---

## 4. Why Historical Probability Scoring Matters

Supply chain dependencies are not binary (`true`/`false`):
- **Single-Source Bottlenecks**: Components with no alternative supplier have very high failure transmission coefficients (0.95 - 0.98). If the upstream plant halts, downstream assembly stops immediately.
- **Dual-Source Components**: Components with qualified secondary suppliers have lower transmission coefficients (0.70 - 0.85), as production orders can be partially re-routed.
- **Logistics Routes**: Shipping lanes with buffer buffers dampen shock waves.

AnchorDB multiplies edge transmission weights along the multi-hop path automatically via `CausalChain`:

> **P(Disruption)** = w₁ · w₂ · ... · wₙ (product of path edge weights)

---

## 5. Why In-Storage Math Is Critical

Knowing a product has a 79% disruption probability is only half the answer. The executive question is: **"Will current stock run out within 30 days?"**

AnchorDB stores entity attributes:
- `inventory_units`: Current buffer inventory units available in the warehouse/store.
- `daily_burn_rate`: Daily sales or consumption rate (units/day).

AnchorDB calculates the time-to-stockout on-the-fly directly inside the storage engine without round-trip data transfer:

> **Days to Stockout** = inventory units / daily burn rate

If `Days to Stockout <= 30.0`, the engine triggers an **ALERT (<30 Days)** and outputs priority mitigation recommendations (e.g., expediting air freight or reallocating regional buffer stock).

---

## 6. Directory Structure

```
examples/supply_chain/
├── main.go               # AnchorWire program (6-hop causal graph, 30-day math, selected restart checks, 5M-entity benchmark)
├── docker-compose.yml    # Container service definition with persistent volume & WAL sync
└── README.md             # Complete technical guide & operational instructions (this file)
```

---

## 7. How to Run (AnchorWire Binary TCP)

AnchorDB communicates via the high-performance **AnchorWire Binary TCP Protocol** (default port `7448`):

Set `ANCHORDB_AUTH_TOKEN` to the server token before starting the server or running the client commands.

```bash
# Run realistic Vietnam flood disruption analysis via AnchorWire TCP
go run ./examples/supply_chain/main.go -url=wire://localhost:7448 -token="$ANCHORDB_AUTH_TOKEN" -model=supply_chain_readme -scenario=analysis
```

#### Observed Sample Output

On a fresh model (`-model=supply_chain_readme`), the run reached 4, 7, 10, 13, 18, then 24 entities across the six traversal depths, and reported 22 reached entities with stockout estimates within 30 days. Because each depth reuses the complete relation sequence, results accumulate entities from earlier hops; the final analysis is not limited to product SKUs. Use a fresh model to reproduce these counts; timings and data-dependent results may vary by server version or pre-existing data and are not performance guarantees.

---

### Container Deployment (Docker & Podman)

Runs AnchorDB as an isolated daemon container exposing AnchorWire TCP on port `7448`:

#### Step 1: Start AnchorDB Container
Using **Docker Compose**:
```bash
cd examples/supply_chain
export ANCHORDB_AUTH_TOKEN="$(openssl rand -hex 24)"
docker compose up -d
```

Or using **Podman**:
```bash
podman run -d \
  --name anchordb-supply-chain \
  -p 7447:7447 \
  -p 7448:7448 \
  -v ./data:/data:Z \
  -e ANCHORDB_PORT=7447 \
  -e ANCHORDB_WIRE_PORT=7448 \
  -e ANCHORDB_DATA_DIR=/data \
  -e ANCHORDB_AUTH_TOKEN="$ANCHORDB_AUTH_TOKEN" \
  -e ANCHORDB_WAL_SYNC=always \
  localhost/anchordb:latest
```

#### Step 2: Execute Supply Chain Analysis via AnchorWire
```bash
go run ./examples/supply_chain/main.go \
  -url=wire://localhost:7448 \
  -token="$ANCHORDB_AUTH_TOKEN" \
  -scenario=analysis
```

---

## 8. Sample Data Checks and Container Restart

AnchorDB utilizes a **Write-Ahead Log (WAL)**; the restart check below verifies selected example records only.

### Automated AnchorWire Session Reconnect Check (no server restart):
```bash
go run ./examples/supply_chain/main.go -token="$ANCHORDB_AUTH_TOKEN" -scenario=crash-test -phase=all
```

This mode closes and reopens the client connection to the same running server. It checks selected schema, canary, and graph data; it does **not** simulate a container restart or prove zero data loss.

Note: `-phase=all` may replace the canary value and should be run against a dedicated example model.

### Container Stop/Restart Check:

1. **Populate initial graph & data in container via AnchorWire**:
   ```bash
    go run ./examples/supply_chain/main.go \
      -url=wire://localhost:7448 \
      -token="$ANCHORDB_AUTH_TOKEN" \
     -scenario=crash-test \
     -phase=populate
   ```

2. **Stop the server container** (this is a normal container stop, not a hard kill):
   ```bash
   docker stop anchordb-supply-chain
   # (or with Podman: podman stop anchordb-supply-chain)
   ```

3. **Reboot the container**:
   ```bash
   docker start anchordb-supply-chain
   # (or with Podman: podman start anchordb-supply-chain)
   ```

4. **Verify selected data and graph reachability after restart**:
   ```bash
    go run ./examples/supply_chain/main.go \
      -url=wire://localhost:7448 \
      -token="$ANCHORDB_AUTH_TOKEN" \
      -scenario=crash-test \
      -phase=verify
   ```

This checks that the expected schema fields exist, the canary value matches, and the selected causal edge is traversable. It does not compare the full dataset or establish zero data loss.
```text
[STEP 2/3] Verifying Selected Data & Graph Integrity After Restart...
    [✓] Expected schema fields present: 5
    [✓] Canary record verified intact: inventory_units = 999999
    [✓] Causal graph verified: factory_vietnam_bac_ninh -> supplier_t1_soc_processor (Score: 0.9600)

==========================================================================================
    >>> POST-RESTART SAMPLE CHECKS PASSED (no full-dataset comparison performed)
==========================================================================================
```

---

## 9. Large-Scale Stress Testing (Up to 5 Million Entities)

To exercise the example under larger supply-chain datasets (50,000 to 5,000,000 entities), the `benchmark` scenario ingests `-scale` entities into the model.

> **Scale Boundary**: Only **entities** scale with `-scale`. The benchmark graph always links a fixed **50,000** causal edges, independent of `-scale`. The scale must therefore be between **50,000 and 5,000,000**. The "5 million" figure refers to entities, not edges. The 5-million command can consume substantial disk and memory; use a dedicated model and sufficient free space.

### Run 100,000 Entity Benchmark:
```bash
go run ./examples/supply_chain/main.go -url=wire://localhost:7448 -token="$ANCHORDB_AUTH_TOKEN" -scenario=benchmark -scale=100000
```
The example reports actual elapsed time, throughput, and query results for the current run.

### Run Full 5,000,000 Entity Stress Test:
```bash
go run ./examples/supply_chain/main.go -url=wire://localhost:7448 -token="$ANCHORDB_AUTH_TOKEN" -scenario=benchmark -scale=5000000
```
The example reports actual elapsed time, throughput, and query results for the current run. Both commands require AnchorWire at `wire://localhost:7448`; run them from the repository root and include `-token="$ANCHORDB_AUTH_TOKEN"` when authentication is enabled.

> **Memory Architecture Note**:
> This example uses the AnchorWire client and concurrent individual writes. It does not establish zero-copy ingestion or a bounded-memory/GC guarantee.
