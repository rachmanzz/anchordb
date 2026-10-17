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

AnchorDB solves this query in **microseconds** directly in-memory and zero-copy.

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
- **Single-Source Bottlenecks**: Components with no alternative supplier have very high failure transmission coefficients ($0.95 - 0.98$). If the upstream plant halts, downstream assembly stops immediately.
- **Dual-Source Components**: Components with qualified secondary suppliers have lower transmission coefficients ($0.70 - 0.85$), as production orders can be partially re-routed.
- **Logistics Routes**: Shipping lanes with buffer buffers dampen shock waves.

AnchorDB multiplies edge transmission weights along the multi-hop path automatically via `CausalChain`:
$$P(\text{Disruption}) = \prod_{i=1}^{n} w_i$$

---

## 5. Why In-Storage Math Is Critical

Knowing a product has a 79% disruption probability is only half the answer. The executive question is: **"Will current stock run out within 30 days?"**

AnchorDB stores entity attributes:
- `inventory_units`: Current buffer inventory units available in the warehouse/store.
- `daily_burn_rate`: Daily sales or consumption rate (units/day).

AnchorDB calculates the time-to-stockout on-the-fly directly inside the storage engine without round-trip data transfer:
$$\text{Days to Stockout} = \frac{\text{inventory\_units}}{\text{daily\_burn\_rate}}$$

If $\text{Days to Stockout} \le 30.0$, the engine triggers an **ALERT (<30 Days)** and outputs priority mitigation recommendations (e.g., expediting air freight or reallocating regional buffer stock).

---

## 6. Directory Structure

```
examples/supply_chain/
├── main.go               # Complete Go program (Embedded & Remote modes, 6-Hop Causal Graph, 30-Day Math, Crash-Test, 5M-Entity Benchmark)
├── docker-compose.yml    # Container service definition with persistent volume & WAL sync
└── README.md             # Complete technical guide & operational instructions (this file)
```

---

## 7. How to Run

### Mode A: Embedded Engine (In-Process Runtime, Zero Dependency)
Runs AnchorDB directly in the Go process memory with zero network overhead and no Docker/server required:

```bash
# Run realistic Vietnam flood disruption analysis (6-Hop Causal + In-Storage Math)
go run ./examples/supply_chain/main.go -mode=embedded -scenario=analysis
```

#### Real Execution Output:
```text
>>> [PHASE 4] Executing Multi-Hop Causal Disruption Traversal (Hop 1 to Hop 6)
    ▶ Hop 1 (supplies_t1) -> Reached 4 downstream targets [Latency: 45.2µs]
    ▶ Hop 2 (supplies_t1 → subassembles) -> Reached 3 downstream targets [Latency: 2.2µs]
    ▶ Hop 3 (supplies_t1 → subassembles → fabricates) -> Reached 3 downstream targets [Latency: 2.2µs]
    ▶ Hop 4 (supplies_t1 → ... → transports) -> Reached 3 downstream targets [Latency: 13.5µs]
    ▶ Hop 5 (supplies_t1 → ... → distributes) -> Reached 5 downstream targets [Latency: 3.6µs]
    ▶ Hop 6 (supplies_t1 → ... → stocks_sku) -> Reached 6 downstream targets [Latency: 11.6µs]

┌───────────────────────────┬──────────────┬────────────┬──────────────┬─────────────┬──────────────┬──────────────────────────────────┐
│ Product SKU Name          │ Current Stock│ Burn Rate  │ Days to Empty│ Prob. Impact│ Status       │ Action Recommendation            │
├───────────────────────────┼──────────────┼────────────┼──────────────┼─────────────┼──────────────┼──────────────────────────────────┤
│ sku_ultrabook_pro_16in    │        150 u │     12.0/d │       12.5 d │       76.6% │ ALERT (<30d) │ EMERGENCY: Reroute air freight   │
│ sku_smartphone_pro_1tb    │        240 u │     14.0/d │       17.1 d │       79.0% │ ALERT (<30d) │ EMERGENCY: Reroute air freight   │
│ sku_smartwatch_fit_band   │        190 u │      9.0/d │       21.1 d │       71.1% │ ALERT (<30d) │ Allocate backup warehouse stock  │
│ sku_smartphone_base_128gb │        520 u │     20.0/d │       26.0 d │       75.0% │ ALERT (<30d) │ Allocate backup warehouse stock  │
│ sku_tablet_air_11in       │        480 u │     10.0/d │       48.0 d │       72.6% │ SAFE (>30d)  │ Monitor normal replenishment     │
│ sku_smartwatch_ultra_49mm │        650 u │     11.0/d │       59.1 d │       70.3% │ SAFE (>30d)  │ Monitor normal replenishment     │
└───────────────────────────┴──────────────┴────────────┴──────────────┴─────────────┴──────────────┴──────────────────────────────────┘

>>> ANALYSIS FINDINGS: Detected 4 SKUs RUNNING OUT OF STOCK WITHIN 30 DAYS due to Vietnam flood!
 [1] sku_ultrabook_pro_16in:
     - Days of Stock Remaining: 12.5 DAYS (Stock: 150 units, Burn Rate: 12.0 units/day)
     - Supply Chain Disruption Probability: 76.63%
     - Component Criticality: SINGLE SOURCE BOTTLENECK (Composite Risk Score: 8.51)

 [2] sku_smartphone_pro_1tb:
     - Days of Stock Remaining: 17.1 DAYS (Stock: 240 units, Burn Rate: 14.0 units/day)
     - Supply Chain Disruption Probability: 79.05%
     - Component Criticality: SINGLE SOURCE BOTTLENECK (Composite Risk Score: 6.54)

 [3] sku_smartwatch_fit_band:
     - Days of Stock Remaining: 21.1 DAYS (Stock: 190 units, Burn Rate: 9.0 units/day)
     - Supply Chain Disruption Probability: 71.11%
     - Component Criticality: DUAL SOURCE (Composite Risk Score: 3.22)

 [4] sku_smartphone_base_128gb:
     - Days of Stock Remaining: 26.0 DAYS (Stock: 520 units, Burn Rate: 20.0 units/day)
     - Supply Chain Disruption Probability: 74.99%
     - Component Criticality: DUAL SOURCE (Composite Risk Score: 2.78)
```

> Disruption probabilities and days-to-stockout are deterministic. Latency figures are indicative only; the Hop 1 value includes first-call warm-up (JIT, mmap page-in) and drops into single-digit microseconds on subsequent runs.

---

### Mode B: Container Mode (Docker & Podman)

Runs AnchorDB as an isolated daemon container on port `7447`, communicating via HTTP REST client driver (`driver.ConnectRemote`).

#### Step 1: Start AnchorDB Container
Using **Docker Compose**:
```bash
cd examples/supply_chain
docker compose up -d
```

Or using **Podman**:
```bash
podman run -d \
  --name anchordb-supply-chain \
  -p 7447:7447 \
  -v ./data:/data:Z \
  -e ANCHORDB_PORT=7447 \
  -e ANCHORDB_DATA_DIR=/data \
  -e ANCHORDB_AUTH_TOKEN="supply_chain_secret_token" \
  -e ANCHORDB_WAL_SYNC=always \
  anchordb:latest
```

Verify container health:
```bash
curl -H "Authorization: Bearer supply_chain_secret_token" http://localhost:7447/health
```

#### Step 2: Execute Supply Chain Analysis via Remote Driver
```bash
go run ./examples/supply_chain/main.go \
  -mode=remote \
  -url=http://localhost:7447 \
  -token=supply_chain_secret_token \
  -scenario=analysis
```

---

## 8. Crash Recovery & WAL Durability Scenario

AnchorDB utilizes a **Write-Ahead Log (WAL)** with CRC32 frame checksums and strict fsync commit semantics.

### Automated In-Process Crash Test:
```bash
go run ./examples/supply_chain/main.go -scenario=crash-test -phase=all
```

### Live Container Crash Test (Abrupt SIGKILL & Reboot):

1. **Populate initial graph & data in container**:
   ```bash
   go run ./examples/supply_chain/main.go \
     -mode=remote \
     -url=http://localhost:7447 \
     -token=supply_chain_secret_token \
     -scenario=crash-test \
     -phase=populate
   ```

2. **Simulate server power loss / hard kill**:
   ```bash
   docker kill anchordb-supply-chain
   # (or with Podman: podman kill anchordb-supply-chain)
   ```

3. **Reboot the container**:
   ```bash
   docker start anchordb-supply-chain
   # (or with Podman: podman start anchordb-supply-chain)
   ```

4. **Verify post-crash data & graph integrity**:
   ```bash
   go run ./examples/supply_chain/main.go \
     -mode=remote \
     -url=http://localhost:7447 \
     -token=supply_chain_secret_token \
     -scenario=crash-test \
     -phase=verify
   ```

Output confirms zero data loss:
```text
[STEP 2/3] Verifying Data & Graph Integrity Post-Crash...
    [✓] Schema survived crash: 5 fields locked
    [✓] Canary record verified intact: inventory_units = 999999
    [✓] Causal graph verified: factory_vietnam_bac_ninh -> supplier_t1_soc_processor (Score: 0.9600)

==========================================================================================
    >>> DURABILITY VERIFICATION RESULT: 100% PASSED (0 DATA LOSS)
==========================================================================================
```

---

## 9. Large-Scale Stress Testing (Up to 5 Million Entities)

To validate AnchorDB under massive enterprise supply chain topologies (e.g., 100,000 to 5,000,000 global parts and SKUs), the `benchmark` scenario ingests `-scale` entities into the model.

> **Scale Boundary**: Only **entities** scale with `-scale`. The benchmark graph always links a fixed **50,000** causal edges (see `numEdges` in `main.go`), independent of `-scale`. The "5 million" figure refers to entities, not edges. (The CLI banner prints `records/edges` as a legacy label; only the entity count reflects `-scale`.)

### Run 100,000 Entity Benchmark:
```bash
go run ./examples/supply_chain/main.go -scenario=benchmark -scale=100000
```
- **Entities Ingested**: 100,000 in ~0.57s (**~174,000 writes/sec**)
- **Causal Edges**: 50,000 (fixed) in ~0.36s (**~137,000 edges/sec**)
- **Multi-Hop Query Latency**: ~237 microseconds (**Sub-Millisecond!**)

### Run Full 5,000,000 Entity Stress Test:
```bash
go run ./examples/supply_chain/main.go -scenario=benchmark -scale=5000000
```
Measured on an 8-logical-core host in embedded mode:
- **Entities Ingested**: 5,000,000 in ~34s (**~147,000 writes/sec**)
- **Causal Edges**: 50,000 (fixed) in ~0.39s (~128,000 edges/sec)
- **Multi-Hop Query Latency**: ~232 microseconds/query (~4,300 queries/sec)
- **Point Compute Latency**: ~11 microseconds
- **Storage (WAL)**: ~586 MB

> **Memory Architecture Note**:
> AnchorDB uses zero-copy memory mapping (`mmap`). The streaming ingestion pipeline operates in concurrent batches without allocating millions of heap objects into the Go runtime garbage collector, preserving stable RAM usage and eliminating GC pause spikes.
