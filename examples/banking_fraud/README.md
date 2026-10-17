# Real-Time Banking Fraud Detection & Anti-Money Laundering (AML) with AnchorDB

Real-world production implementation of AnchorDB for **in-line payment fraud prevention, multi-hop money mule tracking, and impossible travel detection** at a scale of **1,000,000+ bank accounts**.

---

## 1. Domain Problem & The Pre-Authorization Challenge

### Modern Financial Crimes:
1. **Multi-Hop Money Muling & Smurfing**: When a customer's credentials are breached, stolen funds are broken down into small structured transfers (< $10,000 threshold to evade mandatory AML reporting) and hopped through 4 to 8 intermediary mule accounts within minutes before reaching an offshore crypto exchange or cashout ATM.
2. **Card Cloning & Impossible Travel**: Stolen card magnetic stripes or chip data are cloned and swiped across geographically distant terminals (e.g., Jakarta at 14:00, then Surabaya at 14:15).

### The In-Line Latency Bottleneck:
Payment networks (Visa, Mastercard, FedNow, SEPA Instant, Faster Payments, BI-FAST) mandate strict authorization Service Level Agreements (**SLA < 50 ms**, with internal database evaluation budgets often **< 5 ms**).

* **Traditional Graph Databases (e.g. Neo4j)**: Declarative Cypher pattern matching across deep multi-hop graphs under high concurrent transaction load frequently incurs **50 ms to 300 ms** latencies. Consequently, banks are forced to run fraud detection **asynchronously / post-facto** (funds are already gone, and recovery rates are below 5%).
* **AnchorDB Solution**: Evaluates directed multi-hop causal chains and 2D spatial physical distances in **microseconds (0.005 ms - 0.025 ms)**. This allows AnchorDB to be embedded directly inside the **pre-authorization transaction gatekeeper**, blocking fraudulent transfers **in-line before funds leave the bank**.

---

## 2. Architecture: Two-Tier Defense in Banking

```
               Incoming Payment / Wire Transfer Request
                                   │
                                   ▼
            ┌──────────────────────────────────────────────┐
            │        Core Banking Payment Switch           │
            │       (SLA Authorization Budget: < 10 ms)    │
            └──────────────────────┬───────────────────────┘
                                   │
                      Pre-Authorization Check (< 1 ms)
                                   ▼
                    ┌──────────────────────────────┐
                    │  AnchorDB In-Line Gatekeeper │
                    │  - Latency: 5 to 25 µs       │
                    │  - 5-Hop Mule Chain Scoring  │
                    │  - 2D Spatial Velocity Math  │
                    │  - In-Storage Math Filtering │
                    └──────────────┬───────────────┘
                                   │
                    ┌──────────────┴──────────────┐
                    │                             │
          [Score > Threshold]           [Score < Threshold]
                    │                             │
                    ▼                             ▼
        BLOCK & FREEZE (AML Alert)      APPROVE TRANSACTION
                    │                             │
                    └──────────────┬──────────────┘
                                   │
                     Async Event Stream (Kafka / CDC)
                                   ▼
                    ┌──────────────────────────────┐
                    │   Offline Deep Graph DB      │
                    │   (Neo4j / Graph Data Science│
                    │   - Monthly Louvain Clusters │
                    │   - Ad-hoc Cypher Discovery  │
                    │   - Analyst GUI (Bloom)      │
                    └──────────────────────────────┘
```

---

## 3. Key Capabilities Demonstrated

### Scenario 1: Multi-Hop Mule Account & Smurfing Layering Detection
- **Structure**: 5 relational hops connecting a compromised victim account through Tier-1 rapid relays, Tier-2 aggregators, Tier-3 smurfing nodes, Tier-4 P2P crypto escrow mules, down to a blacklisted offshore cash-out wallet.
- **Scoring**: Edge weights represent transaction anomaly risk confidence. `CausalChain` multiplies weights along the path ($P = \prod w_i$).
- **Decision Engine**: Combines path probability with entity attributes (`risk_score`, `is_blacklisted`). If an account links to a known watchlist or exhibits high risk, it is blocked automatically in **under 10 microseconds**.

### Scenario 2: Impossible Travel & Spatial Velocity Anomaly
- **Structure**: ATM and POS physical coordinates mapped into AnchorDB's **2D Spatial Grid**.
- **Velocity Formula**:
  $$\text{Travel Velocity (km/h)} = \frac{\text{Distance}(\text{ATM}_A, \text{ATM}_B)}{\text{Time}(\text{Swipe}_B) - \text{Time}(\text{Swipe}_A)}$$
- **Detection**: A card swiped in Jakarta and 15 minutes later in Surabaya (670 km away) requires a physical speed of **2,694 km/h**. AnchorDB triggers an instant `IMPOSSIBLE TRAVEL ANOMALY` and freezes the cloned card in **1.2 microseconds**.

### Scenario 3: Massive 1,000,000+ Scale Stress Test
- **Scale**: Ingests **1,000,000 bank accounts** with strict schema validation (`risk_score`, `last_tx_timestamp`, `daily_tx_volume`, `is_blacklisted`, `kyc_tier`).
- **Graph Density**: Connects **100,000 fund transfer edges**.
- **Throughput & Latency**: Executes **50,000 live fraud inspection queries**, measuring p50, p95, and p99 latency to prove sub-millisecond execution at scale.

---

## 4. File Structure

```
examples/banking_fraud/
├── main.go               # Complete Go program (Mule Tracking, Impossible Travel, 1M Scale Benchmark)
├── docker-compose.yml    # Container service definition with persistent volume & WAL sync
└── README.md             # Complete technical guide & operational instructions (this file)
```

---

## 5. How to Run

### Prerequisites
- Go 1.26+ (Toolchain `go1.27.2+` recommended)
- Optional: Docker or Podman (for container mode)

### Running via AnchorWire (Binary TCP Protocol)

AnchorDB communicates exclusively via the high-performance **AnchorWire Binary TCP Protocol** (default port `7448`):

#### Step 1: Start Container Daemon
Using **Docker Compose**:
```bash
cd examples/banking_fraud
docker compose up -d
```

Or using **Podman**:
```bash
podman run -d \
  --name anchordb-banking-fraud \
  -p 7447:7447 \
  -p 7448:7448 \
  -v ./data:/data:Z \
  -e ANCHORDB_PORT=7447 \
  -e ANCHORDB_WIRE_PORT=7448 \
  -e ANCHORDB_DATA_DIR=/data \
  -e ANCHORDB_AUTH_TOKEN="banking_fraud_secret_token" \
  -e ANCHORDB_WAL_SYNC=always \
  anchordb:latest
```

#### Step 2: Execute Fraud Detection Scenarios via AnchorWire
```bash
# 1. Run Multi-Hop Mule Account Tracking
go run ./examples/banking_fraud/main.go -url=wire://localhost:7448 -token=banking_fraud_secret_token -scenario=mule-tracking

# 2. Run Impossible Travel Geolocation Anomaly Detection
go run ./examples/banking_fraud/main.go -url=wire://localhost:7448 -token=banking_fraud_secret_token -scenario=impossible-travel

# 3. Run 1,000,000 Scale Benchmark
go run ./examples/banking_fraud/main.go -url=wire://localhost:7448 -token=banking_fraud_secret_token -scenario=scale-benchmark -scale=1000000

# 4. Run All Scenarios Concurrently
go run ./examples/banking_fraud/main.go -url=wire://localhost:7448 -token=banking_fraud_secret_token -scenario=all -scale=1000000
```

---

## 6. Real-World Execution Results

### Scenario 1: Multi-Hop Mule Account Traversal Output
```text
>>> [SCENARIO 1] Real-Time Multi-Hop Mule Account & Smurfing Layering Detection
    Goal: Trace stolen funds flowing across 5 hops from victim to offshore cashout.
    SLA Requirement: Pre-Authorization decision in < 1 millisecond (< 1,000 µs).

    [✓] Ingested 12 bank accounts across 6 laundering layers.
    [✓] Linked 12 directed fund transfer edges across 5 relational hops.

>>> Executing Real-Time Pre-Authorization Multi-Hop Causal Inspection:
    [✓] 5-Hop Causal Chain Completed in 8.041µs (SUB-MILLISECOND IN-MEMORY LATENCY!)

┌──────────────────────────────┬──────────────┬────────────┬────────────┬──────────────────────────────┬──────────────┐
│ Downstream Account (Hop 5)   │ Path Prob.   │ Risk Score │ Watchlist? │ Pre-Authorization Decision   │ Decision Time│
├──────────────────────────────┼──────────────┼────────────┼────────────┼──────────────────────────────┼──────────────┤
│ acc_offshore_crypto_drain    │      83.17% │       99.5 │ YES (ALERT) │ BLOCK & FREEZE (AML VIOLATION) │      9.375µs │
│ acc_legitimate_merchant_safe │      26.53% │        2.0 │ NO         │ APPROVE                      │      9.207µs │
└──────────────────────────────┴──────────────┴────────────┴────────────┴──────────────────────────────┴──────────────┘
```

### Scenario 2: Impossible Travel Output
```text
>>> [SCENARIO 2] Impossible Travel & Geolocation Anomaly Detection (ATM / POS Fraud)
    Method: 2D Spatial Index Euclidean Distance / In-Storage Physical Velocity Check.

    [✓] Registered 3 ATM/POS physical coordinate nodes in 2D Spatial Index.
    ▶ Distance Evaluated in-storage: 670.5 km [Query Latency: 1.255µs]
    ▶ Time Elapsed between swipes:   15 minutes (0.25 hours)
    ▶ Required Travel Speed:         2694.0 km/h
    🚨 [FRAUD ALERT TRIGGERED]: IMPOSSIBLE TRAVEL ANOMALY (CARD CLONING SUSPECTED)
       Action: Transaction at Surabaya ATM REJECTED. Card automatically FROZEN.
```

### Scenario 3: 1,000,000 Accounts Benchmark Output
```text
>>> [SCENARIO 3] Enterprise High-Throughput Scale Benchmark (1,000,000 Accounts)
    Testing zero-copy mmap ingestion throughput and pre-authorization query latency at scale.

[1/3] Streaming 1,000,000 bank account records across 16 parallel workers...
      -> Ingested 250,000 / 1,000,000 accounts (151846 writes/sec) [Elapsed: 1s]
      -> Ingested 500,000 / 1,000,000 accounts (146648 writes/sec) [Elapsed: 3s]
      -> Ingested 750,000 / 1,000,000 accounts (151719 writes/sec) [Elapsed: 4s]
      -> Ingested 1,000,000 / 1,000,000 accounts (144532 writes/sec) [Elapsed: 6s]
    [✓] Ingestion Completed: 1,000,000 records in 6.918903675s (144532 writes/sec)

[2/3] Linking 100,000 transactional transfer edges across accounts...
    [✓] Linked 100,000 edges in 391.340939ms (255532 edges/sec)

[3/3] Benchmarking 50,000 Pre-Authorization Real-Time Fraud Inspections...
------------------------------------------------------------------------------------------
    [✓] Total Inspections Executed: 50,000 queries
    [✓] Inspection Throughput:      1435617 fraud checks / sec
    [✓] Average Latency:            696ns (SUB-MILLISECOND!)
    [✓] p50 Latency:                329ns
    [✓] p95 Latency:                937ns
    [✓] p99 Latency:                1.474µs
------------------------------------------------------------------------------------------
>>> VERDICT: AnchorDB meets the < 10ms Banking Payment Authorization SLA with 99.8% margin!
```

---

## 7. Comparative Summary: AnchorDB vs Neo4j in Banking

| Feature | AnchorDB | Neo4j |
|---|---|---|
| **Primary Placement** | **In-Line Payment Gateway (< 1 ms SLA)** | **Offline Crime Analytics & Forensic Intelligence** |
| **Traversal Latency** | **Sub-millisecond (0.001 - 0.025 ms)** | Single to triple-digit milliseconds (50 - 300 ms) |
| **Throughput** | **140,000+ writes/sec**, 1,400,000+ checks/sec | 10,000 - 40,000 writes/sec |
| **Runtime Footprint** | Native Go binary (~30 MB RAM), zero JVM GC | JVM Heap (requires multi-GB RAM tuning, GC pause spikes) |
| **Spatial Anomaly** | Native 2D Spatial Index & Euclidean velocity | Spatial plugin required (APOC / Neo4j Spatial) |
| **Data Science Algorithms**| None (Focused exclusively on real-time causality & math)| Full Graph Data Science (GDS) library (PageRank, Louvain) |
| **Query Mechanism** | Programmatic Fluent Go Driver / REST API | Declarative Cypher pattern matching language |
| **Storage Architecture**| "One Model = One File" (`banking_fraud.anchordb`) | Monolithic cluster store |
