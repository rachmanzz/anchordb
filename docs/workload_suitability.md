# AnchorDB: Workload Suitability, Limitations & Anti-Patterns

This guide defines where **AnchorDB** excels, where it is not suited, which architectural anti-patterns to avoid, and how to implement hybrid architectures when dealing with rich text or mixed workloads.

---

## Architecture & Design Focus

AnchorDB is engineered from the ground up for high-throughput, low-latency, binary spatial-causal graph storage and in-storage arithmetic. 

Key design constraints:
1. **Numeric-Centric Attributes:** Entity records store scalar numeric attributes as `float32` values (`Props map[string]float32`).
2. **Dense Binary Layout:** Records, headers (64 bytes), nodes (40 bytes), and WAL entries are tightly packed in binary format and memory-mapped (`.anchordb`).
3. **Graph & Spatial Primitives:** Native 2D coordinates (`Point2D`), 4D role equilibrium vectors (`[4]float32`), and directed, weighted causal edges (`Link`, `CausalChain`).
4. **Model Isolation:** Strict single-model file separation (`{model}.anchordb`).

Understanding these foundations determines which workloads fit AnchorDB naturally and which should be delegated to other storage engines.

---

## Highly Suitable Workloads

AnchorDB delivers maximum performance and utility in the following workload profiles:

### 1. Semantic Word Graphs, Thesauri & Conceptual Networks
* **Example Use Case:** A WordNet-style synonym/antonym network, taxonomy tree, or psycholinguistic semantic network.
* **Why AnchorDB Excels:**
  * **Directed Weighted Edges:** Natural representation of relationships like synonyms, antonyms, hypernyms (*is-a*), and meronyms (*part-of*):
    ```
    "cold" --[synonym, weight: 0.95]--> "chilly"
    "cold" --[antonym, weight: -1.0]--> "hot"
    "robin" --[hypernym, weight: 1.0]--> "bird"
    ```
  * **Vector & Spatial Clustered Semantics:** Using `Coord` (`Point2D`) or `Roles` (`[4]float32`) to place words in continuous affective dimensions (e.g., *Valence, Arousal, Dominance*):
    * Spatial proximity calculations (`Distance`, `WithinRadius`, `Box2D`).
    * Directional Ray Casting (DDA) to discover terms aligned along a specific conceptual trajectory.
  * **High-Speed Graph Traversal:** Instant evaluation of *degrees of separation* or causal chains across millions of entities.

### 2. Spatial-Temporal IoT, Telemetry & Sensor Tracking
* **Example Use Case:** Drone fleets, maritime vessels, robotics, automated warehouse AGVs, and industrial sensor arrays.
* **Why AnchorDB Excels:**
  * Ingests high-frequency scalar telemetry (`temperature`, `voltage`, `pressure`, `speed`) with zero serialization overhead.
  * Native 2D bounding boxes (`Box2D`), radial distance filters (`WithinRadius`), polygon geofencing (`Polygon2D`), and geodesic math (`HaversineDistanceKM`).
  * In-storage arithmetic evaluated directly on raw binary slices without deserialization.

### 3. Causal Lineage, Dependency Graphs & Root Cause Analysis
* **Example Use Case:** Microservice failure cascading, distributed supply chain tracing, financial dependency tracking.
* **Why AnchorDB Excels:**
  * First-class causal graph operations (`Causal`, `CausalChain`) that traverse directed causal graphs in memory.
  * Microsecond traversal without recursive SQL joins or expensive Cypher query planners.

### 4. Embedded & Low-Resource Edge Nodes
* **Example Use Case:** On-premise industrial appliances, embedded Linux gateways, in-vehicle computing units.
* **Why AnchorDB Excels:**
  * Runs entirely in-process (`driver.OpenEmbedded`) or via a lightweight daemon listening on binary TCP port `7448` (`driver.ConnectWire`).
  * Bounded memory footprint, proactive memory admission control, and zero external runtime dependencies.

---

## Unsuitable Workloads

AnchorDB is **not** designed for the following workloads and should not be used as the primary database for them:

### 1. Conventional Text Dictionaries & Encyclopedias (e.g., Merriam-Webster, Oxford, Wikipedia)
* **What Fails:** If your application requires storing word definitions, multi-paragraph markdown explanations, phonetic spellings, rich HTML entries, or usage examples.
* **Why AnchorDB is Not Suited:**
  * AnchorDB entity properties (`Props`) store **`float32` numbers**, not text strings or document blobs.
  * There is no built-in Full-Text Search (FTS), inverted index, tokenizer, or BM25/TF-IDF text scoring engine.
* **Recommended Alternative:** PostgreSQL with `tsvector`/GIN indexes, SQLite with `FTS5`, or Elasticsearch / Meilisearch.

### 2. General-Purpose Document & JSON Catalogs
* **What Fails:** Storing arbitrary, deeply-nested JSON documents with dynamic keys, arrays of strings, nested customer profiles, or e-commerce product catalogs with rich descriptions.
* **Recommended Alternative:** MongoDB, PostgreSQL (JSONB), or Couchbase.

### 3. Relational Multi-Table OLTP with ACID Transactions Across Entities
* **What Fails:** Applications relying on SQL foreign keys with cascading updates, multi-entity two-phase commit transactions, or cross-model joins.
* **Why AnchorDB is Not Suited:** AnchorDB enforces single-model isolation (`1 Model = 1 File`). Each model operates on its own WAL and file lock.
* **Recommended Alternative:** PostgreSQL, MySQL, or CockroachDB.

### 4. Large Media & Unstructured Binary Blobs (BLOBs)
* **What Fails:** Storing images, video recordings, audio files, or PDF documents.
* **Recommended Alternative:** S3-compatible object stores (MinIO, AWS S3, Cloudflare R2).

---

## Anti-Patterns & Practices to Avoid

Avoid the following design anti-patterns when building on AnchorDB:

| Anti-Pattern | Why It Fails | Correct Approach |
| :--- | :--- | :--- |
| **Encoding Text into Numbers**<br>*(e.g., storing ASCII/UTF-8 byte values into `float32` properties)* | Destroys indexing, causes precision rounding errors in floating-point representations, wastes CPU cycles, and prevents efficient querying. | Store the text body in a document/relational database and use the entity `Name` (key) in AnchorDB for graph and spatial relations. |
| **Monolithic Model Dump**<br>*(Storing hundreds of unrelated domains in a single `default.anchordb` file)* | Degrades memory caching, creates write lock contention, and eliminates failure domain isolation. | Isolate domains into separate models (`fleet.anchordb`, `semantics.anchordb`, `supply_chain.anchordb`). |
| **Ingesting Unbounded Logs without Lifecycle Partitioning** | Causes memory-mapped files to grow indefinitely until filesystem exhaustion. | Enable automatic partitioning (`PartitionSchedule`) with retention cutoffs (`RetentionDays`) and cold storage offloading. |
| **Bypassing Gateway Mode in Multi-Tenant Environments** | Exposing raw AnchorDB instances to untrusted networks without rate limiting, connection pooling, or session key validation. | Deploy AnchorDB behind `anchor-connector` with `ANCHORDB_GATEWAY_MODE=true` to enforce structured session pairing and proxy routing. |
| **Using Heavyweight Client-Side Join Logic** | Fetching entire models over the network to manually compute barycenters, distances, or causal chains in client code. | Leverage native in-storage compute operators, ray-casting DDA beam searches, and server-side graph traversal primitives. |

---

## Recommended Hybrid Architecture: Intelligent Semantic Lexicon

When building a modern semantic dictionary, thesaurus, or AI knowledge base, the best approach is a **hybrid dual-engine architecture**:

```
                       [ Client / User Application ]
                                     |
               +---------------------+---------------------+
               |                                           |
               v                                           v
       [ Document Store ]                          [ AnchorDB Engine ]
   (PostgreSQL / SQLite / FTS)                  (via AnchorWire Binary TCP)
               |                                           |
     Stores Rich Text Data:                      Stores Semantic Graph:
     - Word: "resonance"                         - Entity: "resonance"
     - Definition: "The reinforcement..."        - Coordinates: Point2D{0.82, -0.45}
     - Pronunciation: "/ˈrɛzənəns/"             - Roles: [4]float32{0.9, 0.4, 0.1, 0.0}
     - Example sentences                         - Links:
     - Markdown formatted notes                    * --[synonym, 0.88]--> "vibration"
                                                   * --[causal, 0.95]--> "acoustic_feedback"
```

### Data Flow in the Hybrid Pattern
1. **Querying Semantic Context (AnchorDB):**
   * The client queries AnchorDB over AnchorWire port `7448` to find semantically related terms, calculate distance in concept space, or perform directional ray casting.
   * AnchorDB returns a list of matching word keys (e.g., `["vibration", "echo", "reverberation"]`) in sub-millisecond time.
2. **Retrieving Text Definitions (Document Store):**
   * The client fetches the full definitions, phonetics, and formatting for the returned keys using `WHERE word_id IN (...)` from PostgreSQL or SQLite.

This separation of concerns preserves AnchorDB's microsecond vector/graph performance while giving your application full rich-text capabilities.

---

## Workload Decision Matrix

| Workload Type | Suitability | Recommended Architecture |
| :--- | :---: | :--- |
| **Word Relations, Thesaurus & WordNet** | **Optimal** | Native AnchorDB (Nodes + Directed Weighted Links + 4D Role Vectors) |
| **Natural Language Dictionary (Definitions & Prose)** | **Unsuitable** | Use SQLite / PostgreSQL / Elasticsearch; or Hybrid with AnchorDB for relations |
| **IoT / Fleet Telemetry & Geofencing** | **Optimal** | Native AnchorDB (Spatial Grid + DDA Ray Casting + In-Storage Math) |
| **Industrial Machine Causal Lineage** | **Optimal** | Native AnchorDB (`Causal` and `CausalChain` directed edges) |
| **E-Commerce Shopping Cart & Checkout** | **Unsuitable** | Traditional ACID RDBMS (PostgreSQL, MariaDB) |
| **Vector-Assisted Semantic Traversal** | **Optimal** | Native AnchorDB (4D Role Vectors + DDA Beam Search) |
| **User Authentication & Blog Posts** | **Unsuitable** | Standard Relational / Document Store |
