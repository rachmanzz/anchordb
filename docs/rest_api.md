# REST API Reference

For application reads, writes, schema, and graph operations, start with the [AnchorWire guide](anchorwire.md). This page documents the HTTP transport and server administration endpoints.

The AnchorDB HTTP server daemon operates on port `7447` by default. `/v1/*` endpoints use JSON; `/metrics` returns Prometheus text.

---

## 1. Authentication & Security

* **Bearer Token**: All endpoints under `/v1/*` require an `Authorization: Bearer <token>` header matching `ANCHORDB_AUTH_TOKEN`.
* **Gateway Mode (`-gateway-mode` / `ANCHORDB_GATEWAY_MODE=true`)**: When enabled, direct HTTP `/v1/*` routes return `403 Forbidden` (`gateway_mode_enforced`). All application mutations and queries must route through the `anchor-connector` gateway over AnchorWire.
* **Insecure Mode**: Set `ANCHORDB_ALLOW_INSECURE=true` to disable authentication for local testing.
* **Public Endpoints**: `/health` (liveness check) and `/metrics` (Prometheus) are unauthenticated.
* **Rate Limiting**: Configurable request rate per client IP with standard HTTP `429 Too Many Requests` responses.
* **IP Whitelisting**: Restricts traffic to defined IP addresses or CIDR blocks (`ANCHORDB_IP_WHITELIST`).
* **Reverse Proxy Trust**: When running behind Nginx, Caddy, or an AWS ALB, set `ANCHORDB_TRUST_PROXY=true` to parse client IPs from `X-Forwarded-For`.

---

## 2. Telemetry & Monitoring

### `GET /health`
Returns server operational health. Unauthenticated requests receive basic status; authenticated requests receive full memory, scheduler, and worker metrics.

```json
{
  "status": "ok",
  "engine": "AnchorDB"
}
```

With a valid bearer token, the response also includes `uptime_seconds`, `port`, `loaded_models`, `memory`, and `background`.

### `GET /metrics`
Exports metrics formatted for Prometheus scrapers:
* `anchordb_uptime_seconds`
* `anchordb_go_goroutines`
* `anchordb_heap_alloc_bytes`
* `anchordb_worker_jobs_dispatched_total`
* `anchordb_worker_jobs_completed_total`
* `anchordb_worker_jobs_failed_total`

---

## 3. Data & Mutation Endpoints

### `POST /v1/save`
Stores an entity with key-value properties.

**Request Payload**:
```json
{
  "model": "vehicles",
  "name": "vehicle_101",
  "props": {
    "distance": 150.0,
    "duration": 2.5,
    "start_date": 20740.0
  }
}
```

**Response**:
```json
{
  "success": true,
  "name": "vehicle_101",
  "operation": 1,
  "message": "record saved"
}
```

### `POST /v1/get` & `GET /v1/record?name={name}&model={model}`
Retrieves an entity by name with optional in-storage computations and selective attribute projection. Supports **Transparent Tiered Storage**: if the record has been pruned from the active Hot DB, AnchorDB transparently retrieves it from the Cold Tier (local or S3 partition) via the Partition Catalog.

**Request Payload (`POST /v1/get`)**:
```json
{
  "model": "vehicles",
  "name": "vehicle_101",
  "select": ["distance", "duration", "speed"],
  "computations": {
    "speed": "distance / duration"
  }
}
```

**Response**:
```json
{
  "success": true,
  "name": "vehicle_101",
  "operation": 1,
  "props": {
    "distance": 120.0,
    "duration": 2.0,
    "speed": 60.0
  },
  "tier": "hot"
}
```
*(If fetched from cold S3 or disk partition, `"tier"` will be `"cold"`)*.

### `POST /v1/list`
Queries records matching attribute ranges or date criteria with pagination.

> **Pagination Limit Cap**: The `limit` parameter is capped at `1000` (`maxQueryLimit`). Requests specifying a limit higher than 1000 are automatically clamped to 1000 to prevent unbounded memory allocation and denial of service.

**Request Payload**:
```json
{
  "model": "vehicles",
  "filters": [
    {"field": "distance", "operator": ">=", "value": "100"}
  ],
  "page": 1,
  "limit": 20
}
```

### `POST /v1/update`
Executes in-place batch arithmetic mutations. The batch is not transactional: if a later record fails, earlier updates remain committed.

**Request Payload**:
```json
{
  "model": "vehicles",
  "filters": [
    {"field": "distance", "operator": "<", "value": "1000"}
  ],
  "mutations": {
    "distance": "distance + 10"
  },
  "limit": 500
}
```

---

## 4. Graph & Causal Endpoints

### `POST /v1/edge`
Registers or unlinks a directed causal edge.

**Request Payload**:
```json
{
  "model": "events",
  "source": "fuel_shortage",
  "relation": "causes",
  "target": "inflation",
  "weight": 0.9,
  "action": "add"
}
```

### `POST /v1/causal`
Queries causal connections from seed nodes.

**Request Payload**:
```json
{
  "model": "events",
  "seeds": ["fuel_shortage"],
  "relations": ["causes", "causes"],
  "limit_k": 5
}
```

### `POST /v1/graph`
Executes 2D spatial ray-projection queries.

**Request Payload**:
```json
{
  "model": "topology",
  "inputs": ["primary_cluster", "backup_cluster"],
  "relation": "route-down",
  "beam_width": 0.05,
  "limit_k": 5
}
```

### `POST /v1/math`
Executes spatial graph math operations and in-storage statistical computations.

Supported `op` values:
- `distance`: Euclidean distance between two spatial nodes (`words: ["nodeA", "nodeB"]`).
- `centroid`: Centroid coordinate and semantic role vector of one or more spatial nodes (`words: ["nodeA", "nodeB", ...]`).
- `cosine_similarity`: Cosine similarity between 4D semantic role vectors of two nodes (`words: ["nodeA", "nodeB"]`).
- `nearest`: Nearest neighbor search for a given spatial node (`words: ["nodeA"]`, `limit_k: 5`).
- `haversine` *(Next release)*: Great-circle distance in kilometers using the WGS84 spherical Earth model (R = 6371.0 km, where X = Longitude, Y = Latitude) between two spatial nodes (`words: ["nodeA", "nodeB"]`).
- `stats` *(Next release)*: Single-pass statistical aggregates (`count`, `sum`, `min`, `max`, `mean`, `variance`, `std_dev`) for a specified numeric field using Welford's algorithm (`field: "fieldname"`).

> [!NOTE]
> The `haversine` and `stats` operations on `POST /v1/math` are scheduled for the **next release**.

**Request Payload (Haversine Distance - Next Release)**:
```json
{
  "model": "airports",
  "op": "haversine",
  "words": ["airport_jfk", "airport_lhr"]
}
```

**Response**:
```json
{
  "success": true,
  "distance_km": 5554.82
}
```

**Request Payload (In-Storage Statistics - Next Release)**:
```json
{
  "model": "telemetry",
  "op": "stats",
  "field": "speed"
}
```

**Response**:
```json
{
  "success": true,
  "stats": {
    "field": "speed",
    "count": 10000,
    "sum": 650000.0,
    "min": 15.2,
    "max": 145.8,
    "mean": 65.0,
    "variance": 125.4,
    "std_dev": 11.1982
  }
}
```

---

## 5. Maintenance & Lifecycle Endpoints

### `GET /v1/sync?model={name}`
Flushes uncommitted memory-mapped modifications to disk. Adding `?async=true` offloads the operation to the worker pool.

### `POST /v1/partition/run`
Archives historical records older than the retention threshold.

**Request Payload**:
```json
{
  "model": "transactions",
  "retention_days": 180,
  "date_field": "date",
  "sink_type": "local",
  "target_path": "./data/partitions",
  "async": true
}
```

For a local sink, `target_path` must resolve inside the configured data directory; omit it to use the default `partitions` subdirectory.

### `GET /v1/partition/list`
Lists available archived partition files in the partition storage directory.

**Response**:
```json
{
  "success": true,
  "partitions": [
    {
      "name": "2026-10-16_transactions_part1.anchordb",
      "size_bytes": 145020,
      "mod_time": "2026-10-16T12:00:00Z"
    }
  ]
}
```

### `POST /v1/partition/inspect`
Safely inspects records inside an archived partition file without exposing raw storage files to clients.

**Request Payload (Inspect Record)**:
```json
{
  "partition_file": "2026-10-16_transactions_part1.anchordb",
  "name": "tx_old_1092"
}
```

**Response**:
```json
{
  "success": true,
  "partition_file": "2026-10-16_transactions_part1.anchordb",
  "name": "tx_old_1092",
  "props": {
    "amount": 250.0,
    "date": 20580.0
  }
}
```

**Request Payload (List Archived Records)**:
```json
{
  "partition_file": "2026-10-16_transactions_part1.anchordb",
  "limit": 10
}
```

### `POST /v1/partition/s3/list`
Lists `.anchordb` partition files directly from an S3 bucket or prefix.

**Request Payload**:
```json
{
  "endpoint": "https://s3.us-east-1.amazonaws.com",
  "bucket": "my-archive-bucket",
  "prefix": "anchordb/partitions/"
}
```

**Response**:
```json
{
  "success": true,
  "objects": [
    {
      "key": "anchordb/partitions/2026-10-16_transactions_part1.anchordb",
      "size_bytes": 145020,
      "mod_time": "2026-10-16T12:00:00Z"
    }
  ]
}
```

### `POST /v1/partition/s3/inspect`
Inspects records inside an archived partition stored in S3 directly in-memory without downloading to local disk storage.

**Request Payload (Inspect Record)**:
```json
{
  "endpoint": "https://s3.us-east-1.amazonaws.com",
  "bucket": "my-archive-bucket",
  "partition_file": "anchordb/partitions/2026-10-16_transactions_part1.anchordb",
  "name": "tx_old_1092"
}
```

**Response**:
```json
{
  "success": true,
  "partition_file": "anchordb/partitions/2026-10-16_transactions_part1.anchordb",
  "name": "tx_old_1092",
  "props": {
    "amount": 250.0,
    "date": 20580.0
  }
}
```

### `GET /v1/partition/catalog?model={name}`
Retrieves the manifest of registered partition files and their date ranges.

**Response**:
```json
{
  "success": true,
  "model": "transactions",
  "items": [
    {
      "partition_file": "2026-10-16_transactions_part1.anchordb",
      "model": "transactions",
      "record_count": 500,
      "min_date": 20400.0,
      "max_date": 20580.0,
      "created_at": "2026-10-16T12:00:00Z",
      "size_bytes": 145020
    }
  ]
}
```

### `POST /v1/partition/catalog/record`
Discovers which partition file contains a specific historical record.

**Request Payload**:
```json
{
  "model": "transactions",
  "name": "tx_old_1092"
}
```

**Response**:
```json
{
  "success": true,
  "name": "tx_old_1092",
  "model": "transactions",
  "partition_file": "2026-10-16_transactions_part1.anchordb"
}
```

### `POST /v1/collector/run`
Purges records currently in the Tombstone state (`operation = 2`). Supports `"async": true`.

### `POST /v1/worker/cancel`
Cancels queued jobs before execution:
```json
{
  "job_id": "a974c021-..."
}
```

---

## 6. Model Schema Management Endpoints

AnchorDB provides strict contract immutability endpoints for model schema registration, validation, and administrative resetting.

### `POST /v1/model/attach`
Attaches and permanently locks a schema contract into a model. If the model already has a locked schema, compares incoming fields, types, and optional (`?`) modifiers.

**Request Payload**:
```json
{
  "model": "vehicles",
  "schema": {
    "voltage": "float",
    "status": "int",
    "is_active": "bool?",
    "calibrated": "date?"
  }
}
```

**Success Response (`200 OK`)**:
```json
{
  "success": true,
  "model": "vehicles",
  "status": "LOCKED",
  "message": "model schema attached and locked successfully"
}
```

**Rejection on Incompatible Schema (`409 Conflict`)**:
```json
{
  "success": false,
  "error": "schema_mismatch",
  "model": "vehicles",
  "message": "schema mismatch: incoming schema does not match registered model contract: field \"voltage\" mismatch (expected float, got int)"
}
```

### `GET /v1/model/schema`
Retrieves the locked schema contract for a specified model.

**Query Parameters**:
* `model`: Target model name (default: `"default"`).

**Response (`200 OK`)**:
```json
{
  "success": true,
  "model": "vehicles",
  "locked": true,
  "schema": {
    "voltage": "float",
    "status": "int",
    "is_active": "bool?",
    "calibrated": "date?"
  }
}
```

### `POST /v1/model/reset`
Administrative hard reset that unmounts and deletes the model's locked schema contract. Requires explicit confirmation.

**Request Payload**:
```json
{
  "model": "vehicles",
  "confirm": true
}
```

**Response (`200 OK`)**:
```json
{
  "success": true,
  "model": "vehicles",
  "message": "model schema reset successfully"
}
```

---

## 7. Design Boundaries & Limitations

1. **Protocol Transport**: The REST API uses HTTP and JSON. AnchorWire is a separate binary TCP protocol.
2. **Model Parameter**: JSON endpoints read `"model"` from the request body; endpoints documented with `?model=` read it from the query string. If omitted, the engine routes requests to the `"default"` model file.
3. **Internal Partition Storage**: Partition archive files stored under `/data/partitions` are inaccessible directly over the network; they must be queried via `/v1/partition/inspect`.
4. **Schema Enforcement Fail-Fast**: When a model is locked, `POST /v1/save` and `POST /v1/update` return HTTP `400 Bad Request` if mandatory fields are omitted, if unknown fields are provided, or if data types conflict with the schema contract.

