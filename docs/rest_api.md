# REST API Reference

The AnchorDB HTTP server daemon operates on port `7447` by default. All API endpoints accept and return JSON payloads.

---

## 1. Authentication & Security

* **Bearer Token**: All endpoints under `/v1/*` require an `Authorization: Bearer <token>` header matching `ANCHORDB_AUTH_TOKEN`.
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
  "engine": "AnchorDB",
  "version": "3",
  "uptime_sec": 3600,
  "loaded_models": ["transactions", "analytics"]
}
```

### `GET /metrics`
Exports metrics formatted for Prometheus scrapers:
* `anchordb_uptime_seconds`
* `anchordb_goroutines`
* `anchordb_heap_alloc_bytes`
* `anchordb_worker_tasks_total`

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
  "status": "saved",
  "id": "vehicle_101"
}
```

### `POST /v1/get`
Retrieves an entity by name with optional in-storage computations and selective attribute projection.


**Request Payload**:
```json
{
  "model": "vehicles",
  "name": "vehicle_101",
  "select": ["distance", "duration", "speed"],
  "computations": {
    "speed": {
      "op": "divide",
      "field_a": "distance",
      "field_b": "duration"
    }
  }
}
```

### `POST /v1/list`
Queries records matching attribute ranges or date criteria with pagination.

> **Pagination Limit Cap**: The `limit` parameter is capped at `1000` (`maxQueryLimit`). Requests specifying a limit higher than 1000 are automatically clamped to 1000 to prevent unbounded memory allocation and denial of service.

**Request Payload**:
```json
{
  "model": "vehicles",
  "where": [
    {"field": "distance", "op": ">=", "val": 100}
  ],
  "page": 1,
  "limit": 20
}
```

### `POST /v1/update`
Executes in-place batch arithmetic mutations.

**Request Payload**:
```json
{
  "model": "vehicles",
  "where": [
    {"field": "distance", "op": "<", "val": 1000}
  ],
  "set": {
    "distance": {
      "op": "add",
      "field_a": "distance",
      "field_b": "10"
    }
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
  "limit": 5
}
```

### `POST /v1/graph`
Executes 2D spatial ray-projection queries.

**Request Payload**:
```json
{
  "model": "topology",
  "from": ["primary_cluster", "backup_cluster"],
  "relate": "route-down",
  "beam_width": 0.05,
  "top_k": 5
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
  "sink_path": "./data/partitions",
  "async": true
}
```

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

1. **Protocol Transport**: All engine communication utilizes HTTP/1.1 REST JSON. Persistent connections are maintained via standard HTTP Keep-Alive.
2. **Model Parameter**: Endpoints accept `"model"` in the request payload or `?model=` query string. If omitted, the engine routes requests to the `"default"` model file.
3. **Internal Partition Storage**: Partition archive files stored under `/data/partitions` are inaccessible directly over the network; they must be queried via `/v1/partition/inspect`.
4. **Schema Enforcement Fail-Fast**: When a model is locked, `POST /v1/save` and `POST /v1/update` return HTTP `400 Bad Request` if mandatory fields are omitted, if unknown fields are provided, or if data types conflict with the schema contract.

