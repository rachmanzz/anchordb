# Model Schema Registration & Strict Immutability Specification

AnchorDB provides an immutable schema contract system called **Register Model** (or **Schema Attachment** in client SDKs). It enforces strict attribute names, scalar numeric/temporal data types, and optionality constraints per model database to prevent schema drift, accidental property typos, and data corruption across distributed services.

---

## 1. Core Principles

1. **One-Time Registration**: A schema contract is declared and permanently locked into the model file upon first registration. Once locked, the schema contract is permanent.
2. **Attach & Connect Verification**: Whenever a service connects (`ConnectRemote`) or attaches (`Attach`) to a model, the engine validates the incoming schema against the model's locked contract.
3. **Fail-Fast Rejection**: If the schema is mismatched or incompatible, the engine **immediately rejects** the attach or connection attempt with `ErrSchemaMismatch` (HTTP 409 Conflict).
4. **Execution-Level Enforcement**: Write operations (`Save`, `SaveAutoID`, `Update`) are validated in-memory before reaching the WAL:
   - Omitting a required attribute fails immediately with `ErrMissingRequiredField`.
   - Supplying an undeclared attribute fails with `ErrUnknownField`.
   - Supplying an invalid data type fails with `ErrInvalidFieldType`.
5. **Administrative Hard Reset**: The schema contract cannot be detached or altered dynamically. The only way to remove or replace a schema is through an explicit administrative reset (`confirm: true`).

---

## 2. Supported Data Types & Modifiers

A schema in AnchorDB is a dictionary mapping attribute names to normalized scalar type identifiers backed by `float32` property storage.

### A. Required vs. Optional Syntax (`?` Suffix)

* **Required (Default)**: Any type specification without a trailing `?` is strictly required. All write payloads must supply this attribute.
* **Optional (`?` Modifier)**: Any type ending with `?` is optional. If omitted in a write payload, the attribute is omitted without reserving unused space.

### B. Scalar Type Table

| Type Identifier | Description | Required Example | Optional Example | Internal Representation |
| :--- | :--- | :--- | :--- | :--- |
| **`int`** | Exact integer number | `"age": "int"` | `"doors": "int?"` | Exact integer (IEEE 754 float32) |
| **`float`** | Floating-point metric | `"speed": "float"` | `"weight": "float?"` | IEEE 754 float32 |
| **`bool`** | Boolean flag (`0.0` or `1.0`) | `"active": "bool"` | `"verified": "bool?"` | Float scalar `1.0` (true) / `0.0` (false) |
| **`date`** | Calendar date (`YYYY-MM-DD`) | `"registered": "date"` | `"updated": "date?"` | Non-negative integer days since Unix epoch (float32) |

> [!NOTE]
> **Design Boundary: Contiguous Scalar Float32 Properties**: Record attributes in AnchorDB are stored in contiguous `map[string]float32` structures (`CompactRecord`) to support zero-copy memory-mapped operations and sub-microsecond in-storage arithmetic. Non-numeric types (`string`) and array modifiers (`[]`) are rejected at schema parse time.

---

## 3. Schema Declaration Example

### JSON Format (REST API)
```json
{
  "model": "vehicles",
  "schema": {
    "age": "int",
    "speed": "float",
    "active": "bool?",
    "manufactured_date": "date"
  }
}
```

### Go Driver Format (`pkg/driver`)
```go
schema := driver.Schema{
    "age":               "int",   // Required int
    "speed":             "float", // Required float
    "active":            "bool?", // Optional bool
    "manufactured_date": "date",  // Required date (epoch days)
}
```

---

## 4. Lifecycle & Verification Architecture

```
                       Client Attaches / Connects with Schema
                                         │
                                         ▼
                      ┌─────────────────────────────────────┐
                      │ Does Model Have a Locked Schema?    │
                      └──────────────────┬──────────────────┘
                                         │
                         ┌───────────────┴───────────────┐
                         ▼                               ▼
                        NO                              YES
                         │                               │
               [First-Time Register]             [Compare Schemas]
                         │                               │
              Save Schema to Sidecar             ┌───────┴───────┐
                Set Status: LOCKED               ▼               ▼
                         │                     MATCH          MISMATCH
                         ▼                       │               │
                   ALLOW ATTACH             ALLOW ATTACH     REJECT (Error)
                                                                 │
                                                                 ▼
                                                        HTTP 409 Conflict
                                                        ErrSchemaMismatch
                                                      All Mutations Blocked
```

### 1. First-Time Registration
If the model file does not yet have a schema contract:
- The incoming schema definition is validated for syntax and type correctness.
- The schema is written atomically (`fsync` + rename + directory sync) to disk alongside the model database file as `{model}.anchordb.schema`.
- The model state is locked (`LOCKED`). Subsequent modifications are blocked.

### 2. Verification on Connect / Attach
If the model already possesses a locked schema contract:
- AnchorDB executes a canonical equality comparison across:
  - Total field count
  - Field names
  - Base data types (`int`, `float`, `bool`, `date`)
  - Optionality modifiers (`?`)
- **Matching Schemas**: The attach call returns successfully.
- **Mismatched Schemas**: The engine rejects the request immediately with `ErrSchemaMismatch` (`HTTP 409 Conflict`), halting client startup before any corrupted writes can occur.

### 3. Fail-Fast Mutation Errors
When writing (`Save`, `SaveAutoID`, `Update`) to a model with a locked schema:

| Error | Condition | HTTP Status |
| :--- | :--- | :---: |
| `ErrMissingRequiredField` | A field declared without `?` was omitted in payload | `400 Bad Request` |
| `ErrUnknownField` | A property was passed that does not exist in the schema | `400 Bad Request` |
| `ErrInvalidFieldType` | An integer/date received a fractional float, or bool received non-binary values | `400 Bad Request` |
| `ErrSchemaMismatch` | Client attempted to attach or connect with conflicting contract | `409 Conflict` |

---

## 5. Go Driver SDK Integration

### A. Attaching Schema to an Active Model
```go
client, err := driver.ConnectRemote("http://localhost:7447", driver.WithToken("token"))
if err != nil {
    log.Fatal(err)
}
defer client.Close()

// Attach and lock schema to active model
err = client.Attach(schema)
if err != nil {
    if errors.Is(err, driver.ErrSchemaMismatch) {
        log.Fatalf("Model schema mismatch! Local contract differs from server contract: %v", err)
    }
    log.Fatal(err)
}
```

### B. Enforcing Schema during Connection
```go
// ConnectRemote will fail at connection time if the remote model contract does not match
client, err := driver.ConnectRemote("http://localhost:7447",
    driver.WithToken("token"),
    driver.WithModel("vehicles"),
    driver.WithAttachSchema(schema),
)
if err != nil {
    log.Fatalf("Connection rejected due to schema conflict: %v", err)
}
```

### C. Spawning a Target Model with Attached Schema
```go
// Spawns a model client and attaches the schema immediately
deviceClient, err := client.AttachModel("devices_zone_1", schema)
if err != nil {
    log.Fatal(err)
}
```

### D. Inspecting the Locked Schema
```go
lockedSchema, err := client.GetSchema(ctx)
if err != nil {
    log.Fatalf("No schema registered on model: %v", err)
}
fmt.Println("Locked Schema:", lockedSchema)
```

### E. Administrative Hard Reset
```go
// Unmounts and purges the schema contract (requires confirm: true)
err = client.ResetModel(ctx, true)
if err != nil {
    log.Fatalf("Reset failed: %v", err)
}
```

---

## 6. REST API Endpoints

### 1. `POST /v1/model/attach`
Locks a schema into a model or verifies an existing contract.

**Request Payload**:
```json
{
  "model": "vehicles",
  "schema": {
    "speed": "float",
    "age": "int",
    "active": "bool?",
    "manufactured_date": "date?"
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

**Rejection Response (`409 Conflict`)**:
```json
{
  "success": false,
  "error": "schema_mismatch",
  "model": "vehicles",
  "message": "schema mismatch: incoming schema does not match registered model contract: field \"age\" mismatch (expected int, got float)"
}
```

---

### 2. `GET /v1/model/schema`
Fetches the registered schema contract for a given model.

**Query Parameters**:
* `model`: Target model name (e.g. `?model=vehicles`).

**Response (`200 OK`)**:
```json
{
  "success": true,
  "model": "vehicles",
  "locked": true,
  "schema": {
    "speed": "float",
    "age": "int",
    "active": "bool?",
    "manufactured_date": "date?"
  }
}
```

---

### 3. `POST /v1/model/reset`
Administrative hard reset to unmount the schema.

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
