# Product Requirements Document (PRD): Model Schema Registration & Strict Immutability Contract

| Document Metadata | Value |
| :--- | :--- |
| **Status** | Approved / In Design |
| **Feature Name** | Register Model (Immutable Strict Schema Contract with 1D Array Support) |
| **Target Subsystems** | `pkg/storage`, `pkg/driver`, `cmd/server` (REST API) |
| **Author** | AnchorDB Core Team |
| **Last Updated** | 2026-10-16 |

---

## 1. Executive Summary & Objective

Currently, AnchorDB operates as a dynamic attribute store where entities accept arbitrary numeric attributes (`map[string]float32`) without upfront schema declaration. 

This PRD introduces **Model Schema Registration** with **Strict Immutability**:
1. **One-Time Registration**: A model schema is registered **only once**. Once written, it is permanently locked into the model file header.
2. **Attach & Compare Validation**: When a service connects or attaches to a model, AnchorDB compares the incoming schema with the registered schema.
3. **Fail-Fast Rejection**: If the schema is incompatible or mismatched, AnchorDB **immediately rejects the attach/register request**, causing all subsequent operations (`insert`, `save`, `update`) to **fail fast**.
4. **Required vs. Optional Modifier**: Uses a clean suffix `?` syntax (e.g. `"age": "int"` is required, `"brand": "string?"` is optional).
5. **Array Support (1D)**: Native support for 1D arrays (`string[]`, `int[]`, `float[]`, `bool[]`, `date[]`) for tags, categorical labels, and high-dimensional vector embeddings, while intentionally keeping deeply nested objects out-of-scope to protect $O(1)$ memory-mapped compute speeds.
6. **Immutable Lifetime**: Once registered, a schema **cannot be modified, detached, or unmounted** until an explicit administrative **Hard Reset**.

---

## 2. Problem Statement

1. **Schema Drift across Microservices**: In distributed systems, different microservices or worker versions may attempt to write incompatible attribute names or conflicting types to the same model.
2. **Accidental Silent Corruption**: Without upfront contract verification, typos in property names (e.g. `"voltge"` instead of `"voltage"`) are silently accepted by the engine, corrupting subsequent in-storage math formulas and analytics.
3. **Mathematical & Embedding Inconsistency**: In vector similarity and geometric operations, if the dimensionality or attribute expectations change mid-lifecycle, vector calculations become mathematically invalid.

---

## 3. Product Requirements & Behavior

### 3.1. Schema Specification & Field Modifiers

A schema is a map of field names to type identifiers.

#### A. Required vs. Optional Syntax (`?` Suffix)
- **Required (Default)**: Any type without a trailing `?` is mandatory.
  - If a payload omitted a required field during `Save`/`Insert`, the engine immediately rejects the write with `ErrMissingRequiredField`.
- **Optional (`?`)**: A type ending with `?` is optional.
  - If omitted in a write payload, the record is stored without consuming unneeded byte slots in binary storage.

#### B. Supported Types (Primitives & 1D Arrays)

| Type Identifier | Description | Required Example | Optional Example | Internal Representation |
| :--- | :--- | :--- | :--- | :--- |
| **`string`** | Text label / identifier | `"color": "string"` | `"brand": "string?"` | Interned String Pool |
| **`int`** | Integer number | `"age": "int"` | `"doors": "int?"` | Exact integer (stored as float32) |
| **`float`** | Floating-point metric | `"speed": "float"` | `"weight": "float?"` | IEEE 754 float32 |
| **`bool`** | Boolean flag | `"active": "bool"` | `"verified": "bool?"` | 1.0 (true) or 0.0 (false) |
| **`date`** | Calendar date | `"created_date": "date"` | `"updated_date": "date?"` | Epoch Days (e.g. 20742) |
| **`string[]`** | Array of string tags / labels | `"roles": "string[]"` | `"tags": "string[]?"` | Length `uint16` + string pool offset array |
| **`float[]`** | Array of floats / Vector embedding | `"embedding": "float[]"` | `"history": "float[]?"` | Length `uint16` + contiguous IEEE 754 float32 array |
| **`int[]`** | Array of integers | `"codes": "int[]"` | `"ports": "int[]?"` | Length `uint16` + contiguous integer array |
| **`bool[]`** | Array of booleans | `"flags": "bool[]"` | `"mask": "bool[]?"` | Bitmask / byte slice |
| **`date[]`** | Array of dates | `"holidays": "date[]"` | `"schedule": "date[]?"` | Length `uint16` + contiguous epoch day array |

> [!NOTE]
> **Scope Boundary**: Deeply nested document structures (`array -> object -> array`) are intentionally out-of-scope. AnchorDB remains a high-performance computational database engine. 1D arrays preserve $O(1)$ memory-mapped traversal and fast vector math without adding recursive tree parsing overhead.

#### Example Model Registration Schema:
```json
{
  "model": "vehicles",
  "schema": {
    "color": "string",
    "age": "int",
    "speed": "float",
    "tags": "string[]?",
    "vector_embedding": "float[]",
    "registered_date": "date"
  }
}
```

---

### 3.2. Attach & Connection Validation (Bandingkan & Tolak)

Every client connecting or attaching to a model in AnchorDB executes schema verification:

```
                      Client Attaches with Schema
                                  │
                                  ▼
               ┌─────────────────────────────────────┐
               │  Does Model have Registered Schema? │
               └──────────────────┬──────────────────┘
                                  │
                  ┌───────────────┴───────────────┐
                  ▼                               ▼
                 NO                              YES
                  │                               │
        [First-Time Register]             [Compare Schemas]
                  │                               │
       Save Schema to File Header         ┌───────┴───────┐
           Set Status: LOCKED             ▼               ▼
                  │                    MATCH          MISMATCH
                  ▼                       │               │
            ALLOW ATTACH             ALLOW ATTACH      REJECT (Error)
                                                          │
                                                          ▼
                                                  ALL INSERTS / UPDATES
                                                   FAIL IMMEDIATELY
```

1. **If Model Has No Registered Schema**:
   The incoming schema is validated and saved as the model's canonical schema. The model is marked as `LOCKED`.
2. **If Model Already Has a Registered Schema**:
   AnchorDB performs an exact canonical comparison:
   - Field names, data types, array dimensions, and `?` modifiers must match.
   - **Case A: Identical**: Connection / attach succeeds.
   - **Case B: Incompatible / Mismatched**:
     - AnchorDB **REJECTS** the attach request immediately.
     - Returns error: `ErrSchemaMismatch: incoming schema incompatible with registered model contract`.
     - HTTP Status: `422 Unprocessable Entity` or `409 Conflict`.

---

### 3.3. Execution-Level Failure (Insert / Save / Update Fails Fast)

- If a client failed the schema attach check, it cannot acquire a valid session or driver instance.
- In-flight operations:
  - If an insert or save payload contains unknown fields not present in the registered schema, the call is rejected immediately.
  - If a required field is missing, the write is aborted before WAL logging.
  - If an array field receives a scalar or vice-versa, the call fails with a type validation error.

---

### 3.4. Lifetime & Reset Policy (Tidak Bisa Dicabut Sampai Reset)

- **Immutable State**: While the model exists, the schema cannot be detached, edited, or dynamically altered.
- **Hard Reset Only**: The only mechanism to remove or replace a registered schema is an explicit administrative Reset:
  - Requires elevated admin permissions (`AUTH_TOKEN` with admin role).
  - Explicit confirmation parameter: `confirm=true` and `action=drop_model`.
  - The model file and WAL are safely archived or purged, resetting the state to clean zero.

---

## 4. Technical Design Specifications

### 4.1. Binary Header Integration (`{model}.anchordb`)
AnchorDB's 64-byte file header will incorporate schema lock flags and a canonical schema hash:

| Byte Offset | Field Name | Type | Description |
| :---: | :--- | :---: | :--- |
| `0x18 - 0x1B` | `SchemaCRC32` | `uint32` | CRC32 checksum of the canonical sorted JSON schema |
| `0x1C` | `SchemaFlags` | `uint8` | Bit flags: `0x01` = `HAS_SCHEMA`, `0x02` = `SCHEMA_LOCKED` |
| `0x1D - 0x1F` | `Reserved` | `[3]byte` | Reserved for alignment |
| `0x20 - 0x27` | `SchemaOffset` | `uint64` | Byte offset in binary file where full JSON schema is stored |

---

## 5. API & SDK Interface Contracts

### 5.1. Go Driver SDK API

```go
// 1. Defining Schema in Application Code with Required & Optional Array Types
modelSchema := driver.Schema{
    "color":            "string",       // Required string
    "age":              "int",          // Required int
    "brand":            "string?",      // Optional string
    "tags":             "string[]?",    // Optional array of strings
    "vector_embedding": "float[]",      // Required float vector (e.g. 64-dim / 128-dim)
}

// 2. Connecting with Strict Schema Enforcement via Option
client, err := driver.ConnectRemote("http://localhost:7447",
    driver.WithToken("secure-token"),
    driver.WithModel("vehicles"),
    driver.WithAttachSchema(modelSchema),
)
if err != nil {
    // Fails here if registered schema in AnchorDB is different!
    log.Fatalf("Attach rejected by AnchorDB: %v", err)
}
defer client.Close()

// 3. Or Attaching Schema explicitly on active client:
err = client.Attach(modelSchema)

// 4. Or Spawning a model with attached schema directly:
vehicleClient, err := client.AttachModel("vehicles", modelSchema)
```

### 5.2. HTTP REST Endpoints

#### A. Attach Model Schema (`POST /v1/model/attach`)
**Request:**
```json
{
  "model": "vehicles",
  "schema": {
    "color": "string",
    "age": "int",
    "brand": "string?",
    "tags": "string[]?",
    "vector_embedding": "float[]"
  }
}
```
**Success Response (`200 OK` or `201 Created`):**
```json
{
  "success": true,
  "model": "vehicles",
  "status": "LOCKED",
  "schema_checksum": "a8f3b901",
  "message": "Model schema registered and locked successfully"
}
```
**Rejection Response on Incompatible Schema (`409 Conflict` / `422 Unprocessable Entity`):**
```json
{
  "success": false,
  "error": "schema_mismatch",
  "model": "vehicles",
  "message": "Incoming schema does not match registered model contract. Registration rejected.",
  "expected_schema": {
    "color": "string",
    "age": "int",
    "vector_embedding": "float[]"
  },
  "received_schema": {
    "color": "string",
    "age": "string",
    "vector_embedding": "float"
  }
}
```

#### B. Administrative Model Reset (`POST /v1/model/reset`)
```json
{
  "model": "vehicles",
  "confirm": true,
  "archive_before_reset": true
}
```

---

## 6. Acceptance Criteria

- [ ] **AC-1 (First Registration)**: Registering a model for the first time writes the schema, locks the model (`FLAG_SCHEMA_LOCKED`), and confirms success.
- [ ] **AC-2 (Identical Schema Attach)**: Attaching to an already registered model with the exact same schema succeeds without error.
- [ ] **AC-3 (Incompatible Schema Rejection)**: Attaching with missing fields, unexpected extra fields, modifier mismatches (`?`), or mismatched types is rejected with HTTP `409`/`422` and Go `ErrSchemaMismatch`.
- [ ] **AC-4 (Operation Block)**: A client that failed the schema verification cannot perform `Save`, `Insert`, or `Update`; calls immediately fail.
- [ ] **AC-5 (Array Validation)**: Writing to an array field validates slice length and item types; scalar to array or array to scalar fails validation.
- [ ] **AC-6 (Required vs Optional Enforcement)**: Omitting a required field fails write operations; omitting an optional field (`?`) succeeds without consuming unnecessary byte slots.
- [ ] **AC-7 (Lock Permanence)**: Attempting to update or detach the schema while the model exists returns an error; the schema persists across server restarts and crash recovery.
- [ ] **AC-8 (Administrative Reset)**: Only an authenticated call with `confirm=true` can reset a registered model.
