# Changelog

All notable changes to AnchorDB are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **WAL durability modes** (`SyncMode`): `always` (default, fsync per write),
  `interval` (background fsync), and `none` (OS-managed). Configurable via
  `storage.OpenWithOptions` / `storage.WALOptions`, and on the server via
  `-wal-sync` / `ANCHORDB_WAL_SYNC` and `-wal-sync-interval` /
  `ANCHORDB_WAL_SYNC_INTERVAL`. This removes the ~fsync-per-write throughput
  ceiling for deployments on slow media (e.g. SD cards) that accept a bounded
  durability window.
- **Per-client rate limiting** (opt-in): `-rate-limit` / `ANCHORDB_RATE_LIMIT`
  and `-rate-limit-burst` / `ANCHORDB_RATE_LIMIT_BURST`. Returns `429` with
  `Retry-After`; `/health` and `/metrics` are exempt.
- **`GET /metrics`**: dependency-free Prometheus text-format metrics (uptime,
  loaded models, draining state, goroutines, heap, GC, worker counters).
- **`/v1/list` result cap**: a request `limit` is clamped to `maxQueryLimit`
  (1000) so a client cannot request the entire dataset in one call.
- CI workflow (`.github/workflows/ci.yml`): gofmt, `go vet`, build,
  `go test -race`, and Linux ARM64/ARMv7/ARMv6 cross-compilation.
- `CHANGELOG.md`.

### Changed
- **`driver.Date` now returns `(float32, error)`** instead of panicking on
  malformed input. A new `driver.MustDate` preserves the panicking behaviour for
  hard-coded date literals. Update call sites that pass user input to handle the
  error, and literals to `MustDate`.

### Fixed
- **WAL edge-delete replay** now maintains the live-edge counter, so a database
  recovered from the WAL reports the correct `EdgeCount` after deletions.
- **Edge tombstones are compacted** when they dominate the live set, preventing
  unbounded memory growth across repeated add/remove cycles. `EdgeCount` is now
  O(1).
- **Interrupted-operation reconciliation failures are no longer ignored**: if the
  WAL write during crash reconciliation fails, `Open` returns an error instead of
  opening a half-durable database.
- **32-bit mmap guard**: database files larger than `MaxInt` are rejected with a
  clear error instead of silently truncating the mapping length.

## [0.1.0] - 2026-10-15

### Added
- Explicit causal edges (format version 3, optional `EDG3` section) with
  `AddEdge`/`RemoveEdge`/`OutgoingEdges`/`IncomingEdges`/`EdgeCount` and
  `ExecuteCausalStep`/`ExecuteCausalChain`.
- WAL opcodes for edge add/delete (`WalOpEdge`, `WalOpEdgeDelete`) and
  `ReplayEvents`.
- REST endpoints `POST /v1/edge` and `POST /v1/causal`; driver
  `Link`/`Unlink`/`Causal`/`CausalChain`.
- Lifecycle and persistence hardening (L1–L12): WAL truncation recovery,
  database lock, proxy trust, async shutdown ordering, and crash reconciliation.
