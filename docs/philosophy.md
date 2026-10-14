# Design Philosophy: Minimal Dependencies & Zero Bloat

> *"Do not chase zero dependencies dogmatically; instead, chase pragmatic, battle-tested, and high-performance minimal dependencies."*

---

## 1. Why Minimal Dependencies over Dogmatic Zero?

Many experimental projects adopt a dogmatic **"Zero Dependencies"** slogan (relying 100% on standard libraries). While well-intentioned, dogmatic zero-dependencies in high-performance production systems introduces severe pitfalls:

1. **Reinventing the Wheel**: Reimplementing complex algorithms (cryptography, wire protocols, cloud SDKs, high-performance compression) in-house frequently produces subtle edge-case bugs and security flaws.
2. **Maintenance Drag**: Internal codebases accumulate boilerplate utility functions that have already been battle-tested by millions of developers in well-maintained libraries.
3. **Performance Ceilings**: Refusing specialized libraries (such as optimized SIMD compression or native storage primitives) in the name of "zero dependencies" leaves real hardware throughput on the table.

Therefore, **AnchorDB adheres to a Pragmatic Minimal Dependencies Philosophy**:
We maintain zero framework bloat, zero heavy ORMs, and zero CGO, while remaining open to strictly-vetted, high-impact dependencies where they deliver concrete advantages in correctness, throughput, or security.

---

## 2. The 5 Golden Rules for Dependency Adoption

Every external dependency considered for AnchorDB must pass 5 strict criteria:

| # | Rule | Requirement |
|---|---|---|
| **1** | **Zero CGO** | Must be 100% pure Go. No C/C++ toolchains, dynamic linker dependencies, or cross-compilation friction. |
| **2** | **No Transitive Tree Explosion** | Dependencies bringing large transitive dependency graphs are strictly prohibited. Self-contained, isolated packages only. |
| **3** | **High Impact to LoC Ratio** | Must provide significant correctness, security, or performance advantages over a custom implementation. |
| **4** | **Production-Grade & Battle-Tested** | Actively maintained, high test coverage, proven in production deployments. |
| **5** | **Tiny Binary Impact** | Must preserve AnchorDB's tiny static binary size (< 15 MB) and rapid compilation (< 2–3s). |

---

## 3. Architecture Alignment

* **Core Storage Engine (Stdlib / Native Go First)**:
  * Memory Mapping: Linux kernel `mmap(MAP_SHARED)`.
  * Geometry & Arithmetic: In-register float32 Euclidean vector projections.
  * Concurrency: `sync.RWMutex` with constant-time bounded locks.
* **System & Memory Management**:
  * Soft Memory Limit: Go runtime `debug.SetMemoryLimit` paired with cgroups v1/v2 container introspection.
  * Timing-Safe Authentication: `crypto/subtle.ConstantTimeCompare`.
* **Pragmatic Minimal Dependencies Boundary**:
  * Cloud Object Sinks: AWS SDK v2 / MinIO client when advanced multipart upload is required.
  * Compression: High-throughput compression (e.g., `klauspost/compress`) for cold archival partitions.
  * Observability: Lightweight Prometheus / OpenTelemetry telemetry.
