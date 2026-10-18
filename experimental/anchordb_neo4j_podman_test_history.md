# AnchorDB vs. Neo4j on Podman — Experiment History

This is a **standalone, public-facing experiment log** for AnchorDB and Neo4j workloads run with Podman. It records what was tested and under which source revision, while keeping different transports, batch granularities, memory limits, and host conditions separate. It is an experiment record, not a general database ranking.

> **Historical evidence notice:** Entries summarize local experiments whose raw logs and harnesses are not published in this repository. The measurements cannot be independently reproduced from this repository and must not be treated as verified benchmarks of the current release. This history documents investigations that helped identify issues and track behavior across revisions; it does not imply that every issue listed was fixed or that a fix was independently verified. Memory values are `podman stats` snapshots unless explicitly labeled as peaks. `OOMKilled=true` means the container reached its own cgroup limit; it does not establish host-level OOM causality.

- **Log started:** 2026-10-18 (earlier 2026-10-17 experiments are included retroactively)
- **Project:** AnchorDB (`github.com/rachmanzz/anchordb`)
- **Format:** Markdown; documentation only, no source code.
- **Chronology:** older experiments first; append each new run at the end without replacing prior results.
- **Evidence rule:** distinguish client-reported success from independently verified counts. Do not infer host-level OOM from a container reaching its own cgroup limit.

## Entry format for future runs

```text
### YYYY-MM-DD — Short experiment title
- Source revision and image:
- Question/objective:
- Environment: host, Podman, cgroup limits, resident databases:
- Workload: API/transport, per-record or batch, workers, batch size, target:
- AnchorDB result:
- Neo4j result:
- Independent verification: entity/node/relationship counts, container status, OOM:
- Interpretation and comparability limits:
- Publicly available logs/artifacts:
```

For a database not exercised in a run, write “not tested”; do not copy results from another run. Record completion, timeout, errors, or manual interruption explicitly. Identify memory values as snapshots or peaks. State relevant differences such as `SaveBatch` versus Bolt `UNWIND`, HTTP versus AnchorWire versus Bolt, storage medium, and simultaneous host load. Never present a timed-out or interrupted workload as completed.

## Experiment history

### 2026-10-17 — Initial HTTP remote benchmark and driver investigation

- **Source revision:** `3f5ccd5`. AnchorDB was tested through its HTTP remote driver; Neo4j 5 Community through Bolt. AnchorDB's default HTTP transport was compared with a separately tuned HTTP client.
- **Objective:** test remote ingestion at 100k, 1M, and 5M records and compare per-record with Neo4j's batched `UNWIND` mode.
- **Workload:** 16 workers; 50k relationships and 20k traversal queries in the larger runs. AnchorDB's original example used per-record writes; Neo4j was tested per-record and in batches.
- **AnchorDB:** default driver reached about 3,224 writes/s at 100k. At 1M it reported completion, but an independent count found only **221,379/1,000,000** records. With a tuned HTTP transport, 1M records were verified at about **31,726 writes/s**, and 5M at about **31,215 writes/s**.
- **Neo4j:** batched `UNWIND` reached about **138,972 nodes/s** at 1M and **275,047 nodes/s** at 5M. Per-record mode reached about **4,993 nodes/s** at 1M.
- **Verification and limits:** AnchorDB counts were checked independently through `/v1/list`. The initial example's success-rate reporting was not reliable when workers encountered errors. AnchorDB relationship writes were per request, while Neo4j relationships were batched, so relationship throughput was not a like-for-like comparison.
- **Interpretation:** separates a client transport bottleneck from engine behavior and keeps per-record and batch results distinct.
- **Reproducible artifacts:** benchmark source and raw logs are not included in this repository. This summary is retained as historical data; do not treat it as a fresh run.

### 2026-10-17 — AnchorWire compared with HTTP

- **Source revision:** `b87fc7f`; AnchorDB ran over HTTP REST and AnchorWire, using the same product driver. Neo4j was a reference from separate runs, not the subject of this experiment.
- **Objective:** measure the effect of AnchorWire transport independently of API semantics.
- **Workload:** 16 workers, batches of 1,000, up to 5M entities. Test data was stored under `/tmp` on a tmpfs-backed filesystem.
- **AnchorDB:** at 1M, AnchorWire reached about **399,844 records/s** and HTTP **262,568/s**. At 5M, AnchorWire reached **337,763/s** and HTTP **219,242/s**. Entity counts were verified for the reported runs.
- **Memory observation:** an older multi-model run with Neo4j resident ended in an AnchorDB OOM kill (`137`, `OOMKilled=true`). A separate clean 5M single-model run peaked at about 5.04 GB and completed. These measurements predate later storage changes.
- **Limits:** this was primarily a Wire-versus-HTTP experiment, not a new head-to-head Neo4j benchmark. Host memory and swap pressure affected absolute throughput. A single Wire connection serializes requests; parallel ingestion used multiple clients/connections.
- **Reproducible artifacts:** benchmark source and raw logs are not included in this repository. Treat these values as historical measurements.

### 2026-10-17 — AnchorWire vs. Neo4j at 1M entities

- **Source revision:** `98dc93b`. AnchorDB AnchorWire (`:7448`) and Neo4j Bolt (`:7687`) were run with Podman on the same host and during the same test period.
- **Workload:** 1M entities, 50k relationships, 20k one-hop queries, 16 workers, batch size 1,000; Neo4j used batched `UNWIND`.
- **Results:** AnchorWire ingested at **262,174 records/s (3.81 s)**; Neo4j at **118,041 nodes/s (8.47 s)**. Relationship rates were 34,603/s and 27,237/s respectively; query latencies were 121.9 μs and 478.8 μs. Both sides stored 1M entities/nodes; Neo4j had 50k relationships; the benchmark reported no write failures.
- **Limits:** the host was under memory pressure, and Neo4j's point lookup was a cold lookup. These values describe this workload and environment, not a general claim that AnchorDB is faster.
- **Reproducible artifacts:** benchmark source and raw logs are not included in this repository. Treat these values as historical measurements.

### 2026-10-17 — Memory-pressure experiments, source `98dc93b`

- **Source revision:** `98dc93b`. Podman cgroup limits were used to constrain individual database containers rather than deliberately exhausting host memory.
- **Workload:** AnchorDB alone, target 2M entities under a 1 GiB limit; Neo4j at a 1.5 GiB limit; then AnchorDB at 400 MiB while Neo4j was resident at about 1.29 GB.
- **AnchorDB:** at 1 GiB, approximately **1,641,000/2,000,000** records had been saved when the container was OOM-killed (`ExitCode=137`, `OOMKilled=true`). WAL replay restored 1,642,000 records. AnchorDB was also OOM-killed under the 400 MiB limit while Neo4j was resident.
- **Neo4j:** completed 2M nodes and 50k relationships under a 1.5 GiB limit with `OOMKilled=false`; query latency increased from approximately 479 μs to 563 μs in the measured comparison.
- **Limits:** this was an older source revision. A cgroup-limited process being killed does not prove that the host kernel killed it because Neo4j consumed host RAM.
- **Reproducible artifacts:** benchmark source and raw logs are not included in this repository. Treat these values as historical measurements.

### 2026-10-18 — Original example and `SaveBatch` retest, source `f2a87e4`

- **Source revision:** `f2a87e4`; the AnchorDB image was built from that source. The original `supply_chain` example's per-record writes were recorded separately from the `SaveBatch` harness.
- **Per-record results:** the AnchorDB example stored 1M entities in about 134.39 s (7,441 writes/s) sequentially and 311.80 s (3,207 writes/s) concurrently. Neo4j per-record took about 204.64 s (4,887/s) and 255.20 s (3,919/s), respectively. Entity/node counts were checked. The original AnchorDB example's relationship results have caveats: it ignored `Link` errors and did not create the required hubs in that workload.
- **Batch results:** AnchorDB's official `driver.Client.SaveBatch` over AnchorWire was compared with Neo4j Bolt `UNWIND`, using 16 workers and 1,000 records per batch. Sequential: AnchorDB 1M in **7.46 s (134,100/s)**; Neo4j **12.21 s (81,885/s)**. Concurrent: AnchorDB **23.96 s (41,729/s)**; Neo4j **16.69 s (59,932/s)**.
- **Memory pressure without pacing:** with Neo4j resident and AnchorDB limited to 700 MiB, the 1.5M AnchorDB batch workload was OOM-killed around 564k SKU records; WAL replay restored 564,100 total entities.
- **Reverse pressure direction:** with AnchorDB resident and Neo4j limited to 1.5 GiB, Neo4j completed 1M nodes, 50k relationships, and 20k queries without OOM.
- **Limits:** relationship creation used a per-edge `Link` on AnchorDB versus batched `UNWIND` on Neo4j. Cgroup tests do not establish host-global OOM causality.
- **Reproducible artifacts:** benchmark harnesses and raw logs are not included in this repository. These results are historical, not reruns from the current source.

### 2026-10-18 — `SaveBatch` with memory negotiation, source `f2a87e4`

- **Source revision:** `f2a87e4`; `WithAdaptivePacing` was enabled on all AnchorWire clients. These results must not be combined with the non-paced batch measurements above.
- **Negotiated responses:** on an isolated run the server returned `Throttled` (host use 76%, batch 500, 15 ms interval). With Neo4j resident, it returned `Hold` (host use around 80–81%, batch 100, 100 ms interval).
- **Results:** isolated 1M ingestion completed in 17.72 s (56,433/s). With Neo4j resident and AnchorDB limited to 700 MiB, 200k completed in 15.22 s (13,143/s). A 1.5M target did not complete within 350 s; a subsequent count was about 766,300 total entities, and AnchorDB was not OOM-killed. With AnchorDB limited to 2 GiB and Neo4j resident, 1M completed in 137.69 s (7,263/s).
- **Limits:** an initial run was invalidated by lack of space on `/tmp` tmpfs; the reruns used disk-backed `/var/tmp`. The client ignored errors returned by the initial negotiation call; negotiation failure was not deliberately injected in this test.
- **Reproducible artifacts:** harness and raw logs are not included in this repository. Values are retained as historical measurements.

### 2026-10-18 — Pressure test without vs. with negotiation, source `aa764d2`

- **Source revision:** `aa764d2` (`perf(storage): zero-copy stringPool and streamed sync snapshots to eliminate container OOM`). AnchorDB was rebuilt from this source; image ID began `110a3d24a021`. The Go harness was also built with a module replacement pointing to the `aa764d2` source tree.
- **Shared conditions:** Neo4j resident with 1M SKU nodes and 50k relationships; AnchorDB cgroup limit 700 MiB and swap limit 700 MiB; AnchorWire `SaveBatch`, 16 workers, 1,000 records per request, target 1.5M SKU entities; no additional relationship/query workload. Each run used a fresh AnchorDB data directory and model.
- **Without negotiation:** all **1,500,000 SKU records succeeded with zero reported failures** in 326 s total (5m25.68s), about **4,606 records/s**. Independent count: 1,500,100 total entities including 100 hubs. AnchorDB remained alive, `OOMKilled=false`; a post-run `podman stats` snapshot showed 670.8 MB / 734 MB (91.39%).
- **With negotiation:** server returned `Hold` (host use 80%, batch 100, 100 ms interval; response `HeadroomBytes=171,433,984`). The client was manually stopped after 349 s (`rc=143`) before reaching the target. Independent count after the client stopped: **1,034,900 total entities**, approximately 1,034,800 SKU plus 100 hubs. AnchorDB remained alive, `OOMKilled=false`; a post-run snapshot showed 607.4 MB / 734 MB (82.75%).
- **Interpretation:** on this source and these single runs, unpaced ingestion completed the target but approached the cgroup limit. `Hold` showed lower post-run memory usage, at the cost of substantially lower throughput. This is not a statistical result or a guarantee that pacing prevents OOM. The paced run was manually interrupted before completion; it was not a natural timeout. Memory figures are snapshots, not continuous peak measurements. Neo4j was stopped after the test and exited with code 137 while `OOMKilled=false`; this is not recorded as an OOM event.
- **Reproducible artifacts:** harness, data directories, and raw logs are not included in this repository; these are local test artifacts and are not part of this public document.

### 2026-10-18 — Retest without vs. with negotiation, source `8d21d13`

- **Source revision:** `8d21d13` (`fix(wire/system): resolve 85% memory hold and restore full ingestion throughput`). Both `localhost/anchordb:8d21d13` and `localhost/anchordb:wire` / `:latest` referred to image ID `81666d5d1f48`. The harness was rebuilt against the checked-out `8d21d13` source.
- **Shared conditions:** the existing Neo4j 5.26.32 container was resident with 1M SKU nodes and 50k relationships; AnchorDB cgroup and swap limits were 700 MiB each. AnchorWire `SaveBatch`, 16 workers, batch size 1,000, target 1.5M SKU entities, no additional relationship/query workload. Each mode used a new data directory and model. Neo4j counts were checked before and after the pair of runs.
- **Without negotiation:** 1,500,000 SKU records saved, zero client-reported failures, in **24.73 s** (**60,656 records/s**). Independent count: 1,500,100 total entities including 100 hubs. AnchorDB remained running with `OOMKilled=false`; post-run memory snapshot: **655.7 MB / 734 MB** (about 89.3%).
- **With negotiation:** response was `Throttled` (`host_used=78%`, allocated batch 1,000, release interval 5 ms, headroom 731,682,696 B). The full 1,500,000 SKU target saved with zero client-reported failures in **26.26 s** (**57,123 records/s**). Independent count: 1,500,100 total entities including 100 hubs. AnchorDB remained running with `OOMKilled=false`; post-run memory snapshot: **634.8 MB / 734 MB** (about 86.5%).
- **Neo4j verification and cleanup:** before and after the AnchorDB runs, Neo4j had 1,000,000 SKU nodes and 50,000 relationships. Neo4j remained running without OOM during the test and was stopped afterward; it exited with code 0 and `OOMKilled=false`.
- **Interpretation:** in this single sequential pair, negotiation returned `Throttled`, the paced run completed the same target, and its throughput was about **5.8% lower** while its post-run memory snapshot was about **20.9 MB lower**. These snapshots are not peak measurements; the result does not establish a general throughput or memory guarantee. The 5 ms interval and response status differ materially from the `Hold` response in the `aa764d2` test, so the runs must remain separate.
- **Reproducible artifacts:** harness, data directories, and raw logs are not included in this repository; these are local test artifacts and are not part of this public document.

### 2026-10-18 — `supply_chain` example vs. Neo4j on Podman, source `8d21d13`

- **Source and image:** AnchorDB source `8d21d13`, image `localhost/anchordb:8d21d13` (image ID `81666d5d1f48`); Neo4j 5.26.32. The AnchorDB executable was built from `examples/supply_chain/main.go`; Neo4j used the existing per-record Bolt harness. The example's benchmark mode uses 16 concurrent goroutines on this 8-core host, and writes each entity with `client.Save`.
- **Sequential, 100k target:** AnchorDB completed 100,000 entities in **148.29 s (674 writes/s)**, reported 50,000 `Link` calls in 74.27 s, and ran 20,000 causal queries in 6.50 s. Independent count: 100,000 entities. Neo4j then completed 100,000 SKU nodes in **24.88 s (4,020 nodes/s)**, 50,000 relationships in 2.48 s, and 20,000 one-hop queries in 9.70 s. Independent count: 100,000 SKU nodes and 50,000 relationships. Both containers used 2 GiB cgroup and swap limits; both completed without OOM. Post-run memory snapshots were AnchorDB 90.05 MB / 2.147 GB and Neo4j 1.092 GB / 2.147 GB.
- **Started simultaneously, 100k target:** AnchorDB completed 100,000 entities in **166.23 s (602 writes/s)**; Neo4j completed 100,000 SKU nodes in **30.10 s (3,323 nodes/s)**, after cleaning its prior 100k-node test dataset in 3.86 s. Counts after completion were 100,000 AnchorDB entities and 100,000 Neo4j SKU nodes plus 50,000 relationships. Both had 2 GiB cgroup/swap limits and remained free of OOM. Post-run snapshots: AnchorDB 89.86 MB / 2.147 GB; Neo4j 1.152 GB / 2.147 GB.
- **1M sequential attempt:** the original example was also started at 1M entities, but the runner reached its 900-second execution cap while AnchorDB was still ingesting. A subsequent count found **607,706** entities; the container was still running with `OOMKilled=false`. The partial run was stopped and is not a completed 1M benchmark. The 100k runs above were used for the completed sequential and simultaneous comparisons.
- **Neo4j resident, AnchorDB cgroup-limited:** Neo4j was resident with 1M SKU nodes and 50k relationships. AnchorDB had a 700 MiB memory limit and equal swap limit; the example targeted 1.5M per-record writes. At the 600-second client time limit it had reached about **400,000** progress-reported writes; an independent count found **417,330** entities. AnchorDB remained running, `OOMKilled=false`; post-run memory snapshot was 182.6 MB / 734 MB. Neo4j remained at 1M/50k and `OOMKilled=false`. The partial result reflects the time cap and slow per-record path, not an OOM.
- **AnchorDB resident, Neo4j cgroup-limited:** AnchorDB was started from the same `8d21d13` image with a previously populated **1,500,100-entity `SaveBatch` dataset** (1.5M SKU plus 100 hubs); it was not populated by `supply_chain` example in this run. Neo4j used a fresh volume and a 1.5 GiB memory/swap limit. Neo4j completed 1M SKU nodes in **172.54 s (5,796 nodes/s)**, 50k relationships in 1.81 s, and 20k queries in 8.80 s. Independent count: 1M SKU nodes and 50k relationships. Neo4j stayed running with `OOMKilled=false`; snapshot was 1.196 GB / 1.611 GB. AnchorDB stayed running with `OOMKilled=false`; snapshot was 457.4 MB / 2.147 GB.
- **Comparability limits:** the AnchorDB example creates no 100 hub entities before its 50,000 `Link` attempts and ignores `Link` errors; therefore its edge count/query behavior cannot be compared fairly with Neo4j's successful hub-and-relationship workload. The normal comparison is per-record ingestion at a common 100k SKU target; Neo4j's simultaneous run also paid a cleanup cost. Pressure A was an interrupted 1.5M per-record workload, while pressure B used an already populated AnchorDB `SaveBatch` resident dataset, so the two pressure directions are not perfectly symmetric. Cgroup results do not establish host-level OOM causality.
- **Cleanup and artifacts:** all test containers were stopped without OOM; the original Neo4j data volume was left intact. New retest volumes were retained, not deleted. Logs and data are local under `/var/tmp/anchordb-neo4j-example-8d21d13/` and are not included in this repository.

## Cross-experiment notes

1. Results depend on source revision, transport (HTTP/Wire/Bolt), write granularity (per-record/batch), cgroup limits, storage medium, resident neighboring databases, and host load.
2. Do not compare per-record results with batched results as if they were equivalent workloads. A new pacing comparison should use the same source, image, dataset size, resident database, worker count, and batch size in both modes.
3. The older OOM result near 564k on `f2a87e4` and the 1.5M completion without pacing on `aa764d2` are from different source revisions and are not contradictory measurements of one unchanged build.
4. The `aa764d2` one-run comparison with `Hold` showed much slower ingestion and a lower post-run memory snapshot. The `8d21d13` one-run comparison with `Throttled` showed a smaller throughput reduction and a modestly lower snapshot. Repeat runs and continuous peak-memory/throughput measurements are needed before drawing broader conclusions.
