# Background Subsystems: Scheduler & Worker Pool

AnchorDB contains built-in background management subsystems implemented entirely in pure Go, eliminating the need for external brokers (such as Redis, Kafka, or RabbitMQ) for task scheduling and asynchronous processing.

---

## 1. Cron Scheduler (`pkg/scheduler`)

The scheduler runs periodic maintenance jobs, such as automated partition archival and memory-mapped disk flushes.

### Wall-Clock Semantics
* **Monotonic vs. Wall-Clock**: Scheduled events (e.g. "run every day at 23:00") correspond to real-world wall-clock instants. The scheduler uses calendar arithmetic (`time.Date`) so that Daylight Saving Time (DST) shifts do not cause scheduled tasks to drift.
* **Clock Jump Resilience**: The scheduler sleeps in bounded slices (maximum 1 minute). When an NTP time synchronization or manual clock adjustment occurs, the scheduler detects the shift within one slice rather than being suspended inside an outdated timer.

### Single-Flight Overlap Prevention
If a task takes longer to execute than its scheduled interval:
* The scheduler detects the active run using atomic guards.
* It skips the overlapping execution and increments the `skipped_runs` metric instead of spawning concurrent, conflicting instances.

### Supported Schedule Formats

| Format | Example | Behavior |
| :--- | :--- | :--- |
| `HH:MM` | `23:00` | Runs daily at the specified local time. |
| `M H * * *` | `0 23 * * *` | 5-field cron notation (daily fixed time). |
| `@daily` | `@daily` | Equivalent to `00:00` every day. |
| `@hourly` | `@hourly` | Triggers at the start of every hour. |
| Duration | `5m`, `@every 30m` | Recurring interval. |

### API Controls
* **Status**: `GET /v1/scheduler/status` returns active schedules, last status, last run duration, and next scheduled fire time.
* **On-Demand Run**: `POST /v1/scheduler/run?task=<name>` triggers a task immediately without modifying its recurring schedule.

---

## 2. Asynchronous Worker Pool (`pkg/worker`)

The worker pool provides bounded, non-blocking asynchronous task execution for heavy workloads, such as S3 uploads and database compaction.

```
       Task Dispatch (e.g. POST /v1/partition/run?async=true)
                          |
                          v
               +----------------------+
               |  Task Admission Gate |  <-- Checks Memory Limits
               +----------+-----------+
                          |
                          v
               +----------------------+
               |  Bounded Channel     |  <-- Size: ANCHORDB_WORKER_QUEUE_SIZE
               |  (FIFO Task Queue)   |      (Fails fast if saturated)
               +----------+-----------+
                          |
         +----------------+----------------+
         |                |                |
         v                v                v
    [Worker 1]       [Worker 2]       [Worker N]  <-- Pool Size: CPU * 2
```

### Key Capabilities
1. **Bounded Queue & Fast Failure**: Tasks are queued in a bounded Go channel. If the queue fills up, new submissions fail fast with `ErrQueueFull` (`HTTP 503` or `429`) rather than consuming unbounded memory.
2. **Fixed Concurrency**: Limits concurrent worker goroutines (`ANCHORDB_WORKER_CONCURRENCY`, default `CPU * 2`) to prevent CPU exhaustion.
3. **Memory Pressure Admission**: When enabled (`ANCHORDB_WORKER_ENFORCE_MEMORY=true`), task submissions are rejected if total memory usage exceeds the configured threshold (`ANCHORDB_WORKER_MEMORY_THRESHOLD`, e.g., 85%).
4. **Scoped Task Cancellation**: Tasks can be canceled before execution by Job ID (`job_id`), Target Model (`model`), or Global Drain (`{"all": true}`).
5. **Panic Recovery**: Worker loops isolate panics in user tasks or completion callbacks, converting failures to logged error events without crashing the pool.

### Worker Status & Inspection
Use `GET /v1/worker/status` to monitor the pool:
```json
{
  "concurrency": 8,
  "queue_size": 1000,
  "queue_depth": 3,
  "active_workers": 2,
  "total_dispatched": 1420,
  "total_completed": 1415,
  "total_failed": 3,
  "total_retried": 5,
  "memory_pressure_active": false
}
```
