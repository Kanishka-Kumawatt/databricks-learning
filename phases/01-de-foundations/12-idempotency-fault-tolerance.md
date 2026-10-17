# 1.12 Idempotency, Fault Tolerance, Scalability and Reliability

**Time:** 40 min · **Prerequisites:** 1.6, 1.7, 1.11

**After this topic you can:**
- define idempotency precisely and implement it four ways;
- explain at-most-once, at-least-once and exactly-once;
- explain how Spark, Delta and the orchestrator each provide fault tolerance;
- explain what scales horizontally and what doesn't;
- talk about reliability in terms of SLIs, SLOs and SLAs.

---

## A. Concept deep dive

### 1. Idempotency — the simplest explanation

> **An operation is idempotent if doing it once or many times gives the same final result.**

- Pressing a lift button five times = pressing it once → idempotent.
- `x = 5` is idempotent; `x = x + 5` is not.
- `INSERT` (append) is not idempotent; `MERGE` by key or "replace the data for 2026-10-16" is.

For pipelines: **re-running a job for the same input must leave the target in the same state as running it once.**

### 2. Why idempotency is the most important pipeline property

Pipelines **will** be re-run:
- automatic **retries** after transient failures;
- manual re-runs after a fix;
- **backfills** of past periods;
- sources that deliver the same data **twice** (at-least-once delivery);
- streaming restarts that replay the last micro-batch.

If the pipeline is idempotent, all of these are safe. If not, each one silently duplicates or corrupts data.

### 3. How to make a pipeline idempotent

| Technique | How | Typical use |
|---|---|---|
| **Deterministic inputs** | Process a parameterised `run_date`/range, never `now()`; read a fixed snapshot/version | Every batch job |
| **Overwrite the slice** | `mode("overwrite")` + `replaceWhere "order_date = '…'"` (or dynamic partition overwrite) | Daily aggregates, partitioned facts |
| **MERGE on a business key** | Upsert: same key → same row | Silver entities, CDC, incremental loads |
| **Dedupe before writing** | `row_number()` by key, keep latest | Sources with duplicates/resends |
| **Idempotent Delta writes** | `txnAppId` + `txnVersion` options: a write with an already-committed version for that app id is skipped | Appends inside streaming `foreachBatch` |
| **Streaming checkpoints** | Offsets tracked; with an idempotent sink → exactly-once results | Structured Streaming, Auto Loader |
| **Unique batch/run IDs** | Tag rows with `_run_id`; delete-then-insert for that run in **one** transaction | Audit/restart frameworks |
| **Guard side effects** | Emails, API calls, file moves aren't rolled back — record that they happened and check before repeating | Notifications, exports |

**Non-idempotent patterns to spot in code reviews:** `mode("append")` without dedup/keys; `current_date()`/`now()` deciding what to process; counters like `x = x + n`; delete in one statement and insert in another (not atomic); sending a file to a partner on every retry.

### 4. Delivery semantics

| Semantics | Meaning | Risk |
|---|---|---|
| **At-most-once** | Each record processed 0 or 1 times | Data loss |
| **At-least-once** | Each record processed 1 or more times | Duplicates |
| **Exactly-once** | Each record affects the result exactly once | — (the goal) |

In practice **exactly-once = at-least-once delivery + idempotent processing/sink**. Structured Streaming achieves end-to-end exactly-once with replayable sources, checkpoints and idempotent sinks like Delta.

### 5. Fault tolerance — failures are normal

On a 100-node cluster running for hours, *something* fails regularly. Fault tolerance is handled at several levels:

| Level | Mechanism |
|---|---|
| **Spark tasks** | A failed task is retried automatically (by default up to 4 attempts, `spark.task.maxFailures`) |
| **Lost executors / data** | Spark knows the **lineage** (the DAG of transformations) and **recomputes** lost partitions from source — no need to restart the job |
| **Slow tasks** | Optional speculative execution launches a duplicate of a straggler task |
| **Driver failure** | The application dies → the **orchestrator** retries the job/task |
| **Writes** | Delta commits are **atomic** — a failed write leaves no partial data visible; uncommitted files are cleaned by VACUUM |
| **Streaming** | Restart resumes from the **checkpoint** |
| **Pipeline** | Retries with backoff, timeouts, repair runs (re-run only failed tasks), idempotent tasks |
| **Data** | Bronze keeps raw data for **replay**; Delta time travel / RESTORE for rollback |

### 6. Scalability

| Vertical (scale up) | Horizontal (scale out) |
|---|---|
| Bigger machine | More machines |
| Simple, but has a ceiling and gets expensive | Near-linear — **if** work can be split into independent pieces |

Spark scales horizontally by splitting data into **partitions** processed in parallel (Phase 2). Things that **don't** scale horizontally:

| Anti-pattern | Why |
|---|---|
| `collect()` / `toPandas()` of large data | Everything goes to the single **driver** → OOM |
| Python loops over rows / per-row API calls | Single-threaded |
| **Skew** (one key with 40 % of the data) | One task does most of the work while others wait |
| One huge non-splittable file (gzip CSV) | One task reads it |
| Millions of tiny files | Overhead per file dominates |
| Full reloads that grow daily | Cost grows with history, not with change |

### 7. Reliability — speak like a production engineer

| Term | Meaning | Example |
|---|---|---|
| **SLI** (indicator) | What you measure | Time Gold table is ready; % of runs succeeding; freshness lag |
| **SLO** (objective) | Internal target | Gold ready by 07:00 on 99 % of days |
| **SLA** (agreement) | Commitment to consumers | Finance dashboard data by 08:00 on business days |

Reliability practices: idempotent tasks, retries for transient errors, timeouts, alerting on SLIs, data-quality gates, runbooks ("if X fails, do Y"), reconciliation, tested deployments, and the ability to **backfill**.

### 8. Misconceptions

1. **"Delta is ACID, so my pipeline is idempotent."** ACID makes *each write* atomic; it doesn't stop you appending the same data twice.
2. **"Exactly-once is a Spark setting."** It's a property of the whole chain: replayable source + checkpoint + idempotent sink.
3. **"Spark retries failed tasks, so I don't need job retries."** Task retries don't cover driver failure, cluster loss or external errors.
4. **"Scalable = add nodes."** Only if the work is evenly partitionable.

---

## B. Practical — four ways to write the same batch twice

**Runnable notebook:** [`exercises/phase-01/12_idempotency_demo.py`](../../exercises/phase-01/12_idempotency_demo.py)

| Strategy | Code (core) | After 2 runs | Idempotent? |
|---|---|---|---|
| Append | `.mode("append")` | 851.00 | ❌ |
| Overwrite slice | `.mode("overwrite").option("replaceWhere", "order_date = '2026-10-16'")` | 425.50 | ✅ |
| MERGE on key | `merge(... "t.order_date = s.order_date AND t.region = s.region")` | 425.50 | ✅ |
| Delta idempotent append | `.option("txnAppId", "daily_revenue_feed").option("txnVersion", batch_id)` | 425.50 | ✅ |

The idempotent append, line by line:

```python
def append_batch(df, batch_id):
    (df.write.format("delta")
       .option("txnAppId", "daily_revenue_feed")   # stable id of this writer/application
       .option("txnVersion", batch_id)             # monotonically increasing batch number
       .mode("append")
       .saveAsTable(TGT))
```
Delta stores the `(txnAppId, txnVersion)` pair in the commit. If the same app id writes a version that isn't newer than the last committed one, the write is skipped. In streaming, `foreachBatch` passes a `batch_id` that is **re-used on replay**, which is exactly what makes this work.

When to choose which:
- **replaceWhere** — outputs naturally partitioned by a period (daily aggregates, backfills).
- **MERGE** — entity tables with keys; CDC; incremental loads where rows change.
- **txnVersion** — high-volume appends from streaming where MERGE would be too costly.

`DESCRIBE HISTORY` at the end shows each operation — note how the skipped write does not produce a new data commit.

---

## C. Interview questions

### 🟢 Basic

**B1.** What does idempotent mean for a data pipeline?

<details><summary>Model answer</summary>

Running the pipeline once or multiple times for the same input produces the same final state in the target — re-runs, retries and backfills don't create duplicates or change results.
</details>

**B2.** Give two ways to make a Delta write idempotent.

<details><summary>Model answer</summary>

MERGE on a business key (upsert), overwrite only the slice being processed with `replaceWhere`, or append with Delta's `txnAppId`/`txnVersion` options so repeated batch versions are skipped. Plus deterministic, parameterised inputs.
</details>

**B3.** What is the difference between at-least-once and exactly-once?

<details><summary>Model answer</summary>

At-least-once guarantees no loss but allows duplicates (a record may be processed again after a failure). Exactly-once means each record affects the result once; in practice achieved with at-least-once delivery plus idempotent processing/sinks and checkpoints.
</details>

### 🟡 Medium

**M1.** How does Spark recover when an executor dies in the middle of a job?

<details><summary>Model answer</summary>

The driver detects the lost executor, reschedules its tasks on other executors, and recomputes any lost partitions using lineage (the recorded DAG of transformations from the source). Shuffle outputs stored on the lost executor may require re-running parent stages. The job continues without manual action; task-level retries are limited by `spark.task.maxFailures`.
</details>

**M2.** Why isn't "Delta Lake provides ACID transactions" enough to make a pipeline idempotent?

<details><summary>Model answer</summary>

ACID guarantees that each individual write is atomic and isolated. An append of the same data executed twice is two valid, atomic transactions — the table ends up with duplicates. Idempotency is a property of pipeline design (keys, slices, deterministic inputs, idempotent write options).
</details>

**M3.** Name three things that prevent a Spark job from scaling horizontally.

<details><summary>Model answer</summary>

Collecting large data to the driver (`collect`, `toPandas`), data skew concentrating work in a few tasks, non-splittable large files, single-threaded Python per-row logic or per-row API calls, huge numbers of tiny files.
</details>

### 🔴 Tricky

**T1.** A restart framework deletes today's rows and then inserts them again in two separate statements. Is it idempotent? What can go wrong?

<details><summary>Model answer</summary>

Re-running it gives the same end state, so it looks idempotent — but it isn't **atomic**: if it fails after the DELETE and before the INSERT, the table has no data for today and readers in between see it empty. Use a single atomic operation: `replaceWhere` overwrite or a MERGE (or both statements in one transaction where supported).
</details>

**T2.** Your streaming job's `foreachBatch` writes to Delta with MERGE and also sends a notification email per batch. After a crash, the last batch is replayed. What happens?

<details><summary>Model answer</summary>

The MERGE is idempotent — the table is fine. The email is sent again: side effects outside the transactional system aren't rolled back or deduplicated. Record sent notifications (keyed by batch_id) and check before sending, or move notifications to a separate idempotent step.
</details>

**T3.** "Our pipeline is exactly-once because we use Structured Streaming." When is that statement false?

<details><summary>Model answer</summary>

When the source isn't replayable, when the checkpoint is lost/changed/shared, when the sink isn't idempotent (e.g. plain append in `foreachBatch` without `txnVersion`, writes to an external system), or when side effects occur. Exactly-once is end-to-end: replayable source + reliable checkpoint + idempotent sink.
</details>

---

## E. Revision summary

- Idempotent = once or N times → same final state. Pipelines are re-run constantly, so this is non-negotiable.
- Techniques: deterministic inputs, **replaceWhere**, **MERGE**, dedupe, **txnAppId/txnVersion**, checkpoints, guarded side effects.
- Exactly-once = at-least-once + idempotent sink + checkpoint.
- Fault tolerance: task retries, **lineage recompute**, atomic Delta commits, checkpoints, orchestrator retries.
- Scale-out fails with driver collects, skew, non-splittable files, tiny files, row-by-row Python.
- Reliability vocabulary: SLI → SLO → SLA.