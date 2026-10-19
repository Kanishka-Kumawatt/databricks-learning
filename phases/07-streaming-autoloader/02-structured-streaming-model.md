# 7.2 Structured Streaming Model, Triggers and Output Modes

**Time:** 25 min · **Prerequisites:** 1.6, 7.1

**After this topic you can:**
- explain the unbounded-table model and the lifecycle of one micro-batch, step by step;
- list common sources and sinks and say which give exactly-once;
- choose a trigger (default, `processingTime`, `availableNow`, real-time mode) and know what is deprecated;
- choose an output mode (append, update, complete) and know which ones the Delta sink accepts.

> 1.6 introduced micro-batches, offsets, checkpoints and triggers. This lesson goes one level deeper: **what exactly happens inside a micro-batch, and why that gives exactly-once.**

---

## A. Concept deep dive

### 1. The simplest explanation

You write an ordinary DataFrame query. Spark treats the input as a **table that keeps growing** and runs your query **only on the new rows**, again and again, writing the results to a sink.

```
  input table (unbounded)                 result                      sink
  ┌──────────────┐                        ┌──────────┐            ┌──────────┐
  │ rows t0      │ ─ query on new rows ─▶ │ rows t0  │ ─ write ─▶ │  Delta   │
  │ rows t1  NEW │ ─ query on new rows ─▶ │ rows t1  │ ─ write ─▶ │  table   │
  │ rows t2  NEW │ ─ query on new rows ─▶ │ rows t2  │ ─ write ─▶ │          │
  └──────────────┘                        └──────────┘            └──────────┘
```

### 2. Why it exists

Older streaming systems (DStreams, Storm) had special APIs and semantics different from batch. Structured Streaming lets you reuse **the same DataFrame/SQL code** for batch and streaming, while the engine handles progress tracking, failure recovery and incremental state.

### 3. How one micro-batch works internally

```
 ┌─────────────────────────────── micro-batch N ───────────────────────────────┐
 │ 1. PLAN     ask each source for its latest available offset                  │
 │             (files: listing/notifications; Kafka: partition offsets;         │
 │              Delta: table version)  — capped by rate limits                  │
 │ 2. WAL      write offsets/N = range [end of N-1, new end) ──▶ checkpoint     │
 │ 3. EXECUTE  run the query on that range (read + update state if stateful)    │
 │ 4. SINK     write the output (Delta: one transaction tagged queryId+batchId) │
 │ 5. MARK     write commits/N ──▶ checkpoint                                   │
 └──────────────────────────────────────────────────────────────────────────────┘
 Crash after step 2, before step 5? On restart offsets/N exists but commits/N does not
 → batch N is re-run with EXACTLY the same offset range → the Delta sink sees that
   (queryId, N) is already in the table's log and skips the write → exactly-once result.
```

**Exactly-once end-to-end = replayable source + checkpoint (offsets WAL) + idempotent sink.**

| Requirement | Why | Example |
|---|---|---|
| Replayable source | Batch N must be re-readable with the same offsets | Files, Kafka (within retention), Delta. A socket is **not** replayable |
| Checkpoint | Remembers what batch N contained and whether it was committed | `checkpointLocation` |
| Idempotent sink | Re-writing batch N must not duplicate | Delta sink; `foreachBatch` **only if you make it idempotent** (7.9) |

### 4. Sources and sinks

| Source | Notes |
|---|---|
| File source (`spark.readStream.json/parquet/csv…`) | Universal Spark. Schema required (unless `spark.sql.streaming.schemaInference` is on). Logs every seen file in the checkpoint |
| **Auto Loader** (`format("cloudFiles")`) | Databricks. Scalable file discovery, schema inference/evolution, rescued data (7.5–7.7) |
| Kafka (incl. Event Hubs Kafka endpoint) | Offsets per topic-partition |
| Delta table (`readStream.table(...)`) | Reads new table versions. **Append-only input**: an UPDATE/DELETE/MERGE on the source fails the stream unless you set `skipChangeCommits` or read the change data feed |
| Rate (`format("rate")`) | Test generator |

| Sink | Output modes | Exactly-once? |
|---|---|---|
| **Delta** (`toTable` / `start(path)`) | append, complete — **not update** | Yes |
| `foreachBatch` | any; each micro-batch arrives as a normal DataFrame | Only if your function is idempotent (otherwise at-least-once) |
| Kafka | append, update, complete | At-least-once (dedupe downstream) |
| `foreach` | custom row writer | Your responsibility |
| console / memory / noop | testing | No guarantees |

### 5. Triggers — when micro-batches run

| Trigger | Syntax | Behaviour | Use |
|---|---|---|---|
| Default (unspecified) | — | Same as `processingTime` 0: next batch as soon as the previous finishes | Low latency; many storage API calls (cost) |
| Fixed interval | `.trigger(processingTime="1 minute")` | One batch per interval (a late batch is followed immediately by the next) | Predictable latency and cost |
| **Available now** | `.trigger(availableNow=True)` | Process everything available **at start**, in one or more rate-limited batches, then stop | Scheduled incremental batch — the usual Databricks choice for ingestion jobs |
| Once (**deprecated**) | `.trigger(once=True)` | One single batch, ignores rate limits | Deprecated on DBR 11.3 LTS+ — use `availableNow` |
| Real-time mode (Databricks) | `.trigger(realTime="5 minutes")` | Sub-second operational streaming; the duration is the length of a long-running batch | Fraud, personalisation (check availability/limits) |
| Continuous (OSS) | `.trigger(continuous="1 second")` | Experimental since Spark 2.3 | **Not supported on Databricks** — use real-time mode |

- **Serverless compute:** notebooks/jobs support only `availableNow` (and the deprecated `once`). For continuous streaming on serverless, use a Lakeflow pipeline in continuous mode.
- You **can change the trigger** between runs with the same checkpoint.
- `availableNow` respects `maxFilesPerTrigger` / `maxBytesPerTrigger` (several batches → no OOM on a big backlog); `once` crams everything into one batch.

### 6. Output modes — what is written each trigger

| Mode | Emits | Allowed for | Delta sink |
|---|---|---|---|
| **Append** (default) | Only rows that will **never change** again | Stateless queries; aggregations **with a watermark** (row emitted once its window is final); stream-stream joins | ✅ |
| **Update** | Rows that **changed** in this trigger | Aggregations | ❌ — use `foreachBatch` + `MERGE` |
| **Complete** | The **entire** result table every trigger | Aggregations only | ✅ (table rewritten) |

For stateless queries (select/filter/withColumn) the mode makes no difference: each input row produces its output once.

### 7. Stateless vs stateful

- **Stateless:** each row processed independently (parse, filter, cast, add audit columns). Bronze ingestion is stateless.
- **Stateful:** the result depends on earlier batches — aggregations, `dropDuplicates`, stream-stream joins, `transformWithState`. State lives in a **state store** that is checkpointed (7.3, 7.4).
- **Stream-static join:** stream joined to a Delta table read as a normal table (enrich orders with current customers). No join state; the static side is re-read at its latest version each batch.

### 8. ShopCo example

- Bronze orders: Auto Loader → stateless → append → `availableNow` every 15 minutes (cluster stops between runs).
- Live "revenue per minute" for the ops screen: Kafka → windowed aggregation + watermark → update mode via `foreachBatch` MERGE → `processingTime="30 seconds"` on jobs compute.
- Daily KPI: not streaming at all — a batch job or materialized view.

### 9. When to use / not use

| Use Structured Streaming when… | Prefer batch / materialized view when… |
|---|---|
| Data arrives continuously or in many increments (files, events) | One-off historical load of a fixed folder |
| You want exactly-once progress tracking for free | The source rewrites history and you need full recomputation |
| The same logic may need different latencies later (`availableNow` today, continuous tomorrow) | Complex multi-table batch logic with no incremental structure |

### 10. Misconceptions

1. **"Streaming = always-on cluster."** `availableNow` streams are scheduled jobs.
2. **"Exactly-once means each record is processed once."** It means each record affects the **result** once. A batch can be *executed* twice after a crash; the sink makes the repeat a no-op.
3. **"Update mode works with Delta tables."** The Delta sink supports append and complete only.
4. **"`trigger(once=True)` is fine."** Deprecated on DBR 11.3+, and it ignores rate limits.
5. **"The default trigger is cheap."** It polls continuously — many LIST/API calls on cloud storage.

---

## B. Practical — anatomy of a stream

**Runnable notebook:** [`exercises/phase-07/01_streaming_checkpoints.py`](../../exercises/phase-07/01_streaming_checkpoints.py) (Parts 1, 3, 5)

### Input files (written by the notebook into a volume)

```
orders_0001.json   {"order_id":101,"customer_id":"C1","amount":250.0,"order_ts":"2026-10-16T09:15:00"}
                   {"order_id":102,"customer_id":"C2","amount":100.0,"order_ts":"2026-10-16T10:02:00"}
orders_0002.json   (lands later)  103 C3 40.0 · 104 C9 75.5
```

### Core code

```python
q = (spark.readStream.schema(SCHEMA_DDL).json(SRC)         # 1
     .withColumn("_ingested_at", F.current_timestamp())     # 2
     .writeStream
     .option("checkpointLocation", checkpoint)              # 3
     .trigger(availableNow=True)                            # 4
     .toTable(T(target)))                                   # 5
q.awaitTermination()                                        # 6
for p in progress_dicts(q):                                 # 7
    print(p["batchId"], p["numInputRows"])
```

| # | Explanation |
|---|---|
| 1 | Plain Spark **file source**: the folder is the unbounded input. A schema is mandatory for file streams |
| 2 | Stateless transformation — runs per micro-batch on new rows only |
| 3 | Where offsets and commits are stored (7.3). One per query |
| 4 | Process everything present at start, then stop |
| 5 | Delta sink, default output mode **append**; creates the table if missing |
| 6 | Block until the `availableNow` run finishes and surface failures. (For continuous streams in Lakeflow Jobs, Databricks recommends *not* calling it) |
| 7 | `recentProgress`: one JSON per batch with `batchId`, `numInputRows`, durations, source offsets |

### Expected output

| Run | New files | `numInputRows` | Rows in table |
|---|---|---|---|
| 1 | orders_0001 | 2 | 2 |
| 2 | orders_0002 | 2 | 4 |
| 3 | none | no batch (or 0) | 4 |

Part 5 tries the same aggregation in each output mode:

| Attempt | Result |
|---|---|
| append + aggregation without watermark | Rejected at start (AnalysisException) |
| update → Delta table | Rejected (Delta sink does not support update) |
| complete → Delta table | OK; whole table rewritten each run |

**Common mistakes**

| Mistake | Consequence |
|---|---|
| No `checkpointLocation` (or a temporary one) | No fault tolerance; restart reprocesses or loses progress |
| `display(streaming_df)` in production | Temporary checkpoint, no guarantees |
| Default trigger on a directory-listing file source 24/7 | High storage API cost |
| `outputMode("update")` straight into a Delta table | Query fails; use `foreachBatch` + MERGE |

---

## C. Interview questions

### 🟢 Basic

**B1.** Describe the Structured Streaming programming model in two sentences.

<details><summary>Model answer</summary>

A stream is treated as an unbounded table that keeps receiving new rows, and you write a normal DataFrame/SQL query on it. The engine runs that query incrementally in micro-batches on the new data and writes results to a sink, tracking progress in a checkpoint.
</details>

**B2.** What are the three output modes?

<details><summary>Model answer</summary>

Append (default): only rows that will never change again are written. Update: rows that changed in this trigger are written. Complete: the entire result table is written every trigger (aggregations only).
</details>

**B3.** What does `trigger(availableNow=True)` do, and what does it replace?

<details><summary>Model answer</summary>

It processes all data available when the query starts — possibly in several micro-batches that respect rate limits — and then stops. It replaces `trigger(once=True)`, deprecated on Databricks Runtime 11.3 LTS+, which processed everything in one batch and ignored rate limits.
</details>

### 🟡 Medium

**M1.** Walk through one micro-batch and say what happens if the cluster dies in the middle.

<details><summary>Model answer</summary>

The driver asks each source for its latest offset, writes the batch's offset range to `offsets/N` (write-ahead log), executes the query on that range (reading/updating state), the sink commits the output (Delta: one transaction tagged with query ID and batch ID), then `commits/N` is written. If it dies after `offsets/N` but before `commits/N`, the restart re-runs batch N with the same offsets; the idempotent sink recognises the already-committed batch, so the result is written once.
</details>

**M2.** You need per-window counts that update live in a Delta table. Which output mode and sink?

<details><summary>Model answer</summary>

The Delta sink doesn't support update mode. Use `outputMode("update")` with `foreachBatch` and MERGE the changed windows into the Delta table on (window, key). Alternatives: append mode with a watermark if you can wait until windows are final, or complete mode if the result is small.
</details>

**M3.** A stream reading a Delta table fails right after someone ran `DELETE` on that source table. Why, and what are the options?

<details><summary>Model answer</summary>

The Delta streaming source expects append-only input; DELETE/UPDATE/MERGE rewrite files and the stream throws. Options: `skipChangeCommits` (ignore change commits; changes are not propagated), stream the table's change data feed and handle each change type, or full-refresh the downstream table.
</details>

### 🔴 Tricky

**T1.** "Exactly-once processing" — does Spark guarantee each record is processed only once?

<details><summary>Model answer</summary>

No. A micro-batch can be executed more than once after a failure. The guarantee is an exactly-once *effect on the sink*: the replay uses identical offsets (replayable source + WAL) and the sink recognises the committed batch (idempotent sink). Side effects inside the query — calling an API, sending email, a non-idempotent `foreachBatch` — can happen twice.
</details>

**T2.** A team moves an always-on stream to serverless jobs and it fails at start. Why?

<details><summary>Model answer</summary>

Serverless notebooks/jobs support only `Trigger.AvailableNow` (and the deprecated `Once`); a default or `processingTime` trigger is rejected. Either run with `availableNow` on a schedule or file-arrival trigger, or run continuous streaming in a Lakeflow pipeline in continuous mode.
</details>

**T3.** An ingestion stream with the default trigger costs more in storage API calls than in compute. Explain and fix.

<details><summary>Model answer</summary>

The default trigger is `processingTime` 0: a new batch starts as soon as the last finishes, and with directory listing every batch lists the source path — huge numbers of LIST calls even when nothing arrives. Fix: set a `processingTime` interval that matches the latency need, or use `availableNow` on a schedule / file-arrival trigger, and use file events (notification mode) for discovery.
</details>

---

## E. Revision summary

- Unbounded table + incremental query + micro-batches; same DataFrame code as batch.
- Micro-batch: plan offsets → **offsets WAL** → execute → **sink commit** → **commit marker**.
- Exactly-once = **replayable source + checkpoint + idempotent sink**; a batch may *run* twice.
- Triggers: default (= 0 ms interval), `processingTime`, **`availableNow`**, real-time mode; `once` **deprecated** (DBR 11.3+); continuous unsupported on Databricks; serverless = `availableNow` only.
- Output modes: append (default), update (**not on the Delta sink** → `foreachBatch` MERGE), complete (aggregations; rewrites everything).