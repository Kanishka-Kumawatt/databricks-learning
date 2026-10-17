# 1.6 Batch vs Streaming

**Time:** 25 min · **Prerequisites:** 1.1, 1.4

**After this topic you can:**
- explain bounded vs unbounded data and the latency spectrum;
- explain how Spark Structured Streaming works at a high level (micro-batches, offsets, checkpoints, triggers);
- explain why "streaming" does not have to mean "always running";
- choose batch or streaming for a requirement and defend the choice.

---

## A. Concept deep dive

### 1. The simplest explanation

- **Batch:** collect data for a period, then process it all at once. *"Every night at 02:00, process yesterday's orders."*
- **Streaming:** process data continuously as it arrives. *"As each card transaction arrives, score it for fraud within seconds."*

### 2. Bounded vs unbounded

| | Batch | Streaming |
|---|---|---|
| Data | **Bounded** — a known, finite set (yesterday's file) | **Unbounded** — never ends (event stream) |
| Trigger | Schedule or event | Continuous / very frequent |
| Latency | Minutes to hours | Seconds (micro-batch) to milliseconds (real-time) |
| State | Recomputed each run | Must be **remembered** across events (running counts, dedup keys) |
| Failure recovery | Re-run the batch | Resume from the last **checkpoint** |
| Complexity / cost | Lower | Higher (always-on compute, state, late data) |

### 3. Why streaming exists

- **Freshness requirements:** fraud detection, operational dashboards, alerting, personalisation.
- **Smoother load:** process a little continuously instead of a huge nightly spike.
- **Incremental by nature:** each event is processed once, not reprocessed nightly.

But many "real-time" requests are really "every 15 minutes is fine" — ask what decision the latency enables.

### 4. How Spark Structured Streaming works

**Model:** a stream is treated as an **unbounded table** that keeps getting new rows. You write normal DataFrame logic; Spark runs it **incrementally** on the new rows.

```
 source (files / Kafka / Event Hubs / Delta)
     │  new data since last offset
     ▼
 ┌─────────── micro-batch N ────────────┐
 │ read new rows → transform → write    │──▶ sink (Delta table)
 └──────────────────────────────────────┘
     │ records offsets + state
     ▼
 checkpoint location (progress: "processed up to offset X")
```

| Concept | Meaning |
|---|---|
| **Micro-batch** | Default execution mode: process new data in small batches, one after another |
| **Offset** | Position in the source (Kafka offset, file list, Delta table version) |
| **Checkpoint** | Folder where Spark stores processed offsets and state. On restart, it **resumes exactly where it stopped**. Each streaming query needs its **own** checkpoint |
| **State** | Data remembered between micro-batches (running aggregates, dedup keys) — stored with the checkpoint |
| **Trigger** | *When* micro-batches run (below) |
| **Watermark** | How long to wait for late events before finalising time windows (Phase 7) |

**Triggers:**

| Trigger | Behaviour | Use |
|---|---|---|
| default (none set) | Next micro-batch starts as soon as the previous finishes | Low latency, always-on |
| `processingTime="1 minute"` | A micro-batch every minute | Predictable latency/cost |
| `availableNow=True` | Process **everything available now** (in one or more batches), then **stop** | **Incremental batch** — schedule it like a batch job but get streaming's exactly-once bookkeeping |

`availableNow` is extremely common on Databricks: a job runs every hour, picks up only new files/rows since the last checkpoint, then shuts down the cluster.

### 5. Event time vs processing time (preview of Phase 7)

- **Event time** — when the event actually happened (timestamp in the data).
- **Processing time** — when Spark processes it.
A mobile app can send an event from 09:00 at 09:40 (phone was offline). Aggregations "per hour" should use **event time**, and the watermark decides how long to wait for such late data.

### 6. Lambda vs Kappa (vocabulary)

- **Lambda:** separate batch and streaming pipelines for the same data → two codebases, numbers drift.
- **Kappa:** one streaming pipeline; reprocess by replaying the stream.
- Spark/Databricks reduces the gap: the same DataFrame code runs as batch (`read`) or stream (`readStream`), and Delta tables can be both a streaming source and sink.

### 7. Misconceptions

1. **"Streaming means a cluster running 24/7."** With `availableNow`, a stream can run on a schedule and stop.
2. **"Running a batch job every 5 minutes is streaming."** It's frequent batch: no offsets/state are tracked for you — you must handle what was already processed.
3. **"Streaming is always better because it's fresher."** It adds cost and complexity; only pay for the latency you need.
4. **"The checkpoint is optional."** Without a stable checkpoint, a restart either reprocesses everything (duplicates) or loses track of progress.

---

## B. Practical — the same logic as batch and as stream

```python
from pyspark.sql import functions as F

src = "/Volumes/shopco/raw/orders_json/"                        # folder where new JSON files land
schema = "order_id INT, customer_id STRING, amount DOUBLE, order_ts TIMESTAMP"

# ---------- BATCH: reads everything in the folder, every run ----------
batch_df = spark.read.schema(schema).json(src)                  # 1
(batch_df.filter("amount > 0")
   .write.mode("overwrite")                                     # 2
   .saveAsTable("main.de_prep.orders_batch"))

# ---------- STREAM: reads only files not processed before ----------
stream_df = spark.readStream.schema(schema).json(src)           # 3
(stream_df.filter("amount > 0")
   .writeStream
   .option("checkpointLocation",
           "/Volumes/main/de_prep/checkpoints/orders_stream")   # 4
   .trigger(availableNow=True)                                  # 5
   .toTable("main.de_prep.orders_stream"))                      # 6
```

| # | Explanation |
|---|---|
| 1 | Batch read of the **whole** folder — every run re-reads every file ever delivered |
| 2 | Overwrite to stay idempotent — but cost grows with the folder forever |
| 3 | `readStream` treats the folder as a source; a schema is **required** for file streams |
| 4 | Checkpoint stores which files were already processed → each file is processed once |
| 5 | Process everything new, then stop — schedule hourly like a batch job |
| 6 | Appends new rows to a Delta table (the default output mode is append) |

**Exercise:** drop a new JSON file in the folder, run both again. The batch re-reads all files; the stream reads only the new one. Then delete the checkpoint folder and run the stream again — what happens to the target table? (Answer: every file is processed again → duplicates in an append sink.)

> On Databricks, **Auto Loader** (`format("cloudFiles")`) is the preferred way to stream files — Phase 7.

---

## C. Interview questions

### 🟢 Basic

**B1.** What is the difference between batch and streaming processing?

<details><summary>Model answer</summary>

Batch processes a bounded set of data at scheduled intervals; streaming processes unbounded data continuously or in frequent micro-batches as it arrives. Streaming gives lower latency but requires tracking progress (offsets/checkpoints), managing state and handling late data.
</details>

**B2.** What is a checkpoint in Structured Streaming and why is it needed?

<details><summary>Model answer</summary>

A storage location where the query records which source offsets have been processed and any state it maintains. On restart or failure the query resumes from there, so data is neither skipped nor reprocessed (with an idempotent sink like Delta this gives exactly-once results).
</details>

### 🟡 Medium

**M1.** What does `trigger(availableNow=True)` do and why is it popular?

<details><summary>Model answer</summary>

It processes all data available at start time (possibly in multiple micro-batches) and then stops. It gives incremental processing with streaming's checkpoint bookkeeping while running on a schedule, so compute runs only briefly — cheap and simple.
</details>

**M2.** A stakeholder asks for "real-time" revenue. How do you respond?

<details><summary>Model answer</summary>

Clarify the decision and acceptable latency: what action depends on it and how fast? If 15–60 minutes is fine, scheduled incremental processing (`availableNow`) is cheaper and simpler. If seconds matter (fraud, operations), a continuous stream is justified — with the cost of always-on compute and more operational complexity.
</details>

### 🔴 Tricky

**T1.** Someone deletes the checkpoint folder of a streaming job writing to a Delta table in append mode, then restarts it. What happens?

<details><summary>Model answer</summary>

The query has no memory of progress, so it starts from the beginning of the source (or from whatever starting option is set) and reprocesses everything → duplicate rows in the target. Recovery: restore the checkpoint if possible; otherwise dedupe the target or rebuild it, and in future make the sink idempotent (e.g. MERGE in `foreachBatch`).
</details>

**T2.** Your batch job runs every 5 minutes reading `WHERE ts > now() - interval 5 minutes`. Why is that fragile?

<details><summary>Model answer</summary>

Time windows based on run time miss data whenever a run is delayed, skipped or slow, and double-process when runs overlap or retry; late-arriving events with older timestamps are never picked up. Track progress explicitly (watermark/control table or streaming checkpoints) instead of relying on the clock.
</details>

**T3.** Two different streaming queries write to the same table using the same checkpoint location. What's wrong?

<details><summary>Model answer</summary>

A checkpoint belongs to exactly one query. Sharing it corrupts progress tracking — queries overwrite each other's offsets/state, leading to failures, skipped or duplicated data. Each query needs its own checkpoint.
</details>

---

## E. Revision summary

- Batch = bounded data on a schedule; streaming = unbounded data processed incrementally.
- Structured Streaming = unbounded table + micro-batches + **offsets in a checkpoint** (+ state).
- Triggers: default, `processingTime`, **`availableNow`** (incremental batch, then stop).
- Event time ≠ processing time; watermarks handle late data.
- Only pay for the latency the business actually needs.