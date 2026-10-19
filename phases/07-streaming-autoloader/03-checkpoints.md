# 7.3 Checkpoints — What They Store, Why They Matter

**Time:** 20 min · **Prerequisites:** 7.2

**After this topic you can:**
- list what is inside a checkpoint folder and what each part is for;
- explain the "one checkpoint per query" rule and where to store checkpoints;
- say which query changes are safe with an existing checkpoint and which need a new one;
- explain exactly what happens when a checkpoint is deleted, and how to recover.

---

## A. Concept deep dive

### 1. The simplest explanation

A checkpoint is the **memory of a streaming query**: which input it has already read, which output it has already committed, and any running state (counts, dedup keys). Delete it and the query has amnesia.

### 2. Why it exists

A stream never "finishes", and clusters fail, get restarted, or are replaced by a new job run. Without durable memory, every restart would have to choose between **re-reading everything** (duplicates) or **starting from "now"** (data loss). The checkpoint lets a restarted query continue **exactly** where the last committed batch ended.

### 3. What is inside

```
 /Volumes/main/de_prep/phase07/_checkpoints/bronze_orders/
 ├── metadata              {"id":"<query id>"}  — identity of the query, stable across restarts
 ├── offsets/              write-ahead log, one file per batch, written BEFORE the batch runs
 │   ├── 0                 v1 / {batchWatermarkMs, batchTimestampMs, conf:{…}} / {source offset}
 │   └── 1
 ├── commits/              one file per batch, written AFTER the sink committed it
 │   ├── 0
 │   └── 1
 ├── sources/0/            per-source bookkeeping
 │      plain file source → a log listing every file already seen
 │      Auto Loader       → RocksDB key-value store of discovered files (Databricks)
 ├── state/                only for stateful queries
 │   └── <operatorId>/<partition>/…   state store files (snapshots/changelogs)
 └── _schemas/             only if Auto Loader's schemaLocation points here (7.6)
```

| Part | Purpose | What breaks without it |
|---|---|---|
| `metadata` | Query ID | Delta sink dedups by (queryId, batchId); a new ID = a "new" query |
| `offsets/N` | Exactly what batch N reads (and the watermark + pinned configs) | Replays can't reproduce batch N |
| `commits/N` | Batch N is done | Restart can't tell finished from unfinished batches |
| `sources/` | Which files/offsets were already seen | Files re-ingested |
| `state/` | Aggregation values, dedup keys, join buffers | Counts restart from zero, duplicates pass |

Restart logic: find the latest `offsets/N`; if `commits/N` exists, plan batch N+1; otherwise **re-run batch N with the same offsets**.

> **Pinned configuration:** some SQL configs are stored in the offset log and reused on restart. The best-known example: `spark.sql.shuffle.partitions` is fixed for a stateful query by its checkpoint, because the state is partitioned that way. Changing the setting later has no effect on that query.

### 4. State store (preview of 7.4)

Stateful operators keep state per partition in a **state store**, checkpointed each batch into `state/`.
- **Databricks:** RocksDB is the **default** state store provider on DBR 17.3+; earlier runtimes can enable it with `spark.sql.streaming.stateStore.providerClass = com.databricks.sql.streaming.state.RocksDBStateStoreProvider`. **Changelog checkpointing** (upload only changed state) is default on 17.3+.
- **Open-source default:** HDFS-backed state store (state in executor memory, snapshot + delta files). Spark also ships a RocksDB provider.

### 5. The rules

| Rule | Why |
|---|---|
| **One checkpoint per query** — never share | Two queries overwriting the same offsets/commits corrupt each other's progress |
| **Durable cloud storage** (volume / ADLS), not local disk or temp | Must survive cluster loss |
| **No lifecycle/retention policy** on the checkpoint path | If files are deleted, state is corrupted and the stream must restart from scratch |
| **Keep it with its target** (e.g. `<table>/_checkpoints/` or a dedicated volume folder) | Delta `VACUUM` skips directories starting with `_`, so `_checkpoints` beside table data is safe |
| **Treat it as part of the pipeline's state** | Back it up / restrict delete permissions like a table |

In **Lakeflow pipelines** (declarative pipelines), checkpoints and Auto Loader schema locations are managed for you; "full refresh" is the managed way to reset them.

### 6. Changes after the first run

From the Databricks docs on recovering after query changes:

| Change | With same checkpoint |
|---|---|
| Add/remove filters; projections with the same output schema | ✅ allowed |
| Change rate limits (`maxFilesPerTrigger`, `maxOffsetsPerTrigger`) or trigger interval | ✅ allowed |
| Number/type of input sources | ❌ not allowed (sources are identified by position) — except with *source evolution* (named sources, DBR 18.2+, needs a fresh checkpoint to enable) |
| Subscribed Kafka topics / **Auto Loader input path** | ❌ generally needs a new checkpoint |
| Stateful operations: grouping keys, aggregates, dedup keys, join type/keys, state schema | ❌ not allowed |
| Output sink **type** | Mostly ❌ (a few combos allowed, e.g. file → Kafka) |
| File-sink output **directory** | ❌ |
| Projection with **different** output schema | Only if the sink accepts the schema change |

### 7. What happens when a checkpoint is deleted

The next start has **no offsets, no commits, no file state, no state, new query ID**. What it reads first depends on the source's *initial* options:

| Source | Restart reads… | Result in an append sink |
|---|---|---|
| File source / Auto Loader (`includeExistingFiles=true`, default) | **every file in the path again** | Duplicates of everything |
| Auto Loader with `includeExistingFiles=false` | only files arriving after the new start | **Gap** for files that arrived while it was down |
| Kafka | `startingOffsets` (`latest` by default for streams) | Gap (latest) or duplicates (earliest) |
| Delta table | Full current snapshot, then new versions (unless `startingVersion`/`startingTimestamp`) | Duplicates |

Stateful queries also lose their aggregates — counts restart from zero.

**Recovery playbook**
1. **Best:** restore the checkpoint folder (storage soft-delete/versioning, backups).
2. **Idempotent target?** If Silver is written with MERGE on a business key, simply re-run — duplicates collapse.
3. **Append-only Bronze:** either rebuild (truncate target + new checkpoint, reprocess all files) or start a new checkpoint with `includeExistingFiles=false` **and** backfill the missing window once (batch read of the folder filtered by `_metadata.file_modification_time`, or `COPY INTO` for those files), then dedupe on `_source_file`.
4. If `foreachBatch` uses `txnAppId`, **change the `txnAppId`** with the new checkpoint — batch IDs restart at 0 and Delta would otherwise skip them as "already written".

### 8. ShopCo example

An engineer "cleans up the volume" and deletes `_checkpoints/bronze_orders`. The 15-minute job reruns and Auto Loader ingests all 40,000 historical files again into append-only `bronze_orders` → every order appears twice; Silver is fine because it MERGEs on `order_id`; Bronze row counts double and the reconciliation alert fires. Fix: delete the rows with `_ingested_at` in the bad run (Bronze has audit columns — 7.8), or restore Bronze with `RESTORE TABLE … TO VERSION AS OF` the version before the run; restore or rebuild the checkpoint.

### 9. Misconceptions

1. **"The checkpoint is just a cache."** It is the authoritative record of progress and state.
2. **"I'll reuse one checkpoint folder for two streams into the same table."** Each query needs its own.
3. **"Changing the checkpoint path is harmless."** It abandons the old stream and starts a new one.
4. **"`DataFrame.checkpoint()` is the same thing."** That truncates a batch DataFrame's lineage; unrelated to streaming checkpoints.

---

## B. Practical — open a checkpoint and break it

**Runnable notebook:** [`exercises/phase-07/01_streaming_checkpoints.py`](../../exercises/phase-07/01_streaming_checkpoints.py) (Parts 2 and 4)

```python
for f in dbutils.fs.ls(CHK):                       # 1
    print(f.name)
print(dbutils.fs.head(f"{CHK}/metadata"))           # 2
print(dbutils.fs.head(f"{CHK}/offsets/0"))          # 3
print(dbutils.fs.head(f"{CHK}/commits/0"))          # 4
for f in dbutils.fs.ls(f"{CHK}/sources/0"):        # 5
    print(dbutils.fs.head(f.path, 1500))
display(spark.sql(f"DESCRIBE HISTORY {T('orders_stream')}")
        .select("version", "operation", "operationParameters"))   # 6

run_stream(f"{BASE}/_checkpoints/orders_stream_NEW", "orders_stream_dupes")   # 7
```

| # | Explanation |
|---|---|
| 1 | Expect `commits/`, `metadata`, `offsets/`, `sources/` (no `state/` — the query is stateless) |
| 2 | `{"id": "…"}` — the query ID. Same after every restart |
| 3 | Three lines: version (`v1`), batch metadata JSON (watermark, timestamp, pinned confs), then the source offset (for the file source, a `logOffset`) |
| 4 | Small marker file — batch 0 is committed |
| 5 | The file source's log of files it has seen (path, timestamp, batchId). Auto Loader replaces this with a RocksDB store |
| 6 | Each streaming write is a Delta commit `STREAMING UPDATE` with `queryId` and `epochId` → how the sink skips replays |
| 7 | Same logic + empty checkpoint = "new query" → re-reads every file |

### Expected output (Part 4)

| order_id | count in `orders_stream_dupes` |
|---|---|
| 101 | 2 |
| 102 | 2 |
| 103 | 2 |
| 104 | 2 |

**Common mistakes**

| Mistake | Consequence |
|---|---|
| Checkpoint under `/tmp` or the driver's local disk | Lost on cluster restart → reprocessing |
| Lifecycle rule "delete files older than 30 days" on the whole container | Old offset/state files removed → stream corrupted |
| Copy-pasting a notebook and keeping the same `checkpointLocation` | Two queries share a checkpoint → unpredictable failures |
| Changing `groupBy` keys and restarting | State schema mismatch → failure or wrong results; needs a new checkpoint |

---

## C. Interview questions

### 🟢 Basic

**B1.** What does a Structured Streaming checkpoint contain?

<details><summary>Model answer</summary>

The query ID (`metadata`), the offset ranges of every batch written before it runs (`offsets/`, a write-ahead log that also records the watermark and pinned configs), commit markers written after the sink commits (`commits/`), per-source bookkeeping (`sources/` — e.g. the files already seen; RocksDB file state for Auto Loader) and, for stateful queries, the state store data (`state/`).
</details>

**B2.** Can two streaming queries share one checkpoint location?

<details><summary>Model answer</summary>

No. A checkpoint belongs to exactly one query. Sharing makes the queries overwrite each other's offsets, commits and state, leading to failures, skipped data or duplicates.
</details>

**B3.** Where should you store checkpoints?

<details><summary>Model answer</summary>

In durable cloud storage (a Unity Catalog volume or external location path) with no lifecycle/retention policy, one folder per query, ideally next to its target (e.g. a `_checkpoints` folder, which Delta VACUUM ignores). Never on local/temporary storage.
</details>

### 🟡 Medium

**M1.** The cluster crashes in the middle of batch 42. What happens on restart?

<details><summary>Model answer</summary>

`offsets/42` exists but `commits/42` doesn't, so the query re-runs batch 42 with exactly the offsets recorded in `offsets/42`. If the sink had already committed batch 42 (Delta records queryId + batchId in its log), the repeated write is skipped; otherwise it is written now. Either way the result appears once.
</details>

**M2.** You add a filter and change `maxFilesPerTrigger`, then change the `groupBy` key. Which changes are safe with the existing checkpoint?

<details><summary>Model answer</summary>

Adding a filter and changing rate limits are allowed. Changing grouping keys changes the state schema of a stateful operator — not allowed; the restart fails or behaves unpredictably. That change needs a new checkpoint (and usually a rebuild of the target).
</details>

**M3.** Someone deleted the checkpoint of an Auto Loader stream writing append-only to Bronze. What happens on restart and how do you recover?

<details><summary>Model answer</summary>

With default `includeExistingFiles=true`, Auto Loader treats every file in the path as new and re-ingests all of them → duplicates in Bronze. Recover by restoring the checkpoint if possible; otherwise delete the duplicate rows using audit columns (`_ingested_at` of the bad run) or `RESTORE` the table to the previous version, then either rebuild the checkpoint by reprocessing from scratch into an empty table, or start a new checkpoint with `includeExistingFiles=false` and backfill the gap once. Downstream MERGE-based Silver stays correct.
</details>

### 🔴 Tricky

**T1.** You raise `spark.sql.shuffle.partitions` from 200 to 800 to speed up a stateful streaming aggregation. Nothing changes. Why?

<details><summary>Model answer</summary>

For stateful queries the number of shuffle partitions is fixed by the checkpoint: the state store is partitioned into exactly that many partitions, and the config is stored in the offset log and reused on restart. To change it you need a new checkpoint (and to rebuild state).
</details>

**T2.** After resetting a checkpoint, a `foreachBatch` writer using `txnAppId`/`txnVersion` writes nothing for hours. Why?

<details><summary>Model answer</summary>

The new checkpoint restarts batch IDs at 0. Delta has already recorded (txnAppId, versions 0…N) from the old run, so it treats the new batches as duplicates and skips them until the batch ID exceeds the old maximum. When you reset the checkpoint you must use a new `txnAppId`.
</details>

**T3.** Is changing the Auto Loader input path on the same checkpoint a good way to "move" ingestion to a new folder?

<details><summary>Model answer</summary>

No. Changing source paths generally requires a new checkpoint; results are unpredictable (and in classic file notification mode files already in the new location may be missed). Start a new stream with a new checkpoint for the new path, decide explicitly how existing files there are handled (`includeExistingFiles`), and retire the old stream.
</details>

---

## E. Revision summary

- Checkpoint = query memory: `metadata` (query ID), `offsets/` (WAL, before), `commits/` (after), `sources/` (seen files; Auto Loader RocksDB), `state/`.
- Restart: last offsets without commit → re-run that batch with the same offsets; idempotent sink → exactly-once.
- **One per query**, durable storage, **no lifecycle policy**, `_checkpoints` beside the table is VACUUM-safe.
- Safe changes: filters, rate limits, trigger. Unsafe: sources/paths/topics, stateful keys or aggregates, sink type.
- Deleted checkpoint → reprocess from scratch (duplicates) or from "now" (gap). Restore, rebuild, or new checkpoint + backfill; new `txnAppId`.
- State store: RocksDB default on DBR 17.3+; shuffle partitions fixed by the checkpoint.