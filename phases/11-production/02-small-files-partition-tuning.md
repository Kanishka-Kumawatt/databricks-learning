# 11.2 Small-File Problem and Partition Tuning

**Time:** 20 min · **Prerequisites:** 11.0, Phase 6 (Delta log, OPTIMIZE, liquid clustering)

**After this topic you can:**
- explain why many small files make reads, writes and the Delta log slow;
- diagnose small files from `DESCRIBE DETAIL` and Spark UI evidence;
- fix them (OPTIMIZE) and prevent them (optimized writes, auto compaction, liquid clustering, predictive optimization);
- choose sensible partitioning and shuffle-partition settings.

---

## A. Concept deep dive

### 1. The problem

A table with 10 GB in 100,000 files of 100 KB is far slower than the same 10 GB in 80 files of 128 MB. Every file costs a fixed overhead: listing, opening, reading the Parquet footer, scheduling, and — for Delta — one `add` action in the transaction log. With tiny files, overhead dominates real work.

### 2. Why small files appear

| Cause | Mechanism |
|---|---|
| **Frequent small appends** | Streaming with short triggers, many tiny batch loads → each commit adds ≥ 1 file per writing task |
| **Too many write tasks** | Each task writes ≥ 1 file per partition it holds. 2000 shuffle partitions → up to 2000 files, even for 50 MB of data |
| **Over-partitioning** | `partitionBy` a high-cardinality column (customer_id, hour) → files per partition value × tasks |
| **Upstream** | Source systems dropping thousands of tiny JSON files (an ingestion problem — Bronze reads them, but Silver shouldn't inherit them) |
| **DML on many files** | MERGE/UPDATE rewriting small fractions of many files (11.5) |

### 3. How to recognise it — evidence

| Evidence | What you see |
|---|---|
| `DESCRIBE DETAIL t` | `numFiles` huge; `sizeInBytes / numFiles` in KB rather than tens/hundreds of MB |
| Spark UI scan stage | Thousands of very short tasks; task time dominated by scheduling; "number of files read" in the SQL tab scan node is large |
| Slow planning / long time before the first job starts | Delta must process a large log (many add/remove actions) — also check log checkpoints |
| Streaming | Batch duration increasing over time while input rate is constant |
| Storage | Many objects per folder; high cloud request costs (LIST/GET) |

### 4. Fixes, ranked

1. **Stop creating them**:
   - **Optimized writes** (`delta.autoOptimize.optimizeWrite = true`, session `spark.databricks.delta.optimizeWrite.enabled`): Databricks adds a shuffle before the write so each partition gets fewer, larger files. Enabled by default for `MERGE`, `UPDATE`/`DELETE` with subqueries, CTAS/INSERT on SQL warehouses, and partitioned tables on DBR 13.3 LTS+.
   - **Auto compaction** (`delta.autoOptimize.autoCompact = true`, session `spark.databricks.delta.autoCompact.enabled`): after a write succeeds, compacts small files in the partitions just written. Always on for MERGE/UPDATE/DELETE per current docs.
   - **Fewer write tasks**: rely on AQE coalescing; `coalesce(n)`/`repartition(n)` before writing small outputs; longer streaming triggers or `availableNow` batches.
2. **Compact existing files**: `OPTIMIZE t` (bin-packing, target ~1 GB by default for OPTIMIZE; `delta.targetFileSize` overrides). Idempotent; old files are removed later by `VACUUM` after the retention period (default 7 days).
3. **Let the platform do it**: **predictive optimization** for Unity Catalog managed tables runs OPTIMIZE/VACUUM automatically (account/catalog setting — check yours).
4. **Fix the layout**: replace over-partitioning with **liquid clustering** (`CLUSTER BY (col)`), which keeps file sizes healthy and supports changing keys (`ALTER TABLE … CLUSTER BY`).

### 5. Partitioning guidance (Delta on Databricks)

- Most tables under ~1 TB **don't need partitioning**; use liquid clustering (or nothing).
- If you partition: a **low-cardinality** column used in almost every filter (date), aiming for ≥ ~1 GB per partition.
- Never partition by IDs or timestamps.
- Partitioning **does** give clean `replaceWhere`/concurrency isolation by partition (11.5, 11.8) — liquid clustering with row-level concurrency covers much of that on current runtimes.

### 6. In-memory partitions (the other "partition")

| Setting | Effect | Tuning |
|---|---|---|
| `spark.sql.files.maxPartitionBytes` (128 MB) | Max bytes per **read** task; small files are packed together (with `spark.sql.files.openCostInBytes`) | Lower for more read parallelism on CPU-heavy parsing |
| `spark.sql.shuffle.partitions` (200) | Reduce tasks after a shuffle | Aim for ~100–200 MB per partition; AQE coalesces down from this number; Databricks also supports `auto` (verify) |
| `repartition(n)` | Full shuffle to exactly n (even) partitions | Before a heavy step needing parallelism, or by key |
| `coalesce(n)` | Merges partitions without a shuffle | Reduce file count before writing; careful — it also reduces upstream parallelism |

### 7. Misconceptions

1. **"OPTIMIZE deletes the small files."** It writes new big files and marks the small ones removed in the log; they stay on storage for time travel until `VACUUM`.
2. **"Partition everything by date for speed."** Small tables partitioned by date produce tiny files per day; clustering or no partitioning is faster.
3. **"`coalesce(1)` is a good way to get one nice file."** Forces all upstream work into one task; fine only for tiny outputs.
4. **"Z-ORDER is the current way."** Still supported, but liquid clustering is the recommended layout for new tables; you can't combine them on the same table.

---

## B. Practical — create and fix small files

**Runnable notebook:** [`exercises/phase-11/02_small_files.py`](../../exercises/phase-11/02_small_files.py)

| Step | Expected observation |
|---|---|
| 150 appends with optimize-write/auto-compact **off** | ~600 files, ~100–200 KB average |
| Timed scan before | Slower; scan stage with many short tasks |
| `OPTIMIZE` | A handful of files; `numFilesRemoved` ≈ 600 in `operationMetrics`; scan faster |
| 20 appends with protections **on** | Far fewer new files than the 80 you'd expect |
| `CREATE TABLE … CLUSTER BY` + `OPTIMIZE` | `clusteringColumns = [event_date]` in `DESCRIBE DETAIL` |
| `partitionBy(bucket)` with 2000 values | Thousands of files of a few KB |
| AQE off + 2000 shuffle partitions → AQE on → `coalesce(4)` | Up to 2000 files → a few → 4 |

Key lines:

```python
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'false', 'delta.autoOptimize.autoCompact' = 'false')
```
We deliberately switch off the protections to reproduce what happens on older runtimes, on non-Databricks Delta, or when someone disabled them.

```python
spark.sql("DESCRIBE DETAIL t").select("numFiles", "sizeInBytes")
```
Your first piece of evidence for any "the table got slow" question. Average file size = `sizeInBytes / numFiles`.

**Common mistakes:** timing `count()` (Delta can answer from log statistics without reading files); running `VACUUM … RETAIN 0 HOURS` (breaks time travel and concurrent readers; blocked by a safety check unless disabled); expecting `OPTIMIZE` to fix over-partitioning (it compacts within each partition, so 2000 partitions still have ≥ 2000 files).

---

## C. Interview questions

### 🟢 Basic

**B1.** Why are many small files bad for performance?

<details><summary>Model answer</summary>

Each file has fixed costs — listing, opening, reading footers/metadata, task scheduling, and a log entry for Delta — so with many tiny files the overhead dominates the actual data processing. It also increases cloud API calls and slows query planning.
</details>

**B2.** What does `OPTIMIZE` do?

<details><summary>Model answer</summary>

Compacts small files into larger ones (bin-packing; and clusters data on liquid-clustered tables, or Z-orders with `ZORDER BY`). It's a Delta transaction: new files are added and old ones marked removed; readers keep working; old files are physically deleted later by VACUUM.
</details>

**B3.** Difference between `repartition` and `coalesce`?

<details><summary>Model answer</summary>

`repartition(n)` does a full shuffle and produces n roughly equal partitions (can increase or decrease, optionally by key). `coalesce(n)` only reduces partitions by merging existing ones without a shuffle — cheaper, but partitions can be uneven and it can reduce parallelism of the upstream stage.
</details>

### 🟡 Medium

**M1.** A streaming job writing to Delta every 30 seconds has become slower every week, though the input rate is flat. Diagnose.

<details><summary>Model answer</summary>

Symptom: gradual degradation with constant input → something accumulating. Evidence: `DESCRIBE DETAIL` shows a large and growing `numFiles` with tiny average size; batch duration in streaming progress grows; downstream readers slow. Root cause: each micro-batch adds small files; nothing compacts them. Fix: enable optimized writes and auto compaction, schedule `OPTIMIZE` (or predictive optimization), consider a longer trigger interval. Verify: file count stabilises, batch duration flat; monitor `numFiles`.
</details>

**M2.** When would you partition a Delta table, and when would you use liquid clustering instead?

<details><summary>Model answer</summary>

Partition only large tables (roughly > 1 TB) on a low-cardinality column that almost every query filters on, with ≥ ~1 GB per partition — e.g. date — or when you need partition-level operations. For most tables, use liquid clustering: it gives data skipping on one or more columns without tiny-file problems, keys can change later, and it works with row-level concurrency. Never partition on high-cardinality columns.
</details>

**M3.** A job writes a 200 MB result as 2000 files. Why, and what are two fixes?

<details><summary>Model answer</summary>

The final stage has 2000 tasks (e.g. `spark.sql.shuffle.partitions = 2000` with AQE off/coalescing disabled), and each task writes one file. Fixes: enable AQE partition coalescing, `coalesce(n)`/`repartition(n)` before the write, optimized writes on the table, or lower shuffle partitions.
</details>

### 🔴 Tricky

**T1.** You ran OPTIMIZE but storage usage went **up**, and queries are only slightly faster. Explain.

<details><summary>Model answer</summary>

OPTIMIZE adds compacted files and logically removes the small ones; the small files stay until VACUUM after the retention period, so storage temporarily grows. Queries are only slightly faster if the bottleneck wasn't file count — e.g. the table is over-partitioned (OPTIMIZE compacts within partitions only), queries don't filter on clustered columns, or the real problem is skew/joins. Check `DESCRIBE DETAIL` and the query's scan metrics (files read vs pruned).
</details>

**T2.** Someone proposes `partitionBy("customer_id")` because "most queries filter by customer". Respond.

<details><summary>Model answer</summary>

High cardinality → millions of directories and tiny files, slow listing and planning, huge log, terrible write performance. Use liquid clustering on `customer_id` (data skipping via file statistics) instead; verify with the SQL tab's files-pruned metric.
</details>

**T3.** A table has 1 million files of 10 KB and a query fails on the driver before any task runs. Why?

<details><summary>Model answer</summary>

Planning happens on the driver: it must reconstruct the table state from the Delta log (a million `add` actions with statistics) and plan splits for a million files, which takes memory and time → driver OOM or very slow planning. Fix: compact with OPTIMIZE (on a sufficiently large driver, possibly partition by partition with `WHERE`), stop the source of small files, and let checkpoints reduce log replay.
</details>

---

## E. Revision summary

- Small files: per-file overhead (list/open/footer/schedule/log entry) dominates.
- Causes: tiny frequent appends, too many write tasks, over-partitioning, DML.
- Evidence: `DESCRIBE DETAIL` numFiles & avg size; thousands of short scan tasks; slow planning; growing streaming batch time.
- Fix: prevent (**optimized writes**, **auto compaction**, AQE coalesce, fewer tasks) → **OPTIMIZE** → **predictive optimization** → **liquid clustering** instead of over-partitioning.
- Partition only big tables by low-cardinality filter columns (≥ ~1 GB per partition).
- OPTIMIZE doesn't delete files; VACUUM does (default retention 7 days).