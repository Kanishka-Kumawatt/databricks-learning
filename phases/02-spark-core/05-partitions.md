# 2.5 Partitions — the Unit of Parallelism

**Time:** 25 min · **Prerequisites:** 2.2, 2.3

**After this topic you can:**
- explain what a partition is and why it is "the unit of parallelism";
- explain how Spark decides the number of partitions when **reading files**, **after a shuffle**, and for explicit `repartition`/`coalesce`;
- explain `spark.sql.shuffle.partitions` (default 200) and how AQE coalesces shuffle partitions;
- check partition counts on classic compute **and** on serverless/standard access mode;
- tell in-memory partitions apart from on-disk table partitioning.

---

## A. Concept deep dive

### 1. The simplest explanation

A DataFrame is not one big table in one place. It's split into **partitions** — chunks of rows — spread across executors.

**One task processes one partition** in a stage. So the number of partitions decides how many tasks there are, and therefore how much work can run in parallel.

```
DataFrame orders (logical: one table)
 ┌──────────┬──────────┬──────────┬──────────┬──────────┬──────────┐
 │ part 0   │ part 1   │ part 2   │ part 3   │ part 4   │ part 5   │   6 partitions
 └────┬─────┴────┬─────┴────┬─────┴────┬─────┴────┬─────┴────┬─────┘
      ▼          ▼          ▼          ▼          ▼          ▼
   task 0     task 1     task 2     task 3     task 4     task 5      6 tasks per stage
   ─── executor A (4 cores) ───      ── executor B (4 cores) ──     8 slots → all 6 run at once
```

### 2. Why partitions exist

| Problem | How partitions solve it |
|---|---|
| Parallelism | Independent chunks can be processed at the same time on different cores |
| Memory | Each task only needs its chunk in memory, not the whole dataset |
| Fault tolerance | If a task fails, only that partition is recomputed |
| Scheduling | Many small units balance across workers better than a few big ones |

### 3. Where the number of partitions comes from

| Source of the DataFrame | Number of partitions | Config / control |
|---|---|---|
| **Reading files** (Parquet, Delta, CSV, JSON, ORC) | Based on total bytes, file sizes and cores — see §4 | `spark.sql.files.maxPartitionBytes` (default **128 MB**), `spark.sql.files.openCostInBytes` (default **4 MB**), `spark.sql.files.minPartitionNum` (default = default parallelism) |
| **After a shuffle** (groupBy, join, distinct, orderBy, window) | `spark.sql.shuffle.partitions` (default **200**), then **AQE may coalesce** | §5 |
| `spark.range(start, end, step, numPartitions)` | As given (else default parallelism) | argument |
| `spark.createDataFrame(local_list)` | Small; depends on rows and cluster (verify for your runtime) | — |
| JDBC read | **1** unless you set `partitionColumn`, `lowerBound`, `upperBound`, `numPartitions` | options |
| Narrow transformations (filter, select, withColumn) | Same as parent (rows may shrink, partitions don't) | — |
| `union` | Sum of both inputs' partitions | — |
| `repartition(n)` / `repartition(n, col)` | Exactly `n` (shuffle) | — |
| `coalesce(n)` | `min(n, current)` (no shuffle; merges neighbours) | — |

### 4. File reads — how input partitions are computed (Apache Spark file sources)

```
maxSplitBytes = min( maxPartitionBytes,
                     max( openCostInBytes, totalBytes / minPartitionNum ) )
                totalBytes counts every file as (file size + openCostInBytes)
```

1. Every **splittable** file is cut into chunks of `maxSplitBytes`.
2. Chunks (and small files) are **packed** into partitions up to `maxSplitBytes`; each file also "costs" `openCostInBytes`, so many tiny files don't all land in one task.
3. Non-splittable files (e.g. **gzip CSV/JSON**) are one chunk each → one task per file, whatever its size.
4. Parquet splits are aligned to **row groups**: a split that contains no row-group start reads nothing (you can see empty tasks).

Worked examples (defaults, cluster with 64 cores):

| Input | Calculation | ≈ Partitions |
|---|---|---|
| 10 GB Parquet, large files | 10 GB/64 = 160 MB → min(128 MB, 160 MB) = 128 MB splits | ~80 |
| 1 GB Parquet, large files | 1 GB/64 = 16 MB → 16 MB splits (small data is spread over **all cores**) | ~64 |
| 10 000 files × 100 KB | each counts ~4.1 MB → packed ~31 files per 128 MB partition | ~320 |
| One 20 GB `.csv.gz` | not splittable | **1** (one core does everything) |

So "partitions = size / 128 MB" is only the rule for large inputs. Delta tables are read through the same file-source mechanism; Databricks may add its own optimisations — **verify on your runtime** with the notebook below.

### 5. Shuffle partitions and AQE

After a wide transformation, rows are redistributed into `spark.sql.shuffle.partitions` partitions — **200 by default in Apache Spark**, whether your data is 1 MB or 1 TB.

| Data size after shuffle | 200 partitions means | Problem |
|---|---|---|
| 50 MB | 200 tasks of 250 KB | Overhead, 200 tiny output files if written directly |
| 2 TB | 200 tasks of 10 GB | Spill to disk, OOM, slow stragglers |

**Adaptive Query Execution (AQE)** — on by default since Spark 3.2 (`spark.sql.adaptive.enabled = true`) and on Databricks — measures actual shuffle output sizes at runtime and **coalesces** small shuffle partitions toward `spark.sql.adaptive.advisoryPartitionSizeInBytes` (default **64 MB**) (`spark.sql.adaptive.coalescePartitions.enabled = true`). AQE can **merge** partitions; it can't create more than the initial number (except when splitting skewed partitions for joins). So for very large shuffles you still set the initial number high enough.

| Setting | Where | Meaning |
|---|---|---|
| `spark.sql.shuffle.partitions = 200` | Apache default; Databricks classic default (docs) | Fixed initial count |
| `spark.sql.shuffle.partitions = auto` | Databricks (auto-optimized shuffle) | Databricks chooses the number from the plan and input size |
| default `auto` | Databricks **serverless** (settable) | Same, by default |

### 6. How many partitions should you have?

Rules of thumb (not laws):

- At least **2–4× the number of cores** so all cores are busy and stragglers are balanced.
- Roughly **100–200 MB** of data per partition when processing; much smaller = overhead; much bigger = spill/OOM risk.
- Number of partitions at write time ≈ number of output files (per table partition) → too many partitions = small-file problem.

### 7. In-memory partitions ≠ table partitioning

| | Spark partition (this topic) | Table partitioning (`PARTITIONED BY`, Phase 6) |
|---|---|---|
| What | Chunk of rows processed by one task | Directory layout on storage by column values (e.g. `order_date=2026-10-16/`) |
| Lifetime | Exists only during execution | Persistent on disk |
| Purpose | Parallelism | Data skipping / pruning on read |
| Controlled by | Reads, shuffles, `repartition`, `coalesce` | Table DDL / `partitionBy` on write |

### 8. Checking partition counts — compute-dependent ⚠️

| Method | Classic compute (dedicated / no isolation) | Standard access mode / serverless |
|---|---|---|
| `df.rdd.getNumPartitions()` | ✅ works (on AQE plans it reflects the plan before runtime re-optimisation — treat with care) | ❌ RDD APIs not supported |
| `df.select(F.spark_partition_id()).distinct().count()` | ✅ (runs a job; counts **non-empty** partitions) | ✅ |
| `explain()` → `Exchange hashpartitioning(..., N)` / `RoundRobinPartitioning(N)` | ✅ | ✅ |
| Spark UI → stage → number of tasks | ✅ | ❌ serverless has no Spark UI → use the **query profile** |

### 9. Real data-engineering example — ShopCo

| Situation | Symptom | Partition-level fix |
|---|---|---|
| Daily CDC file arrives as one 15 GB `.json.gz` | Bronze ingestion uses 1 core for 40 min | Ask source for splittable/multiple files; or land, then `repartition` once before heavy work |
| Silver daily increment is 300 MB but `groupBy` creates 200 tasks and 200 tiny files | Slow job, small files | AQE coalescing (default on), `auto`, or a lower `spark.sql.shuffle.partitions` |
| Yearly backfill: 3 TB shuffle with 200 partitions | 15 GB per task → spills and OOM | Raise initial shuffle partitions (e.g. 3 TB / 200 MB ≈ 15 000) or `auto`; AQE coalesces down if too many |
| Customer lookup read over JDBC | Whole table through 1 task | `partitionColumn` + bounds + `numPartitions` |

### 10. Misconceptions

1. **"`spark.sql.shuffle.partitions` controls how many partitions a file read has."** No — reads use `maxPartitionBytes` & co.; shuffle partitions apply only after shuffles.
2. **"Partitions = file size / 128 MB."** Only for large inputs; small inputs spread across cores, tiny files are packed, gzip = 1 per file.
3. **"More partitions = faster."** Past the core count and a sensible size, more partitions only add overhead and small files.
4. **"`filter` reduces the number of partitions."** It reduces rows; partition count stays the same (some may become empty).
5. **"Spark partitions are the same as Delta table partitions."** Different concepts (§7).

---

## B. Practical — count, see and change partitions

**Runnable notebook:** [`exercises/phase-02/05_partitions.py`](../../exercises/phase-02/05_partitions.py)

Key part:

```python
from pyspark.sql import functions as F

def num_partitions(df):
    """Partition count. Uses the RDD API where allowed, else counts non-empty partitions."""
    try:
        return df.rdd.getNumPartitions()                                        # 1
    except Exception:
        return df.select(F.spark_partition_id().alias("p")).distinct().count()  # 2

nums = spark.range(0, 100, 1, numPartitions=4)
print(num_partitions(nums))                                                     # 3
print(num_partitions(nums.filter("id < 10")))                                   # 4
print(num_partitions(nums.union(nums)))                                         # 5

(nums.groupBy(F.spark_partition_id().alias("pid")).count()
     .orderBy("pid").show())                                                    # 6
```

**Expected output:**

```
4
4          ← classic (RDD path): 4.   Serverless/standard (fallback): 1 — only partition 0 has ids < 10!
8
+---+-----+
|pid|count|
+---+-----+
|  0|   25|
|  1|   25|
|  2|   25|
|  3|   25|
+---+-----+
```

| # | Explanation |
|---|---|
| 1 | Classic compute: asks the RDD how many partitions the plan has (no job needed for simple plans) |
| 2 | Fallback for Spark Connect: tag each row with its partition id, count distinct ids. Runs a job and only counts partitions that contain rows |
| 3 | `spark.range(..., numPartitions=4)` → 4 |
| 4 | `filter` is narrow: still 4 partitions — but only partition 0 (ids 0–24) has rows left. The two methods disagree: a lesson in what each measures |
| 5 | `union` concatenates partition lists: 4 + 4 = 8 |
| 6 | Rows per partition: `range` splits evenly, 25 each |

### Repartition vs coalesce — preview (full treatment in Phase 4)

```python
nums.repartition(8).explain()   # Exchange RoundRobinPartitioning(8)  → a shuffle
nums.coalesce(2).explain()      # Coalesce 2                           → no shuffle, merges partitions
```

| | `repartition(n)` / `repartition(n, "col")` | `coalesce(n)` |
|---|---|---|
| Shuffle? | Yes (full) | No |
| Can increase partitions? | Yes | No (only decrease) |
| Balance | Even (round-robin) or by hash of `col` | Can be uneven (merges neighbours) |
| Typical use | Increase parallelism, spread skew, co-locate by key before a write | Reduce output file count cheaply at the end |
| Trap | Costs a full shuffle | Pulls the reduced parallelism **upstream** into the whole stage (2.9) |

### Common mistakes

- Using `df.rdd.getNumPartitions()` in code that must run on serverless / standard access mode.
- Setting `spark.sql.shuffle.partitions = 2000` globally for a job whose daily increment is 100 MB (mostly hidden by AQE coalescing — but not for every operation or setting).
- `repartition(1)` / `coalesce(1)` "to get one file" on big data → one task does all the work.
- Confusing `df.repartition("order_date")` (in-memory, shuffle) with `write.partitionBy("order_date")` (directory layout).

---

## C. Interview questions

### 🟢 Basic

**B1.** What is a partition in Spark, and why is it called the unit of parallelism?

<details><summary>Model answer</summary>

A partition is a chunk of a dataset's rows that lives on one executor during processing. In each stage, Spark runs one task per partition, and one task runs on one core, so the number of partitions determines how many tasks exist and how much of the work can run in parallel.
</details>

**B2.** What is `spark.sql.shuffle.partitions` and its default?

<details><summary>Model answer</summary>

The number of partitions Spark uses for the output of shuffles (joins, aggregations, windows, distinct, sorts). Default 200 in Apache Spark. On Databricks it can be set to `auto` (auto-optimized shuffle) and serverless defaults to `auto`. AQE may coalesce partitions at runtime.
</details>

**B3.** How does Spark decide the number of partitions when reading Parquet files?

<details><summary>Model answer</summary>

It computes a split size `min(maxPartitionBytes, max(openCostInBytes, totalBytes / minPartitionNum))` — defaults 128 MB, 4 MB, and default parallelism — splits files into chunks of that size (aligned to Parquet row groups) and packs chunks and small files into partitions. Large data → ~128 MB partitions; small data → spread across available cores.
</details>

### 🟡 Medium

**M1.** What does AQE do to shuffle partitions, and what can't it do?

<details><summary>Model answer</summary>

After a shuffle's map stage finishes, AQE looks at the actual size of each shuffle partition and merges adjacent small ones toward the advisory size (64 MB default), so you don't get hundreds of tiny tasks. It can't increase the number beyond the initial shuffle partition count (apart from splitting skewed partitions in joins), so if the initial number is far too low for a huge shuffle, partitions stay too big.
</details>

**M2.** Your job reads one 30 GB gzip CSV and is slow. Why, and what can you do?

<details><summary>Model answer</summary>

Gzip isn't splittable, so the entire file is one partition, read and decompressed by one task on one core. Fixes: get the source to deliver multiple files or a splittable format (Parquet, uncompressed or bzip2 CSV); otherwise ingest once into Delta (Bronze) and `repartition` before heavy transformations so later stages run in parallel.
</details>

**M3.** How do you check the number of partitions on serverless compute?

<details><summary>Model answer</summary>

RDD APIs aren't supported, so `df.rdd.getNumPartitions()` fails. Use `df.select(spark_partition_id()).distinct().count()` (counts non-empty partitions, runs a job), inspect `explain()` for the partition counts in `Exchange` nodes, or check the query profile after running.
</details>

### 🔴 Tricky

**T1.** `df.rdd.getNumPartitions()` returns 200 after a `groupBy`, but the Spark UI shows the reduce stage ran only 3 tasks. Who's right?

<details><summary>Model answer</summary>

Both, at different moments. The static plan has 200 shuffle partitions (`spark.sql.shuffle.partitions`). At runtime AQE measured the shuffle output and coalesced the small partitions into 3, so 3 tasks ran. The UI shows what actually executed; with AQE always trust runtime metrics (Spark UI / query profile, `isFinalPlan=true` plan) over static counts.
</details>

**T2.** After `filter(order_date = '2026-10-16')` on a 2 TB table, a `write` produces 16 000 tiny files. Why?

<details><summary>Model answer</summary>

`filter` is narrow and keeps the input partition count (~16 000 from reading 2 TB), so most partitions contain only a few rows, and each write task produces at least one file. If the table had been partitioned/clustered by date, data skipping would have read far fewer files in the first place. Otherwise reduce partitions before writing (`coalesce`/`repartition`), or rely on Databricks optimized writes / auto compaction where enabled (Phase 6).
</details>

**T3.** A teammate sets `spark.sql.shuffle.partitions = 8` for a 1 TB join "because the cluster has 8 cores". What happens?

<details><summary>Model answer</summary>

Each shuffle partition holds ~125 GB; tasks spill heavily or OOM, and skew makes it worse. AQE can't split partitions up beyond 8 (except skew-join splitting). The number of partitions should follow data size (~100–200 MB each, e.g. thousands here), not core count — more partitions than cores simply run in waves.
</details>

---

## E. Revision summary

- Partition = chunk of rows; **one task per partition per stage** → partitions = parallelism.
- Reads: `min(maxPartitionBytes 128 MB, max(openCost 4 MB, totalBytes/minPartitionNum))`; gzip = 1 per file; tiny files are packed.
- Shuffles: `spark.sql.shuffle.partitions` (200; Databricks `auto` option; serverless `auto`); AQE coalesces toward 64 MB, can't add partitions.
- Narrow ops keep the count; `union` adds; `repartition` shuffles; `coalesce` merges without shuffle.
- `.rdd.getNumPartitions()` not on serverless/standard → `spark_partition_id()`, `explain()`, query profile. Spark partitions ≠ table partitions.