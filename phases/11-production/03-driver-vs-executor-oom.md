# 11.3 Driver vs Executor OOM (and Spill)

**Time:** 20 min · **Prerequisites:** 11.0, Phase 2 (driver/executors), Phase 3 (memory, spill)

**After this topic you can:**
- tell a driver OOM from an executor OOM by its error message and logs;
- explain Spark's executor memory model and why spill happens;
- list the code patterns that cause each, and the fixes in the right order;
- explain why "just add memory" is usually the wrong first answer.

---

## A. Concept deep dive

### 1. Two different machines, two different problems

| | Driver | Executors |
|---|---|---|
| Role | Plans queries, schedules tasks, holds **results brought back** (`collect`, `toPandas`), builds broadcast tables, runs your plain Python | Run tasks on partitions; hold shuffle/sort/aggregation buffers and cached data |
| One or many | **One** — a single point of failure | Many — Spark re-runs failed tasks elsewhere |
| OOM effect | Whole application dies; on Databricks the driver restarts; **every notebook on that cluster loses state** | Task fails, executor may be lost, task retried (up to `spark.task.maxFailures` = 4); repeated failure aborts the stage/job |

### 2. Driver OOM — causes and signatures

**Causes:**
- `collect()`, `toPandas()`, `df.rdd.collect`, `.take(huge_n)` on large data.
- Broadcasting a table that is too big (explicit hint or a wrong size estimate) — the driver collects it first.
- Huge query plans: hundreds of chained `withColumn`s or unions in a loop; planning over millions of files (11.2).
- Plain Python/pandas work on the driver: building giant lists/dicts, reading big files with pandas.
- Many concurrent notebooks/jobs on one all-purpose cluster's driver.

**Signatures (evidence):**
- `org.apache.spark.SparkException: Job aborted … Total size of serialized results of N tasks (X GB) is bigger than spark.driver.maxResultSize (Y GB)` — a guard that fires *before* OOM. (Spark default 1g; Databricks clusters may set a different default — check your config.)
- `java.lang.OutOfMemoryError: Java heap space` / `GC overhead limit exceeded` in the **driver log4j/stderr**.
- Run page / notebook: **"The spark driver has stopped unexpectedly and is restarting. Your notebook will be automatically reattached."**
- Python process OOM (pandas on driver): kernel crashes, "Fatal error: The Python kernel is unresponsive."
- Driver metrics (cluster metrics page): driver memory at 100 % while executors idle.

### 3. Executor OOM and spill — causes and signatures

**Executor memory model (unified memory, Spark ≥ 1.6):** executor heap minus ~300 MB reserved; of the rest, `spark.memory.fraction` (0.6) is *unified* memory shared by **execution** (shuffles, joins, sorts, aggregations) and **storage** (cache); the remainder is "user memory" for your objects. Plus off-heap **overhead** (`spark.executor.memoryOverhead`, ~10 %) for JVM internals, Python workers (PySpark UDFs, pandas UDFs, Arrow buffers), native libraries (Photon uses off-heap memory).

When execution memory for a task is exhausted, sorts/aggregations **spill** to local disk — slow but survivable. OOM happens when something can't spill: a single huge record, a giant array (`collect_list` of a hot key), a hash table for a broadcast/shuffle-hash build side that's too big, Python worker memory (pandas UDF on a huge group), or overhead exceeded.

**Causes:**
- **Skew**: one partition far larger than others (11.1).
- Too few shuffle partitions → each partition too large.
- `collect_list`/`collect_set`/`groupBy().applyInPandas` on a hot key (the whole group must be in memory at once).
- Exploding joins / `explode` of large arrays.
- Wide rows (huge strings/binary/JSON blobs).
- Caching too much (storage evicts, but it competes).

**Signatures:**
- Spark UI stage: **Spill (Memory)** / **Spill (Disk)** columns non-zero (warning sign); high **GC time**; failed tasks with retries.
- `ExecutorLostFailure (executor N exited caused by one of the running tasks) Reason: …` with "Container killed … exceeding memory limits", "Remote RPC client disassociated", or **exit code 137** (killed by the OS/container for memory).
- `java.lang.OutOfMemoryError` in **executor** stderr (Spark UI → Executors → logs).
- Python: `Python worker exited unexpectedly (crashed)` — often a pandas UDF processing a too-large group.
- Cluster event log: executors lost / replaced.

### 4. Quick decision table

| Message / evidence | Where | Which side |
|---|---|---|
| `maxResultSize` exceeded | Task error | Driver (result collection) |
| "driver has stopped unexpectedly and is restarting" | Run page/notebook | Driver |
| Driver memory 100 %, executors idle | Cluster metrics | Driver |
| Spill > 0, max ≫ median | Stage task summary | Executor (skew/partition size) |
| `ExecutorLostFailure`, exit 137 | Task error, executor logs | Executor |
| `Python worker exited unexpectedly` | Task error | Executor Python memory |
| Broadcast timeout / OOM during `BroadcastExchange` | Task error, SQL tab | Driver (and executors) |

### 5. Fixes, ranked

**Driver:**
1. **Don't bring data to the driver.** Write results to tables; `display()` / `limit()` / aggregates; `take(n)` with small n.
2. Replace driver loops with distributed code (built-in functions; `mapInPandas`/pandas UDFs if you must use Python).
3. Remove explicit broadcasts of large tables; check the "small" side's real size.
4. Simplify huge plans (build column lists and use one `select`; avoid unions in loops; checkpoint very long lineages).
5. Use job compute instead of a crowded shared all-purpose cluster.
6. Only then: larger driver / raise `spark.driver.maxResultSize` (raising it alone often turns a clean error into a real OOM).

**Executor:**
1. Fix **skew** (broadcast, AQE skew join, salting, isolate hot keys — 11.1).
2. More/right-sized shuffle partitions so each partition is ~100–200 MB (AQE helps).
3. Avoid non-spillable patterns: replace `collect_list` on hot keys; aggregate before windowing; limit group sizes for pandas UDFs.
4. Project only needed columns early (smaller rows); avoid caching unnecessarily.
5. Then: memory-optimised instance types / larger executors / more overhead (for Python-heavy work).

### 6. Misconceptions

1. **"OOM? Add more workers."** Driver OOM isn't helped by workers at all; skew-caused executor OOM isn't helped either (the hot partition is the same size).
2. **"Spill means OOM."** Spill is Spark protecting itself; it's a performance problem and a warning sign.
3. **"Raise `maxResultSize` to unlimited."** It's a safety net; removing it risks crashing the driver for everyone on the cluster.
4. **"`toPandas()` is fine because Arrow is fast."** Arrow makes the transfer faster, not smaller — all data still lands in driver memory.
5. **"Caching always helps."** Cache competes with execution memory; caching a huge DataFrame used once makes things worse.

---

## B. Practical — failure signatures, safely

**Runnable notebook:** [`exercises/phase-11/03_oom_signatures.py`](../../exercises/phase-11/03_oom_signatures.py)

We don't crash clusters on purpose (a driver OOM restarts the driver and kills colleagues' notebooks). Instead:

| Cell | What you do | Expected observation |
|---|---|---|
| Config | Print `spark.driver.maxResultSize` etc. | Values, or "not readable" on serverless |
| Estimate | `limit(10000).toPandas()` → bytes/row × rows | A 5 M-row `toPandas()` would need roughly 1 GB+ on the driver |
| Safe alternatives | `groupBy…limit(20)`, write to table, `take(5)` | Only small results reach the driver |
| `mapInPandas` | Python logic on executors | No driver memory growth |
| Skewed window | `row_number()` over `partitionBy(customer_id)` with 70 % hot key | Window stage: max ≫ median; **Spill (Memory/Disk)** > 0 on the max task |
| Fix | Hot key split into range buckets + offsets | No dominant task; verify ranks 1..N |

Line-by-line on the key fix:

```python
hot_b = hot.withColumn("bucket", F.floor(F.col("amount") * 100).cast("int"))
```
Range-bucket the hot key's rows by the window's **order** column, so every bucket is an independent, ordered slice.

```python
offsets = bucket_sizes.withColumn("offset", F.coalesce(F.sum("count").over(
    Window.orderBy("bucket").rowsBetween(Window.unboundedPreceding, -1)), F.lit(0)))
```
Running total of the sizes of all previous buckets (only 100 rows — a single-partition window is fine here). Row number inside a bucket + its offset = global row number.

**Common mistakes:** "testing" a driver OOM on a shared cluster; reading `spark.sparkContext` on shared/serverless compute (not available under Spark Connect); interpreting zero spill on a big cluster as "no problem" (look at max vs median).

---

## C. Interview questions

### 🟢 Basic

**B1.** Name three operations that can cause a driver OOM.

<details><summary>Model answer</summary>

`collect()`/`toPandas()` on large data, broadcasting a large table, building huge plans (unions/withColumn in long loops) or planning over millions of files, and pure-Python/pandas processing of large data on the driver.
</details>

**B2.** What is spill?

<details><summary>Model answer</summary>

When a task's execution memory (for sort, aggregation, join buffers) is exhausted, Spark writes intermediate data to local disk and merges it later. It prevents OOM but slows the task. Visible as Spill (Memory)/Spill (Disk) in the stage's task metrics.
</details>

**B3.** Which is worse for a shared cluster — a driver OOM or an executor OOM? Why?

<details><summary>Model answer</summary>

Driver OOM: there's one driver; when it dies the application restarts and every attached notebook/job on that cluster loses state. An executor failure is handled by Spark — tasks are retried on other executors (up to the task-failure limit).
</details>

### 🟡 Medium

**M1.** A notebook fails with "Total size of serialized results … is bigger than spark.driver.maxResultSize". A colleague suggests setting it to 0 (unlimited). Your response?

<details><summary>Model answer</summary>

Symptom: the guard fired because an action returned too much data to the driver. Evidence: find the action (`collect`/`toPandas`/broadcast) in the failing cell. Root cause: design — pulling data to one machine. Fix: keep it distributed (write to a table, aggregate, `display`, `limit`), or for broadcasts reduce the build side. Raising the limit just converts the clean error into a driver OOM that restarts the cluster. Verify the job completes and driver memory stays flat.
</details>

**M2.** A job fails with `ExecutorLostFailure … exit code 137` on the same stage every run. Diagnose.

<details><summary>Model answer</summary>

Exit 137 = process killed for memory. Evidence: Spark UI for that stage — max task input/shuffle read far above median (skew), spill before failure; executor logs with OOM; check the operation (window/collect_list/pandas UDF on a key group, exploding join). Root cause usually one oversized partition or a non-spillable structure. Fix: skew handling, more partitions, change the algorithm (avoid collect_list on hot keys), project fewer columns; memory-optimised nodes or more overhead only if needed (e.g. Python UDF heavy). Verify stage completes with balanced tasks.
</details>

**M3.** How is memory divided inside an executor?

<details><summary>Model answer</summary>

JVM heap: ~300 MB reserved; of the rest, `spark.memory.fraction` (default 0.6) is unified memory shared dynamically between execution (shuffle, sort, join, aggregation) and storage (cache) — execution can evict storage down to `spark.memory.storageFraction`; the remaining ~40 % is user memory. Outside the heap: memory overhead for JVM internals, Python workers, Arrow, and native/off-heap engines like Photon.
</details>

### 🔴 Tricky

**T1.** A pandas UDF (`groupBy("customer_id").applyInPandas`) fails with "Python worker exited unexpectedly" only on month-end runs. Why?

<details><summary>Model answer</summary>

`applyInPandas` loads **each whole group** into one pandas DataFrame in a Python worker. At month end a large customer's group becomes huge (or the data is skewed), exceeding the Python worker's memory (overhead memory, not JVM heap). Fix: reduce group size (pre-aggregate, split groups by time/salt and combine), use built-in Spark functions instead, or increase overhead/memory-optimised nodes as a last resort. Verify with the max group size: `groupBy(key).count()`.
</details>

**T2.** The job has no spill, no skew, but the driver OOMs during planning after a refactor. What changed?

<details><summary>Model answer</summary>

Likely a much larger plan: e.g. a Python loop that `union`s hundreds of DataFrames or adds hundreds of columns one `withColumn` at a time, generating a huge logical plan that Catalyst analyses repeatedly. Or the new code reads a path with millions of files. Fix: build columns in one `select`, union via a single `unionByName` chain on fewer, larger inputs or use a single read with a filter, break lineage with a checkpoint/write-and-reread. Evidence: driver memory climbs before any Spark job appears.
</details>

**T3.** Doubling executor memory made a skewed job *slower*. Possible?

<details><summary>Model answer</summary>

Yes. With the same total budget, bigger executors may mean fewer executors/cores → less parallelism for the many normal tasks, while the hot partition is still one task. Larger heaps also have longer GC pauses. The real fix is splitting the hot partition (broadcast, AQE skew, salting), not memory.
</details>

---

## E. Revision summary

- Driver = one machine: results, broadcasts, planning, plain Python. OOM kills the whole app.
- Executor = many: shuffles, sorts, joins, aggregations, cache; spill first, OOM when data can't spill.
- Driver signatures: `maxResultSize` exceeded, "driver has stopped unexpectedly and is restarting", driver OOM in log4j.
- Executor signatures: spill, GC time, `ExecutorLostFailure`, exit 137, Python worker crashed.
- Fix code first (no collect, fix skew, more partitions, avoid collect_list/huge groups), memory last.