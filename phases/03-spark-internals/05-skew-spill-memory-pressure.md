# 3.5 Data Skew, Spill, Memory Pressure and GC

**Time:** 30 min · **Prerequisites:** 2.5 (partitions), 2.10 (shuffle), 3.3, 3.4

**After this topic you can:**
- define skew and recognise it in the Spark UI (one long task, max ≫ median);
- fix skewed joins with AQE skew handling, broadcast, hot-key isolation or salting — and explain the trade-offs;
- explain spill (memory → disk), what causes it and how to read the spill metrics;
- explain memory pressure and garbage collection (GC) conceptually and their symptoms;
- choose a fix from the symptom.

---

## A. Concept deep dive

### 1. The simplest explanation

- **Skew**: work is split into partitions, but **one partition is much bigger** than the others — so one task works for an hour while 199 finish in seconds. The stage (and the job) is only as fast as its slowest task.
- **Spill**: a task needs more memory than it has for a sort / aggregation / join, so it **writes part of its data to local disk** and reads it back later. Correct, but slow.
- **Memory pressure / GC**: the JVM heap is nearly full, so the garbage collector runs often and long, pausing the task. In the worst case: **OutOfMemoryError** (3.6).

They are related: skew → one huge partition → spill → memory pressure → GC → OOM.

### 2. Why skew happens

Hash partitioning sends **all rows with the same key** to the same partition (2.10). If the key distribution is uneven, partitions are uneven.

| ShopCo example | Hot key |
|---|---|
| `orders` joined on `customer_id`; 90 % of orders are from one marketplace account | `customer_id = 1` |
| `transactions` with missing customer stored as `'UNKNOWN'` or `-1` or `NULL` | placeholder value |
| `clicks` grouped by `country` | `'AE'` has most traffic |
| `events` partitioned by `event_date` | Black Friday |

NULL keys deserve special mention: in an inner/left join, rows with NULL keys never match — but in a shuffle they still all hash to **one partition**. Filter them out (or handle them separately) before the join.

### 3. Recognising skew in the Spark UI

Stage detail page → **Summary Metrics for Completed Tasks**:

```
                 Min     25th pct   Median   75th pct    Max
Duration         0.4 s   0.6 s      0.7 s    0.9 s       38 min     ← max ≫ median = skew
Shuffle Read     1 MB    2 MB       2 MB     3 MB        41 GB
Spill (Disk)     0       0          0        0           22 GB
```

Other signals: event timeline with one long bar; "199/200 tasks complete" for a long time; one executor busy while others idle; AQE plan shows `skew=true` (handled) or not.

**Not skew**: all tasks slow and similar → too few partitions / not enough cores / heavy computation. **Not skew**: only a few tasks slow at random → a straggler node (Spark's speculative execution exists for that).

### 4. Fixing a skewed join — the options

| Fix | How | Pros | Cons |
|---|---|---|---|
| **AQE skew join** (default on) | Splits partitions > 5 × median **and** > 256 MB; replicates the other side's partition | Automatic, no code | Shuffle joins only; join-type limits; doesn't help aggregations; thresholds may not trigger for "moderately" skewed data |
| **Broadcast the other side** | If the non-skewed side is small → BHJ, no shuffle, so no skewed partition | Simple, fastest | Only if that side is small |
| **Filter / isolate the hot key** | Drop NULL/placeholder keys before the join; or process the hot key separately (e.g. broadcast join for it) and `union` | Precise | More code, two paths |
| **Salting** | Add a random/`id % N` *salt* to the skewed side's key; replicate the other side N times; join on `(key, salt)` | Works for any shuffle join and for aggregations | N× more rows on the replicated side; manual; choose N |
| **Skew hint** | Databricks documents a skew hint, but recommends AQE skew handling instead | — | Legacy approach |

**How salting works**

```
orders (skewed)                    customers (replicated N times)
customer_id  salt = order_id % 4   customer_id  salt
1            0                     1            0
1            1                     1            1
1            2                     1            2
1            3                     1            3
1            0  …                  2            0 … 3
                 join ON customer_id AND salt
 hot key 1 is now spread over 4 partitions: (1,0) (1,1) (1,2) (1,3)
```

Salting an **aggregation** (two-phase): `groupBy(key, salt).agg(partial)` → `groupBy(key).agg(combine)`. Works for `sum`, `count`, `min`, `max`; for averages carry `sum` and `count`; for distinct counts use approximate or a different approach.

Choosing N: enough to bring the hot partition near the median × a few (e.g. hot = 40 GB, target ≈ 256 MB → N ≈ 160 just for that key). Salt **only the hot keys** if possible, to avoid multiplying the whole other side.

### 5. Spill

Execution memory (3.6) holds sort buffers, hash tables for aggregates and joins. When it's full:

```
task memory full ─▶ sort what's in memory ─▶ write a sorted "spill file" to local disk ─▶ continue
                    … at the end: merge spill files (read them back) ─▶ output
```

| Metric (Spark UI stage page) | Meaning |
|---|---|
| **Spill (Memory)** | Size of the spilled data in its *deserialized in-memory* form |
| **Spill (Disk)** | Size of the same data *serialized on disk* (smaller) |

Spill ≠ shuffle write. Shuffle write is normal for every wide stage; spill is **extra** disk I/O because memory ran out.

Causes and fixes:

| Cause | Fix |
|---|---|
| Skewed partition | Fix skew (above) |
| Partitions too big (too few shuffle partitions, huge input splits) | More shuffle partitions / AQE advisory size; `spark.sql.files.maxPartitionBytes` (default 128 MB) for scans |
| Wide rows, carrying unused columns | Select only needed columns before the shuffle |
| `explode` / exploding joins multiplying rows | Filter first; check duplicate keys |
| `collect_list` / `collect_set` on big groups | Avoid or bound; aggregate differently |
| Too little memory per core | Memory-optimized instance types, fewer cores per executor |

A little spill is acceptable; spill in the tens of GB on a few tasks is a red flag.

### 6. Memory pressure and garbage collection (conceptual)

Spark executors are JVM processes. Objects created while processing (rows, buffers, cached deserialized data, broadcast relations) live on the **heap**. The **garbage collector** frees objects no longer used — and some collections pause the work.

```
heap usage
 100% ┤          ████        ████  ← full GC pauses, task stalls
      │      ████    ████ ████
  50% ┤  ████
      └──────────────────────────────▶ time
```

| Symptom | Where you see it |
|---|---|
| High **GC Time** relative to task time | Stage task metrics, Executors tab (GC time column) |
| Tasks slow without much spill or shuffle | Stage page |
| Executor heartbeats time out, "executor lost" | Jobs/stages failing and retrying |
| `java.lang.OutOfMemoryError: GC overhead limit exceeded` / `Java heap space` | Task/driver error |

Reducing GC: use DataFrame/SQL instead of RDDs of Python/Java objects (Tungsten stores rows in compact binary form, far fewer objects); avoid caching huge deserialized data you don't reuse; smaller partitions; don't broadcast big tables; enough memory per core. On Databricks, Photon executes supported operators natively in C++ with its own memory management, which reduces JVM GC for those operators. GC tuning flags are rarely the first fix — and on standard access mode DBR 19+ JVM option properties (e.g. `spark.executor.extraJavaOptions`) cannot be set.

### 7. Real DE example — ShopCo

`silver_transactions ⋈ silver_orders` runs for 2 h; yesterday 12 min. Spark UI: stage 7, 400 tasks, median 9 s, max 1 h 48 min, max shuffle read 61 GB, spill 70 GB. `transactions.groupBy("order_id").count().orderBy(desc)` shows `order_id = -1` with 1.4 billion rows — a source system started sending `-1` for "no order".

Fix in order: (1) quarantine/filter `-1` rows before the join (they can never match anyway); (2) data-quality rule + alert on placeholder rates in Silver; (3) if a legitimate hot key remains, rely on AQE skew (check `skew=true` in the final plan) or salt it.

### 8. When to use / not use

- **First** find the cause: `groupBy(key).count()` on the join/group key; look at max vs median in the Spark UI.
- **Prefer** removing bad keys and broadcasting over salting — simpler and cheaper.
- **Use salting** when the skew is legitimate, AQE doesn't handle it (aggregations, thresholds, outer-join side), and broadcast isn't possible.
- **Don't** throw more nodes at skew — one task still processes the hot partition on one core.

### 9. Misconceptions

1. **"More executors fix skew."** The hot partition is still one task.
2. **"`repartition(1000)` fixes skewed joins."** The join re-shuffles by key; the hot key still lands in one partition. (Repartitioning by a non-key column helps only narrow work before the shuffle.)
3. **"Spill means the job will fail."** Spill keeps the job alive at the cost of speed; OOM is what fails.
4. **"AQE handles all skew."** Only shuffle joins, above both thresholds, on allowed sides.
5. **"NULL keys are ignored by joins, so they cost nothing."** They are still shuffled — usually all into one partition.

---

## B. Practical — create skew, see it, fix it with AQE and with salting

**Runnable notebook:** [`exercises/phase-03/05_skew_salting.py`](../../exercises/phase-03/05_skew_salting.py)

### Input

- `sk_orders`: 4,000,000 rows. If `order_id % 10 != 0` → `customer_id = 1` (**90 %** of rows); else `customer_id = (order_id / 10) % 100000 + 2`. `amount = order_id % 1000`.
- `sk_customers`: 100,001 rows, `customer_id` 1–100001.

### Key code

```python
# 1. See the skew: rows per key and per shuffle partition
sk_orders.groupBy("customer_id").count().orderBy(F.desc("count")).show(3)
part = (sk_orders.repartition(200, "customer_id")
          .groupBy(F.spark_partition_id().alias("p")).count())
part.agg(F.max("count"), F.expr("percentile_approx(count, 0.5)")).show()

# 2. Naive sort-merge join (AQE skew handling off where possible)
naive = sk_orders.join(sk_customers.hint("merge"), "customer_id")

# 3. Salted join
SALT = 16
orders_s    = sk_orders.withColumn("salt", (F.col("order_id") % SALT).cast("int"))
customers_s = sk_customers.withColumn("salt", F.explode(F.sequence(F.lit(0), F.lit(SALT - 1))))
salted = orders_s.join(customers_s.hint("merge"), ["customer_id", "salt"])
```

| Step | Explanation |
|---|---|
| `groupBy(customer_id).count()` | Find hot keys — always the first diagnostic |
| `repartition(200, "customer_id")` + `spark_partition_id()` | Simulates the join shuffle and shows rows per partition (an explicit `repartition(n, …)` is not coalesced by AQE) |
| `max` vs `percentile_approx(…, 0.5)` | The same "max vs median" check the Spark UI shows |
| `hint("merge")` | Force a shuffle join so the skew matters (a broadcast join would avoid it — fix #2 in the table above) |
| `order_id % SALT` | Deterministic salt 0–15 spreads key 1 over 16 sub-keys |
| `explode(sequence(0, SALT-1))` | Replicates every customer 16 times so each `(customer_id, salt)` finds its match |
| join on `["customer_id", "salt"]` | Partitioned by both columns → hot key in up to 16 partitions |

### Expected output

| Check | Expected |
|---|---|
| Top key | `customer_id = 1`, count `3,600,000`; other keys `4` each |
| Rows per partition (unsalted) | max ≈ `3,602,000` (hot key + its share of cold keys), median ≈ `2,000` |
| Rows per partition (salted) | max ≈ `225,000` (3.6 M / 16), or a multiple if two salts hash to the same partition |
| Naive, salted and AQE results | identical: `rows = 4,000,000`, `sum(amount) = 1,998,000,000` |
| Spark UI, naive run (skew handling off) | join stage: one task with ~3.6 M rows, max duration ≫ median |
| Spark UI, AQE run (classic, lowered thresholds) | final plan `SortMergeJoin … skew=true` / `AQEShuffleRead … skewed`; hot partition split into several tasks |
| Spark UI, salted run | hot key spread over ~16 tasks; `customers_s` shuffle is 16× larger (1.6 M rows) — the price of salting |

The demo data is small (tens of MB), so AQE's default 256 MB threshold would **not** trigger; the notebook lowers it on classic compute. On **serverless** these confs can't be set — AQE skew handling then only kicks in for genuinely large skew, while salting works everywhere.

### Common mistakes

- Salting both sides randomly — then matching rows get different salts and the join loses rows. One side gets the salt, the other is **replicated** over all salt values.
- Using `rand()` without a seed for the salt and re-running a retried task — fine for joins (each row still matches), but nondeterministic; `id % N` is reproducible.
- Forgetting to drop the `salt` column afterwards.
- Salting every key when only one is hot — multiplies the replicated side for no benefit.

---

## C. Interview questions

### 🟢 Basic

**B1.** What is data skew and why does it slow a job?

<details><summary>Model answer</summary>

Uneven distribution of data across partitions, usually because some key values are far more frequent. Hash partitioning sends all rows of a key to one partition, so one task processes far more data than others; the stage can't finish until that task does, and the task may spill or OOM.
</details>

**B2.** How do you recognise skew in the Spark UI?

<details><summary>Model answer</summary>

On the stage detail page, the task summary metrics show max duration / shuffle read / spill far above the median (e.g. median 5 s, max 40 min); the event timeline shows one long bar; the stage sits at "199/200" for a long time; one executor is busy while others are idle.
</details>

**B3.** What is spill?

<details><summary>Model answer</summary>

When a task's execution memory is insufficient for a sort, aggregation or join buffer, Spark writes intermediate data to local disk and reads it back later. Shown as Spill (Memory) — deserialized size — and Spill (Disk) — serialized size on disk. It keeps the job correct but adds disk I/O and serialization cost.
</details>

### 🟡 Medium

**M1.** Explain salting for a skewed join step by step.

<details><summary>Model answer</summary>

Add a salt column to the skewed side (random or `id % N`, values 0..N-1); replicate each row of the other side N times with salt 0..N-1 (`explode(sequence(0, N-1))`); join on `(key, salt)`. Rows of the hot key now hash to up to N partitions, so N tasks share the work. Cost: the replicated side grows N×. Best applied only to hot keys.
</details>

**M2.** AQE skew join is on, yet a skewed join still has one slow task. Give three possible reasons.

<details><summary>Model answer</summary>

(1) The partition doesn't exceed both thresholds (5× median and 256 MB by default). (2) The skewed side can't be split for that join type (e.g. right side of a LEFT OUTER, either side of FULL OUTER). (3) It isn't a shuffle join, or the slow operation is an aggregation/window, not the join. Also possible: the skew is in a later stage, or the data is a single huge row group/file (input skew).
</details>

**M3.** Spill (Disk) is 50 GB on a stage with no visible skew. What do you check and change?

<details><summary>Model answer</summary>

Partition sizes: if all tasks are big, there are too few partitions for the data (raise the initial shuffle partitions / let AQE size them; check `advisoryPartitionSizeInBytes`), or the rows are too wide (prune columns before the shuffle), or an explode/duplicate-key join multiplied rows. Also memory per core: memory-optimized instances or fewer cores per executor. Check whether a `collect_list`-style aggregate is building huge groups.
</details>

### 🔴 Tricky

**T1.** 40 % of `transactions` have `customer_id IS NULL`. An inner join to `customers` is skewed. Why, given NULLs never match?

<details><summary>Model answer</summary>

Spark may add an `isnotnull` filter for inner joins (inferred filters), but if it doesn't apply — e.g. a LEFT join where unmatched rows must be kept, or a placeholder like `-1`/`'UNKNOWN'` instead of NULL — those rows are still shuffled and all hash to one partition. Fix: filter or split them before the join (for a left join: join only non-null keys and `union` the null-key rows with NULL customer columns).
</details>

**T2.** You salt `groupBy(customer_id).agg(countDistinct(product_id))`. Is the two-phase approach correct?

<details><summary>Model answer</summary>

Not with naive summing: distinct counts per salt can't be added (the same product can appear under several salts of the same customer). Options: salt by a hash of `product_id` instead of random, so each product lands in exactly one salt bucket — then per-bucket distinct counts are disjoint and can be summed; or first `distinct()` on `(customer_id, product_id)` (which itself is evenly spread) then count; or use `approx_count_distinct`.
</details>

**T3.** Adding 4× more workers made a skewed job only 5 % faster. Explain to a manager.

<details><summary>Model answer</summary>

Most of the runtime is a single task processing the hot key's partition on one core; extra machines can only speed up the other tasks, which were already fast. The fix is to split the hot key's work (AQE skew handling, salting, broadcast, or filtering placeholder keys), not to add hardware.
</details>

---

## E. Revision summary

- Skew = one partition ≫ others (hot key, NULL/placeholder keys). Symptom: **max ≫ median** task time/shuffle read/spill.
- Fixes: remove bad keys → broadcast → AQE skew join (shuffle joins, > 5× median **and** > 256 MB) → salting (replicate other side N×; two-phase for aggregations).
- Spill = execution memory full → disk; Spill (Memory) deserialized vs Spill (Disk) serialized; causes: skew, big partitions, wide rows, explode.
- Memory pressure → long GC (GC time in Executors tab) → executor loss/OOM; prefer DataFrames, smaller partitions, less caching/broadcast.
- More nodes don't fix skew.