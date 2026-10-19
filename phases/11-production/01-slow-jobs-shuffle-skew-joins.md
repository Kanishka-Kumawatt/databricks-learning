# 11.1 Slow Job: Shuffle Bottleneck, Skew, Wrong Join Strategy

**Time:** 30 min · **Prerequisites:** 11.0, Phase 2 (shuffle, stages), Phase 3 (AQE, join strategies)

**After this topic you can:**
- recognise skew, shuffle bottlenecks and a bad join strategy from Spark UI evidence;
- explain *why* a hot key creates one straggler task, mechanically;
- fix skew with broadcast, AQE skew join and salting — and say which to try first;
- answer "the job went from 20 min to 3 h" with a structured diagnosis.

---

## A. Concept deep dive

### 1. The problem in one sentence

A stage can't finish until its **slowest task** finishes. If one task gets 60 % of the data, the stage takes as long as that one task — the other 199 cores sit idle.

### 2. Why it happens internally

A **wide transformation** (join, groupBy, window, distinct) needs all rows with the same key in the same partition. Spark writes **shuffle files**: each map task hashes the key, `partition = hash(key) % spark.sql.shuffle.partitions`, and the reduce tasks each read one partition.

- **Skew**: if one key is very frequent (`customer_id = 'MKT-0001'`, `country = 'AE'`, `status = NULL`), every one of its rows goes to the **same** reduce partition. Changing the number of partitions changes *which* partition, not the fact that it's one.
- **NULL keys** are a classic hidden skew: millions of NULL `customer_id`s in a left join all hash to the same partition (and never match anyway).
- **Shuffle bottleneck without skew**: the shuffle itself is too big — e.g. joining two large tables when one could be broadcast, or shuffling all columns when only three are needed; tasks are slow and similar.
- **Wrong join strategy**:

| Strategy | When Spark picks it | Cost |
|---|---|---|
| **Broadcast hash join** | One side estimated below `spark.sql.autoBroadcastJoinThreshold` (Spark default 10 MB), or a `broadcast` hint; AQE can switch at runtime (Databricks: `spark.databricks.adaptive.autoBroadcastJoinThreshold`, default 30 MB) | Small side copied to every executor; **no shuffle** of the big side |
| **Shuffle hash join** | Preferred by config/hint, or AQE when partitions are small | Shuffle both, hash-build one side per partition |
| **Sort-merge join** | Default for two large sides with equi-join | Shuffle **and sort** both sides |
| **Broadcast nested loop / cartesian** | Non-equi join conditions (`a.ts BETWEEN b.start AND b.end`) without an equality | Can be catastrophically slow (rows × rows) |

A regression often appears when a dimension **grows past the broadcast threshold** (or statistics are missing/stale), and the plan silently changes from broadcast to sort-merge — now the big fact table is shuffled and the hot key concentrates in one task.

### 3. How to recognise it — exact evidence

| Evidence | Skew | Shuffle bottleneck | Wrong join strategy |
|---|---|---|---|
| Stage task summary | **Max duration ≫ median** (minutes vs seconds); max shuffle read ≫ median | All tasks similarly slow; very large total shuffle read/write | Large shuffle on both sides of the join |
| Event timeline | One/few long bars at the end | Uniformly long | — |
| SQL tab / `explain` | SMJ on a hot key; with AQE skew handling: `SortMergeJoin ... isSkew=true` | `Exchange hashpartitioning` with huge data size | `SortMergeJoin` where you expected `BroadcastHashJoin`; `BroadcastNestedLoopJoin`/`CartesianProduct` for non-equi joins |
| Rows per operator | — | — | Join output rows ≫ input rows → **exploding join** (duplicate keys on both sides) |
| Data check | `groupBy(key).count().orderBy(desc)` shows one key with a huge share | — | `DESCRIBE DETAIL` / size of the "small" side grew |
| Progress bar | Stuck at "199/200" for a long time | — | — |

### 4. Fixes, ranked

1. **Avoid the shuffle: broadcast** the small side (`F.broadcast(dim)` or `/*+ BROADCAST(d) */`). Works if the small side fits comfortably in executor and driver memory (tens to a few hundred MB; the driver collects it first). Skew becomes irrelevant.
2. **Filter / project early**: filter NULL keys before the join (and union them back if a left join needs them), select only needed columns, pre-aggregate before joining.
3. **AQE skew join** (Spark 3.0+, on by default from Spark 3.2): `spark.sql.adaptive.enabled` and `spark.sql.adaptive.skewJoin.enabled` = true. At runtime, after the shuffle map stage, AQE sees partition sizes; a partition > `skewedPartitionFactor` (5) × median **and** > `skewedPartitionThresholdInBytes` (256 MB) is split into several tasks, and the matching partition of the other side is replicated. Applies to **sort-merge and shuffle hash joins**, not to aggregations or broadcast joins, and only for the side the join type allows (e.g. the left side of a left outer join).
4. **Salting**: add a random/derived salt `0..N-1` to the hot key on the big side; replicate the matching rows on the other side N times; join on `(key, salt)`. Manual, but works anywhere (including aggregations: aggregate by `(key, salt)` then by `key`).
5. **Isolate the hot key**: process the hot key separately (broadcast its few dimension rows) and union with the normal join.
6. **Bigger executors** only if a single partition genuinely needs more memory and can't be split — rarely the right first answer.

For skewed **aggregations**: `sum`/`count`/`min`/`max` are partially aggregated on the map side, so they usually survive skew; `collect_list`, exact `percentile`, `countDistinct` and windows partitioned by the hot key do not → two-phase aggregation or redesign.

### 5. Shuffle partitions and AQE coalescing

- `spark.sql.shuffle.partitions` (default 200) sets the number of reduce partitions. Too few → huge partitions → spill; too many → thousands of tiny tasks and tiny output files.
- AQE **coalesces** small shuffle partitions at runtime toward `spark.sql.adaptive.advisoryPartitionSizeInBytes` (64 MB). On Databricks you can also set `spark.sql.shuffle.partitions = auto` (Databricks-specific "auto-optimized shuffle"; verify for your runtime).
- On **serverless**, you can set only a handful of Spark properties (`spark.sql.shuffle.partitions`, `spark.sql.files.maxPartitionBytes`, `spark.sql.session.timeZone`, `spark.sql.ansi.enabled`, `spark.sql.legacy.timeParserPolicy`, `spark.databricks.execution.timeout`) — AQE is managed for you.

### 6. Misconceptions

1. **"Increase shuffle partitions to fix skew."** The hot key still lands in one partition.
2. **"AQE fixes all skew."** Only shuffle joins, above thresholds, for supported join sides; not aggregations, windows or broadcast joins.
3. **"Broadcast anything under 1 GB."** The driver collects and every executor holds the table; large broadcasts cause driver OOM and timeouts (`spark.sql.broadcastTimeout`).
4. **"Salting is free."** It multiplies the small side by N and adds complexity; apply it only to hot keys.
5. **"The join was slow because the cluster is small."** Check the task summary first — if one task does the work, a bigger cluster won't help.

---

## B. Practical — reproduce and fix a skewed join

**Runnable notebook:** [`exercises/phase-11/01_skewed_join.py`](../../exercises/phase-11/01_skewed_join.py) — run on classic compute.

| Step | Setting | Expected observation (2–4 workers) |
|---|---|---|
| 0 | `groupBy(customer_id).count()` | Customer 0 has 12 M of 20 M rows |
| 1 | SMJ, broadcast off, AQE skew off | Slowest; join stage max task ≫ median; timeline shows one long bar |
| 2 | AQE skew join on, thresholds lowered for the demo | Faster; SQL tab shows `isSkew=true`, the hot partition split into several tasks |
| 3 | `F.broadcast(customers)` | Fastest; `BroadcastHashJoin`, no exchange on the orders side |
| 4 | Salting with 16 salts | Close to step 2; works without AQE |
| 5–6 | `countDistinct` per customer vs distinct-then-count | Shows skewed aggregation and the two-phase fix |

Line-by-line highlights:

```python
.withColumn("customer_id", F.when(F.col("id") % 100 < HOT_SHARE, F.lit(0)) ...)
```
Deterministic skew: 60 % of rows get key 0 → after `hashpartitioning(customer_id, 200)` they all go to one partition.

```python
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
spark.conf.set("spark.databricks.adaptive.autoBroadcastJoinThreshold", "-1")
```
Disables both the static broadcast and Databricks' AQE runtime broadcast, so we see the "lost broadcast" plan that typically appears in production when a dimension grows.

```python
df.write.format("noop").mode("overwrite").save()
```
The `noop` sink executes the whole plan but writes nothing — the right way to benchmark (`count()` can be optimised away; `display` only computes a sample).

```python
customers_salted = customers.withColumn("salt", F.explode(
    F.when(F.col("customer_id") == 0, F.sequence(F.lit(0), F.lit(SALT - 1))).otherwise(F.array(F.lit(0)))))
```
Replicates **only the hot key's** dimension row 16 times; other keys get salt 0, matching the fact side's salt 0. The verify cell proves the answer didn't change — always verify a salting rewrite.

**Common mistakes:** salting both sides randomly (rows no longer match); forgetting to replicate the dimension; benchmarking with `display()`; leaving lowered AQE thresholds in production; running on serverless and wondering why configs are ignored.

---

## C. Interview questions

### 🟢 Basic

**B1.** What is data skew and how does it show up in the Spark UI?

<details><summary>Model answer</summary>

Uneven distribution of rows across partitions, usually because some key values are very frequent. In the Spark UI the slow stage's task summary shows max duration and max shuffle read far above the median; the timeline shows one or a few long tasks while others finished; progress sits at "199/200".
</details>

**B2.** When does Spark use a broadcast hash join?

<details><summary>Model answer</summary>

When one side of an equi-join is estimated below `spark.sql.autoBroadcastJoinThreshold` (10 MB by default in Spark), when a broadcast hint is given, or when AQE sees at runtime that a side is small (Databricks: `spark.databricks.adaptive.autoBroadcastJoinThreshold`, 30 MB). The small side is collected and sent to every executor, so the big side isn't shuffled.
</details>

**B3.** Why doesn't increasing `spark.sql.shuffle.partitions` fix skew?

<details><summary>Model answer</summary>

Rows are assigned to partitions by hash of the key. All rows with the hot key have the same hash, so they always land in the same single partition, whatever the partition count. More partitions only make the non-hot partitions smaller.
</details>

### 🟡 Medium

**M1.** Explain how AQE's skew join optimisation works and its limits.

<details><summary>Model answer</summary>

After the shuffle map stage completes, AQE has real partition sizes. A partition larger than `skewedPartitionFactor` × median and above `skewedPartitionThresholdInBytes` is split into multiple smaller tasks, each joined with a copy of the corresponding partition from the other side. Limits: only sort-merge/shuffle hash joins; only when AQE and skew join are enabled; only above thresholds; only the sides permitted by the join type (e.g. not the right side of a left outer join); doesn't help aggregations or windows.
</details>

**M2.** A job became slow after the customer dimension grew from 8 MB to 60 MB. What probably happened?

<details><summary>Model answer</summary>

Symptom: step-change slowdown. Evidence: SQL tab shows `SortMergeJoin` where earlier runs had `BroadcastHashJoin`; large exchange on the fact side; possibly skew in the join stage. Root cause: the dimension crossed the broadcast threshold, so the fact table is now shuffled (and skewed keys concentrate). Fix: broadcast hint or raise the threshold if 60 MB is safe for driver/executors; or rely on AQE runtime broadcast; reduce dimension columns before the join. Verify the plan and duration, and reconcile the output.
</details>

**M3.** How does salting work, and what does it cost?

<details><summary>Model answer</summary>

Add a salt column `0..N-1` to the skewed side for hot keys (e.g. `order_id % N`), replicate the matching rows on the other side N times (explode a sequence), and join on `(key, salt)`. The hot key is spread over N partitions. Cost: the replicated side grows by N× for those keys, extra code, and you must verify results; for aggregations you need a second aggregation step.
</details>

### 🔴 Tricky

**T1.** A left join of 2 billion events to users is stuck on the last task. 30 % of events have `user_id = NULL`. What's going on and how do you fix it?

<details><summary>Model answer</summary>

NULL keys all hash to the same partition, so one task gets 600 M rows — and none of them can ever match. Fix: split the input — join only non-NULL keys, then `unionByName` the NULL-key rows with NULL user columns (or filter them if the business allows). Verify the row count equals the original event count. AQE skew join may also help, but removing pointless work is better.
</details>

**T2.** You added a broadcast hint and the job now fails with a driver OutOfMemoryError. Why?

<details><summary>Model answer</summary>

To broadcast, Spark first collects the small side to the **driver**, builds a hash relation and sends it to executors. If the "small" table is actually large (or explodes after filters weren't applied, or is a wide table), the driver runs out of memory (or the build exceeds limits/timeouts). Check the actual size; select only join keys and needed columns; filter first; or use sort-merge with AQE skew handling/salting instead.
</details>

**T3.** After a join, the output has 50× more rows than the fact table and the job takes hours. Is that skew?

<details><summary>Model answer</summary>

It's an **exploding (many-to-many) join**, not (only) skew: the join key isn't unique on the side you assumed was a dimension (e.g. SCD2 dimension joined without `is_current`/date range, or duplicates in a lookup). Evidence: SQL tab rows-out of the join operator ≫ rows-in; `groupBy(key).count() > 1` on the dimension. Fix the join condition or dedupe the dimension; add a uniqueness check on dimension keys.
</details>

---

## E. Revision summary

- Stage time = slowest task. Skew = one hot key → one partition → one task.
- Evidence: task summary **max ≫ median**, long single bar in timeline, `isSkew=true`, `SortMergeJoin` where broadcast was expected, rows-out ≫ rows-in (exploding join).
- Fix order: **broadcast** → filter/project (NULL keys!) → **AQE skew join** → **salting** / isolate hot key → bigger executors.
- AQE skew join: SMJ/SHJ only; factor 5 × median and > 256 MB; not aggregations/windows.
- Benchmark with the `noop` sink; verify results after rewrites.
- Serverless: only a few Spark configs are settable.