# 3.3 Adaptive Query Execution (AQE)

**Time:** 20 min · **Prerequisites:** 2.10 (shuffle), 3.1, 3.2

**After this topic you can:**
- explain what AQE is and why static planning alone is not enough;
- name its main optimizations — coalescing shuffle partitions, switching join strategy at runtime, skew-join handling, empty-relation propagation — and how each works;
- state the key defaults (Apache Spark vs Databricks) and where they are documented;
- recognise AQE in a plan (`AdaptiveSparkPlan`, `isFinalPlan`, `AQEShuffleRead`, skew flags);
- say what AQE does **not** do.

---

## A. Concept deep dive

### 1. The simplest explanation

Catalyst plans a query **before** it runs, using **estimates**. Estimates are often wrong — after a filter, after a join, after a `groupBy`, nobody knows exactly how big the data will be.

**AQE re-plans the rest of the query while it runs**, each time a shuffle (or broadcast) stage finishes, because at that moment Spark knows the **actual** size of every shuffle partition.

> Static plan = planning a road trip from a 5-year-old map. AQE = checking live traffic at every junction.

### 2. Why it exists — the problems with static plans

| Problem | Static plan without AQE | AQE fix |
|---|---|---|
| `spark.sql.shuffle.partitions = 200` for every shuffle | 200 tiny tasks for 50 MB of data (overhead), or 200 huge tasks for 5 TB (spill) | **Coalesce** small post-shuffle partitions into fewer, right-sized ones |
| Size estimate of a filtered/joined side is wrong | Sort-merge join of 5 TB with something that turns out to be 3 MB | **Switch** to broadcast hash join at runtime |
| One join key holds 90 % of rows | One task runs for an hour, 199 finish in seconds | **Split** the skewed partition into several tasks |
| A side turns out empty | Still runs the join/aggregate | **Propagate** the empty relation, skip work |

### 3. How it works internally

```
 Query plan split into "query stages" at every Exchange (shuffle / broadcast)

   Stage A (scan orders → partial agg → shuffle write)      Stage B (scan customers → shuffle write)
              │                                                         │
              └──────────── both finish: AQE now has MapOutputStatistics ┘
                             (bytes per shuffle partition, row counts)
                                         │
                       re-optimize the REMAINING plan with real sizes
                       ├─ coalesce 200 partitions → 12
                       ├─ SMJ → BHJ if one side is now below the adaptive broadcast threshold
                       └─ split skewed partition 37 into 6 sub-partitions
                                         │
                                 run next stage(s) … repeat at the next Exchange
```

Key point: AQE decides **at stage boundaries**. Work already done (e.g. both sides already shuffled) is not undone — AQE just uses it better (e.g. a local shuffle reader after converting to BHJ).

### 4. The four optimizations

#### a) Coalescing post-shuffle partitions

Spark starts with a large initial number of shuffle partitions (default `spark.sql.shuffle.partitions = 200`, or `…coalescePartitions.initialPartitionNum` if set). After the map stage, adjacent small partitions are merged.

```
 before:  [2MB][1MB][3MB][0.5MB][4MB][1MB] … 200 partitions
 after :  [   ~target size   ][   ~target size   ] …  e.g. 12 partitions
```

| Config (Apache Spark) | Default | Meaning |
|---|---|---|
| `spark.sql.adaptive.coalescePartitions.enabled` | `true` | Turn coalescing on/off |
| `spark.sql.adaptive.advisoryPartitionSizeInBytes` | `64MB` | Target size when coalescing (and when splitting skew) |
| `spark.sql.adaptive.coalescePartitions.parallelismFirst` | `true` | Ignore the advisory size and only respect `minPartitionSize`, to keep parallelism high — Spark docs recommend setting it to `false` on busy clusters |
| `spark.sql.adaptive.coalescePartitions.minPartitionSize` | `1MB` | Smallest partition after coalescing |

Databricks also offers **auto-optimized shuffle**: set `spark.sql.shuffle.partitions = auto` (serverless default is `auto`) and the initial number is chosen from the plan and input size.

#### b) Switching the join strategy at runtime

If, after the shuffle, one side's actual size is below the **adaptive** broadcast threshold, AQE replaces the planned `SortMergeJoin` with a `BroadcastHashJoin` (and can read the already-written shuffle files locally — `spark.sql.adaptive.localShuffleReader.enabled`, default `true`).

| Setting | Apache Spark | Databricks |
|---|---|---|
| Static threshold (planning time) | `spark.sql.autoBroadcastJoinThreshold` = `10MB` | same property, check runtime |
| Adaptive threshold (runtime) | `spark.sql.adaptive.autoBroadcastJoinThreshold` — defaults to the static value | `spark.databricks.adaptive.autoBroadcastJoinThreshold` = **`30MB`** |

AQE can also prefer a **shuffled hash join** over sort-merge when every post-shuffle partition is small enough (`spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold`, default `0` = off in Apache Spark).

Databricks notes that a broadcast **planned up front** (statistics or a hint) is usually faster than one AQE decides later, because AQE may only switch after both sides were already shuffled.

#### c) Skew-join handling

A partition is **skewed** only if **both** are true (Apache Spark and Databricks defaults):

```
size > spark.sql.adaptive.skewJoin.skewedPartitionFactor (5) × median partition size
AND
size > spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes (256MB)
```

AQE splits the skewed partition into several sub-partitions (around the advisory size) and **replicates** the matching partition of the other side to each, so several tasks share the work:

```
 orders  partition 37 (customer C1, 4 GB) ──▶ 37a 37b 37c 37d … each ~advisory size
 customers partition 37 (C1, 1 row)      ──▶ copied to 37a 37b 37c 37d …
```

Limits: only for **shuffle-based joins** (sort-merge and shuffle hash), never broadcast joins; the side that can be split depends on the join type (e.g. `LEFT OUTER` only the left side, `FULL OUTER` neither — Databricks docs table). Skewed **aggregations** (`groupBy` on a hot key) are not split by this feature — partial aggregation already reduces them, otherwise use salting (3.5).

#### d) Empty-relation propagation

If a stage produces zero rows, the parts of the plan that depend on it (e.g. inner join with it) are replaced by an empty `LocalTableScan`.

### 5. Defaults and scope

| Fact | Value / source |
|---|---|
| `spark.sql.adaptive.enabled` | `true` by default **since Spark 3.2.0** (Spark SQL performance tuning docs) |
| On Databricks | AQE enabled by default (`spark.databricks.optimizer.adaptive.enabled = true`) |
| Applies to | Non-streaming queries with at least one exchange (join, aggregate, window) or subquery |
| Does not apply to | Structured Streaming micro-batch plans; plans with no shuffle |
| Does **not** do | Reorder joins (use statistics/CBO, 3.2) |

All defaults can change between versions — check with `spark.conf.get(...)`. On **serverless** you can't set most of these properties (only a short allow-list such as `spark.sql.shuffle.partitions`, `spark.sql.ansi.enabled`, `spark.sql.session.timeZone`, `spark.sql.files.maxPartitionBytes`…), so AQE runs with platform defaults there.

### 6. Seeing AQE in a plan

| Plan feature | Meaning |
|---|---|
| `AdaptiveSparkPlan isFinalPlan=false` | AQE-enabled plan, not yet (fully) executed — what `explain()` shows before running |
| `AdaptiveSparkPlan isFinalPlan=true` | Final, executed plan (Spark UI SQL tab; or `explain()` on the same DataFrame after an action on classic compute) |
| `AQEShuffleRead coalesced` (older name `CustomShuffleReader`) | Partitions were coalesced |
| `AQEShuffleRead … skewed` / `SortMergeJoin(skew=true)` (Databricks UI: `isSkew` true) | Skew join handling applied |
| `BroadcastHashJoin` in final plan where initial had `SortMergeJoin` | Runtime join switch |
| `LocalTableScan <empty>` | Empty-relation propagation |
| `Statistics(sizeInBytes=…, isRuntime=true)` (Databricks explain output) | Runtime statistics vs compile-time estimates (`isRuntime=false`) |

`SQL EXPLAIN` never runs the query, so it shows only the initial plan.

### 7. Real DE example — ShopCo

Nightly job: `orders` (2 TB) joined to `customers` filtered to `segment = 'gold' AND country = 'AE'`, then aggregated by day.

- Planner: no stats on the filtered `customers` → estimates 4 GB → `SortMergeJoin`, 200 shuffle partitions.
- At runtime: filtered customers = 6 MB → **AQE switches to BHJ** (under the 30 MB Databricks adaptive threshold).
- The daily aggregate shuffle produces 1.2 GB over 200 partitions → **coalesced** to ~20 partitions of ~64 MB.
- Before AQE (Spark 2.x) engineers hand-tuned `spark.sql.shuffle.partitions` per job and added hints everywhere; today most jobs need neither.

### 8. When to use / not use (tune)

- **Leave it on.** Disabling AQE is almost never right; when debugging a plan change, compare initial vs final plan instead.
- **Still add** broadcast hints/statistics when you know a side is small — up-front broadcast avoids shuffling the big side at all.
- **Tune** `advisoryPartitionSizeInBytes` if post-shuffle tasks are too big (spill) or too small; lower the skew thresholds only if you have evidence of skew below 256 MB that matters.
- **Don't** expect AQE to fix skewed aggregations, skew in broadcast joins, or a bad join order.

### 9. Misconceptions

1. **"AQE makes `spark.sql.shuffle.partitions` irrelevant."** It's still the starting number; AQE only **coalesces** (reduces) — it doesn't add partitions beyond it (except skew splits). Too low a starting value still means huge tasks.
2. **"AQE fixes all skew."** Only skewed partitions in shuffle-based **joins**, above both thresholds.
3. **"`explain()` shows the AQE result."** Before execution it shows the initial plan.
4. **"AQE works in streaming."** Not for streaming queries.
5. **"AQE re-plans continuously."** Only at query-stage (exchange) boundaries.

---

## B. Practical — watch AQE coalesce partitions and switch a join

**Runnable notebook:** [`exercises/phase-03/03_aqe_demo.py`](../../exercises/phase-03/03_aqe_demo.py)

### Input

- `aqe_orders`: 2,000,000 rows, `customer_id = id % 5000 + 1`, `amount = id % 1000`.
- `aqe_customers`: 1,000,000 rows with a 64-character `profile` hash column (so the table is tens of MB on disk — larger than 10 MB).

### Key code

```python
agg = aqe_orders.groupBy("customer_id").agg(F.sum("amount").alias("revenue"))   # 1
n = agg.select(F.spark_partition_id().alias("p")).distinct().count()            # 2

small = aqe_customers.filter(F.col("customer_id") % 1000 == 0)                    # 3
j = aqe_orders.join(small, "customer_id")                                        # 4
j.explain()                                                                      # 5
j.agg(F.count("*")).collect()                                                    # 6
j.explain()                                                                      # 7
```

| # | Explanation |
|---|---|
| 1 | One shuffle by `customer_id`; initial partitions = `spark.sql.shuffle.partitions` |
| 2 | Counts how many post-shuffle partitions actually held data after AQE |
| 3 | A filter whose selectivity the planner can't estimate without column stats → it assumes the side is still large |
| 4 | Join planned before execution |
| 5 | Initial plan: expect `AdaptiveSparkPlan isFinalPlan=false` with `SortMergeJoin` |
| 6 | Run it — AQE sees `small` is ~1,000 rows after the shuffle |
| 7 | On classic compute, explaining the same DataFrame after the action shows the final plan with `BroadcastHashJoin` (on serverless/Spark Connect check the query profile instead) |

### Expected output

| Check | Expected |
|---|---|
| Partitions used by `agg` with AQE coalescing on | far fewer than 200 (exact number depends on cores and data size) |
| Same with `spark.sql.adaptive.coalescePartitions.enabled=false` (classic only) | `200` |
| `agg.count()` | `5000` |
| Initial join plan | `SortMergeJoin` under `AdaptiveSparkPlan isFinalPlan=false` (if your runtime already estimated the filtered side as small, you'll see `BroadcastHashJoin` from the start — that's fine) |
| Final join plan / Spark UI | `BroadcastHashJoin` with an `AQEShuffleRead local` |
| Join row count | `2000` — `small` has 1,000 customers (ids 1000, 2000, …), but only 5 of them (1000–5000) exist in orders, each with 400 orders → 5 × 400 |

### Common mistakes

- Concluding "AQE didn't work" from `explain()` output taken before the query ran.
- Disabling AQE to "make plans predictable".
- Setting `spark.sql.shuffle.partitions` to 10 on a TB-scale job because "AQE coalesces anyway" — AQE never increases it.

---

## C. Interview questions

### 🟢 Basic

**B1.** What is Adaptive Query Execution?

<details><summary>Model answer</summary>

A Spark SQL feature that re-optimizes the remaining physical plan during execution, at each shuffle/broadcast stage boundary, using actual runtime statistics (shuffle partition sizes). Enabled by default since Spark 3.2 and on Databricks.
</details>

**B2.** Name three things AQE can change at runtime.

<details><summary>Model answer</summary>

(1) Coalesce small post-shuffle partitions into fewer right-sized ones. (2) Convert a sort-merge join into a broadcast hash join (or prefer shuffled hash join) when a side turns out small. (3) Split skewed partitions in shuffle-based joins. Also: propagate empty relations.
</details>

**B3.** Is AQE enabled by default?

<details><summary>Model answer</summary>

Yes — `spark.sql.adaptive.enabled` defaults to `true` since Apache Spark 3.2.0, and Databricks enables it by default. It applies to non-streaming queries that have at least one exchange or subquery.
</details>

### 🟡 Medium

**M1.** When does AQE consider a join partition skewed?

<details><summary>Model answer</summary>

When it is larger than `skewedPartitionFactor` (default 5) × the median partition size **and** larger than `skewedPartitionThresholdInBytes` (default 256 MB). It then splits that partition and replicates the other side's matching partition. Only for sort-merge and shuffle hash joins, and only on the side(s) the join type allows.
</details>

**M2.** Why might AQE switch to a broadcast join even though `explain()` showed sort-merge?

<details><summary>Model answer</summary>

`explain()` shows the plan from compile-time estimates. After the shuffle map stages ran, AQE knew the real size of one side was below the adaptive broadcast threshold (Databricks: `spark.databricks.adaptive.autoBroadcastJoinThreshold`, 30 MB; OSS: `spark.sql.adaptive.autoBroadcastJoinThreshold`, defaulting to the 10 MB static threshold) and replaced the join with BHJ, reading shuffle output locally.
</details>

**M3.** Does AQE remove the need to set `spark.sql.shuffle.partitions`?

<details><summary>Model answer</summary>

Mostly for small/medium data, because AQE coalesces down. But it only merges partitions — it doesn't create more than the initial number (skew splitting aside). For very large shuffles, a too-low initial value still yields huge, spilling tasks. Databricks also offers `spark.sql.shuffle.partitions=auto` (serverless default) to pick the initial number automatically.
</details>

### 🔴 Tricky

**T1.** AQE is on, but a `groupBy(customer_id)` where one customer has 80 % of rows still has one 40-minute task. Why didn't AQE help, and what do you do?

<details><summary>Model answer</summary>

AQE skew handling targets **joins**, not aggregations. All rows of one key must end up in one reducer for the final aggregate. Partial (map-side) aggregation already shrinks simple aggregates like `sum`/`count`; if the task is still slow the aggregate probably can't be partially combined well (e.g. `collect_list`, `countDistinct`, a UDAF). Fix: two-phase aggregation with a salt (aggregate by `(customer_id, salt)` then by `customer_id`), or handle the hot key separately.
</details>

**T2.** A broadcast hint is on a `LEFT OUTER JOIN`'s left (preserved) table. What happens?

<details><summary>Model answer</summary>

A broadcast hash join cannot build on the preserved side of an outer join — for `LEFT OUTER` only the right side can be broadcast. Spark ignores the hint (it may log a warning) and uses another strategy; AQE also can't broadcast the left side of a `LEFT OUTER JOIN` (Databricks FAQ).
</details>

**T3.** Your teammate disabled AQE because "query plans kept changing between runs". Respond.

<details><summary>Model answer</summary>

Changing plans is AQE's purpose — it adapts to the data each run (sizes differ day to day). Disabling it loses coalescing, runtime broadcast and skew handling, usually making jobs slower and more brittle. If a specific runtime decision is bad, fix that decision: a hint, statistics, a targeted threshold change, or investigate the data change. Compare initial vs final plans in the Spark UI to understand what changed.
</details>

---

## E. Revision summary

- AQE = re-plan at **every shuffle/broadcast boundary** using **actual** partition sizes; default on since Spark 3.2 and on Databricks.
- Four tricks: **coalesce partitions**, **SMJ → BHJ** (Databricks adaptive threshold 30 MB), **skew-join split** (> 5× median **and** > 256 MB), **empty relation propagation**.
- Plan markers: `AdaptiveSparkPlan isFinalPlan`, `AQEShuffleRead coalesced/local/skewed`, skew flag on SMJ.
- Not for streaming, doesn't reorder joins, doesn't split skewed aggregations or broadcast joins.
- `explain()` before running = initial plan; Spark UI / query profile = final plan. Serverless: most AQE confs are not settable.