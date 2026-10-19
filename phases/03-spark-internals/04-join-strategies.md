# 3.4 Join Strategies — Broadcast Hash, Sort-Merge, Shuffle Hash, Nested Loop

**Time:** 35 min · **Prerequisites:** 2.10 (shuffle), 3.1, 3.3

**After this topic you can:**
- explain how each physical join strategy works internally and what it costs;
- predict which strategy Spark picks (size estimates, thresholds, join type, equi vs non-equi, hints);
- use join hints in PySpark and SQL, and know when Spark ignores them;
- name the join in a plan and propose a better one;
- explain the risks of broadcasting (driver/executor memory, timeouts).

---

## A. Concept deep dive

### 1. The simplest explanation

To join `orders` with `customers`, matching rows (same `customer_id`) must meet **on the same machine**. There are two ways to make that happen:

1. **Move the small table to everyone** — copy all of `customers` to every executor (*broadcast*). No shuffle of `orders`.
2. **Move both tables by key** — shuffle both so that all rows with the same `customer_id` land in the same partition (*shuffle join*), then join partition by partition.

Then, within a partition, Spark matches rows either by **hashing** (build a hash table on one side, probe with the other) or by **sorting** both sides and merging. Combine those choices and you get the strategies.

### 2. The strategies

| Strategy | Data movement | Matching | Needs | Typical use |
|---|---|---|---|---|
| **Broadcast hash join (BHJ)** | Small side → every executor; big side stays put | Hash table of small side, probe with big side | Equi-join; one side small enough to broadcast | Fact ⋈ dimension (orders ⋈ customers) |
| **Sort-merge join (SMJ)** | Shuffle **both** sides by key | Sort both partitions, merge | Equi-join, sortable keys | Big ⋈ big (orders ⋈ payments) — Spark's default for large joins |
| **Shuffle hash join (SHJ)** | Shuffle both sides by key | Hash table per partition of the smaller side | Equi-join; build side partitions fit in memory | Big ⋈ medium when sorting is the bottleneck |
| **Broadcast nested loop join (BNLJ)** | Broadcast one side | Compare every pair (loop) | Any condition (non-equi OK) | Range/band joins with a tiny side; fallback |
| **Cartesian product** | Shuffle-replicate | Every pair | Inner/cross, no equi keys | Explicit `crossJoin` — rarely wanted |

### 3. How each works internally

**Broadcast hash join**

```
 Driver: collect customers (2 MB) ─▶ build broadcast relation ─▶ send to every executor
                                                                 │
 Executor 1: orders part 1 ──probe──▶ [hash(customer_id) → row] ─┤  no shuffle of orders
 Executor 2: orders part 2 ──probe──▶ [hash(customer_id) → row] ─┤  one stage for the join
 Executor 3: orders part 3 ──probe──▶ [hash(customer_id) → row] ─┘
```

- Cost: small side collected through the **driver** and stored once per executor; big side read once, never shuffled. Usually the **fastest** join.
- Risks: broadcasting something large → driver OOM, executor memory pressure, `spark.sql.broadcastTimeout` (default 300 s) exceeded. Apache Spark also has a hard limit (a broadcast table larger than 8 GB fails).
- Plan: `BroadcastExchange` + `BroadcastHashJoin … BuildRight` (or `BuildLeft`).

**Sort-merge join**

```
 orders    ── Exchange hashpartitioning(customer_id, N) ── Sort ──┐
                                                                   ├─ SortMergeJoin (merge two sorted streams)
 customers ── Exchange hashpartitioning(customer_id, N) ── Sort ──┘
```

- Cost: **two shuffles + two sorts**. Scales to any size because sorted data can **spill** to disk and still merge.
- Default for large equi-joins (`spark.sql.join.preferSortMergeJoin = true`).
- Plan: two `Exchange hashpartitioning`, two `Sort`, `SortMergeJoin`. (If a side is already partitioned and sorted correctly — e.g. bucketed tables — its exchange/sort can disappear.)

**Shuffle hash join**

- Shuffle both sides like SMJ, but **no sort**: build a hash table from the smaller side's partition and probe with the other.
- Faster than SMJ when the build side partitions fit in memory; risk of OOM/spill if they don't. Chosen when `preferSortMergeJoin` is false and size rules allow, via the `SHUFFLE_HASH` hint, or by AQE (3.3). On Databricks, Photon often favours hash joins — the plan shows what was chosen.

**Broadcast nested loop join**

- Broadcast one side, compare **every** row pair with the join condition. O(n × m).
- Used for **non-equi** conditions (`o.amount BETWEEN b.low AND b.high`, `o.ts > p.start`) and for some outer-join cases with no equi keys. Fine when the broadcast side is tiny; catastrophic when both are big.

### 4. How Spark chooses (Apache Spark rules, simplified)

```
Equi-join (has a = b condition)?
 ├─ yes ─▶ hint present? → follow hint priority: BROADCAST > MERGE > SHUFFLE_HASH > SHUFFLE_REPLICATE_NL
 │         else one side's estimated size ≤ spark.sql.autoBroadcastJoinThreshold (10MB)
 │              and join type allows building on that side?            → BroadcastHashJoin
 │         else preferSortMergeJoin=false and a side is small enough per partition → ShuffledHashJoin
 │         else keys sortable                                          → SortMergeJoin
 │         else                                                       → Cartesian / BNLJ
 └─ no  ─▶ one side broadcastable?  → BroadcastNestedLoopJoin
           inner/cross?             → CartesianProduct
           else                     → BroadcastNestedLoopJoin (even if large — danger)
then AQE may switch SMJ → BHJ/SHJ at runtime (3.3)
```

Which side can be the broadcast/build side depends on the **join type**:

| Join type | Can broadcast left? | Can broadcast right? |
|---|---|---|
| `INNER`, `CROSS` | ✅ | ✅ |
| `LEFT OUTER`, `LEFT SEMI`, `LEFT ANTI` | ❌ | ✅ |
| `RIGHT OUTER` | ✅ | ❌ |
| `FULL OUTER` | ❌ (BHJ not possible) | ❌ |

Rule of thumb: you can't broadcast the side whose unmatched rows must be preserved.

### 5. The thresholds — verify on your runtime

| Property | Apache Spark default | Databricks |
|---|---|---|
| `spark.sql.autoBroadcastJoinThreshold` (planning) | `10485760` (10 MB); `-1` disables | same property; check `spark.conf.get` |
| `spark.sql.adaptive.autoBroadcastJoinThreshold` (AQE) | defaults to the static value | Databricks uses `spark.databricks.adaptive.autoBroadcastJoinThreshold` = **30MB** |
| `spark.sql.broadcastTimeout` | `300` seconds | check runtime |
| `spark.sql.join.preferSortMergeJoin` | `true` | check runtime |

The size compared is Spark's **estimate** of the side *after* filters/projections (from file sizes or statistics), not the row count. Compressed Parquet on disk can be much smaller than in memory — a "9 MB" table can be 100 MB+ once broadcast. On serverless you can't set these properties — use **hints**.

### 6. Join hints

```python
from pyspark.sql import functions as F

orders.join(F.broadcast(customers), "customer_id")              # broadcast
orders.join(customers.hint("broadcast"), "customer_id")         # same
orders.join(customers.hint("merge"), "customer_id")             # sort-merge
orders.join(customers.hint("shuffle_hash"), "customer_id")      # shuffle hash
orders.join(bands.hint("shuffle_replicate_nl"), cond)           # cartesian-style nested loop
```

```sql
SELECT /*+ BROADCAST(c) */ o.*, c.city FROM orders o JOIN customers c ON o.customer_id = c.customer_id;
SELECT /*+ MERGE(o, c) */ ...;          -- aliases: SHUFFLE_MERGE, MERGEJOIN
SELECT /*+ SHUFFLE_HASH(c) */ ...;
SELECT /*+ SHUFFLE_REPLICATE_NL(b) */ ...;
-- BROADCAST also accepts BROADCASTJOIN and MAPJOIN
```

Facts about hints:
- Priority when both sides are hinted: **BROADCAST > MERGE > SHUFFLE_HASH > SHUFFLE_REPLICATE_NL**.
- A hint is a **request**: Spark ignores it if it's impossible (e.g. broadcast the left side of a `LEFT OUTER JOIN`, `SHUFFLE_HASH` for a non-equi join) and logs a warning.
- A broadcast hint **overrides the size threshold** — so it can broadcast something huge and crash the driver. Use only when you know the size.
- AQE respects hints (Databricks docs) and still applies the other runtime optimizations.

### 7. Real DE example — ShopCo

| Join | Sizes | Best strategy | Why |
|---|---|---|---|
| `orders` ⋈ `customers` | 2 TB ⋈ 40 MB | BHJ (hint or raise threshold) | Avoid shuffling 2 TB |
| `orders` ⋈ `payments` | 2 TB ⋈ 1.5 TB | SMJ (default) | Neither fits in memory; SMJ spills safely |
| `orders` ⋈ `fx_rates` on `order_ts BETWEEN valid_from AND valid_to` | 2 TB ⋈ 50 k rows | BNLJ with the small side broadcast — or rewrite as equi-join on `currency, rate_date` | Non-equi condition; adding an equality key lets Spark use BHJ/SMJ |
| `transactions` ⋈ `orders` where 70 % of transactions have `order_id = NULL`-like placeholder `-1` | skewed | SMJ + AQE skew / salting (3.5), or filter placeholder rows out | Hot key |

### 8. When to use / not use

- **Broadcast** when one side is small after filtering (dimensions, lookups, config tables) — a few MB to a few hundred MB depending on memory; measure.
- **Don't broadcast** a side that grows over time without monitoring (today 50 MB, next year 5 GB) — hints don't re-check size.
- **SMJ** for big ⋈ big; make sure the shuffle partitions are right-sized (AQE).
- **Avoid** non-equi joins on large data; add an equality key (bucket/date) so Spark can hash-partition.
- **Watch** for exploding joins: duplicate keys on both sides multiply rows (`n × m` per key) regardless of strategy.

### 9. Misconceptions

1. **"Broadcast is always fastest."** Not when the "small" side is large: driver collection, network to every executor, and memory per executor can make it slower or crash.
2. **"The threshold compares row counts."** It compares estimated **bytes**.
3. **"A hint forces the strategy."** It's ignored when the join type or condition makes it impossible.
4. **"SMJ is bad."** It is the robust, scalable default; it's only "bad" when a broadcast was possible.
5. **"Broadcast happens on the executors only."** The small side is collected to the **driver** first — driver memory matters.

---

## B. Practical — compare join strategies, plans and timings

**Runnable notebook:** [`exercises/phase-03/04_join_strategies.py`](../../exercises/phase-03/04_join_strategies.py)

### Input

| Table | Rows | Columns |
|---|---|---|
| `j_orders` | 5,000,000 | `order_id`, `customer_id = id % 100000 + 1`, `amount = id % 1000` |
| `j_customers` | 100,000 | `customer_id 1–100000`, `city` |
| `j_bands` | 3 | `band`, `low`, `high`: small `[0,100)`, medium `[100,500)`, large `[500,1000)` |

### Key code

```python
def run(df, label):
    t0 = time.perf_counter()
    row = df.agg(F.count("*").alias("n"), F.sum("amount").alias("s")).collect()[0]   # 1
    print(f"{label:<14} rows={row.n:,} sum={row.s:,} {time.perf_counter()-t0:.1f}s")

auto  = j_orders.join(j_customers, "customer_id")                       # 2
bhj   = j_orders.join(F.broadcast(j_customers), "customer_id")          # 3
smj   = j_orders.join(j_customers.hint("merge"), "customer_id")         # 4
shj   = j_orders.join(j_customers.hint("shuffle_hash"), "customer_id")  # 5
band  = j_orders.join(F.broadcast(j_bands),
          (j_orders.amount >= j_bands.low) & (j_orders.amount < j_bands.high))   # 6
for df, label in [(auto,"auto"),(bhj,"broadcast"),(smj,"sort-merge"),(shj,"shuffle-hash"),(band,"band BNLJ")]:
    df.explain(); run(df, label)
```

| # | Explanation |
|---|---|
| 1 | Forces the full join to run while returning one row to the driver (no `collect()` of millions of rows) |
| 2 | Let Spark decide — depends on the estimated size of `j_customers` vs the threshold |
| 3 | Broadcast forced by hint: `BroadcastExchange` + `BroadcastHashJoin`, `j_orders` is not shuffled |
| 4 | `MERGE` hint: two `Exchange hashpartitioning` + two `Sort` + `SortMergeJoin` |
| 5 | `SHUFFLE_HASH` hint: two exchanges, no sorts, `ShuffledHashJoin` |
| 6 | Non-equi condition → `BroadcastNestedLoopJoin` |

### Expected output

| Label | Join node in plan | rows | sum(amount) |
|---|---|---|---|
| auto | `BroadcastHashJoin` if `j_customers` is estimated under the threshold (likely: ~1 MB), else `SortMergeJoin` | 5,000,000 | 2,497,500,000 |
| broadcast | `BroadcastHashJoin … BuildRight` | 5,000,000 | 2,497,500,000 |
| sort-merge | `SortMergeJoin` | 5,000,000 | 2,497,500,000 |
| shuffle-hash | `ShuffledHashJoin … BuildRight` | 5,000,000 | 2,497,500,000 |
| band BNLJ | `BroadcastNestedLoopJoin BuildRight, Inner` | 5,000,000 | 2,497,500,000 |

Same results, different plans. Timings depend on cluster size — typically broadcast is fastest and sort-merge slowest here, because the 5 M-row side is shuffled and sorted. (`sum` = 5,000 × (0 + 1 + … + 999) = 5,000 × 499,500.) On Photon the node names may carry a `Photon` prefix.

The notebook also shows a `LEFT OUTER JOIN` with a broadcast hint on the **left** side — Spark ignores it and the plan shows another strategy.

### Common mistakes

- Timing with `.count()` directly on the join — Spark may optimize away the join's columns; fine for comparison here, but be aware. The notebook sums `amount` so data really flows.
- Timing the first run (includes caching of file metadata, JIT warm-up); run each twice.
- Using `collect()` or `display()` of the whole join to "run" it — that measures the driver, not the join.

---

## C. Interview questions

### 🟢 Basic

**B1.** What is a broadcast join and when would you use it?

<details><summary>Model answer</summary>

The smaller side is collected and sent to every executor; each executor builds a hash table and probes it with its partitions of the large side, so the large side is never shuffled. Use it for large-fact ⋈ small-dimension equi-joins where the small side (after filters) fits comfortably in driver and executor memory.
</details>

**B2.** What is the default join strategy for two large tables, and how does it work?

<details><summary>Model answer</summary>

Sort-merge join: both sides are shuffled by the join key into the same number of partitions, each partition is sorted on the key, and matching rows are found by merging the two sorted streams. It scales because sorting and merging can spill to disk.
</details>

**B3.** What is the default `spark.sql.autoBroadcastJoinThreshold`?

<details><summary>Model answer</summary>

10 MB (10485760 bytes) in Apache Spark; `-1` disables automatic broadcast. Databricks additionally uses a runtime AQE threshold `spark.databricks.adaptive.autoBroadcastJoinThreshold` of 30 MB. Always check your runtime with `spark.conf.get`.
</details>

### 🟡 Medium

**M1.** Why can't Spark broadcast the left table in a LEFT OUTER JOIN?

<details><summary>Model answer</summary>

Each executor sees only its partition of the right (streamed) side. If the left side were broadcast, no single executor could know whether a left row matched *anywhere* — so it couldn't decide whether to emit the NULL-padded unmatched row without duplicates or losses. The preserved side must be the streamed side; only the right side can be the build/broadcast side.
</details>

**M2.** When would you prefer shuffle hash join over sort-merge join?

<details><summary>Model answer</summary>

When both sides are too big to broadcast but one side's per-partition data comfortably fits in memory as a hash table: SHJ skips the two sorts, saving CPU and spill. Risk: if build partitions are larger than memory (e.g. skew), it can spill heavily or OOM. Use the `SHUFFLE_HASH` hint or let AQE choose it.
</details>

**M3.** Your join condition is `o.order_ts BETWEEN p.start_ts AND p.end_ts`. Which strategy will Spark use, and how could you improve it?

<details><summary>Model answer</summary>

No equality → no hash/sort-merge possible → BroadcastNestedLoopJoin (if one side is broadcastable) or Cartesian product. Improve by adding an equi-key: e.g. bucket by date (`to_date(order_ts) = p.date` after exploding each period into its dates), or join on a coarse key plus the range filter, so Spark can use BHJ/SMJ and then apply the range condition.
</details>

### 🔴 Tricky

**T1.** You added `broadcast(customers)` a year ago. Today the job fails with driver OOM. What happened?

<details><summary>Model answer</summary>

The hint bypasses the size threshold. `customers` grew (or the filter before it changed) so the broadcast side became too large: it's collected to the driver and shipped to every executor. Fix: remove/condition the hint, filter/project the dimension before broadcasting, rely on AQE/threshold, or switch to SMJ; add monitoring on dimension size.
</details>

**T2.** Two tables of 50 MB each are joined, the threshold is 10 MB, and the plan shows SortMergeJoin. A teammate sets the threshold to 1 GB "for all jobs". Your view?

<details><summary>Model answer</summary>

For this join, broadcasting 50 MB is reasonable (a targeted hint, or a moderate threshold). Setting 1 GB globally is risky: any side estimated under 1 GB — including compressed tables that expand to several GB in memory — will be collected on the driver and copied to every executor, risking OOMs and broadcast timeouts. Prefer targeted hints or a modest increase, keep statistics accurate, and on Databricks remember AQE already broadcasts up to 30 MB at runtime.
</details>

**T3.** A join returns 10× more rows than `orders` has. The plan is a normal BroadcastHashJoin. What's wrong?

<details><summary>Model answer</summary>

Not the strategy — the data: the "dimension" has duplicate keys (e.g. SCD2 history without a current-row filter, or duplicate loads), so each order matches several rows. Check `groupBy(key).count() > 1` on the dimension; filter to current rows or deduplicate. Strategy changes performance, never results.
</details>

---

## E. Revision summary

- **BHJ**: small side → every executor, no shuffle of the big side; fastest; risk = memory/driver. **SMJ**: shuffle + sort both; robust default. **SHJ**: shuffle, hash per partition, no sort. **BNLJ/Cartesian**: non-equi or no keys — dangerous at scale.
- Choice: hints → size estimate vs `autoBroadcastJoinThreshold` (10 MB OSS) → SHJ/SMJ; AQE may switch at runtime (Databricks adaptive threshold 30 MB).
- Build side limited by join type: LEFT OUTER/SEMI/ANTI → only right; FULL OUTER → no broadcast.
- Hints: `broadcast()`/`BROADCAST`, `MERGE`, `SHUFFLE_HASH`, `SHUFFLE_REPLICATE_NL`; priority in that order; ignored when impossible; broadcast hint ignores size.
- Strategy changes speed, not results; row explosions come from duplicate keys.