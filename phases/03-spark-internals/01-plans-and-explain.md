# 3.1 Logical vs Physical Plans — Reading `explain()`

**Time:** 30 min · **Prerequisites:** 2.6 (jobs/stages/tasks), 2.8 (lazy evaluation), 2.10 (shuffle)

**After this topic you can:**
- describe the four plans Spark builds for every DataFrame / SQL query (parsed → analyzed → optimized logical → physical);
- run `explain()` in its different modes and know which one to use;
- read a physical plan bottom-up and point to the scan, the filters, the shuffles (`Exchange`) and the join strategy;
- read `FileScan` details — `PushedFilters`, `PartitionFilters`, `ReadSchema` — and say whether pushdown worked.

---

## A. Concept deep dive

### 1. The simplest explanation

When you write a DataFrame chain or a SQL query, Spark does **not** run your code line by line. It first turns it into a **plan** — a tree of operations — improves that plan, and only then decides *how* to execute it on the cluster.

- **Logical plan** = **WHAT** you want ("orders where amount > 500, joined to customers, summed by city"). No machines, no algorithms.
- **Physical plan** = **HOW** to do it ("scan these Parquet files reading only 3 columns, filter, shuffle by `customer_id` into 200 partitions, sort-merge join…").

`explain()` prints these plans **without running the query**. It is the single most useful debugging tool for "why is this query slow?".

### 2. Why plans exist

Because Spark evaluates lazily (2.8), it sees the **whole** query before running anything. That lets it:

| Benefit | Example |
|---|---|
| Reorder and simplify your logic | Move a `filter` below a `join` so fewer rows are joined |
| Read less data | Read only 3 of 40 columns; skip row groups/files that cannot match |
| Pick the best algorithm | Broadcast a 2 MB `customers` table instead of shuffling 500 GB of `orders` |
| Generate fast code | Fuse several operators into one compiled Java function (whole-stage codegen) |

The same logical query written in SQL, PySpark or Scala produces the **same plan** — that is why there is no performance difference between them for DataFrame/SQL code (Python UDFs are the exception, Phase 4).

### 3. How it works internally — the pipeline

```
 SQL text / DataFrame code
          │  parse
          ▼
 ① Parsed (unresolved) logical plan   names not checked yet: 'orders, 'amount
          │  analysis — resolve tables, columns, types against the catalog
          ▼
 ② Analyzed logical plan              orders#12, amount#3: decimal(10,2)   ← errors like
          │                                                                   UNRESOLVED_COLUMN come from here
          │  logical optimization — Catalyst rules (3.2)
          ▼
 ③ Optimized logical plan             filters pushed down, columns pruned, constants folded
          │  physical planning — strategies pick operators (join type, aggregate type)
          ▼
 ④ Physical plan                      FileScan, Filter, Exchange, SortMergeJoin, HashAggregate…
          │  code generation (WholeStageCodegen) → RDD of tasks
          ▼
    Jobs → stages → tasks on executors (Phase 2)
```

With **AQE** (3.3, on by default since Spark 3.2) the physical plan is wrapped in `AdaptiveSparkPlan` and can still **change while the query runs**.

### 4. `explain()` modes

| Call | What it prints | Use it for |
|---|---|---|
| `df.explain()` (= `"simple"`) | Physical plan only | Quick look: join type, exchanges, scan |
| `df.explain(True)` (= `"extended"`) | All four plans | See what the optimizer changed vs your code |
| `df.explain("formatted")` | Physical plan outline + numbered node details | **Best for reading** — long lines split into sections |
| `df.explain("cost")` | Optimized logical plan with `Statistics(sizeInBytes, rowCount)` | Check what size estimates the planner used |
| `df.explain("codegen")` | Generated Java code | Rarely needed |
| SQL: `EXPLAIN [EXTENDED / FORMATTED / COST / CODEGEN] SELECT …` | Same, from SQL | SQL users, notebooks |

`explain()` does **not** execute the query, so with AQE it shows the **initial** plan (`isFinalPlan=false`). To see what AQE actually did, look at the SQL tab of the Spark UI (or the query profile on serverless), or call `explain()` on the same DataFrame *after* an action on classic compute (3.3).

### 5. How to read a physical plan

**Rule 1 — read bottom-up.** Leaves (scans) are at the bottom; the final result is at the top. Data flows **up**.
**Rule 2 — every `Exchange` is a shuffle** → a stage boundary (2.10).
**Rule 3 — `*(n)` or `WholeStageCodegen (n)`** marks operators fused into one generated function; `n` is a codegen id (not the Spark UI stage id).

Representative output (abbreviated, Apache Spark, Parquet source; IDs like `#12` and exact wording vary by version):

```
df = (spark.read.parquet(path)                         # ShopCo orders
        .filter("amount > 500")
        .select("order_id", "customer_id"))
df.explain()

== Physical Plan ==
*(1) Project [order_id#0L, customer_id#1L]
+- *(1) Filter (isnotnull(amount#2) AND (amount#2 > 500.00))
   +- *(1) ColumnarToRow
      +- FileScan parquet [order_id#0L,customer_id#1L,amount#2]
           Batched: true,
           DataFilters: [isnotnull(amount#2), (amount#2 > 500.00)],
           Format: Parquet, Location: InMemoryFileIndex(1 paths)[/Volumes/main/de_prep/...],
           PartitionFilters: [],
           PushedFilters: [IsNotNull(amount), GreaterThan(amount,500.00)],
           ReadSchema: struct<order_id:bigint,customer_id:bigint,amount:decimal(10,2)>
```

| Node / field | Meaning |
|---|---|
| `FileScan parquet [...]` | Reads files; the bracket lists the **only** columns read (column pruning) |
| `ReadSchema` | Exact columns + types requested from the file. Contains `amount` even though it is not selected — the filter needs it |
| `PushedFilters` | Predicates handed to the Parquet reader, which uses row-group min/max statistics to **skip** chunks that cannot match |
| `DataFilters` | Filters on data (non-partition) columns |
| `PartitionFilters` | Filters on partition columns → whole directories skipped (partition pruning). Empty here: data not partitioned |
| `ColumnarToRow` | Parquet is read in columnar batches, converted to rows for the next operators |
| `Filter` above the scan | Spark **still** filters each row — pushdown skips chunks, it does not guarantee only matching rows come back |
| `Project` | The `select` |

A join plan (abbreviated, both sides large → sort-merge):

```
AdaptiveSparkPlan isFinalPlan=false
+- HashAggregate(keys=[city#20], functions=[sum(amount#2)])
   +- Exchange hashpartitioning(city#20, 200), ENSURE_REQUIREMENTS        ← shuffle #3
      +- HashAggregate(keys=[city#20], functions=[partial_sum(amount#2)]) ← map-side partial agg
         +- Project [amount#2, city#20]
            +- SortMergeJoin [customer_id#1L], [customer_id#18L], Inner   ← join strategy
               :- Sort [customer_id#1L ASC NULLS FIRST], false, 0
               :  +- Exchange hashpartitioning(customer_id#1L, 200)       ← shuffle #1 (orders)
               :     +- Filter isnotnull(customer_id#1L)
               :        +- FileScan parquet [customer_id#1L,amount#2] ...
               +- Sort [customer_id#18L ASC NULLS FIRST], false, 0
                  +- Exchange hashpartitioning(customer_id#18L, 200)      ← shuffle #2 (customers)
                     +- Filter isnotnull(customer_id#18L)
                        +- FileScan parquet [customer_id#18L,city#20] ...
```

How to narrate it in an interview: *"Two scans, each reading only the needed columns. Each side is shuffled by `customer_id` (two exchanges), sorted, and sort-merge joined. Then a partial aggregate per partition, a third shuffle by `city`, and the final aggregate. Three exchanges → at least four stages. Note the `isnotnull` filters: Catalyst added them because an inner-join key can never match on NULL."*

The same join with a small `customers` side would show `BroadcastHashJoin … BuildRight` and a `BroadcastExchange` instead of two `Exchange hashpartitioning` + `Sort` (3.4).

### 6. Node cheat sheet

| Node | What it means | Cost signal |
|---|---|---|
| `FileScan` / `Scan parquet` / `PhotonScan` | Read files (Photon names appear on Databricks when Photon runs the operator) | Check pruning and pushed filters |
| `Filter`, `Project` | WHERE, SELECT | Cheap (narrow) |
| `Exchange hashpartitioning(k, n)` | Shuffle by key into `n` partitions | **Expensive** — network + disk |
| `Exchange SinglePartition` | Everything to one partition (e.g. window without `partitionBy`) | Danger: one task |
| `Exchange rangepartitioning` | Shuffle for a global `orderBy` | Expensive |
| `BroadcastExchange` | Collect a small side to the driver and send it to every executor | Fine if small; OOM risk if not |
| `HashAggregate` (`partial_…` then final) | groupBy in two steps around a shuffle | Partial step shrinks the shuffle |
| `SortAggregate` / `ObjectHashAggregate` | Other aggregate implementations (used for some aggregate types) | Usually slower |
| `SortMergeJoin`, `BroadcastHashJoin`, `ShuffledHashJoin`, `BroadcastNestedLoopJoin`, `CartesianProduct` | Join strategy (3.4) | BNLJ/Cartesian on big data = alarm |
| `Sort` | Sort within partitions (for SMJ, window, or `orderBy`) | Can spill |
| `Window` | Window function | Needs shuffle + sort by partition key |
| `InMemoryTableScan` / `InMemoryRelation` | Reading a cached DataFrame (3.7) | Good if reused |
| `AdaptiveSparkPlan` / `AQEShuffleRead` | AQE wrapper / AQE-adjusted shuffle read (3.3) | Shows runtime changes |
| `BatchEvalPython` / `ArrowEvalPython` | Python UDF executed in Python worker processes | Serialization overhead, blocks pushdown |

### 7. Real DE example — ShopCo

An analyst complains: "daily revenue for gold customers takes 20 minutes". You run `explain("formatted")` and see:

1. `ReadSchema` with all 45 columns of `orders` → someone did `select("*")`; prune columns.
2. `PartitionFilters: []` although the table is partitioned by `order_date` → the filter was `date_format(order_ts,'yyyy-MM-dd') = '2026-10-16'` on a non-partition column; filter on `order_date` instead.
3. `SortMergeJoin` with `customers` (80 MB) → hint `broadcast(customers)` or raise the threshold (3.4).
4. `Exchange SinglePartition` → a window function without `partitionBy`; add a partition key.

Each line of the plan maps to a fix. That is the skill this phase builds.

### 8. When to use / not use

- **Use** `explain()` before running anything big, when a query is slow, after a refactor (did the plan change?), and in interviews to reason aloud.
- **Don't** rely on `explain()` alone for runtime facts — it shows estimates and the *initial* AQE plan. For actual rows, time and spill, use the Spark UI SQL tab / query profile (3.9).

### 9. Misconceptions

1. **"`PushedFilters` means only matching rows are read."** Pushdown lets the reader *skip* row groups/files using statistics; a `Filter` node still runs above the scan.
2. **"The plan follows the order I wrote my code."** Catalyst reorders filters and projections (and, with statistics and CBO, joins).
3. **"PySpark is slower than Scala."** For DataFrame/SQL operations both compile to the same plan and JVM execution. Python UDFs and `collect`/`toPandas` are where Python costs extra.
4. **"`explain()` shows what ran."** It shows what *would* run, before AQE re-optimization.
5. **"`*(1)` is stage 1 in the Spark UI."** It is a codegen id; stage ids are assigned by the DAG scheduler.

---

## B. Practical — read plans for filter and select pushdown

**Runnable notebook:** [`exercises/phase-03/01_plans_pushdown.py`](../../exercises/phase-03/01_plans_pushdown.py)

The notebook writes 1,000,000 synthetic ShopCo orders as **Parquet files** (in a UC volume) and as a **Delta table** partitioned by `order_date`, then prints plans for several queries.

### Input (generated)

| order_id | customer_id | amount | order_date | status |
|---|---|---|---|---|
| 0 | 1 | 0.00 | 2026-10-01 | NEW |
| 1 | 2 | 0.73 | 2026-10-02 | PAID |
| 2 | 3 | 1.46 | 2026-10-03 | SHIPPED |
| … | … | … | … | … |

`customer_id = order_id % 50000 + 1`, `amount = ((order_id * 73) % 100000) / 100` (uniform 0.00–999.99), `order_date = 2026-10-01 + (order_id % 30) days`, `status` cycles NEW / PAID / SHIPPED / CANCELLED.

### Key code

```python
q = (spark.read.parquet(PARQUET_PATH)                 # 1
       .filter(F.col("amount") > 500)                 # 2
       .select("order_id", "customer_id"))            # 3
q.explain(True)                                       # 4
q.explain("formatted")                                # 5

d = (spark.table(T("orders_delta"))
       .filter((F.col("order_date") == "2026-10-16") & (F.col("status") == "PAID"))
       .select("order_id", "amount"))
d.explain("formatted")                                # 6
```

| # | Explanation |
|---|---|
| 1 | Lazy: defines a scan, reads only metadata/schema |
| 2 | Becomes `Filter` in the logical plan; the optimizer pushes it into the scan |
| 3 | Column pruning — only `order_id`, `customer_id` (+ `amount` for the filter) will be read |
| 4 | Extended mode: compare the analyzed plan (your order) with the optimized plan (filter next to the relation, projection narrowed) |
| 5 | Formatted mode: find `PushedFilters` and `ReadSchema` in the numbered scan node |
| 6 | Delta table partitioned by `order_date`: the date predicate appears in `PartitionFilters`, `status` in `PushedFilters`/`DataFilters` |

### Expected output (abbreviated — check these fields, not exact text)

| Query | Look for |
|---|---|
| Parquet filter + select | `PushedFilters: [IsNotNull(amount), GreaterThan(amount,500.00)]`, `ReadSchema: struct<order_id:bigint,customer_id:bigint,amount:decimal(10,2)>` |
| Delta partitioned | `PartitionFilters` containing `order_date = 2026-10-16`; `PushedFilters` containing `EqualTo(status,PAID)`; `ReadSchema` without `customer_id` |
| `select("*")` version | `ReadSchema` lists **all** columns |
| Python UDF filter | **No** `GreaterThan(amount…)` in `PushedFilters`; an `ArrowEvalPython` / `BatchEvalPython` node appears |
| `q.count()` | `499990` — `(order_id*73) % 100000` hits every value 0–99,999 exactly 10 times, and 49,999 of those values give amount > 500.00 (500.01…999.99) |
| `d.count()` | `16666` — date 2026-10-16 needs `order_id % 30 = 15`, PAID needs `order_id % 4 = 1`; both hold only when `order_id % 60 = 45` |

On Databricks with Photon, scan/filter nodes may be named `PhotonScan`, `PhotonFilter`, … and wrapped in `PhotonResultStage`; the fields mean the same thing.

### Common mistakes

- Reading the plan top-down and calling the first join you see "the first thing that happens".
- Assuming serverless/Spark Connect `explain()` text looks identical to classic — same information, formatting can differ.
- Looking only at `PushedFilters` and missing that `PartitionFilters` is empty on a partitioned table.
- Forgetting that a filter on a UDF result or a heavily transformed column may not push down.

---

## C. Interview questions

### 🟢 Basic

**B1.** What is the difference between a logical plan and a physical plan?

<details><summary>Model answer</summary>

A logical plan describes **what** to compute (relations, filters, projections, joins, aggregates) independent of execution. A physical plan describes **how**: concrete operators such as `FileScan`, `Exchange` (shuffle), `SortMergeJoin` or `BroadcastHashJoin`, `HashAggregate`, chosen by the planner and executed as stages and tasks.
</details>

**B2.** Name the four plans shown by `explain(True)` in order.

<details><summary>Model answer</summary>

Parsed (unresolved) logical plan → analyzed logical plan (names and types resolved against the catalog) → optimized logical plan (after Catalyst rules) → physical plan.
</details>

**B3.** In a physical plan, what does an `Exchange` node mean?

<details><summary>Model answer</summary>

A shuffle: data is redistributed across partitions (e.g. `hashpartitioning(customer_id, 200)` for a join or aggregate, `rangepartitioning` for a sort, `SinglePartition` for global operations). Each exchange is a stage boundary and usually the most expensive part of the query.
</details>

### 🟡 Medium

**M1.** You see `PushedFilters: [IsNotNull(amount), GreaterThan(amount,500.00)]` but also a `Filter` node above the scan. Is Spark filtering twice? Why?

<details><summary>Model answer</summary>

Yes, by design. The pushed filter is used by the Parquet reader to skip row groups (and, for Delta, data skipping skips whole files) using min/max statistics — that is coarse. Rows inside a row group that is read can still fail the predicate, so Spark's `Filter` operator evaluates it on every row to guarantee correctness.
</details>

**M2.** How do you tell from a plan whether partition pruning happened?

<details><summary>Model answer</summary>

Look at the scan node: `PartitionFilters` contains the predicate on the partition column (e.g. `order_date = 2026-10-16`). If it is empty while the table is partitioned, every partition is scanned. Common causes: filtering on a derived expression instead of the partition column itself. (Dynamic partition pruning shows up as a `dynamicpruningexpression(...)` in `PartitionFilters`.)
</details>

**M3.** Why does `ReadSchema` include a column you did not select?

<details><summary>Model answer</summary>

Because another operator needs it — typically the filter or a join key. Column pruning reads the union of columns needed by *any* operator in the query, not just the output columns.
</details>

### 🔴 Tricky

**T1.** `explain()` shows `SortMergeJoin`, but the Spark UI shows a broadcast hash join ran. Who is wrong?

<details><summary>Model answer</summary>

Neither. `explain()` before execution shows AQE's **initial** plan (`AdaptiveSparkPlan isFinalPlan=false`). After the shuffle map stages ran, AQE saw that one side was actually small (below the adaptive broadcast threshold — 30 MB by default on Databricks via `spark.databricks.adaptive.autoBroadcastJoinThreshold`; in Apache Spark `spark.sql.adaptive.autoBroadcastJoinThreshold`, which defaults to the static threshold) and switched to a broadcast hash join. The Spark UI shows the final plan.
</details>

**T2.** Two developers write the same aggregation — one in SQL, one in PySpark. One claims SQL is faster. How do you settle it?

<details><summary>Model answer</summary>

Compare `explain()` of both. DataFrame and SQL go through the same Catalyst pipeline, so equivalent logic gives an identical physical plan and identical performance. Differences appear only if the logic differs (e.g. a Python UDF, a different join, `select *`) — and the plan shows it.
</details>

**T3.** A plan shows `Exchange SinglePartition` under a `Window` node on a 2-billion-row table. What is the problem and the fix?

<details><summary>Model answer</summary>

The window has no `partitionBy` (e.g. `row_number() over (order by order_ts)`), so Spark must move all rows to a single partition — one task does all the work and will likely spill heavily or OOM. Fix: add a meaningful `partitionBy` (per customer, per day). If you only need unique ids, use `monotonically_increasing_id()`; if you need a true global rank, compute it per day and add per-day offsets.
</details>

---

## E. Revision summary

- Parsed → analyzed → **optimized logical** → **physical**; physical runs as stages/tasks; AQE can change it at runtime.
- `explain()` = physical only; `explain(True)` = all four; `explain("formatted")` = easiest to read; `explain("cost")` = statistics.
- Read **bottom-up**; every `Exchange` = shuffle = stage boundary; `*(n)` = whole-stage codegen.
- Scan node: column list/`ReadSchema` (pruning), `PushedFilters` (row-group/file skipping), `PartitionFilters` (directory/partition skipping).
- `explain()` doesn't run the query and shows the **initial** AQE plan; the Spark UI/query profile shows the final one.