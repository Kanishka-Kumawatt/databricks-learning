# 3.2 Catalyst Optimizer — Predicate Pushdown, Column Pruning, Statistics

**Time:** 25 min · **Prerequisites:** 3.1

**After this topic you can:**
- explain what Catalyst is and the phases it runs (analysis → logical optimization → physical planning → code generation);
- explain the most important rules — predicate pushdown, column pruning, constant folding, filter inference, outer-join simplification — and spot each in a plan;
- explain rule-based vs cost-based optimization and why the cost-based optimizer (CBO) needs statistics;
- collect statistics with `ANALYZE TABLE … COMPUTE STATISTICS` and check them with `EXPLAIN COST`;
- name what blocks optimization (Python UDFs, opaque expressions, missing stats).

---

## A. Concept deep dive

### 1. The simplest explanation

**Catalyst** is Spark SQL's query optimizer. It takes the plan you described (3.1) and **rewrites it into an equivalent but cheaper plan**: same result, less data read, fewer rows shuffled, better algorithms.

Think of it as a very pedantic colleague who reads your query and says: "You filter after the join — I'll filter before. You read 40 columns but use 3 — I'll read 3. You wrote `100 * 5` — I'll write `500` once instead of per row."

### 2. Why it exists

| Without an optimizer | With Catalyst |
|---|---|
| Performance depends on how cleverly each developer writes code | Different ways of writing the same logic run the same plan |
| Every user must know Parquet internals to read less | Pruning and pushdown happen automatically |
| SQL and DataFrame APIs would need separate engines | One optimizer for SQL, Python, Scala, R |
| New optimizations need code changes everywhere | New rules benefit every existing query after an upgrade |

### 3. How it works internally

Catalyst represents a query as a **tree** (nodes = operators, expressions). Optimization = applying **rules** that pattern-match a sub-tree and replace it. Rules are grouped into **batches** that run repeatedly until the tree stops changing (a "fixed point").

```
           ┌─────────────┐   ┌──────────────────────┐   ┌───────────────────┐   ┌──────────────┐
 query ──▶ │  ANALYSIS   │──▶│ LOGICAL OPTIMIZATION │──▶│ PHYSICAL PLANNING │──▶│  CODE GEN    │──▶ RDDs
           │ resolve names│   │ rule-based rewrites  │   │ strategies + cost │   │ whole-stage  │
           │ via catalog  │   │ (+ CBO if stats)     │   │ (join selection)  │   │ codegen      │
           └─────────────┘   └──────────────────────┘   └───────────────────┘   └──────────────┘
```

| Phase | What happens | Example |
|---|---|---|
| **Analysis** | Resolve tables/columns/functions against the catalog (Unity Catalog), assign types, check permissions | `amount` → `amount#3: decimal(10,2)`; misspelt column → error here |
| **Logical optimization** | Rule-based rewrites (below); with statistics + CBO, cost-based join reordering | Push filter below join |
| **Physical planning** | Choose operators: join strategy (size estimates vs broadcast threshold, hints), aggregate implementation | `BroadcastHashJoin` vs `SortMergeJoin` |
| **Code generation** | Whole-stage codegen fuses a pipeline of operators into one Java function, avoiding per-row virtual calls (Project Tungsten) | `*(2)` in the plan |

On Databricks, **Photon** (a native C++ vectorized engine) can execute many physical operators instead of generated Java code; Catalyst still does the logical optimization.

### 4. The rules you must know

| Rule (concept) | What it does | Before → after | Where you see it |
|---|---|---|---|
| **Predicate pushdown** | Move filters as close to the data as possible: below projections and joins, and into the data source | `Filter(Join(o,c))` → `Join(Filter(o), c)`; then into `PushedFilters` | Filter sits right above scan; `PushedFilters`, `PartitionFilters` |
| **Column (projection) pruning** | Read and carry only the columns needed | scan 40 columns → scan 3 | Scan column list, `ReadSchema` |
| **Constant folding** | Evaluate constant expressions once at planning time | `amount > 100 * 5` → `amount > 500` | Optimized logical plan |
| **Combine / simplify filters** | Merge consecutive filters; simplify boolean logic | `filter(a).filter(b)` → `filter(a AND b)`; `x AND true` → `x` | One `Filter` node |
| **Infer filters from constraints** | Add implied predicates | inner join on `customer_id` → add `isnotnull(customer_id)` on both sides | `isnotnull(...)` you never wrote |
| **Outer-join elimination** | A filter that rejects NULLs on the outer side turns an outer join into inner | `LEFT JOIN c … WHERE c.city = 'Dubai'` → `INNER JOIN` | Join type changes |
| **Null propagation / literal simplification** | `NULL + 1` → `NULL`, `col = NULL` → `NULL` | | |
| **Dynamic partition pruning (DPP)** | At runtime, use the filtered dimension's keys to skip partitions of a partitioned fact table | `sales JOIN dates WHERE year=2026` → read only 2026 partitions | `dynamicpruningexpression` in `PartitionFilters` |
| **Join reordering (CBO)** | With statistics, choose the join order with the smallest intermediate results | `(big ⋈ big) ⋈ small` → `(big ⋈ small) ⋈ big` | Join order in optimized plan |

### 5. Pushdown, in detail

There are **two** kinds of "pushdown" — interviewers like this distinction:

1. **Logical pushdown** (inside the plan): Catalyst moves a `Filter` below `Project`, `Join`, `Aggregate` (when the filter is on grouping keys), `Union`.
2. **Source pushdown** (into the reader): the filter is handed to the data source.
   - **Parquet**: skips **row groups** whose min/max statistics cannot match.
   - **Delta**: additionally skips whole **files** using per-file min/max/null counts stored in the transaction log (data skipping; by default statistics are collected on the first 32 columns — `delta.dataSkippingNumIndexedCols`).
   - **Partitioned tables**: skips whole **partitions** (`PartitionFilters`).
   - **JDBC**: the `WHERE` is sent to the database.

What **blocks** pushdown:

| Blocker | Why |
|---|---|
| Python/Scala UDF in the predicate | Catalyst cannot see inside a UDF, so it cannot translate it into a source filter |
| Function on the column: `upper(status) = 'PAID'`, `cast(order_id AS string) = '42'` | The source cannot evaluate arbitrary expressions on stored values (Spark still filters, later) |
| Filter placed after a non-deterministic op (e.g. `rand()`, some window functions) | Moving it would change the result |
| Formats with no statistics (CSV, JSON) | Nothing to skip with — Spark must read every byte (Spark can still prune columns for some formats, not skip data) |

### 6. Rule-based vs cost-based optimization (CBO)

- **Rule-based**: rewrites that are *always* beneficial (pushdown, pruning, folding). No data knowledge needed.
- **Cost-based**: choices that **depend on data size** — join order, join strategy, build side. The CBO estimates the cost of alternatives using **statistics**: table size, row count, and per-column min/max/distinct count/null count/(optional) histograms.

Without statistics the planner falls back to file size estimates and simple heuristics; after filters and joins its estimates become guesses.

| | Apache Spark (OSS) default | Databricks |
|---|---|---|
| `spark.sql.cbo.enabled` | `false` | CBO **enabled by default** (Databricks docs) |
| `spark.sql.cbo.joinReorder.enabled` | `false` | verify for your runtime |
| Statistics collection | manual: `ANALYZE TABLE` | `ANALYZE TABLE`, or automatic via **predictive optimization** on UC managed tables |

Defaults change between versions — check with `spark.conf.get(...)`.

### 7. Collecting and reading statistics

```sql
-- table-level: size in bytes + row count
ANALYZE TABLE main.de_prep.customers COMPUTE STATISTICS;
-- size only, no scan (cheap)
ANALYZE TABLE main.de_prep.customers COMPUTE STATISTICS NOSCAN;
-- column-level: min, max, null count, distinct count, avg/max length
ANALYZE TABLE main.de_prep.customers COMPUTE STATISTICS FOR COLUMNS customer_id, city;
ANALYZE TABLE main.de_prep.customers COMPUTE STATISTICS FOR ALL COLUMNS;

DESCRIBE TABLE EXTENDED main.de_prep.customers;          -- "Statistics: ... bytes, ... rows"
DESCRIBE TABLE EXTENDED main.de_prep.customers city;     -- column stats
EXPLAIN COST SELECT ...;                                 -- Statistics(sizeInBytes=..., rowCount=...)
```

On Databricks Runtime 16.0+, `EXPLAIN` also prints which referenced tables have **missing / partial / full** statistics and suggests `ANALYZE TABLE … FOR ALL COLUMNS`.

Two different kinds of "stats" — don't mix them up:

| | Delta file statistics | Optimizer (CBO) statistics |
|---|---|---|
| Stored in | `_delta_log` per data file | Metastore / Unity Catalog per table/column |
| Collected | Automatically on write | `ANALYZE TABLE` or predictive optimization |
| Used for | **Data skipping** (which files to read) | **Planning** (join order, strategy, estimates) |

### 8. Real DE example — ShopCo

```sql
SELECT c.city, SUM(o.amount)
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
LEFT JOIN returns r ON r.order_id = o.order_id
WHERE o.order_date = '2026-10-16' AND c.segment = 'gold' AND r.reason IS NOT NULL
GROUP BY c.city
```

What Catalyst does:
1. `o.order_date = …` → pushed into the `orders` scan as a **partition filter**.
2. `c.segment = 'gold'` → pushed into the `customers` scan (before the join).
3. `r.reason IS NOT NULL` rejects NULLs from the outer side → `LEFT JOIN returns` becomes an **inner** join, and the filter is pushed into `returns`.
4. `isnotnull(customer_id)`, `isnotnull(order_id)` inferred on join keys.
5. Only `customer_id, amount, order_id` read from `orders`; `customer_id, city, segment` from `customers`.
6. With statistics, the CBO can join the small filtered tables first; the physical planner broadcasts the small sides.

### 9. When to use / not use

- **Rely on** Catalyst for the rewrites — write clear code, don't hand-optimize filter order.
- **Help** it: filter on raw columns (not expressions), avoid UDFs where a built-in function exists, keep statistics fresh for multi-join queries (after big loads), use partition/cluster columns in filters.
- **Don't** blindly collect `FOR ALL COLUMNS` on very wide, huge tables every hour — it scans the table. Collect on join/filter columns, after significant changes, or let predictive optimization handle it.
- If a plan is bad **because of** wrong statistics, Databricks documents dropping them (`ANALYZE TABLE … DROP STATISTICS`) or disabling the CBO as a targeted mitigation.

### 10. Misconceptions

1. **"Catalyst rewrites my Python UDF."** It cannot look inside it; the UDF is a black box.
2. **"Pushdown works the same for every format."** CSV/JSON have no statistics to skip with; Parquet skips row groups; Delta skips files too.
3. **"CBO is always on."** In Apache Spark it is off by default; on Databricks it is on but useless without statistics.
4. **"`ANALYZE TABLE` is the same as Delta stats."** Different stats, different purpose (planning vs skipping).
5. **"AQE replaces the CBO."** AQE re-optimizes at shuffle boundaries using runtime sizes but **does not reorder joins** (Databricks docs) — join order still comes from Catalyst/CBO.

---

## B. Practical — watch Catalyst rules fire, then add statistics

**Runnable notebook:** [`exercises/phase-03/02_catalyst_stats.py`](../../exercises/phase-03/02_catalyst_stats.py)

### Input

- `cat_orders`: 200,000 rows — `order_id`, `customer_id = order_id % 5000 + 1`, `amount = order_id % 1000`, `status`.
- `cat_customers`: 5,000 rows — `customer_id 1–5000`, `city` cycles Dubai / Abu Dhabi / Sharjah / Ajman, `segment` = `gold` for every 10th customer else `standard`.

### Key code

```python
q1 = cat_orders.filter(F.col("amount") > F.lit(100) * 5)                         # 1 constant folding
q2 = cat_orders.filter("amount > 100").filter("status = 'PAID'")                  # 2 combine filters
q3 = (cat_orders.join(cat_customers, "customer_id", "left")
                .filter(F.col("segment") == "gold"))                              # 3 outer → inner
for q in (q1, q2, q3):
    q.explain(True)

spark.sql(f"ANALYZE TABLE {T('cat_customers')} COMPUTE STATISTICS FOR ALL COLUMNS")  # 4
print(spark.sql(f"EXPLAIN COST SELECT * FROM {T('cat_customers')} WHERE segment = 'gold'")
      .collect()[0][0])                                                           # 5
```

| # | Explanation |
|---|---|
| 1 | Optimized logical plan shows `amount > 500` — the multiplication is gone |
| 2 | Two `filter` calls become one `Filter` with `AND` (plus inferred `isnotnull`) |
| 3 | `segment = 'gold'` can never be true for a NULL (unmatched) customer, so the `LEFT OUTER` join becomes `Inner` and the filter moves into the customers side |
| 4 | Collects table and column statistics into the catalog |
| 5 | `EXPLAIN COST` shows `Statistics(sizeInBytes=…, rowCount=…)`; after `ANALYZE` the scan has a real `rowCount=5.00E+3` |

### Expected output (abbreviated)

| Check | Expected |
|---|---|
| q1 optimized plan | `Filter (isnotnull(amount#..) AND (amount#.. > 500))` |
| q2 optimized plan | a single `Filter` containing both `amount > 100` and `status = PAID` |
| q3 optimized plan | `Join Inner` (not `LeftOuter`); `Filter (isnotnull(segment) AND (segment = gold))` under the customers side |
| q3 count | `20000` (500 gold customers × 40 orders each) |
| `DESCRIBE TABLE EXTENDED` after ANALYZE | `Statistics` row with bytes and `5000 rows` |
| `EXPLAIN COST` | `rowCount` present on the relation after ANALYZE (estimates for the filter depend on column stats) |

### Common mistakes

- Expecting `ANALYZE` to make one simple scan faster — it changes **planning**, mostly for joins.
- Collecting stats once and never again; stale stats can mislead the planner worse than none.
- Writing `upper(status) = 'PAID'` "to be safe" and losing pushdown — fix the data in Silver instead.

---

## C. Interview questions

### 🟢 Basic

**B1.** What is the Catalyst optimizer?

<details><summary>Model answer</summary>

Spark SQL's extensible, tree-based query optimizer. It analyzes a query (resolving names against the catalog), applies rule-based (and, with statistics, cost-based) rewrites to the logical plan, chooses physical operators, and generates code. It serves SQL and all DataFrame APIs, so equivalent queries get the same optimized plan.
</details>

**B2.** What are predicate pushdown and column pruning?

<details><summary>Model answer</summary>

Predicate pushdown moves filters as early as possible — below joins/projections and into the data source, so Parquet/Delta can skip row groups, files and partitions. Column pruning reads and carries only the columns the query needs, which is very effective with columnar formats.
</details>

**B3.** What is constant folding?

<details><summary>Model answer</summary>

Evaluating expressions made only of constants once during optimization instead of per row, e.g. `amount > 100 * 5` becomes `amount > 500`.
</details>

### 🟡 Medium

**M1.** Why does a Python UDF in a `filter` hurt performance beyond the UDF's own cost?

<details><summary>Model answer</summary>

Catalyst treats the UDF as a black box, so the predicate cannot be pushed into the scan: no row-group/file/partition skipping, so more data is read. Rows must also be serialized to Python worker processes and back. Prefer built-in functions; if a UDF is unavoidable, apply pushable filters first.
</details>

**M2.** What does the cost-based optimizer need, and how do you provide it?

<details><summary>Model answer</summary>

Statistics: table size and row count, and column statistics (min, max, distinct count, null count, optional histograms). Provide them with `ANALYZE TABLE t COMPUTE STATISTICS [FOR COLUMNS … | FOR ALL COLUMNS]`, or on Databricks let predictive optimization collect them for UC managed tables. Verify with `DESCRIBE TABLE EXTENDED` / `EXPLAIN COST`. In OSS Spark also enable `spark.sql.cbo.enabled` (off by default); Databricks enables CBO by default.
</details>

**M3.** A LEFT JOIN query returns the same rows as an INNER JOIN would and the plan shows `Join Inner`. Why?

<details><summary>Model answer</summary>

A predicate on the right (outer) side's columns in `WHERE` — e.g. `c.segment = 'gold'` — rejects the NULL-padded rows an outer join would produce. So the outer join is semantically an inner join, and Catalyst rewrites it (outer-join elimination). If you meant to keep unmatched orders, put the condition in the `ON` clause.
</details>

### 🔴 Tricky

**T1.** You filter `WHERE cast(order_id AS STRING) = '42'` on a 2 TB Delta table and it scans everything. `WHERE order_id = 42` is instant. Explain.

<details><summary>Model answer</summary>

Data skipping and Parquet row-group pruning compare the predicate against stored min/max values of `order_id`. A predicate on `cast(order_id AS string)` is a different expression whose ordering differs from the numeric ordering, so the stats cannot be used and the predicate is not pushed into the scan — every file is read, then filtered. Filter on the raw column with the right literal type.
</details>

**T2.** After a big backfill, a 5-table join that took 3 minutes now takes 40. No code changed. What do you check?

<details><summary>Model answer</summary>

Stale optimizer statistics: the CBO still believes a now-huge table is small (or vice versa), so it picks a bad join order or strategy (e.g. tries to broadcast a large table or shuffles a small one). Check `EXPLAIN` / `EXPLAIN COST` estimates vs actual rows in the Spark UI SQL tab ("rows output … est: …"), rerun `ANALYZE TABLE … COMPUTE STATISTICS FOR COLUMNS <join keys>` (or check predictive optimization), and compare plans. Also check skew introduced by the backfill.
</details>

**T3.** "AQE knows actual sizes at runtime, so we don't need statistics." True?

<details><summary>Model answer</summary>

Partly false. AQE re-plans only at shuffle/broadcast boundaries — it can coalesce partitions, switch SMJ→BHJ after shuffles already ran, and split skew — but it doesn't reorder joins and can't undo work already done (both sides may be shuffled before AQE decides to broadcast). Good statistics let the planner choose a good join order and broadcast up front. They are complementary.
</details>

---

## E. Revision summary

- Catalyst = analysis → logical optimization (rules, CBO) → physical planning → codegen; one optimizer for SQL and DataFrames.
- Key rules: **predicate pushdown, column pruning, constant folding, combine filters, inferred `isnotnull`, outer→inner, DPP, (CBO) join reorder**.
- Pushdown into Parquet = row-group skipping; Delta adds file skipping; partitions = `PartitionFilters`. UDFs and expressions on columns block it.
- CBO needs **statistics** (`ANALYZE TABLE … COMPUTE STATISTICS [FOR ALL COLUMNS]`); OSS default off, Databricks default on; predictive optimization can collect them.
- Delta file stats (skipping) ≠ optimizer stats (planning). AQE doesn't reorder joins.