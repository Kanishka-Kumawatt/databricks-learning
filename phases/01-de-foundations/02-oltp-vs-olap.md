# 1.2 OLTP vs OLAP

**Time:** 30 min · **Prerequisites:** 1.1

**After this topic you can:**
- explain the difference in purpose, workload, schema design and storage layout;
- explain *why* columnar storage makes analytics fast (and single-row lookups slow);
- explain why we copy data out of OLTP systems instead of querying them;
- place Postgres, SQL Server, Delta Lake, Databricks SQL and Synapse on the right side.

---

## A. Concept deep dive

### 1. The simplest explanation

- **OLTP (Online Transaction Processing)** systems **run** the business: many small, fast operations on individual records. *"Place order 101." "Reduce stock of product P9 by 1."*
- **OLAP (Online Analytical Processing)** systems **analyse** the business: few, large queries that read huge amounts of data. *"Total revenue per region per month for the last 3 years."*

### 2. Why two kinds of systems exist

The two workloads pull a database in opposite directions. A design that makes one fast makes the other slow — so we use different systems and **data engineering moves data from OLTP to OLAP**.

| Dimension | OLTP | OLAP |
|---|---|---|
| Purpose | Run operations | Analyse, report, predict |
| Typical users | Applications, thousands of concurrent end-users | Analysts, BI tools, data scientists |
| Typical query | Read/write **one or a few rows** by key | Scan/aggregate **millions–billions** of rows |
| Operations | INSERT, UPDATE, DELETE, point SELECT | SELECT with GROUP BY, JOIN, window functions |
| Latency target | Milliseconds | Seconds to minutes |
| Concurrency | Very high (many tiny transactions) | Lower (fewer, heavy queries) |
| Data | Current state | History (years) |
| Schema | **Normalised** (3NF) | **Denormalised** (star schema, wide tables) |
| Storage layout | **Row-oriented** | **Column-oriented** |
| Access path | Indexes (B-tree) | Full/partial scans + data skipping |
| Size | GBs – low TBs | TBs – PBs |
| Examples | PostgreSQL, MySQL, SQL Server, Oracle (as app DBs), Cosmos DB | Databricks SQL / Delta Lake, Synapse, Snowflake, BigQuery |

### 3. How it works internally — row vs column storage

Take this table:

| order_id | customer_id | region | amount | status |
|---|---|---|---|---|
| 101 | C1 | North | 250.00 | PLACED |
| 102 | C2 | South | 100.00 | SHIPPED |
| 103 | C3 | North | 40.00 | PLACED |

**Row-oriented (OLTP)** — each row's values are stored together on disk:

```
[101,C1,North,250.00,PLACED][102,C2,South,100.00,SHIPPED][103,C3,North,40.00,PLACED]
```

- "Get order 102" → an index points to one location → read one contiguous block. **Fast.**
- "SUM(amount)" → must read every row *including all other columns* just to get `amount`. **Slow** at scale.

**Column-oriented (OLAP)** — each column's values are stored together:

```
order_id:    [101,102,103]
customer_id: [C1,C2,C3]
region:      [North,South,North]
amount:      [250.00,100.00,40.00]
status:      [PLACED,SHIPPED,PLACED]
```

- "SUM(amount)" → read **only** the `amount` column. With 50 columns, that's ~1/50 of the data. **Fast.**
- "Get order 102" → must fetch a value from each of five separate column chunks and stitch them together. **Slower.**

Why columnar wins big for analytics — three effects stack:

1. **Column pruning** — read only the columns the query uses.
2. **Compression** — values in one column are similar (`North, South, North…`), so encodings like dictionary and run-length compress them 5–10×. Less data to read from storage.
3. **Vectorised execution** — the CPU processes a batch of values of the same type in tight loops (SIMD), much faster than row-by-row.

**Parquet** is a columnar file format; **Delta Lake stores its data as Parquet files**. That's why the lakehouse is an OLAP system.

### 4. Why OLTP is normalised and OLAP is denormalised

**Normalisation** (OLTP) = store each fact once, split into many related tables (`customers`, `addresses`, `orders`, `order_lines`, `products`).
- **Why:** an address change is one UPDATE in one place → no *update anomalies*; small rows; fast writes; strong integrity via foreign keys.
- **Cost:** a business question needs many joins.

**Denormalisation** (OLAP) = fewer, wider tables organised around business events (fact tables) and descriptions (dimension tables) — topic 1.10.
- **Why:** questions need fewer joins; columnar compression makes repeated values cheap; data is written in bulk, not row by row, so update anomalies matter less.
- **Cost:** redundancy; changes must be handled by the pipeline (e.g. SCD).

### 5. Transactions and concurrency

- OLTP databases are built for **thousands of tiny concurrent transactions**: row-level locks or MVCC (multi-version concurrency control), write-ahead logs, ACID per transaction.
- OLAP systems are built for **bulk loads and large reads**. Delta Lake provides ACID too, but at **file level**: an UPDATE of one row **rewrites the whole Parquet file containing it** (unless deletion vectors are enabled, which mark the row as deleted and write the new version separately — still far heavier than an OLTP row update). Great for batch changes, bad for thousands of single-row updates per second.

### 6. Real-world flow

```
 Checkout app ──▶ PostgreSQL (OLTP)  ──CDC / incremental extract──▶  Lakehouse (OLAP: Delta)  ──▶ Dashboards, ML
   writes 1 order          current state                              history, combined sources
```

The analytics side never slows down checkout, keeps history, and combines orders with payments, clicks and CRM data.

### 7. Where Databricks fits

- **Delta Lake + Databricks SQL warehouses + Spark** = OLAP.
- Databricks has also introduced **Lakebase**, a managed PostgreSQL (OLTP) service integrated with the lakehouse — a newer offering; the OLTP/OLAP distinction still holds, they're simply offered on one platform.
- **HTAP** (Hybrid Transactional/Analytical Processing) systems try to serve both; in practice most architectures still separate them.

### 8. Misconceptions

1. **"Columnar is always faster."** Not for single-row lookups or high-frequency updates.
2. **"Delta supports UPDATE and ACID, so it's an OLTP database."** It supports them at file granularity for analytical workloads, not millisecond single-row transactions at high concurrency.
3. **"A read replica solves analytics."** It removes load from the primary, but it's still row-oriented, single-node, current-state only, and contains one system's data.
4. **"OLAP means cubes."** Cubes were one OLAP implementation; today OLAP mostly means columnar warehouses/lakehouses.

---

## B. Practical

### 1. The same business, two workloads (SQL)

**OLTP — placing an order (one transaction, a few rows):**

```sql
BEGIN;
INSERT INTO orders (order_id, customer_id, order_ts, status)
VALUES (105, 'C2', now(), 'PLACED');

INSERT INTO order_lines (order_id, line_no, product_id, quantity, unit_price)
VALUES (105, 1, 'P9', 1, 59.90);

UPDATE inventory
SET    qty_on_hand = qty_on_hand - 1
WHERE  product_id = 'P9' AND qty_on_hand >= 1;   -- must not oversell

COMMIT;
```

- All three statements succeed or none do (**atomicity**).
- Touches ~3 rows, located through primary-key indexes — milliseconds.

**OLAP — the analytical question (scans everything):**

```sql
SELECT  c.region,
        date_trunc('MONTH', o.order_ts) AS order_month,
        SUM(l.quantity * l.unit_price)  AS revenue,
        COUNT(DISTINCT o.order_id)      AS orders
FROM    sales.fact_order_line l
JOIN    sales.dim_customer   c ON l.customer_sk = c.customer_sk
JOIN    sales.orders         o ON l.order_id    = o.order_id
WHERE   o.order_ts >= '2024-01-01'
GROUP BY c.region, date_trunc('MONTH', o.order_ts)
ORDER BY order_month, c.region;
```

- Reads millions of rows but only ~6 columns — exactly what columnar storage is good at.

### 2. See column pruning in Spark (run on Databricks)

```python
from pyspark.sql import functions as F

df = spark.range(1_000_000).select(                          # 1
    F.col("id").alias("order_id"),
    (F.col("id") % 1000).cast("string").alias("customer_id"),
    F.when(F.col("id") % 2 == 0, "North").otherwise("South").alias("region"),
    (F.rand(seed=42) * 500).cast("decimal(10,2)").alias("amount"),
    F.lit("PLACED").alias("status"))

path = "/Volumes/main/de_prep/files/orders_parquet"          # 2  (any volume you can write to)
df.write.mode("overwrite").parquet(path)

spark.read.parquet(path).select(F.sum("amount")).explain()   # 3
```

1. Builds 1 million synthetic orders with five columns.
2. Writes them as Parquet (columnar).
3. Prints the physical plan. Look for the **FileScan parquet** line:

```
... FileScan parquet [amount#..] ... ReadSchema: struct<amount:decimal(10,2)>
```

`ReadSchema` contains **only `amount`** — Spark reads one column out of five. That is column pruning, and it only works because Parquet is columnar. Try `select("*")` and compare.

### Common mistakes

- Pointing a BI tool straight at the production database "just for one report".
- Assuming the warehouse is "real-time" — it's as fresh as the last pipeline run.
- Designing lakehouse tables in strict 3NF and then joining 12 tables in every dashboard query.

---

## C. Interview questions

### 🟢 Basic

**B1.** What is the difference between OLTP and OLAP? Give one example system of each.

<details><summary>Model answer</summary>

OLTP systems run day-to-day operations with many small, fast, concurrent transactions on individual rows (e.g. PostgreSQL behind a checkout app). OLAP systems answer analytical questions with large scans and aggregations over historical data (e.g. Delta tables queried through a Databricks SQL warehouse). OLTP is normalised and row-oriented; OLAP is denormalised and column-oriented.
</details>

**B2.** Why is columnar storage faster for analytical queries?

<details><summary>Model answer</summary>

Analytical queries touch few columns but many rows. Columnar storage lets the engine read only the needed columns (column pruning), compresses each column very well because values are similar, and processes values in vectorised batches. Less data read + better CPU efficiency.
</details>

**B3.** What does normalisation mean, and why do OLTP systems use it?

<details><summary>Model answer</summary>

Storing each fact once by splitting data into related tables linked by keys. It prevents update anomalies (change an address in one place), keeps rows small and writes fast, and enforces integrity — ideal for frequent small writes.
</details>

### 🟡 Medium

**M1.** Why do analytical models (star schemas) deliberately denormalise, when normalisation prevents anomalies?

<details><summary>Model answer</summary>

Analytical data is written in bulk by pipelines, not by thousands of users, so the pipeline controls consistency and anomalies are handled there (e.g. SCD logic). Queries, however, run constantly — fewer joins make them simpler and faster, and columnar compression makes repeated descriptive values cheap. We trade write-time complexity for read-time simplicity.
</details>

**M2.** The team proposes a read replica of the production database for analytics. What does it solve and what does it not solve?

<details><summary>Model answer</summary>

Solves: analytical load no longer hits the primary, so the app isn't slowed down. Doesn't solve: still row-oriented (slow scans), usually single-node (limited scale), only current state (no history), only one source (no integration with payments/CRM), and the same raw schema (no business modelling or quality rules).
</details>

**M3.** Why is updating a single row in a Delta table much more expensive than in PostgreSQL?

<details><summary>Model answer</summary>

Parquet files are immutable. Postgres updates a row in place via an index lookup and a log record. Delta must find the file containing the row, write a new Parquet file with the change (the whole file is rewritten; with deletion vectors, it marks the old row deleted and writes the new row separately), and commit a new entry in the transaction log. That's fine for batch changes, but costly for thousands of single-row updates per second.
</details>

### 🔴 Tricky

**T1.** Could you use a Delta table as the backend database for the checkout application? Argue it.

<details><summary>Model answer</summary>

Technically it accepts INSERT/UPDATE with ACID, but it's the wrong tool: each commit has latency of hundreds of milliseconds to seconds, single-row updates rewrite files, optimistic concurrency causes conflicts under many concurrent writers to the same files, and there are no OLTP features like row locks, secondary indexes or millisecond point lookups. Use an OLTP database (e.g. Postgres, or Lakebase on Databricks) and replicate into Delta for analytics.
</details>

**T2.** On a 2 TB columnar table, `SELECT * FROM orders WHERE order_id = 101` is slow. Why — and is there anything that helps?

<details><summary>Model answer</summary>

`SELECT *` needs every column (no pruning), and there's no B-tree index to jump to the row — the engine may scan many files. What helps: file-level statistics (min/max per column) let Delta skip files whose `order_id` range can't contain 101 — effective when data is clustered by `order_id` (Z-order or liquid clustering). Selecting only the needed columns also helps. But if the workload is mostly point lookups, it belongs in an OLTP store.
</details>

**T3.** An analyst complains the dashboard is "wrong" because it shows yesterday's stock level while the app shows today's. Who is right?

<details><summary>Model answer</summary>

Both, by design. The OLAP copy is as fresh as the last pipeline run. The real question is the **freshness SLA**: if the business needs near-real-time stock, the pipeline must be incremental/streaming more often; otherwise the dashboard should clearly display "data as of <timestamp>". Freshness is a requirement to agree on, not a bug to argue about.
</details>

---

## E. Revision summary

- OLTP **runs** the business (many tiny transactions); OLAP **analyses** it (few huge scans).
- OLTP: normalised, row-oriented, indexes, milliseconds. OLAP: denormalised, columnar, scans + data skipping.
- Columnar wins analytics via **pruning, compression, vectorisation**. Parquet/Delta are columnar.
- Delta has ACID but at **file granularity** — great for batch, wrong for checkout.
- DE exists largely to move data **from OLTP to OLAP** without hurting production.