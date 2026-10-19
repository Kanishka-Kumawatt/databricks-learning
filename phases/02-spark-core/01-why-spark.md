# 2.1 Why Spark Was Created — MapReduce Limits and In-Memory Processing

**Time:** 20 min · **Prerequisites:** Phase 1 (especially 1.3, 1.6)

**After this topic you can:**
- explain the MapReduce model and the problems that made Spark necessary;
- explain what "in-memory" really means in Spark (and what it does **not** mean);
- explain why one engine for SQL, streaming and ML matters to a data engineer;
- place Spark, Databricks Runtime, Photon and Spark Connect correctly relative to each other.

---

## A. Concept deep dive

### 1. The simplest explanation

Spark is a **distributed data-processing engine**: you describe *what* you want done to a big dataset, and Spark splits the work across many machines, runs it in parallel, and recovers if some machines fail.

It was created because the previous standard — **Hadoop MapReduce** — was slow and awkward for anything more than one simple pass over the data.

### 2. A short history (context, not for memorising dates)

| When | What |
|---|---|
| 2004 | Google publishes the MapReduce paper |
| ~2006 | Hadoop (HDFS + MapReduce) — open-source version, becomes the big-data standard |
| 2009 | Spark starts at UC Berkeley AMPLab (Matei Zaharia et al.) |
| 2012 | RDD paper ("Resilient Distributed Datasets") — the core idea |
| 2013–2014 | Spark donated to Apache, becomes a top-level project, 1.0 released. Databricks founded (2013) by Spark's creators |
| 2016 | Spark 2.0 — DataFrames/Datasets as the main API, **SparkSession** |
| 2020 | Spark 3.0 — Adaptive Query Execution (AQE) introduced (on by default since 3.2) |
| 2023–2025 | Spark 3.4 adds **Spark Connect** (client–server); Spark 4.0 turns **ANSI SQL mode on by default** |

### 3. How MapReduce works (so you can see its limits)

```
             MAP phase                 SHUFFLE                 REDUCE phase
HDFS file ─▶ map(record) → (k, v) ─▶ sort + group by key ─▶ reduce(k, [v…]) ─▶ HDFS file
   (disk)                         (disk + network)                          (disk, replicated ~3×)
```

One MapReduce **job** = exactly one map step + one reduce step. Anything more complex is a **chain of jobs**, and every job reads its input from HDFS and writes its full output back to HDFS (replicated).

### 4. The problems Spark solved

| Problem with MapReduce | What it meant in practice | Spark's answer |
|---|---|---|
| **Disk between every step** | A 5-step pipeline = 5 jobs = 5 full writes + reads of HDFS. Iterative algorithms (ML training, graph algorithms) that loop 20 times re-read the data 20 times | Keep intermediate data **in memory where possible**; pipeline many steps inside one job; write to disk only at shuffles (and spill when memory is short) |
| **Rigid model** (only map + reduce) | Joins, multi-step aggregations, sorts had to be hand-built as chains of jobs | A **DAG** (directed acyclic graph) of many operations, optimised as a whole (2.7) |
| **Verbose low-level code** | A word count was dozens of lines of Java | High-level APIs: DataFrames, SQL, Python/Scala/Java/R |
| **One tool per workload** | MapReduce for batch, separate systems for SQL (Hive), streaming (Storm), ML (Mahout) | **Unified engine**: Spark SQL, Structured Streaming, MLlib on the same core |
| **High latency** | Job start-up overhead + disk I/O → interactive queries took minutes | Long-lived executors, in-memory reuse, Catalyst optimiser + code generation → interactive analysis |

### 5. What "in-memory" really means

```
MapReduce chain:   HDFS → map → HDFS → map → HDFS → reduce → HDFS
Spark job:         storage → filter → select → join → aggregate → storage
                           └── pipelined in memory inside tasks ──┘
                               (disk only at shuffle boundaries / spills)
```

1. **Pipelining:** consecutive narrow operations (filter → select → withColumn) run inside one task without writing intermediate results anywhere (2.9).
2. **Caching (optional):** you *can* ask Spark to keep a dataset in executor memory for reuse (`cache()` / `persist()`, Phase 3). It's not automatic, and not available on every compute type (Databricks **serverless** does not support DataFrame/SQL cache APIs).
3. **Not "everything in RAM":** shuffles write files to local disk (2.10), and when memory is short Spark **spills to disk** rather than failing. Data much larger than total cluster memory is normal.

### 6. Real data-engineering example — ShopCo

ShopCo's nightly job: read 2 TB of `orders`, drop cancelled orders, join to `customers`, aggregate revenue per city per day, and also compute customer lifetime value.

| | MapReduce era | Spark |
|---|---|---|
| Structure | 3–4 chained jobs, each writing to HDFS | One application; filter + join + aggregate planned together |
| Intermediate data | Written to HDFS, replicated | Pipelined in memory; shuffle files on local disk only |
| Code | Java mapper/reducer classes | ~15 lines of PySpark or one SQL statement |
| Same logic as a stream later | Rewrite in another system | Same DataFrame code with `readStream` (Phase 7) |

### 7. Where Spark sits today (Databricks context)

| Term | What it is | Universal or Databricks? |
|---|---|---|
| **Apache Spark** | The open-source engine | Universal |
| **Databricks Runtime (DBR)** | Databricks' distribution of Spark + optimisations, Delta Lake, libraries | Databricks |
| **Photon** | Databricks' native (C++) vectorised engine that runs supported Spark operations faster. Same APIs; different operator names in query plans | Databricks |
| **Spark Connect** | Client–server protocol (Spark 3.4+): the notebook is a thin client sending unresolved plans to a remote Spark server | Universal protocol; used by Databricks serverless and standard access mode |

### 8. When to use Spark / when not

| ✅ Use Spark | ❌ Don't bother |
|---|---|
| Data too big or too slow for one machine (GBs–PBs) | A 50 MB CSV — pandas/DuckDB on one machine is simpler and faster |
| Many steps (joins, aggregations) over large data | Millisecond lookups of single rows — that's an OLTP database |
| Batch + streaming + ML on the same data | Tiny row-by-row transactional updates |

### 9. Misconceptions

1. **"Spark is an in-memory database."** It's a processing engine; it stores nothing permanently. Data lives in storage (Delta/Parquet files in cloud storage).
2. **"Spark keeps all data in RAM."** It pipelines in memory, caches only when asked, and spills/shuffles to disk.
3. **"Spark replaced Hadoop."** It replaced **MapReduce** as the processing engine. Spark could run on Hadoop YARN and read HDFS; in the cloud, object storage replaced HDFS.
4. **"Spark is always faster."** For small data, distributed overhead (planning, scheduling, task launch) makes it slower than a single-machine tool.
5. **"Databricks = Spark."** Databricks is a platform built around Spark (plus Delta, Unity Catalog, Photon, Jobs…). Spark runs anywhere.

---

## B. Practical — the same logic, two models

No notebook needed; read and compare. Input: ShopCo orders.

| order_id | city | amount | status |
|---|---|---|---|
| 1 | Dubai | 100 | PAID |
| 2 | Dubai | 50 | CANCELLED |
| 3 | Abu Dhabi | 70 | PAID |
| 4 | Dubai | 30 | PAID |

**MapReduce thinking** (pseudo-code): one job = one map + one reduce, intermediate data on disk.

```text
map(order):            if order.status != "CANCELLED": emit(order.city, order.amount)
                       → (Dubai,100) (Abu Dhabi,70) (Dubai,30)    written to local disk, shuffled, sorted
reduce(city, amounts): emit(city, sum(amounts))
                       → (Abu Dhabi,70) (Dubai,130)                written to HDFS (replicated)
```

**Spark (PySpark):**

```python
from pyspark.sql import functions as F

orders = spark.createDataFrame(
    [(1, "Dubai", 100, "PAID"), (2, "Dubai", 50, "CANCELLED"),
     (3, "Abu Dhabi", 70, "PAID"), (4, "Dubai", 30, "PAID")],
    "order_id INT, city STRING, amount INT, status STRING")             # 1

revenue = (orders
           .filter(F.col("status") != "CANCELLED")                     # 2
           .groupBy("city")                                            # 3
           .agg(F.sum("amount").alias("revenue")))                     # 4

revenue.orderBy("city").show()                                         # 5
```

**Expected output:**

```
+---------+-------+
|     city|revenue|
+---------+-------+
|Abu Dhabi|     70|
|    Dubai|    130|
+---------+-------+
```

| # | Explanation |
|---|---|
| 1 | Builds a small DataFrame from local Python data (sent from the driver) — handy for demos |
| 2 | Same role as the map-side `if`; Spark runs it **inside the same task** that reads the data (pipelined) |
| 3–4 | Same role as shuffle + reduce. Spark also adds a **partial aggregation** before the shuffle automatically (2.10) |
| 5 | Nothing ran until here — `show()` is an **action** (2.8). `orderBy` makes the output order deterministic |

The SQL version is the same plan to the engine:

```sql
SELECT city, SUM(amount) AS revenue
FROM orders WHERE status <> 'CANCELLED'
GROUP BY city ORDER BY city;
```

### Common mistakes

- Saying "Spark is faster because it's in memory" and stopping there. Complete answer: fewer disk round-trips (pipelining), a DAG optimised as a whole, long-lived executors, optional caching, and Catalyst + code generation.
- Believing data must fit in memory.
- Using Spark for tiny datasets and blaming Spark for being slow.

---

## C. Interview questions

### 🟢 Basic

**B1.** Why was Spark created? What was wrong with MapReduce?

<details><summary>Model answer</summary>

MapReduce forces every computation into one map + one reduce step and writes every job's output to replicated HDFS. Multi-step pipelines become chains of jobs with full disk round-trips, iterative algorithms re-read data every iteration, and the code is verbose. Spark generalises the model to a DAG of many operations, pipelines steps in memory and only writes to disk at shuffles or when spilling, and offers high-level APIs (DataFrames, SQL) and one engine for batch, streaming and ML.
</details>

**B2.** What does "in-memory processing" mean in Spark?

<details><summary>Model answer</summary>

Intermediate results between operations in a stage are passed in memory inside a task instead of being written to storage, and datasets can optionally be cached in executor memory for reuse. It does not mean data must fit in RAM: Spark spills to disk and writes shuffle files to local disk.
</details>

**B3.** Is Spark a database?

<details><summary>Model answer</summary>

No. Spark is a compute engine. It reads data from storage (files, tables, databases, streams), processes it and writes results back. Persistence, ACID and table metadata come from storage layers like Delta Lake and catalogs like Unity Catalog.
</details>

### 🟡 Medium

**M1.** Why are iterative workloads (like ML training) the classic example of Spark beating MapReduce?

<details><summary>Model answer</summary>

Each iteration reads the same dataset. In MapReduce every iteration is a separate job that reads input from HDFS and writes output to HDFS. In Spark the dataset can be cached in executor memory once and reused by every iteration, and intermediate data stays in memory, so the per-iteration disk I/O disappears.
</details>

**M2.** What does it mean that Spark is a "unified engine", and why does a data engineer care?

<details><summary>Model answer</summary>

The same engine and API run batch SQL, DataFrame transformations, streaming (Structured Streaming) and ML. A data engineer can reuse the same transformation logic for a batch backfill and a streaming pipeline, operate one platform instead of several, and avoid copying data between systems.
</details>

**M3.** How are Apache Spark, Databricks Runtime and Photon related?

<details><summary>Model answer</summary>

Apache Spark is the open-source engine. Databricks Runtime is Databricks' packaged and optimised Spark distribution plus Delta Lake and libraries. Photon is a Databricks native vectorised engine that executes supported Spark operations under the same APIs; it changes performance and operator names in plans, not the programming model.
</details>

### 🔴 Tricky

**T1.** "Spark is 100× faster than Hadoop, so let's move our 30 MB daily CSV processing to Spark." Respond.

<details><summary>Model answer</summary>

Speed-up claims come from large, iterative, in-memory workloads. At 30 MB, Spark's fixed overheads (planning, scheduling, task launch, cluster start-up) dominate; a single-machine tool would likely be faster and cheaper. Valid reasons to use Spark anyway: data will grow, it must integrate with the lakehouse (Delta, governance, Jobs), or platform consistency — not raw speed at that size.
</details>

**T2.** If Spark is in-memory, why can a Spark job fail or slow down because of disk?

<details><summary>Model answer</summary>

Shuffles write map output files to executors' local disks and reducers read them over the network; when execution memory is short, sorts and aggregations spill to disk; local disk can fill up or be slow. So disk throughput and space matter even for "in-memory" Spark.
</details>

**T3.** A colleague says "Spark replaced Hadoop". What's imprecise about that?

<details><summary>Model answer</summary>

Hadoop has storage (HDFS), resource management (YARN) and a processing engine (MapReduce). Spark replaced MapReduce as the processing engine; it historically ran on YARN and read HDFS. In cloud lakehouses, object storage replaced HDFS and managed platforms replaced YARN — those are separate shifts.
</details>

---

## E. Revision summary

- MapReduce = one map + one reduce per job, HDFS writes between jobs → slow multi-step and iterative work, verbose code.
- Spark = DAG of operations, pipelined in memory, disk only at shuffles/spills, high-level APIs, one engine for batch/streaming/ML.
- "In-memory" ≠ "fits in RAM": Spark spills and shuffles to disk.
- Spark is a compute engine, not a database; Delta + a catalog provide storage guarantees.
- Databricks Runtime and Photon build on Spark; Spark Connect makes the notebook a thin client.