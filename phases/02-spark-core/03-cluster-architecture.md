# 2.3 Cluster Architecture — Driver, Executors, Cluster Manager

**Time:** 35 min · **Prerequisites:** 2.1, 2.2

**After this topic you can:**
- explain the role of the driver, executors and cluster manager, and why each exists;
- trace the life of a Spark application from start to result;
- say which code runs on the driver and which on executors (and why it matters for OOM errors);
- map Spark terms to Databricks compute: driver node, worker nodes, single node, serverless.

---

## A. Concept deep dive

### 1. The simplest explanation

A Spark application is a **team**:

- the **driver** is the manager: it reads your program, makes the plan, hands out work and collects results;
- the **executors** are the workers: they do the actual data processing, in parallel;
- the **cluster manager** is the HR/facilities department: it finds machines and starts the executors for the driver.

```
                    ┌──────────────────────────────┐
  your notebook ───▶│            DRIVER            │
  / job code        │  SparkSession / SparkContext │
                    │  plan → DAG → stages → tasks │
                    │  task scheduler, Spark UI    │
                    └───────┬──────────────▲───────┘
           ① "I need       │ ③ send tasks │ ④ status, results,
              executors"   │              │    metrics
                    ┌──────▼───────┐      │
                    │CLUSTER MANAGER│      │
                    └──────┬───────┘      │
                 ② launch  │              │
          ┌────────────────┼──────────────┴──────────┐
   ┌──────▼──────┐  ┌──────▼──────┐          ┌──────▼──────┐
   │ EXECUTOR 1  │  │ EXECUTOR 2  │   …      │ EXECUTOR N  │
   │ [core][core]│  │ [core][core]│          │ [core][core]│  ← each core runs 1 task at a time
   │ cache, shuffle files on local disk      │             │
   └─────────────┘  └─────────────┘          └─────────────┘
          ▲  ▲  read/write data in storage (S3 / ADLS / GCS, Delta tables)
```

### 2. Official definitions (Apache Spark glossary, paraphrased)

| Term | Definition |
|---|---|
| **Application** | Your program: one driver + its executors |
| **Driver program** | The process running your `main()` (notebook) and creating the SparkContext |
| **Cluster manager** | External service that acquires resources (Standalone, Hadoop YARN, Kubernetes; plus `local` mode for one machine) |
| **Worker node** | Any machine that can run application code |
| **Executor** | A process launched for one application on a worker node; runs tasks and keeps data in memory or disk across them. Each application has its own executors |
| **Task** | A unit of work sent to one executor |

(Mesos support was deprecated in Spark 3.2 and is no longer listed in current Spark docs.)

### 3. The driver — what it does and why we need it

| Responsibility | Detail |
|---|---|
| Runs your program | The notebook/Python process and the JVM that hosts the SparkSession |
| **Plans** | Turns DataFrame/SQL code into a logical plan → optimised (Catalyst) → physical plan (Phase 3) |
| **Schedules** | DAG scheduler splits each job into stages; task scheduler sends tasks to executor cores (2.6, 2.7) |
| **Tracks & recovers** | Knows which tasks finished, where shuffle outputs live; retries failed tasks; resubmits stages |
| **Receives results** | `collect()`, `take()`, `toPandas()`, `count()` results come back to the driver |
| **Coordinates side data** | Broadcast variables/broadcast joins: small data is collected and shipped to executors |
| **Serves the Spark UI** | Jobs, stages, tasks, SQL plans, executors |

**Why a driver at all?** Someone must hold the global view: the full plan, which partitions exist, which are done, where intermediate data lives, and what to retry. Workers only see their own tasks. That central coordination is what turns hundreds of independent processes into one computation.

**Consequence:** the driver is a **single point of failure** for the application. If it dies (e.g. out-of-memory after `collect()` of 50 GB), the whole application fails; executors can't continue alone.

### 4. Executors — what they do

| Responsibility | Detail |
|---|---|
| Run tasks | Each executor has a number of **cores (slots)**; each core runs one task at a time (with default `spark.task.cpus = 1`) |
| Read & write data | Read input partitions from storage; write output files |
| Shuffle | Write map-side shuffle files to **local disk**; serve them to other executors; fetch their own reduce inputs (2.10) |
| Cache | Store cached partitions in memory/disk (block manager) |
| Report | Send task status, metrics and heartbeats to the driver |
| Python | For Python UDFs/pandas UDFs, executors start **Python worker processes** next to the JVM and stream rows/Arrow batches to them |

**Total parallelism** = total executor cores (÷ `spark.task.cpus`). 4 workers × 16 cores = 64 tasks at the same time.

If an executor dies, its running tasks are re-run elsewhere, and any shuffle output it held may be recomputed from lineage — the application survives.

### 5. Cluster manager — what it does

It only deals in **resources**: "give this application N executors with X cores and Y GB each". It doesn't understand DataFrames or stages. Options in Apache Spark: **Standalone**, **YARN**, **Kubernetes**, and `local[*]` (driver and executor threads in one process — your laptop).

**Deploy modes** (open-source `spark-submit`): **client** mode = driver runs where you submit (e.g. your laptop / edge node); **cluster** mode = driver runs inside the cluster.

### 6. Life of an application (step by step)

1. You start a notebook / job → the **driver** starts and creates the SparkSession.
2. The driver asks the **cluster manager** for executors; executors start on worker nodes and **register** with the driver.
3. Your code defines transformations → the driver only builds a plan (lazy, 2.8).
4. You call an **action** → the driver optimises the plan, creates a **job**, splits it into **stages**, each into **tasks** (one per partition).
5. The task scheduler sends tasks (serialised code + which partition to process) to free executor cores, preferring data-local placement.
6. Executors run tasks, write shuffle files or results, report back. Failed tasks are retried (default `spark.task.maxFailures = 4`).
7. Results of the final stage go to the driver (e.g. `collect`) or to storage (e.g. `write`).
8. Executors stay alive for the next action (unless removed by autoscaling/dynamic allocation) until the application ends.

### 7. Mapping to Databricks (Databricks-specific)

| Spark concept | Databricks classic compute | Notes |
|---|---|---|
| Driver | **Driver node** | Docs: the driver node maintains notebook state, maintains the SparkContext, interprets commands from notebooks/libraries, and runs the Spark master that coordinates with executors |
| Executor | **Worker node** | Docs: Databricks runs **one executor per worker node**, so "executor" and "worker" are used interchangeably on Databricks |
| Cluster manager | Managed by Databricks | You pick instance types, worker count / autoscaling range; Databricks provisions VMs in your cloud account and starts Spark — you never configure YARN/Kubernetes |
| Local mode | **Single node compute** | Driver acts as both master and worker, no worker nodes; executor threads ≈ one per logical core minus one for the driver (per docs) |
| — | **Serverless compute** | No nodes to see or size; Databricks manages capacity. Your notebook talks to Spark through **Spark Connect**; no Spark UI (use the **query profile**); RDD APIs not supported |

Practical consequences on Databricks all-purpose compute: **every notebook attached to the cluster shares the same driver**. One user's huge `toPandas()` can slow or crash the driver for everyone attached.

### 8. Where does my code run?

| Code | Runs on |
|---|---|
| `df = spark.read.table(...)`, `df.filter(...)`, `df.groupBy(...)` | **Driver** — only builds a plan |
| The actual filtering/aggregating when an action runs | **Executors** (tasks) |
| `for row in df.collect(): ...` | Executors produce rows → **all sent to the driver** → Python loop on the **driver** |
| `df.toPandas()` | All data to the **driver** |
| A Python UDF body | **Executors** (Python worker processes) |
| `print()` inside a UDF | Executor logs, **not** your notebook output |
| `import pandas as pd; pd.read_csv(...)` | **Driver only** — not distributed at all |
| `F.broadcast(small_df)` | Collected via the driver, then shipped to every executor (implementation details vary by version) |

### 9. Real data-engineering example — ShopCo

ShopCo's nightly job on a cluster with 1 driver (8 cores, 32 GB) and 6 workers (16 cores, 64 GB each):

| Step | Who does it |
|---|---|
| Parse the notebook, plan `orders ⋈ customers → revenue by city` | Driver |
| Read 2 TB of `orders` in ~16 000 partitions, 96 at a time | Executors (6 × 16 cores) |
| Partial aggregation, shuffle write, shuffle read, final aggregation | Executors |
| Write `gold_daily_revenue` files, then commit the Delta transaction | Executors write files; the driver commits the Delta log entry |
| An analyst then runs `display(spark.table("orders").toPandas())` on the same cluster | Pulls 2 TB toward the driver → driver OOM → every notebook on the cluster loses its state |

### 10. When this matters (sizing rules of thumb)

| Situation | Size up |
|---|---|
| Large `collect`/`toPandas`, big broadcast tables, many concurrent notebooks, huge plans, many files to list | **Driver** |
| Big data volume, heavy joins/aggregations, spills, skew-related OOMs | **Executors** (more workers or bigger workers) |

### 11. Misconceptions

1. **"The driver processes data too."** In normal DataFrame execution it doesn't process partitions; it plans and coordinates. It *does* handle anything you bring to it (`collect`, `toPandas`, Python loops, pandas).
2. **"Executor = machine."** In Apache Spark a machine can host several executors; on Databricks it's one executor per worker node.
3. **"Executor = core."** An executor is a process with many cores; each core runs one task at a time.
4. **"The cluster manager schedules tasks."** It allocates resources. Task scheduling is done by the driver.
5. **"If an executor dies the job fails."** Tasks are retried and lost data recomputed. If the **driver** dies, the application fails.

---

## B. Practical — who runs what

**Runnable notebook:** [`exercises/phase-02/03_driver_vs_executors.py`](../../exercises/phase-02/03_driver_vs_executors.py)

Key part:

```python
import socket
from pyspark.sql import functions as F

print("driver host:", socket.gethostname())                              # 1

@F.udf("string")
def executor_host(_):                                                    # 2
    import socket
    return socket.gethostname()

(spark.range(0, 16, 1, numPartitions=8)                                  # 3
      .select(F.spark_partition_id().alias("pid"), executor_host("id").alias("host"))
      .distinct()
      .orderBy("pid")
      .show())                                                           # 4
```

| # | Explanation |
|---|---|
| 1 | Plain Python — executes in the notebook's Python process on the **driver** |
| 2 | A Python UDF — its body is serialised and executed by **executors** (in Python worker processes) |
| 3 | 16 rows in 8 partitions → 8 tasks |
| 4 | The action. Each row shows which partition was processed on which host |

**Expected output (depends on compute):**

| Compute | What you'll see |
|---|---|
| Classic, 2+ workers | 8 rows (pid 0–7); hosts are worker hostnames, **different from the driver host**; partitions spread across workers |
| Single node | All hosts equal the driver host (driver and executor are the same machine) |
| Serverless / standard access mode | UDFs run in an isolated sandbox; hostnames are sandbox/container names — don't over-interpret, the point is that UDF code does not run in your notebook process |

The notebook also shows how to read the number of task slots (`spark.sparkContext.defaultParallelism` on classic compute) and why `collect()` brings data to the driver.

### Common mistakes

- Running `toPandas()`/`collect()` on large tables "to look at it" → driver OOM. Use `display(df)`/`df.limit(n)`, or write results to a table.
- Sizing a huge driver to "make the job faster" when work runs on executors.
- Expecting `print()` inside a UDF or `foreach` to appear in the notebook.
- Assuming a Python `for` loop over files or rows is parallel — it runs on the driver, one at a time.

---

## C. Interview questions

### 🟢 Basic

**B1.** What is a driver?

<details><summary>Model answer</summary>

The driver is the process that runs your Spark program and owns the SparkSession/SparkContext. It converts your code into an optimised plan, splits each action into a job of stages and tasks, schedules those tasks on executors, tracks their progress and retries failures, and receives results of actions like `collect` or `count`. On Databricks it runs on the driver node, which also holds notebook state.
</details>

**B2.** Why do we need a driver?

<details><summary>Model answer</summary>

Distributed work needs a single coordinator with the global view: the full plan, how data is partitioned, which tasks are running or done, where shuffle outputs are, and what to retry. Executors only see the tasks they're given. Without a driver there's no one to plan, schedule, recover from failures or assemble results. The trade-off is that the driver is a single point of failure for the application.
</details>

**B3.** What is an executor?

<details><summary>Model answer</summary>

An executor is a process (JVM) launched on a worker node for one application. It runs tasks on its cores — one task per core at a time — reads and writes data, writes and serves shuffle files, stores cached data, and reports status and metrics to the driver. On Databricks there is one executor per worker node.
</details>

**B4.** What does the cluster manager do?

<details><summary>Model answer</summary>

It allocates resources: it starts executor processes on worker machines when the driver requests them (and removes them for dynamic allocation/autoscaling). It doesn't plan or schedule tasks. In open-source Spark it can be Standalone, YARN or Kubernetes; on Databricks the platform manages this for you.
</details>

### 🟡 Medium

**M1.** A notebook does `rows = df.collect()` on 200 million rows and the cluster becomes unresponsive. Explain what happened physically.

<details><summary>Model answer</summary>

Executors computed the rows and serialised them back to the driver. The driver had to hold all results in memory (JVM, then converted to Python objects), exceeding driver memory or `spark.driver.maxResultSize` (default 1g in Apache Spark; Databricks may set its own value — verify). The driver either errors, garbage-collects endlessly or crashes; on Databricks shared compute, that also kills notebook state for everyone attached. Fix: aggregate/filter in Spark, write to a table, or use `limit`/`display`.
</details>

**M2.** What happens when an executor dies in the middle of a job? And when the driver dies?

<details><summary>Model answer</summary>

Executor: the driver notices (lost heartbeat), marks its running tasks as failed and re-schedules them on other executors; shuffle output stored on that executor is lost, so the parent stage tasks that produced it are recomputed from lineage when a fetch fails. The job usually succeeds, slower. Driver: the application ends — it holds the plan and all scheduling state. The job must be rerun (e.g. job retry), so tasks/writes should be idempotent.
</details>

**M3.** How many tasks can run at the same time on a cluster of 5 workers with 8 cores each? On Databricks, how many executors is that?

<details><summary>Model answer</summary>

5 × 8 = 40 tasks simultaneously (with `spark.task.cpus = 1`). On Databricks that's 5 executors (one per worker). The driver's cores don't run tasks on multi-node clusters.
</details>

### 🔴 Tricky

**T1.** Which of these run on the driver and which on executors: (a) `df.filter(...)` definition, (b) evaluating the filter, (c) body of a Python UDF, (d) `for r in df.take(10): print(r)`, (e) `pd.read_csv("/Volumes/...")`?

<details><summary>Model answer</summary>

(a) Driver — it only adds to the plan. (b) Executors, inside tasks. (c) Executors, in Python worker processes. (d) Executors compute the 10 rows; they're sent to the driver where the loop and `print` run. (e) Driver only — pandas is single-machine, not distributed.
</details>

**T2.** On a shared all-purpose cluster, one user's notebook fails with a driver OOM, and other users lose their variables. Why, and how would you prevent it?

<details><summary>Model answer</summary>

All notebooks attached to the compute share one driver process/node, which holds every notebook's state; an OOM or restart on the driver affects all of them. Prevent: run production work on job compute (isolated per job), avoid `collect`/`toPandas` on large data, size the driver for interactive use, and use separate compute for heavy users. Serverless isolates sessions so one user's notebook doesn't share a driver with others' in the same way.
</details>

**T3.** Your job runs on a Databricks cluster with a 64-core driver and 2 workers with 4 cores each. It's slow. Why might a bigger driver not help?

<details><summary>Model answer</summary>

On multi-node compute, tasks run on executors, so only 2 × 4 = 8 tasks run in parallel regardless of driver size. The 64 driver cores mostly sit idle (they plan and schedule). Unless the bottleneck is driver-side (collect, broadcast, planning, Python loops), add or enlarge workers instead. (On single-node compute, by contrast, the driver's cores do run tasks.)
</details>

---

## E. Revision summary

- Driver = plans, schedules, tracks, retries, receives results; single point of failure for the application.
- Executors = processes on workers that run tasks (1 per core at a time), shuffle, cache, report.
- Cluster manager = allocates resources only (Standalone/YARN/Kubernetes; Databricks-managed on Databricks).
- Databricks: driver node + worker nodes, **one executor per worker**; single node = driver does everything; serverless = managed, Spark Connect, query profile.
- `collect`/`toPandas`/pandas/Python loops run on the driver → driver OOM risk.