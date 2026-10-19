# 5.3 Compute vs Storage; What a Cluster Is; Driver and Worker Nodes

**Time:** 20 min · **Prerequisites:** 5.1, Phase 2 (driver/executors, tasks, partitions)

**After this topic you can:**
- explain "compute" and "storage" and why Databricks separates them;
- define a cluster and say why Spark code needs one;
- explain driver vs worker nodes and what each does at runtime;
- choose single-node vs multi-node, and explain what happens with zero workers;
- explain control plane vs classic compute plane vs serverless compute plane.

---

## A. Concept deep dive

### 1. The simplest explanation

- **Storage** = where data *rests*: files in ADLS Gen2 (Delta tables, volumes). Cheap, durable, always there.
- **Compute** = machines (CPU + memory + local disk) that *process* data for a while and then go away.
- A **cluster** (Databricks UI now says *compute resource*) = **one driver node + zero or more worker nodes** running the Databricks Runtime, working together as one Spark application environment.

> *"Storage remembers, compute thinks. You can switch the thinking off without forgetting anything."*

### 2. Why separate compute and storage?

| Coupled (old Hadoop / on-prem DB) | Separated (lakehouse on Databricks) |
|---|---|
| Data sits on the disks of the same machines that process it | Data in object storage; compute reads it over the network |
| To get more CPU you must buy more disks too (and vice versa) | Scale CPU and storage independently |
| Machines must run 24/7 or data is unavailable | Terminate compute when idle → pay nothing for it; data still there |
| One cluster = one copy of data | Many compute resources (jobs, SQL warehouse, notebooks) read the **same** tables |
| Cluster failure risks data | Compute failure loses only in-flight work; retry on new compute |

Cost model: you pay **storage** (Azure storage per GB + transactions) and **compute** (Azure VM cost for classic + Databricks **DBUs**, or a single serverless DBU price that includes the infrastructure).

### 3. Why do we need a cluster at all?

Spark code (`spark.read…`, `df.groupBy…`, `MERGE INTO …`) is a *plan*. Something must:
1. hold the SparkSession and build/optimise the plan (Catalyst);
2. split work into tasks over partitions;
3. run those tasks in parallel on many CPU cores;
4. move data between machines in shuffles and write results.

That "something" is compute: the **driver** does 1–2, **workers/executors** do 3–4 (exactly Phase 2's model). Without compute, a notebook is just text. Even "serverless" uses clusters — Databricks just creates and manages them for you.

### 4. Driver vs worker nodes

```
                       ┌──────────────────────── DRIVER NODE ────────────────────────┐
 notebook / job  ────▶ │ notebook REPL state (Python/SQL variables)                   │
 commands              │ SparkSession / SparkContext, Catalyst planner, DAG scheduler │
                       │ Spark master / coordination, collects results (collect())     │
                       └──────────────┬───────────────────────┬──────────────────────┘
                         tasks ▼       ▲ status/results          ▼ tasks
              ┌───────── WORKER 1 ─────────┐          ┌───────── WORKER 2 ─────────┐
              │ 1 executor (JVM) per worker │          │ 1 executor                  │
              │ N cores = N parallel tasks  │  shuffle │ N cores = N parallel tasks  │
              │ memory (cache, execution)   │◀───────▶│ local disk (shuffle, spill, │
              │ local disk                  │          │ disk cache)                 │
              └──────────────┬─────────────┘          └──────────────┬─────────────┘
                             └──────── read / write ADLS ─────────────┘
```

| | Driver | Worker (executor) |
|---|---|---|
| Count | Exactly 1 | 0 … many |
| Runs | Your notebook's plain Python, the Spark planner/scheduler, `collect()`/`toPandas()` results | Spark **tasks** on partitions: scans, filters, joins, shuffles, writes |
| Holds state of | All attached notebooks (variables, temp views per session) | Cached partitions, shuffle files |
| Typical failure | **Driver OOM** from `collect()` / huge `toPandas()` / too many attached notebooks; "driver unreachable" | Executor OOM, spill, lost node (spot eviction) → tasks retried |
| Sizing knob | Driver type (default = same as worker; choose bigger only if you pull data to the driver) | Worker type × number of workers |

Databricks-specific facts (docs): **Databricks runs one executor per worker node**, so "executor" and "worker" are used interchangeably. Each worker gets two private IPs (one for Databricks internal traffic, one for the Spark container).

### 5. Single node vs multi node

| | Single node | Multi node (standard) |
|---|---|---|
| Layout | Driver only; acts as master **and** worker | 1 driver + ≥1 workers |
| Parallelism | One executor thread per logical core, minus 1 core for the driver | Sum of worker cores |
| Logs | Everything in the driver log | Driver + worker logs |
| Good for | Small data, pandas/scikit-learn, learning, light ETL, a single analyst | Distributed ETL, large joins, streaming at volume |
| Limits | Can't be converted to multi node; no GPU scheduling | Can't scale to 0 workers — use single node for that |

**Zero workers on a multi-node cluster ≠ single node.** Per the docs: "If the compute resource has zero workers, you can run non-Spark commands on the driver node, but Spark commands will fail." Single-node mode configures Spark to run locally on the driver, which is why it works.

### 6. Where things physically run — three planes

| Plane | Lives in | Contains |
|---|---|---|
| **Control plane** | Databricks' account | Web app, notebook & job definitions, cluster manager, job scheduler, Unity Catalog service, REST APIs |
| **Classic compute plane** | **Your Azure subscription**, in the workspace's virtual network | VMs of all-purpose and jobs compute, classic & pro SQL warehouses, classic pipelines |
| **Serverless compute plane** | **Databricks' account**, same region as the workspace, inside a per-workspace network boundary | Serverless notebooks, jobs, pipelines, serverless SQL warehouses |

And data:

| Storage | What's in it |
|---|---|
| Your ADLS Gen2 accounts | Your tables and volumes (via Unity Catalog managed storage / external locations) |
| **Workspace storage account** (classic workspaces; in your subscription) | Workspace system data (job results, notebook revisions, logs), sometimes the auto-created workspace catalog, and legacy DBFS root. **Do not modify/delete it.** |
| Default storage (serverless workspaces) | Databricks-managed storage for workspace system data and UC assets |

### 7. Real example — ShopCo nightly job

- 200 GB of Parquet order history in `abfss://lake@shopcoadls.dfs.core.windows.net/...` (registered as `shopco_prod.silver.orders`).
- At 02:00 the job creates **job compute**: 1 driver + 4 workers `Standard_E8ds_v5` (8 vCPU, 64 GB each) → **32 task slots**.
- Reading 200 GB ≈ 1,600 input partitions of ~128 MB → 50 "waves" of 32 tasks. The driver schedules; workers read ADLS, shuffle and write Delta.
- At 02:40 the job ends, compute is terminated. The 200 GB never moved "into" Databricks; it stayed in ADLS.

### 8. Legacy vs current terminology

| Legacy | Current | Note |
|---|---|---|
| Cluster | **Compute** / compute resource | API: still `clusters/create`, `cluster_id` |
| "Cluster mode: Single Node" dropdown | **Single node** checkbox / radio | Legacy UI had Standard / High Concurrency / Single Node modes (5.4) |
| Data plane | **Compute plane** (classic / serverless) | Older diagrams say "data plane" |
| DBFS root as data store | UC managed storage, volumes | DBFS root is legacy |

### 9. Misconceptions

1. **"Data is loaded into the cluster and stored there."** Clusters cache some data temporarily; the source of truth is object storage.
2. **"Driver does the heavy lifting."** Workers do; the driver coordinates. Pulling big results to the driver is a classic failure.
3. **"More workers is always faster."** Shuffles get more expensive with more nodes, startup takes longer, and small data can't be split usefully (5.6).
4. **"Serverless means no cluster."** It means *you* don't manage one.
5. **"0 workers is a cheap single node."** Spark commands fail; use Single node mode.

---

## B. Practical — look inside your compute

**Runnable notebook:** [`exercises/phase-05/03_inspect_compute.py`](../../exercises/phase-05/03_inspect_compute.py)

Key part:

```python
import socket
from pyspark.sql import functions as F

driver_host = socket.gethostname()                                   # 1

@F.udf("string")
def where_am_i(_):                                                   # 2
    import socket
    return socket.gethostname()

df = spark.range(0, 1_000_000, numPartitions=16)                     # 3
hosts = (df.select(F.spark_partition_id().alias("pid"), where_am_i("id").alias("host"))
           .groupBy("host").agg(F.countDistinct("pid").alias("partitions")))  # 4
hosts.show(truncate=False)
print("driver:", driver_host)
```

| # | Explanation |
|---|---|
| 1 | Plain Python runs on the **driver** — this is the driver's hostname |
| 2 | A UDF runs inside **tasks on executors**, so it reports the worker's hostname |
| 3 | 16 partitions → 16 tasks |
| 4 | Count how many partitions each host processed |

**Expected results:**

| Compute | What you see |
|---|---|
| Single node | One host — the same as the driver (driver is also the worker) |
| Multi node, 2 workers | Two hosts, neither equal to the driver; partitions split between them |
| Serverless | Hostnames are Databricks-managed; you can't choose or see nodes (and the Spark UI isn't available) |

The notebook also prints `spark.version`, default parallelism (classic compute only) and the cluster tags the platform sets, wrapped in `try/except` because some of these aren't available on serverless or standard access mode.

### Common mistakes
- Calling `df.collect()` on millions of rows "to look at it" → driver OOM. Use `display(df)` / `df.limit(20)`.
- Creating a multi-node cluster with 0 workers "to save money" → Spark commands fail.
- Choosing a huge driver for a job that never collects data — wasted cost.
- Writing data to the driver's local disk (`/tmp`, `file:/`) and expecting it to survive termination — it doesn't. Use volumes/tables.

---

## C. Interview questions

### 🟢 Basic

**B1.** What is compute in Databricks?

<details><summary>Model answer</summary>

Compute is the processing resource that runs your code: VMs (CPU, memory, local disk) with the Databricks Runtime, either provisioned in your cloud subscription (classic: all-purpose, jobs, classic/pro SQL warehouses) or provided on demand by Databricks (serverless notebooks, jobs, pipelines, serverless SQL warehouses). It is separate from storage, which holds the data.
</details>

**B2.** What is a cluster?

<details><summary>Model answer</summary>

A set of VMs that work together as one Spark environment: one driver node and zero or more worker nodes running the Databricks Runtime. The driver coordinates (session, planning, scheduling); workers run executors that process partitions in parallel. Databricks runs one executor per worker.
</details>

**B3.** Why do we need a cluster?

<details><summary>Model answer</summary>

Notebook or job code is only instructions; something must hold the SparkSession, plan the query, split it into tasks and execute those tasks in parallel over the data, including shuffles and writes. That needs CPU, memory and network across machines — compute. Without attached compute (classic, serverless or a SQL warehouse) code cannot run.
</details>

**B4.** Driver vs worker?

<details><summary>Model answer</summary>

The driver runs the notebook REPL and Spark's planner/scheduler, tracks state for attached notebooks and receives results from actions like `collect()`. Workers host executors that run the tasks — reading, transforming, shuffling and writing data. Driver failure kills the application; worker failure only causes task retries.
</details>

### 🟡 Medium

**M1.** Why does Databricks separate compute and storage, and what does it enable?

<details><summary>Model answer</summary>

So that each scales and is paid for independently. Data stays durable in object storage while compute is created per job and terminated when idle; multiple compute resources (jobs, SQL warehouses, notebooks) read the same governed tables; a failed cluster loses no data; and you can choose the right compute per workload.
</details>

**M2.** When would you choose a single-node cluster?

<details><summary>Model answer</summary>

Small data, single-machine libraries (pandas, scikit-learn), development/learning, one analyst exploring data. It's cheap and simple. Not for large distributed ETL — one node's cores and memory become the limit — and it can't be converted to multi node later.
</details>

**M3.** Your notebook fails with "driver ... out of memory" while the workers are idle. Likely causes?

<details><summary>Model answer</summary>

Pulling large results to the driver (`collect()`, `toPandas()`, displaying huge results), broadcasting a table that is too large (built on the driver), many notebooks attached to the same interactive cluster holding big local objects, or plain Python processing big files on the driver. Fix: keep work distributed, limit results, write to tables, size the driver bigger only if collecting is truly needed.
</details>

### 🔴 Tricky

**T1.** A colleague set a multi-node cluster to 0 workers to save cost; now `spark.read.table(...)` fails but `print()` works. Explain.

<details><summary>Model answer</summary>

On multi-node compute with zero workers, there are no executors, so Spark jobs can't run — only non-Spark code on the driver works. Single-node mode is different: Spark runs in local mode on the driver, using its cores as task slots. Recreate it as single node (it can't be converted from multi node).
</details>

**T2.** Where does a serverless notebook's compute run, and what does that mean for network access to a firewalled storage account?

<details><summary>Model answer</summary>

In the serverless compute plane — in Databricks' Azure account, same region, isolated per workspace — not in your VNet. So storage firewalls that only allow your VNet will block it; you must configure serverless network access (e.g. the documented serverless firewall/private connectivity setup) for the storage account.
</details>

**T3.** "We store our intermediate results on the cluster's local disk because it's faster." Problems?

<details><summary>Model answer</summary>

Local disk is ephemeral: it's lost on termination, restart, autoscale-down or spot eviction; it isn't governed by Unity Catalog; other compute can't read it; and job retries on new compute won't find it. Use Delta tables or UC volumes (or temporary views within a session).
</details>

---

## E. Revision summary

- **Storage remembers (ADLS), compute thinks (VMs).** Separated → independent scaling, terminate when idle, many readers.
- **Cluster = 1 driver + 0..N workers**, one executor per worker on Databricks.
- Driver: REPL, planning, scheduling, results. Workers: tasks, shuffles, writes.
- **Single node** = driver runs Spark locally; multi node with **0 workers** can't run Spark.
- Control plane (Databricks) · classic compute plane (your subscription) · serverless compute plane (Databricks' account).