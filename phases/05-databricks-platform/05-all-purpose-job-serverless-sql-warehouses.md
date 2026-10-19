# 5.5 All-Purpose vs Jobs vs Serverless Compute; SQL Warehouses

**Time:** 25 min · **Prerequisites:** 5.3, 5.4

**After this topic you can:**
- choose between all-purpose, jobs, serverless and SQL warehouse compute for a given workload, and justify it;
- explain interactive development vs production execution and why they use different compute;
- explain whether a notebook can run without a dedicated interactive cluster;
- explain cluster vs SQL warehouse in one breath;
- describe how a job uses compute (link to Phase 8).

---

## A. Concept deep dive

### 1. The simplest explanation

| Compute | One-line description |
|---|---|
| **All-purpose** | "My team's workbench" — a cluster we start, share and stop for interactive work |
| **Jobs** | "A disposable machine for one job run" — created by the scheduler, killed when done |
| **Serverless** (notebooks / jobs / pipelines) | "Compute as a service" — pick *Serverless*, Databricks provides and scales machines in seconds |
| **SQL warehouse** | "A SQL engine endpoint" — compute for SQL queries, dashboards and BI tools |

### 2. Why different kinds exist — interactive vs production

| Need | Interactive development | Production execution |
|---|---|---|
| Who runs it | A human, iteratively | A scheduler / trigger |
| Pattern | Many short commands, idle gaps, exploring | Fixed code, start → finish |
| What matters | Fast feedback, shared libraries, notebooks attached | Isolation, reproducibility, cost per run, retries, identity |
| Right compute | Serverless notebooks, or all-purpose (with auto-termination) | Serverless jobs, or jobs compute; SQL tasks on a SQL warehouse |
| Identity | The user | A **service principal** (`run_as`) |

Running production on all-purpose compute is the classic anti-pattern: higher DBU rate, other users' notebooks share the driver, a restart by a developer kills the job, and libraries can change under you.

### 3. Side-by-side comparison

| | All-purpose | Jobs compute | Serverless (notebooks/jobs) | SQL warehouse |
|---|---|---|---|---|
| Created by | User / API (via policy) | Job scheduler per run | Databricks on demand | Admin / privileged user |
| Lives | Until terminated / auto-terminated | One run | Per session / run | Starts on query, auto stops |
| Startup | Minutes (faster with pools) | Minutes (faster with pools) | Seconds (jobs: standard mode may take ~4–6 min) | Serverless: ~2–6 s; pro/classic ~4 min |
| Languages | Python, SQL, Scala, R (R needs Dedicated) | Same | Python, SQL (notebooks: no Scala/R) | **SQL only** |
| Config control | Full (within policy) | Full (within policy) | Minimal (environment, memory, performance mode) | Size, min/max clusters, auto stop, type |
| Infra in | Your subscription | Your subscription | Databricks' serverless plane | Serverless: Databricks; pro/classic: yours |
| Cost model | VM + All-Purpose DBU rate (higher) | VM + Jobs DBU rate (lower) | Serverless DBU rate (infra included) | SQL DBU rate by type/size |
| Spark UI | Yes | Yes | No (query profile instead) | Query profile |
| Best for | Dev on special configs, R/GPU/ML, legacy needs | Production ETL needing classic features | Most new interactive + production work | BI, dashboards, SQL ETL, dbt |

(Exact DBU prices differ by cloud, region and plan — check the Azure Databricks pricing page; don't quote numbers in an interview unless you're sure.)

### 4. How jobs use compute (preview of Phase 8)

```
Lakeflow Job: daily_revenue
 ├── task ingest_orders ─┐   job_cluster_key: etl   ─┐
 ├── task build_silver ──┼─  job_cluster_key: etl    ├─▶ ONE jobs cluster "etl", created at run start,
 ├── task build_gold   ──┘   job_cluster_key: etl   ─┘   shared by these tasks, terminated at run end
 ├── task refresh_kpis       sql_task on SQL warehouse ─▶ existing warehouse (auto-starts)
 └── task notify             (no compute)            ─▶ serverless (if supported task type)
```

- **Task compute choices:** a `job_cluster_key` (jobs compute defined in the job), `existing_cluster_id` (all-purpose — not recommended), no compute (serverless for supported task types: notebook, Python script, dbt, Python wheel, JAR), or a SQL warehouse for SQL tasks.
- Several tasks can **share** a job cluster → avoids paying startup per task.
- If a job targets a **terminated all-purpose** cluster, the cluster **autostarts**; a jobs cluster is never reused after the run.
- Retries, timeouts, parameters, task values → Phase 8.

### 5. Can a notebook run without a dedicated interactive cluster?

**Yes, two ways (verified):**

1. **Serverless compute for notebooks** — choose *Serverless* in the compute drop-down. Requirements: workspace enabled for Unity Catalog, in a supported region, serverless interactive compute enabled. Supports **Python and SQL**; Scala and R aren't supported in notebooks. New notebooks default to serverless on first run if nothing else is selected.
2. **SQL warehouse** — a notebook can attach to a **pro or serverless** SQL warehouse. Only **SQL and Markdown** cells run; Python/R cells error. Variables/temp views persist across runs in a *SQL warehouse session*.

And as a scheduled job: notebook tasks with no cluster run on **serverless jobs** compute.

### 6. Cluster vs SQL warehouse

| | Cluster (all-purpose / jobs) | SQL warehouse |
|---|---|---|
| Purpose | General Spark: Python, Scala, R, SQL, ML, streaming | SQL analytics: queries, dashboards, BI, SQL ETL |
| Interface | Notebooks, jobs, Databricks Connect | SQL editor, dashboards, Genie, JDBC/ODBC (Power BI, Tableau), SQL tasks, notebooks (SQL only) |
| Sizing | Node type × number of workers | T-shirt size (2X-Small … 4X-Large) + min/max clusters for concurrency (≈1 cluster per 10 concurrent queries) |
| Concurrency | Shared driver — many users compete | Built for many concurrent queries; adds clusters under load |
| Engine | DBR (Photon optional) | Photon always; serverless adds Predictive IO + Intelligent Workload Management |
| Access mode | Standard / Dedicated | Not applicable; UC enforced |
| Start | Minutes | Serverless: seconds |

> Interview line: *"A cluster is general-purpose Spark compute for code; a SQL warehouse is managed compute optimised for SQL and BI concurrency — I use clusters or serverless for pipelines and warehouses for analysts and dashboards."*

### 7. Choosing compute — decision table

| Workload | First choice | Fallback / when not |
|---|---|---|
| Scheduled PySpark/SQL ETL | **Serverless jobs** | **Jobs compute** if you need R, RDD, init scripts, custom Spark confs, specific VMs/networking, GPU, >7-day runs, `processingTime` streaming triggers |
| Interactive exploration (Python/SQL) | **Serverless notebooks** | All-purpose with auto-termination if you need Scala/R, Spark UI, special libraries |
| BI dashboards / Power BI / ad-hoc SQL | **Serverless SQL warehouse** | Pro warehouse if serverless unavailable or custom networking to on-prem |
| SQL-only transformations in a job | SQL task on a SQL warehouse | Or notebook on serverless jobs |
| Declarative pipelines (streaming tables, MVs) | **Serverless pipeline** | Classic pipeline compute with enhanced autoscaling |
| Always-on streaming job (`processingTime`) | Jobs compute (continuous job) or pipeline in continuous mode | Serverless supports only `availableNow`/`once` triggers |
| ML training on GPU / R | Dedicated all-purpose or jobs compute with ML runtime | — |
| Tiny Python script, small data | Serverless or single-node jobs compute | — |

### 8. Real example — ShopCo

| Workload | Compute |
|---|---|
| Engineers develop Silver logic | Serverless notebooks; one Standard all-purpose cluster (auto-terminate 30 min) for Scala-based legacy code |
| Nightly `orders_etl` job (PySpark, MERGE) | Serverless jobs, standard performance mode (startup delay is acceptable at 02:00) |
| Hourly finance job with SLA 10 min | Serverless jobs, performance optimized |
| Kafka clickstream, always on, 30 s micro-batches | Jobs compute (continuous job, Standard access mode, fixed size) |
| Power BI revenue dashboard | Serverless SQL warehouse, Small, min 1 / max 3 clusters, auto stop 10 min |

### 9. Legacy vs current terminology

| Legacy | Current |
|---|---|
| Interactive cluster | All-purpose compute |
| Automated / job cluster | Jobs compute |
| SQL endpoint | SQL warehouse |
| Databricks SQL Serverless "endpoint" | Serverless SQL warehouse |
| Workflows | Lakeflow Jobs (sidebar: *Jobs & Pipelines*) |

### 10. Misconceptions

1. **"Serverless is always more expensive."** Infra is included in the DBU price and you don't pay idle VMs or startup; for spiky/short workloads it's often cheaper. Measure with system tables (5.7).
2. **"A SQL warehouse can run my PySpark notebook."** SQL cells only.
3. **"Jobs on all-purpose compute are fine, it's already running."** Higher DBU rate, contention, no isolation, fragile.
4. **"Jobs compute starts instantly."** It provisions VMs (minutes) unless backed by a pool.

---

## B. Practical

### B.1 Compatibility check: does my code run on serverless?

**Runnable notebook:** [`exercises/phase-05/05_serverless_compatibility.py`](../../exercises/phase-05/05_serverless_compatibility.py) — runs several patterns and reports which work on the attached compute.

| Pattern | Classic (Dedicated) | Classic (Standard) | Serverless |
|---|---|---|---|
| DataFrame API, SQL, Delta MERGE | ✅ | ✅ | ✅ |
| `spark.sparkContext.parallelize` (RDD) | ✅ | ❌ | ❌ |
| `df.cache()` | ✅ | ✅ | ❌ (raises an exception) |
| Streaming `trigger(availableNow=True)` | ✅ | ✅ | ✅ |
| Streaming `trigger(processingTime="30 seconds")` / default trigger | ✅ | ✅ | ❌ `INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED` |
| Python UDF | ✅ | ✅ | ✅ (no internet access, ≤1 GB memory) |

**Expected:** run it on two different compute types and compare the printed ✅/❌ table with the one above.

### B.2 UI walkthrough — attach a SQL notebook to a SQL warehouse

1. Create a notebook, default language **SQL**.
2. Compute drop-down → *More…* → *SQL Warehouse* → pick a **serverless or pro** warehouse → *Attach*.
3. Run `SELECT current_user(), current_catalog();` → works.
4. Add a `%python` cell → it fails (SQL warehouses run SQL/Markdown only).

### B.3 UI walkthrough — make a job use serverless vs jobs compute

1. *Jobs & Pipelines → Create → Job* → add a **Notebook** task → compute shows **Serverless** by default.
2. In *Job details → Performance optimized*: off = standard mode (cheaper, slower start), on = faster.
3. Switch the task's compute to *New job cluster* → note you now control runtime, node type, workers, access mode.

### Common mistakes
- Pointing production tasks at `existing_cluster_id` of a developer's all-purpose cluster.
- Using a serverless job for a `processingTime` streaming query → fails immediately.
- Giving analysts an all-purpose cluster instead of a SQL warehouse → poor concurrency, higher cost.
- One job cluster per task in a 15-task job → 15 startups; share a job cluster.

---

## C. Interview questions

### 🟢 Basic

**B1.** What is the difference between a cluster and a SQL warehouse?

<details><summary>Model answer</summary>

A cluster is general-purpose Spark compute (driver + workers) for Python, Scala, R and SQL code in notebooks and jobs, configured by node type and worker count, with an access mode. A SQL warehouse is compute dedicated to SQL — queries, dashboards, BI tools via JDBC/ODBC — sized by T-shirt size and number of clusters for concurrency, always using Photon; serverless warehouses start in seconds. Pipelines → clusters/serverless; analysts and BI → warehouses.
</details>

**B2.** Can a notebook run without a dedicated interactive cluster?

<details><summary>Model answer</summary>

Yes. With serverless compute for notebooks (Unity Catalog-enabled workspace, supported region) you select *Serverless* and run Python or SQL without any cluster. A SQL-only notebook can also attach to a pro or serverless SQL warehouse (SQL and Markdown cells only). Scheduled notebook tasks can run on serverless jobs compute.
</details>

**B3.** Why use jobs compute rather than all-purpose compute for production?

<details><summary>Model answer</summary>

Jobs compute is created for each run and terminated afterwards: isolation from interactive users, a clean reproducible environment, no idle cost, and a lower DBU rate. All-purpose compute is shared, mutable, billed at a higher rate and can be restarted by someone mid-run.
</details>

### 🟡 Medium

**M1.** When would you *not* use serverless for a job?

<details><summary>Model answer</summary>

When the workload hits serverless limitations: R, RDD APIs, Spark UI needed for tuning, unsupported Spark configs, init scripts or compute-scoped libraries, custom containers, `processingTime`/continuous streaming triggers, runs longer than 7 days, specific VM types/GPUs on classic, or network requirements not configured for the serverless plane. Then use jobs compute.
</details>

**M2.** A job has 12 tasks, each with its own new job cluster. It's slow and costly. What do you change?

<details><summary>Model answer</summary>

Define one or a few shared job clusters (`job_clusters` + `job_cluster_key`) so tasks reuse the same compute and pay startup once; group tasks with similar needs; or move supported tasks to serverless; consider an instance pool for faster starts. Check the tasks' actual resource needs before sizing.
</details>

**M3.** Explain standard vs performance-optimized mode for serverless jobs.

<details><summary>Model answer</summary>

Both use the same SKU. Standard mode uses less compute and consumes fewer DBUs but tolerates higher startup latency (around 4–6 minutes); performance-optimized starts and runs faster for time-sensitive workloads. Pick standard for overnight batch with slack, performance-optimized for tight SLAs.
</details>

### 🔴 Tricky

**T1.** Your streaming job runs fine on a classic cluster but fails immediately on serverless with `INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED`. Explain and fix.

<details><summary>Model answer</summary>

Serverless supports only `availableNow` (recommended) and the deprecated `once` triggers. Without a trigger Spark defaults to `processingTime("0 seconds")`, an infinite trigger, which serverless rejects. Options: set `trigger(availableNow=True)` and schedule/continuously re-run the job; use a pipeline in continuous mode; or keep it on classic jobs compute.
</details>

**T2.** Analysts complain that the shared all-purpose cluster is slow at 9 am when 30 people run queries. What do you propose?

<details><summary>Model answer</summary>

Move SQL/BI usage to a serverless SQL warehouse sized for typical queries, with max clusters for concurrency (≈1 cluster per 10 concurrent queries) and short auto stop. A single cluster's driver is a shared bottleneck and it doesn't scale out concurrency the way a warehouse adds clusters. Keep the all-purpose cluster for engineering work only.
</details>

**T3.** A job scheduled at 02:00 points to an all-purpose cluster that auto-terminated at midnight. What happens?

<details><summary>Model answer</summary>

The terminated all-purpose cluster autostarts for the scheduled job (after permissions are checked), so the run starts late by the cluster's startup time and is billed at all-purpose rates; other users may attach meanwhile. Better: give the job its own jobs compute or serverless.
</details>

---

## E. Revision summary

- **Interactive → serverless notebooks / all-purpose. Production → serverless jobs / jobs compute. SQL & BI → SQL warehouse.**
- All-purpose: user-managed, long-lived, higher rate. Jobs: per run, terminated after, lower rate. Serverless: seconds to start, no config, standard-mode semantics + limitations.
- Notebook without a cluster: **serverless** (Python/SQL) or **SQL warehouse** (SQL only, pro/serverless).
- Cluster = general Spark; SQL warehouse = SQL/BI concurrency, Photon always, sized by T-shirt + clusters.
- Share job clusters across tasks; never run prod on someone's all-purpose cluster.