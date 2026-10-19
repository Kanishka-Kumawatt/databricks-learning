# 8.1 Notebook vs Task vs Job vs Pipeline vs Workflow

**Time:** 15 min · **Prerequisites:** 1.11, 5 (compute types)

**After this topic you can:**
- define notebook, task, job, job run, task run, pipeline, pipeline update, flow and "workflow" precisely;
- explain the two different DAGs in Databricks (tasks in a job vs datasets in a pipeline);
- say which product owns which responsibility (Lakeflow Jobs vs Lakeflow Spark Declarative Pipelines);
- map a ShopCo design onto these terms without mixing them up.

---

## A. Concept deep dive

### 1. The simplest explanation

- A **notebook** is a *file of code*. It does nothing until something runs it.
- A **task** is *one step* of a job: "run this notebook / this SQL / this pipeline, on this compute, with these inputs, retry twice".
- A **job** is the *orchestration unit*: a DAG of tasks plus *when* (triggers), *as whom* (run-as), *what if it fails* (retries, notifications).
- A **pipeline** is a *declarative ETL unit*: you declare tables and views; the framework works out order, incrementality, checkpoints and compute, and refreshes them in an **update**.
- **"Workflow"** is not a separate object today. It was the old product name ("Databricks Workflows") for jobs, and in conversation it means "the whole orchestrated process".

### 2. Precise definitions

| Term | What it is | One execution is called | Product |
|---|---|---|---|
| **Notebook** | A source file of cells (Python/SQL/Scala/R). Runs interactively or as a task | (interactive run) / task run | Workspace |
| **Task** | A node in a job's DAG: a type (notebook, pipeline, SQL, …), compute, parameters, retries, timeout, dependencies, run-if | **task run** (retries are new attempts of the task run) | Lakeflow Jobs |
| **Job** | Tasks + dependencies + triggers + job parameters + notifications + health rules + concurrency/queue + run-as + permissions | **job run** (contains task runs) | Lakeflow Jobs (formerly Databricks Workflows / Jobs) |
| **Pipeline** | Source files (`.py` / `.sql`) that *declare* datasets and flows + settings (catalog, schema, compute, mode) | **update** | Lakeflow Spark Declarative Pipelines (SDP; formerly Delta Live Tables, DLT) |
| **Dataset** | A streaming table, materialized view or view declared in a pipeline | refreshed during an update | SDP |
| **Flow** | The query that writes into a dataset (append flow, AUTO CDC flow, MV flow). Streaming flows own a checkpoint | flow progress inside an update | SDP |
| **Workflow** | Informal: the end-to-end orchestrated process. Historical product name for Jobs | — | — |

### 3. Two different DAGs — the key idea

```
JOB  (task DAG — you draw the arrows with depends_on)
 check_landing ──▶ has_data ──▶ refresh_pipeline ──▶ per_region_report ──▶ publish_summary
                                     │
                                     ▼  (a pipeline task starts one UPDATE)
PIPELINE  (dataset DAG — the framework infers the arrows from your queries)
 bronze_orders ──▶ orders_prepared ──▶ silver_orders ──▶ gold_daily_revenue
                                  └──▶ silver_orders_quarantine
```

| | Task DAG (job) | Dataset DAG (pipeline) |
|---|---|---|
| Nodes | Tasks (units of *work*) | Datasets (units of *data*) |
| Edges | Declared by you: `depends_on`, outcomes, run-if | **Inferred** from what each query reads |
| Granularity | Coarse: a whole notebook, a whole pipeline | Fine: each table/view |
| State between runs | None, unless you build it (watermarks, task values per run) | Managed: checkpoints, MV refresh state, schemas |
| Failure unit | Task (retry, repair) | Flow/dataset (refresh failed tables) |

### 4. Who is responsible for what

| Concern | Lakeflow Jobs | Lakeflow SDP pipeline | You |
|---|---|---|---|
| When it runs | Triggers, schedules | Triggered by a job/API/UI, or continuous | Choose the trigger |
| Order of steps | Your `depends_on` | Inferred from dataset references | Write correct references |
| Incremental state | — | Checkpoints, Auto Loader schema location | Pick ST vs MV correctly |
| Compute | Job compute, serverless, (all-purpose) | Serverless or classic pipeline cluster, autoscaling | Size/cost decisions |
| Retries | Per task, configured by you | Automatic in production-style runs | Idempotent logic |
| Data quality | Your code (raise to fail) | **Expectations** + metrics | Choose rules and actions |
| Observability | Run history, task output, system tables | Event log, pipeline graph, Data quality tab | Alerts on what matters |

### 5. When to use which

| Situation | Use |
|---|---|
| Bronze → Silver → Gold tables, incremental ingestion, quality rules | **Pipeline** (inside a job, or continuous) |
| Steps that are not table definitions: call an API, send a report, run ML training, branch on a condition, loop over regions | **Job tasks** (notebook, Python, SQL…) |
| Combine both | **A job whose tasks include a pipeline task** — the common production shape |
| Ad-hoc exploration | Notebook on interactive compute |

### 6. Misconceptions

1. **"A pipeline is a kind of job."** A pipeline is its own resource with its own compute and update history. A *pipeline task* lets a job start an update.
2. **"A pipeline source file runs top to bottom like a notebook."** The runtime evaluates the file to *collect definitions*, builds a graph, then decides what to run and in which order. `display()`, `collect()` or writes inside dataset functions are wrong there.
3. **"Workflows and Jobs are different products."** Same thing; "Workflows" is the legacy name. Current name: **Lakeflow Jobs**.
4. **"One notebook = one job."** Related steps belong in one job with dependencies so failures, retries and repair are coordinated (1.11).
5. **"DLT is gone."** It was renamed: Delta Live Tables → Lakeflow Declarative Pipelines → **Lakeflow Spark Declarative Pipelines**, built on the open-source Apache Spark Declarative Pipelines API. Old `import dlt` code still runs; new code uses `from pyspark import pipelines as dp`.

---

## B. Practical — name everything in the ShopCo bundle

Files: [`databricks.yml`](../../exercises/phase-08/databricks.yml), [`02_shopco_daily.job.yml`](../../exercises/phase-08/02_shopco_daily.job.yml), [`07_shopco_pipeline.pipeline.yml`](../../exercises/phase-08/07_shopco_pipeline.pipeline.yml), [`05_shopco_pipeline.py`](../../exercises/phase-08/05_shopco_pipeline.py).

| Object in the repo | Term |
|---|---|
| `02a_check_landing.py` | Notebook |
| `- task_key: check_landing` | Task (type: notebook) |
| `- task_key: refresh_pipeline` + `pipeline_task` | Task (type: pipeline) that starts a pipeline **update** |
| `resources.jobs.shopco_daily_orders` | Job |
| `resources.pipelines.shopco_orders_py` | Pipeline |
| `05_shopco_pipeline.py` | Pipeline source file (not a notebook — no `# Databricks notebook source` header) |
| `bronze_orders`, `silver_orders`, `gold_daily_revenue` | Datasets (streaming tables / materialized view) |
| The query inside `def silver_orders()` | The (default) flow into `silver_orders` |
| The whole bundle deployed and running daily | "the workflow" (informal) |

Try it after `databricks bundle deploy -t dev`:

```bash
databricks bundle summary -t dev                  # lists the deployed job and pipelines with their IDs/URLs
databricks bundle run shopco_daily_orders -t dev  # one JOB RUN -> several TASK RUNS -> one pipeline UPDATE
```

Expected: the job run page shows 7 tasks (one branch Excluded); clicking `refresh_pipeline` opens the pipeline update with its own dataset graph.

### Common mistakes

- Scheduling the pipeline **and** a job that also triggers it → two updates compete; one orchestrator only.
- Putting non-table side effects (emails, API calls) inside pipeline code instead of a job task.
- Calling a notebook "a job" in an interview — precision matters in this assessment.

---

## C. Interview questions

### 🟢 Basic

**B1.** What is the difference between a job and a task?

<details><summary>Model answer</summary>

A task is one unit of work (run a notebook, SQL, pipeline, another job…) with its own compute, parameters, retries and timeout. A job is the container that orchestrates tasks: their dependencies (DAG), triggers, job parameters, notifications, concurrency and run-as identity. One job run contains one task run per task (plus retry attempts).
</details>

**B2.** What is a pipeline update?

<details><summary>Model answer</summary>

One execution of a Lakeflow Spark Declarative Pipeline: the framework starts compute, evaluates the source files, builds and validates the dataflow graph, creates/updates the tables, and refreshes the selected datasets (all by default) in dependency order. Its progress and metrics go to the event log.
</details>

**B3.** Is "workflow" a Databricks object?

<details><summary>Model answer</summary>

Not a separate object today. "Databricks Workflows" was the earlier product name for jobs (and the umbrella for jobs + DLT). The current product is Lakeflow Jobs; in conversation "workflow" means the end-to-end orchestrated process.
</details>

### 🟡 Medium

**M1.** Contrast the DAG of a job with the DAG of a pipeline.

<details><summary>Model answer</summary>

Job DAG: nodes are tasks, edges are declared explicitly with `depends_on` (plus if/else outcomes and run-if), no data state is kept between runs. Pipeline DAG: nodes are datasets, edges are inferred from which datasets each query reads, and the framework keeps state (checkpoints, MV refresh info, schemas) between updates and decides order, parallelism and incrementality.
</details>

**M2.** When would you put logic in a pipeline and when in a plain notebook task?

<details><summary>Model answer</summary>

Pipeline: declarative table transformations — ingestion, cleaning, aggregations, quality rules, CDC — where you want the framework to manage dependencies, checkpoints, incremental refresh and data-quality metrics. Notebook task: imperative steps that are not table definitions — API calls, notifications, conditional orchestration, ML training, external system writes, complex control flow. Typical production: a job with notebook tasks around a pipeline task.
</details>

**M3.** Why shouldn't a pipeline source file contain `df.collect()` or `df.write.saveAsTable()`?

<details><summary>Model answer</summary>

The file is evaluated to collect dataset definitions, possibly more than once (planning/validation), not executed as a script. Actions there run at the wrong time, may run repeatedly, slow planning, and writes bypass the framework (no checkpoint, no lineage, no expectations, not idempotent). Dataset functions must just *return* a DataFrame.
</details>

### 🔴 Tricky

**T1.** A team schedules the pipeline every hour in its own settings *and* has a job with a pipeline task every hour. What goes wrong?

<details><summary>Model answer</summary>

Two orchestrators start updates of the same pipeline. A triggered pipeline runs one update at a time, so one request conflicts or waits, timings drift, and the job's downstream tasks may read data from an update the job didn't wait for. Choose one orchestrator — usually the job, because it also controls dependencies, notifications and downstream tasks. (Pipeline "schedules" in the UI are themselves implemented as jobs.)
</details>

**T2.** "We migrated from DLT to SDP, so we must rewrite all pipelines." True?

<details><summary>Model answer</summary>

No. It is mainly a rename plus a new open-source-aligned Python module. `import dlt` code continues to work; Databricks recommends `from pyspark import pipelines as dp` for new code and future features. Things to actually change: legacy `LIVE.` references / legacy publishing mode (planned for removal), `dlt.read`/`read_stream` → `spark.read/readStream.table`, `apply_changes` → `create_auto_cdc_flow`. Migrate gradually.
</details>

**T3.** A job "succeeded" but `gold_daily_revenue` is stale. The pipeline task is green. How is that possible?

<details><summary>Model answer</summary>

Green means the update finished without errors, not that new data was processed: no new files arrived (Auto Loader found nothing), expectations dropped everything (drop is not a failure), the job passed `refresh_selection` that excluded Gold, or the task ran against the wrong pipeline/target schema (dev vs prod). Check the event log (`flow_progress` output rows, dropped records), the landing folder, and add freshness checks downstream.
</details>

---

## E. Revision summary

- Notebook = code file. Task = one step. Job = DAG of tasks + triggers + params + notifications + run-as. Pipeline = declared datasets; one execution = **update**. Flow = query writing a dataset. Workflow = legacy/informal name.
- Two DAGs: tasks (explicit `depends_on`) vs datasets (inferred from references).
- Production shape: **job → pipeline task → (notebook/SQL tasks downstream)**.
- DLT → Lakeflow Spark Declarative Pipelines; `import dlt` still works, write new code with `from pyspark import pipelines as dp`.
- Pipeline source files are evaluated for definitions; dataset functions only return DataFrames.