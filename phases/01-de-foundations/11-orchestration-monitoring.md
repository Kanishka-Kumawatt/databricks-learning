# 1.11 Orchestration, Scheduling, Dependencies and Monitoring

**Time:** 30 min · **Prerequisites:** 1.1, 1.8

**After this topic you can:**
- distinguish scheduling from orchestration;
- explain DAGs, dependencies, triggers, parameters, retries, timeouts and concurrency;
- explain why job monitoring is not enough and what data observability adds;
- read and write a Databricks job definition.

---

## A. Concept deep dive

### 1. The simplest explanation

- **Scheduling** = *when* something runs ("every day at 05:00").
- **Orchestration** = coordinating *many* steps: what runs, in which order, with which inputs, on which compute, and what happens when something fails.

A cron entry is scheduling. "Load orders and payments in parallel, build Silver when both finish, retry transient failures twice, alert on-call if Gold isn't ready by 07:00" is orchestration.

### 2. Why orchestration exists

Without it, teams chain scripts by time ("payments at 02:00, hope orders finished by then"). That breaks when anything is slow, late or fails — and nobody is told.

### 3. Core concepts

| Concept | Meaning | Example |
|---|---|---|
| **DAG** (directed acyclic graph) | Tasks as nodes, dependencies as arrows, no cycles | ingest → silver → gold → checks |
| **Task** | One unit of work | Run a notebook, a SQL query, a pipeline, another job |
| **Dependency** | Task B starts only when A finishes (with a condition) | `build_silver` after both ingestions |
| **Fan-out / fan-in** | One task triggers many in parallel / many must finish before one | Ingest 10 sources in parallel, then one Silver build |
| **Trigger** | What starts a run | Time (cron), **file arrival**, table update, upstream job, API/manual |
| **Parameters** | Inputs to a run | `run_date`, `env`, `source_system` |
| **Retries** | Re-run a failed task automatically | 2 retries, 5 min apart — for *transient* errors |
| **Timeout** | Kill a task/run that runs too long | Prevents a hung job blocking the next day |
| **Concurrency limit** | Max simultaneous runs of a job | `max_concurrent_runs: 1` — no overlapping runs |
| **Run-if conditions** | Run a task only if upstream succeeded / failed / always | Cleanup task that always runs; alert task on failure |
| **Backfill** | Run the pipeline for past periods | Re-run 30 days with `run_date` overridden |
| **SLA** | When data must be ready | Gold by 07:00 |

### 4. Orchestration tools

| Tool | Notes |
|---|---|
| **Databricks Lakeflow Jobs** (formerly Workflows / Jobs) | Native: notebooks, Python, SQL, declarative pipelines, dbt, run-job, if/else and for-each tasks; job compute; file-arrival and table-update triggers; repair runs (re-run only failed tasks) |
| **Azure Data Factory** | Strong at connecting to many sources (Copy activity), hybrid networks (self-hosted IR), orchestrating across Azure services (Phase 10) |
| **Apache Airflow** | Open-source, Python-defined DAGs, platform-neutral |

### 5. Retries — useful only for the right failures

| Failure | Retry helps? |
|---|---|
| Network blip, throttled API, cloud capacity, spot instance lost | ✅ transient |
| Concurrent write conflict | ✅ often |
| Bug in code, missing column, wrong permission | ❌ fails again identically — wastes time and money |
| Bad source data | ❌ fix data/logic; maybe quarantine |

**Retries are only safe if the task is idempotent** (topic 1.12). Retrying an `append` duplicates data.

### 6. Monitoring — jobs vs data

| Job monitoring | Data observability |
|---|---|
| Did the run succeed or fail? | Is the data **fresh**? (last update / max event time vs SLA) |
| How long did it take? | **Volume**: is today's row count normal? |
| Which task failed, with which error? | **Schema**: did columns/types change? |
| Retries, cost | **Distribution**: did null rate or average amount jump? |
| | **Quality**: rule failures, quarantine rate, reconciliation |

A green job can produce wrong or empty data (1.1). Monitoring must cover both.

**Alerting good practice:** alert on symptoms that need action, route to an owner/on-call, include run link and context, avoid noisy alerts that people learn to ignore.

### 7. Misconceptions

1. **"Schedule upstream at 02:00 and downstream at 03:00."** Time-based chaining breaks on delays — use dependencies or triggers.
2. **"More retries = more reliable."** Retries hide bugs and duplicate data when tasks aren't idempotent.
3. **"Job succeeded = SLA met."** Data could be stale or empty.
4. **"Every notebook should be its own scheduled job."** Related steps belong in one job with dependencies, so failures and re-runs are coordinated.

---

## B. Practical — a production-style job definition

**File:** [`exercises/phase-01/11_daily_revenue.job.yml`](../../exercises/phase-01/11_daily_revenue.job.yml) (Databricks Asset Bundle format)

```
 ingest_orders ──┐
                 ├──▶ build_silver ──▶ build_gold_revenue ──▶ dq_and_reconciliation
 ingest_payments ┘
```

| Setting | Why |
|---|---|
| `schedule` (Quartz cron + `timezone_id`) | Explicit time zone avoids DST/UTC surprises |
| `max_concurrent_runs: 1` | A slow run and the next scheduled run never overlap and fight over the same tables |
| `timeout_seconds` (job and task) | A hung task can't block tomorrow's run |
| `parameters.run_date = {{job.start_time.iso_date}}` | Default to the run's date; **override for backfills** without code changes |
| `max_retries` + `min_retry_interval_millis` on ingestion | Transient source/network failures; ingestion must be idempotent |
| `depends_on` with two upstream tasks | Fan-in: Silver waits for orders **and** payments |
| `job_clusters` | **Job compute**: created for the run, terminated after → cheaper than an always-on all-purpose cluster (Phase 5) |
| `health` rule on run duration | Early warning before the SLA is missed |
| `email_notifications.on_failure` | Someone is told |
| Final DQ/reconciliation task | Turns "silent wrong data" into a failed, alerted run |

Reading the parameter inside a notebook task:

```python
from datetime import date, timedelta

run_date = dbutils.widgets.get("run_date")                     # job parameters arrive as widgets
process_date = (date.fromisoformat(run_date) - timedelta(days=1)).isoformat()   # e.g. process "yesterday"
```

Raising an exception from the reconciliation notebook fails the task, which fails the run and triggers the notification:

```python
if gold_total != silver_total:
    raise ValueError(f"Reconciliation failed for {process_date}: gold={gold_total} silver={silver_total}")
```

---

## C. Interview questions

### 🟢 Basic

**B1.** What is the difference between scheduling and orchestration?

<details><summary>Model answer</summary>

Scheduling decides when a job starts. Orchestration coordinates multiple tasks: dependencies and order, parameters, compute, retries, timeouts, failure handling and notifications.
</details>

**B2.** What is a DAG in the context of pipelines?

<details><summary>Model answer</summary>

A directed acyclic graph: tasks are nodes, dependencies are directed edges, and there are no cycles, so there's always a valid execution order. Independent branches can run in parallel.
</details>

### 🟡 Medium

**M1.** Why is chaining jobs by time ("A at 02:00, B at 03:00") a bad idea?

<details><summary>Model answer</summary>

If A is slow, late or fails, B still runs at 03:00 on incomplete or stale data and nobody is warned; if A is fast, B waits needlessly. Dependencies (B starts when A succeeds) or event triggers make the chain correct and as fast as possible.
</details>

**M2.** Which failures should be retried automatically and which shouldn't?

<details><summary>Model answer</summary>

Retry transient failures: network errors, throttling, temporary capacity issues, lost spot instances, some concurrent-write conflicts. Don't rely on retries for deterministic failures: code bugs, schema changes, permission errors, bad data — they fail again. Retries also require the task to be idempotent.
</details>

**M3.** Your job is green every day. What else should you monitor, and why?

<details><summary>Model answer</summary>

Data observability: freshness (last update vs SLA), volume (row counts vs normal), schema changes, distribution shifts (null rates, averages), rule failures and quarantine rate, reconciliation between layers/source. A job can succeed while processing nothing or processing wrong data.
</details>

### 🔴 Tricky

**T1.** The source file normally arrives at 04:30 but sometimes at 06:00. Your job is scheduled at 05:00. Design a better approach.

<details><summary>Model answer</summary>

Use a **file-arrival trigger** (or a sensor/wait task with a timeout) so the job starts when the file lands; make ingestion incremental and idempotent (Auto Loader) so early/late/duplicate files are safe; set an SLA alert if the file hasn't arrived by a cut-off time and notify the source owner; optionally publish data with an "as of" timestamp.
</details>

**T2.** A job's run takes longer than its schedule interval, and runs start overlapping. What problems arise and how do you prevent them?

<details><summary>Model answer</summary>

Two runs write the same tables concurrently → write conflicts, duplicates (if not idempotent), watermarks updated out of order, resource contention making both slower. Prevent with `max_concurrent_runs: 1` (later runs are skipped/queued), timeouts, making the job faster or incremental, and idempotent writes.
</details>

**T3.** A task that appends to a table failed after writing its data, during a later step in the same notebook. The orchestrator retries it. What happens?

<details><summary>Model answer</summary>

The append runs again → duplicate rows. The retry setting turned a harmless failure into data corruption. Fix the task to be idempotent (MERGE by key, `replaceWhere` overwrite of the run's slice, or Delta idempotent writes with `txnAppId`/`txnVersion`), or split the work so the write is the last, atomic step.
</details>

---

## E. Revision summary

- Scheduling = *when*; orchestration = *what, order, inputs, compute, failure handling*.
- DAG, dependencies, fan-out/fan-in, triggers (time, file arrival, table update), parameters, retries, timeouts, concurrency, backfills.
- Retry **transient** errors only — and only **idempotent** tasks.
- Monitor jobs **and** data: freshness, volume, schema, distribution, quality.
- Use dependencies or event triggers, never time-chaining.