# 12.2 Orchestration — ADF Master/Wrapper/Child, Metadata-Driven Loads, and the Move to Databricks Jobs

**Time:** 20 min · **Prerequisites:** 1.11, Phase 8 (Jobs), Phase 10 (ADF)

**After this topic you can:**
- explain the master → wrapper → child pipeline hierarchy and why teams build it;
- describe a metadata-driven Lookup → ForEach → Copy/Notebook pattern end to end;
- explain how failures were alerted and how parallel notebook fan-out worked (and its pitfalls);
- argue when to keep ADF and when to move orchestration into Databricks Jobs deployed with Asset Bundles.

---

## A. Concept / story

### 1. The hierarchy

```
 Schedule trigger (e.g. daily 02:00)
        │
        ▼
 MASTER pipeline ── one per domain / load window; owns ordering between subject areas
        │  Execute Pipeline (wait on completion = true)
        ├──────────────► WRAPPER pipeline: "bookings subject area"
        │                    │  1. Web/Notebook: audit "pipeline start"
        │                    │  2. Lookup: read config rows for this subject area
        │                    │  3. ForEach (parallel, batchCount N) ──► CHILD pipeline per source object
        │                    │                                          ├─ Copy  (source → landing)
        │                    │                                          ├─ Notebook (landing → raw → staging)
        │                    │                                          └─ on failure → audit error + alert
        │                    │  4. Notebook: curated build
        │                    └─ 5. audit "pipeline end" (success/fail)
        └──────────────► WRAPPER pipeline: "flights subject area" ...
```

| Level | Responsibility | Why separate |
|---|---|---|
| **Master** | Order and dependencies between subject areas; one trigger | One place to see "did last night's load finish?" |
| **Wrapper** | One subject area: audit start/end, read metadata, fan out, build curated | Reusable unit that can be re-run alone |
| **Child** | One object: copy + load notebook + per-object error handling | Generic — parameterised by the config row |

### 2. Metadata-driven loading

Instead of one pipeline per table, a **config (control) table** lists what to load:

| object_id | source_system | source_object | load_type | watermark_col | target_table | is_active | load_order |
|---|---|---|---|---|---|---|---|
| 101 | res_db | dbo.Booking | incremental | modified_ts | staging.bookings | 1 | 1 |
| 102 | res_db | dbo.Passenger | incremental | updated_at | staging.passengers | 1 | 1 |
| 201 | partner_sftp | fares_*.csv | file | — | raw.partner_fares | 1 | 2 |

`Lookup` returns the active rows → `ForEach` iterates them → the child receives `@item()` fields as parameters.
Adding a table becomes **a row insert, not a new pipeline**. Generic version: [Phase 10](../10-adf/).

### 3. Failure handling and alerting

- Activities have **success / failure / completion / skipped** dependency paths. Failure paths wrote the error to
  the audit error table and called an **alert** (a Web activity posting to a webhook — e.g. a Logic App or a chat
  channel — or an email via a DQ notebook).
- **Retries** are configured per activity (count + interval); they suit transient errors (throttling, network),
  not logic bugs.
- **Trap — "the pipeline succeeded but the activity failed":** if a failure path is handled (e.g. the error-logging
  activity succeeds), ADF can report the *pipeline* as succeeded. The fix is an explicit **Fail activity** at the end
  of the failure branch so the run status is honest and the master stops.

### 4. Parallel notebook fan-out

Two levels of parallelism were used:

1. **ADF ForEach** with `isSequential = false` and a `batchCount` (max 50) — each iteration a separate Databricks
   activity run.
2. **Inside Databricks**, a driver notebook launching child notebooks in threads:

```python
from concurrent.futures import ThreadPoolExecutor
tables = ["bookings", "passengers", "flights"]
def run(t):
    return dbutils.notebook.run("./load_one_table", 3600, {"table": t, "env": env})   # strings only
with ThreadPoolExecutor(max_workers=4) as pool:
    results = list(pool.map(run, tables))
```

Pitfalls: all children share **one cluster** (driver memory and cores), errors must be collected rather than
swallowed, `dbutils.notebook.run` arguments and return values are **strings**, and parameters must be passed
**explicitly** — a child notebook does not see the parent's widgets (see [12.5](05-hms-to-uc-migration.md)).

### 5. Moving to Databricks Jobs with Asset Bundles

Newer pipelines were built as **Databricks Jobs** (Lakeflow Jobs) defined in **Declarative Automation Bundles / Asset
Bundles** (`databricks.yml` + resource YAML) and deployed by CI/CD. ADF stayed where it adds unique value (on-prem
connectivity, SFTP/SharePoint copies) and can trigger a Databricks job with the **Job activity**.

| Concern | ADF calling notebooks | Databricks Jobs |
|---|---|---|
| Task graph | Across ADF + notebook-to-notebook calls | One DAG; repair run re-runs only failed tasks |
| Compute | Per-activity cluster or a shared all-purpose cluster | Job clusters shared across tasks, serverless |
| Parameters | ADF expressions → widgets | Job parameters, task values, `for_each` task |
| Lineage / observability | Split between two UIs | System tables, run timeline, UC lineage |
| Deployment | ARM template export | Bundles (`databricks bundle deploy -t prod`) |

### 6. Misconceptions

1. **"Retries fix failures."** Only transient ones; a retried non-idempotent load duplicates data.
2. **"More parallelism is always faster."** Parallel children on one cluster compete; sources may throttle.
3. **"ADF and Jobs are competitors."** They compose: ADF for connectivity, Jobs for Spark DAGs.

---

## B. Practical — a metadata-driven fan-out in pure Databricks

```python
# Databricks: a config table drives which objects load, with errors collected per object
from concurrent.futures import ThreadPoolExecutor, as_completed
CATALOG, SCHEMA = "main", "de_prep"
spark.sql(f"""CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.orch_config AS SELECT * FROM VALUES
  (101, 'bookings',   'incremental', 1),
  (102, 'passengers', 'incremental', 1),
  (103, 'old_feed',   'full',        0)
  AS t(object_id, object_name, load_type, is_active)""")

cfg = [r.asDict() for r in spark.table(f"{CATALOG}.{SCHEMA}.orch_config").filter("is_active = 1").collect()]  # small control data: OK to collect

def load(obj):
    # in real life: dbutils.notebook.run("./child", 3600, {k: str(v) for k, v in obj.items()})
    if obj["object_name"] == "passengers":
        raise ValueError("simulated source timeout")
    return f"{obj['object_name']} loaded"

ok, failed = [], []
with ThreadPoolExecutor(max_workers=4) as pool:
    futures = {pool.submit(load, o): o for o in cfg}
    for f in as_completed(futures):
        o = futures[f]
        try:
            ok.append(f.result())
        except Exception as e:
            failed.append((o["object_name"], str(e)))
print("ok:", ok, "failed:", failed)
if failed:
    raise RuntimeError(f"{len(failed)} object(s) failed: {failed}")   # the 'Fail activity' equivalent
```

Expected: `ok: ['bookings loaded']`, `failed: [('passengers', 'simulated source timeout')]`, then the cell fails —
the orchestrator sees an honest failure, but `bookings` was still loaded.

A minimal bundle job equivalent (YAML, illustrative):

```yaml
resources:
  jobs:
    bookings_daily:
      name: bookings_daily
      parameters:
        - {name: env, default: dev}
      tasks:
        - task_key: load_objects
          for_each_task:
            inputs: '["bookings","passengers"]'
            concurrency: 4
            task:
              task_key: load_one
              notebook_task:
                notebook_path: ../src/load_one_table.py
                base_parameters: {table: "{{input}}"}
```

---

## C. Interview questions

### 🟢 Basic

**B1.** What is a metadata-driven pipeline?

<details><summary>Model answer</summary>

One generic pipeline whose behaviour is driven by rows in a config table — source object, load type, watermark
column, target. On our project ADF's Lookup read the active rows and ForEach ran a child pipeline per row, so
onboarding a new table was a config insert, not new pipeline code.
</details>

**B2.** Why have master, wrapper and child pipelines?

<details><summary>Model answer</summary>

Separation of concerns: the master owns cross-subject-area ordering and the schedule; a wrapper owns one subject
area's audit start/end, metadata lookup and curated build and can be re-run on its own; the child is a generic,
parameterised per-object load. It keeps each pipeline small and reusable.
</details>

**B3.** How were failures alerted?

<details><summary>Model answer</summary>

Failure dependency paths logged the error to the audit error table and called a webhook/email alert; DQ
notebooks sent email alerts for data issues. We also made sure the branch ended in a Fail activity so the run
status was really "failed" and downstream pipelines didn't continue.
</details>

### 🟡 Medium

**M1.** A ForEach runs 40 children in parallel and some fail with throttling errors. What do you do?

<details><summary>Model answer</summary>

Lower `batchCount`, add activity retries with an interval for the transient errors, group objects by source so
one source isn't hit 40 times at once, and make sure each child is idempotent so retries are safe. Check whether
the Databricks side is the bottleneck too — many notebook activities on one cluster.
</details>

**M2.** Why can an ADF pipeline show "Succeeded" when an activity inside it failed?

<details><summary>Model answer</summary>

Pipeline status is evaluated from the leaf activities. If a failed activity has an "on failure" path that
succeeds (e.g. logging the error), the pipeline can be marked successful. Add a Fail activity at the end of the
error branch, or design the dependencies so failure propagates.
</details>

**M3.** Why move orchestration from ADF-calls-notebooks to Databricks Jobs?

<details><summary>Model answer</summary>

One DAG with repair runs, job clusters shared across tasks instead of spin-up per activity, task values and
`for_each` instead of notebook-to-notebook calls, better observability in one place, and deployment as code via
bundles with dev/prod targets. ADF can still trigger the job with the Job activity when it needs to sit inside
an existing ADF flow.
</details>

### 🔴 Tricky

**T1.** A driver notebook uses `dbutils.notebook.run` in a thread pool of 16 and the cluster becomes unstable. Why?

<details><summary>Model answer</summary>

Every child is an ephemeral notebook run on the **same** cluster: they share the driver's memory and the
executors. Sixteen children each collecting data or creating many DataFrames can OOM the driver; scheduler
contention slows everything. Limit concurrency, keep children lightweight, or split into separate job tasks
with their own (or shared job) compute.
</details>

**T2.** A child pipeline is retried after it failed half-way through the notebook. What could go wrong, and how did your framework handle it?

<details><summary>Model answer</summary>

If the notebook appends, the retry duplicates the rows that were written before the failure. Our audit framework
tagged rows with the run and deleted that run's data before reloading — which is idempotent but not atomic.
The better pattern is a single atomic write: `replaceWhere` on the run's slice or a MERGE on the key.
</details>

**T3.** When would you **not** move an ADF pipeline to Databricks Jobs?

<details><summary>Model answer</summary>

When the work is pure data movement from sources ADF connects to and Databricks can't easily reach (on-prem via
self-hosted IR, some SaaS connectors), when the pipeline is stable and cheap and the migration risk outweighs the
benefit, or when another team owns the ADF estate. Then ADF stays and triggers the Databricks job.
</details>

---

## E. Revision summary

- **Master** (order, schedule) → **wrapper** (subject area, audit, lookup, fan-out) → **child** (one object).
- **Metadata-driven:** Lookup config → ForEach → parameterised child. New table = new row.
- Failures: failure paths log + alert; end with **Fail activity**; retries only for transient errors.
- Fan-out: ForEach `batchCount` (≤ 50) or thread pool + `dbutils.notebook.run` (shared cluster, string params, explicit params).
- Direction of travel: **Databricks Jobs in bundles**, ADF for connectivity, ADF **Job activity** to trigger jobs.