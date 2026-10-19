# 11.0 The Diagnosis Framework

**Time:** 20 min (read first, re-read before every scenario) · **Prerequisites:** Phases 2, 3, 6 (Spark execution, Spark UI, Delta log)

**After this topic you can:**
- apply **symptom → evidence → root cause → fix → verify** to any production problem;
- name the exact place on Databricks where each kind of evidence lives;
- query the `system.lakeflow` and `system.billing` system tables for run history and cost;
- answer "what would you do if…" interview questions in a structured, senior-sounding way.

---

## A. Concept deep dive

### 1. Why a framework?

Under pressure, most people jump straight to a fix ("add more nodes", "increase memory", "repartition to 1000"). Sometimes it works; often it hides the real problem, costs money, and comes back next week. Interviewers know this. What they test in production questions is **whether you investigate before you act**.

The framework:

| Step | Question you answer | Output |
|---|---|---|
| **1. Symptom** | What exactly is observed, since when, how big, how often? | "Job `daily_revenue` went from 20 min to 3 h starting Tuesday; only task `build_silver`; every run since." |
| **2. Evidence** | Where do I look, and what do I see? | "Stage 14 has 200 tasks; median 4 s, max 2.6 h; one task reads 38 GB shuffle; others 50 MB." |
| **3. Root cause** | *Why* does the evidence look like this — mechanism, not label | "60 % of Tuesday's orders belong to one marketplace customer → one shuffle partition → one task." |
| **4. Fix** | Options ranked by preference (cheapest, safest, most targeted first) | "Broadcast the dimension (8 MB) → no shuffle of the fact side. Fallback: AQE skew join, then salting." |
| **5. Verify** | How do I prove it worked and won't regress? | "Re-ran: 14 min; max task 40 s. Added duration health rule at 30 min. Data unchanged (row count + sum reconciled)." |

Two extra habits make it production-grade:
- **Scope first** — one job or all jobs? One task or the whole run? Since a specific date or gradual? A sudden change almost always has a *change* behind it (code deploy, data volume/shape, runtime upgrade, cluster config, upstream schema). Ask "**what changed?**"
- **Protect the data before the speed** — if the problem may have written wrong data, first stop it spreading (pause downstream, mark the partition bad), then fix performance.

### 2. Classify the symptom first

Almost every production issue falls in one of six buckets. The bucket tells you where to look.

| Bucket | Typical symptom | First evidence to open |
|---|---|---|
| **Slow** | Duration grew; SLA missed | Run page task durations → Spark UI stages → task summary (max vs median) |
| **Failed (resource)** | OOM, executor lost, driver restarted | Task error message → driver log / executor log → Spark UI executors tab |
| **Failed (logic/data)** | AnalysisException, schema mismatch, null violation, MERGE duplicate | Error class in task output → `DESCRIBE HISTORY` / schema → source files |
| **Failed (concurrency/infra)** | ConcurrentAppendException, throttling, cloud capacity, permission denied | Error message → other jobs writing the same table → `DESCRIBE HISTORY` timestamps |
| **Wrong data, green job** | Duplicates, missing rows, totals don't match | Reconciliation queries → `DESCRIBE HISTORY` (what wrote, when, metrics) → time travel diff |
| **Cost** | DBUs spiked | `system.billing.usage` by job/cluster → cluster size/autoscaling → run durations |

### 3. Where evidence lives on Databricks

| Evidence | Where | What it tells you |
|---|---|---|
| **Job run page** (Lakeflow Jobs UI) | Jobs & Pipelines → job → Runs; matrix view across runs | Which task failed/slowed, duration per task across runs (spot the trend), retries, parameters used, the cluster used |
| **Task output / error** | Click the task in the run | The exception and error class (e.g. `DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE`), notebook cell that failed |
| **Driver logs** | Cluster (or job compute) → Driver logs: **stdout**, **stderr**, **log4j** | stdout = your `print`s; stderr = Python tracebacks, warnings; log4j = Spark/JVM logs (OOM, GC, "executor lost", scheduler messages) |
| **Executor logs** | Spark UI → Executors tab → stdout/stderr links per executor | Errors inside tasks: OOM on an executor, Python worker crashes, UDF exceptions |
| **Spark UI — Jobs/Stages** | Run → task → Spark UI (or cluster → Spark UI) | Which stage is slow; number of tasks; **task summary metrics (min/25th/median/75th/max)**: duration, input, shuffle read/write, **spill (memory/disk)**, GC time |
| **Spark UI — SQL / DataFrame tab** | Spark UI → SQL | The physical plan as a graph with row counts per operator: join strategy (`SortMergeJoin` vs `BroadcastHashJoin`), files read / pruned, AQE changes (`isSkew`, coalesced shuffle reads) |
| **Spark UI — Executors** | Spark UI → Executors | Memory used, GC time, failed tasks, dead executors |
| **Query profile** | SQL warehouse / serverless query history → query → profile | Operator-level time, rows, bytes, spill, pruning — same idea as SQL tab, for SQL warehouses and serverless |
| **`df.explain("formatted")`** | Notebook | The plan before running: join type, pushed filters, partition filters |
| **`DESCRIBE HISTORY t`** | SQL | Every commit: operation, user, time, `operationParameters` (e.g. merge predicate), `operationMetrics` (rows/files added/removed, `numTargetRowsCopied`, execution time) |
| **`DESCRIBE DETAIL t`** | SQL | `numFiles`, `sizeInBytes`, partition / clustering columns, table properties → small-file diagnosis |
| **Streaming query progress** | Notebook stream UI, `query.lastProgress`, Spark UI → Structured Streaming tab | Input rows/sec vs processed rows/sec, batch duration, backlog, state size |
| **Pipeline event log** (Lakeflow Declarative Pipelines) | Pipeline UI, or the event log TVF / published event log table | Flow progress, expectation pass/fail counts, errors per update (Phase 8) |
| **System tables — jobs** | `system.lakeflow.jobs`, `job_tasks`, `job_run_timeline`, `job_task_run_timeline` | Run history across all jobs in the account: result state, start/end, termination code — trend analysis and alerting |
| **System tables — billing** | `system.billing.usage` (+ `system.billing.list_prices`) | DBUs by `usage_metadata.job_id`, `sku_name`, date → cost per job |
| **Audit** | `system.access.audit` (verify it is enabled in your account) | Who did what: permission changes, table access, job edits |
| **Lineage** | Catalog Explorer → table → Lineage; `system.access.table_lineage` (verify) | Which upstream jobs/tables feed this one → who changed what |

System table facts (verified, Databricks docs — names may evolve, so "verify for your account"):
- `system.lakeflow.job_run_timeline` columns include `workspace_id`, `job_id`, `run_id`, `period_start_time`, `period_end_time`, `result_state` (`SUCCEEDED`, `FAILED`, `SKIPPED`, `CANCELLED`, `TIMED_OUT`, `ERROR`, `BLOCKED`), `termination_code`, `run_type`. Long runs are split into **hourly rows**, so aggregate by `run_id` (min start, max end; the final row carries `result_state`).
- `system.billing.usage` has `usage_date`, `sku_name`, `usage_quantity` (DBUs), `billing_origin_product`, `usage_metadata.job_id`, `usage_start_time`/`usage_end_time`. `system.billing.list_prices` has `sku_name`, `price_start_time`, `price_end_time`, `pricing.default`, `currency_code`.
- System tables must be enabled by an account admin and you need SELECT on them.

```sql
-- Runs that failed in the last 7 days, with duration
WITH runs AS (
  SELECT workspace_id, job_id, run_id,
         MIN(period_start_time) AS started,
         MAX(period_end_time)   AS ended,
         MAX_BY(result_state, period_end_time) AS result_state
  FROM system.lakeflow.job_run_timeline
  WHERE period_start_time >= current_date() - INTERVAL 7 DAYS
  GROUP BY ALL
)
SELECT r.*, timestampdiff(MINUTE, started, ended) AS minutes, j.name
FROM runs r
LEFT JOIN system.lakeflow.jobs j USING (workspace_id, job_id)   -- jobs is SCD2-like: may have several rows per job; take latest in real use
WHERE result_state IN ('FAILED', 'ERROR', 'TIMED_OUT')
ORDER BY started DESC;

-- DBUs per job per day (multiply by list price for money)
SELECT usage_date, usage_metadata.job_id, sku_name, SUM(usage_quantity) AS dbus
FROM system.billing.usage
WHERE usage_metadata.job_id IS NOT NULL AND usage_date >= current_date() - INTERVAL 30 DAYS
GROUP BY ALL
ORDER BY dbus DESC;
```

### 4. Reading the Spark UI in 60 seconds

1. **Jobs tab** → the job with the longest duration.
2. Click it → **stages** sorted by duration. Ignore fast stages.
3. Click the slow stage → **Summary Metrics for tasks**:
   - **max ≫ median** duration (e.g. 40 min vs 5 s) → **skew** (11.1) or one huge non-splittable file.
   - all tasks slow and similar → too little parallelism, or too much work per task (scan too much: missing pruning) → 11.1/11.5.
   - thousands of tasks of ~10 ms each → **too many tiny partitions/files** (11.2).
   - **Spill (Disk)** > 0 → partitions too large for execution memory (11.3).
   - high **GC time** (> ~10 % of task time) → memory pressure (11.3).
   - **Shuffle read** huge → a join/aggregation moves a lot of data; check join strategy.
4. **SQL tab** → find the query → check join operators, "number of files read" vs "files pruned", rows out of each operator (an exploding join shows rows ×1000 after the join).
5. **Executors tab** → dead executors, failed tasks, GC.

### 5. Root cause: mechanism, not label

"It's skew" is a label. A root cause explains the mechanism *and* why now:

> "The orders table started receiving marketplace orders on Tuesday; all of them carry `customer_id = 'MKT-0001'`. The join to customers is a sort-merge join on `customer_id`, so all those rows hash to one shuffle partition. That partition is 38 GB; one task processes it while 199 tasks finish in seconds. AQE skew handling didn't trigger because the job compute has it disabled in its Spark config."

That sentence gives you the fix options automatically.

### 6. Fixes — rank them

General order of preference:
1. **Remove unnecessary work** (filter early, prune partitions/files, select only needed columns, don't re-read).
2. **Fix the data layout** (compaction, clustering, partitioning) or the **plan** (broadcast, avoid exploding joins).
3. **Use the engine's adaptive features** (AQE, optimized writes, auto compaction, Photon).
4. **Manual techniques** (salting, repartitioning, splitting the job).
5. **Bigger / more compute** — last, and justified with evidence ("spill persists after fix; executor memory is the limit").

For failures, also choose between **retry**, **repair**, **fix forward** and **roll back** (11.6).

### 7. Verify — the step most people skip

- Re-run on the **same input** (same parameters) and compare duration/stage metrics with the evidence you collected.
- **Reconcile the data** (counts, sums, keys) — a "fix" that drops rows looks fast.
- Check `DESCRIBE HISTORY` for the expected operation and metrics.
- Add a **guard** so you learn next time before users do: duration health rule, row-count/freshness check, DQ threshold, alert.
- Write a short **post-incident note**: symptom, cause, fix, prevention.

### 8. Answering production questions in interviews — template

> "**First I'd clarify the symptom**: … (scope: which task, since when, all runs or intermittent, what changed).
> **Then gather evidence**: I'd open … (run page → Spark UI stage → task summary / `DESCRIBE HISTORY` / driver log) looking for …
> **Likely root causes**, in order of probability: (1) … (2) … — the evidence that distinguishes them is …
> **Fix**: preferred … because …; alternatives … with trade-off …
> **Verify**: re-run with the same parameters, compare …, reconcile data, and add … (alert/test) so it doesn't recur."

Worked example (2-minute answer): *"A job that took 20 min now takes 3 h."*

> "First I'd scope it: is it the whole job or one task, and since when — gradual growth suggests data volume or small files accumulating; a step change suggests a code, data-shape, cluster or runtime change. I'd compare the job's run matrix and the last good run's parameters and cluster.
> Then evidence: on the slow task's Spark UI I'd find the slowest stage and compare max vs median task duration, shuffle read, spill and input size. On the SQL tab I'd check the join strategy and files read versus pruned. For Delta targets I'd check `DESCRIBE DETAIL` for the number of files and `DESCRIBE HISTORY` for MERGE metrics.
> Most likely causes: skew (one task dominates), a lost broadcast (dimension grew past the threshold → sort-merge join of the big side), loss of pruning (a MERGE or filter now scans the whole table), small-file explosion, spill from more data per partition, or a smaller/cheaper cluster.
> I'd fix the specific cause — e.g. broadcast or AQE skew join for skew, add a partition/cluster predicate to the MERGE, compact files — rather than just adding nodes.
> Then verify by re-running the same date, reconciling output, and adding a duration health rule and a volume check so we're alerted before the SLA."

### 9. Misconceptions

1. **"Add more nodes."** Doesn't help skew (one task is still one core), driver OOM, small files overhead, or a missing filter. It only helps evenly parallel work.
2. **"Increase `spark.sql.shuffle.partitions` fixes skew."** A hot key still hashes to one partition, however many partitions there are.
3. **"The job succeeded, so the data is right."** Verify with reconciliation (11.7).
4. **"Retry until it works."** Retries help transient failures; deterministic failures (bugs, schema, permissions, bad data) fail again and cost money (11.6).
5. **"I'll look at the error at the bottom of the stack trace."** Find the *first* error (often a `Caused by:` in the middle, or the first failed task); later errors are consequences.

---

## B. Practical — build your evidence checklist

No notebook here — every later lab uses this checklist. For each lab, fill this table in your notes **before** reading the explanation:

| Step | What I saw | Where |
|---|---|---|
| Symptom | | |
| Evidence 1 | | |
| Evidence 2 | | |
| Root cause (mechanism) | | |
| Fix chosen + why | | |
| Verification | | |

Try the two system-table queries above in your workspace (they are read-only). If you get `TABLE_OR_VIEW_NOT_FOUND`, the system schema isn't enabled for you — say so in an interview: "we'd ask the account admin to enable `system.lakeflow` and `system.billing`".

---

## C. Interview questions

### 🟢 Basic

**B1.** Name the five steps you use to troubleshoot a production pipeline problem.

<details><summary>Model answer</summary>

Symptom (precise and scoped: what, since when, how often), evidence (run page, logs, Spark UI, Delta history, system tables), root cause (the mechanism, and what changed), fix (ranked options, most targeted first), verify (re-run with the same parameters, reconcile the data, add an alert/test to prevent recurrence).
</details>

**B2.** Where do you find (a) a Python traceback, (b) a JVM OutOfMemoryError from the driver, (c) why one stage was slow?

<details><summary>Model answer</summary>

(a) Task output in the job run page and the driver's **stderr**. (b) Driver **log4j** log (and stderr); the run page usually also shows "driver … stopped unexpectedly / restarting". (c) Spark UI → Stages → that stage's task summary metrics (max vs median duration, shuffle read, spill, GC) and the SQL tab for the plan.
</details>

**B3.** Which system tables would you query to see every failed job run in the last week, and to see what a job costs?

<details><summary>Model answer</summary>

`system.lakeflow.job_run_timeline` (aggregate per `run_id`, filter `result_state` = `FAILED`/`ERROR`/`TIMED_OUT`), joined to `system.lakeflow.jobs` for names. Cost: `system.billing.usage` grouped by `usage_metadata.job_id` and `sku_name`, multiplied by `system.billing.list_prices.pricing.default` for the matching SKU and time range.
</details>

### 🟡 Medium

**M1.** A job that took 20 minutes now takes 3 hours. Walk me through what you do.

<details><summary>Model answer</summary>

Symptom: scope it — which task, since when, step change vs gradual, every run or one. Ask what changed (deploy, data volume/shape, cluster, runtime). Evidence: run matrix to find the task; Spark UI → slowest stage → task summary (max ≫ median = skew; spill; many tiny tasks = small files), SQL tab (join type changed from broadcast to sort-merge? files pruned?), `DESCRIBE DETAIL` (file count), `DESCRIBE HISTORY` (MERGE metrics, files rewritten). Root cause with mechanism. Fix the specific cause (broadcast/AQE skew/salting, pruning predicate, compaction), not just bigger compute. Verify: re-run the same date, compare metrics, reconcile output, add a duration health rule.
</details>

**M2.** Why is "what changed?" usually the most useful question for a sudden regression?

<details><summary>Model answer</summary>

Systems that ran stably don't degrade suddenly without a cause. A step change points to a specific event: code deployment, runtime/cluster config change, upstream schema change, a new data pattern (new hot key, volume spike), table layout change (partitioning, lost clustering), permissions. Comparing the last good run with the first bad run (parameters, cluster, code version, input size, plan) narrows the search immediately. Gradual change points to growth: data volume, files accumulating, state growing.
</details>

**M3.** How do you verify a performance fix properly?

<details><summary>Model answer</summary>

Re-run with the same input and parameters (not a lighter day), compare the same metrics you used as evidence (stage time, max task time, spill, files read), confirm the data is identical (row counts, sums, key anti-join vs previous output or the source), check `DESCRIBE HISTORY` for the expected operation, then add monitoring (duration health rule, volume/freshness checks) and document it.
</details>

### 🔴 Tricky

**T1.** The team doubled the cluster size and the job got only 5 % faster. What does that tell you?

<details><summary>Model answer</summary>

The bottleneck isn't parallel capacity. Typical causes: one straggler task from skew or a non-splittable file (one core does the work no matter how many there are), driver-side work (collect, Python loops, listing millions of files, planning a huge query), small-file overhead/metadata operations, an external system (JDBC source, API) limiting throughput, or sequential steps. Evidence: Spark UI timeline (are executors idle?), task summary max vs median, driver CPU. Fix the bottleneck and probably shrink the cluster back to save cost.
</details>

**T2.** The job is green but the business says yesterday's revenue is 30 % low. How do you approach it?

<details><summary>Model answer</summary>

Treat it as a data incident. Symptom: which table/metric, which dates, how much. Evidence: reconcile layer by layer — source vs Bronze vs Silver vs Gold counts and sums for that date; quarantine counts; `DESCRIBE HISTORY` for each table (did the write happen, how many rows, which operation, was there an overwrite with a wrong `replaceWhere`?); check freshness (did all source files arrive?); inner join dropping orders with unknown customers? Root cause found at the layer where numbers diverge. Fix: correct logic or reload data, then backfill the affected dates idempotently (`replaceWhere`/MERGE). Verify with reconciliation; add a volume anomaly check and a source-to-target reconciliation task that fails the job.
</details>

**T3.** You can't reproduce an intermittent failure in dev. How do you make progress?

<details><summary>Model answer</summary>

Collect evidence across occurrences: `system.lakeflow.job_task_run_timeline` to see when it fails (time of day? concurrent with another job? only on some days?), error messages/classes per run, cluster events (spot loss, autoscaling), concurrent writers in `DESCRIBE HISTORY` around the failure time. Intermittent usually means environmental: concurrency conflicts, transient cloud/network errors, capacity, data-dependent (a specific day's bad record), or timing (upstream file late). Add structured logging and audit records, reproduce with the same input data/version (time travel), and decide: retry with backoff for transient, fix for deterministic.
</details>

---

## E. Revision summary

- **Symptom → evidence → root cause → fix → verify**; scope first and ask **what changed**.
- Buckets: slow · resource failure · logic/data failure · concurrency/infra · wrong data · cost.
- Evidence: run page/matrix, task output, driver stdout/stderr/log4j, Spark UI (stages, task summary, SQL tab, executors), query profile, `DESCRIBE HISTORY/DETAIL`, streaming progress, pipeline event log, `system.lakeflow.*`, `system.billing.usage`.
- Task summary patterns: max ≫ median = skew; many tiny tasks = small files; spill/GC = memory; huge shuffle = join strategy.
- Fix order: less work → better layout/plan → adaptive features → manual techniques → bigger compute.
- Verify with the same input, reconcile data, add an alert/test.