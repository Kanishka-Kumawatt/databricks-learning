# 10.5 Dependency Conditions, Failure Handling, Monitoring and Troubleshooting

**Time:** 12 min · **Prerequisites:** 10.3, 1.11, 1.12

**After this topic you can:**
- use the four dependency conditions and combine them (AND across activities, OR within one dependency);
- predict whether a pipeline run shows Succeeded or Failed (the documented leaf-activity rule);
- implement Try-Catch, Do-If-Else, generic error handler + Fail, best-effort steps;
- set activity retries/timeouts, monitor and rerun runs, alert via Azure Monitor;
- troubleshoot the common error families (IR connectivity, auth, timeout, schema mapping).

---

## A. Concept deep dive

### 1. The simplest explanation

Each arrow between two activities carries a **condition**: "run B if A **Succeeded** / **Failed** / **Completed** (either) / was **Skipped**". With these four you build normal flow, catch blocks, cleanup steps and alerting.

### 2. The four conditions

| Condition (JSON) | UI name | B runs when A… |
|---|---|---|
| `Succeeded` | Upon Success (default) | succeeded |
| `Failed` | Upon Failure | failed |
| `Completed` | Upon Completion | succeeded **or** failed |
| `Skipped` | Upon Skip | never ran (an upstream dependency wasn't met) |

Combining:
- **Several conditions in one dependency = OR**: `{"activity": "A", "dependencyConditions": ["Failed", "Skipped"]}` → run if A failed *or* was skipped.
- **Several dependencies (arrows into B) = AND**: B waits until *every* listed dependency is satisfied.
- From one activity, *Upon Completion* can't coexist with *Upon Success* or *Upon Failure* paths.
- For "OR across different activities", connect them with *Upon Completion* into an **If Condition** that checks `@activity('X').Status`.

### 3. How ADF decides the pipeline status (the documented rule)

> Evaluate all **leaf** activities (activities with nothing after them). If a leaf was **skipped**, evaluate its **parent** instead. The pipeline **succeeds if and only if all evaluated nodes succeed**.

Consequences — memorize this table:

| Pattern | Defined paths | Main activity succeeds → pipeline | Main activity fails → pipeline |
|---|---|---|---|
| **Try-Catch** | only *Upon Failure* → handler | Succeeded | **Succeeded** (handler succeeded — failure is "handled") |
| **Do-If-Else** | *Upon Success* → next, *Upon Failure* → handler | Succeeded | **Failed** (success branch skipped → its parent failed) |
| **Do-If-Skip-Else** | Do-If-Else + dummy *Upon Skip* after the success branch | Succeeded | Succeeded |

The trap: **Try-Catch hides failures** — a pipeline with a failure-only email step shows green even when the load failed. If you want "alert *and* show Failed", end the catch path with a **Fail** activity.

### 4. Failure-handling patterns

```
 (a) Try-Catch                     (b) Generic handler + Fail (recommended for loads)
  A ──Failed──▶ LogError            A ─▶ B ─▶ C (last step)
                                               │ Failed or Skipped
                                               ▼
                                          SendAlert ──Succeeded──▶ Fail("load failed")
  (c) Best effort                   (d) Try-Catch-Proceed
  A ──Completed──▶ B                 A ──Failed──▶ Cleanup
  (B runs whatever A did)            Cleanup ──Failed/Skipped──▶ Next   (Next runs whether A worked or not)
```

Pattern (b) works because: if *any* earlier step fails, every step after it is **skipped**, so the last step is either Failed or Skipped → the handler runs once. On success the handler and Fail are skipped → evaluated parent (C) succeeded → run is green.

### 5. Retries and timeouts (activity `policy`)

| Property | Default | Notes |
|---|---|---|
| `timeout` | **12 hours** (minimum 10 minutes) — older definitions often show `7.00:00:00` | Set realistic values so a hung step fails fast |
| `retry` | 0 | Only for **transient** errors, and only if the activity is idempotent (1.12) |
| `retryIntervalInSeconds` | 30 | Give throttled sources time to recover |
| `secureInput` / `secureOutput` | false | Hide secrets/tokens from monitoring logs |

Other levels: pipeline `concurrency` (queue extra runs), tumbling window `retryPolicy` (re-runs the whole pipeline for that window), Web activity `httpRequestTimeout` (default 1 min).

### 6. Monitoring and rerun

| Need | Where / how |
|---|---|
| See pipeline runs | Monitor hub → Pipeline runs (status: Succeeded, Failed, In progress, Queued, Canceled) |
| See each step | Click a run → **activity runs**: input JSON, output JSON, error, IR used, duration; Databricks activities show a link to the Databricks run page |
| Rerun | **Rerun** (whole pipeline), **Rerun from activity** (choose a starting activity), **Rerun from failed activity**; reruns are grouped under the original run; rerun with new parameters = new run |
| Alerts | Monitor → **Alerts & metrics** (Azure Monitor): e.g. *Failed pipeline runs* metric filtered by pipeline name → action group (email/SMS/Teams webhook) |
| Long-term history & KQL | **Diagnostic settings** → Log Analytics (tables such as `ADFPipelineRun`, `ADFActivityRun`, `ADFTriggerRun`); ADF's own monitoring history is limited (about 45 days) |
| In-pipeline alerting | Failure path → Web activity to a **Logic App** / Teams / email API, with run ID and pipeline name |

Azure Monitor alerts catch **everything** (even failures before your error path exists, e.g. trigger problems); in-pipeline alerts carry **business context**. Use both for important pipelines.

### 7. Troubleshooting — error families

| Symptom / error | Likely cause | Fix |
|---|---|---|
| "Cannot connect to SQL Server", timeouts on test connection, SHIR shows *Limited/Offline* | SHIR down, firewall/DNS from SHIR to source, port 1433 blocked, Azure SQL firewall | Check SHIR node status & logs; test from the SHIR machine; add a second node; allow the IR/PE in the firewall |
| `403 AuthorizationPermissionMismatch` (ADLS), "Login failed for user '<token-identified principal>'" (Azure SQL) | Managed identity lacks data-plane role or DB user | Grant *Storage Blob Data Contributor* / ACLs; `CREATE USER [<factory-name>] FROM EXTERNAL PROVIDER` + roles |
| Key Vault "secret not found / forbidden" | MI lacks Get/List, wrong secret name, KV firewall | Fix access policy/RBAC; allow trusted services/private endpoint |
| Activity "timed out" | `policy.timeout` too low, hung source query, Web activity 1-min default | Raise timeout realistically, fix the query, use async pattern/polling |
| "Column X not found", type conversion errors, `UserErrorInvalidDataValue` | Schema drift at source; explicit mapping out of date; strict sink types | Refresh/remove explicit mapping, land raw Parquet, handle drift in Databricks (schema evolution, rescued data) |
| Databricks activity failed | Notebook exception, cluster start failure (quota, policy), invalid token/permissions | Open the run page link from activity output; check cluster event log; fix MI/PAT permissions |
| Lookup "exceeds 4 MB" / only 5,000 rows | Lookup used for data | Use Copy or a two-level pipeline |

### 8. ShopCo example

The nightly orders pipeline (10.8) uses pattern (b): any failure → `SendFailureAlert` (Web → Logic App → Teams channel) → `FailPipeline` so the run is **red** in monitoring and Azure Monitor's *Failed pipeline runs* alert also fires for the on-call engineer. Copy has `retry: 2`; the Databricks notebook has `retry: 1` (it's idempotent: replaceWhere + MERGE).

### 9. Misconceptions

1. **"An Upon Failure email means the pipeline will show Failed."** Not with Try-Catch — it shows Succeeded. Add a Fail activity.
2. **"Upon Failure on the last activity catches everything."** If an *earlier* activity fails, the last one is **Skipped**, not Failed — include `Skipped`.
3. **"Retries fix failures."** They fix *transient* failures; deterministic errors just fail N times slower — and non-idempotent retries duplicate data.
4. **"Default timeout is fine."** 12 hours of a hung step can block the next day's run.

---

## B. Practical — failure-handling patterns

**File:** [`exercises/phase-10/05_failure_handling_patterns.json`](../../exercises/phase-10/05_failure_handling_patterns.json) (study bundle, abbreviated): Try-Catch, Do-If-Else and Generic handler + Fail. Predict each run's status before reading the comments.

The generic handler, explained:

```json
{
  "name": "SendFailureAlert",
  "type": "WebActivity",
  "dependsOn": [ { "activity": "BuildSilver", "dependencyConditions": [ "Failed", "Skipped" ] } ],
  "typeProperties": {
    "method": "POST",
    "url": { "value": "@pipeline().parameters.alertUrl", "type": "Expression" },
    "headers": { "Content-Type": "application/json" },
    "body": { "value": "@json(concat('{\"pipeline\":\"', pipeline().Pipeline, '\",\"runId\":\"', pipeline().RunId, '\"}'))", "type": "Expression" }
  }
},
{
  "name": "FailPipeline",
  "type": "Fail",
  "dependsOn": [ { "activity": "SendFailureAlert", "dependencyConditions": [ "Succeeded" ] } ],
  "typeProperties": { "message": "Orders load failed - see activity runs", "errorCode": "ORDERS_LOAD_FAILED" }
}
```

| Part | Explanation |
|---|---|
| `["Failed", "Skipped"]` on the **last** step | OR: fires whether the last step failed or never ran because something earlier failed |
| `body` built with `concat` + `json` | ADF expressions don't support inline JSON objects with embedded expressions directly; build a string and convert |
| `FailPipeline` after the alert | Keeps the run status **Failed** (leaf Fail failed) so dashboards, Azure Monitor alerts and reruns behave correctly |
| On success | `SendFailureAlert` and `FailPipeline` are skipped → evaluated parent `BuildSilver` succeeded → run **Succeeded** |

### Common mistakes

- Alert attached with `Failed` to *one* middle activity only — failures elsewhere aren't caught.
- Using `Completed` for a cleanup step *and* a separate `Succeeded` path from the same activity (not allowed).
- Logging secrets: Web activity calling an API with a key in the URL, without `secureInput`.
- Retrying a non-idempotent step (append) → duplicates.

---

## C. Interview questions

### 🟢 Basic

**B1.** What are the four dependency conditions?

<details><summary>Model answer</summary>

Succeeded (Upon Success), Failed (Upon Failure), Completed (Upon Completion — either success or failure), Skipped (Upon Skip — the activity never ran because its own dependency wasn't met).
</details>

**B2.** What do `retry`, `retryIntervalInSeconds` and `timeout` in an activity policy do, and what are the defaults?

<details><summary>Model answer</summary>

`retry` = number of automatic retries (default 0); `retryIntervalInSeconds` = delay between attempts (default 30); `timeout` = maximum activity run time (default 12 hours, minimum 10 minutes). Use retries only for transient failures on idempotent steps; set realistic timeouts.
</details>

**B3.** Name three ways to re-run work after a failure in ADF.

<details><summary>Model answer</summary>

Rerun the whole pipeline; rerun from a chosen activity; rerun from the failed activity (all from the monitoring view, grouped with the original run). Also: rerun a failed tumbling window, or start a new run with new parameters (a separate run).
</details>

### 🟡 Medium

**M1.** A pipeline has `Copy → Notebook` and an email step on `Notebook` *Upon Failure* only. The Copy fails. Does the email go out? What's the run status?

<details><summary>Model answer</summary>

No email: Copy failed → Notebook skipped → the email step's condition (Notebook *Failed*) isn't met, so it's skipped too. Status: leaves are the email step (skipped → evaluate parent Notebook, skipped → evaluate Copy, failed) → pipeline Failed. Fix: connect the alert to the last activity with `["Failed", "Skipped"]` (generic handler).
</details>

**M2.** Explain the documented rule for pipeline success, and apply it to Try-Catch.

<details><summary>Model answer</summary>

ADF evaluates leaf activities; for a skipped leaf it evaluates the parent; the run succeeds only if all evaluated nodes succeeded. Try-Catch (only an Upon Failure handler): if the main activity fails and the handler succeeds, the handler is the evaluated leaf and succeeded → pipeline Succeeded. If the main activity succeeds, the handler is skipped → parent succeeded → Succeeded. So Try-Catch always shows green when the handler works.
</details>

**M3.** Azure Monitor alert or Web-activity alert in the pipeline — which and why?

<details><summary>Model answer</summary>

Both for critical pipelines. Azure Monitor metric/log alerts (failed runs, trigger failures, SHIR health) are central, catch failures the pipeline logic can't (trigger didn't fire, pipeline definition errors), and route to action groups. In-pipeline Web/Logic App alerts add business context (which table, which watermark window, row counts). With Log Analytics you also keep history beyond ADF's retention and can query with KQL.
</details>

### 🔴 Tricky

**T1.** The failure email works, but the on-call never sees red runs and Azure Monitor "failed runs" alerts never fire. Why?

<details><summary>Model answer</summary>

The pipeline uses Try-Catch: the failure is "handled" by a successful email activity, so the run is Succeeded and no failed-run metric is emitted. Add a Fail activity after the alert (or use Do-If-Else) so the run ends Failed.
</details>

**T2.** You need: run `Merge` only if *both* `CopyOrders` and `CopyPayments` succeed; otherwise run `LogFailure` once. Design it.

<details><summary>Model answer</summary>

Option 1: `Merge` depends on both with `Succeeded` (AND). For the failure path, `LogFailure` can't depend on "A failed OR B failed" with plain arrows (that would be AND). Connect both copies with `Completed` into an If Condition: `@and(equals(activity('CopyOrders').Status,'Succeeded'), equals(activity('CopyPayments').Status,'Succeeded'))` → true branch: Merge; false branch: LogFailure + Fail. Option 2: generic handler on the last step with `["Failed","Skipped"]`.
</details>

**T3.** A Copy activity has `retry: 3`, writes with `append` to a fixed folder, and the sink file naming isn't run-specific. After a network blip, Databricks counts are 2× for that day. Explain and fix.

<details><summary>Model answer</summary>

The first attempt wrote part of the files before failing; retries wrote the data again into the same folder with different file names; Databricks read all files → duplicates. Fix: run-specific (or attempt-safe) landing folders, clear the target folder before copy (Delete activity / overwrite semantics), and make downstream idempotent (dedupe + MERGE, or replaceWhere by load ID). Retries are only safe when the step is idempotent.
</details>

---

## E. Revision summary

- Conditions: **Succeeded, Failed, Completed, Skipped**. Conditions inside one dependency = **OR**; multiple dependencies = **AND**.
- Status rule: evaluate **leaves**; skipped leaf → evaluate parent; success **iff** all evaluated nodes succeed.
- Try-Catch → green even on failure; Do-If-Else → red on failure; generic handler on last step with `["Failed","Skipped"]` + **Fail** activity → alert and red.
- Policy: `timeout` default 12 h, `retry` default 0, `retryIntervalInSeconds` 30; retries only for transient + idempotent.
- Monitor: activity runs (input/output/error), rerun (all / from activity / from failed), Azure Monitor alerts, Log Analytics for history + KQL.
- Error families: IR connectivity, auth (MI roles, Key Vault), timeout, schema mapping, Databricks cluster/notebook.