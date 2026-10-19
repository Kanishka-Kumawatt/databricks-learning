# 12.7 Compute — Job Clusters, Tags, Policies, Access Modes, and the All-Purpose Anti-Pattern

**Time:** 10 min · **Prerequisites:** Phase 5 (compute types, access modes, sizing & cost)

**After this topic you can:**
- explain why production runs on job clusters (or serverless), not all-purpose clusters;
- explain tags for cost attribution and cluster policies for guardrails;
- explain the choice of dedicated (single-user) job clusters during and after the UC migration;
- quantify the all-purpose reuse anti-pattern and propose a migration.

---

## A. Concept / story

```
                     ┌────────── cluster policy (per team) ──────────┐
 Job / ADF run ────► │ runtime = approved LTS, node types allow-list │ ──► job cluster created for the run
                     │ autoscale max, auto-termination, tags fixed   │      (tags: team, domain, cost centre)
                     └───────────────────────────────────────────────┘      terminated when the run ends
 Interactive dev ──► all-purpose cluster (shared by a team, auto-terminate)
 Anti-pattern   ───► production ADF activities pointed at an all-purpose cluster ID
```

| Choice | What we did | Why |
|---|---|---|
| **Job clusters per team** | Each team's jobs ran on job compute defined by a team policy | Isolation, right-sizing, cheaper job-compute pricing than all-purpose |
| **Tags** | Team / domain / cost-centre tags enforced by policy | Cost attribution in billing / `system.billing.usage` |
| **Policies** | Approved runtimes, node types, max workers, auto-termination | Guardrails without reviewing every config |
| **Dedicated (single-user) job clusters** | Job runs as one identity (an SP) | Full Spark API compatibility for legacy code while still UC-enabled |
| **Runtime upgrade** | Moved to a current LTS as part of the UC migration | UC features, security fixes, performance (Photon, AQE improvements) |
| **Some workloads on all-purpose** | Legacy jobs reusing interactive clusters | The anti-pattern below |

### The all-purpose reuse anti-pattern

| Problem | Effect |
|---|---|
| All-purpose compute is billed at a higher rate than job compute | Same work costs more |
| Cluster stays up between jobs (or never terminates) | Idle cost |
| Many jobs + users share one driver | Noisy neighbours, driver OOM, library conflicts |
| Restarting it to fix one job kills others | Low reliability |
| Config drift | "Works on that cluster" |

**Better:** job clusters per job (one shared job cluster across tasks of the same job to avoid per-task spin-up),
serverless jobs where supported, **instance pools** to cut start-up time if needed.

### Misconceptions

1. **"Job clusters are slow because of start-up."** A few minutes per job; shared job clusters across tasks, pools or serverless reduce it.
2. **"Bigger cluster = faster."** Only if the work parallelises; check the Spark UI for skew/spill first.

---

## B. Practical — a policy and a job cluster (illustrative JSON/YAML)

```json
{
  "spark_version": {"type": "allowlist", "values": ["<current-lts-runtime>"]},
  "node_type_id":  {"type": "allowlist", "values": ["<general-purpose-node>", "<memory-optimised-node>"]},
  "autoscale.max_workers": {"type": "range", "maxValue": 8},
  "custom_tags.team":   {"type": "fixed", "value": "bookings"},
  "data_security_mode": {"type": "fixed", "value": "SINGLE_USER"}
}
```

```yaml
job_clusters:
  - job_cluster_key: shared
    new_cluster:
      policy_id: ${var.team_policy_id}
      spark_version: <current-lts-runtime>
      autoscale: {min_workers: 1, max_workers: 4}
      custom_tags: {domain: bookings}
tasks:
  - {task_key: raw,     job_cluster_key: shared, notebook_task: {notebook_path: ../src/raw.py}}
  - {task_key: staging, job_cluster_key: shared, depends_on: [{task_key: raw}], notebook_task: {notebook_path: ../src/staging.py}}
```

Cost-attribution query (system tables):

```sql
SELECT custom_tags['team'] AS team, sku_name, sum(usage_quantity) AS dbus
FROM system.billing.usage
WHERE usage_date >= current_date() - 30
GROUP BY ALL ORDER BY dbus DESC;
```

---

## C. Interview questions

### 🟢 Basic

**B1.** Job cluster vs all-purpose cluster?

<details><summary>Model answer</summary>

A job cluster is created for a job run and terminated after; billed at the job-compute rate; isolated. An
all-purpose cluster is long-lived for interactive work, shared, billed higher. Production should run on job
clusters or serverless.
</details>

**B2.** What are cluster policies for?

<details><summary>Model answer</summary>

Rules that constrain cluster configuration — allowed runtimes and node types, max size, auto-termination, fixed
tags, access mode — so teams self-serve within guardrails and cost is attributable.
</details>

**B3.** Why tag clusters?

<details><summary>Model answer</summary>

Tags flow to the cloud bill and Databricks usage system tables, so cost can be attributed per team/domain and
anomalies spotted.
</details>

### 🟡 Medium

**M1.** Why single-user (dedicated) job clusters during the UC migration?

<details><summary>Model answer</summary>

They're UC-enabled and support the full Spark API, so legacy code using RDDs or certain libraries could run while
being rewritten; the job runs as one identity (an SP) with clear permissions. Standard mode was the target for
rewritten code.
</details>

**M2.** Ten tasks in one job each create their own job cluster. What's the issue and fix?

<details><summary>Model answer</summary>

Ten start-ups and ten sets of VMs — slow and costly for small tasks. Use a shared job cluster (`job_cluster_key`)
across tasks, or serverless; keep separate clusters only for tasks with very different sizing.
</details>

**M3.** How would you find which team drives cost?

<details><summary>Model answer</summary>

Query `system.billing.usage` grouped by custom tags (team/domain), SKU and job id; join to `system.lakeflow` job
tables for job names; look for all-purpose SKU usage by scheduled jobs.
</details>

### 🔴 Tricky

**T1.** Why might production jobs on an all-purpose cluster fail randomly?

<details><summary>Model answer</summary>

Shared driver memory and cores with other jobs and users, library conflicts from someone installing a package,
restarts by other users, auto-termination mid-run, or state left by a previous job (temp views, confs). Isolation
via job clusters removes all of these.
</details>

**T2.** Upgrading the runtime LTS — what can break?

<details><summary>Model answer</summary>

Spark behaviour changes (e.g. ANSI SQL mode, datetime parsing, removed deprecated APIs), library version
conflicts, Python version changes, and changed defaults. Mitigate with a parallel run on the new runtime and
compare outputs before switching.
</details>

**T3.** Serverless is available — why might a team not move immediately?

<details><summary>Model answer</summary>

Code that needs RDDs, DBFS paths, custom init scripts or specific libraries/confs; network setups requiring
specific connectivity; cost-model review. Making code UC-standard-compatible is the prerequisite — another
reason we fixed code rather than relying on dedicated mode.
</details>

---

## E. Revision summary

- Production = **job clusters** (shared across tasks) or **serverless**; all-purpose only for dev.
- **Policies** = guardrails (runtime, nodes, size, tags, access mode); **tags** = cost attribution.
- **Dedicated** job clusters for legacy compatibility; standard-compatible code unlocks serverless.
- Runtime upgrade to current LTS = parallel-run and compare.