# 5.4 "How Many Types of Clusters / Compute Are There in Databricks?"

**Time:** 30 min · **Prerequisites:** 5.3

**After this topic you can:**
- explain why this question has **no single correct number**, and answer it confidently anyway;
- classify any compute resource along four independent axes: **purpose/lifecycle**, **node layout**, **access mode**, and (historically) **cluster mode**;
- map every legacy name (High Concurrency, Standard mode, Shared, Single user, No isolation shared, Custom) to current terminology;
- state what each access mode implies for Unity Catalog, languages and APIs;
- give a crisp 60-second interview answer.

> ⚠️ **Verified against official docs on 2026-10-18** (see *Sources* at the end). Databricks changes names and defaults often. If an interviewer uses an older term, map it — don't argue.

---

## A. Concept deep dive

### 1. Why "how many types?" is a trick question

Old blog posts say "there are 2 types (interactive and job)" or "3 cluster modes (Standard, High Concurrency, Single Node)". Both were true at some point and **both are incomplete today**. The question mixes **different classification axes**:

| Axis | Question it answers | Values today |
|---|---|---|
| **A. Purpose / lifecycle** | *What is it for, who creates it, how long does it live?* | All-purpose · Jobs · Serverless (notebooks / jobs / pipelines) · Pipeline compute · SQL warehouses (serverless / pro / classic) — plus instance pools as a supporting resource |
| **B. Node layout** | *How many machines?* | Single node · Multi node |
| **C. Access mode** | *Who can share it and how is data access enforced?* | **Standard** (formerly Shared) · **Dedicated** (formerly Single user) · Auto (chooses one) · legacy No isolation shared |
| **D. Legacy cluster mode** | *What did the old UI call it?* | Standard · High Concurrency · Single Node — **replaced** by B + C |

Plus hardware/runtime choices that are *settings*, not types: Databricks Runtime (standard / ML), Photon on/off, CPU vs GPU, instance family, on-demand vs spot, pool-backed or not.

A good answer names the axes, then gives the values on each.

### 2. Axis A — compute types by purpose and lifecycle

```
                                  ┌────────────── COMPUTE IN A WORKSPACE ──────────────┐
             CLASSIC (VMs in your subscription)                     SERVERLESS (Databricks-managed)
   ┌──────────────────┬───────────────────┬────────────┐   ┌─────────────┬────────────┬─────────────┐
   │ All-purpose      │ Jobs compute       │ Pipeline   │   │ Notebooks   │ Jobs       │ Pipelines   │
   │ (interactive)    │ (per job run)      │ compute    │   │             │            │             │
   └──────────────────┴───────────────────┴────────────┘   └─────────────┴────────────┴─────────────┘
   SQL warehouses:   Classic  ·  Pro  (compute in your subscription)   |   Serverless (Databricks-managed)
   Supporting (not compute itself): instance pools (pre-warmed VMs) · compute policies (rules)
```

| # | Compute | Plane | Who creates it | Lifecycle | Typical use | Notes |
|---|---|---|---|---|---|---|
| 1 | **All-purpose compute** (interactive cluster) | Classic | Users (UI/API/CLI/Terraform), usually via a **policy** | Runs until manually terminated or **auto-terminated** after inactivity; can be restarted; config kept 30 days after termination unless pinned | Interactive development, exploration, notebooks shared by a team | Billed at the **All-Purpose** DBU rate (higher). Jobs *can* run on it, but that's not recommended for production |
| 2 | **Jobs compute** (job cluster) | Classic | The **job scheduler**, from the job's cluster definition | Created for the run, **terminated when the job ends**; cannot be restarted | Scheduled production ETL | Billed at the **Jobs** DBU rate (lower). Several tasks of one job can share one job cluster (`job_cluster_key`) |
| 3 | **Serverless compute for notebooks** | Serverless | Nobody — select **Serverless** in the notebook compute menu | On demand; Databricks manages scaling and idle shutdown | Interactive Python/SQL without waiting for VMs | Requires Unity Catalog + supported region. Python and SQL only (no Scala/R in notebooks) |
| 4 | **Serverless compute for jobs** | Serverless | Nobody — default compute for notebook, Python script, dbt, Python wheel and JAR tasks | Per run | Production jobs without cluster config | **Performance optimized** vs **standard** performance mode (standard = cheaper, ~4–6 min startup tolerance). No cluster-creation permission needed |
| 5 | **Pipeline compute** (Lakeflow Spark Declarative Pipelines, formerly DLT) | Classic **or** serverless | The pipeline itself | Per pipeline update (triggered) or continuous | Declarative streaming/batch pipelines | Classic pipelines use **enhanced autoscaling** by default; serverless pipelines always use it plus vertical autoscaling |
| 6 | **SQL warehouses** | Serverless, or Pro/Classic in your subscription | Admins or users with unrestricted cluster creation | Start on query, **auto stop** after idle (default 10 min serverless, 45 min pro/classic) | SQL editor, dashboards, BI tools (Power BI, Tableau), dbt, SQL tasks in jobs | SQL only. Photon always on. (Lakehouse Real-Time is a Beta serverless type for sub-second reads) |
| — | **Instance pools** | Classic | Users with *Allow pool creation* (admins by default) | Keep idle VMs warm | Faster start/autoscale for classic compute | **Not compute by itself** — a pool of ready VMs that clusters draw from. No DBUs while idle, but you pay the VM. Not used by serverless |

SQL warehouse types in detail:

| Type | Compute runs in | Photon | Predictive IO | Intelligent Workload Management | Startup |
|---|---|---|---|---|---|
| **Serverless** | Databricks' account (serverless plane) | ✅ | ✅ | ✅ | typically 2–6 s |
| **Pro** | Your subscription | ✅ | ✅ | ❌ | typically ~4 min |
| **Classic** | Your subscription | ✅ | ❌ | ❌ | typically ~4 min |

UI default is serverless where available; the **SQL Warehouses API default is classic** unless you set `enable_serverless_compute: true` with `warehouse_type: pro`.

### 3. Axis B — node layout

| | Single node | Multi node |
|---|---|---|
| Machines | Driver only (acts as master + worker) | 1 driver + N workers (fixed or autoscaling min–max) |
| Good for | Small data, pandas/ML on one machine, learning | Distributed ETL, big joins, high-volume streaming |
| Gotchas | Can't be converted to multi node; no GPU scheduling | Can't scale to 0 workers; 0 workers = Spark commands fail |

Applies to classic all-purpose and jobs compute. Serverless and SQL warehouses hide node layout from you (warehouses use T-shirt sizes: 2X-Small … 4X-Large, plus min/max *clusters* for concurrency).

### 4. Axis C — access modes (classic all-purpose & jobs compute)

Access mode = **who can use the compute and how data access is enforced.** Set under *Advanced* in the compute UI; `data_security_mode` in the API. The UI default is **Auto**.

| Current name | API value (`data_security_mode`) | Legacy name(s) | Who can attach | Unity Catalog | Languages | What it implies |
|---|---|---|---|---|---|---|
| **Standard** | `DATA_SECURITY_MODE_STANDARD` (legacy alias `USER_ISOLATION`) | **Shared** access mode; before that "High Concurrency + Table ACLs" | Many users, **isolated from each other** (Lakeguard) | ✅ | Python, SQL, Scala (no **R**) | **Recommended default.** No RDD APIs; no Databricks Runtime ML; no GPU; no spark-submit tasks; restricted Spark configs; Scala `sc`/`sqlContext` not supported; Python `sc` needs DBR 14.0+ |
| **Dedicated** | `DATA_SECURITY_MODE_DEDICATED` (legacy alias `SINGLE_USER`) | **Single user** access mode ("Assigned") | **One user**, or **one group** (group assignment: Public Preview) | ✅ | Python, SQL, Scala, **R** | Use for RDD APIs, GPU, ML runtime, R, privileged machine access. Fine-grained access control (row filters, column masks, dynamic views) needs **DBR 15.4 LTS+ and serverless enabled** (filtering is done on serverless) |
| **Auto** | `DATA_SECURITY_MODE_AUTO` | — | — | — | — | UI default: picks **Standard**, unless an ML runtime, a GPU instance type or DBR < 14.3 is selected → **Dedicated** |
| No isolation shared *(legacy)* | `NONE` | Legacy **Standard cluster mode**; **High Concurrency without** Table ACLs/passthrough | Many users, **no isolation** | ❌ | Python, SQL, Scala, R | Users' code shares one environment — credentials/data on the cluster may be visible to others. Admins can hide it ("enforce user isolation") |
| Table ACL only *(legacy)* | `LEGACY_TABLE_ACL` | HC + Table Access Control | Many users | ❌ (Hive metastore table ACLs only) | — | Legacy governance pre-UC |
| Credential passthrough *(legacy)* | `LEGACY_PASSTHROUGH` | HC/Standard + "Credential passthrough" | — | ❌ incompatible with UC | — | Legacy Azure AD passthrough; replaced by UC external locations |
| *Custom* (UI label only) | — | Clusters created with legacy modes | — | ❌ | — | Shown for old cluster definitions without an access mode; can't be selected for new compute |

Other notes:
- **Serverless** compute (notebooks/jobs) runs with **standard access mode** semantics — your code must be compatible with standard mode limitations (plus serverless-specific ones: no R, no RDDs, no Spark UI, limited Spark confs, no `df.cache()`).
- **SQL warehouses** have no access-mode setting; Unity Catalog is enforced.
- Only **Standard** and **Dedicated** work with Unity Catalog. If your project still has "No isolation shared" clusters, they cannot read UC tables.

### 5. Axis D — legacy cluster modes and what replaced them

The old create-cluster form had a **Cluster mode** dropdown with three values:

| Legacy cluster mode | What it meant | Replaced by |
|---|---|---|
| **Standard** (the old default) | For a single user; no isolation between users | Access mode **No isolation shared** (legacy) → today use **Dedicated** (single user) or **Standard** (shared, isolated) |
| **High Concurrency** | Shared by many users; user code in separate processes (SQL, Python, R — not Scala); with **Table ACLs** or **credential passthrough** it enforced Hive-metastore-era security; did not auto-terminate by default | HC + Table ACLs → **Shared**, now **Standard** access mode. HC without TACL/passthrough → No isolation shared. **Not available** on workspaces with a Unity Catalog metastore |
| **Single Node** | Driver only, no workers | The **Single node** checkbox (node-layout axis) |

Official migration mapping (archived doc):

| Original settings | New access mode |
|---|---|
| Standard | No isolation shared |
| Standard + credential passthrough | Single user (Dedicated) + passthrough |
| High Concurrency | No isolation shared |
| High Concurrency + Table Access Control | Shared (now Standard) |
| High Concurrency + credential passthrough | Shared + passthrough |

⚠️ Name collision to call out in interviews: **legacy "Standard cluster mode" ≈ *no isolation*, while today's "Standard access mode" = *shared with isolation*.** Same word, nearly opposite meaning.

### 6. Putting the axes together — examples

| Real resource | A. Purpose | B. Layout | C. Access mode |
|---|---|---|---|
| Team dev cluster for ShopCo engineers | All-purpose | Multi node (autoscale 1–4) | Standard |
| Data scientist's R/GPU box | All-purpose | Single node | Dedicated (user) |
| Nightly `orders_etl` cluster | Jobs | Multi node (fixed 4) | Standard or Dedicated (service principal) |
| Nightly job, no cluster config | Serverless jobs | (hidden) | (standard semantics) |
| Power BI endpoint | SQL warehouse (serverless) | (T-shirt size, 1–3 clusters) | n/a |
| Legacy 2020 "HC + Table ACL" cluster | All-purpose | Multi node | Shows as **Custom**/legacy; migrate to Standard |

### 7. Real example — ShopCo's compute inventory

ShopCo's platform team lists 7 resources and is asked "how many cluster types do we use?" The right answer: *"Along purpose: 4 — all-purpose, jobs, serverless jobs, serverless SQL warehouse. Along access mode: 2 — Standard and Dedicated. Along layout: 2 — single and multi node. Plus one instance pool feeding the job clusters."*

### 8. Legacy vs current terminology (summary)

| You may hear | Say / map to |
|---|---|
| Interactive cluster | All-purpose compute |
| Automated cluster / job cluster | Jobs compute |
| Shared cluster / Shared access mode | **Standard** access mode |
| Single user / Assigned cluster | **Dedicated** access mode |
| High Concurrency cluster | Legacy cluster mode → Standard access mode (if it had Table ACLs) |
| Standard cluster *mode* | Legacy → No isolation shared (don't confuse with Standard *access* mode) |
| SQL endpoint | **SQL warehouse** |
| DLT cluster | Pipeline compute (Lakeflow SDP) |
| Cluster policy | Compute policy |
| `USER_ISOLATION` / `SINGLE_USER` | `DATA_SECURITY_MODE_STANDARD` / `DATA_SECURITY_MODE_DEDICATED` (old values still accepted as aliases) |

### 9. Misconceptions

1. **"There are exactly N cluster types."** There are several axes; give the axes.
2. **"Instance pools are a cluster type."** A pool is idle VMs that clusters *use*; it runs no code.
3. **"A SQL warehouse is just a cluster."** It's compute managed for SQL only — no Python, no access mode, scaled by size + number of clusters.
4. **"Standard access mode = the old Standard cluster."** Opposite meaning (see §5).
5. **"Dedicated is more secure, so always use it."** Standard is the recommended default; Dedicated is for workloads that need RDD/R/GPU/ML runtime or privileged access.
6. **"Serverless supports everything classic does."** No R, no RDD, no Spark UI, limited Spark configs, no caching APIs, no init scripts/pools/policies.

---

## B. Practical — the same job expressed on different compute types

**Config file:** [`exercises/phase-05/04_compute_types.job.yml`](../../exercises/phase-05/04_compute_types.job.yml) — an Asset Bundle (Declarative Automation Bundle) job that uses five different compute choices so you can see how each is declared.

Key fragments:

```yaml
job_clusters:
  - job_cluster_key: etl_multi            # (1) JOBS compute, multi node, Standard access mode
    new_cluster:
      spark_version: 16.4.x-scala2.12
      node_type_id: Standard_E8ds_v5
      autoscale: {min_workers: 2, max_workers: 6}
      data_security_mode: DATA_SECURITY_MODE_STANDARD
      runtime_engine: PHOTON
  - job_cluster_key: small_single         # (2) JOBS compute, SINGLE node, Dedicated
    new_cluster:
      spark_version: 16.4.x-scala2.12
      node_type_id: Standard_D4ds_v5
      num_workers: 0
      spark_conf:
        spark.databricks.cluster.profile: singleNode
        spark.master: "local[*, 4]"
      custom_tags: {ResourceClass: SingleNode}
      data_security_mode: DATA_SECURITY_MODE_DEDICATED
tasks:
  - task_key: ingest_serverless           # (3) no cluster key => SERVERLESS jobs compute
    notebook_task: {notebook_path: ../src/ingest_orders.py}
  - task_key: gold_sql                    # (4) SQL WAREHOUSE
    sql_task:
      warehouse_id: ${var.warehouse_id}
      file: {path: ../src/gold_revenue.sql}
```

| # | What to notice |
|---|---|
| 1 | Jobs compute is defined *inside* the job → created per run, terminated after. `data_security_mode` is the API name of access mode |
| 2 | Single node = `num_workers: 0` **plus** the single-node profile/master/tag. Without those, 0 workers means Spark can't run |
| 3 | A notebook task with no compute → serverless (if enabled in the workspace) |
| 4 | SQL tasks run on a SQL warehouse, not on a cluster |
| (file) | Also shows a pool-backed cluster (`instance_pool_id`) and, commented out, `existing_cluster_id` — running on all-purpose compute, which works but is billed at the higher all-purpose rate |

**Expected after `databricks bundle validate`:** "Validation OK!" (with your own `warehouse_id`/pool id). Do **not** deploy to a shared/prod workspace — this is a reading exercise.

**UI exercise (5 min):** *Compute → Create compute* → open *Advanced* → change the runtime to an **ML** runtime and watch Access mode **Auto** resolve to **Dedicated**; switch back to a standard LTS runtime → it resolves to **Standard**.

### Common mistakes
- Saying "High Concurrency" for a Unity Catalog workspace — it doesn't exist there.
- Running production jobs on all-purpose compute (higher DBU rate, shared state, no isolation per run).
- Picking Dedicated "for safety" and then hitting cost/management problems — Standard is the recommended default.
- Expecting R or RDD code to run on Standard or serverless.

---

## C. Interview questions

### 🟢 Basic

**B1.** How many types of clusters are there in Databricks?

<details><summary>Model answer (≈60 seconds)</summary>

"It depends which axis you mean, so let me split it.
**By purpose and lifecycle**, there's *all-purpose* compute for interactive work, *jobs* compute that's created for a job run and terminated after it, *serverless* compute for notebooks, jobs and pipelines where Databricks manages the machines, *pipeline* compute for Lakeflow Declarative Pipelines, and *SQL warehouses* — serverless, pro or classic — for SQL and BI. Instance pools aren't compute themselves; they keep VMs warm so clusters start faster.
**By node layout**, a cluster is single node or multi node.
**By access mode**, classic compute is *Standard* — shared with user isolation, formerly called Shared — or *Dedicated* — one user or group, formerly Single user — and both support Unity Catalog. 'No isolation shared' is a legacy mode without Unity Catalog.
The old *Standard / High Concurrency / Single Node* cluster modes are legacy; they were replaced by access modes plus the single-node option.
For a new ETL job I'd use serverless or jobs compute with Standard access mode, and a SQL warehouse for BI."
</details>

**B2.** What's the difference between all-purpose and jobs compute?

<details><summary>Model answer</summary>

All-purpose compute is created by users for interactive work, can be shared and restarted, and runs until terminated or auto-terminated; it's billed at the higher all-purpose DBU rate. Jobs compute is created by the job scheduler for a single run and terminated when the run finishes, giving isolation per run and a lower DBU rate — the right choice for production.
</details>

**B3.** What is an access mode?

<details><summary>Model answer</summary>

A compute security setting that determines who can use the compute and how data access is enforced. Current modes are Standard (multi-user with isolation; Python/SQL/Scala) and Dedicated (single user or group; also R, RDD, GPU, ML runtime). Both support Unity Catalog. Auto picks one based on runtime and hardware.
</details>

### 🟡 Medium

**M1.** Map these legacy terms to current ones: Shared, Single user, High Concurrency, SQL endpoint, `USER_ISOLATION`.

<details><summary>Model answer</summary>

Shared → Standard access mode. Single user → Dedicated access mode. High Concurrency → legacy cluster mode; with Table ACLs it maps to Shared/Standard, without them to No isolation shared; not available with Unity Catalog. SQL endpoint → SQL warehouse. `USER_ISOLATION` → legacy alias of `DATA_SECURITY_MODE_STANDARD`.
</details>

**M2.** When would you choose Dedicated over Standard access mode?

<details><summary>Model answer</summary>

When the workload needs something Standard doesn't support: RDD APIs, R, GPU instances, Databricks Runtime ML, spark-submit-style or privileged machine access, or restricted Spark/JVM configs. Otherwise Standard is recommended — it's shareable, isolated and cheaper to manage. Note that on Dedicated, querying objects with row filters/column masks requires DBR 15.4 LTS+ with serverless enabled.
</details>

**M3.** What are the SQL warehouse types and how do they differ?

<details><summary>Model answer</summary>

Serverless, pro and classic. Serverless runs in Databricks' serverless plane, starts in seconds, and has Photon, Predictive IO and Intelligent Workload Management. Pro runs in your subscription with Photon and Predictive IO but no IWM, starting in minutes; it's used where serverless isn't available or you need custom networking. Classic has only Photon — entry-level performance.
</details>

### 🔴 Tricky

**T1.** An interviewer says, "There are three cluster modes: Standard, High Concurrency and Single Node." How do you respond without sounding argumentative?

<details><summary>Model answer</summary>

"That was the legacy cluster-mode dropdown. In current Databricks it's split into node layout — single node or multi node — and access mode — Standard, formerly Shared, and Dedicated, formerly Single user. High Concurrency isn't available on Unity Catalog workspaces; its Table-ACL behaviour became the Shared/Standard access mode, and legacy Standard mode maps to 'No isolation shared'. One trap: today's *Standard access mode* is shared with isolation, the opposite of the old *Standard cluster mode*."
</details>

**T2.** Your team's 2021 cluster shows access mode "Custom" and can't read Unity Catalog tables. Why, and what do you do?

<details><summary>Model answer</summary>

"Custom" is the UI label for clusters defined with legacy settings (e.g. High Concurrency / No isolation / passthrough) — they have no UC-compatible access mode. Only Standard and Dedicated support Unity Catalog. Create a new compute (or edit) with Standard access mode (or Dedicated if you need R/RDD/GPU), on a recent LTS runtime, ideally via a compute policy; remove credential-passthrough settings.
</details>

**T3.** Is an instance pool a type of cluster? Does it save money?

<details><summary>Model answer</summary>

No — a pool is a set of idle, ready VMs that classic clusters draw nodes from, which cuts start and autoscale time. Idle pool instances incur no DBUs but you **do** pay the Azure VM cost while they idle, so a pool with many idle instances can *increase* cost. It saves money only indirectly (shorter runs/less waiting) and only for classic compute; serverless doesn't use pools.
</details>

**T4.** A job runs on serverless and fails on `sc.parallelize(...)` and on an R cell. Why?

<details><summary>Model answer</summary>

Serverless uses Spark Connect with standard access mode semantics: RDD APIs (including `sc.parallelize`) aren't supported, and R isn't supported. Rewrite with DataFrame APIs (`spark.createDataFrame`), and move R work to Dedicated classic compute.
</details>

---

## E. Revision summary

- **No single number — name the axes.** A: purpose/lifecycle · B: node layout · C: access mode · D: legacy cluster mode.
- **A:** all-purpose · jobs · serverless (notebooks/jobs/pipelines) · pipeline compute · SQL warehouses (serverless/pro/classic). Pools = warm VMs, not compute; policies = rules.
- **B:** single node (driver only, local Spark) vs multi node.
- **C:** **Standard** (formerly Shared; multi-user, isolated, no R/RDD/ML/GPU) · **Dedicated** (formerly Single user; user or group; R/RDD/GPU/ML) · Auto · legacy No isolation shared (no UC).
- **D:** Standard / High Concurrency / Single Node → replaced by access modes + single-node option. Legacy *Standard mode* ≠ today's *Standard access mode*.

---

## Sources

Checked **2026-10-18** (Microsoft Learn pages show `ms.date` between 2026-09-11 and 2026-10-15):

- Compute overview — https://learn.microsoft.com/en-us/azure/databricks/compute/
- Compute configuration reference (access modes, Auto, single node, autoscaling, spot, Photon) — https://learn.microsoft.com/en-us/azure/databricks/compute/configure
- Standard compute requirements and limitations — https://learn.microsoft.com/en-us/azure/databricks/compute/standard-limitations
- Dedicated compute overview — https://learn.microsoft.com/en-us/azure/databricks/compute/dedicated-overview
- Serverless compute for notebooks — https://learn.microsoft.com/en-us/azure/databricks/compute/serverless/notebooks
- Serverless compute limitations — https://learn.microsoft.com/en-us/azure/databricks/compute/serverless/limitations
- Serverless compute for jobs — https://learn.microsoft.com/en-us/azure/databricks/jobs/run-serverless-jobs
- SQL warehouse types — https://learn.microsoft.com/en-us/azure/databricks/compute/sql-warehouse/warehouse-types
- Create a SQL warehouse (sizes, auto stop) — https://learn.microsoft.com/en-us/azure/databricks/compute/sql-warehouse/create
- Instance pools — https://learn.microsoft.com/en-us/azure/databricks/compute/pool-index
- Compute policies / default policy families — https://learn.microsoft.com/en-us/azure/databricks/admin/clusters/policies , https://learn.microsoft.com/en-us/azure/databricks/admin/clusters/policy-families
- Pipeline enhanced autoscaling — https://learn.microsoft.com/en-us/azure/databricks/ldp/auto-scaling
- Legacy: Clusters UI changes and cluster access modes (mapping table) — https://learn.microsoft.com/en-us/azure/databricks/archive/compute/cluster-ui-preview
- Legacy: Configure compute (cluster modes, security modes) — https://learn.microsoft.com/en-us/azure/databricks/archive/compute/configure
- Clusters API `data_security_mode` values (incl. `DATA_SECURITY_MODE_STANDARD/DEDICATED/AUTO`, aliases) — https://docs.databricks.com/api/azure/workspace/clusters/create