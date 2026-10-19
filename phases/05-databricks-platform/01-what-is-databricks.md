# 5.1 What Databricks Is — Relationship to Spark, the Lakehouse

**Time:** 15 min · **Prerequisites:** 1.3 (lake / warehouse / lakehouse), Phase 2 (Spark core)

**After this topic you can:**
- explain in two sentences what Databricks is and what it adds on top of Apache Spark;
- explain what the Databricks Runtime and Photon are, and when Photon does *not* help;
- place Databricks in the lakehouse picture: storage in your cloud, compute managed by Databricks, governance by Unity Catalog;
- translate old product names you will hear in interviews (Workflows, DLT, Repos, clusters) into the current ones.

---

## A. Concept deep dive

### 1. The simplest explanation

**Apache Spark** is an open-source engine that splits a big data job across many machines (Phase 2).
**Databricks** is a managed *data and AI platform* built around Spark: it creates and manages the machines for you, adds a faster engine (Photon), a transactional table format (Delta Lake), governance (Unity Catalog), orchestration (Lakeflow Jobs), SQL warehousing, notebooks, CI/CD tooling and more — all in one workspace.

> One-liner for interviews: *"Spark is the engine; Databricks is the managed platform around it — compute, storage format, governance, orchestration and collaboration — so teams build a lakehouse without running Spark infrastructure themselves."*

### 2. Why it exists — the problem with "just Spark"

| If you run open-source Spark yourself, you must… | Databricks gives you… |
|---|---|
| Provision VMs, install Spark/Java/Python, patch them | Compute created in minutes from a form, policy or API; or **serverless** (no VMs to manage at all) |
| Tune and upgrade Spark versions, resolve library conflicts | **Databricks Runtime** — a tested bundle of Spark + Delta + libraries, with LTS versions |
| Build a table format with ACID guarantees on files | **Delta Lake** built in (open source, but integrated and optimised) |
| Build your own permissions, audit and lineage | **Unity Catalog** — grants, lineage, audit, discovery in one place |
| Integrate a scheduler (Airflow, cron) | **Lakeflow Jobs** (orchestration) and **Lakeflow Spark Declarative Pipelines** |
| Give analysts a SQL endpoint | **SQL warehouses** + SQL editor + dashboards |
| Build collaboration (notebooks, Git) | Notebooks, Git folders, bundles for CI/CD |

### 3. How it fits together internally

```
                 ┌──────────────── Databricks ACCOUNT (accounts.azuredatabricks.net) ─────────────────┐
                 │  identities (users, groups, service principals) · workspaces · UC metastores · billing │
                 └────────────────────────────────────────────────────────────────────────────────────────┘
                                   │ one account → many workspaces; a metastore is attached to workspaces
     ┌─────────────────────────────▼──────────────────────────────┐
     │ CONTROL PLANE (runs in Databricks' cloud account)            │
     │  web UI · notebooks & job definitions · cluster manager ·    │
     │  job scheduler · Unity Catalog service · REST APIs           │
     └───────────────┬───────────────────────────────┬─────────────┘
                     │ launches & manages             │ launches & manages
     ┌───────────────▼──────────────┐   ┌─────────────▼───────────────────────┐
     │ CLASSIC COMPUTE PLANE         │   │ SERVERLESS COMPUTE PLANE             │
     │ VMs in YOUR Azure subscription │   │ compute in Databricks' account,      │
     │ (your VNet): all-purpose/job  │   │ same region: serverless notebooks,   │
     │ compute, classic/pro SQL WH   │   │ jobs, pipelines, serverless SQL WH   │
     └───────────────┬──────────────┘   └─────────────┬───────────────────────┘
                     └──────── read / write ──────────┘
                                   ▼
                YOUR STORAGE: ADLS Gen2 (Delta / Parquet files, volumes)
                — governed by Unity Catalog; compute never "owns" the data
```

Key point: **storage and compute are separated.** Data stays in cloud object storage as open-format files. Compute is temporary — clusters come and go, the data does not move. (Detail in 5.3.)

### 4. What is in the Databricks Runtime (DBR)?

| Component | Purpose |
|---|---|
| Apache Spark (Databricks-optimised build) | The distributed engine — every DBR version includes Spark |
| Delta Lake | ACID table format (Phase 6) |
| Photon | Native C++ vectorised execution engine (below) |
| Pre-installed libraries | Python/Scala/Java/R libraries pinned per version |
| Variants | **Standard** and **ML** (adds ML libraries; use only for ML/GPU/AutoML work), released as **LTS** (long-term support) and non-LTS versions |

### 5. Photon — what it is and when it doesn't help

- Photon replaces the JVM-based Spark SQL **execution** layer with native C++ for supported operations. Catalyst still **plans** the query; Photon **executes** it in columnar batches (SIMD, no JVM garbage collection).
- It's **enabled** on serverless compute, SQL warehouses and serverless pipelines; on classic all-purpose/jobs compute it's on by default in the UI (checkbox *Use Photon Acceleration*). Via the Clusters/Jobs API you set `runtime_engine: PHOTON` explicitly.
- **Biggest wins:** large scans, joins, aggregations, writes (including MERGE/UPDATE/DELETE), wide tables.
- **No/low benefit:** queries under ~2 seconds; **UDFs, RDD and Dataset APIs** are not supported (Photon falls back to Spark for those parts — results stay correct); **stateful streaming** isn't supported (stateless is).
- **Cost:** Photon consumes DBUs at a different rate than the same instance without Photon — check the pricing page; it pays off when runtime drops enough.

### 6. Where Databricks sits in the lakehouse

Recall 1.3: *lakehouse = cheap open files in object storage + a transaction log + governance*. On Databricks:

| Lakehouse layer | Databricks piece |
|---|---|
| Storage | ADLS Gen2 (your subscription) — Delta/Iceberg tables, volumes for files |
| Table format / transactions | Delta Lake (`_delta_log`) — Apache Iceberg is also supported for managed tables |
| Governance & catalog | Unity Catalog (permissions, lineage, audit, discovery) |
| Compute | All-purpose, jobs, serverless, SQL warehouses, pipelines |
| Processing frameworks | Spark (PySpark/SQL), Structured Streaming, Auto Loader, Lakeflow pipelines |
| Orchestration | Lakeflow Jobs |
| Consumption | SQL warehouses, dashboards, BI tools, ML |

### 7. Real example — ShopCo

ShopCo (an online retailer, our running example) lands order files from its OLTP database into ADLS. On Databricks:
1. A **Lakeflow Job** on **job compute** (or serverless) runs Auto Loader to append raw files into `shopco_prod.bronze.orders`.
2. The same job builds Silver and Gold with PySpark/SQL (Photon accelerates the joins and MERGEs).
3. Analysts query Gold through a **serverless SQL warehouse** from Power BI.
4. **Unity Catalog** controls who sees PII columns and records lineage from file → Bronze → Gold.

Nothing here requires installing Spark or managing VMs by hand.

### 8. Legacy vs current terminology

You will hear old names in interviews and in older projects. Use current names, but recognise both.

| Legacy / older name | Current name (docs checked 2026-10-18) | Note |
|---|---|---|
| Clusters (UI menu) | **Compute** | API and many people still say "cluster"; both fine |
| Databricks Workflows / Jobs | **Lakeflow Jobs** | Same jobs/tasks concept (Phase 8) |
| Delta Live Tables (DLT) | **Lakeflow Spark Declarative Pipelines** ("Lakeflow pipelines", SDP) | Billing system table still shows `DLT` as a product value |
| Repos | **Git folders** | 5.2 |
| Databricks Asset Bundles (DABs) | **Declarative Automation Bundles** | Same `databricks bundle …` CLI (5.11) |
| Cluster policies | **Compute policies** | 5.6 |
| Shared access mode | **Standard** access mode | 5.4 |
| Single user access mode | **Dedicated** access mode | 5.4 |
| Serverless budget policies | **Serverless usage policies** | 5.7 |
| DBFS mounts (`/mnt/...`) | **Unity Catalog volumes + external locations** | Mounts are deprecated (5.9) |
| Hive metastore (`hive_metastore` catalog) | **Unity Catalog** | 5.8 |

### 9. Misconceptions

1. **"Databricks is just hosted Spark."** Spark is the core, but governance (UC), Delta, Photon, serverless, SQL warehouses, orchestration and CI/CD are what you use day to day.
2. **"Databricks stores my data."** In classic setups your tables live in *your* ADLS account; Databricks compute reads/writes them. (Serverless *workspaces* can use Databricks-managed default storage — check your workspace type.)
3. **"Photon makes everything faster."** Not UDF-heavy code, not tiny queries, not stateful streaming.
4. **"Delta Lake is proprietary."** Delta Lake is open source; Databricks adds optimisations and management features around it.

---

## B. Practical — find the platform pieces in your workspace

UI walkthrough (read-only, ~10 minutes):

1. **Left sidebar** → note: *Workspace*, *Catalog*, *Jobs & Pipelines*, *Compute*, *SQL Warehouses*, *SQL Editor*. Each maps to a row in the table in §6.
2. **Compute** → open any compute → *Configuration* tab → note *Databricks Runtime version* (e.g. `x.y LTS`), *Use Photon Acceleration*, *Access mode*. Click **JSON** (top-right of the Configuration tab) to see the same config as the Clusters API sees it.
3. **Catalog** → expand a catalog → schema → table → *Details* tab shows *Type: MANAGED/EXTERNAL* and *Storage location* — proof that data lives in ADLS, not "inside" the cluster.
4. In a notebook attached to compute run:

```python
print(spark.version)                       # Spark version inside the Databricks Runtime
print(spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion", "n/a (e.g. serverless)"))
```

| Line | Explanation |
|---|---|
| 1 | Apache Spark version bundled in the DBR, e.g. `3.5.x` / `4.x` |
| 2 | The DBR version string the compute was created with (e.g. `16.4.x-scala2.12`). On serverless this conf may not be readable — the default value avoids an error (verify in your workspace) |

**Expected result:** two version strings; they differ because DBR versions (16.4, 17.3 …) and Spark versions (3.5, 4.0 …) are numbered independently.

A fuller version of this check is in [`03_inspect_compute.py`](../../exercises/phase-05/03_inspect_compute.py) (used in 5.3).

### Common mistakes
- Quoting the Spark version when asked for the runtime version (or vice versa).
- Assuming Photon is on for every job created via API — via API you must set `runtime_engine: PHOTON`.
- Using old names ("DLT", "Repos", "high concurrency cluster") without knowing the current ones.

---

## C. Interview questions

### 🟢 Basic

**B1.** What is Databricks, and how is it related to Apache Spark?

<details><summary>Model answer</summary>

Databricks is a managed data and AI platform built by the original creators of Spark. Spark is the distributed processing engine at its core; Databricks adds managed and serverless compute, the Databricks Runtime (optimised Spark + Delta Lake + libraries), the Photon engine, Unity Catalog governance, Lakeflow Jobs and pipelines, SQL warehouses and collaboration/CI-CD tooling. You use Spark APIs, but you don't operate Spark infrastructure yourself.
</details>

**B2.** What is the Databricks Runtime?

<details><summary>Model answer</summary>

The versioned software image that runs on Databricks compute: Apache Spark plus Delta Lake, Photon, performance improvements and a pinned set of libraries. It comes in standard and ML variants, and LTS versions receive long-term fixes — recommended for production jobs.
</details>

**B3.** What is Photon?

<details><summary>Model answer</summary>

Databricks' native C++ vectorised execution engine. Catalyst still plans the query; Photon executes supported operators on columnar batches, which speeds up scans, joins, aggregations and writes. Unsupported parts (UDFs, RDD/Dataset APIs) fall back to regular Spark transparently.
</details>

### 🟡 Medium

**M1.** Where does the data live when you use Databricks on Azure, and why does that matter?

<details><summary>Model answer</summary>

In cloud object storage — typically ADLS Gen2 in the customer's subscription — as open-format files (Delta/Parquet). Compute only reads and writes it. This separation means clusters can be terminated, resized or replaced without data loss, many compute resources can read the same data, you pay for storage and compute separately, and other engines can read the open files (governed through Unity Catalog).
</details>

**M2.** When would enabling Photon not reduce cost?

<details><summary>Model answer</summary>

When the workload doesn't use Photon-supported operations (heavy Python UDFs, RDD code), when queries are tiny (<~2 s, dominated by planning/scheduling), or for stateful streaming. Because Photon has a different DBU rate, you pay more per hour without getting a shorter runtime. Measure: compare runtime × DBU rate with and without Photon.
</details>

**M3.** Name four Databricks products by their current names and their older names.

<details><summary>Model answer</summary>

Lakeflow Jobs (Databricks Workflows/Jobs); Lakeflow Spark Declarative Pipelines (Delta Live Tables/DLT); Git folders (Repos); Declarative Automation Bundles (Databricks Asset Bundles); Compute (Clusters); Standard/Dedicated access modes (Shared/Single user).
</details>

### 🔴 Tricky

**T1.** "We don't need Databricks — we can run open-source Spark on Kubernetes." What do you lose? Give a balanced answer.

<details><summary>Model answer</summary>

You lose managed/serverless compute and autoscaling, the tested runtime, Photon, integrated Delta optimisations (predictive optimisation, liquid clustering), Unity Catalog governance/lineage/audit, built-in orchestration, SQL warehouses and CI/CD tooling — you'd rebuild and operate each yourself. You gain control, potentially lower licence cost and no vendor dependency. The right choice depends on team size, governance needs and the cost of building/operating the platform versus DBU spend.
</details>

**T2.** A colleague says, "Photon is enabled, so our Python UDF pipeline will be 3x faster." Respond.

<details><summary>Model answer</summary>

Photon doesn't execute Python UDFs; those stages fall back to the regular Spark engine (and the Python worker overhead stays). Only the surrounding scans/joins/aggregations benefit. The bigger win is usually replacing UDFs with built-in functions (which Photon can run) — then measure.
</details>

**T3.** Is the control plane in your Azure subscription? What runs where?

<details><summary>Model answer</summary>

No. The control plane (web app, notebook/job definitions, cluster manager, scheduler, Unity Catalog service, APIs) runs in Databricks' account. Classic compute VMs run in the customer's subscription (classic compute plane); serverless compute runs in Databricks' account in the same region (serverless compute plane). Data sits in customer storage (or default storage in serverless workspaces).
</details>

---

## E. Revision summary

- **Spark = engine; Databricks = managed platform** (compute, runtime, Delta, Photon, Unity Catalog, Lakeflow Jobs/pipelines, SQL warehouses, CI/CD).
- **DBR** = Spark + Delta + Photon + libraries; standard vs ML; LTS for production.
- **Photon** = C++ vectorised execution; wins on big scans/joins/writes; no UDF/RDD/Dataset, no stateful streaming; different DBU rate.
- **Control plane** (Databricks' account) vs **compute plane** (classic = your subscription; serverless = Databricks' account). Data in your storage.
- Know the **renames**: Lakeflow Jobs, Lakeflow SDP (was DLT), Git folders (was Repos), Declarative Automation Bundles (was DABs), Standard/Dedicated access modes.