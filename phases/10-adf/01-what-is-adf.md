# 10.1 What ADF Is; ADF vs Databricks Responsibilities

**Time:** 10 min · **Prerequisites:** 1.5 (ETL vs ELT), 1.11 (orchestration)

**After this topic you can:**
- explain in two sentences what Azure Data Factory (ADF) is and why teams use it;
- split work correctly between ADF (move + orchestrate) and Databricks (transform + govern);
- say how ADF relates to Synapse pipelines and Fabric Data Factory;
- avoid the classic anti-pattern of doing heavy transformation in ADF.

---

## A. Concept deep dive

### 1. The simplest explanation

**Azure Data Factory is Azure's managed service for moving data and orchestrating data jobs.** You describe *what* to copy and *in which order* to run steps (as JSON pipelines, usually built in a visual editor), and ADF runs it on managed compute — no servers to manage.

Think of ADF as the **conveyor belt and the foreman**, and Databricks as the **factory floor**:
- the conveyor belt brings raw material in from many places (Copy activity, 90+ connectors);
- the foreman decides who works when, waits for things, retries, and raises the alarm (control flow, triggers, alerts);
- the factory floor does the real work on the material (Spark transformations, Delta MERGE, quality rules).

### 2. Why ADF exists

| Problem | How ADF helps |
|---|---|
| Data lives in dozens of systems: SQL Server on-prem, Azure SQL, Oracle, SAP, Salesforce, SFTP, REST APIs | **Built-in connectors** + Copy activity — configure, don't code |
| Some sources are inside a private network with no inbound access | **Self-hosted Integration Runtime** (agent installed inside the network, only outbound HTTPS) |
| Steps across many Azure services must run in order (copy → Databricks → stored proc → Power BI refresh → email) | **Pipelines** with dependencies, branching, loops, retries |
| Runs must start on a schedule, per time window, or when a file lands | **Triggers**: schedule, tumbling window, storage event, custom event |
| Ops need to see failures and re-run | **Monitoring** UI, rerun from failed activity, Azure Monitor alerts |

### 3. How it works internally (mental model)

```
            ┌──────────────────── Data Factory (control plane, one region) ───────────────────┐
 trigger ──▶│ pipeline JSON ─▶ activity scheduler ─▶ dispatch each activity to an IR          │
            └──────────────────────────────┬───────────────────────────────┬─────────────────┘
                                           │                               │
                        data movement      ▼          activity dispatch    ▼
                ┌─────────────────────────────┐     ┌───────────────────────────────────┐
                │ Integration Runtime (IR)    │     │ External compute does the work:   │
                │ - Azure IR (serverless)     │     │ Databricks, SQL stored proc,      │
                │ - Self-hosted IR (your VM)  │     │ Azure Function, HDInsight, ...    │
                │ reads source, writes sink   │     │ ADF only starts it and polls it   │
                └─────────────────────────────┘     └───────────────────────────────────┘
```

Key point: when ADF "runs a Databricks notebook", **ADF does no Spark work**. It calls the Databricks API to submit a run, polls until it ends, and records the result. The Copy activity is different: there the IR itself reads and writes the bytes.

### 4. ADF vs Databricks — who does what

| Responsibility | ADF | Databricks |
|---|---|---|
| Connect to many SaaS / on-prem sources | ✅ primary strength | ⚠️ possible (JDBC, Lakeflow Connect managed connectors) |
| Reach a private on-prem network | ✅ Self-hosted IR | ⚠️ needs network setup (VNet injection, ExpressRoute/VPN, Lakeflow Connect gateway) |
| Land raw files in ADLS (Bronze landing) | ✅ Copy activity | ✅ Auto Loader reads them from there |
| Joins, dedup, window functions, SCD2, MERGE into Delta | ❌ (Mapping Data Flows exist, but are limited and not a lakehouse engine) | ✅ primary strength |
| Data quality rules, Unity Catalog governance, lineage | ❌ | ✅ |
| Cross-service orchestration (Databricks + SQL + Functions + Logic Apps) | ✅ | ⚠️ Jobs can call some external things, but Azure-wide orchestration is ADF's home turf |
| All-Databricks pipeline (ingest → silver → gold) | unnecessary extra layer | ✅ Lakeflow Jobs alone |

**The common, healthy Azure pattern (and the learner's project):** ADF copies source data to ADLS (landing / Bronze), then triggers a Databricks notebook or job that does Bronze → Silver → Gold. This is **ELT** (phase 1.5): load first, transform in the platform.

### 5. ADF, Synapse pipelines, Fabric Data Factory (brief)

| Product | What it is | Notes |
|---|---|---|
| **Azure Data Factory** | Standalone Azure resource | What this phase covers |
| **Synapse pipelines** | The same pipeline engine embedded in a Synapse workspace | Mostly the same activities/JSON; some features differ (e.g. no Azure-SSIS IR in Synapse) |
| **Data Factory in Microsoft Fabric** | Next-generation Data Factory inside Fabric (SaaS) | Different architecture ("connections" instead of linked services; no IR objects in the same way); Microsoft now recommends Fabric for new data-integration projects and offers ADF → Fabric migration guidance |

In an interview, say: "My project uses classic ADF; Fabric Data Factory is the successor product with a similar pipeline concept."

### 6. ShopCo example

ShopCo (online retailer) has: orders in **Azure SQL Database**, inventory in **on-prem SQL Server**, customers in **Salesforce**, and a lakehouse on **ADLS Gen2 + Databricks (Unity Catalog)**.

```
Azure SQL (orders) ─┐ Copy (Azure IR)
SQL Server on-prem ─┼─ Copy (Self-hosted IR) ─▶ ADLS landing/  ─▶ Databricks notebook/job
Salesforce ─────────┘ Copy (Azure IR)            (Parquet)          Bronze → Silver (MERGE) → Gold
                                                                    returns JSON result to ADF
ADF: schedule trigger 02:00 · watermark control table · alert via Logic App on failure
```

### 7. When to use ADF / when not

| Use ADF when | Don't add ADF when |
|---|---|
| Many heterogeneous sources, especially on-prem behind a firewall | Everything already lands in cloud storage and Databricks can ingest it (Auto Loader, Lakeflow Connect) |
| The company already runs an ADF estate and an ops team monitors it | A small team wants one tool, one place to debug |
| You orchestrate several Azure services, not only Databricks | You need complex transformation logic — put that in Spark, not ADF expressions |

### 8. Misconceptions

1. **"ADF transforms the data."** Mostly no — in this architecture it moves data and orchestrates; Spark transforms.
2. **"ADF runs the notebook, so ADF compute size matters."** The Databricks cluster size matters; ADF only dispatches and polls.
3. **"ADF and Databricks Jobs are competitors, pick one."** They overlap in orchestration, but ADF's strength is connectivity and Azure-wide orchestration; see 10.9.
4. **"Mapping Data Flows replace Databricks."** They run Spark under the hood for visual transformations, but lack Delta/UC governance and the flexibility of code — not what a lakehouse team uses for Silver/Gold.

---

## B. Practical — responsibility map (design exercise, 3 min)

For each ShopCo requirement, write **ADF**, **Databricks**, or **both**, then compare.

| # | Requirement | Your answer |
|---|---|---|
| 1 | Pull yesterday's changed orders from Azure SQL | |
| 2 | Read inventory from SQL Server inside the head-office network | |
| 3 | Deduplicate orders and MERGE into `shopco.silver.orders` | |
| 4 | Fail the run if Silver row count doesn't reconcile with source | |
| 5 | Start at 02:00 UTC, retry transient copy errors, email on failure | |
| 6 | Refresh a Power BI dataset after Gold is ready | |

<details><summary>Answers</summary>

1 ADF (Copy with a watermark query). 2 ADF (Copy on Self-hosted IR). 3 Databricks. 4 Databricks raises an exception → notebook activity fails → ADF's failure path alerts (both). 5 ADF (trigger + activity retry + failure path) — or Databricks Jobs if there were no ADF. 6 ADF (Web activity to the Power BI REST API) or a Databricks job task — both are possible; ADF is common when ADF already orchestrates.
</details>

### Common mistakes

- Building 50-activity pipelines full of string-manipulating expressions that should be 10 lines of PySpark.
- Using ADF Copy to write directly into Silver Delta tables, bypassing Bronze — you lose replay and audit.
- Chaining ADF and a Databricks Job by time ("ADF at 01:00, Databricks job at 02:00") instead of ADF calling Databricks.

---

## C. Interview questions

### 🟢 Basic

**B1.** What is Azure Data Factory?

<details><summary>Model answer</summary>

A managed, serverless Azure service for data integration: it moves data between 90+ kinds of sources and sinks (Copy activity) and orchestrates multi-step workflows (pipelines with dependencies, loops, branching, triggers, retries, monitoring). It doesn't need servers, except an optional self-hosted IR to reach private networks.
</details>

**B2.** In an ADF + Databricks architecture, what does each tool do?

<details><summary>Model answer</summary>

ADF ingests (copies source data to ADLS landing/Bronze), schedules and orchestrates, passes parameters, handles failure paths and alerting. Databricks transforms: Bronze → Silver → Gold with Spark/Delta (dedup, MERGE, SCD, quality checks) under Unity Catalog governance, and returns results/status to ADF.
</details>

**B3.** Is the ADF + Databricks pattern ETL or ELT? Why?

<details><summary>Model answer</summary>

ELT: data is loaded raw into the lake first by ADF, then transformed inside the lakehouse by Databricks. That keeps raw data for replay and puts transformation on scalable Spark compute.
</details>

### 🟡 Medium

**M1.** When ADF runs a Databricks notebook, where does the compute happen and what does ADF actually do?

<details><summary>Model answer</summary>

All Spark compute happens on a Databricks cluster (new job cluster, existing cluster, pool, or serverless for the Job activity). ADF's IR only dispatches: it calls the Databricks API to submit the run, passes parameters, polls the run state, and captures the exit value (`runOutput`) and status. So scaling the ADF IR doesn't speed up the notebook.
</details>

**M2.** What is the difference between ADF, Synapse pipelines and Fabric Data Factory?

<details><summary>Model answer</summary>

Synapse pipelines are the ADF pipeline engine embedded in a Synapse workspace — largely the same activities and JSON, with some differences (e.g. no Azure-SSIS IR). Fabric Data Factory is the next-generation SaaS product inside Microsoft Fabric with a simpler architecture (connections instead of linked services/IRs as separate objects); Microsoft positions it as the successor and provides migration guidance from ADF.
</details>

**M3.** Why not do the Silver transformations in ADF Mapping Data Flows instead of Databricks?

<details><summary>Model answer</summary>

Data Flows are a visual Spark-based tool, fine for simple shaping, but a lakehouse team needs Delta-native operations (MERGE with conditions, SCD2, time travel, OPTIMIZE), Unity Catalog governance and lineage, testable code in Git, and freedom to tune Spark. Splitting transformation logic between two tools also makes debugging and ownership harder.
</details>

### 🔴 Tricky

**T1.** "We have ADF and Databricks, so let's schedule ADF at 01:00 and the Databricks job at 02:00." What's wrong, and what's better?

<details><summary>Model answer</summary>

Time-chaining: if the copy is slow, late or fails, Databricks processes incomplete or stale data and no one is told; if it's fast, time is wasted. Better: one orchestrator owns the chain — ADF calls the Databricks notebook/job activity after the copy succeeds (dependency), passing the exact landing path and watermark; or, if all ingestion can live in Databricks, use a Databricks job with a file-arrival trigger.
</details>

**T2.** Your interviewer says: "ADF is legacy; why would anyone still use it?" Respond.

<details><summary>Model answer</summary>

It's a mature, widely deployed service with broad connectors, self-hosted IR for hybrid networks, and Azure-wide orchestration; many enterprises have large ADF estates, monitoring and CI/CD built around it. Fabric Data Factory is the strategic successor, and Databricks Jobs + Lakeflow Connect reduce the need for ADF in all-Databricks pipelines — so for *new* designs I'd check whether ADF is needed at all, but for existing estates and hybrid sources it remains a sensible choice.
</details>

**T3.** A notebook triggered by ADF is slow. A colleague proposes increasing the ADF Azure IR size. Will it help?

<details><summary>Model answer</summary>

No. For a Databricks activity the IR only dispatches and polls. Performance depends on the Databricks cluster configuration (linked service: node type, workers, autoscale, pool/policy) and the notebook's Spark code (Phase 3). IR sizing (DIUs) matters for Copy activity throughput, not for external transformation activities.
</details>

---

## E. Revision summary

- ADF = managed **data movement + orchestration** on Azure; JSON pipelines, many connectors, triggers, monitoring.
- ADF **moves and orchestrates**; Databricks **transforms and governs**. Pattern: ADF Copy → ADLS landing → Databricks Bronze/Silver/Gold (ELT).
- For Databricks activities, ADF only **dispatches and polls**; compute is the Databricks cluster.
- Synapse pipelines ≈ ADF inside Synapse; **Fabric Data Factory** = successor SaaS product.
- Don't time-chain tools; don't put heavy transformation logic in ADF.