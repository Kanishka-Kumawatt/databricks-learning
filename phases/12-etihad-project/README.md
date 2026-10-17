# Phase 12 — Etihad Project Learnings

**Day 5 evening · 2.5 h · Level: ⭐ "Walk me through your project"**

**Goal:** an architecture walkthrough, 3–4 deep-dive stories, and a convincing "what I would improve".

> Confidentiality: this file records **patterns and decisions only** — no client paths, names of internal
> systems beyond the obvious, credentials or security findings. Speak the same way in the interview.
> Use "I" only for work you personally did; "our team" otherwise.

## A. Sections

| # | Section | Pattern used on the project | Concept it demonstrates | Time |
|---|---|---|---|---|
| 12.1 | Architecture | Many source types (SFTP, SQL Server, SAP, SharePoint, Event Hub, Cosmos DB, REST APIs) → ADF Copy to ADLS landing → Databricks notebooks → raw / staging / curated schemas in per-domain Unity Catalog catalogs → BI | Medallion, ingestion patterns, lakehouse | 30 min |
| 12.2 | Orchestration | ADF master → wrapper → child pipelines; metadata-driven Lookup/ForEach; schedule triggers; alerting via webhooks; parallel notebook fan-out; gradual move to Databricks Jobs in Asset Bundles | ADF vs Jobs, parameters, failure handling | 20 min |
| 12.3 | Frameworks | Audit framework (config tables + per-run pipeline/activity/error tables with source/target counts); restart by delete-and-reload of the run's data; DQ checks with email alerts; GDPR retention/anonymisation driven by a rules table | Reconciliation, idempotency, data quality, governance | 20 min |
| 12.4 | Delta, CDC, SCD | Hand-written SCD2 (expire with MERGE, then insert new versions; surrogate keys from a control table); SCD1 upserts with a timestamp watermark; newer pipelines use AUTO CDC, expectations and Auto Loader with rescued data | MERGE, SCD1/2, watermarks, declarative pipelines | 25 min |
| 12.5 | HMS → Unity Catalog migration | Parallel code tree migrated domain by domain; 2-part → 3-part names; mounts / DBFS paths → external locations & Volumes; environment parameter resolves the catalog; child notebooks don't inherit parameters; shared access mode blocks RDD APIs and local-file access to DBFS; parallel-run validation before cut-over | Unity Catalog, access modes, external locations, volumes | 30 min |
| 12.6 | Security & CI/CD | Key Vault-backed secret scopes; moving ADF → Databricks auth from PAT to managed identity; PGP encryption of outbound files; bundles with dev/prod targets, service-principal run-as, PR validation, code-quality gates; ADF ARM export | Least privilege, secrets, environment promotion | 15 min |
| 12.7 | Compute | Job clusters per team with cost tags; runtime upgrade to a current LTS; single-user job clusters with policies; some workloads still on all-purpose clusters | Job vs all-purpose compute, policies, cost | 10 min |

## 12.8 "What I would improve"

| Pattern seen | Why it's a problem | Better approach |
|---|---|---|
| SCD2 as two statements (MERGE, then INSERT) | Two transactions — failure between them leaves expired rows with no current version | Single MERGE with staged union, or AUTO CDC SCD2 |
| Heavy `collect()` / `toPandas()` | Driver OOM, no parallelism | Keep work distributed; collect only small control data |
| DQ null check = one `count()` per column | N full scans | One aggregation pass |
| Restart via delete-then-reinsert, no retries | Not atomic, manual recovery | `replaceWhere` overwrite or MERGE; sensible retries/timeouts |
| External Parquet tables, retry loops for concurrent writes | No ACID | Delta + optimistic concurrency |
| Static VACUUM list, little clustering | Maintenance drifts | Predictive optimisation, liquid clustering |
| SQL built by string formatting | Injection risk, fragile | Parameterised SQL / DataFrame API |
| Many near-duplicate linked services and backup copies of notebooks | Hard to maintain | Parameterised linked services; Git instead of copies |

## B. Practical

1. Rewrite a per-column null check as a single pass:
   ```python
   # before: one Spark job per column
   nulls = {c: df.filter(F.col(c).isNull()).count() for c in df.columns}
   # after: one pass
   nulls = df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns])
   ```
2. Migrate a legacy cell (2-part table name, `/mnt` path, `.rdd.map`) so it runs on a shared-access UC cluster.
3. Redesign the two-step SCD2 as one atomic MERGE (after Phase 9).

## C. Questions

### 🟢 Basic
- Walk me through your project's architecture in 2 minutes.
- Mount point vs Unity Catalog external location?
- Why a three-level namespace?

### 🟡 Medium
- Why did `.rdd.map` and `open('/dbfs/...')` break on shared access mode?
- How does the audit framework make reruns safe — and where is it weak?
- A parameter passed to a parent notebook isn't seen by child notebooks — why, and how do you fix it?

### 🔴 Tricky
- SCD2 = MERGE then INSERT; the cluster dies between them. Table state? What happens on rerun?
- When is `collect()` actually acceptable?
- Why would a team keep external Parquet over Delta — and what does it cost them?

## Personal contributions (fill in)

| Area | What I did | Result / impact |
|---|---|---|
| | | |