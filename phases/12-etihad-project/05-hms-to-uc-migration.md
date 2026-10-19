# 12.5 Hive Metastore → Unity Catalog Migration (the strongest story)

**Time:** 30 min · **Prerequisites:** Phase 5 (UC, access modes, volumes), Phase 6

**After this topic you can:**
- explain *why* the migration was done and *what* changed in code, storage and compute;
- list the concrete breakages and the fix for each: 2-part names, mounts and `/dbfs` paths, RDD APIs on standard
  access mode, child notebooks not inheriting parameters, Python imports from workspace files;
- explain how our team validated with **parallel runs** and cut over domain by domain;
- tell this as a STAR story with numbers you can defend (see [13-project-stories.md](13-project-stories.md)).

---

## A. Concept / story

### 1. Why migrate

| Hive metastore (workspace-local) | Unity Catalog |
|---|---|
| Per-workspace metastore, table ACLs only on certain clusters | Account-level metastore shared by workspaces |
| `database.table` (2-part) | `catalog.schema.table` (3-part) |
| Storage reached through **mounts** with credentials baked into the mount | **Storage credentials + external locations**; access checked per user/group |
| Files on DBFS root / mounts | **Volumes** (`/Volumes/catalog/schema/volume/...`) for non-tabular files |
| No built-in lineage / audit | Lineage, system tables, audit logs, fine-grained grants, row filters/column masks |

DBFS mounts are a deprecated pattern; mounted data does not work with Unity Catalog, and Databricks recommends
external locations instead.

### 2. The migration approach

```
 HMS code tree (live)                    UC code tree (parallel copy, migrated domain by domain)
 ─────────────────────                   ──────────────────────────────────────────────────────
 notebooks read  db.table      ──►       notebooks read  {catalog}.schema.table   (catalog from env param)
 paths /mnt/<container>/...    ──►       abfss://… via external location, or /Volumes/...
 all-purpose / no-UC clusters  ──►       UC-enabled job clusters (standard or dedicated access mode)
 runtime: older LTS            ──►       current LTS
        │                                         │
        └──────── both run on the same inputs ────┘
                         │
                  compare counts / checksums / sample diffs per table  →  sign-off  →  repoint ADF  →  retire HMS
```

1. **Inventory**: tables, paths, mounts, clusters, jobs per domain; map each old `db.table` to `catalog.schema.table`
   (a mapping sheet — some legacy databases split across schemas, some schemas moved to a different domain catalog).
2. **Storage**: create storage credentials (managed identity via an access connector) and external locations;
   register or migrate tables (`SYNC`, `CREATE TABLE … LOCATION`, deep clone for managed).
3. **Code**: rewrite in a parallel tree with a tag comment on every change so reviews and rollbacks are easy.
4. **Compute**: UC-capable access mode, current LTS runtime, cluster policies.
5. **Validate**: parallel runs, reconciliation, then **cut over per domain** by repointing orchestration.

### 3. The code changes — breakage → cause → fix

| # | Breakage | Cause | Fix |
|---|---|---|---|
| 1 | `Table or view not found: sales.bookings` | 2-part name resolves against the default catalog | 3-part names; catalog from an **`env` parameter** (`dev`/`test`/`prod` → catalog name), or `USE CATALOG` once at the top |
| 2 | `/mnt/...` path not found / permission denied | Mounts not supported with UC | `abfss://container@account.dfs.core.windows.net/...` governed by an **external location**, or a **volume** path |
| 3 | `open('/dbfs/mnt/...')`, `os.listdir('/dbfs/...')` fail | On **standard (shared)** access mode, DBFS root and mounts don't support FUSE; POSIX `/dbfs` paths aren't supported | Use `dbutils.fs` / Spark readers on cloud paths, or put files in a **volume** — `/Volumes/...` paths support POSIX file APIs |
| 4 | `.rdd.map(...)`, `sc.parallelize` fail | RDD APIs are not supported on standard access mode | DataFrame API (`select`, `withColumn`, `F.transform`), or collect small control data and use plain Python |
| 5 | Child notebook uses a default catalog instead of prod | `dbutils.notebook.run` starts a new run; the child only sees **arguments you pass** (they set its widgets) | Pass `env` (and every needed param) explicitly in `arguments`; give widgets a safe default (e.g. sandbox), never prod |
| 6 | `ModuleNotFoundError` for shared helpers | Helpers moved to workspace files in a new folder; `sys.path` differed | Package helpers as workspace files / a wheel; append the repo-relative path; use `importlib` for non-standard names; pass `dbutils`/`spark` into helper functions — they aren't globals in imported modules |
| 7 | Hive UDFs / some Spark confs rejected | Standard mode restricts Hive UDFs and many confs | UC SQL/Python UDFs; move confs to cluster policy |
| 8 | Logs written to DBFS | DBFS root not for governed data | Write logs / staging files to a **volume**; clean per-run staging dirs |
| 9 | Behaviour changes after runtime upgrade | New Spark version (ANSI mode defaults, deprecated APIs) | Upgrade as part of migration with the parallel-run comparison catching differences |

### 4. When to choose dedicated vs standard access mode

- **Standard** (formerly *shared*): multi-user with isolation; most UC features; restrictions above (no RDDs, no
  DBFS FUSE, no Hive UDFs, limited confs; R not supported).
- **Dedicated** (formerly *single user*, can be assigned to a group): full Spark APIs incl. RDDs, R, ML runtimes.
  Job clusters running as a service principal are a natural fit.

Our default was to **fix the code** (DataFrame API, volumes) rather than fall back to dedicated mode, so notebooks
work on any UC compute, including serverless.

### 5. Parallel-run validation and cut-over

| Check | How |
|---|---|
| Row counts | Per table, per load date, old vs new |
| Content | `EXCEPT ALL` both directions on a sample window, or aggregate checksums (`sum(hash(*))`) per partition |
| Schema | Column names/types/nullability, including after runtime upgrade |
| Timing / cost | Run duration on new compute |
| Downstream | BI reports pointed at new tables for UAT |

Cut-over per domain: freeze the HMS version, final sync, repoint ADF/Jobs to the UC notebooks, keep HMS tables
read-only for a rollback window, then retire.

### 6. Misconceptions

1. **"UC migration = add a catalog prefix."** Names are the easy part; storage access, compute modes and file APIs are where it breaks.
2. **"Switch to dedicated mode and the problems go away."** It hides tech debt and blocks serverless.
3. **"Widgets propagate to child notebooks."** Only with `%run` (same context); `dbutils.notebook.run` needs explicit arguments.

---

## B. Practical

Run [`exercises/phase-12/01_uc_migration_rewrite.py`](../../exercises/phase-12/01_uc_migration_rewrite.py): a legacy
cell (2-part name, `/mnt` path, `.rdd.map`, `open('/dbfs/...')`) shown as text, and its UC-ready rewrite — 3-part names
via a catalog widget, volume paths, DataFrame API instead of RDD, and file access through volumes / `dbutils.fs` —
with a comment on every change. Then run it on a **standard access mode** cluster or serverless to prove it works.

---

## C. Interview questions

### 🟢 Basic

**B1.** Mount point vs Unity Catalog external location?

<details><summary>Model answer</summary>

A mount maps a storage path into DBFS using credentials stored with the mount — anyone on the workspace who can
use the mount gets the mount's access, and there is no per-user governance or lineage. An external location pairs a
cloud path with a storage credential (managed identity) and is a UC securable: you grant `READ FILES`/`WRITE FILES`
or create external tables/volumes on it, and access is audited per user. Mounts are deprecated and don't work with UC.
</details>

**B2.** Why a three-level namespace?

<details><summary>Model answer</summary>

`catalog.schema.table` adds a top level above the database so one metastore can serve many workspaces and
domains/environments — e.g. a catalog per domain, schemas per layer. Grants inherit down the hierarchy and
catalogs can be bound to workspaces.
</details>

**B3.** What is a volume and when did you use one?

<details><summary>Model answer</summary>

A UC object for non-tabular files in cloud storage, addressed as `/Volumes/catalog/schema/volume/path`. We used
volumes for landing files that notebooks read with Python file APIs, for per-run staging directories, and for log
files that used to go to DBFS.
</details>

### 🟡 Medium

**M1.** Why did `.rdd.map` and `open('/dbfs/...')` break on shared access mode?

<details><summary>Model answer</summary>

Standard (shared) access mode isolates users from each other, so it only allows APIs it can govern. RDD APIs bypass
Spark Connect/UC enforcement and are not supported. DBFS root and mounts don't support FUSE there, so POSIX
`/dbfs/...` paths fail. We replaced RDD code with DataFrame functions and moved file access to volumes or
`dbutils.fs` on governed paths.
</details>

**M2.** A parameter passed to a parent notebook isn't seen by child notebooks — why, and how do you fix it?

<details><summary>Model answer</summary>

`dbutils.notebook.run` starts the child as a separate ephemeral run; its widgets get only the `arguments` you pass
(strings) or their defaults. The parent's widget values aren't inherited. During migration a child fell back to its
default catalog. Fix: pass every parameter explicitly — we passed `env` everywhere — and make defaults safe
(never prod). `%run` is different: it runs inline in the same context.
</details>

**M3.** How did you validate the migration before cut-over?

<details><summary>Model answer</summary>

Parallel runs: HMS and UC versions processed the same inputs; we compared row counts per table and load date,
did `EXCEPT ALL` both ways or checksum aggregates per partition, compared schemas, and had BI users UAT reports
on the UC tables. Only after a clean comparison window did we repoint orchestration, keeping HMS read-only for rollback.
</details>

### 🔴 Tricky

**T1.** After migration a job writes to the dev catalog in production. How could that happen?

<details><summary>Model answer</summary>

The catalog is resolved from an `env` parameter with a default. If the orchestrator didn't pass it — or a child
notebook was invoked with `dbutils.notebook.run` without forwarding it — the widget default applied. Prevention:
no defaults that point to a real environment, fail fast if `env` is missing, derive it from the job/bundle target,
and use UC grants so the prod service principal can't write to dev (and vice versa).
</details>

**T2.** Why not just run everything on dedicated (single-user) clusters to avoid rewrites?

<details><summary>Model answer</summary>

It works short-term but keeps RDD and DBFS-path code, blocks moving to serverless and standard compute, and
pushes sharing problems to later. We fixed the code where it was cheap and used dedicated mode only where a
library genuinely required it.
</details>

**T3.** A table was migrated by registering the same external location in UC while HMS jobs still write to it. Risk?

<details><summary>Model answer</summary>

Two metastores pointing at the same Delta path: both can read and write, so data is consistent at the Delta level,
but governance is split (HMS writers bypass UC grants), and if one side changes the table (schema evolution,
`VACUUM` with different retention) it can break the other's time travel or readers. Treat dual registration as a
short, controlled window with one writer.
</details>

---

## E. Revision summary

- Why: account-level governance, 3-part names, external locations, volumes, lineage, audit.
- Breakages: **2-part names**, **mounts**, **`/dbfs` FUSE**, **RDD APIs**, **child-notebook params**, **imports**, Hive UDFs, runtime changes.
- Fixes: env → catalog, external locations/`abfss://`, volumes, DataFrame API, explicit `arguments`, packaged helpers.
- Standard (shared) mode: no RDDs, no DBFS FUSE, no Hive UDFs; dedicated (single user) has full APIs.
- Validate with **parallel runs** (counts, `EXCEPT ALL`, checksums, UAT) → cut over **per domain** → HMS read-only → retire.