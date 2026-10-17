# 1.3 Data Lake vs Data Warehouse vs Lakehouse

**Time:** 35 min · **Prerequisites:** 1.2

**After this topic you can:**
- explain each architecture, what problem it solved and what problem it created;
- explain schema-on-write vs schema-on-read;
- explain *how* a lakehouse adds reliability to plain files (transaction log);
- explain why separating storage from compute matters.

---

## A. Concept deep dive

### 1. The simplest explanation

- **Data warehouse** — a database built for analytics. Clean, structured, reliable, fast SQL. Historically expensive and only for structured data.
- **Data lake** — a huge, cheap storage area (cloud object storage) where you can drop *any* file in *any* format. Flexible and cheap, but nothing stops it becoming a mess.
- **Lakehouse** — keep the cheap, open lake storage, but add a **transaction layer** (Delta Lake, Apache Iceberg, Apache Hudi) and governance so it behaves like a warehouse.

### 2. Data warehouse

| Aspect | Detail |
|---|---|
| Data | Structured, modelled (star schemas) |
| Schema | **Schema-on-write** — data must match the table's schema *when loaded*, otherwise the load fails |
| Storage | Managed by the warehouse, often proprietary format |
| Strengths | ACID transactions, fast SQL, strong governance, trusted numbers |
| Weaknesses | Expensive storage; poor fit for JSON/images/logs; ML tools must export data out; classic warehouses coupled storage and compute |
| Examples | Teradata, Oracle Exadata, Azure Synapse dedicated SQL pool, Snowflake, BigQuery |

### 3. Data lake

| Aspect | Detail |
|---|---|
| Data | Anything: CSV, JSON, Parquet, images, logs, audio |
| Schema | **Schema-on-read** — files are stored as-is; structure is applied by whoever reads them |
| Storage | Cloud object storage: Azure Data Lake Storage Gen2 (ADLS), Amazon S3, Google Cloud Storage |
| Strengths | Very cheap, virtually unlimited, open formats, any engine can read it, great for ML |
| Weaknesses | See below — the "data swamp" problems |

**Why plain lakes became swamps — the concrete technical problems:**

| Problem | What actually happens |
|---|---|
| **No atomic writes** | A Spark job writing 200 files crashes after 120. Readers now see a *partial* dataset — and nothing tells them. |
| **No isolation** | A reader lists the folder while a writer is replacing files → reads a mix of old and new. |
| **No updates/deletes** | GDPR "delete customer C1" means finding and rewriting every file containing C1, by hand. |
| **No schema enforcement** | A new file with `amount` as string, or an extra column, lands silently. Downstream queries break later. |
| **No history** | Overwrite = old data gone. No audit, no rollback. |
| **Small files & slow listing** | Thousands of tiny files; listing directories on object storage is slow. |
| **No governance** | Folder-level permissions only; nobody knows what a file means or who owns it. |

### 4. The two-tier problem

Many companies ran **both**: land everything in the lake, then copy curated data into a warehouse for BI.

```
Sources ──▶ Data lake (raw, ML) ──copy/ETL──▶ Warehouse (BI)
```

Result: two copies of data, two security models, extra pipelines, stale warehouse data, double cost — and BI and ML teams seeing different numbers.

### 5. Lakehouse — how it actually works

A lakehouse stores data as **open Parquet files in object storage** (lake economics), plus a **transaction log** that records which files make up each version of a table (warehouse reliability).

```
orders/                          ← a Delta table is just a folder
├── _delta_log/
│   ├── 00000000000000000000.json   ← commit 0: "add file part-000.parquet, part-001.parquet"
│   ├── 00000000000000000001.json   ← commit 1: "remove part-001, add part-002"
│   └── ...
├── part-000.parquet
├── part-001.parquet               ← no longer in the current version, still on disk until VACUUM
└── part-002.parquet
```

- **Readers** don't trust the folder listing; they read the log to know **exactly which files** form the current version.
- **Writers** first write new Parquet files (invisible to readers), then **atomically create the next log file**. If the job dies before the commit, the new files are simply never referenced → readers never see partial data.
- That one mechanism gives: **atomic commits, isolation, UPDATE/DELETE/MERGE, schema enforcement, time travel** (read an older log version). Phase 6 goes deep.

Add **governance** (Unity Catalog: permissions, lineage, audit) and **performance** (data skipping, caching, Photon engine) and you have a lakehouse.

| | Data lake | Data warehouse | Lakehouse |
|---|---|---|---|
| Storage cost | Low | High | Low |
| Formats | Any, open | Proprietary/structured | Open (Parquet + Delta/Iceberg) |
| Data types | All | Structured | All |
| ACID | ❌ | ✅ | ✅ |
| Schema | On read | On write | Enforced on write, evolvable |
| UPDATE/DELETE/MERGE | ❌ (manual rewrite) | ✅ | ✅ |
| Time travel | ❌ | Limited | ✅ |
| BI performance | Poor | Excellent | Very good |
| ML / data science | Excellent | Poor (export needed) | Excellent |
| Copies of data | — | Lake + warehouse | One |

### 6. Separation of storage and compute

| Coupled (classic warehouse) | Separated (lakehouse, modern cloud warehouses) |
|---|---|
| Storage and CPU live in the same machines | Data lives in object storage; compute clusters are started on demand |
| More data → buy more machines, even if CPU is idle | Scale storage and compute **independently** |
| Pay for the cluster 24/7 | Pay compute **only while jobs run**; clusters auto-terminate |
| One engine reads the data | Many engines (Spark, SQL warehouses, other tools) read the **same** files |

This is why "the cluster" in Databricks can be deleted without losing data — the data was never on the cluster (Phase 5).

### 7. Schema-on-write vs schema-on-read

| Schema-on-write | Schema-on-read |
|---|---|
| Validate structure at load time | Store raw, interpret at query time |
| Bad data rejected early | Bad data discovered late, by every reader |
| Less flexible | Very flexible |
| Warehouse, Delta tables (enforcement) | Raw files in a lake |

A lakehouse **uses both**: raw files land schema-on-read in Bronze/landing; tables from Bronze onward are schema-enforced Delta tables, with controlled **schema evolution** when sources add columns.

### 8. Misconceptions

1. **"A lakehouse is just a lake with SQL on top."** No — the transaction log (ACID, isolation, DML, time travel) is the essential difference.
2. **"Delta is a different file format from Parquet."** Delta = Parquet data files **+** a JSON/Parquet transaction log.
3. **"Lakehouse means no warehouse concepts."** You still model facts, dimensions and Gold tables — on lake storage.
4. **"Schema-on-read means no schema."** The schema still exists; it's just applied later, by every reader, inconsistently.

---

## B. Practical — see the difference between Parquet and Delta

```python
from pyspark.sql import functions as F

base = "/Volumes/main/de_prep/files/lakehouse_demo"                                 # 1
v1 = spark.createDataFrame([(101, "C1", 250.0), (102, "C2", 100.0)],
                           "order_id INT, customer_id STRING, amount DOUBLE")

# --- Plain Parquet folder (data lake) ---
v1.write.mode("overwrite").parquet(f"{base}/orders_parquet")                       # 2
v2 = spark.createDataFrame([(103, "C3", "40.0", "web")],
                           "order_id INT, customer_id STRING, amount STRING, channel STRING")
v2.write.mode("append").parquet(f"{base}/orders_parquet")                          # 3 succeeds silently
spark.read.parquet(f"{base}/orders_parquet").printSchema()                         # 4

# --- Delta table (lakehouse) ---
v1.write.format("delta").mode("overwrite").save(f"{base}/orders_delta")            # 5
try:
    v2.write.format("delta").mode("append").save(f"{base}/orders_delta")           # 6 rejected
except Exception as e:
    print("Delta rejected the write:", type(e).__name__)

display(dbutils.fs.ls(f"{base}/orders_delta/_delta_log"))                          # 7
display(spark.sql(f"DESCRIBE HISTORY delta.`{base}/orders_delta`"))                # 8
```

| Line | What happens | Lesson |
|---|---|---|
| 1 | Working folder in a Unity Catalog volume (create `main.de_prep` and a volume `files`, or use any path you can write to) | |
| 2 | Writes two orders as plain Parquet | |
| 3 | Appends a file with `amount` as **string** and an extra `channel` column — **no error** | A lake accepts anything |
| 4 | Reading now either fails with a type-conflict error or shows an inconsistent schema, depending on which file Spark samples for the schema | The problem is discovered *by the reader*, later |
| 5 | Writes the same two orders as a Delta table | |
| 6 | Same bad append → **AnalysisException** (schema mismatch) | **Schema enforcement** at write time |
| 7 | Shows the commit files `00000000000000000000.json`, … | The transaction log is just files |
| 8 | Shows each version: operation, timestamp, parameters | History and audit for free |

**Extra:** run `spark.read.format("delta").option("versionAsOf", 0).load(f"{base}/orders_delta")` — time travel.

### Common mistakes

- Writing Parquet to a path and calling it "a table" — no ACID, no enforcement.
- Using `mergeSchema` everywhere "to stop errors" — that silently accepts every schema change (Phase 6).
- Deleting files manually inside a Delta table folder — corrupts the table (log references missing files).

---

## C. Interview questions

### 🟢 Basic

**B1.** Define a data lake, a data warehouse and a lakehouse in one sentence each.

<details><summary>Model answer</summary>

A data lake is cheap object storage holding raw data of any format, structured at read time. A data warehouse is a system optimised for analytical SQL over structured, schema-enforced data with ACID guarantees. A lakehouse stores data in open formats on lake storage but adds a transaction layer (e.g. Delta Lake) and governance so it provides warehouse reliability and performance on that same storage.
</details>

**B2.** What is the difference between schema-on-write and schema-on-read?

<details><summary>Model answer</summary>

Schema-on-write validates data against a defined schema when it's loaded and rejects non-conforming data. Schema-on-read stores data as-is and applies a structure when it's queried — flexible, but errors are found late and each reader may interpret the data differently.
</details>

**B3.** What makes Delta Lake different from a folder of Parquet files?

<details><summary>Model answer</summary>

The `_delta_log` transaction log. It records which Parquet files belong to each table version, which enables atomic commits, isolation between readers and writers, schema enforcement, UPDATE/DELETE/MERGE, and time travel.
</details>

### 🟡 Medium

**M1.** A Spark job writing to a plain Parquet folder crashes halfway. What do readers see? What would they see with Delta, and why?

<details><summary>Model answer</summary>

Parquet: readers list the folder and see whatever files were written — a partial dataset, silently. Delta: the new files are written but the commit (next log JSON) never happens, so the log still points to the previous version's files; readers see the complete previous version. Orphan files are later removed by VACUUM.
</details>

**M2.** Why is the separation of storage and compute important for cost and scalability?

<details><summary>Model answer</summary>

You can scale and pay for them independently: storage is cheap and always on; compute is started only when needed and sized per workload, then terminated. Many engines/clusters can read the same data concurrently without copying it. In a coupled system you buy compute to get storage and pay for it even when idle.
</details>

**M3.** What is the "two-tier architecture" problem and how does a lakehouse address it?

<details><summary>Model answer</summary>

Companies kept raw data in a lake and copied curated data into a warehouse for BI: two copies, two security models, extra ETL, staleness, cost and inconsistent numbers between BI and ML. A lakehouse serves BI and ML from one governed copy of the data in open format.
</details>

### 🔴 Tricky

**T1.** "Delta Lake is a file format like Parquet or ORC." Correct the statement.

<details><summary>Model answer</summary>

Delta is a **table format / storage layer**, not a file format. Data files are standard Parquet; Delta adds a transaction log (JSON commit files plus periodic Parquet checkpoints) and a protocol that readers and writers follow. Any engine implementing the Delta protocol can read the table.
</details>

**T2.** A colleague deletes some old Parquet files from inside a Delta table's folder to "save space". What happens?

<details><summary>Model answer</summary>

The log still references those files for the current (or older) versions, so queries fail with file-not-found errors or time travel breaks. Space must be reclaimed with `VACUUM`, which only deletes files no longer referenced by versions inside the retention window.
</details>

**T3.** If a lakehouse gives warehouse features, why do some companies still keep a separate data warehouse?

<details><summary>Model answer</summary>

Existing investments and skills, tooling built around the warehouse, specific features (e.g. very high-concurrency BI, specific security/compliance certifications, workload management they trust), migration cost/risk, and organisational ownership. The architecture choice is also a business decision; a good answer acknowledges trade-offs instead of claiming one is always better.
</details>

---

## E. Revision summary

- Warehouse: reliable, structured, schema-on-write, historically costly. Lake: cheap, any format, schema-on-read, becomes a swamp.
- Lake problems: no atomic writes, no isolation, no DML, no enforcement, no history, small files, weak governance.
- Lakehouse = open Parquet on object storage **+ transaction log** (+ governance + performance).
- Writers write files, then atomically commit a log entry; readers trust the log, not the folder.
- Storage/compute separation → independent scaling, pay compute only when running, one copy for all engines.