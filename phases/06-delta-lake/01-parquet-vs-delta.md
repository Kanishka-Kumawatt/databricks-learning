# 6.1 Parquet vs Delta Lake

**Time:** 15 min · **Prerequisites:** 1.3 (lake / warehouse / lakehouse), Phase 4 (reading and writing Parquet)

**After this topic you can:**
- explain what Parquet gives you and what it cannot give you;
- explain exactly what Delta Lake adds on top of Parquet (and what it does *not* change);
- describe the folder layout of a Delta table;
- explain the difference between the open-source Delta protocol and Databricks-only features.

---

## A. Concept deep dive

### 1. The simplest explanation

- **Parquet** is a **file format**: one file holds rows stored column by column, compressed, with a schema and min/max statistics in the file footer.
- **Delta Lake** is a **table format**: a folder of ordinary Parquet files **plus a transaction log** (`_delta_log/`) that says *which* files make up the table at each version, what the schema is, and what happened in each change.

> Parquet answers "how are bytes laid out in one file?". Delta answers "which files are the table right now, and how do many writers change that safely?".

### 2. What Parquet is good at

| Feature | Why it matters |
|---|---|
| Columnar layout | `SELECT SUM(amount)` reads only the `amount` column chunks (column pruning) |
| Compression + encoding per column (dictionary, RLE, Snappy/ZSTD) | Similar values sit together → small files, less I/O |
| Row groups with min/max stats in the footer | A reader can skip row groups whose range can't match the filter (predicate pushdown) |
| Self-describing schema | Every file carries its own schema |
| Open, engine-neutral | Spark, Trino, pandas/pyarrow, Snowflake, DuckDB all read it |

### 3. What a *folder* of Parquet files cannot do

A "table" made of a Parquet folder is just whatever files a reader finds when it lists the directory. That causes every lake problem from 1.3:

| Need | Plain Parquet folder | Why |
|---|---|---|
| Atomic multi-file write | ❌ | A job writing 50 files that dies after 30 leaves 30 visible files |
| Isolation (readers vs writers) | ❌ | A reader listing during an overwrite sees a mix |
| UPDATE / DELETE / MERGE | ❌ | Files are immutable; you rewrite by hand and hope nobody reads mid-way |
| Schema enforcement | ❌ | Any file with any schema can be dropped in |
| History / rollback | ❌ | Overwrite destroys the old files |
| Concurrent writers | ❌ | Two jobs overwriting the same folder corrupt each other |
| Fast file discovery | ❌ | Listing millions of objects in cloud storage is slow |
| Table-level stats for skipping | ⚠️ | Only per-file footers — you must *open* each file to read them |

### 4. What Delta adds — and how

```
orders/                                   ← the table is a folder (a path in object storage)
├── _delta_log/                           ← the transaction log (the "brain")
│   ├── 00000000000000000000.json         ← commit v0: protocol, metaData (schema), add files
│   ├── 00000000000000000001.json         ← commit v1: add / remove actions
│   ├── ...
│   ├── 00000000000000000010.checkpoint.parquet   ← full table state at v10 (periodic)
│   └── _last_checkpoint                  ← pointer: "latest checkpoint is v10"
├── part-00000-3f1c…-c000.snappy.parquet  ← ordinary Parquet data files
├── part-00001-9ab2…-c000.snappy.parquet
└── deletion_vector_7e1d….bin             ← only if deletion vectors are enabled (6.4)
```

| Delta capability | Mechanism (one line) | Lesson |
|---|---|---|
| Atomic commits | Write data files first; the commit is creating the *next* log file, which is atomic ("put-if-absent") | 6.2, 6.3 |
| Isolation | Readers read a snapshot = the set of files listed in the log at version N | 6.2, 6.3 |
| Concurrent writers | Optimistic concurrency control: validate against commits made since you read, then commit | 6.3, 6.9 |
| UPDATE/DELETE/MERGE | Write new files, log `remove` for old files (or attach a deletion vector) | 6.4 |
| Schema enforcement / evolution | Schema stored in the `metaData` action; every write is checked against it | 6.5 |
| Time travel / RESTORE | Replay the log only up to version N | 6.6 |
| Audit | `commitInfo` in every commit → `DESCRIBE HISTORY` | 6.6 |
| Data skipping | Per-file min/max/nullCount stats stored **in the log** (`add.stats`) — no need to open files | 6.8 |
| Compaction / layout | OPTIMIZE rewrites files; log records remove+add with `dataChange=false` | 6.7 |

**What Delta does NOT change:** the data files are still standard Parquet. Delta does not add indexes inside the files, does not run a server, and does not lock the table. All "intelligence" is in the log and in the engine that follows the protocol.

### 5. Open-source protocol vs Databricks features

| Open-source Delta Lake (docs.delta.io, `PROTOCOL.md`) | Databricks-specific (on top) |
|---|---|
| Transaction log format, actions, checkpoints | Predictive optimization (automatic OPTIMIZE/VACUUM/ANALYZE) |
| ACID via optimistic concurrency | Row-level concurrency (fewer conflicts) |
| Time travel, RESTORE, VACUUM, OPTIMIZE, Z-order | Photon engine, predictive I/O |
| Deletion vectors, liquid clustering, column mapping (protocol table features) | `CLUSTER BY AUTO`, auto-tuned file sizes, Unity Catalog governance |
| Change Data Feed | "Automatic CDF" on the newest runtimes (verify for your runtime) |

Interview tip: say "the Delta Lake **protocol**" for the open part and "on Databricks" for the managed extras. It shows you know the boundary.

### 6. When to use which

| Use plain Parquet | Use Delta |
|---|---|
| One-off export/hand-off file to a system that can't read Delta | Any table that is written repeatedly, updated, or read while being written |
| Write-once immutable archive with no concurrent readers | Bronze / Silver / Gold tables |
| Landing-zone files produced by an external tool (you then ingest them into Delta) | Anything needing MERGE, history, rollback, governance |

### 7. Misconceptions

1. **"Delta is a different file format."** No — data files are Parquet; Delta is the log + protocol.
2. **"Delta converts my data into something proprietary."** The protocol is open; other engines (Trino, Flink, delta-rs, DuckDB, and Iceberg readers via UniForm) read it.
3. **"The folder listing is the table."** For Delta, a file in the folder that is not referenced by the log is **not** part of the table (failed writes, removed files awaiting VACUUM).
4. **"Delta updates Parquet files in place."** Parquet files are immutable. Delta writes new files and changes the log (or writes a deletion vector).
5. **"You can read a Delta folder with `spark.read.parquet`."** It will "work" but reads *every* file in the folder, including removed and failed ones, and ignores deletion vectors → wrong results.

---

## B. Practical — prove the difference

```python
base = "/Volumes/main/de_prep/files/p6_parquet_vs_delta"                     # 1
df = spark.createDataFrame([(101, "C1", 250.0), (102, "C2", 100.0)],
                           "order_id INT, customer_id STRING, amount DOUBLE")
df.write.format("delta").mode("overwrite").save(f"{base}/orders")           # 2  v0
spark.sql(f"DELETE FROM delta.`{base}/orders` WHERE order_id = 102")        # 3  v1

print(spark.read.format("delta").load(f"{base}/orders").count())            # 4
print(spark.read.parquet(f"{base}/orders").count())                         # 5
display(dbutils.fs.ls(f"{base}/orders"))                                    # 6
```

**Expected output (representative):**

```
1        ← line 4: Delta reads the log → only order 101
2 or 3   ← line 5: Parquet reads every .parquet file in the folder, including the logically removed one
```

| Line | What happens | Lesson |
|---|---|---|
| 1 | Working path in a UC volume (change catalog/schema/volume) | |
| 2 | Writes Parquet data file(s) and `_delta_log/00000000000000000000.json` | Delta = Parquet + log |
| 3 | Copy-on-write: writes a new file without 102 (or nothing, if 102 was alone in its file) and logs `remove` for the old one. With deletion vectors: writes a small `.bin` file and logs remove+add of the *same* Parquet file carrying a DV | Old file stays on disk |
| 4 | Snapshot = files listed in the log (minus DV-deleted rows) → correct answer | |
| 5 | Ignores the log; reads all files → **stale/duplicate rows**. With deletion vectors it also ignores the DV, so the deleted row is still there | Never read Delta as Parquet |
| 6 | Shows old and new files side by side, plus `_delta_log/` | Files leave only through VACUUM |

### Common mistakes

- Reading a Delta folder with `spark.read.parquet(...)` or with an engine that doesn't understand Delta.
- Copying "just the Parquet files" to migrate a table — you lose history, schema, DVs and possibly correctness. Use `DEEP CLONE`.
- Deleting files by hand to free space — use `VACUUM`.
- Converting an existing Parquet folder by rewriting it: `CONVERT TO DELTA parquet.\`path\`` builds a log over the existing files without rewriting them.

---

## C. Interview questions

### 🟢 Basic

**B1.** What is the difference between Parquet and Delta Lake?

<details><summary>Model answer</summary>

Parquet is a columnar file format describing how data is laid out in one file. Delta Lake is a table format: Parquet data files plus a transaction log (`_delta_log`) that records which files form each table version, the schema, and every change. The log gives ACID transactions, isolation, DML, schema enforcement, time travel and file-level statistics.
</details>

**B2.** What is inside a Delta table folder?

<details><summary>Model answer</summary>

Parquet data files (possibly in partition sub-folders), optional deletion-vector `.bin` files, and the `_delta_log/` folder containing JSON commit files (one per version, 20-digit zero-padded), periodic checkpoint Parquet files and `_last_checkpoint`. If Change Data Feed is enabled, a `_change_data/` folder may also exist.
</details>

**B3.** Name three things a Parquet folder can't do that a Delta table can.

<details><summary>Model answer</summary>

Atomic multi-file commits (no partial results after a crash), UPDATE/DELETE/MERGE, schema enforcement, time travel/rollback, safe concurrent writers, data skipping using stats stored in the log. Any three, with the mechanism.
</details>

### 🟡 Medium

**M1.** How does `CONVERT TO DELTA` work, and does it rewrite data?

<details><summary>Model answer</summary>

It lists the existing Parquet files, takes or infers the schema (partition columns must be given for partitioned folders), collects statistics, and writes a first commit to `_delta_log` that `add`s every existing file. Data files are not rewritten — it's a metadata operation whose cost is listing plus reading footers/stats.
</details>

**M2.** Why are data-skipping statistics in the Delta log better than Parquet footer statistics alone?

<details><summary>Model answer</summary>

Footer stats require opening each file (object-storage requests per file) just to decide it can be skipped. Delta stores per-file min/max/nullCount/numRecords in the `add` action, so the planner prunes files from the log/checkpoint before any data file is opened. Row-group stats can still be used inside files that are opened.
</details>

**M3.** Which parts of what you know are open-source Delta and which are Databricks-only?

<details><summary>Model answer</summary>

Open protocol: log format, ACID/OCC, time travel, RESTORE, VACUUM, OPTIMIZE/Z-order, deletion vectors, liquid clustering, column mapping, CDF. Databricks: predictive optimization, row-level concurrency, Photon/predictive I/O, auto-tuned file sizes, `CLUSTER BY AUTO`, Unity Catalog governance and managed-table automation.
</details>

### 🔴 Tricky

**T1.** A colleague reads a Delta table from another tool with `read_parquet('path/*.parquet')` and gets more rows than Databricks shows. Explain.

<details><summary>Model answer</summary>

The tool ignores the transaction log. It reads every Parquet file in the folder, including files logically removed by UPDATE/DELETE/MERGE/OPTIMIZE (still on disk until VACUUM) and files from failed writes. It also ignores deletion vectors, so soft-deleted rows reappear. Use a Delta-aware reader (delta-rs, DuckDB delta extension, Trino Delta connector, UniForm) or read through Databricks.
</details>

**T2.** "Delta Lake locks the table during writes." True?

<details><summary>Model answer</summary>

No. Delta uses optimistic concurrency: no locks while working; writers write files independently and only at commit time check for conflicts with commits that happened since they read. Mutual exclusion exists only for the instant of creating the next log file (atomic put-if-absent, or a commit service/coordinator on storage without that guarantee).
</details>

**T3.** If Delta files are Parquet, why can't you `CONVERT TO DELTA` and then time travel to last month?

<details><summary>Model answer</summary>

Time travel replays log versions. Conversion creates version 0 *now*; there is no earlier log, so there is nothing to travel back to. History starts at the first commit.
</details>

---

## E. Revision summary

- Parquet = **file** format (columnar, compressed, footer stats). Delta = **table** format = Parquet + `_delta_log`.
- The log says which files are the table at each version → atomicity, isolation, DML, enforcement, time travel, audit, skipping stats.
- Files are immutable; changes = new files + log actions (or deletion vectors). Old files stay until VACUUM.
- Never read a Delta folder as plain Parquet.
- Know the line between the open protocol and Databricks extras (predictive optimization, row-level concurrency, Photon, `CLUSTER BY AUTO`).