# 6.2 The Transaction Log (`_delta_log`), Commits, Checkpoints

**Time:** 30 min (+30 min for the key practical) · **Prerequisites:** 6.1

**After this topic you can:**
- name the log actions (`protocol`, `metaData`, `add`, `remove`, `txn`, `commitInfo`) and what each field is for;
- explain why a commit is atomic and why readers never see half a write;
- explain checkpoints, `_last_checkpoint`, and exactly how a reader rebuilds the table snapshot;
- explain log retention vs data-file retention and what each one controls;
- read a real commit file and say what changed on disk.

---

## A. Concept deep dive

### 1. The simplest explanation

The `_delta_log` folder is the table's **ledger**. Every change to the table is one numbered entry (a **commit**). Each entry lists **actions**: "file X is now part of the table", "file Y is no longer part of the table", "the schema is now S", "this was an UPDATE by user U". The current table is what you get by applying all entries in order.

> The table is not "the files in the folder". The table is **"the files the log says are live at version N"**.

### 2. Why a log instead of the folder listing

| Problem with listing the folder | How the log fixes it |
|---|---|
| Listing millions of objects in S3/ADLS is slow and paginated | The live file list is *in* the log/checkpoint — read a few files instead of listing |
| A half-finished write's files are visible | New files are invisible until a commit **references** them |
| No way to say "this file is replaced by that one" atomically | One commit = `remove` old + `add` new, applied together or not at all |
| No history | Every commit is kept (for the retention period) → time travel and audit |
| Statistics require opening files | Per-file stats live in the `add` action |

### 3. File naming

```
_delta_log/
  00000000000000000000.json                 commit for version 0 (20-digit, zero-padded)
  00000000000000000001.json                 version 1
  ...
  00000000000000000010.checkpoint.parquet   classic checkpoint of the state AT version 10
  00000000000000000010.crc                  (optional) checksum/summary file: numFiles, size, ... for v10
  _last_checkpoint                          small JSON pointer to the newest checkpoint
```

Other names you may meet: multi-part checkpoints `…10.checkpoint.0000000001.0000000003.parquet` (deprecated), **V2 checkpoints** `…10.checkpoint.<uuid>.parquet|json` with `_sidecars/` (Databricks uses V2 by default for liquid-clustered tables created on DBR 14.1+), and `_staged_commits/` for coordinated commits. The version number in the name is what matters.

### 4. The actions — what each one means

| Action | Meaning | Key fields | When it appears |
|---|---|---|---|
| `protocol` | Minimum reader/writer capabilities needed to touch this table | `minReaderVersion`, `minWriterVersion`, `readerFeatures`, `writerFeatures` (table features such as `deletionVectors`, `columnMapping`, `v2Checkpoint`) | v0; again on upgrade (enabling a feature) |
| `metaData` | Table identity and definition | `id` (table UUID), `schemaString` (JSON schema), `partitionColumns`, `configuration` (table properties), `createdTime` | v0; again on any schema/property/partitioning change |
| `add` | "This file is now part of the table" | `path`, `partitionValues`, `size`, `modificationTime`, **`dataChange`**, **`stats`** (JSON: `numRecords`, `minValues`, `maxValues`, `nullCount`), `tags`, `deletionVector` | Every write that creates data files |
| `remove` | "This file is no longer part of the table" (a **tombstone** — the file is *not* deleted) | `path`, `deletionTimestamp`, `dataChange`, optional `size`, `partitionValues`, `deletionVector` | UPDATE, DELETE, MERGE, overwrite, OPTIMIZE |
| `txn` | Idempotency marker for an application: "app X has committed its batch Y" | `appId`, `version`, `lastUpdated` | Streaming sinks, `foreachBatch` with `txnAppId`/`txnVersion` (see [1.12 notebook](../../exercises/phase-01/12_idempotency_demo.py)) |
| `commitInfo` | Provenance — *who/what/how* (not used to compute state) | `timestamp`, `userName`, `operation`, `operationParameters`, `readVersion`, `isolationLevel`, `isBlindAppend`, `operationMetrics`, `engineInfo` | Every commit (Databricks always writes it) → `DESCRIBE HISTORY` |
| `cdc` | Change-data file for Change Data Feed | `path`, `size`, `dataChange=false` | When CDF is on and the operation needs explicit change files (6.6) |
| `domainMetadata` | Named config domains (e.g. clustering columns for liquid clustering) | `domain`, `configuration`, `removed` | Liquid clustering, row tracking, etc. |

**`dataChange` is important:** `true` = rows were logically inserted/updated/deleted. `false` = same rows, rearranged (OPTIMIZE, Z-order). Streaming readers of the table skip `dataChange=false` commits, which is why OPTIMIZE doesn't cause downstream duplicates.

### 5. What a commit looks like (representative, abbreviated)

**`00000000000000000000.json` — CREATE TABLE** (one JSON object per line; line order may differ):

```json
{"commitInfo":{"timestamp":1760779200000,"userName":"user@example.com","operation":"CREATE TABLE","operationParameters":{"partitionBy":"[]","clusterBy":"[]","properties":"{\"delta.enableDeletionVectors\":\"true\",\"delta.checkpointInterval\":\"5\"}"},"isolationLevel":"WriteSerializable","isBlindAppend":true,"engineInfo":"Databricks-Runtime/…"}}
{"metaData":{"id":"5c1e…","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"order_id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}, …]}","partitionColumns":[],"configuration":{"delta.enableDeletionVectors":"true","delta.checkpointInterval":"5"},"createdTime":1760779199000}}
{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["deletionVectors"],"writerFeatures":["deletionVectors", …]}}
```

**`00000000000000000001.json` — append of 6 rows in 2 files:**

```json
{"commitInfo":{"operation":"WRITE","operationParameters":{"mode":"Append","partitionBy":"[]"},"readVersion":0,"isolationLevel":"WriteSerializable","isBlindAppend":true,"operationMetrics":{"numFiles":"2","numOutputRows":"6","numOutputBytes":"3480"}, …}}
{"add":{"path":"part-00000-1a2b…-c000.snappy.parquet","partitionValues":{},"size":1742,"modificationTime":1760779260000,"dataChange":true,"stats":"{\"numRecords\":3,\"minValues\":{\"order_id\":101,\"amount\":40.0,…},\"maxValues\":{\"order_id\":105,\"amount\":300.0,…},\"nullCount\":{\"order_id\":0,…}}"}}
{"add":{"path":"part-00001-9f8e…-c000.snappy.parquet","partitionValues":{},"size":1738,"modificationTime":1760779260000,"dataChange":true,"stats":"{\"numRecords\":3, …}"}}
```

Note that `stats` is a JSON **string** inside JSON. It is what data skipping reads (6.8).

### 6. Why a commit is atomic

```
Writer                                           Storage
  │ 1. read latest snapshot (say v4)               _delta_log/…04.json is newest
  │ 2. write new Parquet files                     part-…new.parquet appear — referenced by nobody
  │ 3. validate: did anyone commit v5 meanwhile? (6.3)
  │ 4. create _delta_log/…05.json  ◀── the ONLY moment the table changes
  ▼    • "put-if-absent": succeeds only if 05.json doesn't exist yet
       • if it already exists → someone else won → re-check conflicts, try 06.json
```

- If the writer crashes **before step 4**, the new files are **orphans**: invisible, later removed by VACUUM.
- If it crashes **after step 4**, the commit is complete. There is no "half" state, because one file creation either happened or didn't.
- Mutual exclusion on the version number needs storage that supports atomic "create if not exists". ADLS/GCS do; S3 historically did not, so open-source Delta needed a single writer cluster or a DynamoDB-backed LogStore; Databricks uses its own commit service. (Newer Delta versions also support **coordinated / catalog-managed commits**, where a coordinator assigns versions.)

### 7. Checkpoints — so readers don't replay 100,000 JSON files

A **checkpoint** is a Parquet file containing the **complete reconciled state** at version N: the protocol, metaData, every live `add`, live `txn` versions, plus not-yet-expired `remove` tombstones (VACUUM needs them).

| Fact | Value / detail |
|---|---|
| Interval | Table property `delta.checkpointInterval`. Open-source Delta default: **every 10 commits**. Databricks staff have stated newer runtimes checkpoint less often (every 100 commits); the runtime decides — **check your `_delta_log` listing** rather than assuming |
| Written by | The writer that commits version N (synchronously or right after the commit) |
| Format | Parquet with one column per action type (`add`, `remove`, `metaData`, `protocol`, `txn`, …); one row per action |
| Stats inside | `delta.checkpoint.writeStatsAsStruct = true` (default) → stats as typed columns (fast skipping); `writeStatsAsJson` default `false` on Databricks |
| `_last_checkpoint` | Small JSON e.g. `{"version":10,"size":27,"sizeInBytes":21344,"numOfAddFiles":18, …}` so readers find the newest checkpoint without listing the whole folder |

### 8. How a reader reconstructs a snapshot (the algorithm)

```
1. Read _last_checkpoint                      → "checkpoint at v10"
2. List _delta_log/ starting from v10          → 10.checkpoint.parquet, 11.json, 12.json, 13.json
3. Load the checkpoint (state at v10)
4. Apply 11.json, 12.json, 13.json in order:
     protocol / metaData  → keep the LATEST one
     txn                  → keep the latest version per appId
     add path P           → P is live
     remove path P        → P is no longer live (keep the tombstone for VACUUM)
     (a file is identified by path + deletionVector id, so "remove P, add P-with-DV" = same file, new DV)
5. Result = snapshot v13: schema, properties, list of live files with stats (+ DVs)
6. Query planning: prune that file list with partition values and min/max stats → read only needed files
```

Time travel to v12 = same algorithm but stop at 12 (start from a checkpoint ≤ 12).

### 9. Retention — two different clocks

| Setting | Default | Controls | Who enforces it |
|---|---|---|---|
| `delta.logRetentionDuration` | **30 days** | How long **commit JSONs/checkpoints** (= history) are kept | Log cleanup, which runs automatically when a checkpoint is written; only drops entries older than retention that are already covered by a newer checkpoint |
| `delta.deletedFileRetentionDuration` | **7 days** | How long a **removed data file** must be kept before `VACUUM` may delete it | `VACUUM` (manual or predictive optimization) |

To time-travel to version N you need **both**: the log entries for N *and* the data files N referenced. In practice your time-travel window is the **shorter** of the two (often 7 days, because VACUUM runs). Raising only `logRetentionDuration` does not help if VACUUM has deleted the files.

### 10. ShopCo example — reading the log during an incident

The finance team says "Gold revenue for 16 Oct dropped by 40 % at 03:00". You run:

```sql
DESCRIBE HISTORY shopco.gold.daily_revenue LIMIT 5;
```

and see version 812 at 02:58: `operation = WRITE`, `operationParameters.mode = Overwrite`, `operationMetrics.numOutputRows = 3`, `userName = svc-legacy-job`. The commit's actions are `remove` × 140 and `add` × 1 → somebody overwrote the whole table with 3 rows. You `RESTORE TABLE … TO VERSION AS OF 811` (6.6) and fix the job. The log told you **who, when, what operation, and exactly which files**.

### 11. When it matters / when not to touch it

- **Do** read the log to debug (history, metrics, which files changed), to explain behaviour, and to verify maintenance.
- **Never** edit, delete or copy individual files in `_delta_log`, and never delete data files by hand. Use SQL/DeltaTable APIs.

### 12. Misconceptions

1. **"Delta keeps a copy of the data per version."** No. Versions share files; a version is a *list*. Only changed files are new.
2. **"A `remove` action deletes the file."** It is a tombstone; the file stays until VACUUM.
3. **"Checkpoints are backups."** They are a *cache* of the reconciled log state for fast reads, not a data backup.
4. **"`logRetentionDuration = 365 days` gives a year of time travel."** Only if deleted-file retention (VACUUM) is also ≥ 365 days.
5. **"`commitInfo` defines the table state."** It's provenance only; state comes from protocol/metaData/add/remove/txn.

---

## B. Practical — trace the log through a sequence of DML

**Runnable notebook:** [`exercises/phase-06/01_trace_delta_log.py`](../../exercises/phase-06/01_trace_delta_log.py) — the key Phase 6 practical. Run it with `USE_DELETION_VECTORS = True`, then again with `False`.

Key helper (how we read a commit file):

```python
def read_commit(version):
    p = f"{LOG}/{version:020d}.json"                         # 1
    with open(p) as fh:                                      # 2
        text = fh.read()
    return [json.loads(line) for line in text.splitlines()]  # 3

def step(title):
    h = spark.sql(f"DESCRIBE HISTORY {T} LIMIT 1").first()   # 4
    d = spark.sql(f"DESCRIBE DETAIL {T}").first()            # 5
    display(spark.createDataFrame(summarize(read_commit(h["version"])), ACTION_SCHEMA))  # 6
```

| # | Explanation |
|---|---|
| 1 | Commit files are named by version, zero-padded to 20 digits |
| 2 | `/Volumes/...` paths can be opened as local files on UC compute (fallback: `dbutils.fs.head`) |
| 3 | Newline-delimited JSON: **one action per line** |
| 4 | Latest version, operation, `operationMetrics` |
| 5 | `numFiles` = live files in the snapshot (not files on disk) |
| 6 | Turns actions into a table: action, file, `dataChange`, `numRecords`, DV cardinality, size |

### What changed on disk — the whole sequence (representative; file names shortened)

Starting state: empty folder. "On disk" = Parquet files physically present.

| v | Operation | Actions in the commit (DV mode) | Actions (copy-on-write mode) | Live files (`numFiles`) DV / CoW | On disk DV / CoW |
|---|---|---|---|---|---|
| 0 | CREATE TABLE | `commitInfo`, `metaData` (schema, `configuration`), `protocol` (reader 3 / writer 7, `deletionVectors`) | same; protocol may be lower (e.g. 1/2) without table features | 0 | 0 |
| 1 | INSERT 6 rows (2 files) | `add` A (3 rows), `add` B (3 rows), `dataChange=true` | same | 2 | 2 |
| 2 | INSERT 2 rows | `add` C; `isBlindAppend=true` | same | 3 | 3 |
| 3 | UPDATE 102 | `remove` A, `add` A **+DV(1)**, `add` D (1 row: 102 shipped) | `remove` A, `add` A′ (all 3 rows rewritten) | 4 / 3 | 4 + 1 `.bin` / 4 |
| 4 | DELETE 105 | `remove` B, `add` B **+DV(1)** — no new data file | `remove` B, `add` B′ (2 rows) | 4 / 3 | 4 + `.bin` / 5 |
| 5 | MERGE (upd 101, ins 109) | `remove`/`add` of the file holding 101 with a bigger DV, `add` E (new 101 + 109) | `remove` file with 101, `add` its rewrite (+109, or 109 in its own file) | ~5 / ~4 | grows |
| — | *checkpoint* | `00000000000000000005.checkpoint.parquet` + `_last_checkpoint` (interval set to 5 in the demo) | same | | |
| 6a | append with extra column | **rejected — no commit** (schema enforcement) | same | unchanged | unchanged (orphan files possible) |
| 6b | append + `mergeSchema` | **`metaData`** (schema now has `channel`), `add` F | same | +1 | +1 |
| 7 | OPTIMIZE | `remove` × all small files, `add` × 1 big file, **all `dataChange=false`**; DVs materialised | same | 1 | all old files still there |
| — | time travel v1 | reads A, B from disk | reads A, B | — | — |
| — | VACUUM DRY RUN | lists nothing (files younger than 7 days) | same | 1 | unchanged |

Exact counts depend on how Spark splits rows into files, so the notebook prints the real numbers next to each step.

### Common mistakes

- Using `dbutils.fs.ls(path)` to count files in a table — that counts dead files too; use `DESCRIBE DETAIL` `numFiles`.
- Assuming a checkpoint every 10 commits on Databricks — look at the folder.
- Copying a table by copying the folder while it is being written — you may copy a log that references files you didn't copy. Use `DEEP CLONE`.
- Reading `stats` as an object — it's a JSON string; parse it.

---

## C. Interview questions

### 🟢 Basic

**B1.** What is stored in `_delta_log`?

<details><summary>Model answer</summary>

Numbered JSON commit files (one per table version), each with actions: `protocol`, `metaData`, `add`, `remove`, `txn`, `commitInfo` (and `cdc`/`domainMetadata` when used); periodic Parquet checkpoints holding the full reconciled state at a version; `_last_checkpoint` pointing to the newest checkpoint; optionally `.crc` checksum files.
</details>

**B2.** What does an `add` action contain, and why are its `stats` important?

<details><summary>Model answer</summary>

Path, partition values, size, modification time, `dataChange`, stats, optional tags and deletion-vector descriptor. Stats (numRecords, per-column min, max, nullCount) let the planner skip files whose value ranges can't match a filter without opening them.
</details>

**B3.** What is a checkpoint and why does it exist?

<details><summary>Model answer</summary>

A Parquet file with the complete table state at version N (protocol, metadata, all live files, txn versions, tombstones). Readers start from the latest checkpoint and replay only later JSON commits, instead of all commits since v0. Open-source default interval is 10 commits (`delta.checkpointInterval`); Databricks runtimes may use a different cadence.
</details>

### 🟡 Medium

**M1.** Walk through how a reader builds the current snapshot.

<details><summary>Model answer</summary>

Read `_last_checkpoint` → load that checkpoint → list commits after it → apply them in order: latest protocol and metaData win, latest txn per appId, `add` makes a file live, `remove` makes it dead (file identity = path + DV id). The result is the schema, properties and live-file list with stats; the planner then prunes files by partition values and stats.
</details>

**M2.** Why can't a reader ever see half of a Delta write?

<details><summary>Model answer</summary>

Data files are written first and are unreferenced; the table only changes when the writer atomically creates the next numbered commit file (put-if-absent / commit service). Readers build snapshots only from committed log entries, so they see either the whole commit or none of it. A crashed writer leaves orphans that VACUUM cleans.
</details>

**M3.** `delta.logRetentionDuration` vs `delta.deletedFileRetentionDuration`?

<details><summary>Model answer</summary>

Log retention (default 30 days) — how long commit/checkpoint history is kept; enforced by automatic log cleanup at checkpoint time. Deleted-file retention (default 7 days) — the minimum age of a removed data file before VACUUM may delete it. Time travel to a version needs both its log entries and its data files, so the effective window is the shorter one.
</details>

### 🔴 Tricky

**T1.** Your table has 2 live files per `DESCRIBE DETAIL` but `dbutils.fs.ls` shows 900 Parquet files. Is the table broken?

<details><summary>Model answer</summary>

No. The 898 others are files removed by OPTIMIZE/UPDATE/MERGE/overwrite (tombstoned but kept for time travel and concurrent readers) or orphans from failed writes. They are deleted by VACUUM once older than the retention period. If storage cost matters, check VACUUM is running (or predictive optimization); don't delete by hand.
</details>

**T2.** What is `txn` used for, and what would break without it?

<details><summary>Model answer</summary>

It records the last committed `version` per application `appId` in the table state. Streaming sinks and `foreachBatch` writers (`txnAppId`/`txnVersion`) check it before committing: if this batch's version is not newer, the write is skipped. Without it, a micro-batch retried after a crash (commit happened, checkpoint not updated) would be appended twice → duplicates. It's how exactly-once writes into Delta are achieved.
</details>

**T3.** Why does OPTIMIZE not make a downstream streaming job reprocess data, but RESTORE might?

<details><summary>Model answer</summary>

OPTIMIZE writes its remove/add actions with `dataChange=false`, and Delta streaming sources skip such commits — same rows, new layout. RESTORE re-adds old files with `dataChange=true` (it is a logical data change), so a stream may see them as new rows and process them again — the docs warn of duplicates downstream.
</details>

---

## E. Revision summary

- Table = files the log says are live at version N; not the folder listing.
- Actions: `protocol` (capabilities), `metaData` (id, schema, partitions, properties), `add` (file + stats + DV), `remove` (tombstone), `txn` (idempotency), `commitInfo` (audit), `cdc`, `domainMetadata`.
- Commit = atomic creation of the next numbered JSON; data files first, commit last; crash → orphans.
- Checkpoint = Parquet state at N; `_last_checkpoint` points to it; reader = checkpoint + later JSONs. OSS default interval 10 — verify on Databricks.
- Retention: log 30 days (history), deleted files 7 days (VACUUM). Time travel needs both.
- `dataChange=false` = layout-only (OPTIMIZE) → streams ignore it.