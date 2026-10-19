# 6.3 ACID & Optimistic Concurrency Control

**Time:** 25 min · **Prerequisites:** 6.2

**After this topic you can:**
- explain how Delta delivers each letter of ACID, with the mechanism;
- walk through optimistic concurrency control: read → write → validate → commit;
- explain what "conflict" means in Delta and name the exception types;
- compare `WriteSerializable` (default) and `Serializable`;
- explain what row-level concurrency changes on Databricks and its requirements.

---

## A. Concept deep dive

### 1. The simplest explanation

Many jobs can write to the same Delta table at the same time **without locking it**. Each job works on its own private copy of "the table as of version N", writes its new files, and at the end asks: *"Did anyone commit something since N that changes the facts I relied on?"* If not, it commits as the next version. If yes, it fails with a conflict exception and must be retried.

That is **optimistic** concurrency control: assume conflicts are rare, check only at the end.

### 2. ACID in Delta — letter by letter

| Property | Meaning | How Delta achieves it |
|---|---|---|
| **A**tomicity | All of a write happens, or none | One commit = one atomically created JSON file. Data files are invisible until referenced. Crash → orphans, no partial table |
| **C**onsistency | Every commit moves the table from one valid state to another | Schema enforcement against `metaData`, `NOT NULL` and `CHECK` constraints, protocol checks, OCC conflict validation |
| **I**solation | Concurrent operations don't see each other's partial work | Readers use a fixed **snapshot** (version N) for the whole query. Writers are validated by OCC at commit (WriteSerializable / Serializable) |
| **D**urability | Once committed, it stays | The commit and data files are in cloud object storage (replicated, durable), not on cluster disks |

Scope: classic Delta guarantees are **per table, per commit**. A notebook that updates table A and then table B is two independent transactions — if the second fails, the first is *not* rolled back. (Databricks has introduced multi-statement / multi-table transactions on recent runtimes — check current docs before relying on them.)

### 3. Why not locks?

| Pessimistic locking (classic DB) | Optimistic concurrency (Delta) |
|---|---|
| Lock rows/tables before changing | No locks; check at commit |
| Needs a central lock manager | No server — just files in object storage + atomic "create next version" |
| Long jobs block others | Long jobs never block; they may *lose* the race instead |
| Good when conflicts are frequent | Good when conflicts are rare (typical analytics: appends, partition-disjoint jobs) |

### 4. OCC step by step

```
Writer W (an UPDATE)                                   _delta_log
 ─────────────────────────────────────────────────────────────────────────
 1 READ      snapshot = v10; record what I read:        …10.json  (latest)
             "files A,B matching status='placed'"
 2 WRITE     write new files A', B' (invisible)
 3 VALIDATE  try to create …11.json
               ├─ success → done (v11)
               └─ already exists (someone committed v11, maybe v12)
                    for each winning commit since v10:
                      • did it ADD files that match my read predicate?   → ConcurrentAppendException
                      • did it REMOVE a file I read?                    → ConcurrentDeleteReadException
                      • did it REMOVE a file I also remove?             → ConcurrentDeleteDeleteException
                      • did it change metaData (schema/properties)?     → MetadataChangedException
                      • did it change protocol / create the table?      → ProtocolChangedException
                      • same streaming txn appId committed?             → ConcurrentTransactionException
                    no conflict → retry commit as …13.json automatically (files are reused, not rewritten)
 4 COMMIT    …13.json written
```

Key points:
- The **automatic retry** after a *non*-conflicting race is cheap: Delta just tries the next version number.
- A **real conflict** surfaces as an exception; Delta does **not** redo your work. Your job/orchestrator must retry the whole operation (it will re-read the new snapshot).
- `commitInfo.readVersion` records the snapshot the writer read (10 above), so history shows "read 10, committed 13".

### 5. The exception types (verified names, Delta docs)

| Exception | Cause | Typical scenario |
|---|---|---|
| `ConcurrentAppendException` | A concurrent operation **added files** in the data your operation **read** | MERGE on `region='North'` while another job appends rows to North |
| `ConcurrentDeleteReadException` | A concurrent operation **deleted a file you read** | Two MERGEs/UPDATEs touch the same file; the winner removed it |
| `ConcurrentDeleteDeleteException` | A concurrent operation **deleted a file you also delete** | Two DELETE/compaction jobs on the same files |
| `MetadataChangedException` | A concurrent transaction **changed metadata** (ALTER TABLE, schema-evolving write, properties) | Schema evolution in one job while another writes |
| `ProtocolChangedException` | Protocol upgrade, concurrent **create/replace**, or concurrent writes to an empty path | Two jobs both creating the same table for the first time |
| `ConcurrentTransactionException` | Two streaming queries with the **same checkpoint** writing at once | Accidentally starting a stream twice |

All are subclasses of `DeltaConcurrentModificationException` (Python: `delta.exceptions.*`).

### 6. Isolation levels

| | `WriteSerializable` (**default**) | `Serializable` |
|---|---|---|
| Guarantee | Writes are serializable: there is an order of *writes* consistent with the log | Writes **and** reads are serializable: as if every operation ran one at a time in history order |
| Blind INSERT + UPDATE/DELETE/MERGE at the same time | Allowed (no conflict) — the table may reach a state that "never existed" at any single version (e.g. the UPDATE didn't see the concurrently appended row) | Can conflict → UPDATE/DELETE/MERGE fails |
| Cost | Fewer conflicts | More conflicts |
| Set with | `ALTER TABLE t SET TBLPROPERTIES ('delta.isolationLevel' = 'Serializable')` | |

Reads are always **snapshot isolation**: a query sees one version throughout.

### 7. Write conflict matrix (Databricks docs)

**Without row-level concurrency** (open-source behaviour is close to this):

| | INSERT | UPDATE / DELETE / MERGE | OPTIMIZE |
|---|---|---|---|
| **INSERT** | Cannot conflict | | |
| **UPDATE / DELETE / MERGE** | WriteSerializable: cannot conflict · Serializable: can conflict | **Can conflict** (same partition/files) | |
| **OPTIMIZE** | Cannot conflict | Cannot conflict *with deletion vectors* (unless ZORDER BY); otherwise can conflict | Cannot conflict with DVs (unless ZORDER BY); otherwise can conflict |

**With row-level concurrency** (Databricks): UPDATE/DELETE/MERGE vs UPDATE/DELETE/MERGE conflict **only when they modify the same row**; OPTIMIZE conflicts only with ZORDER BY.

### 8. Row-level concurrency (Databricks-only)

| | Detail |
|---|---|
| What | Conflict detection at **row** level instead of file/partition level |
| Requirements | Databricks Runtime **14.3 LTS+**, table **not partitioned** (liquid clustering or unclustered), **deletion vectors enabled** |
| On by default? | Yes, automatically when the requirements are met |
| Limits | Complex conditions, non-deterministic expressions, some MERGE forms fall back to file-level detection; row-level checks can add commit time under heavy concurrency |

This is one of the reasons Databricks recommends liquid clustering over partitioning for tables with concurrent writers.

### 9. ShopCo example

Two jobs write `silver.orders` at 02:00:

- **Job A**: `MERGE` of web orders (`channel = 'web'`).
- **Job B**: `MERGE` of store orders (`channel = 'store'`).

Without any predicate on `channel`, both MERGEs **read the whole table** to find matches. Whoever commits second gets `ConcurrentAppendException` ("files were added to the data you read"). Fixes, in order of preference:
1. Row-level concurrency (unpartitioned + DVs + DBR 14.3+) → only same-row conflicts.
2. Make the read sets disjoint: add `AND t.channel = 'web'` to the ON condition (and partition/cluster by `channel`).
3. Serialise the jobs (one task after the other) or retry on conflict with backoff.

### 10. Misconceptions

1. **"Delta locks the table."** No locks; validation at commit.
2. **"Delta retries conflicts for me."** Only *non-conflicting* races are retried automatically. Real conflicts throw; you retry.
3. **"Two appends can conflict."** Blind appends never conflict with each other.
4. **"ACID covers my whole notebook."** Per commit, per table (unless you use newer multi-statement transactions).
5. **"Serializable is the default."** WriteSerializable is.

---

## B. Practical — read the OCC evidence and cause a conflict

**Runnable notebook:** [`exercises/phase-06/09_concurrent_writes.py`](../../exercises/phase-06/09_concurrent_writes.py) (shared with 6.9)

```python
from concurrent.futures import ThreadPoolExecutor

def upd(region, amount):                                                       # 1
    spark.sql(f"UPDATE {TGT} SET amount = {amount} WHERE region = '{region}'")

with ThreadPoolExecutor(2) as pool:                                            # 2
    futures = [pool.submit(upd, "North", 1), pool.submit(upd, "North", 2)]
for f in futures:                                                              # 3
    try: f.result(); print("committed")
    except Exception as e: print(type(e).__name__)

display(spark.sql(f"DESCRIBE HISTORY {TGT} LIMIT 3")
        .select("version", "operation", "readVersion", "isolationLevel"))       # 4
```

**Expected output (representative; concurrency makes it non-deterministic):**

```
committed
ConcurrentDeleteReadException      ← or both commit if they did not overlap in time,
                                     or if row-level concurrency resolved them (different rows)
version  operation  readVersion  isolationLevel
5        UPDATE     4            WriteSerializable
```

| # | Explanation |
|---|---|
| 1 | An UPDATE reads files matching `region='North'` and rewrites (or DV-marks) them |
| 2 | Two threads run the same kind of UPDATE at the same time from one driver — enough to race |
| 3 | The loser of a real conflict raises a `Concurrent…Exception`; the winner commits |
| 4 | `readVersion` shows which snapshot the winner read; compare it with `version` |

### Common mistakes

- Retrying with no backoff/limit and hiding the real cause (two schedules overlapping).
- Running schema evolution (`mergeSchema`) in a job that runs concurrently with others → `MetadataChangedException`.
- Expecting partition-disjoint MERGEs not to conflict while the ON clause doesn't mention the partition column.
- Catching `Exception` broadly and swallowing a conflict — the data was *not* written.

---

## C. Interview questions

### 🟢 Basic

**B1.** How does Delta provide atomicity?

<details><summary>Model answer</summary>

Data files are written first and are not part of the table until a commit references them. The commit is the atomic creation of the next numbered JSON file in `_delta_log`. Either that file exists (whole change visible) or it doesn't (nothing visible); crashed writes leave orphans that VACUUM removes.
</details>

**B2.** What is optimistic concurrency control?

<details><summary>Model answer</summary>

Writers don't lock. Each reads a snapshot, writes new files, then at commit time checks the commits that happened since its snapshot for conflicts with what it read/changed. No conflict → commit as the next version; conflict → exception and the operation must be retried.
</details>

**B3.** What is the default isolation level in Delta on Databricks?

<details><summary>Model answer</summary>

`WriteSerializable`. Writes are serializable; a blind append concurrent with an UPDATE/DELETE/MERGE doesn't conflict. `Serializable` is stricter and also makes reads serializable.
</details>

### 🟡 Medium

**M1.** Explain `ConcurrentAppendException` with an example and two fixes.

<details><summary>Model answer</summary>

Your operation read some data (e.g. MERGE scanning all of `orders`), and a concurrent commit added files to the data you read (another job appended/merged into the same partition). Your result might be wrong, so commit is refused. Fixes: make the read sets disjoint (partition/cluster on the job's key and put that predicate in the ON/WHERE clause), use row-level concurrency, serialise the jobs, or retry with backoff.
</details>

**M2.** Two daily jobs append to the same table at the same time. Will they conflict?

<details><summary>Model answer</summary>

No. Blind appends don't read the table, so there's nothing for a concurrent commit to invalidate. If one commits first, the other just retries at the next version number automatically. (Exceptions: one of them changes schema with `mergeSchema` → MetadataChangedException; both are creating the table for the first time → ProtocolChangedException.)
</details>

**M3.** What does row-level concurrency change, and what does it require?

<details><summary>Model answer</summary>

Concurrent UPDATE/DELETE/MERGE operations conflict only if they modify the same row, not merely the same file. Requires Databricks Runtime 14.3 LTS+, a non-partitioned table (liquid clustering or none) and deletion vectors enabled; it's automatic when those hold. It's Databricks-specific.
</details>

### 🔴 Tricky

**T1.** Under WriteSerializable, an UPDATE (`SET status='late' WHERE order_date < today`) and a blind INSERT of an old-dated order commit concurrently. What is the final state, and why might Serializable reject it?

<details><summary>Model answer</summary>

Both commit. The UPDATE's snapshot didn't contain the inserted row, so that old-dated row remains `status` unchanged even though it matches the predicate — a state that wouldn't arise if they ran one after the other with INSERT first. WriteSerializable accepts this (writes are still ordered); Serializable treats the INSERT as having added data the UPDATE read and fails the UPDATE with a conflict.
</details>

**T2.** "Delta retried my commit and it succeeded — so it re-ran my MERGE on the new data?" Correct this.

<details><summary>Model answer</summary>

No. Delta only re-attempts the *commit* at a higher version if the winning commits don't conflict with what it read; the files written from the old snapshot are reused as they are. If they did conflict, the operation fails — nothing is re-computed automatically. Re-running against the new snapshot is the caller's job (job retry).
</details>

**T3.** Why is a two-table update (`INSERT INTO fact`, then `UPDATE control_table`) not atomic, and how do you make the pipeline safe?

<details><summary>Model answer</summary>

Each statement is its own Delta commit on its own table; a failure between them leaves the fact table changed and the control table not. Make each step idempotent (MERGE / replaceWhere / txnAppId+txnVersion), order them so a re-run is safe (data first, watermark second), or use Databricks multi-statement transactions where available. Design for retry rather than assuming cross-table atomicity.
</details>

---

## E. Revision summary

- A = atomic commit file; C = schema/constraints/validation; I = snapshot reads + OCC; D = object storage.
- OCC: read snapshot → write files → validate against winning commits → commit next version. Non-conflicting races retried automatically; real conflicts throw.
- Exceptions: ConcurrentAppend, ConcurrentDeleteRead, ConcurrentDeleteDelete, MetadataChanged, ProtocolChanged, ConcurrentTransaction.
- Default `WriteSerializable`; blind appends never conflict.
- Row-level concurrency (Databricks): DBR 14.3+, unpartitioned, DVs → only same-row conflicts.
- Guarantees are per table, per commit.