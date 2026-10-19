# 6.4 INSERT / UPDATE / DELETE / MERGE — What Happens to Files

**Time:** 40 min · **Prerequisites:** 6.2, 6.3, 4.x (joins)

**After this topic you can:**
- explain, file by file, what each DML statement does to data files and the log;
- explain copy-on-write vs deletion vectors, and when each is used;
- explain MERGE's two phases and why it needs at most one source row per target row;
- write MERGE with `WHEN MATCHED`, `WHEN NOT MATCHED`, `WHEN NOT MATCHED BY SOURCE`;
- make a slow MERGE fast (pruning in the ON clause, dedup, file layout).

---

## A. Concept deep dive

### 1. The simplest explanation

Parquet files can't be edited. So every change is: **find the files that contain affected rows → produce new files (or a "deleted rows" bitmap) → commit `remove` + `add` actions in one atomic commit.** The only question per statement is *how many files get touched and rewritten*.

### 2. INSERT — the cheap one

| Statement | Data files | Log actions |
|---|---|---|
| `INSERT INTO` / `.mode("append")` | New files only | `add` × n (`dataChange=true`), `commitInfo.isBlindAppend = true` |
| `INSERT OVERWRITE` / `.mode("overwrite")` | New files | `remove` × all live files + `add` × n |
| `.option("replaceWhere", "order_date = '2026-10-16'")` | New files for that slice | `remove` files in the slice + `add` × n; fails if new rows fall outside the predicate |
| `INSERT INTO … REPLACE WHERE …` (SQL form, newer runtimes) | same | same |

Nothing existing is read or rewritten (except overwrite's removes, which are metadata-only).

### 3. DELETE — three possible paths

```
DELETE FROM orders WHERE <predicate>

 ┌ Predicate covers whole files (e.g. only partition columns, or stats prove every row matches)?
 │    → metadata-only: log `remove` for those files. No data read, no data written.
 ├ Deletion vectors ENABLED?
 │    → read candidate files, find row positions that match,
 │      write a deletion vector (bitmap of deleted row indexes) per touched file,
 │      log `remove` file F + `add` file F (same path) with deletionVector={…, cardinality}
 └ Otherwise (copy-on-write)
      → read candidate files, write a NEW file per touched file containing the rows that survive,
        log `remove` F + `add` F′
```

Candidate files are found first with **data skipping** (min/max stats in the log): a file whose `order_id` range is 500–900 can't contain `order_id = 105`.

### 4. UPDATE — copy-on-write vs deletion vectors

`UPDATE orders SET status = 'shipped' WHERE order_id = 102` where file A (1 GB, 8 M rows) contains 102:

| | Copy-on-write (classic) | Deletion vectors |
|---|---|---|
| Find | Scan candidate files to find which contain matches | same |
| Write | Rewrite **all 8 M rows** of A into A′ with 102 changed | Write a DV for A marking row #k deleted + write a tiny new file D with the updated 102 |
| Log | `remove` A, `add` A′ | `remove` A, `add` A (with DV), `add` D |
| Bytes written | ~1 GB | a few KB |
| Read cost afterwards | Normal | Reader loads A, drops row #k using the DV, plus reads D (slightly more work) |
| Physically gone? | Old row only in A (removed file, until VACUUM) | Old row still **inside A** until the DV is materialised (OPTIMIZE / `REORG … APPLY (PURGE)` / auto compaction) **and** VACUUM removes A |

This is called **write amplification**: changing 1 row cost a 1 GB rewrite. Deletion vectors exist to remove it — a "merge-on-read" approach.

### 5. Deletion vectors — the facts to know

| Fact | Detail |
|---|---|
| What | A compressed bitmap (RoaringBitmap) of deleted row positions for one data file |
| Where | `deletion_vector_<uuid>.bin` in the table folder (several DVs per file, by offset), or **inline** in the log when tiny |
| Descriptor in `add` | `storageType` (`u` relative/UUID, `i` inline, `p` absolute path), `pathOrInlineDv`, `offset`, `sizeInBytes`, `cardinality` (rows deleted) |
| Protocol | Table feature `deletionVectors` → reader v3 / writer v7. Older clients can't read the table (Databricks: read needs DBR 12.2 LTS+, full write support DBR 14.3 LTS+) |
| Enable | `TBLPROPERTIES ('delta.enableDeletionVectors' = true)` |
| Default on Databricks | Enabled by default for **new tables** on SQL warehouses and DBR 14.3 LTS+ (workspace/region settings can vary — check `SHOW TBLPROPERTIES`) |
| Used by | DELETE, UPDATE, MERGE (Photon also uses them for predictive I/O) |
| Materialise | `OPTIMIZE`, `REORG TABLE t APPLY (PURGE)`, auto compaction; then `VACUUM` to delete the old files |
| Why it matters for GDPR | A DV-deleted row is logically gone but physically present until purge + VACUUM |

### 6. MERGE — the internals

```sql
MERGE INTO silver.orders AS t
USING updates AS s
ON t.order_id = s.order_id
WHEN MATCHED AND s.op = 'D' THEN DELETE
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE AND t.order_date >= current_date() - 7 THEN DELETE;
```

**Phase 1 — find touched files.** Inner-join the source with the target on the ON condition (target files first pruned by partition values / stats using any target-only predicates in ON). Output: the set of target files containing at least one matching row. Here Delta also detects **multiple source rows matching one target row** → error.

**Phase 2 — rewrite.** Join the source with *only the touched files* (outer join as needed) and, for each row, apply the first `WHEN` clause whose condition is true:

| Row category | Action | Copy-on-write output | DV output |
|---|---|---|---|
| Target row matched → UPDATE | new values | in rewritten file | DV marks old row; new row in new file |
| Target row matched → DELETE | dropped | not written | DV marks it |
| Target row in a touched file, **not** matched | unchanged | **copied** into the rewritten file (`numTargetRowsCopied`) | nothing — stays in the old file |
| Source row not matched → INSERT | inserted | new file | new file |
| Target row not matched by source → UPDATE/DELETE | per clause | rewritten | DV / new file |

**Commit:** one commit with `remove` for every touched file, `add` for every new file (and DV-carrying re-adds). `operationMetrics` tells the story:

| Metric | Meaning |
|---|---|
| `numSourceRows` | rows in the source |
| `numTargetRowsUpdated` / `Inserted` / `Deleted` | logical changes |
| `numTargetRowsCopied` | unchanged rows rewritten (CoW cost — near 0 with DVs) |
| `numTargetFilesRemoved` / `numTargetFilesAdded` | file churn |
| `numTargetDeletionVectorsAdded` | DVs written (DV mode) |
| `executionTimeMs`, `scanTimeMs`, `rewriteTimeMs` | where time went |

**Special cases Delta optimises:**
- **Insert-only MERGE** (only `WHEN NOT MATCHED`): a left-anti join + append; no target files rewritten.
- **Matched-only MERGE**: inner join only.
- `WHEN NOT MATCHED BY SOURCE` needs to see **every target row** (within its condition), so without a condition it touches every file. Requires Databricks SQL / DBR 12.2 LTS+ (also in OSS Delta 2.3+).
- Databricks **low-shuffle MERGE** keeps unmodified rows of touched files out of the expensive shuffle (default on modern runtimes).

### 7. Why at most one source row per target row

If source has two rows for `order_id = 102` (`amount 100` and `amount 120`), which one wins? SQL MERGE semantics say this is ambiguous, so Delta fails with `DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE` rather than silently picking one (the result would depend on task ordering → non-deterministic). Exception: if the matched action is an **unconditional DELETE**, multiple matches are allowed (deleting twice is not ambiguous).

Fix: deduplicate the source to one row per key **before** MERGE (`row_number()` over key ordered by sequence/timestamp desc), Phase 4 and 1.7.

### 8. Clause rules

| Rule | Detail |
|---|---|
| Clause types | `WHEN MATCHED` (UPDATE / DELETE), `WHEN NOT MATCHED [BY TARGET]` (INSERT), `WHEN NOT MATCHED BY SOURCE` (UPDATE / DELETE) |
| Multiple clauses | Allowed; evaluated in order; first whose condition is true wins |
| Conditions | Within each group, every clause except the last must have a condition |
| `UPDATE SET *` / `INSERT *` | All columns by name; with schema evolution, can add new source columns (6.5) |
| Python API | `.whenMatchedUpdate(...)`, `.whenMatchedUpdateAll()`, `.whenMatchedDelete()`, `.whenNotMatchedInsertAll()`, `.whenNotMatchedBySourceDelete()`, `.whenNotMatchedBySourceUpdate(...)`, `.withSchemaEvolution()` |

### 9. Making MERGE fast

| Lever | Why |
|---|---|
| **Put target pruning predicates in ON** — `AND t.order_date >= '2026-10-01'` | Phase 1 scans fewer files; fewer touched files; fewer conflicts. Only add it if it's logically true for every match |
| Partition / liquid-cluster the target on the match key or a date | Matches concentrate in few files → small rewrite |
| Deduplicate and filter the source first | Correctness + smaller join |
| Deletion vectors on | Rewrites become DV writes (big win for few matches spread across many files) |
| Keep files reasonably sized (OPTIMIZE; `delta.tuneFileSizesForRewrites`) | Huge files → big rewrites; tiny files → big Phase-1 overhead |
| Broadcast a small source | Avoid shuffling the target |
| Avoid `NOT MATCHED BY SOURCE` without a condition on a big table | Touches all files |

### 10. ShopCo example — the slow nightly MERGE

`silver.orders` is 2 TB, 4,000 files. Nightly CDC batch: 50,000 changed orders, almost all from the last 3 days. MERGE takes 70 min; `operationMetrics` show `numTargetFilesRemoved = 3,900`, `numTargetRowsCopied = 1.9 B`. Changed order_ids are spread across all files because the table is ordered by ingestion, not by date.

Fix: liquid-cluster on `order_date`, add `AND t.order_date >= current_date() - INTERVAL 7 DAYS` to ON (business confirms updates never arrive for older orders — otherwise route those to a separate path), and keep DVs on. Result: ~40 files touched, rows copied ≈ 0, MERGE in minutes, and fewer conflicts with the concurrent late-data job.

### 11. Misconceptions

1. **"UPDATE edits the row in place."** Files are immutable; it's rewrite or DV + new file.
2. **"Deleted with DVs = gone from storage."** Physically present until purge and VACUUM.
3. **"MERGE cost depends on source size."** It depends mostly on **how many target files are touched**.
4. **"A filter in WHEN MATCHED prunes files."** No — pruning comes from the ON condition (and target-only predicates in it); WHEN conditions are applied after the join.
5. **"Multiple source matches: last one wins."** It errors (unless unconditional delete).

---

## B. Practical — MERGE patterns and their file effects

**Runnable notebooks:** [`exercises/phase-06/04_merge_patterns.py`](../../exercises/phase-06/04_merge_patterns.py) and the log trace in [`01_trace_delta_log.py`](../../exercises/phase-06/01_trace_delta_log.py) (steps 3–5).

### Input

Target `silver_orders`:

| order_id | customer_id | amount | status | order_date |
|---|---|---|---|---|
| 101 | C1 | 250.0 | placed | 2026-10-14 |
| 102 | C2 | 100.0 | placed | 2026-10-15 |
| 103 | C3 | 40.0 | placed | 2026-10-15 |
| 104 | C1 | 75.5 | placed | 2026-10-16 |

Source batch (`op`: U=upsert, D=delete), deduplicated:

| order_id | customer_id | amount | status | order_date | op |
|---|---|---|---|---|---|
| 102 | C2 | 100.0 | shipped | 2026-10-15 | U |
| 103 | C3 | 40.0 | cancelled | 2026-10-15 | D |
| 105 | C4 | 300.0 | placed | 2026-10-16 | U |

### Code

```python
from delta.tables import DeltaTable

(DeltaTable.forName(spark, TGT).alias("t")
   .merge(src.alias("s"),
          "t.order_id = s.order_id AND t.order_date >= '2026-10-01'")     # 1
   .whenMatchedDelete(condition="s.op = 'D'")                              # 2
   .whenMatchedUpdate(condition="s.op = 'U'",                              # 3
                      set={"amount": "s.amount", "status": "s.status"})
   .whenNotMatchedInsert(condition="s.op = 'U'",                           # 4
                         values={c: f"s.{c}" for c in TARGET_COLS})
   .execute())

display(spark.sql(f"DESCRIBE HISTORY {TGT} LIMIT 1")
        .select("operation", "operationMetrics"))                          # 5
```

| # | Explanation |
|---|---|
| 1 | Key match + a target-pruning predicate (only valid because no change is older than 1 Oct) |
| 2 | First matching clause wins: deletes come first |
| 3 | Updates only the listed columns (not `SET *`, so `op` is not needed in the target) |
| 4 | Inserts new keys; ignores deletes for keys that don't exist |
| 5 | Check the metrics, not just the data |

### Expected output

| order_id | amount | status |
|---|---|---|
| 101 | 250.0 | placed |
| 102 | 100.0 | shipped |
| 104 | 75.5 | placed |
| 105 | 300.0 | placed |

`operationMetrics` (representative, DV mode): `numSourceRows=3, numTargetRowsUpdated=1, numTargetRowsDeleted=1, numTargetRowsInserted=1, numTargetRowsCopied=0, numTargetDeletionVectorsAdded=1, numTargetFilesAdded=1`. In copy-on-write mode `numTargetRowsCopied` > 0 and `numTargetFilesRemoved` ≥ 1.

### Common mistakes

- Not deduplicating the source → `DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE`.
- `UPDATE SET *` when the source has extra helper columns (`op`, `_rn`) → error, or with schema evolution on, the helper columns get added to the target.
- Putting a pruning predicate in ON that isn't always true → genuine matches become "not matched" → **duplicate inserts**.
- Using `NOT MATCHED BY SOURCE ... DELETE` with a partial (incremental) source → deletes everything not in today's batch.
- Forgetting that a MERGE with no matches still scans the target (Phase 1).

---

## C. Interview questions

### 🟢 Basic

**B1.** What happens to data files when you UPDATE one row in a Delta table (no deletion vectors)?

<details><summary>Model answer</summary>

Delta finds candidate files using stats, scans them to find the ones containing the row, rewrites each such file completely with the new value, and commits `remove` (old file) + `add` (new file). The old file stays on disk for time travel until VACUUM.
</details>

**B2.** What is a deletion vector?

<details><summary>Model answer</summary>

A bitmap of row positions in a data file that are logically deleted. With DVs, DELETE/UPDATE/MERGE mark rows instead of rewriting files; the `add` action for the file carries a DV descriptor and readers filter those rows. DVs are materialised by OPTIMIZE / REORG PURGE / auto compaction.
</details>

**B3.** What are the three kinds of `WHEN` clauses in MERGE?

<details><summary>Model answer</summary>

`WHEN MATCHED` (UPDATE or DELETE target rows that matched), `WHEN NOT MATCHED [BY TARGET]` (INSERT source rows with no target match), and `WHEN NOT MATCHED BY SOURCE` (UPDATE or DELETE target rows with no source match; DBR 12.2+). Multiple of each allowed; first true condition wins; all but the last in a group need a condition.
</details>

### 🟡 Medium

**M1.** Explain MERGE internally in two phases.

<details><summary>Model answer</summary>

Phase 1: inner join source and target on the ON condition (with file pruning) to find which target files contain matches, and check for multiple source matches. Phase 2: join the source with only those touched files, apply the WHEN clauses to produce updated/deleted/copied/inserted rows, write new files (or DVs + new files), and commit removes of touched files and adds of new files in one commit.
</details>

**M2.** Why does MERGE fail when two source rows match one target row, and when is it allowed?

<details><summary>Model answer</summary>

Applying two different updates to one row is ambiguous and would be non-deterministic, so Delta errors (`DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE`). It's allowed when the matched action is an unconditional DELETE. Fix by deduplicating the source to the latest row per key.
</details>

**M3.** When are deletion vectors a bad fit, and what do you do about it?

<details><summary>Model answer</summary>

Readers pay to apply DVs, so tables with many accumulated DVs read slower; old engines/clients that don't support the `deletionVectors` feature can't read the table; and compliance deletes aren't physical until purge + VACUUM. Run OPTIMIZE/predictive optimization regularly, `REORG TABLE … APPLY (PURGE)` for GDPR, and keep DVs off for tables shared with readers that lack support.
</details>

### 🔴 Tricky

**T1.** A MERGE of 1,000 source rows into a 5 TB table rewrites 4 TB. Why, and how do you fix it?

<details><summary>Model answer</summary>

Cost depends on touched files, not source size. The 1,000 keys are spread across almost every file (no layout by the key/date, huge files), so CoW rewrites nearly all of them (`numTargetRowsCopied` huge). Fixes: enable deletion vectors; cluster/partition by the key or date the changes concentrate on; add a valid target predicate in ON; tune file size for rewrites; dedupe/filter source.
</details>

**T2.** A colleague adds `AND t.order_date = s.order_date` to the ON clause to speed up MERGE. After a week, duplicates appear. Why?

<details><summary>Model answer</summary>

If an order's `order_date` can change (correction) or differs between source and target, the key no longer matches, the row is "not matched" and gets inserted as a new row → duplicate order_id. Pruning predicates in ON must be logically implied by the true match; prefer a constant range on the target (`t.order_date >= X`) that is guaranteed to cover all matches.
</details>

**T3.** A GDPR request: delete customer C9. You run `DELETE` and the auditor finds C9's data still in storage two weeks later. Explain.

<details><summary>Model answer</summary>

With deletion vectors the rows are only marked deleted inside the existing files; without DVs the old files are tombstoned but remain for time travel. Physical removal needs: `REORG TABLE … APPLY (PURGE)` (or OPTIMIZE) to rewrite files without DV rows, then `VACUUM` after the retention period (or a deliberately shorter retention) to delete the old files — also consider CDF `_change_data` files and any clones/copies.
</details>

---

## E. Revision summary

- Files immutable → DML = find touched files → rewrite (CoW) or DV-mark → `remove` + `add` in one commit.
- INSERT = add only; overwrite/replaceWhere = remove slice + add; DELETE can be metadata-only for whole files.
- DVs: bitmap of deleted rows, reader v3/writer v7, default for new tables on DBR 14.3+/SQL warehouses; materialise with OPTIMIZE/REORG PURGE; then VACUUM.
- MERGE: phase 1 join to find touched files (+ multi-match check), phase 2 rewrite touched files + inserts, one commit. Cost ∝ touched files.
- ≤ 1 source row per target row (unless unconditional DELETE) → dedupe first.
- Speed: pruning predicate in ON, layout on key/date, DVs, sane file sizes, insert-only when possible.