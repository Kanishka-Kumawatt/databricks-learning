# 9.1 What CDC Is — Log-Based, Query-Based and Snapshot Comparison

**Time:** 15 min · **Prerequisites:** 1.7, 1.8

**After this topic you can:**
- define change data capture (CDC) and explain why it exists;
- explain how log-based CDC works internally and name real tools (SQL Server CDC / Change Tracking, Debezium, Lakeflow Connect);
- compare log-based, query-based (timestamp) and snapshot-comparison CDC — especially on **deletes** and **intermediate changes**;
- implement a snapshot diff with hashes;
- separate "CDC into Databricks" from Delta's **Change Data Feed** (CDC *out of* a Delta table).

---

## A. Concept deep dive

### 1. The simplest explanation

> **Change data capture (CDC) = recording every insert, update and delete that happens in a source system, as a stream of change events, so another system can apply the same changes.**

Instead of copying the whole `customers` table every night, you receive messages like:

| op | customer_id | city | seq |
|---|---|---|---|
| U | C1 | Abu Dhabi | 10 |
| D | C4 | | 15 |
| I | C6 | Dubai | 16 |

and apply them to your copy.

### 2. Why CDC exists

Recall the blind spots of a timestamp watermark load from 1.7: **hard deletes are invisible**, rows whose `updated_at` isn't maintained are missed, intermediate changes between two runs are lost, and every extract queries the production database. CDC fixes these:

| Need | Full load | Timestamp incremental | CDC |
|---|---|---|---|
| Cost proportional to change volume | ❌ | ✅ | ✅ |
| Sees hard deletes | ✅ (row disappears) | ❌ | ✅ (D event) |
| Sees every intermediate change (for SCD2 / audit) | ❌ | ❌ only latest state per run | ✅ (log-based) |
| Load on the source database | High | Medium (queries) | Low (reads the log) |
| Ordering information | — | timestamp only | sequence (LSN) |

### 3. The three families

| Family | How it detects change | Deletes? | Intermediate changes? | Source impact | Needs |
|---|---|---|---|---|---|
| **Log-based** | Reads the database **transaction log** (every committed change, in commit order) | ✅ | ✅ | Low | CDC enabled on the DB, permissions, a capture tool, log retention |
| **Query-based** (timestamp / version column) | `SELECT … WHERE updated_at > :watermark` | ❌ (unless soft delete) | ❌ | Medium | Reliable `updated_at` maintained on every change |
| **Snapshot comparison** (full diff) | Compare today's full extract with yesterday's (key + hash of columns) | ✅ (key missing) | ❌ | High (full extracts), compute-heavy | Full extracts, storage for the previous snapshot |

Also seen: **trigger-based** CDC (DB triggers write to an audit table) — complete but adds write latency to every source transaction; mostly legacy.

### 4. How log-based CDC works internally

Every relational database writes each change to a **transaction log** (write-ahead log, redo log, binlog) *before* it changes the data files — that's how it recovers after a crash. Each log record has a position: **LSN** (log sequence number, SQL Server/Postgres), SCN (Oracle), binlog file+position/GTID (MySQL). Positions increase in **commit order**.

```
 app: UPDATE customers SET city='Abu Dhabi' WHERE id='C1'
        │
        ▼
 ┌──────────────────────────┐   capture process reads the log   ┌────────────────────┐
 │ transaction log          │ ────────────────────────────────▶ │ change events      │
 │ LSN 0x…10  UPDATE C1 …   │   (no query on the tables)        │ op, before, after, │
 │ LSN 0x…15  DELETE C4     │                                   │ LSN, commit time   │
 └──────────────────────────┘                                   └─────────┬──────────┘
                                                                          ▼
                                             Kafka / files / Delta Bronze → MERGE / AUTO CDC
```

Real tools (know the names, and what they produce):

| Tool | What it is | What you receive |
|---|---|---|
| **SQL Server Change Data Capture (CDC)** | A capture job reads the log and writes **change tables** (`cdc.<schema>_<table>_CT`) | Full row values + `__$start_lsn`, `__$seqval`, `__$operation` (1 = delete, 2 = insert, 3 = update *before* image, 4 = update *after* image) |
| **SQL Server Change Tracking** | Lightweight: records **which keys changed** and a version number — **not the values** | Key + operation + version; you join back to the table for current values (intermediate values are lost) |
| **Debezium** | Open-source log-based CDC (Kafka Connect) for Postgres, MySQL, SQL Server, Oracle, … | JSON/Avro envelope with `before`, `after`, `op` (`c` create, `u` update, `d` delete, `r` snapshot read), `source` (LSN, txn), `ts_ms` |
| **Oracle GoldenGate, AWS DMS, Fivetran, Qlik** | Commercial/managed CDC | Similar change records |
| **Lakeflow Connect** (Databricks) | Managed ingestion connectors; the SQL Server connector uses the database's change tracking or CDC, with an ingestion gateway + ingestion pipeline | Lands the changes and applies them to streaming tables (SCD1 or SCD2) for you |

Databricks-specific, don't confuse:
- **Lakeflow Connect** = CDC **into** Databricks from an external DB.
- **AUTO CDC** (9.7) = **applies** a CDC feed to a target table in a declarative pipeline.
- **Delta Change Data Feed (CDF)** = CDC **out of** a Delta table (row-level changes between table versions) — used to propagate Silver → Gold incrementally (9.6, notebook 02).

### 5. ShopCo example — what each method sees

Between two nightly runs, customer C1 (Asha) moves Dubai → Abu Dhabi at 09:00, then upgrades tier plus → premium at 15:00; customer C4 is hard-deleted.

**Source at run 1 (before):**

| customer_id | city | tier | updated_at |
|---|---|---|---|
| C1 | Dubai | plus | 10-01 00:00 |
| C4 | Sharjah | plus | 10-01 00:00 |

**Source at run 2 (after):**

| customer_id | city | tier | updated_at |
|---|---|---|---|
| C1 | Abu Dhabi | premium | 10-02 15:00 |

**What arrives:**

| Method | Events received for C1 | For C4 |
|---|---|---|
| Log-based | `U seq 10 (Abu Dhabi, plus)`, `U seq 11 (Abu Dhabi, premium)` | `D seq 12` |
| Change Tracking | "C1 changed (version 11)" → you read current row | "C4 deleted" |
| Query-based | one row: (Abu Dhabi, premium) | **nothing** — C4 stays in the target forever |
| Snapshot diff | `U` (Abu Dhabi, premium) | `D` (key missing) |

Consequence for SCD2: only log-based CDC can build the history "Dubai → Abu Dhabi/plus → Abu Dhabi/premium". The others only know "Dubai/plus → Abu Dhabi/premium".

### 6. Snapshot comparison internals

1. Keep yesterday's snapshot (or the current Silver table).
2. For each row compute `sha2(concat_ws('||', coalesce(col1,'<null>'), …))` over the non-key columns.
3. `FULL OUTER JOIN` old and new on the key:
   - key only in new → **I**; key only in old → **D**; in both with different hash → **U**; same hash → unchanged.

Hash instead of comparing every column: one comparison, works for wide tables, and the hash can be stored. `COALESCE` matters: `concat_ws` **skips** NULLs, so `('a', NULL, 'b')` and `('a', 'b', NULL)` would otherwise produce the same string.

### 7. When to use which

| Situation | Choice |
|---|---|
| OLTP database you're allowed to enable CDC on; need deletes and history | Log-based (Lakeflow Connect, Debezium, SQL Server CDC) |
| Need deletes and current state only, lightweight on SQL Server | Change Tracking |
| SaaS API with `modified_since` filter, soft deletes | Query-based |
| Legacy system / partner sends only full files | Snapshot comparison (or `create_auto_cdc_from_snapshot_flow`, 9.7) |
| Small table | Just full load (1.7) |

### 8. Misconceptions

1. **"CDC is a Databricks feature."** CDC is a universal pattern; Databricks has features that **produce** (Lakeflow Connect), **consume** (AUTO CDC) or **expose** (Delta CDF) changes.
2. **"Query-based incremental is CDC, so it handles deletes."** It doesn't see hard deletes.
3. **"Change Tracking gives me history."** It gives keys that changed, not every intermediate value.
4. **"Log-based CDC delivers each event exactly once and in order."** Most pipelines are **at-least-once**; duplicates and re-ordering across partitions happen (9.3).
5. **"Delta CDF is the same as CDC from the source."** CDF describes changes to a **Delta table**; it is not connected to the upstream database.

---

## B. Practical — snapshot diff with hashes

**Runnable notebook:** [`exercises/phase-09/04_snapshot_diff.py`](../../exercises/phase-09/04_snapshot_diff.py)

**Input**

| snap_customer_day1 | | | | snap_customer_day2 | | | |
|---|---|---|---|---|---|---|---|
| customer_id | name | city | tier | customer_id | name | city | tier |
| C1 | Asha Rao | Dubai | plus | C1 | Asha Rao | **Abu Dhabi** | plus |
| C2 | Ben Ali | Abu Dhabi | basic | C2 | Ben Ali | Abu Dhabi | basic |
| C3 | Chen Wu | Dubai | premium | **C4** | Dana Okafor | Sharjah | plus |

```sql
WITH o AS (SELECT customer_id, sha2(concat_ws('||', coalesce(name,'<null>'), coalesce(city,'<null>'),
                                              coalesce(tier,'<null>')), 256) AS h FROM snap_customer_day1),
     n AS (SELECT customer_id, sha2(concat_ws('||', coalesce(name,'<null>'), coalesce(city,'<null>'),
                                              coalesce(tier,'<null>')), 256) AS h FROM snap_customer_day2)
SELECT COALESCE(n.customer_id, o.customer_id) AS customer_id,
       CASE WHEN o.customer_id IS NULL THEN 'I'
            WHEN n.customer_id IS NULL THEN 'D'
            ELSE 'U' END AS op
FROM o FULL OUTER JOIN n ON o.customer_id = n.customer_id
WHERE o.customer_id IS NULL OR n.customer_id IS NULL OR o.h <> n.h
```

| Line | Explanation |
|---|---|
| `sha2(concat_ws('||', coalesce(...)))` | One fingerprint per row over the non-key columns; `<null>` placeholder keeps NULL positions distinct |
| `FULL OUTER JOIN … ON key` | Keeps keys present on either side — needed to see both inserts (right only) and deletes (left only) |
| `CASE` | Left side missing → insert; right side missing → delete; both present → update (the `WHERE` already removed unchanged rows) |
| `WHERE … o.h <> n.h` | Emit only real changes |

**Expected output**

| customer_id | op |
|---|---|
| C1 | U |
| C3 | D |
| C4 | I |

The notebook then applies day 2 to a target in **one** MERGE with `WHEN NOT MATCHED BY SOURCE THEN DELETE`:

| Target before | → | Target after |
|---|---|---|
| C1 Dubai plus · C2 Abu Dhabi basic · C3 Dubai premium | | C1 **Abu Dhabi** plus · C2 Abu Dhabi basic · **C4** Sharjah plus |

`operationMetrics`: 1 updated, 1 inserted, 1 deleted.

### Common mistakes

- `concat_ws` without `coalesce` → different rows produce identical hashes.
- Inner join instead of full outer → inserts and deletes vanish.
- `WHEN NOT MATCHED BY SOURCE THEN DELETE` on a **partial** extract → deletes every row not in the extract. Use it only with a complete snapshot, or add a condition that limits it to the extracted range.
- Comparing with `=` on nullable columns: `NULL = NULL` is NULL → "changed" every time. Use `<=>` or hashes.

---

## C. Interview questions

### 🟢 Basic

**B1.** What is CDC?

<details><summary>Model answer</summary>

Change data capture records the inserts, updates and deletes that happen in a source system as an ordered stream of change events (with the operation, the new and often old values, and a sequence/position), so downstream systems can apply the same changes incrementally instead of reloading everything.
</details>

**B2.** Why is log-based CDC preferred over timestamp-based incremental extraction?

<details><summary>Model answer</summary>

It reads the transaction log, so it sees every committed change including hard deletes and intermediate updates, in commit order (LSN), without depending on an application-maintained `updated_at`, and with low load on the source tables. Timestamp extraction misses deletes, misses changes that don't update the timestamp, and only sees the latest state per run.
</details>

**B3.** How does a snapshot comparison detect deletes?

<details><summary>Model answer</summary>

By full outer joining the previous and current full snapshot on the key: a key present in the old snapshot but absent from the new one is a delete. New-only keys are inserts; keys in both with different column hashes are updates.
</details>

### 🟡 Medium

**M1.** What's the difference between SQL Server Change Data Capture and Change Tracking?

<details><summary>Model answer</summary>

CDC reads the transaction log asynchronously and stores every change with full column values (before and after images for updates) plus LSN and operation code in change tables — enough to build history. Change Tracking only records which primary keys changed and a version number, without values; you join back to the table for the current values, so intermediate changes are lost. CT is lighter; CDC is richer.
</details>

**M2.** A partner sends a full customer file daily, with no change indicators. Design the CDC.

<details><summary>Model answer</summary>

Land each file in Bronze (append, with file name and load date). Compute a hash of non-key columns per row, full-outer-join today's snapshot with the previous one (or with Silver) on the key, classify I/U/D, and apply with MERGE (or one MERGE with `WHEN NOT MATCHED BY SOURCE THEN DELETE` because the file is complete). Use the file date/sequence as the ordering value for SCD2 effective dates. In a declarative pipeline `create_auto_cdc_from_snapshot_flow` does the comparison for you. Guard against a truncated/partial file — it would look like mass deletes — with a row-count threshold check before applying.
</details>

**M3.** Is Delta Change Data Feed a CDC tool for your source database?

<details><summary>Model answer</summary>

No. CDF records row-level changes made to a Delta table (insert, update_preimage, update_postimage, delete per commit version). It's used to propagate changes from one Delta table to the next (Silver → Gold) or to external consumers. Capturing changes from an upstream database needs a source-side CDC mechanism (Lakeflow Connect, Debezium, SQL Server CDC, etc.).
</details>

### 🔴 Tricky

**T1.** Your snapshot-diff job suddenly reports 95 % of customers as deleted. What happened and how do you protect against it?

<details><summary>Model answer</summary>

Most likely a partial or truncated extract (failed export, wrong filter, empty file), not real deletes. Protect with pre-apply checks: row count versus previous snapshot within a threshold, file completeness markers/trailer counts, and a maximum delete ratio that fails the run. Applying it blindly would delete (or close in SCD2) most of the dimension; if it happened, roll back with Delta time travel / RESTORE and replay.
</details>

**T2.** You use Change Tracking and want SCD2 history with every change. Is that possible?

<details><summary>Model answer</summary>

Not fully. Change Tracking only tells you that a key changed since a version; you read the current values at extraction time, so several changes between two syncs collapse into one. You get "history at sync granularity", not per change. For per-change history you need log-based CDC with values (SQL Server CDC, Debezium) — or accept the coarser history and document it.
</details>

**T3.** Why can't you simply compare `updated_at` between two snapshots instead of hashing?

<details><summary>Model answer</summary>

`updated_at` may not be maintained by every write path (bulk scripts, bugs), may change without any business column changing (touch updates → false new versions), and can't tell you which columns changed. Hashing the actual tracked columns detects real value changes only. Many teams use both: `updated_at` to limit the scan, hash to decide whether something really changed.
</details>

---

## E. Revision summary

- CDC = stream of I/U/D events with ordering, applied downstream.
- **Log-based** (transaction log, LSN): deletes ✅, intermediate changes ✅, low source load. Tools: SQL Server CDC (values, `__$operation` 1–4), Change Tracking (keys only), Debezium (`before`/`after`/`op` c,u,d,r), Lakeflow Connect.
- **Query-based**: simple, misses hard deletes and intermediate changes.
- **Snapshot diff**: full outer join + hash; catches deletes; expensive; guard against partial files.
- Databricks: Lakeflow Connect produces CDC, AUTO CDC applies it, Delta CDF exposes changes of a Delta table.