# 10.6 Watermark-Based Incremental Ingestion and CDC Patterns in ADF

**Time:** 10 min · **Prerequisites:** 1.7 (full vs incremental), 10.3, 10.5

**After this topic you can:**
- build the classic ADF watermark pattern (two Lookups → bounded Copy → stored procedure) and explain each step;
- explain why the window is bounded by a *new* watermark read before the copy;
- describe ADF's CDC options: SQL Server Change Tracking with Copy, mapping data flow CDC, and the native Change Data Capture resource (preview);
- say when to move CDC ingestion to Databricks (Lakeflow Connect) instead.

---

## A. Concept deep dive

### 1. The simplest explanation

Remember phase 1.7: *read watermark → extract rows changed after it → write → only then move the watermark*. In ADF each of those is an activity:

```
 LookupOldWatermark ─┐
                     ├─▶ CopyChangedRows ─▶ (Databricks MERGE, 10.8) ─▶ UpdateWatermark (stored procedure)
 LookupNewWatermark ─┘     WHERE updated_at >  old
                                 AND updated_at <= new
```

### 2. Why two Lookups (old and new)?

| Approach | Problem |
|---|---|
| `WHERE updated_at > old` and afterwards set watermark = `utcnow()` | Rows committed during the copy with earlier timestamps are skipped forever (1.7) |
| `WHERE updated_at > old`, afterwards set watermark = `MAX(updated_at)` *queried again* | Rows inserted between the copy and the second MAX query are skipped |
| **Read `new = MAX(updated_at)` *before* the copy; copy `> old AND <= new`; store `new`** | ✅ The window is closed and pinned; the stored value is exactly the upper bound of what was copied. Rows arriving later have `updated_at > new` and go to the next run |

This is the pattern Microsoft documents for incremental copy from a database with a watermark table. Add a small **lookback** (`> DATEADD(minute, -10, old)`) when the source can commit late; it's safe because the downstream write is a **MERGE** (re-processing the overlap creates no duplicates).

### 3. The control table and the stored procedure

```
 etl.watermark
 ┌──────────────┬──────────────────────────┬──────────────────────┬─────────────┬──────────────┐
 │ table_name   │ last_value (datetime2)   │ last_run_id          │ rows_loaded │ updated_at   │
 ├──────────────┼──────────────────────────┼──────────────────────┼─────────────┼──────────────┤
 │ dbo.orders   │ 2026-10-17 23:59:41.1234 │ 7f3c…                │ 18 422      │ 2026-10-18…  │
 └──────────────┴──────────────────────────┴──────────────────────┴─────────────┴──────────────┘
```

- `UpdateWatermark` is a **Stored Procedure activity** (`SqlServerStoredProcedure` type) that runs only if the Databricks step **Succeeded**.
- The procedure should **never move the watermark backwards** (`WHERE last_value < @NewValue`) — protects against an old rerun overwriting a newer value.
- Store `last_run_id` and row counts for audit/reconciliation.
- The factory's managed identity needs `EXECUTE` on the procedure and `SELECT` on the source.

### 4. Edge cases

| Case | Handling |
|---|---|
| **First run** (no row in control table) | Seed with `'1900-01-01'` → first run is the full initial load (chunk it for big tables) |
| **No changes** | Copy writes 0 rows; `new = old`; notebook gets an empty folder (or skip it with If on `rowsCopied`); watermark unchanged |
| **Empty source table** | `MAX()` returns NULL → `COALESCE(MAX(updated_at), old)` |
| **Deletes** | Invisible to a timestamp watermark → soft deletes, Change Tracking/CDC, or periodic key reconciliation (1.7) |
| **Time zones / datetime formatting** | Return watermarks as `CONVERT(varchar(27), x, 121)` strings ('yyyy-mm-dd hh:mi:ss.fffffff') so ADF doesn't reformat them; compare in the source's time basis |
| **Overlapping runs** | Pipeline `concurrency: 1` — two runs reading the same old watermark is a race |

### 5. CDC patterns available in ADF

| Option | How it works | Captures deletes? | Notes |
|---|---|---|---|
| **Timestamp watermark** (above) | `updated_at` window | ❌ | Simplest; needs a reliable `updated_at` |
| **SQL Server / Azure SQL Change Tracking + Copy** | Store last `SYS_CHANGE_VERSION`; Lookup `CHANGE_TRACKING_CURRENT_VERSION()`; Copy query joins `CHANGETABLE(CHANGES dbo.orders, @last_version)` to the table; save the new version | ✅ (`SYS_CHANGE_OPERATION = 'D'`, key only) | Lightweight (no full history, only "latest net change" per key); retention must exceed the gap between runs, else full reload. Documented ADF tutorial pattern |
| **SQL Server CDC (`cdc.fn_cdc_get_all_changes_…`) + Copy** | Copy queries the CDC functions between LSNs | ✅ with before/after images | More detail than Change Tracking; LSN bookkeeping in the control table |
| **Mapping Data Flow source with "Enable change data capture"** | Data flow source reads only changes since the last run (checkpoint kept by ADF) | depends on source (CDC/CT enabled) | Runs on ADF Spark; checkpoint tied to pipeline/activity name |
| **ADF Change Data Capture resource** | Top-level "Change Data Capture" object: continuous, near-real-time, guided mapping (sources incl. Azure SQL, SQL Server, Cosmos DB, files; targets incl. Delta, Parquet, Azure SQL) | ✅ for DB sources | **Public preview** (per Microsoft docs); no self-hosted IR; billed as 4 vCores of General Purpose data flow while running |

**Databricks alternative:** Lakeflow Connect's managed SQL Server connector (CDC or Change Tracking, with an ingestion gateway) lands changes directly into Unity Catalog streaming tables — then ADF isn't needed for that source at all (10.9). Phase 9 covers applying the changes (MERGE, AUTO CDC, SCD).

### 6. ShopCo example

`dbo.orders` in Azure SQL has `updated_at` maintained by a trigger and soft-delete flag `is_deleted`. ShopCo uses the timestamp watermark nightly (cheap, simple) and a weekly key reconciliation for hard deletes. For `dbo.payments`, where rows are hard-deleted, the DBA enabled **Change Tracking** and the pipeline copies `SYS_CHANGE_OPERATION` with the rows so Databricks can apply deletes in the MERGE.

### 7. Misconceptions

1. **"Update the watermark right after Copy."** If Databricks then fails, the next run skips the window — those rows are only in landing and never reach Silver. Update after the **transformation** succeeded.
2. **"Watermark = when the pipeline ran."** It's the max value of the *data* processed (the pinned `new` value).
3. **"Change Tracking gives full history."** It gives the net latest change per key since a version; use CDC for every intermediate change.
4. **"The ADF CDC resource is GA and works with on-prem through SHIR."** Per the docs it's in preview and doesn't support SHIR — verify the current status before proposing it.

---

## B. Practical — control table, procedure, Lookups

**File:** [`exercises/phase-10/06_watermark_control.sql`](../../exercises/phase-10/06_watermark_control.sql) (T-SQL for Azure SQL / SQL Server): control table, seed row, `etl.usp_update_watermark`, source table, ADF managed-identity user, and a Change Tracking variant.

The two Lookups (full pipeline in 10.8):

```json
{
  "name": "LookupOldWatermark",
  "type": "Lookup",
  "typeProperties": {
    "source": {
      "type": "AzureSqlSource",
      "sqlReaderQuery": "SELECT CONVERT(varchar(27), last_value, 121) AS last_value FROM etl.watermark WHERE table_name = 'dbo.orders'"
    },
    "dataset": { "referenceName": "DS_AzureSql_Query", "type": "DatasetReference" },
    "firstRowOnly": true
  }
},
{
  "name": "LookupNewWatermark",
  "type": "Lookup",
  "typeProperties": {
    "source": {
      "type": "AzureSqlSource",
      "sqlReaderQuery": "SELECT CONVERT(varchar(27), COALESCE(MAX(updated_at), CAST('1900-01-01' AS datetime2)), 121) AS new_value FROM dbo.orders"
    },
    "dataset": { "referenceName": "DS_AzureSql_Query", "type": "DatasetReference" },
    "firstRowOnly": true
  }
}
```

The Copy query then uses both:

```sql
SELECT order_id, customer_id, order_status, amount, currency, is_deleted, created_at, updated_at
FROM dbo.orders
WHERE updated_at >  CAST('@{activity('LookupOldWatermark').output.firstRow.last_value}' AS datetime2)
  AND updated_at <= CAST('@{activity('LookupNewWatermark').output.firstRow.new_value}' AS datetime2)
```

| Part | Explanation |
|---|---|
| `CONVERT(varchar(27), …, 121)` | Watermark travels through ADF as an exact string — no JSON date reformatting, no lost fractional seconds |
| Two Lookups have no dependency between them | They run **in parallel**; Copy depends on both (AND) |
| `> old AND <= new` | Half-open window: each row belongs to exactly one run |
| `COALESCE(MAX(...), '1900-01-01')` | Empty table doesn't produce NULL |

### Common mistakes

- Comparing against an ISO string with `T`/`Z` that the source column type interprets differently (time zone confusion).
- Stored procedure parameter types (`datetime` vs `datetime2`) truncating precision → the next run re-reads or skips boundary rows.
- Lookup on a view that does `SELECT MAX(updated_at)` over a huge table without an index on `updated_at` — slow and locks. Index the watermark column.

---

## C. Interview questions

### 🟢 Basic

**B1.** Describe the standard ADF watermark pipeline.

<details><summary>Model answer</summary>

Lookup the old watermark from a control table; Lookup the new watermark (`MAX(updated_at)` from the source) before copying; Copy rows with `updated_at > old AND <= new` to the lake; process them (Databricks MERGE); then a Stored Procedure activity updates the control table to the new watermark — only on success.
</details>

**B2.** What happens on the first run when the control table has no watermark?

<details><summary>Model answer</summary>

Seed it with a minimum date (e.g. 1900-01-01) so the first run extracts everything — the initial full load. For big tables, chunk the initial load (by month or ID range) and then switch to incremental.
</details>

**B3.** What CDC options does ADF offer for SQL Server sources?

<details><summary>Model answer</summary>

Copy with Change Tracking (`CHANGETABLE` + stored version), Copy with SQL CDC functions (LSN ranges), mapping data flow sources with "enable change data capture", and the native Change Data Capture resource (public preview). Timestamp watermarks are the non-CDC option.
</details>

### 🟡 Medium

**M1.** Why read the new watermark *before* the copy rather than computing `MAX(updated_at)` after it?

<details><summary>Model answer</summary>

To pin a closed window. If you compute MAX after the copy, rows committed between the copy and the MAX query get a timestamp ≤ the stored watermark although they weren't copied → lost. Reading `new` first and copying `<= new` guarantees the stored watermark equals the upper bound of the copied data.
</details>

**M2.** Why is the watermark updated after the Databricks step and not right after the Copy?

<details><summary>Model answer</summary>

The data isn't "processed" until it's in Silver. If the watermark moved after Copy and the notebook failed, the next run would start after the window, and those rows would never be merged (they'd sit unprocessed in landing). Updating after the notebook succeeds means a failure leaves the old watermark and the next run reprocesses the window; MERGE makes that safe.
</details>

**M3.** Change Tracking vs CDC in SQL Server — which would you choose for ingestion into Delta?

<details><summary>Model answer</summary>

Change Tracking if you only need the latest state per key plus deletes (net changes) — lighter on the source and simpler; you join to the base table for current values. CDC if you need every intermediate change with before/after values (audit, SCD2 per change, exact ordering). Both require retention longer than the maximum gap between runs.
</details>

### 🔴 Tricky

**T1.** Your nightly load runs at 02:00 and also gets triggered manually at 02:05 by an operator. What can go wrong and how do you prevent it?

<details><summary>Model answer</summary>

Both runs read the same old watermark and copy overlapping windows; whichever finishes last writes its watermark — possibly the *smaller* one, moving it backwards, or both MERGE the same rows concurrently (Delta write conflicts). Prevent: pipeline `concurrency: 1`, a stored procedure that never moves the watermark backwards, idempotent MERGE, and run-specific landing folders.
</details>

**T2.** Change Tracking retention is 2 days; the pipeline was broken for 3 days. What happens and what do you do?

<details><summary>Model answer</summary>

The stored `last_sync_version` is older than `CHANGE_TRACKING_MIN_VALID_VERSION(OBJECT_ID('dbo.orders'))`, so changes are no longer available — reading them gives incomplete results. The pipeline must detect this (compare versions in the Lookup) and do a full reload (or a snapshot comparison), then reset the version. Also: increase retention to cover realistic outages and alert on staleness.
</details>

**T3.** The source's `updated_at` is set by the application in local time (with DST). What problems appear, and what's your fix?

<details><summary>Model answer</summary>

During the DST fall-back hour timestamps repeat → rows can fall before an already stored watermark and be missed; comparisons with UTC values from ADF expressions are off by hours. Fix: compare in the source's own time basis using values read *from the source* (old/new watermarks from SQL, not `utcnow()`), add a lookback window ≥ 1 hour with MERGE, and push for a UTC or database-generated rowversion/Change Tracking column.
</details>

---

## E. Revision summary

- Pattern: **Lookup old** + **Lookup new (before copy)** → **Copy `> old AND <= new`** → **Databricks MERGE** → **Stored proc update (on success only)**.
- Watermark = **pinned upper bound of copied data**; never `utcnow()`; never backwards; store run ID + counts.
- Seed 1900-01-01 for first run; `COALESCE` empty MAX; pipeline `concurrency: 1`; lookback + MERGE for late commits.
- Timestamp loads miss deletes → **Change Tracking** (net changes + deletes), **CDC** (all changes), data flow CDC, ADF **CDC resource (preview, no SHIR)**, or Databricks **Lakeflow Connect**.