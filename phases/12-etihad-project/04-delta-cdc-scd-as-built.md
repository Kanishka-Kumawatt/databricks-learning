# 12.4 Delta, CDC & SCD — As Built, and As It Should Be

**Time:** 25 min · **Prerequisites:** Phase 6 (MERGE internals), Phase 9 (CDC & SCD)

**After this topic you can:**
- describe the hand-written two-step SCD2 our team used and exactly what happens if it fails half-way;
- rewrite it as a **single atomic MERGE** using the staged-union (`merge_key = NULL`) trick;
- explain the SCD1 upsert with a timestamp watermark and its ordering rule;
- explain what the newer pipelines used instead: **AUTO CDC**, **expectations**, **Auto Loader + rescued data**.

---

## A. Concept / story

### 1. Two-step SCD2 (as built)

```
Step 1  MERGE target USING changes ON business_key AND is_current
        WHEN MATCHED AND hash(tracked cols) differs  THEN UPDATE SET is_current = false, end_date = change_ts
                                                                   ─── commit #1 ───
Step 2  INSERT INTO target SELECT new versions (surrogate key from a control table, is_current = true,
        start_date = change_ts, end_date = '9999-12-31')  for new keys + keys expired in step 1
                                                                   ─── commit #2 ───
```

Surrogate keys came from a **key control table** (max key per table, read then incremented) — not an identity column.

**Failure between commit #1 and #2:** the changed customers/passengers now have **no current row** — joins on
`is_current = true` silently drop them from curated. On rerun, step 1 finds nothing to expire (already expired), and
step 2 must still insert the new versions — correct only if step 2 derives "new versions" from the change set, not
from what step 1 updated. If it derives them from step 1's output, the versions are lost. Plus the key control
table may have been incremented twice → gaps (harmless) or, with concurrent runs, duplicates (harmful).

### 2. Single atomic MERGE (how it should be)

```
staged = changes (merge_key = business_key)      ← matches the current row → UPDATE to expire it
         UNION ALL
         changed rows only (merge_key = NULL)    ← never matches → INSERT the new version

MERGE INTO dim t USING staged s
  ON t.business_key = s.merge_key AND t.is_current
  WHEN MATCHED AND t.row_hash <> s.row_hash THEN UPDATE SET is_current = false, end_date = s.change_ts
  WHEN NOT MATCHED THEN INSERT (... is_current = true, start_date = s.change_ts, end_date = NULL)
                                                                   ─── ONE commit ───
```

Either both the expiry and the new version are committed, or neither is. Surrogate keys:
`BIGINT GENERATED ALWAYS AS IDENTITY` — no control table, no race. Runnable: [`04_atomic_scd2.py`](../../exercises/phase-12/04_atomic_scd2.py).
Generic lesson: [Phase 9](../09-cdc-scd/).

### 3. SCD1 upsert with a watermark

```
last_wm = SELECT max_ts FROM watermark_table WHERE object = 'bookings'
changes = source WHERE modified_ts > last_wm            (ADF query or Spark filter)
dedup   = latest row per key by modified_ts             (row_number)
MERGE target USING dedup ON key
   WHEN MATCHED AND s.modified_ts > t.modified_ts THEN UPDATE *     ← protects against out-of-order replays
   WHEN NOT MATCHED THEN INSERT *
UPDATE watermark_table SET max_ts = max(changes.modified_ts)       ← ONLY after the MERGE commits
```

Rules: advance the watermark **after** a successful write (else a failure loses data); use the **max of the data
loaded**, not `now()` (clock skew, in-flight transactions); use `>` with a small overlap/lookback if the source
commits late, and rely on the MERGE to make the overlap harmless.

### 4. Newer pipelines — declarative

| Concern | Hand-written | Declarative pipeline (Lakeflow SDP) |
|---|---|---|
| Ingest files | Copy + notebook read with fixed schema | **Auto Loader** (`cloudFiles`) with schema location; unexpected columns/types go to **`_rescued_data`** |
| Quality | DQ framework after load | **Expectations**: `@dp.expect`, `expect_or_drop`, `expect_or_fail` — metrics in the event log |
| SCD1/SCD2 | MERGE code + key control table | **AUTO CDC** (`dp.create_auto_cdc_flow`, SQL `AUTO CDC INTO`), `keys`, `sequence_by`, `stored_as_scd_type=2`, `apply_as_deletes`; SCD2 gets `__START_AT` / `__END_AT` |
| Out-of-order events | Manual guards | `sequence_by` handles ordering |
| Restart | Audit framework | Checkpoints, full refresh when needed |

AUTO CDC replaces the older `APPLY CHANGES` API (same semantics) — the `dlt` module is the legacy name for `dp`.

```python
from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(comment="raw passenger changes")
@dp.expect_or_drop("valid_key", "passenger_id IS NOT NULL")
def passenger_changes():
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .load("/Volumes/main/de_prep/landing/passengers/"))

dp.create_streaming_table("passenger_dim")
dp.create_auto_cdc_flow(
    target="passenger_dim", source="passenger_changes",
    keys=["passenger_id"], sequence_by=F.col("change_ts"),
    apply_as_deletes=F.expr("op = 'D'"),
    except_column_list=["op", "_rescued_data"],
    stored_as_scd_type=2)
```

### 5. Misconceptions

1. **"MERGE is atomic, so my SCD2 is atomic."** Each statement is; a *two*-statement SCD2 isn't.
2. **"Watermark = now()."** Use the max of what you actually loaded.
3. **"Rescued data means bad data is dropped."** It's *kept* in `_rescued_data` — you must monitor it.

---

## B. Practical

Run [`exercises/phase-12/04_atomic_scd2.py`](../../exercises/phase-12/04_atomic_scd2.py): it builds a passenger
dimension, applies a change batch with the **two-step** method, simulates a crash between the steps and shows the
missing current rows, then applies the same batch with the **single MERGE** and verifies exactly one current row per key.

Self-check queries for any SCD2 table:

```sql
-- exactly one current row per key
SELECT passenger_id, count(*) FROM dim WHERE is_current GROUP BY passenger_id HAVING count(*) <> 1;
-- no overlapping validity windows
SELECT a.passenger_id FROM dim a JOIN dim b
  ON a.passenger_id = b.passenger_id AND a.sk < b.sk
 AND a.start_date < coalesce(b.end_date, '9999-12-31') AND b.start_date < coalesce(a.end_date, '9999-12-31');
```

---

## C. Interview questions

### 🟢 Basic

**B1.** SCD1 vs SCD2 — which did you use where?

<details><summary>Model answer</summary>

SCD1 (overwrite) for entities where only the current value matters — e.g. a booking's latest status in staging.
SCD2 (keep history with start/end dates and a current flag) for dimensions where reports need the value as of
the event date — e.g. a passenger's loyalty tier at the time of the flight.
</details>

**B2.** Why advance the watermark only after the write?

<details><summary>Model answer</summary>

If you advance it first and the write fails, the next run starts after rows that were never loaded — silent data
loss. Advancing after the commit means a failure just re-reads the same window, and the MERGE makes that safe.
</details>

**B3.** What is `_rescued_data`?

<details><summary>Model answer</summary>

An Auto Loader column that captures data that doesn't fit the schema — unexpected columns, type mismatches,
case mismatches — as JSON, so it isn't lost. You monitor it and evolve the schema or fix the source.
</details>

### 🟡 Medium

**M1.** Explain the staged-union trick for a single-MERGE SCD2.

<details><summary>Model answer</summary>

Build the source as the change set twice: once with `merge_key = business_key`, which matches the current target
row so the MATCHED clause expires it; and once — only for changed keys — with `merge_key = NULL`, which can't
match, so NOT MATCHED inserts the new version. New keys come through the first copy and are inserted as well.
One MERGE, one commit.
</details>

**M2.** Two changes for the same passenger arrive in one batch. What breaks?

<details><summary>Model answer</summary>

MERGE fails if multiple source rows match the same target row ("multiple source rows matched"). For SCD1 keep
the latest per key with `row_number()` by sequence column. For SCD2, if you need all intermediate versions,
process them in sequence order or let AUTO CDC handle it with `sequence_by`.
</details>

**M3.** Why move from a key control table to identity columns?

<details><summary>Model answer</summary>

Reading the max key and incrementing is a read-modify-write across two tables — not atomic, racy under
concurrent runs, and an extra failure point. `GENERATED ALWAYS AS IDENTITY` assigns unique (not necessarily
consecutive) keys inside the write. Caveat: identity columns limit concurrent writers to the table, which is fine
for a dimension loaded by one job.
</details>

### 🔴 Tricky

**T1.** SCD2 = MERGE then INSERT; the cluster dies between them. Table state? What happens on rerun?

<details><summary>Model answer</summary>

After commit #1 the changed keys' old rows are expired and there is no current row for them — curated joins on
`is_current` drop those passengers. On rerun, step 1 finds nothing to expire; whether step 2 inserts the new
versions depends on how it computes them — if from "rows just expired", they're lost forever; if from the change
set anti-joined to current rows, it recovers. Either way readers saw a broken state in between. Fix: single atomic
MERGE, or AUTO CDC; and use Delta time travel/RESTORE to repair.
</details>

**T2.** Your SCD1 MERGE has `WHEN MATCHED THEN UPDATE *` without a timestamp guard. A replay of last week's file arrives. What happens?

<details><summary>Model answer</summary>

Older values overwrite newer ones — the table regresses. Add `AND s.modified_ts > t.modified_ts` to the matched
clause (and dedupe the source by latest). That makes the MERGE order-insensitive for replays.
</details>

**T3.** Would you rewrite all existing SCD2 notebooks to AUTO CDC?

<details><summary>Model answer</summary>

Not big-bang. New pipelines: yes. Existing: first fix atomicity with a single MERGE (low risk, same table
shape), then migrate the high-change or most-incident-prone ones to AUTO CDC behind a parallel run, because AUTO
CDC changes column names (`__START_AT`/`__END_AT`) and requires a streaming-table target — downstream
consumers must be adapted or given a view with the old column names.
</details>

---

## E. Revision summary

- As built: SCD2 = **MERGE (expire) + INSERT (new)** = two commits; failure in between → keys with no current row.
- Better: **one MERGE with staged union** (`merge_key = NULL` for the insert copy); identity surrogate keys.
- SCD1: dedupe latest → MERGE with **timestamp guard** → advance watermark **after** commit, to max loaded.
- Newer: **Auto Loader** + `_rescued_data`, **expectations**, **AUTO CDC** (`create_auto_cdc_flow`, `sequence_by`, SCD2 `__START_AT/__END_AT`).