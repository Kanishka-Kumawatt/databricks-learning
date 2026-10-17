# 1.7 Full Load vs Incremental Load

**Time:** 40 min · **Prerequisites:** 1.5, 1.6

**After this topic you can:**
- explain full and incremental loading, and when each is the right choice;
- name five incremental strategies and their blind spots;
- implement a watermark (high-water mark) load correctly, including ordering and overlap;
- explain why deletes, late commits and missing timestamps break naive incremental loads.

---

## A. Concept deep dive

### 1. The simplest explanation

- **Full load:** every run, copy **all** the data from the source and replace the target.
- **Incremental load:** every run, copy **only what is new or changed** since the last run, and apply it to the target.

### 2. Why incremental loading exists

| Source size | Full load each night | Incremental each night |
|---|---|---|
| 10 MB customer list | Seconds — fine | Not worth the complexity |
| 500 GB orders table, 0.2 % changes daily | Hours, heavy load on source, big cluster | Minutes, ~1 GB read |

Full loads get **slower and more expensive every day** as data grows, and they hammer the source system. Incremental loads keep cost proportional to **change volume**, not total volume.

### 3. Full load — honest trade-offs

| ✅ Advantages | ❌ Disadvantages |
|---|---|
| Simplest logic | Cost and time grow with total size |
| **Self-healing** — any earlier mistake is overwritten | Heavy load on source systems |
| **Captures deletes** automatically (row gone from source → gone from target) | Long windows; may not fit SLA |
| No state to manage | No history of changes (unless you snapshot) |

**Still the right choice for:** small dimension/reference tables, sources without any change tracking, and **periodic reconciliation** (e.g. weekly full reload to correct drift from incremental loads).

### 4. Incremental strategies

| # | Strategy | How it detects change | Blind spots |
|---|---|---|---|
| 1 | **Append-only key** | `id > last_max_id` | Only inserts; no updates or deletes |
| 2 | **Timestamp watermark** | `updated_at > last_watermark` | Deletes; rows whose `updated_at` isn't maintained; late commits; clock/time-zone issues |
| 3 | **CDC (change data capture)** | Reads the database's transaction log (inserts/updates/deletes with order) | Needs CDC enabled + tooling; ordering & duplicates must be handled (Phase 9) |
| 4 | **New files** | Files not processed before (Auto Loader, streaming checkpoints) | Files that are *replaced* in place; duplicate content under new names |
| 5 | **Snapshot comparison** | Compare today's full snapshot with yesterday's (hash per row) | Needs full extracts; compute-heavy — but catches everything incl. deletes |

### 5. How a watermark load works internally

```
          ┌──────────────── control table ───────────────┐
          │ table_name = orders, last_value = 09:00       │
          └───────────────────────────────────────────────┘
 1. read watermark ──▶ 2. extract WHERE updated_at > 09:00 (minus lookback)
                      3. dedupe: latest version per key
                      4. MERGE into target (upsert)
                      5. ONLY THEN set watermark = MAX(updated_at) of the extracted rows
```

Rules that make it correct:

1. **Order of operations:** write data first, update the watermark second. If the write fails, the watermark doesn't move → the next run retries the same window. If you update the watermark first and the write fails, those rows are **lost forever**.
2. **New watermark = max value in the data you processed**, not `now()`. Using `now()` skips rows committed with earlier timestamps during the run.
3. **Use MERGE (upsert), not append** — a row updated three times must end up as one row, and re-processing an overlap must not create duplicates.
4. **Overlap / lookback window:** extract `updated_at > watermark − N minutes`. This catches **late commits** (a long transaction that started at 08:30 but committed after you saved 09:00). The overlap is harmless *because* the write is a MERGE.
5. **Pin a consistent snapshot** of what you extracted (land it in Bronze first) so count, write and max-watermark all refer to the same data.

### 6. The blind spots in detail

| Problem | Example | Mitigation |
|---|---|---|
| **Deletes** | Order 103 deleted in source — no row to extract, so it stays in the target forever | Soft-delete flags in source; CDC; periodic full reconciliation / key comparison |
| **Unmaintained `updated_at`** | An app bug or bulk script updates `amount` without touching `updated_at` | Verify with the source team; database-maintained triggers/columns; CDC; periodic reconciliation |
| **Late commits** | Long-running transaction with an older timestamp commits after the run | Lookback overlap + MERGE |
| **Time zones / clock skew** | Source stores local time, pipeline compares in UTC | Normalise to UTC; store the watermark in the source's own time basis |
| **NULL timestamps** | `updated_at IS NULL` for legacy rows | Handle in the initial full load; `COALESCE(updated_at, created_at)` |
| **Equal timestamps at the boundary** | Many rows share 09:00:00; using `>` vs `>=` | `>=`/overlap + MERGE (idempotent) is safer than strict `>` |

### 7. Initial load vs incremental runs

- **Initial (historical) load:** one-time full extract, often chunked (by month/ID range) to avoid overloading the source; then set the first watermark.
- **Incremental runs:** use the watermark from then on.
- **Backfill/reload:** reset the watermark (or run a full load) for a range when logic changes.

### 8. Misconceptions

1. **"Incremental is always better."** For small tables, full loads are simpler and self-healing.
2. **"`updated_at > last_run_time` is enough."** Without overlap, MERGE and correct ordering it silently loses rows.
3. **"Incremental captures everything."** Timestamp-based loads never see hard deletes.
4. **"Append is fine for incremental."** Updates then appear as duplicates.

---

## B. Practical — watermark load with MERGE

**Runnable notebook:** [`exercises/phase-01/07_incremental_watermark.py`](../../exercises/phase-01/07_incremental_watermark.py)

Key part of the code:

```python
def incremental_load(lookback_minutes: int = 0):
    wm = get_watermark("src_orders")                                       # 1

    src_version = spark.sql(f"DESCRIBE HISTORY {SRC} LIMIT 1").first()["version"]
    changes = spark.sql(f"SELECT * FROM {SRC} VERSION AS OF {src_version}")  # 2

    if wm is not None:
        lower = wm - timedelta(minutes=lookback_minutes)
        changes = changes.filter(F.col("updated_at") > F.lit(lower))       # 3

    w = Window.partitionBy("order_id").orderBy(F.col("updated_at").desc())
    changes = changes.withColumn("_rn", F.row_number().over(w)) \
                     .filter("_rn = 1").drop("_rn")                        # 4

    if changes.count() == 0: return

    (DeltaTable.forName(spark, TGT).alias("t")                             # 5
        .merge(changes.alias("s"), "t.order_id = s.order_id")
        .whenMatchedUpdateAll(condition="s.updated_at > t.updated_at")
        .whenNotMatchedInsertAll()
        .execute())

    new_wm = changes.agg(F.max("updated_at")).first()[0]                   # 6
    if wm is None or new_wm > wm:
        set_watermark("src_orders", new_wm)
```

| # | Explanation |
|---|---|
| 1 | Read the last successful watermark from the control table (`None` on the first run → full initial load) |
| 2 | Pin one snapshot of the source so every step sees identical data |
| 3 | Extract only rows changed after the watermark, minus an optional overlap |
| 4 | If a key changed several times, keep only its latest version — MERGE requires at most one source row per target key |
| 5 | Upsert. The condition stops an older version from overwriting a newer one (matters with overlap and out-of-order data) |
| 6 | Advance the watermark **after** the write, to the max timestamp actually processed, never backwards |

**What the notebook demonstrates:**

| Run | Source change | Result in target |
|---|---|---|
| 1 | initial 101, 102, 103 | 3 rows inserted |
| 2 | 102 updated, 104 inserted, **103 deleted** | 102 updated, 104 inserted, **103 still present** (delete blind spot) |
| 3 | nothing | "No changes" — idempotent |
| 4 | 105 inserted with `updated_at` 08:30 (late commit, older than 09:00 watermark) | **missed** |
| 5 | same, with 60-minute lookback | 105 captured; 104 re-read but unchanged thanks to MERGE condition |

### Common mistakes

- Updating the watermark before writing.
- Setting the watermark to `current_timestamp()`.
- Appending instead of merging.
- MERGE failing with "multiple source rows matched" because the extract wasn't deduplicated.
- Never reconciling, so deletes and missed updates accumulate for years.

---

## C. Interview questions

### 🟢 Basic

**B1.** What is the difference between a full load and an incremental load? When would you still choose a full load?

<details><summary>Model answer</summary>

A full load re-extracts all source data each run and replaces the target; an incremental load extracts only new/changed data since the last run and applies it. Choose full for small tables, sources without change tracking, or periodic reconciliation, because it's simple, self-healing and captures deletes.
</details>

**B2.** What is a watermark (high-water mark) in incremental loading?

<details><summary>Model answer</summary>

The highest value of a monotonically increasing column (e.g. `updated_at` or an ID) that the pipeline has successfully processed, stored in a control table. The next run extracts rows above it.
</details>

### 🟡 Medium

**M1.** Why must the watermark be updated *after* the data is written? What happens otherwise?

<details><summary>Model answer</summary>

If the watermark is advanced and the write then fails, the next run starts from the new watermark and the failed window's rows are never loaded — silent data loss. Writing first means a failure leaves the old watermark, so a retry reprocesses the same window; with MERGE that reprocessing is safe.
</details>

**M2.** Why is MERGE preferred over append for incremental loads?

<details><summary>Model answer</summary>

Incremental extracts contain updates to existing keys; append would create duplicates. MERGE upserts by key, so updates replace old versions, and re-processing the same rows (retries, overlap windows) doesn't create duplicates — it makes the load idempotent.
</details>

**M3.** Your incremental load uses `updated_at`. Name three situations where it misses changes.

<details><summary>Model answer</summary>

(1) Hard deletes in the source. (2) Updates that don't touch `updated_at` (app bug, bulk scripts). (3) Late-committing transactions whose timestamp is older than the saved watermark. Also: time-zone mismatches and NULL timestamps.
</details>

### 🔴 Tricky

**T1.** Your watermark is set to `now()` at the end of each run. Over months, a few orders are missing. Explain why.

<details><summary>Model answer</summary>

Rows committed during the run, or committed late with timestamps earlier than `now()`, fall between the data you actually extracted and the time you recorded. The next run starts after `now()`, so they're skipped. Set the watermark to the max timestamp of the processed data and use an overlap window with MERGE.
</details>

**T2.** The source team says "we hard-delete cancelled orders". Your pipeline is timestamp-incremental. Design a fix.

<details><summary>Model answer</summary>

Options: ask for soft deletes (`is_deleted` + `updated_at`) or enable CDC to receive delete events; otherwise run a periodic key reconciliation — extract only the primary keys from the source (cheap), anti-join with the target, and delete/flag target rows whose keys no longer exist. Decide whether the target should physically delete or mark as deleted (history/audit requirements).
</details>

**T3.** MERGE fails with "multiple source rows matched the same target row". Why, and what's the fix?

<details><summary>Model answer</summary>

The extract contains more than one row for the same key (several updates in one window, or overlap). MERGE can't decide which to apply. Deduplicate the source first — e.g. `row_number()` over the key ordered by `updated_at`/sequence descending, keep row 1.
</details>

---

## E. Revision summary

- Full = simple, self-healing, catches deletes, doesn't scale. Incremental = scales with change volume, needs state.
- Strategies: append key, timestamp watermark, CDC, new files, snapshot diff — each has blind spots.
- Watermark rules: **write first, watermark second**, watermark = **max processed value**, **MERGE**, **overlap**, **pinned snapshot**.
- Timestamp loads **never see hard deletes** → CDC, soft deletes or reconciliation.
- Dedupe the extract before MERGE.