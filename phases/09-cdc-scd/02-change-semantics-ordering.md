# 9.2 Inserts, Updates, Deletes — Before/After Images, Sequence Columns and Ordering

**Time:** 20 min · **Prerequisites:** 9.1

**After this topic you can:**
- explain the meaning of I, U and D events and what an "upsert" interpretation means;
- explain before-images, after-images, full vs partial row images;
- map op codes from Debezium, SQL Server CDC and Delta CDF to I/U/D;
- explain why a **sequence column** (LSN) is better than an **event timestamp** for ordering, and how to break ties;
- write "latest event per key" correctly.

---

## A. Concept deep dive

### 1. The simplest explanation

Every change event answers three questions:

| Question | Column | Example |
|---|---|---|
| **What** happened? | operation `op` | `I` insert, `U` update, `D` delete |
| **To which** row? | key | `customer_id = 'C1'` |
| **In what order?** | sequence `seq` (and the time it happened, `event_ts`) | `seq = 10`, `event_ts = 2026-10-02 09:00` |

plus the data: the row's values **after** the change (after-image) and sometimes **before** it (before-image).

### 2. Semantics of each operation

| op | Meaning in the source | How a target applies it (SCD1) | SCD2 |
|---|---|---|---|
| **I** | Row created | Insert; if the key already exists (replay, re-insert) treat as **upsert** | New version |
| **U** | Row changed | Overwrite; if the key is unknown (missed insert, initial-load gap) treat as **upsert** = insert | Close current version, open new one |
| **D** | Row removed | Delete or mark `is_deleted = true` | Close current version, open nothing |

Practical rule: **treat I and U the same ("upsert the after-image")** and decide only by key existence and sequence. This makes the pipeline robust to a missed insert or a replayed one.

### 3. Before and after images

| Image | Content | Used for |
|---|---|---|
| **After-image** | Row values after the change | Applying I/U — what the target should look like |
| **Before-image** | Row values before the change | Auditing, conflict detection, computing deltas (`new_amount − old_amount`), D events (what was deleted) |

**Full vs partial images:** a **full** image carries every column. A **partial** image carries only the key and the changed columns (some connectors, some APIs, Postgres without `REPLICA IDENTITY FULL`, MongoDB-style patches). With partial images, a NULL can mean "set to NULL" **or** "not sent" — you must know which. AUTO CDC's `ignore_null_updates` exists for the "not sent" case (9.7).

Op codes you will meet:

| Source | Insert | Update | Delete | Other |
|---|---|---|---|---|
| Debezium `op` | `c` | `u` (`before` + `after`) | `d` (`before`, `after` = null) | `r` = row read during initial snapshot, `t` = truncate |
| SQL Server CDC `__$operation` | 2 | 3 = before image, 4 = after image | 1 | |
| Delta CDF `_change_type` | `insert` | `update_preimage`, `update_postimage` | `delete` | |
| Our ShopCo feed `op` | `I` | `U` (after-image only) | `D` (key only) | |

### 4. Ordering — why it matters

Applying the same events in a different order produces a different result:

| Events for C1 | Apply in seq order | Apply in arrival order (seq 11 first, then 10) |
|---|---|---|
| seq 10: city = Abu Dhabi | Abu Dhabi | |
| seq 11: city = Al Ain | **Al Ain** ✅ | **Abu Dhabi** ❌ (older event overwrote the newer one) |

Events can arrive out of order because of parallel Kafka partitions, retries, multiple files landing at once, or multiple extract threads. So the target must order by something **the source assigned**, not by arrival.

### 5. Sequence column vs event timestamp

| | Sequence (LSN, SCN, commit sequence, Kafka offset within a partition, version number) | Event timestamp (`updated_at`, `event_ts`) |
|---|---|---|
| Assigned by | The database log, in commit order | Application or DB clock |
| Strictly increasing? | Yes (per database / per key) | No — **ties** (same millisecond), clock **skew** between servers, clock changes (NTP, DST if local time) |
| Reflects commit order? | Yes | No — a long transaction may get an early timestamp and commit late |
| Human meaning | None ("0x0000002A…") | Yes — good for `effective_from` |

**Best practice:** order by the sequence; store the timestamp for business meaning (effective dates). If you only have timestamps, order by a **composite**: `(event_ts, tie_breaker)` — e.g. `(updated_at, lsn)` or `(updated_at, source_offset)`. In AUTO CDC: `sequence_by = struct("event_ts", "seq")` orders by the first field, then the second on ties.

### 6. Ordering is needed **per key**, not globally

To get the right final state for C1, only C1's events must be ordered. That's why per-key `row_number() OVER (PARTITION BY key ORDER BY seq DESC)` is enough, and why Kafka producers partition by key (ordering is guaranteed only within a partition).

### 7. ShopCo example — the tie problem

Two updates to C3 committed in the same millisecond (a batch job):

| customer_id | tier | event_ts | seq |
|---|---|---|---|
| C3 | premium | 2026-10-02 09:10:00.000 | 13 |
| C3 | plus | 2026-10-02 09:10:00.000 | 14 |

- `ORDER BY event_ts DESC` → **tie**: Spark may return either row; re-runs can produce different results (non-deterministic).
- `ORDER BY seq DESC` → always seq 14 (`plus`).

### 8. Misconceptions

1. **"Events arrive in order, so I don't need a sequence."** Arrival order is not guaranteed in distributed systems.
2. **"`updated_at` is good enough to order changes."** Ties and skew make it ambiguous; it doesn't reflect commit order.
3. **"An update for an unknown key is an error."** Usually it means you missed the insert (or loaded from a snapshot taken after it) — upsert it.
4. **"A delete event contains the row."** Often it contains only the key (or only a before-image). Don't copy its NULL attributes into the target.

---

## B. Practical — latest event per key, deterministically

**Input:** batch 2 of the ShopCo feed (as received, file order) — the full feed is in [`exercises/phase-09/01_customer_cdc_bronze_silver_gold.py`](../../exercises/phase-09/01_customer_cdc_bronze_silver_gold.py).

| # | customer_id | email | city | tier | op | seq | event_ts |
|---|---|---|---|---|---|---|---|
| 1 | C1 | asha@shop.co | Abu Dhabi | plus | U | 10 | 10-02 09:00 |
| 2 | C2 | ben@shop.co | Abu Dhabi | plus | U | 11 | 10-02 09:05 |
| 3 | C3 | chen.wu@shop.co | Dubai | plus | U | 14 | 10-02 09:20 |
| 4 | C3 | chen.wu@shop.co | Dubai | premium | U | 13 | 10-02 09:10 |
| 5 | C2 | ben@shop.co | Abu Dhabi | plus | U | 11 | 10-02 09:05 |
| 6 | C4 | | | | D | 15 | 10-02 09:30 |
| 7 | C6 | farah@shop.co | Dubai | basic | I | 16 | 10-02 09:40 |

```sql
SELECT *
FROM bronze_customer_cdc
WHERE _batch_id = 2
QUALIFY row_number() OVER (
          PARTITION BY customer_id
          ORDER BY seq DESC, _ingested_at DESC) = 1
```

```python
w = Window.partitionBy("customer_id").orderBy(F.col("seq").desc(), F.col("_ingested_at").desc())
latest = events.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
```

| Part | Explanation |
|---|---|
| `PARTITION BY customer_id` | Ordering only needs to be correct per key |
| `ORDER BY seq DESC` | Source-assigned order; highest = newest. Row 3 (seq 14) beats row 4 (seq 13) although it arrived first |
| `_ingested_at DESC` | Tie-breaker for identical seq (only exact duplicates should tie) → deterministic |
| `row_number() = 1` | Exactly one row per key — required by MERGE (9.6). `rank()` would keep both tied rows |
| `QUALIFY` | Databricks SQL filter on a window function without a subquery |

**Expected output (latest per key)**

| customer_id | city | tier | op | seq |
|---|---|---|---|---|
| C1 | Abu Dhabi | plus | U | 10 |
| C2 | Abu Dhabi | plus | U | 11 |
| C3 | Dubai | plus | U | 14 |
| C4 | | | D | 15 |
| C6 | Dubai | basic | I | 16 |

7 rows in → 5 rows out (one exact duplicate of C2 removed, one older C3 event superseded).

### Common mistakes

- Ordering by `event_ts` only → ties are resolved randomly.
- `dropDuplicates(["customer_id"])` → keeps an **arbitrary** row, not the latest.
- `rank()` / `dense_rank()` instead of `row_number()` → ties keep two rows → MERGE fails.
- Copying a D event's NULL columns into the target instead of deleting/flagging.

---

## C. Interview questions

### 🟢 Basic

**B1.** What are before- and after-images in CDC?

<details><summary>Model answer</summary>

The after-image is the row's values after the change (used to apply inserts and updates); the before-image is the row's values before the change (used for audit, delta calculations, and to know what a delete removed). An update in SQL Server CDC produces both (operation 3 and 4); Debezium puts both in `before` and `after`; Delta CDF has `update_preimage` and `update_postimage`.
</details>

**B2.** Why do CDC events need a sequence column?

<details><summary>Model answer</summary>

Events can arrive out of order or be duplicated; to apply them correctly the target needs a source-assigned, monotonically increasing value (LSN, commit sequence) that tells which change is newer for the same key. The latest sequence wins; older ones must not overwrite newer state.
</details>

**B3.** How should a target handle an update event for a key it has never seen?

<details><summary>Model answer</summary>

Usually as an upsert: insert the after-image. It typically means the insert was missed or the initial snapshot was taken after the insert — refusing it would lose the row. This works only with full after-images; with partial images you'd insert a row with missing columns, so flag it for review.
</details>

### 🟡 Medium

**M1.** Why is `event_ts` a weak ordering column? How do you make it safe?

<details><summary>Model answer</summary>

Timestamps can tie (same millisecond), can be skewed between servers, may be local time with DST changes, and reflect when the change was made rather than commit order (long transactions). Use the log sequence instead; if only timestamps exist, order by a composite `(event_ts, tie_breaker)` such as LSN, source offset or file name + row number, so the order is total and deterministic.
</details>

**M2.** Your source sends partial update events (only changed columns). What breaks in a simple `UPDATE SET *` MERGE?

<details><summary>Model answer</summary>

Columns not sent arrive as NULL and overwrite good values with NULL. You need column-wise logic: `SET col = COALESCE(s.col, t.col)` (if NULL means "not sent"), or a list of changed columns per event (AUTO CDC `ignore_null_updates` / `columns_to_update`). And you can't distinguish "set to NULL" from "not sent" unless the source tells you.
</details>

**M3.** Is global ordering of all events required?

<details><summary>Model answer</summary>

No — only per-key ordering is needed to compute each key's final state, which is why we partition by key in windows and Kafka partitions by key. Global/transaction ordering matters only when you need cross-row consistency (e.g. order header and lines from the same transaction appearing together).
</details>

### 🔴 Tricky

**T1.** Two events for the same key have the same `seq`. What does it mean and what do you do?

<details><summary>Model answer</summary>

If the content is identical, it's a duplicate delivery — keep one. If the content differs, the sequence isn't unique per key (wrong column, merged sources, or the sequence is per transaction and the transaction changed the row twice) — the pipeline can't know which is newer. Add a tie-breaker that reflects true order (e.g. `__$seqval` in SQL Server, operation order within the transaction), and alert on such conflicts rather than picking randomly.
</details>

**T2.** You receive SQL Server CDC rows with `__$operation` 3 and 4 for one update. You MERGE all rows by key. What goes wrong?

<details><summary>Model answer</summary>

Two source rows for the same key (before and after image) → "multiple source rows matched" error, or if deduplicated by LSN only, the before image (3) might be chosen because both share `__$start_lsn`. Filter out operation 3 (or use it only for audit), order by `__$start_lsn, __$seqval, __$operation` and take the last, and map 1 → D, 2 → I, 4 → U.
</details>

**T3.** Events are partitioned in Kafka by `order_id`, but your target is keyed by `customer_id`. Why can that cause wrong results?

<details><summary>Model answer</summary>

Kafka guarantees order only within a partition. Changes to the same customer can be in different partitions and be consumed in any relative order, so per-customer ordering by arrival is not guaranteed. You must order by a source sequence per customer (and handle out-of-order arrival with a guard), or partition the topic by the target key.
</details>

---

## E. Revision summary

- Event = **op + key + sequence + after-image** (+ before-image).
- Treat I and U as **upsert**; D = delete or tombstone; don't copy a D's NULLs.
- Op codes: Debezium `c/u/d/r`, SQL Server CDC 1 delete / 2 insert / 3 before / 4 after, CDF `insert / update_preimage / update_postimage / delete`.
- Order by **sequence** (LSN), keep timestamps for business meaning; tie-break composite `(event_ts, seq)`.
- Ordering is required **per key**. `row_number()` with a deterministic order, never `dropDuplicates(key)`.
- Partial images: NULL ambiguity → `COALESCE` or `ignore_null_updates`.