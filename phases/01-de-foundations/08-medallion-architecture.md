# 1.8 Medallion Architecture — Bronze, Silver, Gold

**Time:** 35 min · **Prerequisites:** 1.3, 1.5, 1.7

**After this topic you can:**
- explain what belongs in each layer and — more importantly — what does **not**;
- explain why the layering exists (replay, debugging, quality gates, access);
- trace a record from raw file to dashboard;
- critique bad medallion designs.

---

## A. Concept deep dive

### 1. The simplest explanation

Medallion architecture organises lakehouse tables into layers of increasing quality:

```
 Sources ──▶  BRONZE  ──▶  SILVER  ──▶  GOLD  ──▶ BI, ML, apps
              raw,         clean,       business-
              as-is        conformed    ready
```

It's a **design pattern**, not a product feature. Names vary: raw / staging / curated, landing / refined / presentation — the idea is the same.

### 2. What each layer is for

| | 🥉 Bronze | 🥈 Silver | 🥇 Gold |
|---|---|---|---|
| **Purpose** | Faithful copy of the source | Trustworthy, reusable entities | Answers business questions |
| **Content** | Raw records as received (often strings / raw JSON), **plus audit columns** | Typed, deduplicated, validated, standardised, conformed keys; history (SCD) where needed | Aggregates, star schemas (facts/dims), KPIs, feature tables |
| **Grain** | Source record | Business entity / event (one row per order, per customer version) | Per business need (revenue per day per city) |
| **Write pattern** | **Append-only** (incremental: Auto Loader, streaming) | MERGE / upsert, incremental | Overwrite/MERGE of affected slices, materialised views |
| **Quality** | None or minimal (can it be read?) | **Rules enforced**; bad rows quarantined | Business rules, reconciled totals |
| **Who reads it** | DEs (debugging, replay) | DEs, analytics engineers, data scientists | Analysts, BI, business users, apps |
| **Retention** | Long enough to replay (policy-driven) | Long-term | Long-term |

**Audit columns in Bronze** (essential for debugging and replay):

| Column | Example | Why |
|---|---|---|
| `_source_file` | `orders_0002.csv` (from `_metadata.file_path`) | Trace any row to its file |
| `_ingested_at` | ingestion timestamp | Order of arrival, dedup tie-break, freshness |
| `_batch_id` / `_run_id` | job run id | Find/rollback everything from one run |
| `_rescued_data` | unparseable fields (Auto Loader) | Never silently lose data (Phase 7) |

### 3. Why the layers exist

| Reason | Explanation |
|---|---|
| **Replay** | Bronze keeps raw history. Bug in Silver logic? Fix it and rebuild from Bronze — no re-extraction from a source that may have purged the data |
| **Debugging** | "Why is this revenue number wrong?" → Gold → Silver row → Bronze row → exact source file |
| **Separation of concerns** | Ingestion (get data in reliably) is separated from business logic (interpret it). Source changes affect Bronze/Silver, not every dashboard |
| **Quality gates** | Each hop is a checkpoint: nothing reaches Gold without passing Silver's rules |
| **Reuse** | One `silver_orders` feeds many Gold tables → one definition of an order |
| **Access control** | Raw PII in Bronze restricted to DEs; Gold broadly shared, possibly masked |
| **Performance** | Gold is pre-aggregated → dashboards query small tables |

### 4. Data flow example

```
orders_0002.csv
  "102","C2","100.00","2026-10-16 10:02:00"     (resend of 102)
  "103","C3","-40.00",...
        │ append + _source_file, _ingested_at
        ▼
BRONZE bronze_orders              6 rows, all strings, duplicates and bad values kept
        │ cast · dedupe by order_id · validate
        ├──────────────▶ silver_orders_quarantine   103 (amount_not_positive), 105 (amount_invalid)
        ▼
SILVER silver_orders              101, 102, 104 — typed, one row per order
       silver_customers           trimmed, standardised city/segment
        │ join + aggregate
        ▼
GOLD   gold_daily_revenue_by_city 2026-10-16 · Dubai · gold · 250.00 …
```

### 5. Common design mistakes

| Mistake | Why it's bad |
|---|---|
| Cleaning/filtering **in Bronze** | Destroys raw fidelity → no replay, no proof of what the source sent |
| Silver = a copy of Bronze with types | Pointless layer; dedup/validation/conformance are what make Silver valuable |
| Gold built **directly from Bronze** | Every Gold table re-implements cleaning → inconsistent numbers |
| Business aggregates in Silver | Silver should be reusable entities; aggregates belong in Gold |
| Dropping bad rows silently | Data loss nobody knows about → quarantine with a reason |
| 7 layers "to be safe" | More copies, more cost, more latency, no benefit |
| No audit columns | Can't trace rows to files/runs; can't dedupe reliably |
| Overwriting Bronze each run | Destroys history; Bronze should be append-only |

### 6. Misconceptions

1. **"Medallion is a Databricks feature."** It's a pattern; Databricks popularised the name.
2. **"Exactly three layers, always."** It's a guideline — some add a landing zone before Bronze, some have Silver sub-layers.
3. **"Bronze = garbage."** Bronze is the most *valuable* layer for recovery; it's just not for business users.
4. **"Gold must be aggregated."** Gold is *consumption-ready*: can be aggregates, star schemas, or wide denormalised tables.

---

## B. Practical — Bronze → Silver → Gold end to end

**Runnable notebook:** [`exercises/phase-01/08_medallion_end_to_end.py`](../../exercises/phase-01/08_medallion_end_to_end.py)

### Input (Bronze, as received)

| order_id | customer_id | amount | order_ts | _source_file |
|---|---|---|---|---|
| 101 | C1 | 250.00 | 2026-10-16 09:15:00 | orders_0001.csv |
| 102 | C2 | 100.00 | 2026-10-16 10:02:00 | orders_0001.csv |
| 102 | C2 | 100.00 | 2026-10-16 10:02:00 | orders_0002.csv |
| 103 | C3 | -40.00 | 2026-10-16 11:30:00 | orders_0002.csv |
| 104 | C9 | 75.50 | 2026-10-16 14:45:00 | orders_0002.csv |
| 105 | C2 | abc | 2026-10-16 15:00:00 | orders_0002.csv |

Customers: C1 " Asha Rao ", "dubai", "GOLD" · C2 Ben Ali, Abu Dhabi, standard · C3 Chen Wu, Dubai, Standard.

### Key Silver logic explained

```python
w = Window.partitionBy("order_id").orderBy(F.col("_ingested_at").desc(), F.col("_source_file").desc())
deduped = typed.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
```
One row per `order_id`: number the copies newest-first and keep #1. The second sort key breaks ties deterministically when both copies have the same `_ingested_at` — without it, *which* copy survives could change between runs.

```python
checked = deduped.withColumn("_dq_errors", F.filter(F.array(
    F.when(F.col("order_id").isNull(), F.lit("order_id_missing")),
    F.when(F.col("amount").isNull(), F.lit("amount_invalid")),
    F.when(F.col("amount") <= 0, F.lit("amount_not_positive")),
    F.when(F.col("order_ts").isNull(), F.lit("order_ts_invalid")),
), lambda x: x.isNotNull()))
```
Each `when` yields a reason string or `null`; `F.filter` keeps only the non-null reasons. Valid rows have an empty array → Silver; others → quarantine **with the reasons**. Note the separate `isNull` check: `null <= 0` is `null`, so `when(null <= 0, …)` would *not* flag a null amount on its own.

### Expected results

**silver_orders**

| order_id | customer_id | amount | order_ts |
|---|---|---|---|
| 101 | C1 | 250.00 | 2026-10-16 09:15:00 |
| 102 | C2 | 100.00 | 2026-10-16 10:02:00 |
| 104 | C9 | 75.50 | 2026-10-16 14:45:00 |

**silver_orders_quarantine**

| order_id | amount | _dq_errors |
|---|---|---|
| 103 | -40.00 | [amount_not_positive] |
| 105 | null | [amount_invalid] |

**gold_daily_revenue_by_city**

| order_date | city | segment | revenue | orders |
|---|---|---|---|---|
| 2026-10-16 | Abu Dhabi | standard | 100.00 | 1 |
| 2026-10-16 | Dubai | gold | 250.00 | 1 |
| 2026-10-16 | UNKNOWN | UNKNOWN | 75.50 | 1 |

**Reconciliation:** 5 distinct orders in Bronze = 3 valid + 2 quarantined ✅

Design choices to be able to defend:
- **Left join** in Gold so revenue from unknown customer C9 isn't lost (an inner join would silently drop 75.50).
- **Standardisation** in Silver (`initcap(city)`, `lower(segment)`) — otherwise "dubai" and "Dubai" become two groups in Gold.
- **countDistinct** for orders — safe even if a later join fans out.

> Simplification: this demo overwrites Silver from all of Bronze each run. Production Silver is incremental (MERGE / streaming), covered in Phases 6–9.

---

## C. Interview questions

### 🟢 Basic

**B1.** Explain Bronze, Silver and Gold in one sentence each.

<details><summary>Model answer</summary>

Bronze holds raw source data as received, append-only, with audit metadata for replay and debugging. Silver holds cleaned, typed, deduplicated, validated and conformed business entities that are reusable across use cases. Gold holds consumption-ready, business-level datasets such as aggregates, star schemas and KPIs for BI and ML.
</details>

**B2.** Why keep raw data in Bronze if nobody in the business uses it?

<details><summary>Model answer</summary>

Replay and traceability: when Silver/Gold logic is wrong or changes, you rebuild from Bronze without re-extracting from sources that may no longer have the data; and any number can be traced back to the exact source record and file.
</details>

### 🟡 Medium

**M1.** Name four transformations that belong in Silver and two that belong in Gold.

<details><summary>Model answer</summary>

Silver: type casting, deduplication, validation/quarantine, standardisation (trim, case, codes), conforming keys across sources, SCD history. Gold: business aggregates (daily revenue), star-schema facts/dimensions, KPI calculations, denormalised tables for a specific dashboard or model.
</details>

**M2.** Your Gold revenue table is built directly from Bronze for speed. What problems will appear?

<details><summary>Model answer</summary>

Every Gold table must re-implement casting, dedup and validation — inconsistently — so different dashboards disagree; duplicates and bad rows leak into business numbers; debugging is harder; and any source change breaks many Gold tables at once instead of one Silver table.
</details>

### 🔴 Tricky

**T1.** In the Gold job, someone changes the left join to an inner join "because unknown customers look ugly". What happens, and how would anyone notice?

<details><summary>Model answer</summary>

Orders whose customer is missing from `silver_customers` (late-arriving dimension, data error) silently disappear from revenue — 75.50 in the demo. Nobody notices unless there's reconciliation: compare `SUM(amount)` in `silver_orders` with `SUM(revenue)` in Gold. Correct approach: keep a left join with an "UNKNOWN" member, alert on unknowns, and fix them when the dimension arrives.
</details>

**T2.** Bronze is written with `mode("overwrite")` every day. Why is that a design bug even if everything downstream looks fine?

<details><summary>Model answer</summary>

Bronze loses its history: yesterday's raw data is gone, so you can't replay or audit past loads, and a bad or partial source file today destroys the previous good copy. Bronze should be append-only (incremental) with retention governed by policy.
</details>

**T3.** Should data-quality failures stop the pipeline or quarantine rows? Argue both.

<details><summary>Model answer</summary>

Quarantine when bad rows are isolated and the rest is still valuable (a few malformed orders shouldn't block all revenue) — with alerting and thresholds. Fail when the issue invalidates the whole dataset or is dangerous (primary key nulls everywhere, schema completely changed, totals off by 50 %, financial reporting where partial data is worse than late data). Often both: quarantine individual rows, fail if the quarantine rate exceeds a threshold.
</details>

---

## E. Revision summary

- Bronze = raw, append-only, audit columns. Silver = typed, deduped, validated, conformed entities. Gold = consumption-ready.
- Layers exist for **replay, traceability, separation of concerns, quality gates, reuse, access control**.
- Quarantine bad rows **with reasons**; reconcile counts across layers.
- Never clean in Bronze, never skip Silver, never drop silently.
- It's a pattern — names and number of layers can vary.