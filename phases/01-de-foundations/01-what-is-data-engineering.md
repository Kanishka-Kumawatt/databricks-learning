# 1.1 What Is Data Engineering?

## A. Concept deep dive

### Definition
**Data engineering is designing, building, and operating systems that collect data from source systems, turn it into reliable and usable datasets, and deliver it to the people and applications that need it — repeatedly, at scale, and recoverably.**

- **Designing, building *and operating*** — running it every day is half the job.
- **Source systems** — the DE never creates the data; apps, databases, APIs, devices and files do.
- **Reliable and usable** — correct types, no duplicates, documented, on time.
- **Repeatedly, at scale, recoverably** — the difference between a script and a production pipeline.

### Why it exists
Operational systems *run* the business; analytics *understands* it. Their needs conflict:

| Operational systems need… | Analytics needs… |
|---|---|
| Fast single-row reads/writes | Scans of millions–billions of rows |
| Current state | History |
| One system's data | Many systems combined |
| Schema optimised for the app | Schema optimised for questions |

Data engineering is the bridge. Without it: slow production apps, conflicting numbers, no trust.

### Short history (each step fixed the previous one's biggest problem)
1. **1990s–2000s — Warehouses + ETL tools.** Structured only, expensive, scale-up.
2. **~2006 — Hadoop.** Cheap machines, HDFS + MapReduce. Huge scale, but slow (disk-heavy) and hard to code.
3. **~2014 — Apache Spark.** In-memory, far faster, one engine for SQL / streaming / ML.
4. **Cloud.** Storage separated from compute; lakes cheap but became "swamps" (no transactions or quality).
5. **~2019+ — Lakehouse (Delta Lake, Databricks).** Warehouse reliability (ACID, schema) on lake storage.

### The lifecycle

```
SOURCE ──▶ INGEST ──▶ STORE ──▶ TRANSFORM ──▶ SERVE
   └── undercurrents: orchestration · quality · monitoring · security · governance · cost ──┘
```

| Stage | Key question | DE decisions | ShopCo example |
|---|---|---|---|
| Source | Where does data come from, how does it change? | Pull vs push, full vs changes, API limits | Postgres `orders`, payment CSVs, click JSON |
| Ingest | How do we copy it in safely? | Batch vs streaming, frequency, late/duplicate files | Hourly order extract; Auto Loader for CSVs |
| Store | Where, in what format? | Lake vs warehouse, Delta/Parquet, layout, retention | Delta tables in Bronze/Silver/Gold |
| Transform | How do we make it trustworthy and useful? | Cleaning, dedup, joins, business logic, history (SCD) | Dedupe payments, join orders ↔ customers |
| Serve | Who uses it, how? | BI tables, ML features, APIs, freshness SLAs | Revenue dashboard by 07:00 |

### The undercurrents (what makes it *engineering*)
- **Orchestration** — order, schedule, retries.
- **Data quality** — rules like `amount > 0`; quarantine bad records.
- **Monitoring & alerting** — know before the CEO does.
- **Idempotency** — running twice gives the same result as running once.
- **Governance & security** — access control, PII masking, lineage.
- **Cost** — idle compute burns money.

### Roles compared

| Role | Main question | Output |
|---|---|---|
| Data Engineer | Is the right data available, correct, on time? | Pipelines, tables, platform |
| Analytics Engineer | Is business logic modelled well? | Gold / semantic models |
| Data Analyst | What happened and why? | Dashboards, reports |
| Data Scientist / ML Eng | What will happen? | Models, predictions |

### Misconceptions
1. **"Succeeded = correct."** Silent failures: zero rows, duplicates, stale file re-loaded.
2. **"DE = moving data."** Moving is easy; moving correctly, repeatedly and recoverably is the job.
3. **"Bigger cluster fixes slowness."** Bad design (skew, full reloads, small files) just gets more expensive.
4. **"Bad source data isn't my problem."** You can't fix the source, but you must detect, quarantine and alert.

## B. Practical

Code: [`exercises/phase-01/01_mini_pipeline.py`](../../exercises/phase-01/01_mini_pipeline.py) · Data: [`data/orders_2026-10-16.csv`](../../data/orders_2026-10-16.csv)

**Input**

| order_id | customer_id | region | amount | order_ts |
|---|---|---|---|---|
| 101 | C1 | North | 250.00 | 2026-10-16 09:15:00 |
| 102 | C2 | South | 100.00 | 2026-10-16 10:02:00 |
| 102 | C2 | South | 100.00 | 2026-10-16 10:02:00 |
| 103 | C3 | North | -40.00 | 2026-10-16 11:30:00 |
| 104 | C1 | North | 75.50 | 2026-10-16 14:45:00 |

**Expected output** — `shopco.gold.daily_revenue`

| order_date | region | revenue | orders |
|---|---|---|---|
| 2026-10-16 | North | 325.50 | 2 |
| 2026-10-16 | South | 100.00 | 1 |

**Key engineering decisions in the code**
- `run_date` is a **parameter** → re-runs and backfills for any past date.
- CSV without a schema reads **every column as string** → explicit casts.
- `decimal`, not `float`, for money → no rounding errors.
- A failed cast becomes **`null` silently** → needs a quality check.
- `dropDuplicates(["order_id"])` removes the source retry (order 102).
- Negative amount filtered — in production, **quarantine** instead of silently dropping.
- `overwrite` + `replaceWhere` replaces only that day's slice → **idempotent**; `append` would double revenue on every retry.

**Design exercise (paper, 10 min):** draw ShopCo's lifecycle for Postgres orders, payment CSVs and click events. On each arrow write one thing that can go wrong.

## C. Interview questions (answer before reading any solution)

### 🟢 Basic
- **B1.** What is data engineering? (2–3 sentences, as if asked cold.)
- **B2.** How does a Data Engineer's job differ from a Data Analyst's and a Data Scientist's?
- **B3.** Name the five lifecycle stages and one decision a DE makes at each.

### 🟡 Medium
- **M1.** Analysts want to run heavy revenue queries directly on production Postgres. Give two technical reasons this is bad, and what you'd offer instead.
- **M2.** Why is `run_date` a parameter rather than "today" inside the code? Describe a real situation where it matters.
- **M3.** `"25O.00"` (letter O) silently becomes `null` on cast. Why is that dangerous for this pipeline, and how would you detect it?

### 🔴 Tricky
- **T1.** The job is ✅ *Succeeded* but yesterday's revenue is exactly **double**. Three possible causes — which do you check first, and why?
- **T2.** Order 102 reappears in **tomorrow's** file (late resend). Does `dropDuplicates` protect you? What happens to the numbers?
- **T3.** "We don't need a DE — an analyst can export Postgres to Excel every morning." When is that genuinely fine, and what signals tell you it no longer is?

## D. Evaluation
_Filled in after answers are graded — see [`interview/answers-log.md`](../../interview/answers-log.md)._

## E. Revision summary
_Added after evaluation._