# 1.1 What Is Data Engineering?

**Time:** 35 min · **Prerequisites:** none

**After this topic you can:**
- define data engineering in one sentence and defend every word of it;
- describe the data lifecycle and the "undercurrents" that make it engineering;
- explain what a data pipeline is and what separates a script from a production pipeline;
- distinguish a Data Engineer from an Analyst, Analytics Engineer, Data Scientist and ML Engineer.

---

## A. Concept deep dive

### 1. The simplest explanation

Companies produce data everywhere — apps, payments, websites, sensors, partners. That data is scattered, messy and constantly changing. **A data engineer builds and runs the systems that collect it, clean it, combine it and deliver it, so that other people can trust it and use it.**

### 2. The precise definition (memorise the shape, not the words)

> **Data engineering is designing, building and operating systems that collect data from source systems, transform it into reliable, usable datasets, and deliver it to the people and applications that need it — repeatedly, at scale, and in a way that recovers from failure.**

| Phrase | What it really means | Why an interviewer cares |
|---|---|---|
| *designing, building **and operating*** | A pipeline runs every day for years. Monitoring it, fixing it and changing it is most of the work. | Shows you think beyond "I wrote a notebook". |
| *source systems* | The DE does not create data. Apps, databases, APIs, files and devices do — and they change without asking you. | You must design for sources you don't control. |
| *reliable, usable datasets* | Correct types, no duplicates, documented, consistent definitions, available on time. | "Usable" = someone can answer a question without cleaning it again. |
| *the people and applications that need it* | Analysts, BI dashboards, data scientists, ML models, other systems, regulators. | You serve consumers; their needs define "done". |
| *repeatedly, at scale* | Same logic, every hour/day, on 1 GB or 10 TB. | Scripts don't scale; systems must. |
| *recovers from failure* | Machines die, files arrive late, jobs get re-run. The result must still be correct. | This is idempotency and fault tolerance (topic 1.12). |

### 3. Why data engineering exists — the core conflict

Every company has two very different needs:

1. **Run the business** — take an order, charge a card, update stock. These are *operational* systems (OLTP — topic 1.2).
2. **Understand the business** — revenue by region, churn risk, fraud patterns. This is *analytics* (OLAP).

They need data in different shapes:

| Operational systems need… | Analytics needs… |
|---|---|
| Fast reads/writes of **one row** ("order 101") | Scans of **millions or billions** of rows ("all orders in 2026") |
| The **current** state only | **History** (what was this customer's address last year?) |
| Data from **one** application | Data **combined** from many systems |
| Schema shaped for the app (normalised) | Schema shaped for questions (denormalised, star schema) |
| Must never slow down | Heavy queries are normal |

If analysts query production directly, checkout slows down, there is no history, and every team calculates "revenue" differently. **Data engineering is the bridge between the systems that create data and the people who need to understand it.**

### 4. A short history — each step fixed the previous one's biggest problem

| Era | What appeared | Problem it solved | Problem it created |
|---|---|---|---|
| 1990s–2000s | **Data warehouses** (Teradata, Oracle) + **ETL tools** (Informatica, SSIS) | One trusted place for reporting | Expensive, structured data only, scale up = buy a bigger machine |
| ~2006 | **Hadoop** (HDFS + MapReduce) | Cheap storage and processing on many commodity machines | Slow (writes to disk between every step), hard to program |
| ~2014 | **Apache Spark** | In-memory, much faster, one engine for SQL, streaming, ML | Still needed a reliable storage layer |
| 2010s | **Cloud object storage** (S3, ADLS) + **data lakes** | Very cheap, unlimited storage of any format; storage separate from compute | No transactions, no schema enforcement, no updates → "data swamp" |
| ~2019+ | **Lakehouse** (Delta Lake, Iceberg, Hudi; Databricks) | Warehouse reliability (ACID, schema, DML) on cheap open lake storage | — the current mainstream |

Knowing *why* each tool exists lets you answer "Why Spark?" or "Why Delta?" from reasoning instead of memory.

### 5. The data lifecycle

```
 ┌─────────┐   ┌─────────┐   ┌─────────┐   ┌───────────┐   ┌─────────┐
 │ SOURCE  │──▶│ INGEST  │──▶│  STORE  │──▶│ TRANSFORM │──▶│  SERVE  │
 └─────────┘   └─────────┘   └─────────┘   └───────────┘   └─────────┘
 ═══════════════ UNDERCURRENTS (apply to every stage) ═══════════════
  orchestration · data quality · monitoring · security · governance · cost · idempotency
```

| Stage | Key question | Decisions a DE makes | ShopCo example |
|---|---|---|---|
| **Source** | Where does data come from and how does it change? | Pull vs push; full extract vs changes only; API limits; who owns it | Postgres `orders`, partner payment CSVs, click JSON events, CRM API |
| **Ingest** | How do we copy it in safely and completely? | Batch vs streaming; frequency; handling late, duplicate or malformed files | Hourly incremental extract of orders; Auto Loader for payment CSVs |
| **Store** | Where, in what format, for how long? | Lake vs warehouse; Delta/Parquet; folder/table layout; retention; access | Delta tables in Unity Catalog, Bronze/Silver/Gold |
| **Transform** | How do we make it correct and useful? | Types, dedup, joins, business rules, history (SCD), aggregations | Dedupe payments, join orders ↔ customers, daily revenue |
| **Serve** | Who uses it and how? | BI tables, ML features, APIs, exports; freshness SLAs | Revenue dashboard ready by 07:00; churn features for the DS team |

### 6. The undercurrents — what makes it *engineering*

| Undercurrent | Question it answers | Example |
|---|---|---|
| **Orchestration** | What runs, when, in which order, and what happens on failure? | Revenue builds only after orders *and* payments load; retry twice |
| **Data quality** | Is the data correct? | `amount > 0`, `order_id` unique and not null; bad rows quarantined |
| **Monitoring & alerting** | Do we know about problems before users do? | Alert if today's row count is < 50 % of the 7-day average |
| **Idempotency** | Is a re-run safe? | Re-running yesterday's load doesn't double revenue |
| **Security & governance** | Who can see what? Where did this number come from? | Emails masked except for the CRM team; lineage from dashboard back to source |
| **Cost** | Are we paying for value? | Job clusters that shut down after the run; no 24/7 idle clusters |

### 7. What is a data pipeline?

A **data pipeline** is a repeatable sequence of steps that moves data from one or more sources to a destination, transforming it along the way.

| Script | Production pipeline |
|---|---|
| Runs once, on someone's laptop | Runs on schedule or on events, on managed compute |
| Hard-coded paths and dates | Parameterised (environment, run date) |
| Fails silently or crashes | Logs, alerts, retries, timeouts |
| Re-running may corrupt data | Idempotent — re-runs are safe |
| No checks | Data-quality rules, reconciliation |
| Lives in a personal folder | Version-controlled, tested, deployed through CI/CD |

**Vocabulary you will need from day 1** (each gets a deep dive later):

| Term | Meaning |
|---|---|
| **Notebook** | Interactive document with code cells — where you *write and develop* code. |
| **Task** | One unit of work in a job (e.g. "run this notebook", "run this SQL"). |
| **Job** | A scheduled/triggered collection of tasks with dependencies, compute and settings. |
| **Workflow** | Generic word for orchestrated steps; on Databricks, "Workflows" was the product name for Jobs (now Lakeflow Jobs). |
| **Pipeline** | Generic: data flow from source to target. On Databricks, also a specific product: Lakeflow Spark Declarative Pipelines (formerly DLT). |

### 8. Roles compared

| Role | Main question | Typical output | Main tools |
|---|---|---|---|
| **Data Engineer** | Is the right data available, correct, on time, at acceptable cost? | Pipelines, tables, platform | Spark, SQL, Python, Delta, orchestration |
| **Analytics Engineer** | Is the business logic modelled well and consistently? | Gold/semantic models, metric definitions | SQL, dbt, BI semantic layers |
| **Data Analyst** | What happened, and why? | Dashboards, reports, ad-hoc analysis | SQL, Power BI/Tableau |
| **Data Scientist** | What will happen? What drives it? | Models, experiments, insights | Python, statistics, ML libraries |
| **ML Engineer** | How do we run models reliably in production? | Model serving, feature pipelines, monitoring | MLflow, serving, feature stores |

The DE builds the **supply chain**; everyone else consumes from it. If the supply chain is wrong, every downstream number is wrong.

### 9. How Databricks maps to the lifecycle (preview)

| Stage | Databricks capability |
|---|---|
| Ingest | Auto Loader, Lakeflow Connect, COPY INTO, Structured Streaming, JDBC |
| Store | Delta Lake tables on cloud storage, governed by Unity Catalog |
| Transform | Spark (PySpark/SQL), Lakeflow Spark Declarative Pipelines |
| Orchestrate | Lakeflow Jobs (Workflows) |
| Serve | Databricks SQL warehouses, dashboards, model serving, Delta Sharing |
| Govern | Unity Catalog: permissions, lineage, audit, tags |

### 10. Misconceptions

1. **"The job succeeded, so the data is correct."** Success means *the code didn't crash*. It can still load zero rows, duplicates, or yesterday's file again — a **silent failure**. Always check data, not just status.
2. **"DE is just moving data from A to B."** Copying is easy. Copying *correctly, completely, repeatedly, recoverably and cheaply* is the job.
3. **"Slow job? Add more nodes."** Skew, full reloads and small files are design problems. More nodes make them more expensive, not faster.
4. **"Bad source data isn't my problem."** You can't fix the source, but you must *detect* it, *isolate* it (quarantine) and *alert* the owner.
5. **"Data engineering is a tool skill."** Tools change every few years. Concepts — incremental loads, idempotency, partitioning, modelling — don't.

---

## B. Practical — a mini pipeline touching every stage

**Runnable:** [`exercises/phase-01/01_mini_pipeline.py`](../../exercises/phase-01/01_mini_pipeline.py) · **Data:** [`data/orders_2026-10-16.csv`](../../data/orders_2026-10-16.csv)

### Input

| order_id | customer_id | region | amount | order_ts |
|---|---|---|---|---|
| 101 | C1 | North | 250.00 | 2026-10-16 09:15:00 |
| 102 | C2 | South | 100.00 | 2026-10-16 10:02:00 |
| 102 | C2 | South | 100.00 | 2026-10-16 10:02:00 |
| 103 | C3 | North | -40.00 | 2026-10-16 11:30:00 |
| 104 | C1 | North | 75.50 | 2026-10-16 14:45:00 |

### Code

```python
from pyspark.sql import functions as F                                    # 1

run_date = dbutils.widgets.get("run_date") if "dbutils" in globals() else "2026-10-16"   # 2

raw = (spark.read                                                          # 3
       .option("header", "true")                                           # 4
       .csv(f"/Volumes/shopco/raw/orders/{run_date}/"))                    # 5

clean = (raw                                                               # 6
         .withColumn("amount", F.expr("try_cast(amount AS DECIMAL(10,2))"))   # 7
         .withColumn("order_ts", F.expr("try_to_timestamp(order_ts)"))        # 8
         .dropDuplicates(["order_id"])                                     # 9
         .filter(F.col("amount") > 0)                                      # 10
         .filter(F.to_date("order_ts") == F.lit(run_date)))                # 11

daily_revenue = (clean                                                     # 12
                 .groupBy(F.to_date("order_ts").alias("order_date"), "region")
                 .agg(F.sum("amount").alias("revenue"),
                      F.count("order_id").alias("orders")))

(daily_revenue.write                                                       # 13
   .format("delta")
   .mode("overwrite")
   .option("replaceWhere", f"order_date = '{run_date}'")                   # 14
   .saveAsTable("shopco.gold.daily_revenue"))
```

> In a real notebook you would create the widget once with `dbutils.widgets.text("run_date", "2026-10-16")`.

### Line by line

| Line | What it does | Engineering reason |
|---|---|---|
| 1 | Imports Spark's built-in functions | Built-ins run inside Spark's optimised engine; prefer them over Python UDFs |
| 2 | Reads the run date as a **parameter** | Lets you re-run or backfill *any* date; "today" inside the code breaks re-runs after midnight |
| 3–5 | **Ingest** — reads that day's CSV folder | Without a schema, **every column is a string** — types must be fixed explicitly |
| 7 | `try_cast` to DECIMAL | `decimal` not `float`: floats can't represent 0.10 exactly → money rounding errors. `try_cast` returns `null` for bad values **regardless of ANSI mode** (see box below) |
| 8 | Parses the timestamp; bad values → `null` | Same reasoning |
| 9 | Removes duplicate `order_id`s | Order 102 arrived twice (source retry). **Only within this batch** — see T2 |
| 10 | Quality rule: `amount > 0` | Order 103 (−40) is removed. In production, **quarantine** it (topic 1.9) instead of dropping silently. Note: `null > 0` is `null`, so null amounts are also filtered out |
| 11 | Keeps only rows for the run date | Must match the `replaceWhere` predicate, otherwise the write fails |
| 12 | **Transform** — revenue & count per date & region | Business logic lives in one place |
| 13–14 | **Serve, idempotently** — replaces only the `order_date = run_date` slice | Run it 1× or 5× → same result. `append` would add the rows again every re-run |

> **Configuration-dependent behaviour — `cast` vs `try_cast`.**
> With ANSI mode **off** (default in Spark 3.x), `cast('25O.00' AS DECIMAL)` returns `null` silently.
> With ANSI mode **on** (default in Spark 4.x, newer Databricks Runtimes and Databricks SQL), the same `cast` **raises an error** and the job fails.
> `try_cast` always returns `null` on failure. Choose deliberately: fail fast, or null + quarantine — but never be surprised.

### Expected output — `shopco.gold.daily_revenue`

| order_date | region | revenue | orders |
|---|---|---|---|
| 2026-10-16 | North | 325.50 | 2 |
| 2026-10-16 | South | 100.00 | 1 |

North = 250.00 + 75.50 (order 103 rejected). South = 100.00 (order 102 counted once).

### Common mistakes

- Using `mode("append")` for a daily aggregate → every retry duplicates revenue.
- Using `current_date()` instead of a parameter → a re-run at 00:30 processes the wrong day.
- Using `float`/`double` for currency.
- Dropping bad rows without recording them → nobody knows data was lost.
- Trusting "Succeeded" without checking row counts.

### Design exercise (paper, 10 min)

Draw ShopCo's lifecycle for **three** sources: Postgres orders, partner payment CSVs, click JSON events. On every arrow, write one thing that can go wrong and how you would detect it.

---

## C. Interview questions — answer *before* opening the model answers

### 🟢 Basic

**B1.** What is data engineering? Answer in 2–3 sentences as if asked cold.

<details><summary>Model answer</summary>

Data engineering is designing, building and operating systems that move data from source systems, transform it into reliable and usable datasets, and deliver it to analysts, applications and models. The key word is *operating*: the pipelines must run repeatedly, at scale, and produce correct results even when things fail or are re-run.
</details>

**B2.** How is a Data Engineer's job different from a Data Analyst's and a Data Scientist's?

<details><summary>Model answer</summary>

The DE builds and runs the data supply chain — ingestion, storage, transformation, quality, orchestration. The Analyst *uses* that data to explain what happened (dashboards, reports). The Data Scientist *uses* it to predict what will happen (models). If the DE's data is wrong, every analysis and model downstream is wrong.
</details>

**B3.** Name the five lifecycle stages and one decision a DE makes at each.

<details><summary>Model answer</summary>

Source (full vs incremental extract), Ingest (batch vs streaming, how to handle late/duplicate files), Store (format such as Delta, layout, retention), Transform (dedup, types, business rules, history), Serve (which tables/warehouse, freshness SLA). Plus undercurrents: orchestration, quality, monitoring, security, cost.
</details>

### 🟡 Medium

**M1.** Analysts want to run heavy revenue queries directly on the production Postgres database. Give at least two technical reasons this is a bad idea, and what you would offer instead.

<details><summary>Model answer</summary>

1. **Resource contention:** long scans consume CPU, memory, I/O and can hold locks → checkout slows or times out.
2. **Wrong storage shape:** Postgres is row-oriented and tuned for single-row lookups; a revenue query must read every column of every row.
3. **No history:** updates overwrite old values, so trends over time are impossible.
4. **One system only:** payments, clicks and CRM data aren't there.
5. **Security:** analysts get access to raw PII in a production system.

Offer: replicate incrementally (or via CDC) into the lakehouse, model Gold tables, serve them via a SQL warehouse. Short term, a read replica reduces load on the primary, but it doesn't fix storage shape, history or integration.
</details>

**M2.** Why is `run_date` a parameter rather than "today" computed inside the code? Describe a real situation where it matters.

<details><summary>Model answer</summary>

It makes the job deterministic and re-runnable for any date. Situations: (a) the 23:50 run fails, the retry starts at 00:10 — with "today" it would process the wrong day and skip yesterday; (b) a bug is found and the last 30 days must be **backfilled**; (c) a partner sends a late file for last Tuesday; (d) testing with a fixed date.
</details>

**M3.** The value `"25O.00"` (letter O instead of zero) arrives in `amount`. What happens in this pipeline, why is it dangerous, and how would you detect it?

<details><summary>Model answer</summary>

`try_cast` turns it into `null`; `null > 0` evaluates to `null`, so the filter drops the row. The job succeeds, but revenue is **understated silently**. (With plain `cast` and ANSI on, the job would fail instead — loud but blocks the whole load; with ANSI off it would behave like `try_cast`.)

Detect: compare rows where the raw value is non-null but the cast value is null; route those to a quarantine table with a reason; alert above a threshold; reconcile counts (input = valid + quarantined).
</details>

### 🔴 Tricky

**T1.** The job shows ✅ *Succeeded*, but yesterday's revenue on the dashboard is **exactly double** the real number. Give three possible causes. Which do you check first, and why?

<details><summary>Model answer</summary>

"Exactly double" suggests the *whole* dataset was counted twice, not random bad rows.

1. **Re-run with append** — someone re-ran the job (or a retry fired) and the write appends.
2. **Join fan-out** — a dimension (e.g. customers) now has two rows per key, so every order matches twice.
3. **Source delivered twice** under a different file name / batch id and dedup doesn't catch it (e.g. dedup key includes the file name).
4. **BI layer double counting** — the dashboard sums two overlapping grains or two copies of the table.

Check first: the **run history** (did it run twice?) and a quick query `SELECT order_id, COUNT(*) ... HAVING COUNT(*) > 1` on the gold source data — both cheap and they split causes into "pipeline wrote twice" vs "logic multiplies rows".
</details>

**T2.** Order 102 reappears in **tomorrow's** file (a late resend, maybe with a corrected amount). Does `dropDuplicates` protect you? What happens to the numbers?

<details><summary>Model answer</summary>

`dropDuplicates` only works **within one DataFrame (this batch)** — it knows nothing about yesterday's data. In *this* code, line 11 keeps only rows whose `order_ts` date equals tomorrow's run date, so order 102 (dated 2026-10-16) is **silently dropped**: no double counting, but if the resend carried a correction, the correction is lost and 2026-10-16 is never recomputed.

Fix: maintain an order-level Silver table with `MERGE` on `order_id` (latest version wins), then recompute Gold for **all affected dates**, not just the run date.
</details>

**T3.** A manager says: *"We don't need a data engineer — an analyst can export from Postgres to Excel every morning."* When is that genuinely fine, and what specific signals tell you it no longer is?

<details><summary>Model answer</summary>

Fine when: one small source, low volume, one consumer, non-critical decisions, no history needed, manual errors are tolerable.

Signals it's no longer enough: multiple sources must be combined; volume exceeds Excel/laptop limits; teams report different numbers for the same metric; history/trends are needed; freshness SLAs; manual steps get missed during holidays; PII/compliance requirements; the export itself slows production; the business wants automation, ML or self-service.
</details>

---

## D. Evaluation
Answers are graded in [`interview/answers-log.md`](../../interview/answers-log.md).

## E. Revision summary

- DE = **design, build, operate** pipelines: source → ingest → store → transform → serve.
- Exists because operational systems and analytics need data in **different shapes**.
- The **undercurrents** (orchestration, quality, monitoring, idempotency, security, cost) make it engineering.
- A production pipeline is **parameterised, idempotent, monitored, tested, version-controlled**.
- "Succeeded" ≠ correct — check the data, not just the status.