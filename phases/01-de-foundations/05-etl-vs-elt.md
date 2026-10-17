# 1.5 ETL vs ELT

**Time:** 20 min · **Prerequisites:** 1.3

**After this topic you can:**
- explain both patterns, why ETL came first and why ELT became dominant;
- explain the trade-offs (replay, cost, compliance);
- classify real architectures (ADF + Databricks, medallion) correctly.

---

## A. Concept deep dive

### 1. The simplest explanation

Both patterns have the same three steps — **Extract** (read from source), **Transform** (clean/reshape), **Load** (write to target). The difference is **where and when** the transformation happens.

```
ETL:  Source ──Extract──▶ [ separate transform engine ] ──Load──▶ Warehouse (only clean data)

ELT:  Source ──Extract──▶ ──Load──▶ Lake/Lakehouse (raw) ──Transform inside the platform──▶ clean tables
```

### 2. Why ETL came first

In the 1990s–2000s, warehouse storage and compute were **expensive and fixed**. You didn't want raw junk in the warehouse, and you didn't want transformations competing with BI queries. So a dedicated ETL server (Informatica, SSIS, DataStage) transformed data **before** loading it.

### 3. Why ELT took over

Cloud changed the economics:
- object storage became very cheap → storing raw data is affordable;
- compute became elastic (Spark clusters, SQL warehouses) → transform at scale *inside* the platform;
- data volume and variety grew → you can't always know in advance how data will be used.

So: **land raw first, transform later, inside the lakehouse.**

### 4. Trade-offs

| | ETL | ELT |
|---|---|---|
| Where transform runs | External engine before load | Inside the target platform (Spark, SQL) |
| Raw data kept? | Usually not | **Yes** — can **replay/reprocess** when logic changes or bugs are found |
| Flexibility | New requirement → change pipeline and re-extract | New requirement → new transformation on existing raw data |
| Time to land data | Slower (transform first) | Fast (load first) |
| Storage | Less | More (raw + curated) |
| Compliance | Sensitive fields can be removed before they ever land | Raw PII lands → needs strong governance, masking, retention |
| Typical tools | Informatica, SSIS, DataStage | ADF/Fivetran/Auto Loader for EL + Spark/SQL/dbt for T |

### 5. Real architectures are usually a mix

- **ADF Copy → ADLS → Databricks notebooks** = ELT: ADF does Extract-Load, Databricks transforms.
- **Medallion** = ELT: Bronze is the raw Load; Silver/Gold are in-platform Transforms.
- Small transformations often happen *before* landing — e.g. hashing a national ID, converting encodings, decompressing — sometimes called **EtLT** (small t first, big T later).
- **Reverse ETL** = pushing curated data *from* the lakehouse back into operational tools (CRM, marketing platforms).

### 6. Misconceptions

1. **"Spark transforms data before writing to Delta, so Databricks is ETL."** The classification is about architecture: if raw data is landed first and transformed inside the platform, it's ELT — even though each Spark job itself reads, transforms and writes.
2. **"ELT means no transformation logic upfront."** Requirements and modelling still matter; ELT just defers *where* it runs.
3. **"ELT is always cheaper."** Storing everything raw forever and recomputing big transforms can be expensive — retention policies and incremental processing matter.

---

## B. Practical — ELT in Databricks SQL

```sql
-- 1. LOAD raw files as-is into a Bronze table (E + L)
CREATE OR REPLACE TABLE main.de_prep.bronze_orders AS
SELECT *,
       _metadata.file_path         AS _source_file,
       current_timestamp()         AS _ingested_at
FROM read_files(
  '/Volumes/shopco/raw/orders/',
  format => 'csv',
  header => true);

-- 2. TRANSFORM inside the platform into a clean Silver table (T)
CREATE OR REPLACE TABLE main.de_prep.silver_orders AS
SELECT  CAST(order_id AS INT)                  AS order_id,
        customer_id,
        region,
        try_cast(amount AS DECIMAL(10,2))      AS amount,
        try_to_timestamp(order_ts)             AS order_ts
FROM    main.de_prep.bronze_orders
QUALIFY row_number() OVER (PARTITION BY order_id ORDER BY _ingested_at DESC) = 1;
```

| Part | Explanation |
|---|---|
| `read_files(...)` | Databricks SQL table-valued function that reads files in a path with a given format |
| `_metadata.file_path` | Hidden file-metadata column: which file each row came from — essential for debugging and audit |
| `current_timestamp()` | When we ingested the row |
| Bronze keeps strings | Raw fidelity: nothing is lost if a cast would fail |
| Silver casts with `try_cast` | Bad values become null (and should be quarantined — topic 1.9) |
| `QUALIFY row_number() ... = 1` | Keeps one row per `order_id` (latest ingested). `QUALIFY` filters on a window function result |

> This is a full-refresh illustration (`CREATE OR REPLACE`). Incremental versions come in topics 1.7 and Phase 7.

**Why ELT helps here:** if the business later says "amount should exclude tax", you rebuild Silver from Bronze — no need to re-extract from the source (which may no longer have the old data).

---

## C. Interview questions

### 🟢 Basic

**B1.** What is the difference between ETL and ELT?

<details><summary>Model answer</summary>

Both extract, transform and load. In ETL, data is transformed in a separate engine before being loaded, so only clean data reaches the target. In ELT, raw data is loaded first into the lake/lakehouse and transformed there using the platform's compute.
</details>

**B2.** Why did ELT become popular in the cloud?

<details><summary>Model answer</summary>

Cheap object storage made keeping raw data affordable, and elastic compute (Spark, SQL warehouses) made transforming large data inside the platform practical. Keeping raw data also enables reprocessing when logic changes.
</details>

### 🟡 Medium

**M1.** Your company uses ADF to copy data from SQL Server to ADLS, then Databricks notebooks build Silver and Gold tables. ETL or ELT? Justify.

<details><summary>Model answer</summary>

ELT: ADF performs Extract and Load of raw data into the lake; transformations run afterwards inside the lakehouse with Databricks. Raw data is preserved in the landing/Bronze layer for replay.
</details>

**M2.** Give one strong advantage of keeping raw data, and one risk.

<details><summary>Model answer</summary>

Advantage: replay — fix a bug or add a new rule and rebuild downstream tables from raw without re-extracting from a source that may have changed or purged history. Risk: raw data may contain PII/sensitive data and grows indefinitely — needs access control, masking, encryption and retention policies.
</details>

### 🔴 Tricky

**T1.** Compliance says national ID numbers must never be stored in the lake. Can you still use ELT?

<details><summary>Model answer</summary>

Yes, with a small transformation before landing (EtLT): drop, hash or tokenise the field during extraction (e.g. in the ingestion tool or source query), then land the rest raw and transform later. You keep ELT's benefits for everything except the forbidden field.
</details>

**T2.** "Each Spark job reads, transforms and writes — so every Spark pipeline is ETL." Respond.

<details><summary>Model answer</summary>

That confuses a single job with the architecture. ETL/ELT describes whether data is transformed before landing in the analytical platform or after. If raw data is landed first (Bronze) and Spark jobs transform it inside the lakehouse, the architecture is ELT, even though each job does read → transform → write.
</details>

---

## E. Revision summary

- Same steps; difference = **where/when transform runs**.
- ETL: transform before load (expensive warehouses era). ELT: load raw, transform in platform (cloud era).
- ELT's superpower is **replay**; its risk is **raw sensitive data** → governance.
- ADF Copy + Databricks transforms, and medallion architecture, are ELT.
- EtLT: small compliance transforms before landing.