# 7.1 Ingestion Types — File, API, Database, Streaming

**Time:** 15 min · **Prerequisites:** 1.6, 1.7, 1.8

**After this topic you can:**
- define ingestion and explain why it is the most fragile part of a pipeline;
- name the four ingestion families (file, API, database, event stream) and the Databricks tool for each;
- explain pull vs push, and full vs incremental *ingestion* (building on 1.7);
- choose an ingestion approach for a new source and defend it.

---

## A. Concept deep dive

### 1. The simplest explanation

**Ingestion** = getting data from a system you do **not** control into the platform you **do** control (Bronze), reliably and repeatably.

Everything after Bronze is your code on your data. Ingestion is the boundary where *other people's* systems, schedules, formats and mistakes enter. That is why most production incidents start here: a file arrives late, twice, half-written, or with a new column.

### 2. The four ingestion families

| Family | Example at ShopCo | How data moves | Typical Databricks tool |
|---|---|---|---|
| **Files** (object storage, SFTP) | Partner drops `orders_*.json` into ADLS every 15 min | Source **pushes** files to a landing folder; you discover new files | **Auto Loader** (`cloudFiles`), `read_files`, `COPY INTO` (legacy SQL) |
| **API** (REST/SaaS) | Product catalog from a REST API; Salesforce accounts | You **pull** pages of JSON; rate limits, pagination, auth tokens | Lakeflow Connect **managed connector** if one exists (e.g. Salesforce); otherwise a Python job that lands raw JSON in a volume, then Auto Loader |
| **Database** (OLTP) | `customers` table in Azure SQL / SQL Server | **Pull** with JDBC queries (full or watermark) **or** read the **change log** (CDC) | Lakeflow Connect managed connectors (e.g. SQL Server, with CDC); JDBC (`spark.read.format("jdbc")`); ADF Copy (Phase 10); Lakehouse Federation to query in place |
| **Event streams** (message bus) | Payment events in Azure Event Hubs | Producers **push** events to a log; you read by **offset** | Structured Streaming Kafka source (Event Hubs exposes a Kafka-compatible endpoint); Kinesis, Pub/Sub, Pulsar sources |

> **Naming (verify for your workspace):** *Lakeflow Connect* is Databricks' umbrella name for ingestion. It includes **managed connectors** (fully managed and source-specific: auth, CDC, retries, schema evolution) and the SQL/Spark options (Auto Loader, Kafka, …). The connector list changes often — check the docs before naming one in an interview.

### 3. Why each family is hard

| Family | What breaks | Defence |
|---|---|---|
| Files | Late, duplicate, partial, overwritten, renamed files; schema drift; tiny files | Auto Loader file tracking + schema location + rescued data; immutable files; audit columns |
| API | Rate limits, pagination bugs, token expiry, API version changes, no "changed since" filter | Land raw responses first (replayable), retries with back-off, store the cursor *after* a successful write |
| Database | Load on production OLTP, missing `updated_at`, hard deletes invisible to watermarks, long transactions | CDC instead of polling, read replicas, partitioned JDBC reads, overlap + MERGE (1.7) |
| Streams | Retention (events expire after N days), ordering only per partition, duplicates on producer retry, late events | Checkpointed offsets, watermarks, dedup on an event ID, idempotent sinks |

### 4. Pull vs push

```
 PULL (you ask)                          PUSH (they send)
 ┌──────────┐  query / GET   ┌───────┐   ┌──────────┐  file / event   ┌──────────┐
 │ pipeline │ ─────────────▶ │source │   │  source  │ ──────────────▶ │ landing  │
 └──────────┘ ◀───────────── └───────┘   └──────────┘                 │ (ADLS /  │
              rows / pages                                            │  Kafka)  │
                                                                      └────┬─────┘
 You control timing; you load the source.                    you discover │ new data
                                                             ┌────────────▼┐
                                                             │  pipeline   │
                                                             └─────────────┘
```

- **Pull** (JDBC, API): you decide when and what; you must not overload the source and must remember where you stopped.
- **Push** (files, events): the source decides when; you must **discover** what is new and survive bursts, duplicates and late arrivals. A **landing zone** (folder or topic) decouples the two sides.

### 5. Full vs incremental ingestion (the ingestion view of 1.7)

| | Full ingestion | Incremental ingestion |
|---|---|---|
| Files | Re-read the whole folder every run (`spark.read`) | Read only **files not seen before** (Auto Loader / file-source checkpoint) |
| Database | `SELECT *` snapshot, overwrite | Watermark query (`updated_at > x`) or **CDC** log |
| API | Download everything | `?modified_since=` cursor, or webhooks |
| Stream | Not applicable (incremental by nature) | Read from the last committed **offset** |

The shift in Phase 7: for files and streams, **Structured Streaming keeps the "what have I already processed" bookkeeping for you** in a checkpoint. You don't build a control table — but you must protect the checkpoint (7.3).

### 6. ShopCo ingestion map

```
 Partner SFTP ──(ADF copy)──▶ ADLS landing/orders/*.json ──Auto Loader──▶ bronze_orders
 Azure SQL customers ──(CDC managed connector, or JDBC watermark)───────▶ bronze_customers
 Event Hubs "payments" ──(Kafka endpoint, Structured Streaming)─────────▶ bronze_payments
 Catalog REST API ──(Python job lands raw JSON in a volume)──Auto Loader──▶ bronze_products
```

The pattern: **land raw first, then ingest with one well-understood tool.** Even API data is written as raw files so it can be replayed.

### 7. Decision guide

1. Is there a **managed connector** for this source? → use it (least code; CDC and schema handled).
2. Data is **files in cloud storage**? → **Auto Loader** (continuous or `availableNow`).
3. An **event bus**? → Structured Streaming Kafka/Kinesis/… source → Delta Bronze.
4. A **database** without a connector? → JDBC with partitioned reads + watermark, or a log-based CDC tool feeding files/events.
5. A **one-off** load of a fixed set of files? → batch `spark.read` / `read_files` is fine.

### 8. Misconceptions

1. **"Ingestion is just copying."** It is copying *plus* discovery, idempotency, schema handling, auditing and recovery.
2. **"JDBC polling is CDC."** Polling `updated_at` misses hard deletes and unmaintained timestamps; real CDC reads the database log.
3. **"Kafka gives exactly-once by itself."** Kafka gives a replayable log, ordered per partition. Exactly-once *results* come from checkpointed offsets + an idempotent sink.
4. **"Streaming ingestion must run 24/7."** File and Kafka ingestion can run with `availableNow` on a schedule.

---

## B. Practical — one snippet per family

**Runnable:** files → [`02_autoloader_schema_drift.py`](../../exercises/phase-07/02_autoloader_schema_drift.py). The snippets below show the shape of the other families (they need real credentials, so they are not in a notebook).

```python
from pyspark.sql import functions as F

# 1) DATABASE via JDBC — incremental pull with parallel reads
jdbc_df = (spark.read.format("jdbc")
  .option("url", "jdbc:sqlserver://shopco-sql.database.windows.net:1433;database=shop")        # 1
  .option("dbtable", "(SELECT * FROM dbo.customers WHERE updated_at > '2026-10-16 00:00') q")  # 2
  .option("partitionColumn", "customer_num")                                                   # 3
  .option("lowerBound", "1").option("upperBound", "5000000").option("numPartitions", "8")
  .option("user", dbutils.secrets.get("shopco", "sql-user"))                                   # 4
  .option("password", dbutils.secrets.get("shopco", "sql-pass"))
  .load())

# 2) EVENT STREAM via the Kafka source (Event Hubs Kafka endpoint)
EH = "shopco-ns.servicebus.windows.net:9093"
conn = dbutils.secrets.get("shopco", "eventhub-conn-string")
payments = (spark.readStream.format("kafka")
  .option("kafka.bootstrap.servers", EH)
  .option("subscribe", "payments")                                                             # 5
  .option("startingOffsets", "earliest")                                                       # 6
  .option("kafka.security.protocol", "SASL_SSL")
  .option("kafka.sasl.mechanism", "PLAIN")
  .option("kafka.sasl.jaas.config",
          'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required '
          f'username="$ConnectionString" password="{conn}";')                                  # 7
  .load()
  .select(F.col("key").cast("string"), F.col("value").cast("string").alias("payload"),        # 8
          "topic", "partition", "offset", F.col("timestamp").alias("kafka_ts")))
```

| # | Explanation |
|---|---|
| 1 | JDBC URL of the source DB. Read from a **replica** if possible |
| 2 | Push the incremental filter **down** to the database as a subquery so only changed rows travel |
| 3 | `partitionColumn` + bounds + `numPartitions` → 8 parallel range queries instead of one. Needs a numeric/date column with an even spread |
| 4 | Credentials from a **secret scope** — never in code |
| 5 | Topic (= Event Hub name) to subscribe to |
| 6 | Used **only on the first start** with an empty checkpoint; afterwards the checkpoint's offsets win. Streaming default is `latest` |
| 7 | Event Hubs Kafka auth pattern; on Databricks the Kafka client classes are shaded (`kafkashaded.`) — verify for your runtime |
| 8 | Kafka returns **binary** key/value plus metadata. Keep `partition` + `offset` in Bronze — together with topic they identify every event uniquely (dedup and audit key) |

**Design exercise (5 min):** for each ShopCo source in §6 write: pull or push? full or incremental? which tool? what is the dedup key? what happens if the job is down for 3 days? (Hint for Event Hubs: retention — if events expire before you read them, they are gone.)

**Common mistakes**

| Mistake | Consequence |
|---|---|
| Single-partition JDBC read of a 200 GB table | One task, hours, source timeouts |
| Incremental filter applied in Spark *after* `load()` instead of inside the subquery | Spark may still pull far more than needed; always check the pushed-down query |
| Not storing Kafka `partition`/`offset` | No reliable dedup or audit key later |
| Calling the API from transformation code | Nothing to replay when logic changes; API limits hit on every rebuild |

---

## C. Interview questions

### 🟢 Basic

**B1.** What is data ingestion, and why is it considered the riskiest stage of a pipeline?

<details><summary>Model answer</summary>

Ingestion moves data from external source systems into the platform's raw layer reliably and repeatedly. It is risky because it is the boundary with systems you don't control: their schedules, formats, schemas, duplicates and outages hit you here first. Downstream logic is your code on your data; ingestion must survive other people's changes.
</details>

**B2.** Name the four main ingestion families and a Databricks tool for each.

<details><summary>Model answer</summary>

Files in object storage → Auto Loader. APIs/SaaS → a Lakeflow Connect managed connector if available, otherwise custom code that lands raw JSON. Databases → managed connectors with CDC, JDBC reads (watermark), or external CDC tools. Event streams → Structured Streaming with the Kafka (Event Hubs), Kinesis or Pub/Sub source.
</details>

**B3.** What is the difference between pull and push ingestion?

<details><summary>Model answer</summary>

Pull: the pipeline queries the source (JDBC, API) on its own schedule and must track where it stopped and avoid overloading the source. Push: the source delivers files/events to a landing zone on its own schedule; the pipeline must discover new data and cope with bursts, duplicates and late arrivals.
</details>

### 🟡 Medium

**M1.** Why land API responses as raw files before parsing, instead of writing parsed rows straight to Silver?

<details><summary>Model answer</summary>

Replay and audit: API data is often not re-fetchable (no history, rate limits, values change). Raw files become an immutable Bronze source; parsing bugs can be fixed and re-run without calling the API again, and calling the API is decoupled from transforming. It also lets one tool (Auto Loader) handle all file-shaped ingestion.
</details>

**M2.** Your JDBC ingestion of a large table takes 3 hours and the DBA complains. What do you change?

<details><summary>Model answer</summary>

Push the incremental filter into the query (watermark on an indexed `updated_at`, overlap + MERGE downstream), parallelise with `partitionColumn`/bounds/`numPartitions` within what the DB tolerates, read from a replica, schedule off-peak — and ideally replace polling with log-based CDC (managed connector), which reads only changes and also captures deletes.
</details>

**M3.** Kafka `startingOffsets` is `earliest` on a query that has run for a month. You restart it. Does it re-read the whole topic?

<details><summary>Model answer</summary>

No. `startingOffsets` applies only when the query starts with a new, empty checkpoint. On restart, the offsets stored in the checkpoint decide where to resume. Only deleting or changing the checkpoint makes `startingOffsets` matter again.
</details>

### 🔴 Tricky

**T1.** The Event Hubs ingestion job was broken for 9 days; hub retention is 7 days. What happened and what do you do?

<details><summary>Model answer</summary>

Events older than the retention were deleted before being read — that data is gone from the bus. On restart the checkpoint points at offsets that no longer exist; the Kafka source fails by default (`failOnDataLoss` = true) instead of silently skipping. Recover the lost period from another source (producer database, Event Hubs Capture archive), and only accept the gap with explicit sign-off (`failOnDataLoss=false`). Prevent it: alert on failure/lag well inside retention, longer retention, or Capture to storage as a replayable archive.
</details>

**T2.** A colleague says: "We poll `customers` every 15 minutes with `updated_at > last_run`, so we have CDC." Challenge this.

<details><summary>Model answer</summary>

That is timestamp-based incremental extraction, not CDC. It misses hard deletes, updates that don't touch `updated_at`, and rows from long transactions committed with older timestamps; it can't see intermediate versions between polls; and it loads the source. True CDC reads the database's transaction log (inserts, updates, deletes, in order). Either adopt log-based CDC or add soft-delete flags, overlap windows + MERGE, and periodic full reconciliation.
</details>

**T3.** An API has no "modified since" filter and returns 2 million records. How do you ingest it incrementally?

<details><summary>Model answer</summary>

You can't *extract* incrementally, but you can *process* incrementally: land a full snapshot as raw files each run (cheap, replayable), derive changes by comparing to the previous snapshot (hash per key → inserts/updates/deletes), and MERGE only the changes into Silver. Also ask the provider for a cursor or webhook, and respect rate limits with paging and back-off.
</details>

---

## E. Revision summary

- Ingestion = external system → Bronze, reliably and repeatably; the boundary with what you don't control.
- Families: **files** (Auto Loader), **API** (connector, or land raw JSON), **database** (connector/CDC, or JDBC watermark), **streams** (Kafka/Event Hubs source).
- **Pull** = you track position and protect the source; **push** = you discover new data and survive duplicates and late arrivals.
- Land raw first → replayable. Keep source identifiers (file path; Kafka topic+partition+offset).
- `startingOffsets` and other "initial" options only matter for a **new** checkpoint.