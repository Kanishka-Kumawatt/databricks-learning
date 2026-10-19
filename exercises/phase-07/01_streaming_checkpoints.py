# Databricks notebook source
# Phase 7.2-7.3 — Structured Streaming model, triggers, output modes and checkpoint anatomy.
#
#   Part 1  A plain Spark file-source stream (no Auto Loader) with trigger(availableNow=True).
#   Part 2  Open the checkpoint: metadata, offsets/, commits/, sources/ — and read them.
#   Part 3  New file -> only the new file is processed. No new file -> nothing happens.
#   Part 4  "Lose" the checkpoint (new path) -> everything is reprocessed -> duplicates.
#   Part 5  Output modes: which combinations Spark and the Delta sink accept.
#
# Universal Spark Structured Streaming behaviour unless marked "Databricks".

# COMMAND ----------

import json
from pyspark.sql import functions as F

CATALOG, SCHEMA = "main", "de_prep"          # change to a catalog/schema you can write to
VOLUME = "phase07"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")
T = lambda name: f"{CATALOG}.{SCHEMA}.{name}"

BASE = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/nb01"
SRC = f"{BASE}/landing/orders"
CHK = f"{BASE}/_checkpoints/orders_stream"
try:
    dbutils.fs.rm(BASE, True)
except Exception:
    pass
for name in ["orders_stream", "orders_stream_dupes", "orders_by_customer"]:
    spark.sql(f"DROP TABLE IF EXISTS {T(name)}")

def land(file_name, rows):
    dbutils.fs.put(f"{SRC}/{file_name}", "\n".join(json.dumps(r) for r in rows), True)
    print("landed", file_name)

def progress_dicts(q):
    """recentProgress items are dicts (classic) or StreamingQueryProgress objects (newer PySpark)."""
    return [p if isinstance(p, dict) else json.loads(p.json) for p in q.recentProgress]

SCHEMA_DDL = "order_id INT, customer_id STRING, amount DOUBLE, order_ts TIMESTAMP"   # file streams need a schema

# COMMAND ----------

# =====================================================================================
# PART 1 — First run
# =====================================================================================
land("orders_0001.json", [
    {"order_id": 101, "customer_id": "C1", "amount": 250.0, "order_ts": "2026-10-16T09:15:00"},
    {"order_id": 102, "customer_id": "C2", "amount": 100.0, "order_ts": "2026-10-16T10:02:00"},
])

def run_stream(checkpoint, target):
    q = (spark.readStream.schema(SCHEMA_DDL).json(SRC)              # source: plain Spark file source
         .withColumn("_ingested_at", F.current_timestamp())          # stateless transformation
         .writeStream
         .option("checkpointLocation", checkpoint)                  # progress lives here
         .trigger(availableNow=True)                                # process what exists now, then stop
         .toTable(T(target)))                                       # sink: Delta table, append mode
    q.awaitTermination()
    for p in progress_dicts(q):
        print(f"batchId={p['batchId']}  numInputRows={p['numInputRows']}")
    return q

run_stream(CHK, "orders_stream")
print("rows in target:", spark.table(T("orders_stream")).count())   # expected 2

# COMMAND ----------

# =====================================================================================
# PART 2 — Checkpoint anatomy
# =====================================================================================
for f in dbutils.fs.ls(CHK):
    print(f.name)
# Expected: commits/  metadata  offsets/  sources/   (state/ appears only for stateful queries)

print("\n--- metadata (query id; stays the same across restarts) ---")
print(dbutils.fs.head(f"{CHK}/metadata"))

print("\n--- offsets/0 (written BEFORE the batch runs: what batch 0 will read) ---")
print(dbutils.fs.head(f"{CHK}/offsets/0"))
# Line 1: log version. Line 2: batchWatermarkMs, batchTimestampMs, conf (key SQL confs pinned to the query).
# Line 3: the source offset (for the file source, a logOffset into sources/0).

print("\n--- commits/0 (written AFTER the sink commit: batch 0 is done) ---")
print(dbutils.fs.head(f"{CHK}/commits/0"))

print("\n--- sources/0 (file source: log of every file already seen) ---")
for f in dbutils.fs.ls(f"{CHK}/sources/0"):
    print(f.name)
    print(dbutils.fs.head(f.path, 1500))

# COMMAND ----------

# The Delta sink records the stream's progress in the TABLE too: each streaming commit carries the
# query id and epoch (batch) id. A replayed batch with an already-committed id is skipped -> exactly-once.
display(spark.sql(f"DESCRIBE HISTORY {T('orders_stream')}")
        .select("version", "operation", "operationParameters"))
# Expected: operation = STREAMING UPDATE, operationParameters include queryId, epochId, outputMode

# COMMAND ----------

# =====================================================================================
# PART 3 — Incremental: only new files are read
# =====================================================================================
land("orders_0002.json", [
    {"order_id": 103, "customer_id": "C3", "amount": 40.0, "order_ts": "2026-10-16T11:30:00"},
    {"order_id": 104, "customer_id": "C9", "amount": 75.5, "order_ts": "2026-10-16T14:45:00"},
])
run_stream(CHK, "orders_stream")                                    # expected numInputRows=2 (only the new file)
print("rows in target:", spark.table(T("orders_stream")).count())   # expected 4

run_stream(CHK, "orders_stream")                                    # no new files -> no new rows
print("rows in target:", spark.table(T("orders_stream")).count())   # still 4
print("offset files:", [f.name for f in dbutils.fs.ls(f"{CHK}/offsets")])

# COMMAND ----------

# =====================================================================================
# PART 4 — Lose the checkpoint -> reprocess everything
# =====================================================================================
# Simulate "someone deleted the checkpoint" by pointing the SAME logic and SAME table at a new,
# empty checkpoint path. First copy the current table so we do not damage orders_stream.
spark.sql(f"CREATE OR REPLACE TABLE {T('orders_stream_dupes')} AS SELECT * FROM {T('orders_stream')}")
run_stream(f"{BASE}/_checkpoints/orders_stream_NEW", "orders_stream_dupes")
display(spark.table(T("orders_stream_dupes")).groupBy("order_id").count().orderBy("order_id"))
# Expected: every order_id has count = 2. The new checkpoint knows nothing, so all files are "new".
# Lesson: the checkpoint IS the pipeline's memory. Protect it (no lifecycle policy, no manual cleanup),
# and make downstream writes idempotent (MERGE on a key) so an accidental replay is harmless.

# COMMAND ----------

# =====================================================================================
# PART 5 — Output modes with an aggregation
# =====================================================================================
agg = (spark.readStream.schema(SCHEMA_DDL).json(SRC)
       .groupBy("customer_id").agg(F.sum("amount").alias("revenue")))

def try_start(label, mode, target):
    try:
        q = (agg.writeStream.outputMode(mode)
             .option("checkpointLocation", f"{BASE}/_checkpoints/agg_{mode}")
             .trigger(availableNow=True).toTable(T(target)))
        q.awaitTermination()
        print(f"[{label}] OK")
    except Exception as e:
        print(f"[{label}] REJECTED: {type(e).__name__}: {str(e)[:300]}")

try_start("append + aggregation without watermark", "append", "orders_by_customer")
# Expected: REJECTED — append needs a watermark for aggregations (a group could still change).
try_start("update -> Delta sink", "update", "orders_by_customer")
# Expected: REJECTED — the Delta sink supports append and complete, not update (use foreachBatch + MERGE).
try_start("complete -> Delta sink", "complete", "orders_by_customer")
# Expected: OK — the whole result table is rewritten each trigger.
display(spark.table(T("orders_by_customer")).orderBy("customer_id"))

# COMMAND ----------

# =====================================================================================
# Optional — processingTime trigger (runs until stopped; not supported on serverless)
# =====================================================================================
RUN_PROCESSING_TIME_DEMO = False
if RUN_PROCESSING_TIME_DEMO:
    import time
    q = (spark.readStream.schema(SCHEMA_DDL).json(SRC).writeStream
         .format("noop")                                             # discard output; we only watch progress
         .option("checkpointLocation", f"{BASE}/_checkpoints/pt_demo")
         .trigger(processingTime="10 seconds")
         .start())
    time.sleep(35)
    print([(p["batchId"], p["numInputRows"]) for p in progress_dicts(q)])
    q.stop()

# COMMAND ----------

# Cleanup (uncomment when finished)
# dbutils.fs.rm(BASE, True)
# for name in ["orders_stream", "orders_stream_dupes", "orders_by_customer"]:
#     spark.sql(f"DROP TABLE IF EXISTS {T(name)}")