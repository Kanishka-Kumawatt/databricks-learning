# Databricks notebook source
# Phase 7.7 / 7.9 — Idempotent Bronze -> Silver upsert with foreachBatch + MERGE.
#
# Bronze is append-only and can contain duplicates (resent files), stale versions and replays.
# Silver must hold ONE current row per order_id no matter how often data is replayed.
#
#   Round 1  first delivery, including an exact duplicate
#   Round 2  an update (newer order_ts), a new order, and a STALE older version that must not win
#   Round 3  the partner resends round 1's data under a new file name (duplicate delivery)
#   Then     call the batch function again by hand with the same data (simulated retry) -> no change
#   Also     txnAppId / txnVersion make a plain append inside foreachBatch idempotent
#
# Delta-specific: MERGE, DeltaTable API, txnAppId/txnVersion (Delta DataFrameWriter options).

# COMMAND ----------

from datetime import datetime
from decimal import Decimal
from delta.tables import DeltaTable
from pyspark.sql import functions as F, Window

CATALOG, SCHEMA = "main", "de_prep"          # change to a catalog/schema you can write to
VOLUME = "phase07"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")
T = lambda name: f"{CATALOG}.{SCHEMA}.{name}"

BASE = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/nb04"
CHK = f"{BASE}/_checkpoints/silver_orders"
APP_ID = "nb04_silver_orders_v1"     # stable, unique per (query, checkpoint). Change it if you reset the checkpoint!
try:
    dbutils.fs.rm(BASE, True)
except Exception:
    pass
for name in ["nb04_bronze_orders", "nb04_silver_orders", "nb04_batch_audit"]:
    spark.sql(f"DROP TABLE IF EXISTS {T(name)}")

BRONZE_SCHEMA = ("order_id INT, customer_id STRING, amount DECIMAL(10,2), order_ts TIMESTAMP, "
                 "_source_file STRING, _ingested_at TIMESTAMP")
spark.sql(f"CREATE TABLE {T('nb04_bronze_orders')} ({BRONZE_SCHEMA})")
spark.sql(f"""CREATE TABLE {T('nb04_silver_orders')} (
                order_id INT, customer_id STRING, amount DECIMAL(10,2), order_ts TIMESTAMP,
                _source_file STRING, _updated_at TIMESTAMP)""")

def ts(s):
    return datetime.strptime(s, "%Y-%m-%d %H:%M")

def land_bronze(rows, file_name):
    """Simulate Auto Loader appending one file's rows into Bronze."""
    df = (spark.createDataFrame([(r[0], r[1], Decimal(r[2]), ts(r[3])) for r in rows],
                                "order_id INT, customer_id STRING, amount DECIMAL(10,2), order_ts TIMESTAMP")
          .withColumn("_source_file", F.lit(file_name))
          .withColumn("_ingested_at", F.current_timestamp()))
    df.write.mode("append").saveAsTable(T("nb04_bronze_orders"))
    print(f"bronze += {len(rows)} rows from {file_name}")

# COMMAND ----------

def upsert_to_silver(batch_df, batch_id):
    """Runs once per micro-batch. Must be safe to run twice with the same input."""
    # 1. Deduplicate INSIDE the batch: MERGE fails if two source rows match the same target row.
    w = Window.partitionBy("order_id").orderBy(F.col("order_ts").desc(),
                                               F.col("_ingested_at").desc(),
                                               F.col("_source_file").desc())
    latest = (batch_df.withColumn("_rn", F.row_number().over(w))
              .filter("_rn = 1").drop("_rn", "_ingested_at")
              .withColumn("_updated_at", F.current_timestamp()))

    # 2. MERGE keyed on order_id; only a NEWER version may overwrite (stale/out-of-order rows lose).
    silver = DeltaTable.forName(batch_df.sparkSession, T("nb04_silver_orders"))
    (silver.alias("t")
     .merge(latest.alias("s"), "t.order_id = s.order_id")
     .whenMatchedUpdateAll(condition="s.order_ts > t.order_ts")
     .whenNotMatchedInsertAll()
     .execute())

    # 3. A plain APPEND is not idempotent by itself. txnAppId + txnVersion let Delta skip a write it
    #    has already committed for this (appId, version) pair, e.g. when the batch is retried.
    (batch_df.sparkSession.createDataFrame([(batch_id, batch_df.count())], "batch_id LONG, input_rows LONG")
     .withColumn("_logged_at", F.current_timestamp())
     .write.format("delta").mode("append")
     .option("txnAppId", APP_ID).option("txnVersion", batch_id)
     .saveAsTable(T("nb04_batch_audit")))


def run_silver():
    q = (spark.readStream.table(T("nb04_bronze_orders"))      # Delta table as a streaming source
         .writeStream
         .foreachBatch(upsert_to_silver)
         .option("checkpointLocation", CHK)
         .trigger(availableNow=True)
         .start())
    q.awaitTermination()

def show_silver(label):
    print(label)
    display(spark.table(T("nb04_silver_orders")).select("order_id", "customer_id", "amount", "order_ts", "_source_file")
            .orderBy("order_id"))

# COMMAND ----------

# Round 1: first delivery, with an exact duplicate of 102 inside the same file.
land_bronze([(101, "C1", "250.00", "2026-10-16 09:15"),
             (102, "C2", "100.00", "2026-10-16 10:02"),
             (102, "C2", "100.00", "2026-10-16 10:02")], "orders_0001.json")
run_silver()
show_silver("after round 1")
# Expected: 101 250.00, 102 100.00 (one row each)

# COMMAND ----------

# Round 2: update of 102 (newer), new 103, and a STALE version of 101 (older order_ts) arriving late.
land_bronze([(102, "C2", "120.00", "2026-10-16 12:00"),
             (103, "C3", "40.00",  "2026-10-16 11:30"),
             (101, "C1", "999.00", "2026-10-16 08:00")], "orders_0002.json")
run_silver()
show_silver("after round 2")
# Expected: 101 250.00 (stale 999 ignored), 102 120.00, 103 40.00

# COMMAND ----------

# Round 3: duplicate delivery — the partner resends round 1 under a NEW file name.
# Auto Loader would ingest it (new path = new file), so Bronze gets the rows again. Silver must not change.
land_bronze([(101, "C1", "250.00", "2026-10-16 09:15"),
             (102, "C2", "100.00", "2026-10-16 10:02")], "orders_0001_resend.json")
run_silver()
show_silver("after round 3 (resend)")
# Expected: unchanged — 101 250.00, 102 120.00 (the resent 100.00 is older), 103 40.00

# COMMAND ----------

# Simulated retry: run the batch function again by hand with the round-2 rows and an ALREADY-COMMITTED
# batch id (as if the cluster died after MERGE but before the checkpoint recorded the commit).
before = sorted(spark.table(T("nb04_silver_orders")).select("order_id", "amount").collect())
audit_before = spark.table(T("nb04_batch_audit")).count()

round2 = spark.table(T("nb04_bronze_orders")).filter("_source_file = 'orders_0002.json'")
replay_id = spark.table(T("nb04_batch_audit")).agg(F.max("batch_id")).first()[0]   # a committed batch id
upsert_to_silver(round2, replay_id)

after = sorted(spark.table(T("nb04_silver_orders")).select("order_id", "amount").collect())
audit_after = spark.table(T("nb04_batch_audit")).count()
assert before == after, "MERGE replay changed Silver — not idempotent!"
assert audit_before == audit_after, "txnAppId/txnVersion did not skip the duplicate append (verify for your runtime)"
print(f"Replay is harmless: silver unchanged, audit rows {audit_before} -> {audit_after}")

display(spark.table(T("nb04_batch_audit")).orderBy("batch_id"))

# COMMAND ----------

final = {r.order_id: r.amount for r in spark.table(T("nb04_silver_orders")).collect()}
assert final == {101: Decimal("250.00"), 102: Decimal("120.00"), 103: Decimal("40.00")}, final
print("Silver is correct after duplicates, stale data, resend and retry.")

# COMMAND ----------

# Cleanup (uncomment when finished)
# dbutils.fs.rm(BASE, True)
# for name in ["nb04_bronze_orders", "nb04_silver_orders", "nb04_batch_audit"]:
#     spark.sql(f"DROP TABLE IF EXISTS {T(name)}")