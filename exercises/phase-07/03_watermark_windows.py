# Databricks notebook source
# Phase 7.4 — Event time, watermarks, late data and output modes (windowed aggregation).
#
# ShopCo store events arrive in 5 deliveries. We compute revenue per 10-minute EVENT-TIME window
# with a 10-minute watermark, and run the same aggregation three ways:
#   w_append      outputMode append   -> a window is written ONCE, after the watermark passes its end
#   w_update_log  outputMode update   -> every changed window is emitted each batch (via foreachBatch,
#                                        because the Delta sink does not support update mode)
#   w_complete    outputMode complete -> the whole result is rewritten every batch; state never dropped
# Plus w_raw: the raw events with processing time, to compare event time vs processing time.
#
# Universal Spark Structured Streaming semantics; Auto Loader is only used as the file source.

# COMMAND ----------

import json
from pyspark.sql import functions as F

CATALOG, SCHEMA = "main", "de_prep"          # change to a catalog/schema you can write to
VOLUME = "phase07"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")
T = lambda name: f"{CATALOG}.{SCHEMA}.{name}"

BASE = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/nb03"
SRC = f"{BASE}/landing/store_events"
try:
    dbutils.fs.rm(BASE, True)
except Exception:
    pass
for name in ["w_append", "w_update_log", "w_complete", "w_raw"]:
    spark.sql(f"DROP TABLE IF EXISTS {T(name)}")

EVENTS_SCHEMA = "order_id INT, store STRING, amount DOUBLE, event_ts TIMESTAMP"

def land(file_name, rows):
    dbutils.fs.put(f"{SRC}/{file_name}", "\n".join(json.dumps(r) for r in rows), True)
    print("landed", file_name, [r["event_ts"][11:16] for r in rows])

def ev(order_id, hhmm, amount):
    return {"order_id": order_id, "store": "S1", "amount": amount, "event_ts": f"2026-10-16T{hhmm}:00"}

def progress_dicts(q):
    return [p if isinstance(p, dict) else json.loads(p.json) for p in q.recentProgress]

# COMMAND ----------

def events():
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .schema(EVENTS_SCHEMA)                       # explicit schema -> no schemaLocation needed
            .load(SRC))

def windowed(df):
    return (df
            .withWatermark("event_ts", "10 minutes")     # accept events up to 10 min behind the max seen
            .groupBy(F.window("event_ts", "10 minutes"), "store")   # 10-min tumbling event-time windows
            .agg(F.sum("amount").alias("revenue"), F.count("*").alias("orders"))
            .select(F.col("window.start").alias("window_start"),
                    F.col("window.end").alias("window_end"), "store", "revenue", "orders"))

def log_update_batch(batch_df, batch_id):
    """foreachBatch sink: record exactly what update mode emitted in this micro-batch."""
    (batch_df.withColumn("batch_id", F.lit(batch_id))
     .write.mode("append").saveAsTable(T("w_update_log")))

def chk(name):
    return f"{BASE}/_checkpoints/{name}"            # one checkpoint per query

def run_all(round_no):
    queries = {
        "append": (windowed(events()).writeStream.outputMode("append")
                   .option("checkpointLocation", chk("w_append"))
                   .trigger(availableNow=True).toTable(T("w_append"))),
        "update": (windowed(events()).writeStream.outputMode("update")
                   .foreachBatch(log_update_batch)
                   .option("checkpointLocation", chk("w_update"))
                   .trigger(availableNow=True).start()),
        "complete": (windowed(events()).writeStream.outputMode("complete")
                     .option("checkpointLocation", chk("w_complete"))
                     .trigger(availableNow=True).toTable(T("w_complete"))),
        "raw": (events().withColumn("processed_at", F.current_timestamp())   # processing time
                .writeStream.option("checkpointLocation", chk("w_raw"))
                .trigger(availableNow=True).toTable(T("w_raw"))),
    }
    for q in queries.values():
        q.awaitTermination()
    # Watermark and late-row drops as reported by the append query.
    for p in progress_dicts(queries["append"]):
        ops = p.get("stateOperators") or [{}]
        print(f"round {round_no} | batch {p['batchId']} | rows in {p['numInputRows']} | "
              f"watermark {p.get('eventTime', {}).get('watermark')} | "
              f"state rows {ops[0].get('numRowsTotal')} | dropped late {ops[0].get('numRowsDroppedByWatermark')}")

# COMMAND ----------

# Round 1: max event time 10:12 -> watermark becomes 10:02 after this batch
land("events_01.json", [ev(1, "10:01", 10.0), ev(2, "10:04", 20.0), ev(3, "10:12", 5.0)])
run_all(1)
display(spark.table(T("w_append")) if spark.catalog.tableExists(T("w_append")) else spark.createDataFrame([], "x INT"))
# Expected: w_append empty — no window can be final yet. w_complete shows [10:00,10:10)=30, [10:10,10:20)=5.

# COMMAND ----------

# Round 2: 10:07 is LATE (arrives after 10:12) but within the watermark: window [10:00,10:10) is still open.
# max event time 10:25 -> watermark 10:15
land("events_02.json", [ev(4, "10:07", 15.0), ev(5, "10:25", 30.0)])
run_all(2)

# COMMAND ----------

# Round 3: 10:05 is TOO LATE — watermark is 10:15, so window [10:00,10:10) is closed and its state dropped.
# max event time 10:31 -> watermark 10:21
land("events_03.json", [ev(6, "10:05", 100.0), ev(7, "10:31", 7.0)])
run_all(3)
# Expected: "dropped late 1" on this round's batch.

# COMMAND ----------

# Rounds 4 and 5: time moves on (10:55, then 11:20) so the watermark passes the remaining windows.
land("events_04.json", [ev(8, "10:55", 1.0)])
run_all(4)
land("events_05.json", [ev(9, "11:20", 1.0)])
run_all(5)

# COMMAND ----------

print("APPEND — each window written once, only after it was final (late 100.0 dropped):")
display(spark.table(T("w_append")).orderBy("window_start"))
# Expected at least:
#   10:00-10:10  revenue 45.0  orders 3     (10 + 20 + the late-but-accepted 15)
#   10:10-10:20  revenue 5.0   orders 1
#   10:20-10:30  revenue 30.0  orders 1
#   10:30-10:40  revenue 7.0   orders 1
# [10:50,11:00) may also appear depending on whether your runtime runs a final no-data batch
# (the watermark computed at the end of one batch is applied in the NEXT batch).

print("UPDATE — every batch emits the windows that changed in that batch:")
display(spark.table(T("w_update_log")).orderBy("batch_id", "window_start"))
# Expected: batch of round 1 emits [10:00)=30 and [10:10)=5; round 2 emits [10:00)=45 and [10:20)=30;
# round 3 emits only [10:30)=7 (the 100.0 row was dropped, so [10:00) did not change).

print("COMPLETE — whole table every batch; watermark does not drop state:")
display(spark.table(T("w_complete")).orderBy("window_start"))
# Expected (verify): [10:00,10:10) = 145.0 with 4 orders — complete mode keeps all state, so the
# very late row is still aggregated. Complete mode's state grows forever: avoid for unbounded keys.

print("EVENT TIME vs PROCESSING TIME:")
display(spark.table(T("w_raw")).select("order_id", "event_ts", "processed_at").orderBy("order_id"))
# Order 6 happened at 10:05 (event time) but was processed in round 3 (processing time). Grouping by
# processed_at would put it in the wrong business window; grouping by event_ts + watermark is correct.

# COMMAND ----------

w0 = (spark.table(T("w_append"))
      .filter(F.col("window_start") == F.lit("2026-10-16 10:00:00").cast("timestamp")).collect())
assert len(w0) == 1 and w0[0].revenue == 45.0 and w0[0].orders == 3, w0
print("Watermark check passed: late-but-within-threshold row kept, too-late row dropped.")

# COMMAND ----------

# Cleanup (uncomment when finished)
# dbutils.fs.rm(BASE, True)
# for name in ["w_append", "w_update_log", "w_complete", "w_raw"]:
#     spark.sql(f"DROP TABLE IF EXISTS {T(name)}")