# Databricks notebook source
# Phase 5.5 — which code patterns run on the compute this notebook is attached to?
# Run it on (a) serverless, (b) classic Standard access mode, (c) classic Dedicated; compare.
# Creates a temporary Delta table in CATALOG.SCHEMA and drops it at the end.

# COMMAND ----------

CATALOG, SCHEMA = "main", "de_prep"          # change to a catalog/schema you can write to
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
TBL = f"{CATALOG}.{SCHEMA}.p5_compat_demo"
CKPT_VOLUME = None                           # e.g. "/Volumes/main/de_prep/checkpoints" to test streaming

results = []

def check(name, fn):
    try:
        fn()
        results.append((name, "OK"))
    except Exception as e:  # noqa: BLE001 - we want to report any failure type
        msg = str(e).split("\n")[0][:110]
        results.append((name, f"FAIL: {type(e).__name__}: {msg}"))

# COMMAND ----------

from pyspark.sql import functions as F

def dataframe_and_sql():
    df = spark.range(10).withColumn("amount", F.col("id") * 10)
    df.write.mode("overwrite").saveAsTable(TBL)
    spark.sql(f"MERGE INTO {TBL} t USING (SELECT 1 AS id, 999 AS amount) s ON t.id = s.id "
              "WHEN MATCHED THEN UPDATE SET amount = s.amount")
    assert spark.table(TBL).filter("id = 1").first()["amount"] == 999

def rdd_api():
    spark.sparkContext.parallelize([1, 2, 3]).sum()     # RDD: not on Standard access mode / serverless

def cache_api():
    spark.table(TBL).cache().count()                    # not supported on serverless

@F.udf("int")
def plus_one(x):
    return x + 1

def python_udf():
    spark.range(5).select(plus_one("id")).collect()

check("DataFrame + SQL + MERGE", dataframe_and_sql)
check("RDD API (sc.parallelize)", rdd_api)
check("df.cache()", cache_api)
check("Python UDF", python_udf)

# COMMAND ----------

# Optional streaming triggers (needs a writable volume path for checkpoints).
if CKPT_VOLUME:
    def stream_available_now():
        (spark.readStream.table(TBL).writeStream
              .option("checkpointLocation", f"{CKPT_VOLUME}/p5_avail")
              .trigger(availableNow=True)
              .toTable(f"{TBL}_stream_copy")
              .awaitTermination())

    def stream_processing_time():
        q = (spark.readStream.table(TBL).writeStream
                  .option("checkpointLocation", f"{CKPT_VOLUME}/p5_proc")
                  .trigger(processingTime="30 seconds")       # rejected on serverless
                  .toTable(f"{TBL}_stream_copy2"))
        q.stop()

    check("Streaming availableNow", stream_available_now)
    check("Streaming processingTime", stream_processing_time)

# COMMAND ----------

for name, status in results:
    print(f"{name:32s} {status}")

# Expected (see lesson 5.5 table):
#   serverless          : DataFrame OK, RDD FAIL, cache FAIL, UDF OK, availableNow OK, processingTime FAIL
#   classic Standard    : DataFrame OK, RDD FAIL, cache OK,   UDF OK, both triggers OK
#   classic Dedicated   : everything OK

# COMMAND ----------

# Clean up
for t in (TBL, f"{TBL}_stream_copy", f"{TBL}_stream_copy2"):
    spark.sql(f"DROP TABLE IF EXISTS {t}")