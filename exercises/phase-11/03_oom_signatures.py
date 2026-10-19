# Databricks notebook source
# Phase 11.3 — Driver OOM vs executor OOM / spill.
# SAFE BY DESIGN: nothing here should crash a cluster. We measure, show the risky pattern on tiny data,
# show the safe alternatives, and produce SPILL (the warning sign before executor OOM) on a skewed window.
# Do NOT scale the "risky" cells up on a shared cluster: a driver OOM restarts the driver and kills
# every notebook attached to it.

# COMMAND ----------

import time
from pyspark.sql import functions as F, Window

CATALOG, SCHEMA = "main", "de_prep"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
T = lambda name: f"{CATALOG}.{SCHEMA}.{name}"

# Evidence you can always read: how much may come back to the driver?
for key in ("spark.driver.maxResultSize", "spark.driver.memory", "spark.executor.memory"):
    try:
        print(f"{key:<30} = {spark.conf.get(key)}")
    except Exception:
        print(f"{key:<30} = (not readable here: serverless / Spark Connect or not set)")

# COMMAND ----------

# ---------------- DRIVER side: collect / toPandas ----------------
events = (spark.range(5_000_000)
          .withColumn("customer_id", (F.col("id") % 50_000).cast("long"))
          .withColumn("amount", (F.rand(seed=1) * 100).cast("double"))
          .withColumn("note", F.repeat(F.lit("n"), 100)))

# How big would a collect be?  Estimate before you pull anything to the driver.
sample = events.limit(10_000).toPandas()                       # small, safe
bytes_per_row = sample.memory_usage(deep=True).sum() / len(sample)
print(f"~{bytes_per_row:.0f} bytes/row in pandas -> full toPandas() ≈ {bytes_per_row * 5_000_000 / 1e9:.1f} GB on the driver")

# RISKY pattern (do not run at scale):
#   pdf = events.toPandas()          # all 5M rows -> driver memory
#   rows = events.collect()          # same, as Python Row objects (even bigger)
# Failure signatures: "Total size of serialized results of N tasks (X GB) is bigger than
# spark.driver.maxResultSize", or java.lang.OutOfMemoryError on the driver, or the run page message
# "The spark driver has stopped unexpectedly and is restarting" (driver log4j shows the OOM / GC overhead).

# SAFE alternatives: keep the work distributed, bring back only small results.
summary = events.groupBy("customer_id").agg(F.sum("amount").alias("total")).orderBy(F.desc("total")).limit(20)
display(summary)                                               # 20 rows to the driver
events.write.mode("overwrite").saveAsTable(T("oom_events"))    # results to a table, not the driver
first_rows = events.take(5)                                    # bounded

# COMMAND ----------

# ---------------- DRIVER side: a Python loop that pulls data row by row ----------------
# Anti-pattern: for row in df.collect(): call_api(row)   -> driver memory + single-threaded.
# Better: process in executors with mapInPandas / a pandas UDF, writing results to a table.
def score(batches):
    for pdf in batches:                      # runs on executors, one Arrow batch at a time
        pdf["score"] = pdf["amount"] * 1.1
        yield pdf[["id", "score"]]

scored = events.select("id", "amount").mapInPandas(score, schema="id long, score double")
scored.write.format("noop").mode("overwrite").save()
print("mapInPandas processed all rows without touching driver memory")

# COMMAND ----------

# ---------------- EXECUTOR side: skewed window -> one huge partition -> SPILL ----------------
# 70 % of rows share customer_id = 0. A window partitioned by customer_id puts them all in one task,
# which must sort and buffer them. When execution memory is exhausted the task SPILLS to disk; with
# non-spillable structures (huge single rows, giant arrays from collect_list) it can OOM instead.
skewed = (spark.range(12_000_000)
          .withColumn("customer_id", F.when(F.col("id") % 10 < 7, F.lit(0)).otherwise(F.col("id") % 100_000))
          .withColumn("amount", F.rand(seed=2))
          .withColumn("note", F.repeat(F.lit("x"), 60)))

w = Window.partitionBy("customer_id").orderBy("amount")
t0 = time.time()
(skewed.withColumn("rank_in_customer", F.row_number().over(w))
    .write.format("noop").mode("overwrite").save())
print(f"skewed window: {time.time() - t0:.0f} s")
# OPEN SPARK UI -> the window stage -> Summary metrics:
#   Duration max >> median; "Spill (Memory)" and "Spill (Disk)" > 0 for the max task (may be 0 on very
#   large executors — then raise the row count or note width). Executors tab: high GC time.
# If this were collect_list per customer instead, the single 8M-element array per key could OOM:
#   ExecutorLostFailure ... / "Container killed ... exceeding memory limits" / exit code 137 in logs.

# COMMAND ----------

# ---------------- FIX: change the algorithm, not the memory size ----------------
# Business question: "rank of each order within its customer" is rarely needed for the hot key.
# Typical fixes: (1) aggregate before windowing, (2) isolate the hot key, (3) spread the hot key.
# Here: global rank for the hot key computed separately with a range-partitioned sort (spills evenly),
# normal window for everyone else.
hot  = skewed.where("customer_id = 0")
rest = skewed.where("customer_id <> 0")

t0 = time.time()
rest_ranked = rest.withColumn("rank_in_customer", F.row_number().over(w))
# For the hot key: a distributed sort + row number via zip-free approach: rank on amount using percent_rank
# buckets would also work; here we just compute it with an ordered window per amount bucket and offsets.
hot_b = hot.withColumn("bucket", F.floor(F.col("amount") * 100).cast("int"))           # 100 range buckets
bucket_sizes = hot_b.groupBy("bucket").count()
offsets = bucket_sizes.withColumn("offset", F.coalesce(
    F.sum("count").over(Window.orderBy("bucket").rowsBetween(Window.unboundedPreceding, -1)), F.lit(0)))
hot_ranked = (hot_b.join(F.broadcast(offsets.select("bucket", "offset")), "bucket")
              .withColumn("rank_in_customer",
                          F.row_number().over(Window.partitionBy("bucket").orderBy("amount")) + F.col("offset"))
              .drop("bucket", "offset"))
result = rest_ranked.unionByName(hot_ranked)
result.write.format("noop").mode("overwrite").save()
print(f"split hot key into 100 range buckets: {time.time() - t0:.0f} s  (check: no single dominant task, little/no spill)")

# Verify the rewrite on the hot key: ranks 1..N without gaps or duplicates.
chk = hot_ranked.agg(F.min("rank_in_customer").alias("mn"), F.max("rank_in_customer").alias("mx"),
                     F.countDistinct("rank_in_customer").alias("nd"), F.count("*").alias("n")).first()
assert chk["mn"] == 1 and chk["mx"] == chk["n"] == chk["nd"], chk
print("Verified hot-key ranks:", chk.asDict())