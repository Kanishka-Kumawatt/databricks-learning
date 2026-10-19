# Databricks notebook source
# Phase 11.1 — Skewed join: reproduce, diagnose in the Spark UI, fix three ways.
# Run on CLASSIC compute (all-purpose or job cluster). Serverless allows only a few Spark configs
# (e.g. spark.sql.shuffle.partitions), so the AQE/broadcast switches below won't apply there.
# Size: ~20M fact rows, fine on a 2-4 worker cluster. Results go to the "noop" sink (nothing is stored).

# COMMAND ----------

import time
from pyspark.sql import functions as F

CATALOG, SCHEMA = "main", "de_prep"   # not written to in this lab; kept for consistency

N_FACT = 20_000_000      # fact rows
N_CUST = 200_000         # dimension rows
HOT_SHARE = 60           # % of fact rows that belong to customer 0 (the "marketplace" customer)

def timed(label, df):
    """Run the full plan without storing output: the noop sink forces execution of every row."""
    t0 = time.time()
    df.write.format("noop").mode("overwrite").save()
    print(f"{label:<45} {time.time() - t0:6.1f} s")

def reset_confs():
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    spark.conf.unset("spark.sql.autoBroadcastJoinThreshold")
    spark.conf.unset("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes")
    spark.conf.unset("spark.sql.adaptive.advisoryPartitionSizeInBytes")
    try:
        spark.conf.unset("spark.databricks.adaptive.autoBroadcastJoinThreshold")
    except Exception:
        pass

# COMMAND ----------

# ---------------- Synthetic data: 60 % of orders belong to ONE customer ----------------
orders = (spark.range(N_FACT)
          .withColumn("customer_id",
                      F.when(F.col("id") % 100 < HOT_SHARE, F.lit(0))           # hot key
                       .otherwise((F.col("id") % N_CUST).cast("long")))
          .withColumn("amount", (F.rand(seed=7) * 100).cast("decimal(10,2)"))
          .withColumnRenamed("id", "order_id"))

customers = (spark.range(N_CUST).withColumnRenamed("id", "customer_id")
             .withColumn("segment", F.when(F.col("customer_id") % 10 == 0, "gold").otherwise("standard"))
             .withColumn("padding", F.repeat(F.lit("x"), 200)))      # make each row ~200 bytes

# Evidence before any Spark UI: the key distribution.
display(orders.groupBy("customer_id").count().orderBy(F.desc("count")).limit(5))

# COMMAND ----------

# ---------------- 1. PROBLEM: sort-merge join, AQE skew handling OFF, broadcast OFF ----------------
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")                       # no static broadcast
spark.conf.set("spark.databricks.adaptive.autoBroadcastJoinThreshold", "-1")       # Databricks AQE runtime broadcast (default 30MB)
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "false")
spark.conf.set("spark.sql.shuffle.partitions", "200")

joined = orders.join(customers, "customer_id").groupBy("segment").agg(F.sum("amount").alias("revenue"))
joined.explain("formatted")      # look for SortMergeJoin + two Exchange hashpartitioning(customer_id, 200)
timed("1. SMJ, skew handling off", joined)

# OPEN THE SPARK UI NOW: Jobs -> latest job -> slowest stage (the join stage, ~200 tasks)
#   Summary metrics: Duration max >> median; Shuffle Read max ~ 60 % of all shuffle data.
#   Event timeline: one long bar, everything else finished early -> classic skew.

# COMMAND ----------

# ---------------- 2. FIX A: AQE skew join (Spark 3.x feature, on by default) ----------------
# AQE considers a shuffle partition skewed if it is > skewedPartitionFactor (5) x median AND
# > skewedPartitionThresholdInBytes (256MB default). Our demo data is small, so lower the thresholds
# to make AQE act; in production the defaults are usually fine.
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "16MB")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "16MB")

joined = orders.join(customers, "customer_id").groupBy("segment").agg(F.sum("amount").alias("revenue"))
timed("2. SMJ + AQE skew join", joined)
# Spark UI -> SQL tab -> this query: SortMergeJoin shows isSkew=true; the hot partition is split into
# several tasks (the matching customer partition is replicated to each). Max task time drops.

# COMMAND ----------

# ---------------- 3. FIX B (preferred here): broadcast the small side ----------------
# customers is ~40 MB: small enough to ship to every executor. No shuffle of the 20M-row side at all,
# so the hot key doesn't matter.
reset_confs()
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")       # keep auto off to prove the hint works
joined = (orders.join(F.broadcast(customers), "customer_id")
          .groupBy("segment").agg(F.sum("amount").alias("revenue")))
joined.explain("formatted")      # BroadcastHashJoin + BroadcastExchange; no Exchange on the orders side
timed("3. Broadcast hash join", joined)

# COMMAND ----------

# ---------------- 4. FIX C (when the other side is too big to broadcast): salting ----------------
# Split the hot key into SALT sub-keys on the big side, replicate the matching dimension rows SALT times.
SALT = 16
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "false")     # show salting working on its own

orders_salted = orders.withColumn(
    "salt", F.when(F.col("customer_id") == 0, (F.col("order_id") % SALT).cast("int")).otherwise(F.lit(0)))

customers_salted = customers.withColumn(
    "salt", F.explode(F.when(F.col("customer_id") == 0, F.sequence(F.lit(0), F.lit(SALT - 1)))
                       .otherwise(F.array(F.lit(0)))))

joined = (orders_salted.join(customers_salted, ["customer_id", "salt"])
          .groupBy("segment").agg(F.sum("amount").alias("revenue")))
timed("4. SMJ + salting (hot key only)", joined)

# COMMAND ----------

# ---------------- Verify: every strategy gives the same answer ----------------
reset_confs()
baseline = orders.join(F.broadcast(customers), "customer_id").groupBy("segment").agg(F.sum("amount").alias("revenue"))
salted   = (orders_salted.join(customers_salted, ["customer_id", "salt"])
            .groupBy("segment").agg(F.sum("amount").alias("revenue")))
diff = baseline.exceptAll(salted).count() + salted.exceptAll(baseline).count()
assert diff == 0, "salting changed the result!"
print("Verified: salted join result == broadcast join result")
display(baseline)

# COMMAND ----------

# ---------------- Bonus: skewed AGGREGATION (AQE skew join does not apply) ----------------
# sum/count use partial (map-side) aggregation, so a hot key is pre-aggregated per task -> usually fine.
# Non-decomposable aggregations (collect_list, percentile, count distinct on a hot key) or windows
# partitioned by the hot key are NOT helped by AQE skew join: the whole hot key lands in one task.
reset_confs()
w_hot = orders.groupBy("customer_id").agg(F.countDistinct("amount").alias("distinct_amounts"))
timed("5. countDistinct per customer (skewed agg)", w_hot)
# Two-phase fix: aggregate by (customer_id, salt) first, then by customer_id (works for sum/count;
# for distinct counts, dedupe (customer_id, amount) first, then count).
two_phase = (orders.select("customer_id", "amount").distinct()
             .groupBy("customer_id").agg(F.count("*").alias("distinct_amounts")))
timed("6. distinct first, then count", two_phase)
reset_confs()