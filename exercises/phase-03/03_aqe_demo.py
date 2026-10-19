# Databricks notebook source
# Phase 3.3 — Adaptive Query Execution: partition coalescing and a runtime SMJ -> BHJ switch.
# Classic compute recommended (to toggle confs and compare). On serverless most spark.sql.adaptive.*
# confs cannot be set; the notebook prints a notice and continues with platform defaults.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG, SCHEMA = "main", "de_prep"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
T = lambda name: f"{CATALOG}.{SCHEMA}.{name}"

def try_set(key, value):
    """Set a Spark conf if this compute allows it (serverless allows only a short list)."""
    try:
        spark.conf.set(key, value)
        print(f"set {key} = {value}")
        return True
    except Exception as e:
        print(f"could not set {key} here ({type(e).__name__}) - continuing with the platform default")
        return False

def show_conf(key):
    try:
        print(f"{key} = {spark.conf.get(key)}")
    except Exception as e:
        print(f"{key} -> not readable here ({type(e).__name__})")

for k in ["spark.sql.adaptive.enabled", "spark.sql.shuffle.partitions",
          "spark.sql.adaptive.coalescePartitions.enabled", "spark.sql.adaptive.advisoryPartitionSizeInBytes",
          "spark.sql.autoBroadcastJoinThreshold", "spark.databricks.adaptive.autoBroadcastJoinThreshold"]:
    show_conf(k)

# COMMAND ----------

# ---------------- Data ----------------
(spark.range(2_000_000)
   .select(F.col("id").alias("order_id"),
           (F.col("id") % 5000 + 1).alias("customer_id"),
           (F.col("id") % 1000).alias("amount"))
   .write.mode("overwrite").saveAsTable(T("aqe_orders")))

# 1M customers with a 64-char hash column, so the table is tens of MB on disk (> 10 MB static threshold).
(spark.range(1, 1_000_001)
   .select(F.col("id").alias("customer_id"),
           F.sha2(F.col("id").cast("string"), 256).alias("profile"))
   .write.mode("overwrite").saveAsTable(T("aqe_customers")))

aqe_orders, aqe_customers = spark.table(T("aqe_orders")), spark.table(T("aqe_customers"))

# COMMAND ----------

# ---------------- 1. Coalescing post-shuffle partitions ----------------
try_set("spark.sql.shuffle.partitions", "200")   # allowed on serverless too

def used_partitions(df):
    # How many post-shuffle partitions actually produced rows.
    return df.select(F.spark_partition_id().alias("p")).distinct().count()

agg = aqe_orders.groupBy("customer_id").agg(F.sum("amount").alias("revenue"))
print("groups:", agg.count())                                # expected 5000
print("partitions with AQE coalescing:", used_partitions(agg))   # expected: far fewer than 200

if try_set("spark.sql.adaptive.coalescePartitions.enabled", "false"):
    agg2 = aqe_orders.groupBy("customer_id").agg(F.sum("amount").alias("revenue"))
    print("partitions without coalescing:", used_partitions(agg2))   # expected 200
    try_set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# COMMAND ----------

# ---------------- 2. Runtime join switch SMJ -> BHJ ----------------
# Without column statistics the planner cannot estimate how selective this filter is,
# so it still believes `small` is tens of MB and plans a SortMergeJoin.
small = aqe_customers.filter(F.col("customer_id") % 1000 == 0)     # actually ~1,000 rows
j = aqe_orders.join(small, "customer_id")

print("=== BEFORE execution (initial plan, isFinalPlan=false) ===")
j.explain()

# COMMAND ----------

print("rows:", j.count())     # expected 2000 (5 matching customers x 400 orders)

# On classic compute, explain() on the same DataFrame after an action shows the final adaptive plan.
# Look for BroadcastHashJoin and AQEShuffleRead local. On serverless (Spark Connect) each action is
# re-planned - open the query profile ("See performance" under the cell) instead.
print("=== AFTER execution ===")
j.explain()

# Then open the Spark UI -> SQL / DataFrame tab -> this query: the final plan graph shows the switch.

# COMMAND ----------

# ---------------- 3. Compare with an up-front broadcast (usually better than a late AQE switch) ----------------
jb = aqe_orders.join(F.broadcast(small), "customer_id")
jb.explain()      # BroadcastHashJoin already in the initial plan; aqe_orders is never shuffled
print("rows:", jb.count())    # 2000