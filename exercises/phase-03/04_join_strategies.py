# Databricks notebook source
# Phase 3.4 — Join strategies: broadcast hash vs sort-merge vs shuffle hash vs broadcast nested loop.
# Uses HINTS (work on classic and serverless) rather than confs (most cannot be set on serverless).

# COMMAND ----------

import time
from pyspark.sql import functions as F

CATALOG, SCHEMA = "main", "de_prep"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
T = lambda name: f"{CATALOG}.{SCHEMA}.{name}"

for k in ["spark.sql.autoBroadcastJoinThreshold", "spark.databricks.adaptive.autoBroadcastJoinThreshold",
          "spark.sql.join.preferSortMergeJoin", "spark.sql.adaptive.enabled"]:
    try:
        print(k, "=", spark.conf.get(k))
    except Exception as e:
        print(k, "-> not readable here:", type(e).__name__)

# COMMAND ----------

# ---------------- Data ----------------
(spark.range(5_000_000)
   .select(F.col("id").alias("order_id"),
           (F.col("id") % 100000 + 1).alias("customer_id"),
           (F.col("id") % 1000).alias("amount"))
   .write.mode("overwrite").saveAsTable(T("j_orders")))

(spark.range(1, 100_001)
   .select(F.col("id").alias("customer_id"),
           F.element_at(F.array(*[F.lit(c) for c in ["Dubai", "Abu Dhabi", "Sharjah", "Ajman"]]),
                        (F.col("id") % 4 + 1).cast("int")).alias("city"))
   .write.mode("overwrite").saveAsTable(T("j_customers")))

spark.createDataFrame([("small", 0, 100), ("medium", 100, 500), ("large", 500, 1000)],
                      "band STRING, low INT, high INT") \
     .write.mode("overwrite").saveAsTable(T("j_bands"))

j_orders, j_customers, j_bands = spark.table(T("j_orders")), spark.table(T("j_customers")), spark.table(T("j_bands"))

# COMMAND ----------

def run(df, label, repeats=2):
    """Run the whole join but return only one row to the driver. Second run = warm timing."""
    for i in range(repeats):
        t0 = time.perf_counter()
        row = df.agg(F.count("*").alias("n"), F.sum("amount").alias("s")).collect()[0]
        dt = time.perf_counter() - t0
    print(f"{label:<14} rows={row.n:,}  sum={row.s:,}  last run {dt:.1f}s")
    return row

auto = j_orders.join(j_customers, "customer_id")
bhj  = j_orders.join(F.broadcast(j_customers), "customer_id")
smj  = j_orders.join(j_customers.hint("merge"), "customer_id")
shj  = j_orders.join(j_customers.hint("shuffle_hash"), "customer_id")
band = j_orders.join(F.broadcast(j_bands),
                     (j_orders["amount"] >= j_bands["low"]) & (j_orders["amount"] < j_bands["high"]))

# COMMAND ----------

# Plans: find the join node and count the Exchange nodes in each.
for df, label in [(auto, "auto"), (bhj, "broadcast"), (smj, "sort-merge"), (shj, "shuffle-hash"), (band, "band BNLJ")]:
    print(f"\n==================== {label} ====================")
    df.explain()

# COMMAND ----------

# Timings and results (expected for every line: rows=5,000,000 sum=2,497,500,000).
results = {label: run(df, label) for df, label in
           [(auto, "auto"), (bhj, "broadcast"), (smj, "sort-merge"), (shj, "shuffle-hash"), (band, "band BNLJ")]}
assert len({(r.n, r.s) for r in results.values()}) == 1, "strategies must give identical results"
print("All strategies returned identical results.")

# COMMAND ----------

# ---------------- SQL hints ----------------
spark.sql(f"""
  SELECT /*+ BROADCAST(c) */ c.city, SUM(o.amount) AS revenue
  FROM {T('j_orders')} o JOIN {T('j_customers')} c ON o.customer_id = c.customer_id
  GROUP BY c.city
""").explain()

spark.sql(f"""
  SELECT /*+ MERGE(o, c) */ c.city, SUM(o.amount) AS revenue
  FROM {T('j_orders')} o JOIN {T('j_customers')} c ON o.customer_id = c.customer_id
  GROUP BY c.city
""").explain()

# COMMAND ----------

# ---------------- A hint Spark must ignore ----------------
# LEFT OUTER JOIN: the left (preserved) side cannot be the broadcast side.
# The plan will NOT broadcast j_customers here; expect SortMergeJoin (or a BHJ that builds the right side).
ignored = j_customers.hint("broadcast").join(j_orders, "customer_id", "left")
ignored.explain()

# COMMAND ----------

# ---------------- Optional (classic compute only): disable auto-broadcast ----------------
try:
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    j_orders.join(j_customers, "customer_id").explain()   # initial plan: SortMergeJoin
    # Note: on Databricks, AQE may still switch to BHJ at runtime via
    # spark.databricks.adaptive.autoBroadcastJoinThreshold (30MB) - check the Spark UI after running it.
    spark.conf.unset("spark.sql.autoBroadcastJoinThreshold")
except Exception as e:
    print("Cannot change the broadcast threshold on this compute (e.g. serverless):", type(e).__name__)