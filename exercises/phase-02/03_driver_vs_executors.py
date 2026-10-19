# Databricks notebook source
# Phase 2.3 — Who runs what: driver vs executors.
# Works on classic compute (any access mode), single node and serverless. Some cells print
# "not available on this compute" instead of failing — that is part of the lesson.
# No tables are written by this notebook.

# COMMAND ----------

import socket
from pyspark.sql import functions as F

CATALOG, SCHEMA = "main", "de_prep"   # not used here; kept for consistency with other notebooks

# Spark Connect (serverless, standard access mode on recent runtimes) uses a different
# SparkSession class that lives in pyspark.sql.connect.
IS_CONNECT = "connect" in type(spark).__module__
print("Spark version:", spark.version)
print("Spark Connect session:", IS_CONNECT)

# COMMAND ----------

# 1) Plain Python runs in the notebook process on the DRIVER.
print("driver host:", socket.gethostname())

# COMMAND ----------

# 2) A Python UDF body is shipped to EXECUTORS and runs there (in Python worker processes).
@F.udf("string")
def executor_host(_):
    import socket
    return socket.gethostname()

where = (spark.range(0, 16, 1, numPartitions=8)                     # 16 rows, 8 partitions -> 8 tasks
         .select(F.spark_partition_id().alias("pid"), executor_host("id").alias("host"))
         .distinct()
         .orderBy("pid"))
where.show()
# Expected: 8 rows (pid 0..7).
#   classic multi-node : worker hostnames, different from the driver host
#   single node        : every host == driver host
#   serverless/standard: sandbox/container names (UDFs are isolated) — not your notebook process

# COMMAND ----------

# 3) How many tasks can run at once? (task slots)
try:
    print("defaultParallelism (≈ total executor cores):", spark.sparkContext.defaultParallelism)
except Exception as e:
    # Serverless: SparkContext is not available (Spark Connect).
    print("spark.sparkContext not available on this compute:", type(e).__name__)
    print("Use the cluster's worker count × cores per worker instead (classic), "
          "or accept that serverless manages parallelism for you.")

# COMMAND ----------

# 4) Results of actions come BACK TO THE DRIVER.
orders = (spark.range(0, 1_000_000, 1, numPartitions=8)
          .select(F.col("id").alias("order_id"),
                  (F.col("id") % 5000).alias("customer_id"),
                  ((F.col("id") % 100) + 1).cast("decimal(10,2)").alias("amount")))

# Good: the aggregation runs on executors; only ONE small row comes back to the driver.
total = orders.agg(F.sum("amount").alias("total")).first()["total"]
print("total revenue:", total)          # Expected: 50500000.00  (10 000 cycles of 1..100 → 10 000 × 5 050)

# Good: limit on executors, then bring 5 rows to the driver.
print(orders.limit(5).collect())

# BAD (don't run on big data): every row travels to the driver and becomes a Python object.
# rows = orders.collect()               # 1M rows -> driver memory; on 2 TB -> driver OOM
# pdf  = orders.toPandas()              # same problem

# COMMAND ----------

# 5) A Python loop over data is DRIVER-side and serial — compare with the Spark version.
small = orders.limit(1000).collect()                     # 1000 rows to the driver
python_sum = sum(r["amount"] for r in small)             # runs on the driver, one row at a time
spark_sum = orders.limit(1000).agg(F.sum("amount")).first()[0]   # runs on executors
print(python_sum, spark_sum)
# Both are 50500.00 here, but only the Spark version scales.
# Note: limit(1000) without orderBy is not guaranteed to pick the same 1000 rows in general;
# on spark.range it does in practice. Add orderBy for real determinism.

# COMMAND ----------

# 6) print() inside a UDF goes to EXECUTOR logs, not to this notebook.
@F.udf("long")
def noisy(x):
    print(f"processing {x}")     # look for this in the executor (stdout) logs on classic compute
    return x

spark.range(3).select(noisy("id")).collect()   # notebook shows only the returned rows

# COMMAND ----------

# Where to look next (classic compute):
#   Compute -> your cluster -> Spark UI -> "Executors" tab: one row for the driver + one per worker,
#   with cores, active tasks, shuffle read/write, and stdout/stderr log links.
# Serverless: no Spark UI. Open the query profile from the cell output ("See performance").