# Databricks notebook source
# Phase 5.3 — look inside your compute: Spark version, driver vs worker hosts, parallelism.
# Read-only: creates no tables. Works on classic compute (single or multi node) and serverless;
# items that are unavailable on serverless / standard access mode are caught and reported.

# COMMAND ----------

import socket
from pyspark.sql import functions as F

print("Spark version     :", spark.version)

def safe_conf(key):
    """Read a Spark conf; some keys are not exposed on serverless or are restricted."""
    try:
        return spark.conf.get(key)
    except Exception as e:  # noqa: BLE001 - we only want to report availability
        return f"n/a ({type(e).__name__})"

# Cluster usage tags are set by Databricks on classic compute.
for key in [
    "spark.databricks.clusterUsageTags.sparkVersion",     # DBR version string, e.g. 16.4.x-scala2.12
    "spark.databricks.clusterUsageTags.clusterName",
    "spark.databricks.clusterUsageTags.clusterId",
]:
    print(f"{key:55s}: {safe_conf(key)}")

# COMMAND ----------

# Default parallelism = total task slots on classic compute.
# spark.sparkContext is not available on serverless (Spark Connect), so guard it.
try:
    print("defaultParallelism:", spark.sparkContext.defaultParallelism)
except Exception as e:  # noqa: BLE001
    print("defaultParallelism: n/a on this compute ->", type(e).__name__)

# COMMAND ----------

# Driver vs workers: plain Python runs on the DRIVER; a UDF runs inside TASKS on EXECUTORS.
driver_host = socket.gethostname()

@F.udf("string")
def where_am_i(_):
    import socket as s
    return s.gethostname()

df = spark.range(0, 1_000_000, numPartitions=16)

hosts = (
    df.select(F.spark_partition_id().alias("pid"), where_am_i("id").alias("host"))
      .groupBy("host")
      .agg(F.countDistinct("pid").alias("partitions"), F.count("*").alias("rows"))
      .orderBy("host")
)

print("driver host:", driver_host)
hosts.show(truncate=False)

# Expected:
#  - single node : one host, equal to the driver host (driver is also the worker)
#  - multi node  : one row per worker host, none equal to the driver; partitions shared
#  - serverless  : Databricks-managed hosts; you cannot choose or inspect nodes

# COMMAND ----------

# Why collect() is dangerous: everything lands on the DRIVER.
small = df.limit(5).collect()          # fine: 5 rows
print("collected rows:", len(small))
# df.collect()  # <- 1M rows to the driver. Harmless here, but with real tables this is how drivers OOM.