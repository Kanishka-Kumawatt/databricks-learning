# Databricks notebook source
# Phase 1.12 — the same batch written twice with four strategies. Only one of them is NOT idempotent.

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

CATALOG, SCHEMA = "main", "de_prep"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
TGT = f"{CATALOG}.{SCHEMA}.idem_daily_revenue"

batch = (spark.createDataFrame(
            [("2026-10-16", "North", 325.50), ("2026-10-16", "South", 100.00)],
            "order_date STRING, region STRING, revenue DOUBLE")
         .withColumn("order_date", F.to_date("order_date")))

def reset():
    spark.sql(f"DROP TABLE IF EXISTS {TGT}")
    batch.limit(0).write.saveAsTable(TGT)          # empty table with the right schema

def total():
    return spark.table(TGT).agg(F.sum("revenue")).first()[0]

# COMMAND ----------

# 1) APPEND — NOT idempotent
reset()
for _ in range(2):
    batch.write.mode("append").saveAsTable(TGT)
print("append x2           ->", total())            # 851.0  (doubled!)

# COMMAND ----------

# 2) OVERWRITE THE RUN'S SLICE (replaceWhere) — idempotent
reset()
for _ in range(2):
    (batch.write.mode("overwrite")
          .option("replaceWhere", "order_date = '2026-10-16'")
          .saveAsTable(TGT))
print("replaceWhere x2     ->", total())            # 425.5

# COMMAND ----------

# 3) MERGE ON THE BUSINESS KEY — idempotent
reset()
for _ in range(2):
    (DeltaTable.forName(spark, TGT).alias("t")
        .merge(batch.alias("s"), "t.order_date = s.order_date AND t.region = s.region")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())
print("merge x2            ->", total())            # 425.5

# COMMAND ----------

# 4) APPEND WITH DELTA IDEMPOTENT-WRITE OPTIONS — idempotent
#    Delta records (txnAppId, txnVersion) in the commit; a write whose version is not newer than the
#    last committed version for that app id is skipped. Typical use: foreachBatch with txnVersion = batch_id.
reset()
def append_batch(df, batch_id):
    (df.write.format("delta")
       .option("txnAppId", "daily_revenue_feed")
       .option("txnVersion", batch_id)
       .mode("append")
       .saveAsTable(TGT))

append_batch(batch, 0)
append_batch(batch, 0)          # same batch id again (e.g. retry after a crash) -> skipped
print("txnVersion append x2 ->", total())           # 425.5

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {TGT}").select("version", "operation", "operationParameters"))