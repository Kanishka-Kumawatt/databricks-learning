# Databricks notebook source
# Phase 1.1 — a minimal pipeline that touches every lifecycle stage.
# Ingest (CSV) -> fix types -> dedup -> quality filter -> aggregate -> idempotent write.
#
# Setup on Databricks:
#   1. Create catalog `shopco` with schemas `raw` and `gold`, and a volume shopco.raw.orders
#   2. Upload data/orders_2026-10-16.csv to /Volumes/shopco/raw/orders/2026-10-16/orders.csv

# COMMAND ----------

dbutils.widgets.text("run_date", "2026-10-16")

# COMMAND ----------

from pyspark.sql import functions as F

# Parameter, not "today": allows re-runs and backfills for any date.
run_date = dbutils.widgets.get("run_date")

# INGEST — without a schema every column is read as string.
raw = (spark.read
       .option("header", "true")
       .csv(f"/Volumes/shopco/raw/orders/{run_date}/"))

# TRANSFORM — fix types, dedup, quality rules.
# try_cast returns null on bad input regardless of ANSI mode (plain cast errors when ANSI is on).
clean = (raw
         .withColumn("amount", F.expr("try_cast(amount AS DECIMAL(10,2))"))
         .withColumn("order_ts", F.expr("try_to_timestamp(order_ts)"))
         .dropDuplicates(["order_id"])                       # removes duplicates within THIS batch only
         .filter(F.col("amount") > 0)                        # production: quarantine instead of drop
         .filter(F.to_date("order_ts") == F.lit(run_date)))  # must match the replaceWhere slice below

daily_revenue = (clean
                 .groupBy(F.to_date("order_ts").alias("order_date"), "region")
                 .agg(F.sum("amount").alias("revenue"),
                      F.count("order_id").alias("orders")))

# SERVE — overwrite only this date's slice, so re-runs give the same result (idempotent).
(daily_revenue.write
   .format("delta")
   .mode("overwrite")
   .option("replaceWhere", f"order_date = '{run_date}'")
   .saveAsTable("shopco.gold.daily_revenue"))

# COMMAND ----------

# Expected:
# | order_date | region | revenue | orders |
# | 2026-10-16 | North  | 325.50  | 2      |
# | 2026-10-16 | South  | 100.00  | 1      |
display(spark.table("shopco.gold.daily_revenue").orderBy("region"))

# COMMAND ----------

# Exercise: run the notebook a second time. Does the result change? Why not?
# Then change mode("overwrite") to mode("append"), run twice, and look again.