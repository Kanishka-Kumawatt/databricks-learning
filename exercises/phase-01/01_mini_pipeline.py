# Databricks notebook source
# Phase 1.1 — a minimal pipeline that touches every lifecycle stage.
# Ingest (CSV) -> fix types -> dedup -> quality filter -> aggregate -> idempotent write.
#
# On Databricks: upload data/orders_2026-10-16.csv to
#   /Volumes/shopco/raw/orders/2026-10-16/orders.csv
# and make sure catalog `shopco` with schema `gold` exists.

from pyspark.sql import functions as F

# Parameter, not "today": allows re-runs and backfills for any date.
run_date = "2026-10-16"

# INGEST — without a schema every column is read as string.
raw = (spark.read
       .option("header", "true")
       .csv(f"/Volumes/shopco/raw/orders/{run_date}/"))

# TRANSFORM — fix types, dedup, quality rules.
clean = (raw
         .withColumn("amount", F.col("amount").cast("decimal(10,2)"))   # failed casts become null silently
         .withColumn("order_ts", F.to_timestamp("order_ts"))
         .dropDuplicates(["order_id"])                                  # removes source retries within this batch only
         .filter(F.col("amount") > 0)                                   # production: quarantine instead of drop
         .filter(F.to_date("order_ts") == F.lit(run_date)))             # must match the replaceWhere slice below

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

# Expected:
# | order_date | region | revenue | orders |
# | 2026-10-16 | North  | 325.50  | 2      |
# | 2026-10-16 | South  | 100.00  | 1      |
display(spark.table("shopco.gold.daily_revenue").orderBy("region"))