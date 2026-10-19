# Databricks notebook source
# Phase 8.2 — nested task of the FOR-EACH task `per_region_report`.
# Runs once per region. The for-each task passes the current element as {{input}} -> widget `region`.

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("region", "UAE")                          # from for-each: base_parameters.region = "{{input}}"
dbutils.widgets.text("run_date", "2026-10-16")                 # job parameter (pushed to every notebook task)
dbutils.widgets.text("pipeline_schema", "main.de_prep_sdp")    # job parameter: where the pipeline publishes

region = dbutils.widgets.get("region")
run_date = dbutils.widgets.get("run_date")
src = f"{dbutils.widgets.get('pipeline_schema')}.silver_orders"
target = "main.de_prep.region_daily_report"

# COMMAND ----------

report = (spark.read.table(src)
          .where((F.col("region") == region) & (F.to_date("order_ts") == F.lit(run_date)))
          .groupBy(F.to_date("order_ts").alias("order_date"), "region")
          .agg(F.count("*").alias("orders"), F.sum("amount").alias("revenue")))

# Idempotent write: overwrite ONLY this (date, region) slice. A retry or a repair run gives the same result.
# replaceWhere requires the table to exist with this schema; the first run creates it.
if spark.catalog.tableExists(target):
    (report.write.mode("overwrite")
        .option("replaceWhere", f"order_date = '{run_date}' AND region = '{region}'")
        .saveAsTable(target))
else:
    report.write.saveAsTable(target)

print(f"{region} {run_date}: {report.collect()}")