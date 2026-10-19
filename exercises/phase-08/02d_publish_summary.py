# Databricks notebook source
# Phase 8.2 — task `publish_summary`: reconcile and publish a one-row summary for run_date.
# Reads a TASK VALUE from an upstream task and the Gold materialized view built by the pipeline.

# COMMAND ----------

import json
from pyspark.sql import functions as F

dbutils.widgets.text("run_date", "2026-10-16")
dbutils.widgets.text("pipeline_schema", "main.de_prep_sdp")
run_date = dbutils.widgets.get("run_date")
schema = dbutils.widgets.get("pipeline_schema")

# Task value from `check_landing`. taskKey = the upstream task_key, not the notebook name.
landed_rows = dbutils.jobs.taskValues.get(taskKey="check_landing", key="row_count", default=0, debugValue=6)

# COMMAND ----------

gold = spark.read.table(f"{schema}.gold_daily_revenue").where(F.col("order_date") == F.lit(run_date))
silver_rows = gold.agg(F.coalesce(F.sum("orders"), F.lit(0))).first()[0]
quarantined = (spark.read.table(f"{schema}.silver_orders_quarantine")
               .where(F.to_date("order_ts") == F.lit(run_date)).count())
revenue = gold.agg(F.coalesce(F.sum("revenue"), F.lit(0))).first()[0]

print(f"landed={landed_rows} silver={silver_rows} quarantined={quarantined} revenue={revenue}")

# Reconciliation: every landed row must be either in Silver or in quarantine.
# (Rows with an unparseable order_ts would escape the date filter; good enough for the demo.)
if silver_rows + quarantined != landed_rows:
    raise ValueError(f"Reconciliation failed for {run_date}: landed={landed_rows}, "
                     f"silver={silver_rows}, quarantined={quarantined}")

dbutils.jobs.taskValues.set(key="revenue", value=float(revenue))
dbutils.notebook.exit(json.dumps({"run_date": run_date, "revenue": float(revenue), "quarantined": quarantined}))