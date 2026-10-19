# Databricks notebook source
# Phase 8.2 — task `check_landing`: inspect the landing folder for run_date and publish TASK VALUES.
# Downstream: an if/else condition task reads `row_count`; a for-each task loops over `regions`.

# COMMAND ----------

import json

# Job parameters arrive in notebook tasks as widgets. The defaults here are only for interactive runs.
dbutils.widgets.text("run_date", "2026-10-16")
dbutils.widgets.text("landing_root", "/Volumes/main/de_prep/landing/orders")

run_date = dbutils.widgets.get("run_date")
landing_root = dbutils.widgets.get("landing_root")
day_path = f"{landing_root}/{run_date}/"
print(f"Checking {day_path}")

# COMMAND ----------

try:
    df = spark.read.json(day_path)              # batch read: we only want a quick profile, not ingestion
    row_count = df.count()
    regions = sorted(r["region"] for r in df.select("region").where("region IS NOT NULL").distinct().collect())
except Exception as e:                         # folder missing -> treat as "no data", do not fail the job
    if "PATH_NOT_FOUND" in str(e) or "FileNotFoundException" in str(e) or "does not exist" in str(e):
        row_count, regions = 0, []
    else:
        raise                                  # anything else (permissions, corrupt files) must fail the task

print(f"row_count={row_count}, regions={regions}")

# COMMAND ----------

# Task values: small JSON-serialisable values (each <= 48 KiB as JSON) that later tasks can read
# via dbutils.jobs.taskValues.get(...) or via the reference {{tasks.check_landing.values.<key>}}.
dbutils.jobs.taskValues.set(key="row_count", value=row_count)   # int  -> used by the condition task
dbutils.jobs.taskValues.set(key="regions",   value=regions)     # list -> used as for-each inputs
dbutils.jobs.taskValues.set(key="day_path",  value=day_path)

# The exit value is shown in the run output and returned to a caller of dbutils.notebook.run().
dbutils.notebook.exit(json.dumps({"row_count": row_count, "regions": regions}))