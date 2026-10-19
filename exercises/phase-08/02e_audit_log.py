# Databricks notebook source
# Phase 8.2 — task `audit_log`: run_if = ALL_DONE, so it runs whether upstream succeeded, failed or was excluded.
# All identifiers come from DYNAMIC VALUE REFERENCES resolved by the Jobs service before the task starts.

# COMMAND ----------

from pyspark.sql import Row, functions as F

for name, default in [("job_id", "local"), ("job_run_id", "local"), ("task_run_id", "local"),
                      ("trigger_type", "manual"), ("repair_count", "0"), ("run_date", "2026-10-16"),
                      ("landed_rows", "0")]:
    dbutils.widgets.text(name, default)

audit = Row(**{k: dbutils.widgets.get(k) for k in
               ["job_id", "job_run_id", "task_run_id", "trigger_type", "repair_count", "run_date", "landed_rows"]})
print(audit)

# Append is acceptable here: an audit log records attempts, and job_run_id + repair_count identify each one.
(spark.createDataFrame([audit])
      .withColumn("logged_at", F.current_timestamp())
      .write.mode("append").saveAsTable("main.de_prep.job_audit_log"))