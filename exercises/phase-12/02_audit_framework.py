# Databricks notebook source
# Phase 12.3 — a generic audit / control-table framework with atomic restart.
# Tables: pipeline_run, activity_run, error_log. Restart uses replaceWhere (ONE commit) instead of
# delete-then-insert (TWO commits). Invented airline-like data.

# COMMAND ----------

import uuid, datetime as dt
from pyspark.sql import functions as F

CATALOG, SCHEMA = "main", "de_prep"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
def t(n): return f"{CATALOG}.{SCHEMA}.{n}"

spark.sql(f"""CREATE TABLE IF NOT EXISTS {t('audit_pipeline_run')} (
  run_id STRING, pipeline_name STRING, run_date DATE, start_ts TIMESTAMP, end_ts TIMESTAMP,
  status STRING, remarks STRING)""")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {t('audit_activity_run')} (
  run_id STRING, activity_name STRING, source_object STRING, target_object STRING,
  source_count BIGINT, target_count BIGINT, start_ts TIMESTAMP, end_ts TIMESTAMP, status STRING)""")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {t('audit_error_log')} (
  run_id STRING, activity_name STRING, error_ts TIMESTAMP, message STRING)""")

# COMMAND ----------

# ---- Framework functions -------------------------------------------------------------------------------
# Small control rows are inserted with DataFrames (not string-formatted SQL) to avoid quoting/injection bugs.
def _now(): return dt.datetime.now()

def start_pipeline(name, run_date):
    run_id = str(uuid.uuid4())
    (spark.createDataFrame([(run_id, name, run_date, _now(), None, "R", None)],
                           spark.table(t("audit_pipeline_run")).schema)
          .write.mode("append").saveAsTable(t("audit_pipeline_run")))
    return run_id

def end_pipeline(run_id, status, remarks=""):
    spark.sql(f"UPDATE {t('audit_pipeline_run')} SET end_ts = current_timestamp(), status = :s, remarks = :r "
              "WHERE run_id = :id", args={"s": status, "r": remarks, "id": run_id})

def log_activity(run_id, activity, src, tgt, src_cnt, tgt_cnt, start, status):
    (spark.createDataFrame([(run_id, activity, src, tgt, src_cnt, tgt_cnt, start, _now(), status)],
                           spark.table(t("audit_activity_run")).schema)
          .write.mode("append").saveAsTable(t("audit_activity_run")))

def log_error(run_id, activity, message):
    (spark.createDataFrame([(run_id, activity, _now(), message[:2000])],
                           spark.table(t("audit_error_log")).schema)
          .write.mode("append").saveAsTable(t("audit_error_log")))

def succeeded_activities(name, run_date):
    """Activities already successful for this pipeline + business date -> skipped on restart."""
    return {r.activity_name for r in spark.sql(f"""
        SELECT a.activity_name FROM {t('audit_activity_run')} a JOIN {t('audit_pipeline_run')} p USING (run_id)
        WHERE p.pipeline_name = :n AND p.run_date = :d AND a.status = 'S'""",
        args={"n": name, "d": run_date}).collect()}       # small control data: collect is fine

# COMMAND ----------

# ---- Sample source and the load activity ---------------------------------------------------------------
RUN_DATE = dt.date(2026, 10, 1)
src = spark.createDataFrame(
    [("BK1", "AUH-LHR", 820.0), ("BK2", "AUH-JFK", 1250.0), ("BK3", "AUH-SYD", 990.0), ("BK4", None, 400.0)],
    "booking_id STRING, route STRING, fare DOUBLE").withColumn("load_date", F.lit(RUN_DATE))
TARGET = t("audit_stg_bookings")
spark.sql(f"CREATE TABLE IF NOT EXISTS {TARGET} (booking_id STRING, route STRING, fare DOUBLE, load_date DATE)")

def load_bookings(run_id, fail_midway=False):
    start = _now()
    cleaned = src.filter("route IS NOT NULL")                       # business rule drops 1 row
    if fail_midway:
        raise RuntimeError("simulated executor loss during write")
    # ATOMIC slice write: one Delta commit replaces exactly this run's business date.
    (cleaned.write.mode("overwrite")
            .option("replaceWhere", f"load_date = '{RUN_DATE}'")
            .saveAsTable(TARGET))
    tgt_cnt = spark.table(TARGET).filter(F.col("load_date") == RUN_DATE).count()
    log_activity(run_id, "load_bookings", "landing/bookings", TARGET, src.count(), tgt_cnt, start, "S")

ACTIVITIES = {"load_bookings": load_bookings}

def run_pipeline(name, fail_midway=False):
    run_id = start_pipeline(name, RUN_DATE)
    done = succeeded_activities(name, RUN_DATE)
    try:
        for act, fn in ACTIVITIES.items():
            if act in done:
                print(f"skip {act}: already succeeded for {RUN_DATE}")
                continue
            try:
                fn(run_id, fail_midway=fail_midway)
            except Exception as e:
                log_error(run_id, act, repr(e))
                log_activity(run_id, act, "landing/bookings", TARGET, None, None, _now(), "F")
                raise
        end_pipeline(run_id, "S")
    except Exception:
        end_pipeline(run_id, "F", "see audit_error_log")
        raise                                                         # surface the failure to the orchestrator
    return run_id

# COMMAND ----------

# Run 1 fails mid-way, run 2 (restart) succeeds, run 3 (accidental rerun) skips.
try:
    run_pipeline("bookings_daily", fail_midway=True)
except RuntimeError as e:
    print("run 1 failed as expected:", e)
run_pipeline("bookings_daily")
run_pipeline("bookings_daily")

display(spark.table(t("audit_pipeline_run")).orderBy("start_ts"))
display(spark.table(t("audit_activity_run")).orderBy("start_ts"))
display(spark.table(t("audit_error_log")))
print("target rows for run date:", spark.table(TARGET).filter(F.col("load_date") == RUN_DATE).count())  # 3

# COMMAND ----------

# Reconciliation: source vs target counts with an explanation column. 4 read, 3 written (1 dropped by rule).
display(spark.sql(f"""
  SELECT activity_name, source_count, target_count, source_count - target_count AS diff,
         CASE WHEN source_count = target_count THEN 'match' ELSE 'investigate / expected rule drops' END AS verdict
  FROM {t('audit_activity_run')} WHERE status = 'S'"""))

# COMMAND ----------

# Why replaceWhere and not delete-then-insert?
#   DELETE FROM target WHERE load_date = d   -- commit 1  (readers now see NO rows for d)
#   INSERT INTO target SELECT ...            -- commit 2  (a crash before this leaves the gap)
# replaceWhere is a single commit: readers see either the old slice or the new one, never a gap.
display(spark.sql(f"DESCRIBE HISTORY {TARGET}").select("version", "operation", "operationParameters"))