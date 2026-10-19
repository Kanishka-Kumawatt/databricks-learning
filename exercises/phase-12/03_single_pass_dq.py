# Databricks notebook source
# Phase 12.3 / 12.8 — per-column null check (N scans) rewritten as ONE aggregation pass, with timing.
# Invented wide "flight events" table.

# COMMAND ----------

import time
from pyspark.sql import functions as F

CATALOG, SCHEMA = "main", "de_prep"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
TBL = f"{CATALOG}.{SCHEMA}.dq_flight_events"

N_ROWS, N_COLS = 2_000_000, 40
# Column c_k is NULL for roughly 1 in (k+2) rows so every column has a different, checkable null count.
cols = [F.when(F.col("id") % (k + 2) == 0, F.lit(None)).otherwise(F.col("id") * k).alias(f"c_{k}")
        for k in range(N_COLS)]
(spark.range(N_ROWS).select("id", *cols)
      .write.mode("overwrite").saveAsTable(TBL))
df = spark.table(TBL)

# COMMAND ----------

# BEFORE: one Spark job (one full scan) per column  -> N_COLS + 1 scans
t0 = time.time()
nulls_before = {c: df.filter(F.col(c).isNull()).count() for c in df.columns}
t_before = time.time() - t0
print(f"per-column loop : {t_before:6.1f}s  ({len(df.columns)} jobs)")

# COMMAND ----------

# AFTER: one job, one scan; every column's null count computed in the same aggregation
t0 = time.time()
row = df.select(
    F.count(F.lit(1)).alias("_rows"),
    *[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns]
).first()
nulls_after = {c: row[c] for c in df.columns}
t_after = time.time() - t0
print(f"single pass     : {t_after:6.1f}s  (1 job)")

assert nulls_before == nulls_after, "results must be identical"
print("identical results; speed-up ~", round(t_before / max(t_after, 0.001), 1), "x")

# COMMAND ----------

# Same single pass, extended into a small DQ report (null %, distinct key check) — still a couple of jobs, not N.
rows = row["_rows"]
report = spark.createDataFrame(
    [(c, nulls_after[c], round(100.0 * nulls_after[c] / rows, 2)) for c in df.columns],
    "column STRING, null_count BIGINT, null_pct DOUBLE")
display(report.orderBy(F.desc("null_pct")))

key_check = df.agg(F.count("id").alias("n"), F.countDistinct("id").alias("n_distinct")).first()
print("primary key unique:", key_check.n == key_check.n_distinct)

# COMMAND ----------

# Notes
# - The loop's cost grows with the number of columns (N scans); the single pass costs ~one scan.
# - Timings vary with caching, Photon and cluster size; the job count in the Spark UI is the robust evidence.
# - In declarative pipelines, express rules as expectations (expect / expect_or_drop / expect_or_fail)
#   so they are evaluated during the write and reported in the pipeline event log.