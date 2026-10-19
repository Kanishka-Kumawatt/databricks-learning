# Databricks notebook source
# Phase 9 — the DECLARATIVE version: Lakeflow Spark Declarative Pipelines + AUTO CDC.
#
# This is pipeline SOURCE CODE, not an interactive notebook: add it to a pipeline
# (Jobs & Pipelines -> Create -> ETL pipeline), set the pipeline's default catalog/schema,
# optionally set the configuration key `cdc_source_table`, and run an update.
# AUTO CDC runs only inside Lakeflow pipelines (serverless, or Pro/Advanced edition),
# not on an all-purpose cluster and not in open-source Apache Spark Declarative Pipelines.
#
# Source: bronze_customer_cdc from notebook 01 (append-only, so it can be read as a stream).
# If you re-run notebook 01's reset cell (drop + recreate Bronze), run a FULL REFRESH of this pipeline.
#
# Targets:
#   silver_customer_auto_cdc  SCD1  (current state; deletes remove the row)
#   dim_customer_auto_cdc     SCD2  (history; __START_AT / __END_AT added by the framework)

# COMMAND ----------

from pyspark import pipelines as dp            # current API. Legacy: `import dlt`
from pyspark.sql import functions as F

SOURCE = spark.conf.get("cdc_source_table", "main.de_prep.bronze_customer_cdc")
AUDIT_AND_CONTROL = ["op", "seq", "event_ts", "_batch_id", "_source_file", "_ingested_at"]

# COMMAND ----------

@dp.temporary_view                              # some docs examples use @dp.view
def customer_cdc_events():
    # Exact duplicates (same key AND same seq) are removed here; do not rely on AUTO CDC to define
    # what happens with two events that have the same key and the same sequence value.
    # Note: streaming dropDuplicates without a watermark keeps state for every (customer_id, seq) seen.
    return spark.readStream.table(SOURCE).dropDuplicates(["customer_id", "seq"])

# COMMAND ----------

# ---- SCD Type 1: current state ----
dp.create_streaming_table(name="silver_customer_auto_cdc", comment="Customer current state (SCD1) via AUTO CDC")

dp.create_auto_cdc_flow(
    target="silver_customer_auto_cdc",
    source="customer_cdc_events",
    keys=["customer_id"],                       # natural key
    sequence_by=F.col("seq"),                   # ordering column; NULL values are not supported
    apply_as_deletes=F.expr("op = 'D'"),        # these events delete the key
    except_column_list=AUDIT_AND_CONTROL,       # not copied to the target
    stored_as_scd_type=1,
)

# COMMAND ----------

# ---- SCD Type 2: full history ----
dp.create_streaming_table(name="dim_customer_auto_cdc", comment="Customer history (SCD2) via AUTO CDC")

dp.create_auto_cdc_flow(
    target="dim_customer_auto_cdc",
    source="customer_cdc_events",
    keys=["customer_id"],
    sequence_by=F.col("seq"),                   # -> __START_AT / __END_AT are BIGINT seq values
    apply_as_deletes=F.expr("op = 'D'"),        # SCD2: a delete closes the current version (sets __END_AT)
    except_column_list=AUDIT_AND_CONTROL,
    stored_as_scd_type=2,
    # track_history_except_column_list=["email"],   # variant: email changes update in place, no new version
    name="dim_customer_scd2_flow",
)

# Expected after all three Bronze batches (__START_AT / __END_AT = seq values):
#   C1 Dubai/plus/asha@        1 -> 10  | C1 Abu Dhabi/plus/asha@      10 -> 20 | C1 Abu Dhabi/premium/asha@ 20 -> 21
#   C1 Abu Dhabi/premium/asha.rao@ 21 -> 22 | C1 Dubai/premium/asha.rao@ 22 -> NULL
#   C2 basic 2 -> 11 | C2 plus 11 -> NULL
#   C3 chen@/premium 3 -> 13 | C3 chen.wu@/premium 13 -> 14 | C3 chen.wu@/plus 14 -> NULL
#   C4 Sharjah 4 -> 12 | C4 Dubai 12 -> 15   (late seq 12 placed INTO history; delete at 15 closes it)
#   C5 Ajman/basic 5 -> NULL                 (seq 23 changed no tracked value -> no new version expected)
#   C6 Dubai/basic 16 -> NULL
# 14 rows. SCD1 target: C1 (asha.rao@, Dubai, premium), C2 (Abu Dhabi, plus), C3 (chen.wu@, Dubai, plus),
# C5 (Ajman, basic), C6 (Dubai, basic); C4 absent (deleted). 5 rows.

# COMMAND ----------

# ---- Snapshot-based CDC (Python only): when the source can only give FULL snapshots ----
# AUTO CDC compares consecutive snapshots itself: new keys -> insert, changed rows -> update,
# keys missing from the newer snapshot -> delete. Example (needs a snapshot table; left commented):
#
# dp.create_streaming_table("customer_from_snapshot")
# dp.create_auto_cdc_from_snapshot_flow(
#     target="customer_from_snapshot",
#     source="main.de_prep.src_customer_snapshot",   # a table/view re-read each update, OR a function:
#     # source=next_snapshot,                         #   lambda latest_version -> (DataFrame, version) or None
#     keys=["customer_id"],
#     stored_as_scd_type=2,
# )
#
# def next_snapshot(latest_version):                 # e.g. one file/table per day
#     v = 1 if latest_version is None else latest_version + 1
#     if v > 2:
#         return None                                # no more snapshots
#     return (spark.read.table(f"main.de_prep.snap_customer_day{v}"), v)