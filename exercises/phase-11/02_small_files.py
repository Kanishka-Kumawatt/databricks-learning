# Databricks notebook source
# Phase 11.2 — The small-file problem and partition tuning.
# Part 1: 150 tiny appends -> many small files -> slow scans. Fix with OPTIMIZE, then prevent with
#         optimized writes / auto compaction / liquid clustering.
# Part 2: over-partitioning (partitionBy a high-cardinality column) and shuffle-partition tuning.
# Classic compute recommended (Part 2 sets AQE configs). Takes ~5 minutes.

# COMMAND ----------

import time
from pyspark.sql import functions as F

CATALOG, SCHEMA = "main", "de_prep"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
T = lambda name: f"{CATALOG}.{SCHEMA}.{name}"

def files_and_size(table):
    d = spark.sql(f"DESCRIBE DETAIL {table}").select("numFiles", "sizeInBytes").first()
    return d["numFiles"], d["sizeInBytes"]

def timed_scan(table):
    """A scan that must read every file (a filter on a non-stats-friendly expression + aggregate)."""
    t0 = time.time()
    (spark.table(table).where(F.col("payload").contains("7"))
        .groupBy("event_date").agg(F.sum("value")).write.format("noop").mode("overwrite").save())
    return round(time.time() - t0, 1)

# COMMAND ----------

# ---------------- PART 1: create the problem ----------------
spark.sql(f"DROP TABLE IF EXISTS {T('sf_events')}")
spark.sql(f"""
  CREATE TABLE {T('sf_events')} (id BIGINT, event_date DATE, value DOUBLE, payload STRING)
  TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'false',   -- switch OFF the protections so we can see the problem
    'delta.autoOptimize.autoCompact'   = 'false'
  )""")

def tiny_batch(i, rows=2_000):
    return (spark.range(rows)
            .withColumn("id", F.col("id") + i * rows)
            .withColumn("event_date", F.date_add(F.lit("2026-10-01"), (F.col("id") % 10).cast("int")))
            .withColumn("value", F.rand(seed=i))
            .withColumn("payload", F.sha2(F.col("id").cast("string"), 256))
            .repartition(4))          # each micro-append writes ~4 files, like a busy streaming writer

t0 = time.time()
for i in range(150):                   # 150 commits ≈ a streaming job with a 1-minute trigger for 2.5 hours
    tiny_batch(i).write.mode("append").saveAsTable(T("sf_events"))
print(f"150 appends took {time.time() - t0:.0f} s")

n, size = files_and_size(T("sf_events"))
print(f"BEFORE: {n} files, {size/1e6:.1f} MB total, avg {size/n/1e3:.0f} KB per file")
print("scan seconds (before):", timed_scan(T("sf_events")))
# Spark UI for the scan: the scan stage has ~1 task per file (or files packed into tasks by
# maxPartitionBytes/openCostInBytes); each task's duration is dominated by open/metadata overhead.

# COMMAND ----------

# ---------------- FIX 1: compact existing files ----------------
display(spark.sql(f"OPTIMIZE {T('sf_events')}"))     # metrics: numFilesAdded / numFilesRemoved
n, size = files_and_size(T("sf_events"))
print(f"AFTER OPTIMIZE: {n} files, avg {size/n/1e6:.1f} MB per file")
print("scan seconds (after):", timed_scan(T("sf_events")))

# The old small files are still on storage (time travel needs them) until VACUUM removes files
# older than the retention period (default 7 days). Do NOT run VACUUM with RETAIN 0 HOURS on real tables.
display(spark.sql(f"DESCRIBE HISTORY {T('sf_events')}").select("version", "operation", "operationMetrics").limit(3))

# COMMAND ----------

# ---------------- FIX 2: prevent it — optimized writes + auto compaction ----------------
spark.sql(f"""ALTER TABLE {T('sf_events')} SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',    -- shuffle before write -> fewer, larger files per commit
  'delta.autoOptimize.autoCompact'   = 'true')""")  # after a write, compacts small files synchronously if worthwhile
before, _ = files_and_size(T("sf_events"))
for i in range(150, 170):
    tiny_batch(i).write.mode("append").saveAsTable(T("sf_events"))
after, _ = files_and_size(T("sf_events"))
print(f"20 more appends with protections ON: files {before} -> {after} (expect far fewer than 80 new files)")

# COMMAND ----------

# ---------------- FIX 3: liquid clustering (current recommendation for new tables) ----------------
# CLUSTER BY replaces partitioning + ZORDER for most tables; OPTIMIZE clusters incrementally.
# On UC managed tables, predictive optimization can run OPTIMIZE for you (check if enabled in your account).
spark.sql(f"DROP TABLE IF EXISTS {T('sf_events_lc')}")
spark.sql(f"CREATE TABLE {T('sf_events_lc')} CLUSTER BY (event_date) AS SELECT * FROM {T('sf_events')}")
spark.sql(f"OPTIMIZE {T('sf_events_lc')}")
display(spark.sql(f"DESCRIBE DETAIL {T('sf_events_lc')}").select("numFiles", "sizeInBytes", "clusteringColumns"))

# COMMAND ----------

# ---------------- PART 2a: over-partitioning ----------------
# partitionBy a high-cardinality column => one folder per value, each with tiny files.
spark.sql(f"DROP TABLE IF EXISTS {T('sf_overpartitioned')}")
(spark.table(T("sf_events")).withColumn("bucket", (F.col("id") % 2000).cast("int"))
    .write.partitionBy("bucket").saveAsTable(T("sf_overpartitioned")))
n, size = files_and_size(T("sf_overpartitioned"))
print(f"partitionBy(bucket with 2000 values): {n} files, avg {size/n/1e3:.0f} KB")
# Rule of thumb: partition only tables > ~1 TB, by a low-cardinality column used in filters, aiming for
# >= ~1 GB per partition. Otherwise don't partition (or use liquid clustering).

# COMMAND ----------

# ---------------- PART 2b: shuffle partitions -> output file count ----------------
agg = lambda: spark.table(T("sf_events")).groupBy("id").agg(F.sum("value").alias("v"))

spark.conf.set("spark.sql.adaptive.enabled", "false")          # legacy behaviour: exactly N reduce tasks
spark.conf.set("spark.sql.shuffle.partitions", "2000")
spark.sql(f"DROP TABLE IF EXISTS {T('sf_agg')}")
spark.sql(f"CREATE TABLE {T('sf_agg')} (id BIGINT, v DOUBLE) TBLPROPERTIES ('delta.autoOptimize.optimizeWrite'='false')")
agg().write.mode("overwrite").saveAsTable(T("sf_agg"))
print("AQE off, 2000 shuffle partitions ->", files_and_size(T("sf_agg"))[0], "files")

spark.conf.set("spark.sql.adaptive.enabled", "true")           # AQE coalesces small shuffle partitions
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
agg().write.mode("overwrite").saveAsTable(T("sf_agg"))
print("AQE on (coalesce)              ->", files_and_size(T("sf_agg"))[0], "files")

agg().coalesce(4).write.mode("overwrite").saveAsTable(T("sf_agg"))   # explicit control before write
print("coalesce(4) before write       ->", files_and_size(T("sf_agg"))[0], "files")
spark.conf.unset("spark.sql.shuffle.partitions")

# COMMAND ----------

# Clean up (optional)
# for t in ("sf_events", "sf_events_lc", "sf_overpartitioned", "sf_agg"): spark.sql(f"DROP TABLE IF EXISTS {T(t)}")