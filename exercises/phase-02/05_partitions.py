# Databricks notebook source
# Phase 2.5 — Partitions: count them, see them, change them, and see how file reads decide them.
# Works on classic and serverless compute. Section 5 writes ONE small table: {CATALOG}.{SCHEMA}.p2_numbers.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG, SCHEMA = "main", "de_prep"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
T = lambda name: f"{CATALOG}.{SCHEMA}.{name}"

IS_CONNECT = "connect" in type(spark).__module__
print("Spark Connect session (serverless / standard access mode):", IS_CONNECT)

def num_partitions(df):
    """Partition count.
    Classic compute: df.rdd.getNumPartitions() (plan-level count, includes empty partitions).
    Serverless / standard access mode: RDD APIs are NOT supported -> count NON-EMPTY partitions
    with spark_partition_id() (this runs a job)."""
    try:
        return df.rdd.getNumPartitions()
    except Exception:
        return df.select(F.spark_partition_id().alias("p")).distinct().count()

def rows_per_partition(df):
    return (df.groupBy(F.spark_partition_id().alias("pid")).count().orderBy("pid"))

def set_conf(key, value):
    try:
        spark.conf.set(key, value)
        print(f"set {key} = {value}")
    except Exception as e:
        print(f"cannot set {key} on this compute ({type(e).__name__}) — continuing with the default")

# COMMAND ----------

# 1) Explicit partitions and how rows are spread.
nums = spark.range(0, 100, 1, numPartitions=4)
print("partitions:", num_partitions(nums))        # Expected: 4
rows_per_partition(nums).show()                   # Expected: pid 0..3, 25 rows each

# COMMAND ----------

# 2) Narrow operations keep the partition count; union adds them.
small = nums.filter("id < 10")
print("after filter:", num_partitions(small))
# Expected: classic 4 (plan still has 4 partitions, 3 of them empty);
#           serverless fallback 1 (only partition 0 has rows 0..9).
rows_per_partition(small).show()                  # Expected: only pid 0, count 10

print("after union:", num_partitions(nums.union(nums)))   # Expected: 8

# COMMAND ----------

# 3) After a shuffle: spark.sql.shuffle.partitions, then AQE coalescing.
def get_conf(key):
    try:
        return spark.conf.get(key)
    except Exception:
        return "n/a on this compute"

print("spark.sql.shuffle.partitions =", get_conf("spark.sql.shuffle.partitions"))
print("spark.sql.adaptive.enabled   =", get_conf("spark.sql.adaptive.enabled"))

by_key = nums.groupBy((F.col("id") % 10).alias("k")).count()
by_key.explain()
# Look for: Exchange hashpartitioning((id#.. % 10), 200)   <- or your configured number / auto
# With AQE the top line is AdaptiveSparkPlan isFinalPlan=false (plan may change at runtime).

print("non-empty partitions after groupBy:",
      by_key.select(F.spark_partition_id().alias("p")).distinct().count())
# Expected: small number (<= 10). With AQE on, tiny shuffle output is usually coalesced into 1 partition.
# Classic compute: open the Spark UI -> SQL/DataFrame tab -> this query: the shuffle read node shows
# "AQEShuffleRead ... coalesced" and the reduce stage ran only a few tasks, not 200.

# COMMAND ----------

# 4) repartition vs coalesce — preview.
r = nums.repartition(8)
c = nums.coalesce(2)
r.explain()     # Expected: Exchange RoundRobinPartitioning(8), REPARTITION_BY_NUM   -> shuffle
c.explain()     # Expected: Coalesce 2                                               -> no Exchange
print("repartition(8):", num_partitions(r))   # Expected: 8
print("coalesce(2):   ", num_partitions(c))   # Expected: 2
rows_per_partition(r).show()                  # roughly 12-13 rows each (round-robin; exact split not guaranteed)
rows_per_partition(c).show()                  # typically 50 / 50 (neighbouring partitions merged)
print("coalesce(10) on 4 partitions:", num_partitions(nums.coalesce(10)))   # Expected: 4 — coalesce can't increase

# COMMAND ----------

# 5) File reads: how many input partitions does a table scan create?
(spark.range(0, 2_000_000)
      .repartition(16)                                  # aim for ~16 files
      .write.mode("overwrite").saveAsTable(T("p2_numbers")))

detail = spark.sql(f"DESCRIBE DETAIL {T('p2_numbers')}").select("numFiles", "sizeInBytes").first()
print("files:", detail["numFiles"], " bytes:", detail["sizeInBytes"])
# Optimized writes / auto compaction on Databricks can change the file count — trust numFiles, not 16.

scan = spark.table(T("p2_numbers"))
print("read partitions (defaults):", num_partitions(scan))
# Predict with: maxSplitBytes = min(128MB, max(4MB, totalBytes/minPartitionNum))
# Data here is only a few MB, so each file (size + 4 MB open cost) is packed: expect roughly
# min(numFiles, ~cores) partitions or fewer. Write your prediction BEFORE looking.

# COMMAND ----------

# 6) Change maxPartitionBytes (allowed on serverless too) and re-read.
original = get_conf("spark.sql.files.maxPartitionBytes")
set_conf("spark.sql.files.maxPartitionBytes", str(1 * 1024 * 1024))     # 1 MB
print("read partitions (maxPartitionBytes=1MB):", num_partitions(spark.table(T("p2_numbers"))))
# Expected: >= the previous number — files larger than 1 MB are split; small files are packed less.
set_conf("spark.sql.files.maxPartitionBytes", original)

# COMMAND ----------

# 7) Clean up (optional)
# spark.sql(f"DROP TABLE IF EXISTS {T('p2_numbers')}")