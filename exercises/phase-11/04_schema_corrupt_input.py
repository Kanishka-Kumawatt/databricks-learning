# Databricks notebook source
# Phase 11.4 — Schema mismatch and corrupt input.
# One small JSON file with: good rows, a malformed line, a type mismatch, an extra column and a
# case-mismatched column. Read it every way and compare what you get.
# Databricks-specific options are marked [DBX]: rescuedDataColumn, badRecordsPath.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType

CATALOG, SCHEMA = "main", "de_prep"
VOLUME = "lab_files"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")
BASE = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/phase11_04"
T = lambda name: f"{CATALOG}.{SCHEMA}.{name}"

lines = [
    '{"order_id": "1", "amount": 10.50, "order_ts": "2026-10-16T09:00:00"}',
    '{"order_id": "2", "amount": 20.00, "order_ts": "2026-10-16T09:05:00"}',
    '{"order_id": "3", "amount": "abc", "order_ts": "2026-10-16T09:10:00"}',                 # type mismatch
    '{"order_id": "4", "amount": 5.25, "order_ts": "2026-10-16T09:15:00", "coupon": "X1"}',  # extra column
    '{"order_id": "5", "Amount": 7.00, "order_ts": "2026-10-16T09:20:00"}',                  # case mismatch
    '{"order_id": "6", "amount": 9.99, "order_ts": ',                                         # malformed (truncated)
]
dbutils.fs.rm(BASE, True)
dbutils.fs.put(f"{BASE}/in/orders_0001.json", "\n".join(lines) + "\n", True)

schema = StructType([
    StructField("order_id", StringType()),
    StructField("amount", DecimalType(10, 2)),
    StructField("order_ts", TimestampType()),
])
src = f"{BASE}/in"

# COMMAND ----------

# ---------------- 1. PERMISSIVE (default) + corrupt record column (open-source Spark) ----------------
schema_with_corrupt = StructType(schema.fields + [StructField("_corrupt_record", StringType())])   # new object: StructType.add() would mutate `schema`
permissive = (spark.read.schema(schema_with_corrupt)
              .option("mode", "PERMISSIVE")
              .option("columnNameOfCorruptRecord", "_corrupt_record")
              .json(src))
# Spark refuses queries that reference ONLY the corrupt-record column of a raw file scan;
# cache (or write) the parsed result first, then filter on it.
permissive = permissive.cache()
display(permissive)
# Expected: rows 1,2,4 parse (coupon silently ignored); row 3 has amount NULL and the full raw line in
# _corrupt_record; row 5: amount NULL (case-sensitivity/matching depends on spark.sql.caseSensitive —
# default false matches "Amount" to "amount" in many cases: compare with your output); row 6: all
# columns NULL, raw text in _corrupt_record.
display(permissive.where("_corrupt_record IS NOT NULL"))

# COMMAND ----------

# ---------------- 2. DROPMALFORMED and FAILFAST ----------------
dropped = spark.read.schema(schema).option("mode", "DROPMALFORMED").json(src)
print("DROPMALFORMED rows:", dropped.count(), " <- silently lost rows: dangerous without a count check")

try:
    spark.read.schema(schema).option("mode", "FAILFAST").json(src).collect()
except Exception as e:
    print("FAILFAST raised:", type(e).__name__, str(e)[:200])

# COMMAND ----------

# ---------------- 3. [DBX] rescued data column ----------------
# Captures values that don't match the schema (type mismatch, extra fields, case mismatch) as JSON,
# plus the source file path, so NOTHING is lost and the row is still loaded.
rescued = (spark.read.schema(schema)
           .option("rescuedDataColumn", "_rescued_data")
           .json(src))
display(rescued)
# Expected: row 3 -> amount NULL, _rescued_data = {"amount":"abc", "_file_path": ...};
# row 4 -> {"coupon":"X1", ...}; row 5 -> {"Amount":7.0, ...}. Malformed row 6: still a problem row
# (rescued data handles mismatches, not broken JSON).

# COMMAND ----------

# ---------------- 4. [DBX] badRecordsPath ----------------
bad_path = f"{BASE}/bad_records"
with_bad = (spark.read.schema(schema)
            .option("badRecordsPath", bad_path)
            .json(src))
print("rows loaded with badRecordsPath:", with_bad.count())
display(dbutils.fs.ls(bad_path))     # <timestamp>/bad_records/part-... JSON files with the record + reason
# Caveat (docs): non-transactional; transient errors are treated as bad records. Prefer the rescued
# data column or a quarantine table for pipelines that must be exactly reproducible.

# COMMAND ----------

# ---------------- 5. Delta schema enforcement on WRITE ----------------
spark.sql(f"DROP TABLE IF EXISTS {T('schema_demo')}")
rescued.select("order_id", "amount", "order_ts").write.saveAsTable(T("schema_demo"))

extra_col = rescued.select("order_id", "amount", "order_ts", F.lit("web").alias("channel"))
try:
    extra_col.write.mode("append").saveAsTable(T("schema_demo"))
except Exception as e:
    print("Schema enforcement blocked the append:", str(e)[:250])     # e.g. a schema mismatch error (DELTA_METADATA_MISMATCH; verify class for your runtime)

# Intentional, reviewed evolution:
extra_col.write.mode("append").option("mergeSchema", "true").saveAsTable(T("schema_demo"))
print(spark.table(T("schema_demo")).columns)                          # now includes channel

wrong_type = rescued.select(F.col("order_id").cast("int").alias("order_id"), "amount", "order_ts")
try:
    wrong_type.write.mode("append").saveAsTable(T("schema_demo"))
except Exception as e:
    print("Type change blocked:", str(e)[:250])                        # string -> int is not a safe widening

# COMMAND ----------

# ---------------- 6. Turning it into a pipeline rule: quarantine instead of drop ----------------
checked = permissive.withColumn("_dq_error",
    F.when(F.col("_corrupt_record").isNotNull() & F.col("order_id").isNull(), "malformed_record")
     .when(F.col("_corrupt_record").isNotNull(), "type_mismatch")
     .when(F.col("amount").isNull(), "amount_missing")          # e.g. the case-mismatched "Amount" row if not matched
     .when(F.col("order_ts").isNull(), "order_ts_missing"))
good = checked.where("_dq_error IS NULL").drop("_dq_error", "_corrupt_record")
bad  = checked.where("_dq_error IS NOT NULL").withColumn("_quarantined_at", F.current_timestamp())
print(f"good={good.count()}  quarantined={bad.count()}  total={checked.count()}")
assert good.count() + bad.count() == checked.count()                 # nothing silently lost
display(bad)
permissive.unpersist()