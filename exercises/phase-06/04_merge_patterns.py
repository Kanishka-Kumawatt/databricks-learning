# Databricks notebook source
# Phase 6.4 — MERGE patterns and what they do to files: upsert + delete, the multiple-match error, insert-only MERGE,
# WHEN NOT MATCHED BY SOURCE, and how operationMetrics expose file churn.

# COMMAND ----------

from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable

CATALOG, SCHEMA = "main", "de_prep"
USE_DELETION_VECTORS = True
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
TGT = f"{CATALOG}.{SCHEMA}.p6_silver_orders"
TARGET_COLS = ["order_id", "customer_id", "amount", "status", "order_date"]

def reset():
    spark.sql(f"DROP TABLE IF EXISTS {TGT}")
    spark.sql(f"""CREATE TABLE {TGT} (order_id INT, customer_id STRING, amount DOUBLE, status STRING, order_date DATE)
                  TBLPROPERTIES ('delta.enableDeletionVectors' = '{str(USE_DELETION_VECTORS).lower()}')""")
    spark.sql(f"""INSERT INTO {TGT} VALUES
        (101, 'C1', 250.0, 'placed', DATE'2026-10-14'),
        (102, 'C2', 100.0, 'placed', DATE'2026-10-15'),
        (103, 'C3',  40.0, 'placed', DATE'2026-10-15'),
        (104, 'C1',  75.5, 'placed', DATE'2026-10-16')""")

def last_metrics():
    r = spark.sql(f"DESCRIBE HISTORY {TGT} LIMIT 1").first()
    m = dict(r["operationMetrics"] or {})
    keys = ["numSourceRows", "numTargetRowsUpdated", "numTargetRowsInserted", "numTargetRowsDeleted",
            "numTargetRowsCopied", "numTargetFilesRemoved", "numTargetFilesAdded", "numTargetDeletionVectorsAdded"]
    print(r["operation"], {k: m.get(k) for k in keys})

reset()
display(spark.table(TGT).orderBy("order_id"))

# COMMAND ----------

# 1) UPSERT + DELETE in one MERGE. Source has a duplicate for 102 on purpose.
raw = spark.createDataFrame([
    (102, "C2", 100.0, "packed",    "2026-10-15", "U", 1),
    (102, "C2", 100.0, "shipped",   "2026-10-15", "U", 2),   # later change of the same key
    (103, "C3",  40.0, "cancelled", "2026-10-15", "D", 1),
    (105, "C4", 300.0, "placed",    "2026-10-16", "U", 1),
], "order_id INT, customer_id STRING, amount DOUBLE, status STRING, order_date STRING, op STRING, seq INT") \
    .withColumn("order_date", F.to_date("order_date"))

def merge(src):
    (DeltaTable.forName(spark, TGT).alias("t")
        .merge(src.alias("s"), "t.order_id = s.order_id AND t.order_date >= DATE'2026-10-01'")
        .whenMatchedDelete(condition="s.op = 'D'")
        .whenMatchedUpdate(condition="s.op = 'U'", set={"amount": "s.amount", "status": "s.status"})
        .whenNotMatchedInsert(condition="s.op = 'U'", values={c: f"s.{c}" for c in TARGET_COLS})
        .execute())

# 1a) Without dedup -> error: two source rows match target row 102
try:
    merge(raw)
except Exception as e:
    print("expected failure:", type(e).__name__, "|", str(e)[:200])

# 1b) Dedup to the latest change per key, then MERGE
w = Window.partitionBy("order_id").orderBy(F.col("seq").desc())
src = raw.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn", "seq")
merge(src)
last_metrics()
display(spark.table(TGT).orderBy("order_id"))      # 101 placed, 102 shipped, 104 placed, 105 placed

# COMMAND ----------

# 2) INSERT-ONLY MERGE ("insert if not exists") — Delta does an anti-join + append; no target file is rewritten.
new_rows = spark.createDataFrame([(104, "C1", 75.5, "placed", "2026-10-16"), (106, "C5", 20.0, "placed", "2026-10-17")],
                                 "order_id INT, customer_id STRING, amount DOUBLE, status STRING, order_date STRING") \
    .withColumn("order_date", F.to_date("order_date"))
(DeltaTable.forName(spark, TGT).alias("t")
    .merge(new_rows.alias("s"), "t.order_id = s.order_id")
    .whenNotMatchedInsertAll()
    .execute())
last_metrics()                                      # numTargetRowsInserted=1, numTargetFilesRemoved=0

# COMMAND ----------

# 3) WHEN NOT MATCHED BY SOURCE — full-snapshot sync. Only safe when the source is a COMPLETE snapshot.
snapshot = spark.table(TGT).where("order_id <> 101")          # pretend the source system no longer has 101
(DeltaTable.forName(spark, TGT).alias("t")
    .merge(snapshot.alias("s"), "t.order_id = s.order_id")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .whenNotMatchedBySourceDelete()
    .execute())
last_metrics()
display(spark.table(TGT).orderBy("order_id"))      # 101 gone

# COMMAND ----------

# 4) Same thing in SQL, with a condition on NOT MATCHED BY SOURCE (limit the blast radius to recent dates).
spark.createDataFrame([(102, "C2", 100.0, "delivered", "2026-10-15")],
                      "order_id INT, customer_id STRING, amount DOUBLE, status STRING, order_date STRING") \
     .withColumn("order_date", F.to_date("order_date")).createOrReplaceTempView("recent_snapshot")
spark.sql(f"""
MERGE INTO {TGT} AS t
USING recent_snapshot AS s
ON t.order_id = s.order_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE AND t.order_date = DATE'2026-10-15' THEN DELETE
""")
last_metrics()
display(spark.table(TGT).orderBy("order_id"))

# COMMAND ----------

# 5) Compare with copy-on-write: set USE_DELETION_VECTORS = False at the top, re-run, and compare
#    numTargetRowsCopied / numTargetFilesRemoved / numTargetDeletionVectorsAdded.
display(spark.sql(f"DESCRIBE HISTORY {TGT}").select("version", "operation", "operationMetrics").orderBy("version"))