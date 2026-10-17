# Databricks notebook source
# Phase 1.7 — incremental load with a high-water mark (watermark) control table.
# Shows: first load, incremental changes, idempotent re-run, the DELETE blind spot,
# and the late-commit problem solved with a lookback window + MERGE.

# COMMAND ----------

from datetime import timedelta
from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable

CATALOG, SCHEMA = "main", "de_prep"          # change to a catalog/schema you can write to
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
SRC = f"{CATALOG}.{SCHEMA}.src_orders"        # simulates the source database table
TGT = f"{CATALOG}.{SCHEMA}.silver_orders_incr"
CTL = f"{CATALOG}.{SCHEMA}.etl_watermark"

# COMMAND ----------

# Reset the demo
for t in (SRC, TGT, CTL):
    spark.sql(f"DROP TABLE IF EXISTS {t}")

spark.sql(f"CREATE TABLE {SRC} (order_id INT, customer_id STRING, amount DECIMAL(10,2), updated_at TIMESTAMP)")
spark.sql(f"""INSERT INTO {SRC} VALUES
  (101, 'C1', 250.00, TIMESTAMP'2026-10-16 09:15:00'),
  (102, 'C2', 100.00, TIMESTAMP'2026-10-16 10:02:00'),
  (103, 'C3',  40.00, TIMESTAMP'2026-10-16 11:30:00')""")
spark.sql(f"CREATE TABLE {CTL} (table_name STRING, last_value TIMESTAMP, updated_at TIMESTAMP)")

# COMMAND ----------

def get_watermark(name):
    row = spark.table(CTL).filter(F.col("table_name") == name).first()
    return row["last_value"] if row else None

def set_watermark(name, value):
    # Parameter markers (:name) instead of string formatting for values.
    spark.sql(f"""
        MERGE INTO {CTL} t
        USING (SELECT :name AS table_name, CAST(:value AS TIMESTAMP) AS last_value) s
        ON t.table_name = s.table_name
        WHEN MATCHED THEN UPDATE SET last_value = s.last_value, updated_at = current_timestamp()
        WHEN NOT MATCHED THEN INSERT (table_name, last_value, updated_at)
                              VALUES (s.table_name, s.last_value, current_timestamp())
    """, args={"name": name, "value": str(value)})

def incremental_load(lookback_minutes: int = 0):
    wm = get_watermark("src_orders")

    # Pin ONE source snapshot so count / merge / max all see the same data.
    # (For a JDBC source you would land the extract in Bronze first, for the same reason.)
    src_version = spark.sql(f"DESCRIBE HISTORY {SRC} LIMIT 1").first()["version"]
    changes = spark.sql(f"SELECT * FROM {SRC} VERSION AS OF {src_version}")

    if wm is not None:
        lower = wm - timedelta(minutes=lookback_minutes)
        changes = changes.filter(F.col("updated_at") > F.lit(lower))

    # Several versions of the same key in one extract -> keep only the latest.
    w = Window.partitionBy("order_id").orderBy(F.col("updated_at").desc())
    changes = changes.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

    n = changes.count()
    if n == 0:
        print(f"No changes after {wm}")
        return

    if not spark.catalog.tableExists(TGT):
        changes.write.saveAsTable(TGT)
    else:
        (DeltaTable.forName(spark, TGT).alias("t")
            .merge(changes.alias("s"), "t.order_id = s.order_id")
            .whenMatchedUpdateAll(condition="s.updated_at > t.updated_at")   # never overwrite newer with older
            .whenNotMatchedInsertAll()
            .execute())

    # Advance the watermark only AFTER the write succeeded, using the max of the data we processed.
    new_wm = changes.agg(F.max("updated_at")).first()[0]
    if wm is None or new_wm > wm:
        set_watermark("src_orders", new_wm)
    print(f"Processed {n} row(s); watermark now {get_watermark('src_orders')}")

# COMMAND ----------

# RUN 1 — first (initial) load: all 3 rows
incremental_load()
display(spark.table(TGT).orderBy("order_id"))

# COMMAND ----------

# Day 2 in the source: one update, one insert, one delete
spark.sql(f"UPDATE {SRC} SET amount = 120.00, updated_at = TIMESTAMP'2026-10-17 08:00:00' WHERE order_id = 102")
spark.sql(f"INSERT INTO {SRC} VALUES (104, 'C1', 75.50, TIMESTAMP'2026-10-17 09:00:00')")
spark.sql(f"DELETE FROM {SRC} WHERE order_id = 103")

# RUN 2 — picks up 102 (update) and 104 (insert). 103 is STILL in the target: deletes are invisible.
incremental_load()
display(spark.table(TGT).orderBy("order_id"))

# COMMAND ----------

# RUN 3 — nothing changed: re-running is safe (idempotent)
incremental_load()

# COMMAND ----------

# Late commit: a transaction that started at 08:30 commits after our 09:00 watermark was saved.
spark.sql(f"INSERT INTO {SRC} VALUES (105, 'C2', 60.00, TIMESTAMP'2026-10-17 08:30:00')")

incremental_load()                         # misses 105: 08:30 is not > 09:00
incremental_load(lookback_minutes=60)      # overlap window catches it; MERGE makes the overlap harmless
display(spark.table(TGT).orderBy("order_id"))

# Expected final target:
# 101 C1 250.00 | 102 C2 120.00 | 103 C3 40.00 (deleted in source, still here!) | 104 C1 75.50 | 105 C2 60.00