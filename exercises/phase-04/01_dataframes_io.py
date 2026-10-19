# Databricks notebook source
# Phase 4.1 + 4.2 — DataFrames, schemas, types; reading and writing CSV / JSON / Parquet / Delta.
# Prerequisite: run 00_setup_datasets.py once. Compare every result with the expected tables in the lessons.

# COMMAND ----------

from decimal import Decimal
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, BooleanType

CATALOG, SCHEMA, VOLUME = "main", "de_prep", "raw"
T = lambda name: f"{CATALOG}.{SCHEMA}.{name}"
VOL = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
spark.conf.set("spark.sql.session.timeZone", "UTC")
print("ANSI mode:", spark.conf.get("spark.sql.ansi.enabled"))

# COMMAND ----------

# 4.1 Example 1 — DDL string vs StructType
rows = [("P1", "Wireless Mouse", "Electronics", Decimal("25.00"), True),
        ("P6", "Fountain Pen",   "Stationery",  Decimal("30.00"), False)]
ddl = "product_id STRING, product_name STRING, category STRING, unit_price DECIMAL(10,2), is_active BOOLEAN"
df_ddl = spark.createDataFrame(rows, ddl)

schema = StructType([
    StructField("product_id",   StringType(), nullable=False),
    StructField("product_name", StringType()),
    StructField("category",     StringType()),
    StructField("unit_price",   DecimalType(10, 2)),
    StructField("is_active",    BooleanType()),
])
df_struct = spark.createDataFrame(rows, schema)
df_struct.printSchema()
print(df_ddl.schema.simpleString())

# COMMAND ----------

# 4.1 Example 2 — double vs decimal
spark.sql("""
  SELECT 0.1D + 0.2D                                             AS as_double,
         CAST(0.1 AS DECIMAL(10,2)) + CAST(0.2 AS DECIMAL(10,2)) AS as_decimal,
         (0.1D + 0.2D) = 0.3D                                    AS double_equal
""").show(truncate=False)

# COMMAND ----------

# 4.1 Example 3 — TIMESTAMP vs TIMESTAMP_NTZ
spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.sql(f"""CREATE OR REPLACE TABLE {T('tz_demo')} AS
              SELECT TIMESTAMP'2026-03-01 10:00:00'     AS ts_ltz,
                     TIMESTAMP_NTZ'2026-03-01 10:00:00' AS ts_ntz""")
spark.conf.set("spark.sql.session.timeZone", "Asia/Dubai")
spark.table(T("tz_demo")).show(truncate=False)      # expect 14:00:00 | 10:00:00
spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.table(T("tz_demo")).show(truncate=False)      # expect 10:00:00 | 10:00:00

# COMMAND ----------

# 4.1 Example 4 — ANSI casts
spark.sql("SELECT try_cast('abc' AS INT) AS t1, try_cast('42' AS INT) AS t2, try_divide(10, 0) AS t3").show()
try:
    spark.sql("SELECT CAST('abc' AS INT) AS x").collect()
    print("No error: ANSI mode is off in this session")
except Exception as e:                        # with ANSI on: CAST_INVALID_INPUT
    print("Error as expected:", str(e).splitlines()[0][:150])

# COMMAND ----------

# 4.2 Example 1 — parse modes on a messy CSV
PATH = f"{VOL}/customers_csv/customers_messy.csv"
print(dbutils.fs.head(PATH))

SCHEMA_CORRUPT = "customer_id STRING, name STRING, signup_date DATE, credit_limit INT, _corrupt_record STRING"
SCHEMA_PLAIN   = "customer_id STRING, name STRING, signup_date DATE, credit_limit INT"

permissive = (spark.read.option("header", True).option("mode", "PERMISSIVE")
              .option("columnNameOfCorruptRecord", "_corrupt_record")
              .schema(SCHEMA_CORRUPT).csv(PATH).cache())
display(permissive)                                              # 5 rows; C2 and C4 have _corrupt_record
display(permissive.filter("_corrupt_record IS NOT NULL"))        # allowed because the DataFrame is cached

dropped = spark.read.option("header", True).option("mode", "DROPMALFORMED").schema(SCHEMA_PLAIN).csv(PATH)
display(dropped)                                                 # C1, C3, C5

try:
    spark.read.option("header", True).option("mode", "FAILFAST").schema(SCHEMA_PLAIN).csv(PATH).count()
except Exception as e:
    print("FAILFAST raised:", str(e).splitlines()[0][:150])

# COMMAND ----------

# 4.2 Example 2 — inferSchema: one bad value turns a column into string (and costs a job)
spark.read.option("header", True).option("inferSchema", True).csv(PATH).printSchema()

# COMMAND ----------

# 4.2 Example 3 — JSON lines vs multi-line JSON
lines = spark.read.json(f"{VOL}/events_json/events.jsonl")
print(lines.columns)
lines.printSchema()
display(lines)

bad = spark.read.json(f"{VOL}/events_multiline/events.json")
print("without multiLine:", bad.columns)
good = spark.read.option("multiLine", True).json(f"{VOL}/events_multiline/events.json")
display(good)

# COMMAND ----------

# 4.2 Example 4 — save modes and partitionBy
OUT = f"{VOL}/out/orders_by_channel"
orders = spark.table(T("orders"))
orders.write.mode("overwrite").partitionBy("channel").parquet(OUT)
display(dbutils.fs.ls(OUT))

back = spark.read.parquet(OUT)
back.printSchema()                                   # channel is last: restored from folder names
print("app orders:", back.filter("channel = 'app'").count())   # 4

try:
    orders.write.mode("errorifexists").parquet(OUT)
except Exception as e:
    print("errorifexists raised:", str(e).splitlines()[0][:150])

orders.write.format("delta").mode("overwrite").saveAsTable(T("orders_copy"))
display(spark.sql(f"DESCRIBE HISTORY {T('orders_copy')}").select("version", "operation"))

# COMMAND ----------

# Try it yourself:
# 1. Re-read the CSV with .option("nullValue", "abc") — what happens to C2?
# 2. Write orders as Delta with .option("replaceWhere", "channel = 'app'") after filtering to app orders only.
#    Check that web orders are untouched.
# 3. Read events.jsonl with the explicit EVENTS_SCHEMA from the setup notebook. Compare the schema with `lines`.