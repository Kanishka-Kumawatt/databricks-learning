# Databricks notebook source
# Phase 1.8 — Medallion architecture end to end (batch, small data).
# Bronze: raw strings + audit columns  ->  Silver: typed, deduplicated, validated (+ quarantine)
# ->  Gold: business aggregate joined to customers.

# COMMAND ----------

from pyspark.sql import functions as F, Window

CATALOG, SCHEMA = "main", "de_prep"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
T = lambda name: f"{CATALOG}.{SCHEMA}.{name}"

for name in ("bronze_orders", "bronze_customers", "silver_orders", "silver_orders_quarantine",
             "silver_customers", "gold_daily_revenue_by_city"):
    spark.sql(f"DROP TABLE IF EXISTS {T(name)}")

# COMMAND ----------

# ---------------- BRONZE: land raw data as-is (all strings) + audit columns ----------------
raw_orders = spark.createDataFrame([
    ("101", "C1", "250.00", "2026-10-16 09:15:00", "orders_0001.csv"),
    ("102", "C2", "100.00", "2026-10-16 10:02:00", "orders_0001.csv"),
    ("102", "C2", "100.00", "2026-10-16 10:02:00", "orders_0002.csv"),   # partner resent the order
    ("103", "C3", "-40.00", "2026-10-16 11:30:00", "orders_0002.csv"),   # invalid amount
    ("104", "C9", "75.50",  "2026-10-16 14:45:00", "orders_0002.csv"),   # customer not (yet) known
    ("105", "C2", "abc",    "2026-10-16 15:00:00", "orders_0002.csv"),   # unparseable amount
], "order_id STRING, customer_id STRING, amount STRING, order_ts STRING, _source_file STRING")

raw_customers = spark.createDataFrame([
    ("C1", " Asha Rao ", "dubai",     "GOLD",     "customers_0001.csv"),
    ("C2", "Ben Ali",    "Abu Dhabi", "standard", "customers_0001.csv"),
    ("C3", "Chen Wu",    "Dubai",     "Standard", "customers_0001.csv"),
], "customer_id STRING, name STRING, city STRING, segment STRING, _source_file STRING")

(raw_orders.withColumn("_ingested_at", F.current_timestamp())
    .write.mode("append").saveAsTable(T("bronze_orders")))
(raw_customers.withColumn("_ingested_at", F.current_timestamp())
    .write.mode("append").saveAsTable(T("bronze_customers")))

# COMMAND ----------

# ---------------- SILVER: typed, cleaned, deduplicated, validated ----------------
typed = (spark.table(T("bronze_orders"))
         .select(F.col("order_id").cast("int").alias("order_id"),
                 F.trim("customer_id").alias("customer_id"),
                 F.expr("try_cast(amount AS DECIMAL(10,2))").alias("amount"),
                 F.expr("try_to_timestamp(order_ts)").alias("order_ts"),
                 "_source_file", "_ingested_at"))

# One row per order: keep the latest ingested copy.
w = Window.partitionBy("order_id").orderBy(F.col("_ingested_at").desc(), F.col("_source_file").desc())
deduped = typed.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

# Validation: collect the reasons a row is invalid.
checked = deduped.withColumn("_dq_errors", F.filter(F.array(
    F.when(F.col("order_id").isNull(), F.lit("order_id_missing")),
    F.when(F.col("amount").isNull(), F.lit("amount_invalid")),
    F.when(F.col("amount") <= 0, F.lit("amount_not_positive")),
    F.when(F.col("order_ts").isNull(), F.lit("order_ts_invalid")),
), lambda x: x.isNotNull()))

(checked.filter(F.size("_dq_errors") == 0).drop("_dq_errors")
    .write.mode("overwrite").saveAsTable(T("silver_orders")))
(checked.filter(F.size("_dq_errors") > 0).withColumn("_quarantined_at", F.current_timestamp())
    .write.mode("overwrite").saveAsTable(T("silver_orders_quarantine")))

(spark.table(T("bronze_customers"))
    .select(F.trim("customer_id").alias("customer_id"),
            F.trim("name").alias("name"),
            F.initcap(F.trim("city")).alias("city"),
            F.lower(F.trim("segment")).alias("segment"))
    .dropDuplicates(["customer_id"])
    .write.mode("overwrite").saveAsTable(T("silver_customers")))

# COMMAND ----------

# ---------------- GOLD: business-level aggregate ----------------
gold = (spark.table(T("silver_orders")).alias("o")
        .join(spark.table(T("silver_customers")).alias("c"), "customer_id", "left")   # keep orders of unknown customers
        .groupBy(F.to_date("o.order_ts").alias("order_date"),
                 F.coalesce("c.city", F.lit("UNKNOWN")).alias("city"),
                 F.coalesce("c.segment", F.lit("UNKNOWN")).alias("segment"))
        .agg(F.sum("o.amount").alias("revenue"),
             F.countDistinct("o.order_id").alias("orders")))

gold.write.mode("overwrite").saveAsTable(T("gold_daily_revenue_by_city"))

# COMMAND ----------

display(spark.table(T("silver_orders")).orderBy("order_id"))
display(spark.table(T("silver_orders_quarantine")).orderBy("order_id"))
display(spark.table(T("gold_daily_revenue_by_city")).orderBy("city"))

# Reconciliation: every distinct order in Bronze is either in Silver or in quarantine.
bronze_n = spark.table(T("bronze_orders")).select("order_id").distinct().count()
silver_n = spark.table(T("silver_orders")).count()
quar_n   = spark.table(T("silver_orders_quarantine")).count()
assert bronze_n == silver_n + quar_n, f"Reconciliation failed: {bronze_n} != {silver_n} + {quar_n}"
print(f"Reconciled: {bronze_n} = {silver_n} valid + {quar_n} quarantined")