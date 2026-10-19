# Databricks notebook source
# Phase 3.1 / 3.2 — Reading physical plans: filter pushdown, column pruning, partition pruning.
# Writes 1,000,000 synthetic ShopCo orders as Parquet files (UC volume) and as a partitioned Delta table,
# then prints plans. Works on classic and serverless compute (explain() is supported on both).

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG, SCHEMA = "main", "de_prep"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.phase03")
T = lambda name: f"{CATALOG}.{SCHEMA}.{name}"
PARQUET_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/phase03/orders_parquet"

# COMMAND ----------

# ---------------- Synthetic data (deterministic) ----------------
N = 1_000_000
orders = (spark.range(N)
          .select(F.col("id").alias("order_id"),
                  (F.col("id") % 50000 + 1).alias("customer_id"),
                  ((F.col("id") * 73) % 100000 / 100).cast("decimal(10,2)").alias("amount"),
                  F.date_add(F.lit("2026-10-01").cast("date"), (F.col("id") % 30).cast("int")).alias("order_date"),
                  F.element_at(F.array(*[F.lit(s) for s in ["NEW", "PAID", "SHIPPED", "CANCELLED"]]),
                               (F.col("id") % 4 + 1).cast("int")).alias("status")))

orders.write.mode("overwrite").parquet(PARQUET_PATH)
spark.sql(f"DROP TABLE IF EXISTS {T('orders_delta')}")
orders.write.mode("overwrite").partitionBy("order_date").saveAsTable(T("orders_delta"))

display(spark.table(T("orders_delta")).orderBy("order_id").limit(5))

# COMMAND ----------

# ---------------- 1. Parquet: filter + select -> pushdown + pruning ----------------
q = (spark.read.parquet(PARQUET_PATH)
       .filter(F.col("amount") > 500)
       .select("order_id", "customer_id"))

# Extended: compare "Analyzed Logical Plan" (your order) with "Optimized Logical Plan".
q.explain(True)

# COMMAND ----------

# Formatted: find the Scan node and read PushedFilters / ReadSchema.
# Expect (abbreviated): PushedFilters: [IsNotNull(amount), GreaterThan(amount,500.00)]
#                       ReadSchema: struct<order_id:bigint,customer_id:bigint,amount:decimal(10,2)>
q.explain("formatted")
print("rows with amount > 500:", q.count())   # expected 499990

# COMMAND ----------

# ---------------- 2. select("*") -> no column pruning ----------------
q_all = spark.read.parquet(PARQUET_PATH).filter(F.col("amount") > 500)
q_all.explain()   # ReadSchema now lists all 5 columns

# COMMAND ----------

# ---------------- 3. Delta table partitioned by order_date ----------------
d = (spark.table(T("orders_delta"))
       .filter((F.col("order_date") == "2026-10-16") & (F.col("status") == "PAID"))
       .select("order_id", "amount"))

# Expect: PartitionFilters with order_date = 2026-10-16 ; PushedFilters with EqualTo(status,PAID)
d.explain("formatted")
print("PAID orders on 2026-10-16:", d.count())   # expected 16666

# COMMAND ----------

# ---------------- 4. Filter on a derived expression -> partition pruning lost ----------------
d_bad = (spark.table(T("orders_delta"))
           .filter(F.date_format("order_date", "yyyyMMdd") == "20261016")
           .select("order_id", "amount"))
# Look at PartitionFilters: the predicate is on date_format(order_date), not order_date itself.
# Depending on the runtime it may still be evaluated against partition values, but it can no longer
# be used for data-file statistics; always prefer a plain predicate on the column.
d_bad.explain("formatted")

# COMMAND ----------

# ---------------- 5. Python UDF in the filter -> NOT pushed down ----------------
@F.udf("boolean")
def is_big(a):
    return a is not None and a > 500

q_udf = spark.read.parquet(PARQUET_PATH).filter(is_big("amount")).select("order_id", "customer_id")
# Expect: an ArrowEvalPython / BatchEvalPython node, and no GreaterThan(amount, ...) in PushedFilters.
q_udf.explain()

# COMMAND ----------

# ---------------- 6. SQL EXPLAIN works the same way ----------------
spark.read.parquet(PARQUET_PATH).createOrReplaceTempView("orders_pq")
plan_text = spark.sql("""
  EXPLAIN FORMATTED
  SELECT customer_id, SUM(amount) AS revenue
  FROM orders_pq
  WHERE status = 'PAID'
  GROUP BY customer_id
""").collect()[0][0]
print(plan_text)
# Read it bottom-up: Scan (PushedFilters EqualTo(status,PAID)) -> HashAggregate(partial_sum)
# -> Exchange hashpartitioning(customer_id, ...) -> HashAggregate(sum). One Exchange = one shuffle.