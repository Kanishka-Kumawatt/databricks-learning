# Databricks notebook source
# Phase 3.2 — Catalyst rules in action (constant folding, combine filters, outer-join elimination,
# inferred isnotnull) and optimizer statistics (ANALYZE TABLE, DESCRIBE EXTENDED, EXPLAIN COST).

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG, SCHEMA = "main", "de_prep"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
T = lambda name: f"{CATALOG}.{SCHEMA}.{name}"

(spark.range(200_000)
   .select(F.col("id").alias("order_id"),
           (F.col("id") % 5000 + 1).alias("customer_id"),
           (F.col("id") % 1000).alias("amount"),
           F.when(F.col("id") % 3 == 0, "PAID").otherwise("NEW").alias("status"))
   .write.mode("overwrite").saveAsTable(T("cat_orders")))

(spark.range(1, 5001)
   .select(F.col("id").alias("customer_id"),
           F.element_at(F.array(*[F.lit(c) for c in ["Dubai", "Abu Dhabi", "Sharjah", "Ajman"]]),
                        (F.col("id") % 4 + 1).cast("int")).alias("city"),
           F.when(F.col("id") % 10 == 0, "gold").otherwise("standard").alias("segment"))
   .write.mode("overwrite").saveAsTable(T("cat_customers")))

cat_orders, cat_customers = spark.table(T("cat_orders")), spark.table(T("cat_customers"))

# COMMAND ----------

# 1. Constant folding: look at "== Optimized Logical Plan ==" -> amount > 500
q1 = cat_orders.filter(F.col("amount") > F.lit(100) * 5)
q1.explain(True)

# COMMAND ----------

# 2. Combine filters: two filter() calls -> one Filter node with AND
q2 = cat_orders.filter("amount > 100").filter("status = 'PAID'")
q2.explain(True)

# COMMAND ----------

# 3. Outer-join elimination + filter pushdown into the customers side.
#    WHERE segment = 'gold' rejects NULL-padded rows, so LEFT OUTER behaves like INNER.
q3 = (cat_orders.join(cat_customers, "customer_id", "left")
                .filter(F.col("segment") == "gold"))
q3.explain(True)              # Optimized plan: Join Inner ... Filter (isnotnull(segment) AND (segment = gold))
print("gold orders:", q3.count())   # expected 20000 (500 gold customers x 40 orders each)

# If you really wanted to keep all orders, put the condition in the join, not in WHERE:
q3_keep = cat_orders.join(cat_customers.filter(F.col("segment") == "gold"), "customer_id", "left")
print("all orders kept:", q3_keep.count())   # expected 200000

# COMMAND ----------

# 4. Statistics before ANALYZE
spark.sql(f"DESCRIBE TABLE EXTENDED {T('cat_customers')}").filter("col_name = 'Statistics'").show(truncate=False)
print(spark.sql(f"EXPLAIN COST SELECT * FROM {T('cat_customers')} WHERE segment = 'gold'").collect()[0][0])

# COMMAND ----------

# 5. Collect table + column statistics, then look again
spark.sql(f"ANALYZE TABLE {T('cat_customers')} COMPUTE STATISTICS FOR ALL COLUMNS")
spark.sql(f"ANALYZE TABLE {T('cat_orders')} COMPUTE STATISTICS FOR COLUMNS customer_id, amount")

spark.sql(f"DESCRIBE TABLE EXTENDED {T('cat_customers')}").filter("col_name = 'Statistics'").show(truncate=False)
spark.sql(f"DESCRIBE TABLE EXTENDED {T('cat_customers')} segment").show(truncate=False)   # min/max/distinct/nulls
print(spark.sql(f"EXPLAIN COST SELECT * FROM {T('cat_customers')} WHERE segment = 'gold'").collect()[0][0])
# Expect: Statistics(sizeInBytes=..., rowCount=5.00E+3) on the relation; the Filter estimate uses column stats.

# COMMAND ----------

# 6. CBO-related settings on this runtime (OSS defaults: cbo.enabled=false, joinReorder=false;
#    Databricks documents CBO as enabled by default). Some confs may not be readable on serverless.
for k in ["spark.sql.cbo.enabled", "spark.sql.cbo.joinReorder.enabled"]:
    try:
        print(k, "=", spark.conf.get(k))
    except Exception as e:
        print(k, "-> not available here:", type(e).__name__)