# Databricks notebook source
# Phase 9.1 — snapshot-comparison CDC: derive inserts / updates / deletes by diffing two full snapshots.
# Use when the source has no change log and no reliable updated_at, but can give you full extracts.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG, SCHEMA = "main", "de_prep"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
P = f"{CATALOG}.{SCHEMA}"
DAY1, DAY2, TGT = f"{P}.snap_customer_day1", f"{P}.snap_customer_day2", f"{P}.silver_customer_from_snapshots"

for t in (DAY1, DAY2, TGT):
    spark.sql(f"DROP TABLE IF EXISTS {t}")

spark.sql(f"""CREATE TABLE {DAY1} AS SELECT * FROM VALUES
  ('C1', 'Asha Rao', 'Dubai',     'plus'),
  ('C2', 'Ben Ali',  'Abu Dhabi', 'basic'),
  ('C3', 'Chen Wu',  'Dubai',     'premium')
  AS t(customer_id, name, city, tier)""")

spark.sql(f"""CREATE TABLE {DAY2} AS SELECT * FROM VALUES
  ('C1', 'Asha Rao',    'Abu Dhabi', 'plus'),
  ('C2', 'Ben Ali',     'Abu Dhabi', 'basic'),
  ('C4', 'Dana Okafor', 'Sharjah',   'plus')
  AS t(customer_id, name, city, tier)""")

# COMMAND ----------

# Full outer join on the key + hash of the non-key columns.
diff = spark.sql(f"""
WITH o AS (
  SELECT customer_id, sha2(concat_ws('||', coalesce(name,'<null>'), coalesce(city,'<null>'), coalesce(tier,'<null>')), 256) AS h
  FROM {DAY1}),
n AS (
  SELECT customer_id, sha2(concat_ws('||', coalesce(name,'<null>'), coalesce(city,'<null>'), coalesce(tier,'<null>')), 256) AS h
  FROM {DAY2})
SELECT COALESCE(n.customer_id, o.customer_id) AS customer_id,
       CASE WHEN o.customer_id IS NULL THEN 'I'
            WHEN n.customer_id IS NULL THEN 'D'
            ELSE 'U' END AS op
FROM o FULL OUTER JOIN n ON o.customer_id = n.customer_id
WHERE o.customer_id IS NULL OR n.customer_id IS NULL OR o.h <> n.h
ORDER BY customer_id""")
display(diff)
# Expected: C1 U (city changed) | C3 D (missing from day 2) | C4 I (new). C2 unchanged -> not emitted.

# COMMAND ----------

# Applying a full snapshot to an SCD1 target in ONE MERGE (no diff table needed):
spark.sql(f"CREATE TABLE {TGT} AS SELECT * FROM {DAY1}")
spark.sql(f"""
MERGE INTO {TGT} AS t
USING {DAY2} AS s
ON t.customer_id = s.customer_id
WHEN MATCHED AND (t.name <=> s.name AND t.city <=> s.city AND t.tier <=> s.tier) = false THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE
""")
display(spark.table(TGT).orderBy("customer_id"))
display(spark.sql(f"DESCRIBE HISTORY {TGT} LIMIT 1").select("operation", "operationMetrics"))
# Expected target: C1 Abu Dhabi plus | C2 Abu Dhabi basic | C4 Sharjah plus
# Metrics: 1 updated (C1), 1 inserted (C4), 1 deleted (C3). C2 matched but unchanged -> not rewritten.
# <=> is NULL-safe equality, so NULL = NULL counts as "unchanged".
# WHEN NOT MATCHED BY SOURCE without a condition is only correct because DAY2 is a COMPLETE snapshot.