# Databricks notebook source
# Phase 1.9 — rule-driven data quality with quarantine, thresholds, constraints and governance SQL.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG, SCHEMA = "main", "de_prep"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
T = lambda name: f"{CATALOG}.{SCHEMA}.{name}"

orders = spark.createDataFrame([
    (101, "C1", 250.00, "PLACED",    "2026-10-16 09:15:00", "asha@example.com"),
    (102, "C2", None,   "SHIPPED",   "2026-10-16 10:02:00", "ben@example.com"),
    (None, "C3", 40.00, "PLACED",    "2026-10-16 11:30:00", "chen@example.com"),
    (104, "C1", -5.00,  "DELIVERED", "2026-10-16 14:45:00", "asha@example.com"),
    (105, None, 60.00,  "PLACED",    None,                  "not-an-email"),
], "order_id INT, customer_id STRING, amount DOUBLE, status STRING, order_ts STRING, email STRING")

# COMMAND ----------

# Rules as data: name -> SQL condition that must be TRUE for a valid row.
RULES = {
    "order_id_not_null":    "order_id IS NOT NULL",
    "customer_id_not_null": "customer_id IS NOT NULL",
    "amount_positive":      "amount > 0",
    "status_allowed":       "status IN ('PLACED', 'SHIPPED', 'CANCELLED')",
    "order_ts_present":     "order_ts IS NOT NULL",
    "email_format":         "email RLIKE '^[^@]+@[^@]+\\\\.[^@]+$'",
}

def apply_rules(df, rules):
    # coalesce(cond, false): a NULL result (e.g. amount IS NULL -> "amount > 0" is NULL) counts as FAILED.
    reasons = [F.when(~F.coalesce(F.expr(cond), F.lit(False)), F.lit(name)) for name, cond in rules.items()]
    return df.withColumn("_dq_errors", F.filter(F.array(*reasons), lambda x: x.isNotNull()))

checked = apply_rules(orders, RULES)
valid      = checked.filter(F.size("_dq_errors") == 0).drop("_dq_errors")
quarantine = checked.filter(F.size("_dq_errors") > 0).withColumn("_quarantined_at", F.current_timestamp())

display(checked)

# COMMAND ----------

# Threshold: quarantine a few rows, but FAIL if too much of the batch is bad.
total, bad = checked.count(), quarantine.count()
bad_ratio = bad / total if total else 0.0
print(f"{bad}/{total} rows failed ({bad_ratio:.0%})")

MAX_BAD_RATIO = 0.9   # demo value; real pipelines use something like 0.01–0.05
if bad_ratio > MAX_BAD_RATIO:
    raise ValueError(f"DQ threshold exceeded: {bad_ratio:.0%} of rows invalid")

valid.write.mode("overwrite").saveAsTable(T("dq_orders"))
quarantine.write.mode("append").saveAsTable(T("dq_orders_quarantine"))

# Rule-level metrics: how often each rule fails (feed these to monitoring).
display(checked.select(F.explode("_dq_errors").alias("rule")).groupBy("rule").count())

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Table-level guarantees enforced by Delta on every future write:
# MAGIC ALTER TABLE main.de_prep.dq_orders ALTER COLUMN order_id SET NOT NULL;
# MAGIC ALTER TABLE main.de_prep.dq_orders ADD CONSTRAINT amount_positive CHECK (amount > 0);
# MAGIC
# MAGIC -- Metadata: document the table and its columns (shows up in Catalog Explorer and information_schema).
# MAGIC COMMENT ON TABLE main.de_prep.dq_orders IS 'Validated orders. One row per order_id. Owner: data-eng.';
# MAGIC ALTER TABLE main.de_prep.dq_orders ALTER COLUMN email COMMENT 'Customer email - PII';
# MAGIC ALTER TABLE main.de_prep.dq_orders SET TAGS ('domain' = 'sales', 'contains_pii' = 'true');

# COMMAND ----------

# Try to violate the CHECK constraint — Delta rejects the whole write.
try:
    spark.sql("INSERT INTO main.de_prep.dq_orders VALUES (999, 'C1', -1.0, 'PLACED', '2026-10-16 12:00:00', 'x@y.com')")
except Exception as e:
    print("Rejected by constraint:", str(e)[:200])

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Governance (requires appropriate privileges; adjust group names):
# MAGIC -- GRANT SELECT ON TABLE main.de_prep.dq_orders TO `analysts`;
# MAGIC --
# MAGIC -- Column mask: only members of pii_readers see real emails.
# MAGIC -- CREATE OR REPLACE FUNCTION main.de_prep.mask_email(email STRING)
# MAGIC --   RETURN CASE WHEN is_account_group_member('pii_readers') THEN email ELSE '***@***' END;
# MAGIC -- ALTER TABLE main.de_prep.dq_orders ALTER COLUMN email SET MASK main.de_prep.mask_email;