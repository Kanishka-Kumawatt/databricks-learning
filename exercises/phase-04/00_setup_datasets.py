# Databricks notebook source
# Phase 4 — ShopCo practice datasets.
# Run this ONCE before any Phase 4 notebook, lesson example or drill. Re-running is safe (it replaces everything).
#
# Creates in CATALOG.SCHEMA:
#   products, customers, orders, order_items, transactions, cdc_customers, events_raw, events
# and raw files in the volume CATALOG.SCHEMA.VOLUME:
#   customers_csv/customers_messy.csv, events_json/events.jsonl, events_multiline/events.json
#
# The data is deliberately small (5-15 rows per table) so every expected output in the lessons can be checked by eye.
# Deliberate problems (do not "fix" them here — the lessons use them):
#   customers     : C2 twice (old + newer version), C3 exact duplicate, one NULL customer_id, NULL email/city/segment,
#                   inconsistent casing ("abu dhabi", "DUBAI", "GOLD"), padded name " Eve Khan ", invalid email "eve@shopco"
#   orders        : O1009 has unknown customer C9, O1010 has NULL customer_id, O1002 is dated before C2's signup_date
#   transactions  : a FAILED payment followed by a retry, a full REFUND, a partial REFUND, a FAILED-only order
#   cdc_customers : out-of-order events (seq 6 arrives before seq 5), an exact duplicate event, a DELETE
#   events        : nested JSON; one event with an empty items array, one with no items key, one malformed line

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG, SCHEMA, VOLUME = "main", "de_prep", "raw"     # change to a catalog/schema you are allowed to write to
spark.conf.set("spark.sql.session.timeZone", "UTC")     # all timestamps in this phase are UTC

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")
T = lambda name: f"{CATALOG}.{SCHEMA}.{name}"
VOL = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

def load(name, cols, rows):
    """Create a Delta table from string rows, casting each column to its declared type.
    cols: list of (column_name, spark_sql_type). rows: tuples of str/None in the same order."""
    raw = spark.createDataFrame(rows, ", ".join(f"{c} STRING" for c, _ in cols))
    typed = raw.select([F.col(c).cast(t).alias(c) for c, t in cols])
    typed.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(T(name))
    print(f"{T(name)}: {typed.count()} rows")

# COMMAND ----------

load("products",
     [("product_id", "STRING"), ("product_name", "STRING"), ("category", "STRING"),
      ("unit_price", "DECIMAL(10,2)"), ("is_active", "BOOLEAN")],
     [("P1", "Wireless Mouse", "Electronics", "25.00", "true"),
      ("P2", "USB-C Cable",    "Electronics", "10.00", "true"),
      ("P3", "Coffee Mug",     "Home",        "12.50", "true"),
      ("P4", "Desk Lamp",      "Home",        "40.00", "true"),
      ("P5", "Notebook",       "Stationery",  "5.00",  "true"),
      ("P6", "Fountain Pen",   "Stationery",  "30.00", "false")])   # never ordered

load("customers",
     [("customer_id", "STRING"), ("name", "STRING"), ("email", "STRING"), ("city", "STRING"),
      ("segment", "STRING"), ("signup_date", "DATE"), ("updated_at", "TIMESTAMP")],
     [("C1", "Asha Rao",   "asha@shopco.com",  "Dubai",     "gold",     "2025-01-10", "2026-01-05 10:00:00"),
      ("C2", "Ben Ali",    "BEN@SHOPCO.COM",   "abu dhabi", "Standard", "2025-03-15", "2026-02-01 09:00:00"),
      ("C3", "Chen Wu",    None,               "DUBAI",     "standard", "2025-06-20", "2026-01-20 12:00:00"),
      ("C4", "Dana Lee",   "dana@shopco.com",  None,        "GOLD",     "2025-08-01", "2026-03-01 08:00:00"),
      ("C5", " Eve Khan ", "eve@shopco",       "Sharjah",   None,       "2025-11-11", "2026-01-15 14:00:00"),
      ("C6", "Farid Noor", "farid@shopco.com", "Dubai",     "standard", "2026-01-02", "2026-01-02 11:00:00"),
      ("C2", "Ben Ali",    "ben@shopco.com",   "Abu Dhabi", "gold",     "2025-03-15", "2026-03-10 16:00:00"),  # newer C2
      ("C3", "Chen Wu",    None,               "DUBAI",     "standard", "2025-06-20", "2026-01-20 12:00:00"),  # exact dup
      (None, "Ghost User", "ghost@shopco.com", "Dubai",     "standard", "2026-02-02", "2026-02-02 10:00:00")])

load("orders",
     [("order_id", "STRING"), ("customer_id", "STRING"), ("order_ts", "TIMESTAMP"), ("status", "STRING"),
      ("channel", "STRING"), ("order_amount", "DECIMAL(10,2)")],
     [("O1001", "C1", "2025-01-12 10:15:00", "DELIVERED", "web", "45.00"),
      ("O1002", "C2", "2025-02-20 18:30:00", "DELIVERED", "app", "25.00"),
      ("O1003", "C1", "2025-03-24 09:05:00", "DELIVERED", "app", "45.00"),
      ("O1004", "C3", "2025-11-03 14:00:00", "CANCELLED", "web", "25.00"),
      ("O1005", "C4", "2025-12-15 20:45:00", "DELIVERED", "web", "80.00"),
      ("O1006", "C2", "2026-01-02 08:10:00", "RETURNED",  "app", "30.00"),
      ("O1007", "C5", "2026-02-14 12:00:00", "DELIVERED", "web", "27.50"),
      ("O1008", "C1", "2026-03-01 16:20:00", "SHIPPED",   "web", "62.50"),
      ("O1009", "C9", "2026-03-05 11:11:00", "DELIVERED", "app", "20.00"),    # unknown customer
      ("O1010", None, "2026-03-08 13:00:00", "PENDING",   "web", "10.00")])   # NULL customer

load("order_items",
     [("order_id", "STRING"), ("line_no", "INT"), ("product_id", "STRING"), ("quantity", "INT"),
      ("unit_price", "DECIMAL(10,2)")],
     [("O1001", "1", "P1", "1", "25.00"), ("O1001", "2", "P2", "2", "10.00"),
      ("O1002", "1", "P3", "2", "12.50"),
      ("O1003", "1", "P4", "1", "40.00"), ("O1003", "2", "P5", "1", "5.00"),
      ("O1004", "1", "P1", "1", "25.00"),
      ("O1005", "1", "P4", "2", "40.00"),
      ("O1006", "1", "P2", "3", "10.00"),
      ("O1007", "1", "P3", "1", "12.50"), ("O1007", "2", "P5", "3", "5.00"),
      ("O1008", "1", "P1", "2", "25.00"), ("O1008", "2", "P3", "1", "12.50"),
      ("O1009", "1", "P5", "4", "5.00"),
      ("O1010", "1", "P2", "1", "10.00")])

load("transactions",
     [("txn_id", "STRING"), ("order_id", "STRING"), ("txn_ts", "TIMESTAMP"), ("txn_type", "STRING"),
      ("status", "STRING"), ("amount", "DECIMAL(10,2)"), ("method", "STRING")],
     [("T01", "O1001", "2025-01-12 10:16:00", "PAYMENT", "SUCCESS", "45.00", "card"),
      ("T02", "O1002", "2025-02-20 18:31:00", "PAYMENT", "FAILED",  "25.00", "card"),
      ("T03", "O1002", "2025-02-20 18:35:00", "PAYMENT", "SUCCESS", "25.00", "wallet"),  # retry succeeded
      ("T04", "O1003", "2025-03-24 09:06:00", "PAYMENT", "SUCCESS", "45.00", "card"),
      ("T05", "O1005", "2025-12-15 20:46:00", "PAYMENT", "SUCCESS", "80.00", "card"),
      ("T06", "O1006", "2026-01-02 08:11:00", "PAYMENT", "SUCCESS", "30.00", "wallet"),
      ("T07", "O1006", "2026-01-10 10:00:00", "REFUND",  "SUCCESS", "30.00", "wallet"),  # full refund
      ("T08", "O1007", "2026-02-14 12:01:00", "PAYMENT", "SUCCESS", "27.50", "card"),
      ("T09", "O1007", "2026-02-20 09:00:00", "REFUND",  "SUCCESS", "12.50", "card"),    # partial refund
      ("T10", "O1008", "2026-03-01 16:21:00", "PAYMENT", "SUCCESS", "62.50", "card"),
      ("T11", "O1009", "2026-03-05 11:12:00", "PAYMENT", "SUCCESS", "20.00", "card"),
      ("T12", "O1010", "2026-03-08 13:01:00", "PAYMENT", "FAILED",  "10.00", "card")])   # never paid

load("cdc_customers",
     [("customer_id", "STRING"), ("op", "STRING"), ("seq", "BIGINT"), ("event_ts", "TIMESTAMP"),
      ("name", "STRING"), ("email", "STRING"), ("city", "STRING"), ("segment", "STRING")],
     [("C1", "INSERT", "1", "2026-03-01 09:00:00", "Asha Rao", "asha@shopco.com",     "Dubai",     "gold"),
      ("C2", "INSERT", "2", "2026-03-01 09:05:00", "Ben Ali",  "ben@shopco.com",      "Abu Dhabi", "standard"),
      ("C1", "UPDATE", "3", "2026-03-01 10:00:00", "Asha Rao", "asha.rao@shopco.com", "Dubai",     "gold"),
      ("C3", "INSERT", "4", "2026-03-01 10:30:00", "Chen Wu",  "chen@shopco.com",     "Dubai",     "standard"),
      ("C2", "UPDATE", "6", "2026-03-01 11:30:00", "Ben Ali",  "ben@shopco.com",      "Abu Dhabi", "gold"),      # arrives early
      ("C2", "UPDATE", "5", "2026-03-01 11:00:00", "Ben Ali",  "ben@shopco.com",      "Sharjah",   "standard"),  # late, older
      ("C3", "DELETE", "7", "2026-03-01 12:00:00", None,       None,                  None,        None),
      ("C1", "UPDATE", "3", "2026-03-01 10:00:00", "Asha Rao", "asha.rao@shopco.com", "Dubai",     "gold"),      # duplicate
      ("C4", "INSERT", "8", "2026-03-01 12:30:00", "Dana Lee", "dana@shopco.com",     "Sharjah",   "gold")])

# Target table for the MERGE examples (lesson 4.12, drill 21): the state BEFORE the CDC batch is applied.
load("dim_customer",
     [("customer_id", "STRING"), ("name", "STRING"), ("email", "STRING"), ("city", "STRING"),
      ("segment", "STRING"), ("last_seq", "BIGINT")],
     [("C1", "Asha Rao", "asha@shopco.com", "Dubai",     "gold",     "1"),
      ("C2", "Ben Ali",  "ben@shopco.com",  "Abu Dhabi", "standard", "2"),
      ("C3", "Chen Wu",  "chen@shopco.com", "Dubai",     "standard", "4"),
      ("C5", "Eve Khan", "eve@shopco.com",  "Sharjah",   "standard", "0")])

# COMMAND ----------

# ---------------- Nested JSON events ----------------
EVENT_LINES = [
    '{"event_id":"E1","event_ts":"2026-03-01T10:00:00Z","event_type":"view","customer":{"id":"C1","device":{"os":"ios","app_version":"5.1"}},"items":[{"product_id":"P1","qty":1}],"props":{"page":"home"}}',
    '{"event_id":"E2","event_ts":"2026-03-01T10:02:00Z","event_type":"add_to_cart","customer":{"id":"C1","device":{"os":"ios","app_version":"5.1"}},"items":[{"product_id":"P1","qty":1},{"product_id":"P2","qty":2}],"props":{"page":"product","campaign":"spring"}}',
    '{"event_id":"E3","event_ts":"2026-03-01T10:05:00Z","event_type":"purchase","customer":{"id":"C1","device":{"os":"ios","app_version":"5.1"}},"items":[{"product_id":"P1","qty":1},{"product_id":"P2","qty":2}],"props":{"page":"checkout"}}',
    '{"event_id":"E4","event_ts":"2026-03-01T11:00:00Z","event_type":"view","customer":{"id":"C2","device":{"os":"android","app_version":"4.9"}},"items":[],"props":{"page":"home"}}',
    '{"event_id":"E5","event_ts":"2026-03-01T11:45:00Z","event_type":"view","customer":{"id":"C2","device":{"os":"android","app_version":"4.9"}},"props":{"page":"search","campaign":"spring"}}',
    '{"event_id":"E6","event_ts":"2026-03-01T13:00:00Z","event_type":"view","customer":{"id":"C1","device":{"os":"web"}},"items":[{"product_id":"P4","qty":1}],"props":{"page":"home"}}',
    '{"event_id":"E7","event_ts":"2026-03-01T13:05:00Z","event_type":"view","customer":{"id":"C3"',   # malformed (truncated)
]
EVENTS_SCHEMA = ("event_id STRING, event_ts TIMESTAMP, event_type STRING, "
                 "customer STRUCT<id: STRING, device: STRUCT<os: STRING, app_version: STRING>>, "
                 "items ARRAY<STRUCT<product_id: STRING, qty: INT>>, props MAP<STRING, STRING>")

# 1) raw JSON lines in the volume (for spark.read.json demos)
dbutils.fs.put(f"{VOL}/events_json/events.jsonl", "\n".join(EVENT_LINES) + "\n", overwrite=True)

# 2) the same lines as a string column (Bronze style: value STRING) for from_json demos
(spark.createDataFrame([(l,) for l in EVENT_LINES], "value STRING")
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(T("events_raw")))

# 3) parsed, valid events only (Silver style), nested types preserved.
#    A corrupt-record column flags the malformed line reliably (its other fields may be NULL or partially filled).
(spark.table(T("events_raw"))
    .select(F.from_json("value", EVENTS_SCHEMA + ", _corrupt_record STRING",
                        {"columnNameOfCorruptRecord": "_corrupt_record"}).alias("e"))
    .filter(F.col("e._corrupt_record").isNull())
    .select("e.*").drop("_corrupt_record")
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(T("events")))
n_events = spark.table(T('events')).count()
print(f"{T('events')}: {n_events} rows")
assert n_events == 6, "expected 6 valid events (E1-E6); E7 is malformed"

# COMMAND ----------

# ---------------- Messy raw files for the read/write lesson (4.2) ----------------
dbutils.fs.put(f"{VOL}/customers_csv/customers_messy.csv", """customer_id,name,signup_date,credit_limit
C1,Asha Rao,2025-01-10,5000
C2,Ben Ali,2025-03-15,abc
C3,"Wu, Chen",2025-06-20,3000
C4,Dana Lee,not-a-date,2500
C5,Eve Khan,2025-11-11
""", overwrite=True)

dbutils.fs.put(f"{VOL}/events_multiline/events.json", """[
  {
    "event_id": "M1",
    "event_type": "view",
    "customer": {"id": "C1"}
  },
  {
    "event_id": "M2",
    "event_type": "purchase",
    "customer": {"id": "C2"}
  }
]
""", overwrite=True)

# COMMAND ----------

for t in ["products", "customers", "orders", "order_items", "transactions", "cdc_customers", "dim_customer", "events"]:
    print(t)
    display(spark.table(T(t)))