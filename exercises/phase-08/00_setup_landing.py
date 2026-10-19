# Databricks notebook source
# Phase 8 — setup: create the schema, a landing volume and sample ShopCo order files.
# Run once (interactively) before the job (8.2/8.3) and the pipeline (8.5–8.7).
# Change CATALOG/SCHEMA to a catalog/schema you can write to. Never run against shared/prod catalogs.

# COMMAND ----------

import json

CATALOG, SCHEMA, VOLUME = "main", "de_prep", "landing"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")

LANDING = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/orders"
print("Landing folder:", LANDING)

# COMMAND ----------

# Day 1 file: 6 rows, of which 3 are dirty (null id, negative amount, unknown status).
day1 = [
    {"order_id": 101, "customer_id": "C1", "region": "UAE", "amount": 250.00, "status": "PLACED",    "order_ts": "2026-10-16T09:15:00"},
    {"order_id": 102, "customer_id": "C2", "region": "KSA", "amount": 100.00, "status": "SHIPPED",   "order_ts": "2026-10-16T10:02:00"},
    {"order_id": None, "customer_id": "C3", "region": "UAE", "amount": 40.00,  "status": "PLACED",    "order_ts": "2026-10-16T11:30:00"},
    {"order_id": 104, "customer_id": "C1", "region": "IN",  "amount": -5.00,  "status": "PLACED",    "order_ts": "2026-10-16T14:45:00"},
    {"order_id": 105, "customer_id": "C4", "region": "KSA", "amount": 60.00,  "status": "DELIVERED", "order_ts": "2026-10-16T15:00:00"},
    {"order_id": 106, "customer_id": "C2", "region": "IN",  "amount": 80.00,  "status": "PLACED",    "order_ts": "2026-10-16T16:20:00"},
]

# Day 2 file: a NEW column (coupon_code) appears -> schema evolution demo in 8.7.
day2 = [
    {"order_id": 107, "customer_id": "C5", "region": "UAE", "amount": 120.00, "status": "PLACED", "order_ts": "2026-10-17T08:00:00", "coupon_code": "DIWALI10"},
    {"order_id": 108, "customer_id": "C1", "region": "KSA", "amount": 30.00,  "status": "PLACED", "order_ts": "2026-10-17T09:30:00", "coupon_code": None},
]

def write_json_lines(rows, path):
    # dbutils.fs.put writes one small text file; fine for a demo (never for real volumes of data).
    dbutils.fs.put(path, "\n".join(json.dumps(r) for r in rows), overwrite=True)

write_json_lines(day1, f"{LANDING}/2026-10-16/orders_0001.json")
display(dbutils.fs.ls(f"{LANDING}/2026-10-16/"))

# COMMAND ----------

# Run this cell LATER (after the first pipeline update) to see schema evolution and incremental processing.
# write_json_lines(day2, f"{LANDING}/2026-10-17/orders_0002.json")