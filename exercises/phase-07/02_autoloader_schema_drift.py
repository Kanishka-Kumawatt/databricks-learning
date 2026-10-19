# Databricks notebook source
# Phase 7.5-7.8 — Auto Loader Bronze ingestion with schema drift.
#
# What this notebook does (all inside ONE Unity Catalog volume folder it owns):
#   Part 1  Bronze ingestion of JSON order files with Auto Loader (cloudFiles):
#           schemaLocation + checkpoint, audit columns from _metadata, _rescued_data.
#           Then a file with a NEW column arrives (stream stops, restart succeeds)
#           and a file with a TYPE change arrives (value lands in _rescued_data).
#   Part 2  The same drift replayed under every cloudFiles.schemaEvolutionMode, side by side.
#   Part 3  Schema inference: strings by default vs cloudFiles.inferColumnTypes vs cloudFiles.schemaHints.
#   Part 4  Malformed (unparseable) records — PERMISSIVE mode and the corrupt-record column (plain Spark).
#
# Requirements: Unity Catalog workspace, permission to create a schema/volume in CATALOG.
# Tested logic targets recent Databricks Runtime / serverless; option semantics verified against
# learn.microsoft.com/azure/databricks/ingestion/cloud-object-storage/auto-loader/schema (Sept 2026).

# COMMAND ----------

import json
from pyspark.sql import functions as F

CATALOG, SCHEMA = "main", "de_prep"          # change to a catalog/schema you can write to
VOLUME = "phase07"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")
T = lambda name: f"{CATALOG}.{SCHEMA}.{name}"

BASE = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/nb02"   # this notebook only touches this folder

# Reset: remove this notebook's files, checkpoints and tables so every run starts clean.
try:
    dbutils.fs.rm(BASE, True)
except Exception:
    pass
for name in ["bronze_orders", "bronze_mode_addNewColumns", "bronze_mode_rescue",
             "bronze_mode_failOnNewColumns", "bronze_mode_none_schema", "bronze_mode_none_rescued"]:
    spark.sql(f"DROP TABLE IF EXISTS {T(name)}")

# COMMAND ----------

# ---------------- Helpers: simulate file arrivals and run one Auto Loader stream ----------------

def land(folder, file_name, rows):
    """Simulate a source system dropping one newline-delimited JSON file into the landing zone."""
    path = f"{BASE}/landing/{folder}/{file_name}"
    body = "\n".join(r if isinstance(r, str) else json.dumps(r) for r in rows)
    dbutils.fs.put(path, body, True)
    print(f"landed {path}")

# The three deliveries used throughout the notebook.
F1_BASE = [
    {"order_id": 101, "customer_id": "C1", "amount": 250.00, "order_ts": "2026-10-16T09:15:00"},
    {"order_id": 102, "customer_id": "C2", "amount": 100.00, "order_ts": "2026-10-16T10:02:00"},
]
F2_NEW_COLUMN = [   # the source team added "coupon" without telling anyone
    {"order_id": 103, "customer_id": "C3", "amount": 40.00, "order_ts": "2026-10-16T11:30:00", "coupon": "WELCOME10"},
]
F3_TYPE_CHANGE = [  # a buggy release sends amount as text
    {"order_id": 104, "customer_id": "C1", "amount": "seventy", "order_ts": "2026-10-16T14:45:00"},
]


def bronze_stream(folder, target, mode="addNewColumns", schema=None, extra=None):
    """One Auto Loader run: process every file not yet in the checkpoint, then stop."""
    src = f"{BASE}/landing/{folder}"
    chk = f"{BASE}/_checkpoints/{target}"            # checkpoint for this query (one per query!)
    reader = (spark.readStream.format("cloudFiles")
              .option("cloudFiles.format", "json")
              .option("cloudFiles.schemaLocation", chk)   # same path as checkpoint: allowed by the docs
              .option("cloudFiles.schemaEvolutionMode", mode))
    for k, v in (extra or {}).items():
        reader = reader.option(k, v)
    if schema is not None:
        reader = reader.schema(schema)
    df = (reader.load(src)
          .select("*",
                  F.col("_metadata.file_path").alias("_source_file"),
                  F.col("_metadata.file_modification_time").alias("_file_modified_at"),
                  F.current_timestamp().alias("_ingested_at")))
    q = (df.writeStream
         .option("checkpointLocation", chk)
         .option("mergeSchema", "true")               # let the Delta sink add evolved columns
         .trigger(availableNow=True)
         .toTable(T(target)))
    q.awaitTermination()                              # raises if the stream failed
    return q


def run(label, **kw):
    """Run bronze_stream and report OK or the failure type instead of crashing the notebook."""
    try:
        bronze_stream(**kw)
        print(f"[{label}] OK")
        return "OK"
    except Exception as e:
        msg = str(e)
        kind = ("UnknownFieldException" if ("UnknownField" in msg or "UNKNOWN_FIELD" in msg)
                else type(e).__name__)
        print(f"[{label}] FAILED: {kind}\n    {msg[:500]}")
        return kind


def show_schema_versions(target):
    """Print the schema history Auto Loader keeps in <schemaLocation>/_schemas/."""
    d = f"{BASE}/_checkpoints/{target}/_schemas"
    files = sorted([f for f in dbutils.fs.ls(d) if f.name.isdigit()], key=lambda f: int(f.name))
    print("schema versions:", [f.name for f in files])
    print(dbutils.fs.head(files[-1].path, 4000))

# COMMAND ----------

# =====================================================================================
# PART 1 — Bronze ingestion, then schema drift (default mode addNewColumns)
# =====================================================================================
# Delivery 1: two well-formed orders.
land("orders", "orders_0001.json", F1_BASE)

OPTS = {
    "cloudFiles.inferColumnTypes": "true",                         # JSON: infer bigint/double instead of all-string
    "cloudFiles.schemaHints": "amount DECIMAL(10,2), order_ts TIMESTAMP",  # override what we know
}
run("run 1", folder="orders", target="bronze_orders", extra=OPTS)
display(spark.table(T("bronze_orders")).orderBy("order_id"))
# Expected: 101 and 102; columns order_id (bigint), customer_id, amount (decimal), order_ts (timestamp),
#           _rescued_data (null), _source_file, _file_modified_at, _ingested_at

# COMMAND ----------

# What is on disk? Checkpoint (progress + file state) and schema location (_schemas) share one folder here.
chk = f"{BASE}/_checkpoints/bronze_orders"
for f in dbutils.fs.ls(chk):
    print(f.name)        # typically: _schemas/  commits/  metadata  offsets/  sources/
show_schema_versions("bronze_orders")   # version 0 = the inferred schema (+ hints)

# Files Auto Loader has recorded in its RocksDB file state (Databricks SQL function, DBR 11.3+):
display(spark.sql(f"SELECT * FROM cloud_files_state('{chk}')"))

# COMMAND ----------

# Delivery 2: a file with a NEW column "coupon".
land("orders", "orders_0002.json", F2_NEW_COLUMN)

r = run("run 2 (new column arrives)", folder="orders", target="bronze_orders", extra=OPTS)
# Expected: FAILED: UnknownFieldException. Before failing, Auto Loader merged "coupon" into the
# schema location (a new _schemas version). Nothing from orders_0002.json was committed.
show_schema_versions("bronze_orders")

# COMMAND ----------

# Restart = what a Lakeflow Job with retries does automatically.
r = run("run 3 (restart)", folder="orders", target="bronze_orders", extra=OPTS)
# Expected: OK. The restarted stream reads the evolved schema and ingests order 103 with coupon.
display(spark.table(T("bronze_orders")).orderBy("order_id"))

# COMMAND ----------

# Delivery 3: a TYPE change — amount arrives as the string "seventy".
land("orders", "orders_0003.json", F3_TYPE_CHANGE)
r = run("run 4 (type change)", folder="orders", target="bronze_orders", extra=OPTS)
# Expected: OK (type changes do NOT stop the stream). amount is NULL for 104 and the original
# value is preserved in _rescued_data as JSON together with the source file path.
display(spark.table(T("bronze_orders"))
        .select("order_id", "amount", "coupon", "_rescued_data",
                F.get_json_object("_rescued_data", "$.amount").alias("rescued_amount"),
                "_source_file")
        .orderBy("order_id"))

# COMMAND ----------

rows = {r.order_id: r for r in spark.table(T("bronze_orders")).collect()}
assert set(rows) == {101, 102, 103, 104}, rows.keys()
assert rows[103].coupon == "WELCOME10"
assert rows[101].coupon is None                      # old rows get NULL for the new column
assert rows[104].amount is None and "seventy" in rows[104]._rescued_data
print("Part 1 checks passed: new column evolved, type mismatch rescued, nothing lost.")

# Re-running with no new files is a no-op (exactly-once per file):
run("run 5 (no new files)", folder="orders", target="bronze_orders", extra=OPTS)
assert spark.table(T("bronze_orders")).count() == 4

# COMMAND ----------

# =====================================================================================
# PART 2 — Same drift under each cloudFiles.schemaEvolutionMode
# =====================================================================================
# Each mode gets its own landing folder, checkpoint, schema location and table.
# Sequence per mode: land base file -> run 1 -> land new-column + type-change files -> run 2 -> run 3 (restart)

EXPLICIT_SCHEMA = "order_id BIGINT, customer_id STRING, amount DOUBLE, order_ts STRING"
INFER = {"cloudFiles.inferColumnTypes": "true"}

def mode_demo(label, mode, schema=None, extra=None):
    target = f"bronze_mode_{label}"
    land(target, "orders_0001.json", F1_BASE)
    r1 = run(f"{label} run1", folder=target, target=target, mode=mode, schema=schema, extra=extra)
    land(target, "orders_0002.json", F2_NEW_COLUMN)
    land(target, "orders_0003.json", F3_TYPE_CHANGE)
    r2 = run(f"{label} run2", folder=target, target=target, mode=mode, schema=schema, extra=extra)
    r3 = run(f"{label} run3 restart", folder=target, target=target, mode=mode, schema=schema, extra=extra)
    tbl = spark.table(T(target))
    rescued = (tbl.filter("_rescued_data IS NOT NULL").count() if "_rescued_data" in tbl.columns else None)
    return (label, mode, r1, r2, r3, "coupon" in tbl.columns, tbl.count(), rescued)

results = [
    mode_demo("addNewColumns", "addNewColumns", extra=INFER),
    mode_demo("rescue", "rescue", extra=INFER),
    mode_demo("failOnNewColumns", "failOnNewColumns", extra=INFER),
    # "none" is the default when YOU provide the schema. Without a rescued column, drift is silently lost.
    mode_demo("none_schema", "none", schema=EXPLICIT_SCHEMA),
    mode_demo("none_rescued", "none", schema=EXPLICIT_SCHEMA, extra={"rescuedDataColumn": "_rescued_data"}),
]
display(spark.createDataFrame(results,
        "label STRING, mode STRING, run1 STRING, run2 STRING, run3 STRING, "
        "has_coupon_column BOOLEAN, row_count LONG, rescued_rows LONG"))

# Expected (verify on your runtime):
# label             run1  run2                   run3                   coupon col  rows  rescued
# addNewColumns     OK    UnknownFieldException  OK                     true        4     1   (104 amount)
# rescue            OK    OK                     OK                     false       4     2   (103 coupon, 104 amount)
# failOnNewColumns  OK    fails                  fails again            false       2     0
# none_schema       OK    OK                     OK                     false       4     null (no rescued column: coupon and "seventy" LOST)
# none_rescued      OK    OK                     OK                     false       4     2

# COMMAND ----------

display(spark.table(T("bronze_mode_rescue")).select("order_id", "amount", "_rescued_data").orderBy("order_id"))
display(spark.table(T("bronze_mode_none_schema")).orderBy("order_id"))   # 104 has amount NULL and no trace why

# COMMAND ----------

# =====================================================================================
# PART 3 — Schema inference: default strings vs inferColumnTypes vs schemaHints
# =====================================================================================
# Defining the streaming DataFrame triggers inference (sample: first 50 GB or 1000 files by default)
# and stores the result in the schema location. Each variant needs its OWN schema location —
# once a schema is stored there, Auto Loader reuses it instead of re-inferring.
land("hints_demo", "orders_0001.json", F1_BASE)

def inferred_schema(variant, extra):
    r = (spark.readStream.format("cloudFiles")
         .option("cloudFiles.format", "json")
         .option("cloudFiles.schemaLocation", f"{BASE}/_schemas_demo/{variant}"))
    for k, v in extra.items():
        r = r.option(k, v)
    return r.load(f"{BASE}/landing/hints_demo").schema.simpleString()

print("default         :", inferred_schema("default", {}))
print("inferColumnTypes:", inferred_schema("typed", {"cloudFiles.inferColumnTypes": "true"}))
print("typed + hints   :", inferred_schema("hints", {"cloudFiles.inferColumnTypes": "true",
                                                      "cloudFiles.schemaHints": "amount DECIMAL(10,2), order_ts TIMESTAMP, channel STRING"}))
# Expected:
# default          -> every column string (JSON/CSV/XML default), plus _rescued_data
# inferColumnTypes -> order_id bigint, amount double, customer_id/order_ts string
# typed + hints    -> amount decimal(10,2), order_ts timestamp, and "channel" added although no file has it yet

# COMMAND ----------

# =====================================================================================
# PART 4 — Malformed records (universal Spark JSON/CSV behaviour)
# =====================================================================================
# A truncated line is not a "type mismatch" — it is a corrupt record. Rescued data does not cover it.
land("malformed", "orders_bad.json", [
    json.dumps({"order_id": 105, "customer_id": "C2", "amount": 60.0, "order_ts": "2026-10-16T15:00:00"}),
    '{"order_id": 106, "customer_id": "C2", "amount": ',          # truncated upload
])
bad = (spark.read
       .schema("order_id BIGINT, customer_id STRING, amount DOUBLE, order_ts STRING, _corrupt_record STRING")
       .option("mode", "PERMISSIVE")                         # default: keep the row, null the fields
       .option("columnNameOfCorruptRecord", "_corrupt_record")
       .json(f"{BASE}/landing/malformed")
       .cache())                                             # Spark requires cache before filtering only on _corrupt_record
display(bad)
display(bad.filter("_corrupt_record IS NOT NULL"))
# Expected: 105 parsed normally; the truncated line has all fields NULL and the raw text in _corrupt_record.
# Alternatives: mode FAILFAST (fail the read) / DROPMALFORMED (silently drop — avoid in Bronze);
# on Databricks, badRecordsPath writes malformed records to files (non-transactional; verify for your runtime).
bad.unpersist()

# COMMAND ----------

# Cleanup (uncomment when finished)
# dbutils.fs.rm(BASE, True)
# for name in ["bronze_orders", "bronze_mode_addNewColumns", "bronze_mode_rescue",
#              "bronze_mode_failOnNewColumns", "bronze_mode_none_schema", "bronze_mode_none_rescued"]:
#     spark.sql(f"DROP TABLE IF EXISTS {T(name)}")