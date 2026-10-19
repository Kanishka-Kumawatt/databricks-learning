# Databricks notebook source
# Phase 6 — KEY PRACTICAL: trace the Delta transaction log through
#   CREATE → INSERT → INSERT → UPDATE → DELETE → MERGE (+checkpoint) → schema change → OPTIMIZE → time travel → VACUUM
# After every step we print: DESCRIBE HISTORY (latest row), DESCRIBE DETAIL (numFiles), the actions inside the newest
# _delta_log/<version>.json commit file, and which physical file each row lives in.
#
# Run it TWICE: once with USE_DELETION_VECTORS = True and once with False, and compare the UPDATE / DELETE / MERGE
# commits (lesson 6.4 explains both behaviours).
#
# Storage: a path-based Delta table in a Unity Catalog volume (same pattern as lesson 1.3). If your workspace does not
# allow Delta tables at volume paths, set BASE to an external-location path you can write to (abfss://...), or run on
# local PySpark with delta-spark and a local folder (replace dbutils calls with os/glob).

# COMMAND ----------

import json
from pyspark.sql import functions as F
from delta.tables import DeltaTable

CATALOG, SCHEMA, VOLUME = "main", "de_prep", "files"
USE_DELETION_VECTORS = True          # False = classic copy-on-write
DESTRUCTIVE_VACUUM_DEMO = False      # True = disable the retention safety check on THIS scratch table to show broken time travel

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")

BASE = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/p6_trace"
PATH = f"{BASE}/orders_{'dv' if USE_DELETION_VECTORS else 'cow'}"
LOG  = f"{PATH}/_delta_log"
T    = f"delta.`{PATH}`"

dbutils.fs.rm(PATH, True)            # start clean (scratch path only!)
print("table path:", PATH)

# COMMAND ----------

# Helpers ---------------------------------------------------------------------------------------------------------

ACTION_SCHEMA = "action STRING, detail STRING, dataChange BOOLEAN, numRecords LONG, dvDeletedRows LONG, sizeBytes LONG"

def read_commit(version: int) -> list:
    """Return the list of actions (one JSON object per line) in _delta_log/<version>.json."""
    p = f"{LOG}/{version:020d}.json"
    try:
        with open(p) as fh:                      # /Volumes paths are readable as local files on UC compute
            text = fh.read()
    except Exception:
        text = dbutils.fs.head(p, 10_000_000)    # fallback (abfss:// etc.)
    return [json.loads(line) for line in text.splitlines() if line.strip()]

def short(path: str) -> str:
    name = path.split("/")[-1]
    return name if len(name) <= 30 else name[:20] + "…" + name[-8:]

def summarize(actions: list) -> list:
    rows = []
    for a in actions:
        (kind, body), = a.items()                # every line holds exactly one action
        if kind in ("add", "remove"):
            stats = json.loads(body.get("stats") or "{}")
            dv = body.get("deletionVector")
            rows.append((kind, short(body["path"]), body.get("dataChange"),
                         stats.get("numRecords"), dv.get("cardinality") if dv else None, body.get("size")))
        elif kind == "metaData":
            fields = json.loads(body["schemaString"])["fields"]
            rows.append((kind, "schema: " + ", ".join(f"{f['name']}:{f['type']}" for f in fields)
                         + f" | partitionColumns={body.get('partitionColumns')} | conf={body.get('configuration')}",
                         None, None, None, None))
        elif kind == "protocol":
            rows.append((kind, f"minReader={body['minReaderVersion']} minWriter={body['minWriterVersion']} "
                               f"readerFeatures={body.get('readerFeatures')} writerFeatures={body.get('writerFeatures')}",
                         None, None, None, None))
        elif kind == "commitInfo":
            rows.append((kind, f"{body.get('operation')} params={json.dumps(body.get('operationParameters', {}))[:120]} "
                               f"readVersion={body.get('readVersion')} isolation={body.get('isolationLevel')} "
                               f"blindAppend={body.get('isBlindAppend')}",
                         None, None, None, None))
        else:                                    # txn, cdc, domainMetadata, ...
            rows.append((kind, json.dumps(body)[:160], None, None, None, None))
    return rows

def files_on_disk() -> dict:
    names = [f.name for f in dbutils.fs.ls(PATH)]
    return {"parquet_files_on_disk": sum(n.endswith(".parquet") for n in names),
            "deletion_vector_files": sum(n.endswith(".bin") for n in names)}

def step(title: str):
    h = spark.sql(f"DESCRIBE HISTORY {T} LIMIT 1").first()
    d = spark.sql(f"DESCRIBE DETAIL {T}").first()
    rows = spark.read.format("delta").load(PATH).count()
    print(f"\n=== {title}\nversion={h['version']}  operation={h['operation']}  "
          f"DESCRIBE DETAIL numFiles={d['numFiles']}  rows={rows}  {files_on_disk()}")
    print("operationMetrics:", dict(h["operationMetrics"] or {}))
    display(spark.createDataFrame(summarize(read_commit(h["version"])), ACTION_SCHEMA))

def where_are_rows():
    """Which physical Parquet file serves each row in the CURRENT snapshot."""
    display(spark.read.format("delta").load(PATH)
              .select("order_id", "status", "amount", F.col("_metadata.file_name").alias("file"))
              .orderBy("order_id"))

# COMMAND ----------

# STEP 0 — CREATE: commit 0 holds protocol + metaData + commitInfo, and NO add actions (empty table).
# checkpointInterval = 5 so that we see a checkpoint inside this short demo (open-source default is 10).
spark.sql(f"""
  CREATE TABLE {T} (order_id INT, customer_id STRING, city STRING, amount DOUBLE, status STRING)
  USING DELTA
  TBLPROPERTIES ('delta.enableDeletionVectors' = '{str(USE_DELETION_VECTORS).lower()}',
                 'delta.checkpointInterval'    = '5')
""")
step("v0 CREATE TABLE")

# COMMAND ----------

# STEP 1 — INSERT 6 orders as 2 files: two `add` actions, each with stats (numRecords, minValues, maxValues, nullCount).
orders = spark.createDataFrame([
    (101, "C1", "Dubai",     250.0, "placed"),
    (102, "C2", "Abu Dhabi", 100.0, "placed"),
    (103, "C3", "Dubai",      40.0, "placed"),
    (104, "C1", "Sharjah",    75.5, "placed"),
    (105, "C4", "Dubai",     300.0, "placed"),
    (106, "C2", "Abu Dhabi",  60.0, "placed"),
], "order_id INT, customer_id STRING, city STRING, amount DOUBLE, status STRING")
orders.repartition(2).write.format("delta").mode("append").save(PATH)
step("v1 INSERT (DataFrame append, 2 files)")
where_are_rows()

# Look at the raw stats string of one add action — this is what data skipping uses (lesson 6.8).
first_add = next(a["add"] for a in read_commit(1) if "add" in a)
print(json.dumps(json.loads(first_add["stats"]), indent=2))

# COMMAND ----------

# STEP 2 — INSERT INTO ... VALUES: one more `add`. commitInfo.isBlindAppend = true (it read nothing from the table).
spark.sql(f"""INSERT INTO {T} VALUES
  (107, 'C5', 'Dubai',   120.0, 'placed'),
  (108, 'C3', 'Sharjah',  20.0, 'placed')""")
step("v2 INSERT INTO VALUES")

# COMMAND ----------

# STEP 3 — UPDATE one row.
#   Copy-on-write : remove(file containing 102) + add(rewritten copy of that whole file with 102 changed)
#   Deletion vector: remove(file) + add(SAME path, now carrying deletionVector cardinality=1) + add(new small file with
#                    the updated 102). The old file is NOT rewritten; a deletion_vector_*.bin marks row 102 as deleted.
spark.sql(f"UPDATE {T} SET status = 'shipped' WHERE order_id = 102")
step("v3 UPDATE order 102")
where_are_rows()

# COMMAND ----------

# STEP 4 — DELETE one row.
#   Copy-on-write : remove(file containing 105) + add(copy without 105)
#   Deletion vector: remove(file) + add(same path with a DV, cardinality grows) — no new Parquet data file at all.
#   (If the predicate removes EVERY row of a file, both modes just log `remove` — a metadata-only delete.)
spark.sql(f"DELETE FROM {T} WHERE order_id = 105")
step("v4 DELETE order 105")

# COMMAND ----------

# STEP 5 — MERGE: update 101 (amount 275), insert 109.
#   Internals: join source to target on the ON condition to find touched files → rewrite (CoW) or DV-mark (DV) those
#   files' matched rows → write updated + inserted rows to new files → one commit.
#   This is commit 5, so with checkpointInterval=5 a checkpoint 00000000000000000005.checkpoint.parquet appears.
src = spark.createDataFrame([(101, "C1", "Dubai", 275.0, "placed"), (109, "C6", "Abu Dhabi", 55.0, "placed")],
                            "order_id INT, customer_id STRING, city STRING, amount DOUBLE, status STRING")
(DeltaTable.forPath(spark, PATH).alias("t")
    .merge(src.alias("s"), "t.order_id = s.order_id")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute())
step("v5 MERGE (update 101, insert 109)")
where_are_rows()

# COMMAND ----------

# Checkpoint + _last_checkpoint. (Your runtime may checkpoint on a different schedule — the listing is the truth.)
display(spark.createDataFrame([(f.name, f.size) for f in dbutils.fs.ls(LOG)], "name STRING, size LONG").orderBy("name"))
try:
    print("_last_checkpoint:", dbutils.fs.head(f"{LOG}/_last_checkpoint"))
except Exception as e:
    print("no _last_checkpoint yet:", type(e).__name__)

# The checkpoint is Parquet: one row per action that is still live at v5 (add, protocol, metaData, txn...).
try:
    cp = spark.read.parquet(f"{LOG}/{5:020d}.checkpoint.parquet")
    display(cp.select(F.col("add.path").alias("live_file"), "add.deletionVector.cardinality",
                      F.col("metaData.id").alias("metaData_id"), F.col("protocol.minReaderVersion"))
              .where("live_file IS NOT NULL OR metaData_id IS NOT NULL OR minReaderVersion IS NOT NULL"))
except Exception as e:
    print("checkpoint not at the expected name (V2/UUID checkpoint or different interval):", type(e).__name__)

# COMMAND ----------

# STEP 6a — SCHEMA ENFORCEMENT: an append with an extra column is rejected and NOTHING is committed.
new_orders = spark.createDataFrame([(110, "C7", "Dubai", 90.0, "placed", "web")],
                                   "order_id INT, customer_id STRING, city STRING, amount DOUBLE, status STRING, channel STRING")
try:
    new_orders.write.format("delta").mode("append").save(PATH)
except Exception as e:
    print("rejected:", type(e).__name__, str(e)[:200])
print("latest version still:", spark.sql(f"DESCRIBE HISTORY {T} LIMIT 1").first()["version"])

# STEP 6b — SCHEMA EVOLUTION: same write with mergeSchema → commit has a NEW metaData action (schema + channel) and an add.
new_orders.write.format("delta").mode("append").option("mergeSchema", "true").save(PATH)
step("v6 append with mergeSchema (new column `channel`)")

# COMMAND ----------

# STEP 7 — OPTIMIZE: many small files → 1 file. All remove/add actions have dataChange = false
# (same rows, new layout → streaming readers ignore this commit). Deletion vectors are materialised (rows physically gone).
display(spark.sql(f"OPTIMIZE {T}"))
step("v7 OPTIMIZE")
where_are_rows()

# COMMAND ----------

# STEP 8 — TIME TRAVEL: the old files are still on disk, so old versions are still readable.
print("rows now        :", spark.read.format("delta").load(PATH).count())
print("rows at v1 (SQL):", spark.sql(f"SELECT COUNT(*) AS c FROM {T} VERSION AS OF 1").first()["c"])
print("rows at v1 (API):", spark.read.format("delta").option("versionAsOf", 1).load(PATH).count())
display(spark.read.format("delta").option("versionAsOf", 2).load(PATH).where("order_id IN (101, 102, 105)"))

# COMMAND ----------

# STEP 9 — FILES ON DISK vs FILES IN THE SNAPSHOT, and VACUUM.
print("snapshot numFiles:", spark.sql(f"DESCRIBE DETAIL {T}").first()["numFiles"], "| on disk:", files_on_disk())

# Default retention is 7 days: every removed file is younger than that, so DRY RUN lists nothing.
display(spark.sql(f"VACUUM {T} DRY RUN"))

# Asking for 0 hours is refused by the safety check (spark.databricks.delta.retentionDurationCheck.enabled = true).
try:
    display(spark.sql(f"VACUUM {T} RETAIN 0 HOURS DRY RUN"))
except Exception as e:
    print("safety check:", type(e).__name__, str(e)[:250])

# COMMAND ----------

# OPTIONAL, scratch table only: show WHY short retention breaks time travel.
# Serverless compute may not allow setting this conf; use classic compute if so.
if DESTRUCTIVE_VACUUM_DEMO:
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    try:
        display(spark.sql(f"VACUUM {T} RETAIN 0 HOURS"))
        print("on disk after VACUUM:", files_on_disk())
        try:
            spark.read.format("delta").option("versionAsOf", 1).load(PATH).count()
        except Exception as e:
            print("time travel to v1 now fails:", type(e).__name__, str(e)[:200])
    finally:
        spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true")

# COMMAND ----------

# Full audit trail.
display(spark.sql(f"DESCRIBE HISTORY {T}")
          .select("version", "timestamp", "userName", "operation", "operationParameters",
                  "readVersion", "isolationLevel", "isBlindAppend", "operationMetrics")
          .orderBy("version"))