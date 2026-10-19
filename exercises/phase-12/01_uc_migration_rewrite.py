# Databricks notebook source
# Phase 12.5 — rewrite a "legacy" Hive-metastore cell so it runs on Unity Catalog compute in STANDARD (shared)
# access mode or serverless. Generic re-creation on invented airline-like data.
#
# The legacy cell is kept as TEXT (it would fail on UC standard compute). Read it, predict each failure, then run
# the rewrite below.

# COMMAND ----------

LEGACY_CELL = r'''
# ---- LEGACY (Hive metastore, mounts, no-isolation cluster) -------------------------------------------
df = spark.table("ops.flights")                                         # (1) 2-part name -> default catalog
fares = spark.read.option("header", True).csv("/mnt/landing/fares/")    # (2) DBFS mount path
routes = df.rdd.map(lambda r: (r.origin + "-" + r.dest, r.seats)) \
           .toDF(["route", "seats"])                                    # (3) RDD API
with open("/dbfs/mnt/landing/config/thresholds.json") as f:             # (4) POSIX /dbfs FUSE path
    thresholds = json.load(f)
routes.write.mode("overwrite").saveAsTable("ops.route_seats")           # (5) 2-part name on write
dbutils.notebook.run("./child_load", 600)                               # (6) child gets NO env/catalog
'''
print(LEGACY_CELL)
# Expected failures on UC standard access mode:
# (1)/(5) TABLE_OR_VIEW_NOT_FOUND or writes into the wrong catalog (whatever the default catalog is)
# (2)     mounts are not supported with Unity Catalog
# (3)     RDD APIs are not supported on standard access mode
# (4)     DBFS root and mounts do not support FUSE on standard access mode
# (6)     the child notebook only sees the arguments passed to it, so it falls back to its widget defaults

# COMMAND ----------

# ---- REWRITE: configuration ---------------------------------------------------------------------------
# CHANGE: the catalog comes from a widget/job parameter instead of being implied by the metastore.
#         Default to a sandbox catalog, never production. In jobs/ADF, pass it explicitly.
import json
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "de_prep")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
if not CATALOG:
    raise ValueError("catalog parameter is required")          # fail fast instead of silently using a default

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
# CHANGE: files that used to sit under /mnt live in a UC VOLUME (governed, works with POSIX APIs).
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.landing")
VOL = f"/Volumes/{CATALOG}/{SCHEMA}/landing"
# For data in an external location you could instead use a cloud URI, e.g.
#   abfss://<container>@<storage-account>.dfs.core.windows.net/landing/fares/
# which UC authorises through the external location's storage credential (no keys in code).

def t(name):                                                    # CHANGE: one helper builds 3-part names
    return f"{CATALOG}.{SCHEMA}.{name}"

# COMMAND ----------

# ---- Sample data (stands in for the legacy source table and landing files) ----------------------------
(spark.createDataFrame(
    [("EY101", "AUH", "LHR", 300), ("EY102", "AUH", "JFK", 350), ("EY103", "AUH", "LHR", 280)],
    "flight_no STRING, origin STRING, dest STRING, seats INT")
 .write.mode("overwrite").saveAsTable(t("mig_flights")))

# CHANGE: write files with dbutils.fs (or Python file APIs) to the VOLUME path, not /dbfs/mnt/...
dbutils.fs.mkdirs(f"{VOL}/fares")
dbutils.fs.mkdirs(f"{VOL}/config")
dbutils.fs.put(f"{VOL}/fares/fares_2026-10-01.csv",
               "route,fare\nAUH-LHR,820\nAUH-JFK,1250\n", overwrite=True)
dbutils.fs.put(f"{VOL}/config/thresholds.json", json.dumps({"min_seats": 290}), overwrite=True)

# COMMAND ----------

# ---- REWRITE: the legacy logic --------------------------------------------------------------------------
# (1) 3-part name
df = spark.table(t("mig_flights"))

# (2) volume path instead of a mount
fares = spark.read.option("header", True).option("inferSchema", True).csv(f"{VOL}/fares/")

# (3) DataFrame API instead of rdd.map — also faster (stays in the JVM, Catalyst-optimised)
routes = (df.select(F.concat_ws("-", "origin", "dest").alias("route"), "seats")
            .groupBy("route").agg(F.sum("seats").alias("seats")))

# (4) Python open() works on /Volumes paths on UC compute (POSIX access to volumes is supported);
#     it would NOT work on /dbfs/mnt/... in standard mode.
with open(f"{VOL}/config/thresholds.json") as f:
    thresholds = json.load(f)

result = (routes.join(fares, "route", "left")
                .withColumn("meets_threshold", F.col("seats") >= F.lit(thresholds["min_seats"])))

# (5) 3-part name on write
result.write.mode("overwrite").saveAsTable(t("mig_route_seats"))
display(spark.table(t("mig_route_seats")).orderBy("route"))
# Expected:
# route    seats fare  meets_threshold
# AUH-JFK  350   1250  true
# AUH-LHR  580   820   true

# COMMAND ----------

# (6) Child notebooks: pass EVERY parameter explicitly; arguments and return values are strings.
#     dbutils.notebook.run("./child_load", 600, {"catalog": CATALOG, "schema": SCHEMA})
# Inside the child, read them with dbutils.widgets.get(...) and fail fast if missing.
# (%run is different: it runs inline in the same context and shares variables.)

# COMMAND ----------

# ---- Self-check: what else to grep for when migrating a notebook ----------------------------------------
PATTERNS = {
    r"/mnt/":            "mount path -> external location URI or /Volumes path",
    r"/dbfs/":           "FUSE path -> /Volumes path or dbutils.fs",
    r"\.rdd\b":          "RDD API -> DataFrame API",
    r"sparkContext|\bsc\.": "SparkContext usage -> SparkSession / DataFrame API",
    r"saveAsTable\(\"\w+\.\w+\"\)": "2-part name -> 3-part name",
    r"notebook\.run\(":  "check that every parameter is forwarded",
}
import re
for pat, fix in PATTERNS.items():
    if re.search(pat, LEGACY_CELL):
        print(f"found {pat!r:35} -> {fix}")