# Phase 8.5 / 8.6 / 8.7 — ShopCo orders pipeline, Lakeflow Spark Declarative Pipelines, CURRENT Python API.
#
# This is a pipeline SOURCE FILE, not a notebook: you do not run it cell by cell. The pipeline runtime
# imports it, collects every dataset definition, builds a dataflow graph and then runs an UPDATE.
# It is deployed by 07_shopco_pipeline.pipeline.yml (libraries -> file). `spark` is provided by the runtime.
#
# Graph:
#   cloudFiles (JSON) ──▶ bronze_orders (ST) ──▶ orders_prepared (temporary view)
#                                                   ├──▶ silver_orders (ST, expectations: warn/drop/fail)
#                                                   └──▶ silver_orders_quarantine (ST, inverse rules)
#                                     silver_orders ──▶ gold_daily_revenue (MV)
#
# ST = streaming table (incremental, each input row processed once)
# MV = materialized view (result always equals the defining query at refresh time)

from pyspark import pipelines as dp          # legacy equivalent: import dlt
from pyspark.sql import functions as F

# Pipeline configuration value (set under `configuration:` in the pipeline settings). No hard-coded paths.
LANDING = spark.conf.get("shopco.landing_path", "/Volumes/main/de_prep/landing/orders")

# --------------------------------------------------------------------------------------------------
# Data-quality rules: name -> SQL boolean expression that must be TRUE for a valid row.
# Written NULL-SAFE on purpose (explicit IS NOT NULL), so "valid" and "quarantine" are exact complements.
# --------------------------------------------------------------------------------------------------
DROP_RULES = {
    "valid_order_id":  "order_id IS NOT NULL",
    "valid_customer":  "customer_id IS NOT NULL",
    "positive_amount": "amount IS NOT NULL AND amount > 0",
    "valid_status":    "status IS NOT NULL AND status IN ('PLACED', 'SHIPPED', 'CANCELLED')",
    "valid_order_ts":  "order_ts IS NOT NULL",
}
# A row is quarantined when ANY drop rule is not TRUE. COALESCE(..., FALSE) = belt-and-braces NULL handling.
QUARANTINE_CONDITION = "NOT COALESCE(({}), FALSE)".format(") AND (".join(DROP_RULES.values()))


# ================================== BRONZE ==================================
@dp.table(
    name="bronze_orders",
    comment="Raw ShopCo orders as received, plus audit columns. Append-only.",
    table_properties={"quality": "bronze"},
)
def bronze_orders():
    # spark.readStream -> a STREAMING DataFrame -> @dp.table creates a STREAMING TABLE.
    # No checkpointLocation and no cloudFiles.schemaLocation: the pipeline manages both for every flow.
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaHints",
                "order_id BIGINT, amount DECIMAL(10,2), order_ts TIMESTAMP")
        # default schemaEvolutionMode = addNewColumns; unexpected values land in _rescued_data
        .load(LANDING)
        .select(
            "*",
            F.col("_metadata.file_path").alias("_source_file"),
            F.current_timestamp().alias("_ingested_at"),
        )
    )


# ============================ SHARED PREPARATION ============================
@dp.temporary_view(name="orders_prepared",
                   comment="Typed and standardised orders; shared by Silver and quarantine. Not published.")
def orders_prepared():
    # Reading a dataset of the SAME pipeline by name creates an edge in the dataflow graph.
    return (
        spark.readStream.table("bronze_orders")
        .select(
            F.col("order_id").cast("bigint").alias("order_id"),
            F.trim("customer_id").alias("customer_id"),
            F.upper(F.trim("region")).alias("region"),
            F.col("amount").cast("decimal(10,2)").alias("amount"),
            F.upper(F.trim("status")).alias("status"),
            F.col("order_ts").cast("timestamp").alias("order_ts"),
            "_rescued_data", "_source_file", "_ingested_at",
        )
    )


# ================================== SILVER ==================================
@dp.table(
    name="silver_orders",
    comment="Validated ShopCo orders. Invalid rows are dropped here and kept in silver_orders_quarantine.",
    table_properties={"quality": "silver"},
)
# WARN (keep row, record metric): schema drift should be visible but must not lose orders.
@dp.expect("no_rescued_data", "_rescued_data IS NULL")
# FAIL (stop the update for this flow, roll back its transaction): an invariant that signals a bug,
# e.g. amounts sent in fils instead of dirhams. Rows with NULL amount pass this rule (the drop rule handles them).
@dp.expect_or_fail("amount_sane", "amount IS NULL OR amount < 100000")
# DROP, single rule (shown separately only to demonstrate the single-rule decorator).
@dp.expect_or_drop("valid_order_id", DROP_RULES["valid_order_id"])
# DROP, a group of rules in one decorator.
@dp.expect_all_or_drop({k: v for k, v in DROP_RULES.items() if k != "valid_order_id"})
def silver_orders():
    return (
        spark.readStream.table("orders_prepared")
        # Streaming de-duplication of partner re-sends, bounded by a watermark so state does not grow forever.
        # dropDuplicatesWithinWatermark needs Spark 3.5+ / DBR 13.3+ — verify for your pipeline channel.
        .withWatermark("order_ts", "2 days")
        .dropDuplicatesWithinWatermark(["order_id"])
    )


# ================================ QUARANTINE ================================
@dp.table(
    name="silver_orders_quarantine",
    comment="Rows that failed at least one Silver rule, with the names of the failed rules.",
    table_properties={"quality": "quarantine"},
)
def silver_orders_quarantine():
    failed = [F.when(~F.coalesce(F.expr(cond), F.lit(False)), F.lit(name)) for name, cond in DROP_RULES.items()]
    return (
        spark.readStream.table("orders_prepared")
        .where(QUARANTINE_CONDITION)                               # the INVERSE of the Silver drop rules
        .withColumn("_failed_rules", F.filter(F.array(*failed), lambda x: x.isNotNull()))
        .withColumn("_quarantined_at", F.current_timestamp())
    )


# =================================== GOLD ===================================
@dp.materialized_view(
    name="gold_daily_revenue",
    comment="Revenue and order count per order date and region.",
    table_properties={"quality": "gold"},
)
def gold_daily_revenue():
    # spark.read (BATCH) of an upstream dataset -> a materialized view. Its result always equals this query
    # over the current Silver data; the engine refreshes it incrementally when it can (serverless), else recomputes.
    return (
        spark.read.table("silver_orders")
        .groupBy(F.to_date("order_ts").alias("order_date"), "region")
        .agg(F.count("*").alias("orders"), F.sum("amount").alias("revenue"))
    )


# ============================ OPTIONAL PATTERNS =============================
# (1) Several sources into ONE streaming table with append flows (no UNION, each flow has its own checkpoint).
#     Uncomment when a second landing folder exists, otherwise Auto Loader fails on a missing path.
#
# dp.create_streaming_table(name="bronze_orders_all", comment="Web + marketplace orders")
#
# @dp.append_flow(target="bronze_orders_all", name="web_orders_flow")
# def web_orders_flow():
#     return spark.readStream.table("bronze_orders")
#
# @dp.append_flow(target="bronze_orders_all", name="marketplace_orders_flow")
# def marketplace_orders_flow():
#     return (spark.readStream.format("cloudFiles").option("cloudFiles.format", "json")
#             .load(LANDING.replace("/orders", "/marketplace_orders")))
#
# @dp.append_flow(target="bronze_orders_all", name="history_backfill", once=True)   # runs once, batch DataFrame
# def history_backfill():
#     return spark.read.table("main.legacy.orders_2019_2025")
#
# (2) CDC upserts / SCD2 are declared with AUTO CDC — deep dive in Phase 9:
#
# dp.create_streaming_table("silver_customers")
# dp.create_auto_cdc_flow(target="silver_customers", source="bronze_customer_changes",
#                         keys=["customer_id"], sequence_by=F.col("change_ts"), stored_as_scd_type=2)