# Databricks notebook source
# Phase 9 — propagate Silver changes to Gold incrementally with Delta Change Data Feed (CDF).
#
# Prerequisite: run 01_customer_cdc_bronze_silver_gold.py first (it creates bronze_customer_cdc).
# This notebook builds its OWN Silver (with CDF enabled) and its OWN Gold, so it does not touch notebook 1's tables.
#
#   bronze_customer_cdc --(SCD1 MERGE per batch)--> silver_customer_cdf  [delta.enableChangeDataFeed = true]
#        --(read table_changes since last processed version)--> dim_customer_cdf  (SCD2, staged-union MERGE)
#
# Key idea: Gold no longer re-reads Bronze. It reads only the ROWS THAT CHANGED in Silver, per commit,
# with _change_type = insert / update_preimage / update_postimage / delete.

# COMMAND ----------

from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable

CATALOG, SCHEMA = "main", "de_prep"
P = f"{CATALOG}.{SCHEMA}"
BRONZE   = f"{P}.bronze_customer_cdc"      # from notebook 1
SILVER   = f"{P}.silver_customer_cdf"
GOLD     = f"{P}.dim_customer_cdf"
STAGE    = f"{P}.stg_dim_customer_cdf"
PROGRESS = f"{P}.cdf_progress"             # control table: last Silver version propagated to Gold
TRACKED  = ["name", "email", "city", "tier"]

assert spark.catalog.tableExists(BRONZE), "Run notebook 01 first"

# COMMAND ----------

for t in (SILVER, GOLD, STAGE, PROGRESS):
    spark.sql(f"DROP TABLE IF EXISTS {t}")

spark.sql(f"""CREATE TABLE {SILVER} (
  customer_id STRING NOT NULL, name STRING, email STRING, city STRING, tier STRING,
  is_deleted BOOLEAN, _seq BIGINT, _event_ts TIMESTAMP, _batch_id INT)
TBLPROPERTIES (delta.enableChangeDataFeed = true)""")          # CDF recorded from this version (v0) on

spark.sql(f"""CREATE TABLE {GOLD} (
  customer_sk BIGINT GENERATED ALWAYS AS IDENTITY, customer_id STRING NOT NULL,
  name STRING, email STRING, city STRING, tier STRING, row_hash STRING,
  effective_from TIMESTAMP, effective_to TIMESTAMP, is_current BOOLEAN,
  start_seq BIGINT, end_seq BIGINT)""")

spark.sql(f"CREATE TABLE {PROGRESS} (source_table STRING, last_version BIGINT)")
spark.sql(f"INSERT INTO {PROGRESS} VALUES ('{SILVER}', 0)")    # v0 = CREATE TABLE, contains no rows

# COMMAND ----------

# Same Silver SCD1 MERGE as notebook 1 (soft delete, ordering guard), written in SQL this time.
def merge_silver_batch(n):
    spark.sql(f"""
    MERGE INTO {SILVER} AS t
    USING (
      SELECT * FROM {BRONZE}
      WHERE _batch_id = {int(n)}
      QUALIFY row_number() OVER (PARTITION BY customer_id ORDER BY seq DESC, _ingested_at DESC) = 1
    ) AS s
    ON t.customer_id = s.customer_id
    WHEN MATCHED AND s.seq > t._seq AND s.op = 'D' THEN UPDATE SET
      t.is_deleted = true, t._seq = s.seq, t._event_ts = s.event_ts, t._batch_id = s._batch_id
    WHEN MATCHED AND s.seq > t._seq THEN UPDATE SET
      t.name = s.name, t.email = s.email, t.city = s.city, t.tier = s.tier,
      t.is_deleted = false, t._seq = s.seq, t._event_ts = s.event_ts, t._batch_id = s._batch_id
    WHEN NOT MATCHED AND s.op <> 'D' THEN INSERT
      (customer_id, name, email, city, tier, is_deleted, _seq, _event_ts, _batch_id)
      VALUES (s.customer_id, s.name, s.email, s.city, s.tier, false, s.seq, s.event_ts, s._batch_id)
    """)

# COMMAND ----------

# Same staged-union SCD2 builder as notebook 1 (copied so this notebook is self-contained).
def row_hash(cols):
    return F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c), F.lit("<null>")) for c in cols]), 256)

def stage_gold(events):
    g = spark.table(GOLD)
    ev = events.select("customer_id", *TRACKED, "op", "seq", "event_ts").dropDuplicates(["customer_id", "seq"])
    last = g.groupBy("customer_id").agg(F.max(F.coalesce("end_seq", "start_seq")).alias("_last_seq"))
    ev = (ev.join(last, "customer_id", "left")
            .filter(F.col("_last_seq").isNull() | (F.col("seq") > F.col("_last_seq"))).drop("_last_seq"))
    ev = ev.withColumn("row_hash", row_hash(TRACKED)).withColumn("_is_anchor", F.lit(False))
    anchors = (g.filter("is_current").join(ev.select("customer_id").distinct(), "customer_id", "left_semi")
                .select("customer_id", *TRACKED, F.lit("A").alias("op"), F.col("start_seq").alias("seq"),
                        F.col("effective_from").alias("event_ts"), "row_hash", F.lit(True).alias("_is_anchor")))
    w = Window.partitionBy("customer_id").orderBy("seq")
    s = (ev.unionByName(anchors).withColumn("_prev_op", F.lag("op").over(w))
           .withColumn("_prev_hash", F.lag("row_hash").over(w)))
    keep_delete = (F.col("op") == "D") & F.col("_prev_op").isNotNull() & (F.col("_prev_op") != "D")
    keep_upsert = (F.col("op") != "D") & (F.col("_prev_op").isNull() | (F.col("_prev_op") == "D")
                                          | (F.col("row_hash") != F.col("_prev_hash")))
    kept = (s.filter(~F.col("_is_anchor") & (keep_delete | keep_upsert))
              .withColumn("_next_seq", F.lead("seq").over(w)).withColumn("_next_ts", F.lead("event_ts").over(w))
              .withColumn("_rank", F.row_number().over(w)))
    inserts = kept.filter(F.col("op") != "D").select(
        F.lit(None).cast("string").alias("merge_key"), "customer_id", *TRACKED, "row_hash",
        F.col("event_ts").alias("effective_from"), F.col("_next_ts").alias("effective_to"),
        F.col("_next_seq").isNull().alias("is_current"), F.col("seq").alias("start_seq"), F.col("_next_seq").alias("end_seq"))
    expires = kept.filter("_rank = 1").join(anchors.select("customer_id"), "customer_id", "left_semi").select(
        F.col("customer_id").alias("merge_key"), "customer_id", *[F.lit(None).cast("string").alias(c) for c in TRACKED],
        F.lit(None).cast("string").alias("row_hash"), F.col("event_ts").alias("effective_from"),
        F.lit(None).cast("timestamp").alias("effective_to"), F.lit(None).cast("boolean").alias("is_current"),
        F.col("seq").alias("start_seq"), F.lit(None).cast("bigint").alias("end_seq"))
    expires.unionByName(inserts).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(STAGE)

def merge_gold():
    if spark.table(STAGE).isEmpty():
        print("Gold: nothing to apply"); return
    spark.sql(f"""
        MERGE INTO {GOLD} AS t USING {STAGE} AS s
        ON t.customer_id = s.merge_key AND t.is_current = true
        WHEN MATCHED THEN UPDATE SET t.is_current = false, t.effective_to = s.effective_from, t.end_seq = s.start_seq
        WHEN NOT MATCHED AND s.merge_key IS NULL THEN INSERT
          (customer_id, name, email, city, tier, row_hash, effective_from, effective_to, is_current, start_seq, end_seq)
          VALUES (s.customer_id, s.name, s.email, s.city, s.tier, s.row_hash, s.effective_from, s.effective_to,
                  s.is_current, s.start_seq, s.end_seq)""")

# COMMAND ----------

def latest_version(table):
    return spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()["version"]

def propagate_cdf():
    last = spark.table(PROGRESS).filter(F.col("source_table") == SILVER).first()["last_version"]
    end = latest_version(SILVER)
    if end <= last:
        print("No new Silver commits"); return
    changes = (spark.read.format("delta")
               .option("readChangeFeed", "true")
               .option("startingVersion", last + 1)     # inclusive
               .option("endingVersion", end)            # inclusive; pin the range we will record as done
               .table(SILVER))
    display(changes.orderBy("_commit_version", "customer_id", "_change_type"))

    # Ignore pre-images (old values). Post-images, inserts and deletes describe the NEW state.
    events = (changes.filter(F.col("_change_type").isin("insert", "update_postimage", "delete"))
              .select("customer_id", *TRACKED,
                      F.when((F.col("_change_type") == "delete") | F.col("is_deleted"), F.lit("D"))
                       .otherwise(F.lit("U")).alias("op"),
                      F.col("_seq").alias("seq"),               # Silver keeps the source seq -> ordering preserved
                      F.col("_event_ts").alias("event_ts")))
    stage_gold(events)
    merge_gold()
    # Record progress only AFTER Gold committed (same rule as a watermark).
    spark.sql(f"UPDATE {PROGRESS} SET last_version = {end} WHERE source_table = '{SILVER}'")
    print(f"Propagated Silver versions {last + 1}..{end}")

# COMMAND ----------

# Batch 1 -> Silver v1 (5 inserts) -> CDF: 5 rows _change_type = insert -> Gold: 5 open versions
merge_silver_batch(1); propagate_cdf()
display(spark.table(GOLD).orderBy("customer_id", "start_seq"))

# COMMAND ----------

# Batch 2 -> Silver v2. CDF for v2: 9 rows = 4 x (update_preimage + update_postimage) for C1, C2, C3, C4 + 1 insert (C6).
# The C4 soft delete is an UPDATE in Silver: its post-image has is_deleted = true -> mapped to op 'D'.
merge_silver_batch(2)
display(spark.sql(f"""SELECT customer_id, city, tier, is_deleted, _seq, _change_type, _commit_version
                      FROM table_changes('{SILVER}', 2, 2) ORDER BY customer_id, _change_type"""))
propagate_cdf()
display(spark.table(GOLD).orderBy("customer_id", "start_seq"))
# Gold: 9 rows. Note C3 gets ONE new version (seq 14) - the intermediate seq 13 state never existed in Silver.

# COMMAND ----------

# Batch 3 -> Silver v3: C1 updated (seq 22), C5 updated (seq 23, same values). C4 late event ignored by Silver.
merge_silver_batch(3); propagate_cdf()
display(spark.table(GOLD).orderBy("customer_id", "start_seq"))
# Gold: 10 rows. C1 jumps from seq 10 straight to seq 22 (Silver collapsed 20, 21, 22). C5: no-op -> no new version.

# COMMAND ----------

# Idempotent: nothing new in Silver -> nothing to do
propagate_cdf()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Streaming alternative (do not run together with the batch cells above on the same Gold)
# MAGIC ```python
# MAGIC def apply_changes(batch_df, batch_id):
# MAGIC     events = (batch_df.filter(F.col("_change_type").isin("insert", "update_postimage", "delete"))
# MAGIC                       .select("customer_id", *TRACKED,
# MAGIC                               F.when((F.col("_change_type") == "delete") | F.col("is_deleted"), "D").otherwise("U").alias("op"),
# MAGIC                               F.col("_seq").alias("seq"), F.col("_event_ts").alias("event_ts")))
# MAGIC     stage_gold(events); merge_gold()          # idempotent: replayed micro-batches are filtered as stale
# MAGIC
# MAGIC (spark.readStream.format("delta").option("readChangeFeed", "true").option("startingVersion", 1).table(SILVER)
# MAGIC     .writeStream.foreachBatch(apply_changes)
# MAGIC     .option("checkpointLocation", f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints/dim_customer_cdf")
# MAGIC     .trigger(availableNow=True).start())
# MAGIC ```
# MAGIC The checkpoint replaces the `cdf_progress` control table.