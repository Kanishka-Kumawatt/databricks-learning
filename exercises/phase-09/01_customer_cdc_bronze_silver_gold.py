# Databricks notebook source
# Phase 9 — customer CDC feed through Bronze -> Silver (SCD1) -> Gold dim_customer (SCD2).
#
# Three batches of CDC events (columns: customer_id, name, email, city, tier, op I/U/D, seq, event_ts):
#   Batch 1  initial snapshot load (C1..C5)
#   Batch 2  inserts, updates, a delete, an exact duplicate event, an out-of-order pair
#   Batch 3  a LATE (stale) event for a deleted key, three updates to the same key, a no-op update
#
# Layers:
#   bronze_customer_cdc          append-only raw events + audit columns (idempotent append via txnAppId/txnVersion)
#   silver_customer              SCD1 current state; dedup latest seq per key; guard s.seq > t._seq; SOFT delete
#   silver_customer_hard_delete  same but HARD delete -> shows the "resurrection" bug with late events
#   dim_customer                 SCD2 with surrogate key, effective_from/to, is_current, start_seq/end_seq,
#                                built with ONE atomic MERGE using the "staged union" pattern
#
# Lesson with all before/after tables: phases/09-cdc-scd/09-practical-customer-cdc.md

# COMMAND ----------

from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable

CATALOG, SCHEMA = "main", "de_prep"          # change to a catalog/schema you can write to
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
P = f"{CATALOG}.{SCHEMA}"

BRONZE       = f"{P}.bronze_customer_cdc"
SILVER       = f"{P}.silver_customer"
SILVER_HARD  = f"{P}.silver_customer_hard_delete"
SILVER_NAIVE = f"{P}.silver_customer_naive"
GOLD         = f"{P}.dim_customer"
STAGE        = f"{P}.stg_dim_customer"          # staged MERGE source, kept as a table for inspection
ORDERS       = f"{P}.cdc_demo_orders"
SILVER_REPLAY, GOLD_REPLAY, STAGE_REPLAY = f"{P}.silver_customer_replay", f"{P}.dim_customer_replay", f"{P}.stg_dim_customer_replay"

TRACKED = ["name", "email", "city", "tier"]     # a change in any of these creates a new SCD2 version

# COMMAND ----------

# Reset the demo (drops ONLY the demo tables above)
for t in (BRONZE, SILVER, SILVER_HARD, SILVER_NAIVE, GOLD, STAGE, ORDERS, SILVER_REPLAY, GOLD_REPLAY, STAGE_REPLAY):
    spark.sql(f"DROP TABLE IF EXISTS {t}")

spark.sql(f"""CREATE TABLE {BRONZE} (
  customer_id STRING, name STRING, email STRING, city STRING, tier STRING,
  op STRING, seq BIGINT, event_ts TIMESTAMP,
  _batch_id INT, _source_file STRING, _ingested_at TIMESTAMP)""")

def create_silver(name):
    spark.sql(f"""CREATE TABLE {name} (
      customer_id STRING NOT NULL, name STRING, email STRING, city STRING, tier STRING,
      is_deleted BOOLEAN, _seq BIGINT, _event_ts TIMESTAMP, _batch_id INT)""")

def create_gold(name):
    spark.sql(f"""CREATE TABLE {name} (
      customer_sk    BIGINT GENERATED ALWAYS AS IDENTITY,  -- surrogate key, never reused
      customer_id    STRING NOT NULL,                      -- natural (business) key
      name STRING, email STRING, city STRING, tier STRING, -- tracked attributes
      row_hash       STRING,                               -- sha2 of tracked attributes (change detection)
      effective_from TIMESTAMP,                            -- version valid from (inclusive)
      effective_to   TIMESTAMP,                            -- valid until (exclusive); NULL = open-ended
      is_current     BOOLEAN,                              -- TRUE only for the open version
      start_seq      BIGINT,                               -- source seq that opened this version
      end_seq        BIGINT)                               -- source seq that closed it; NULL = open""")

for t in (SILVER, SILVER_HARD, SILVER_REPLAY):
    create_silver(t)
for t in (GOLD, GOLD_REPLAY):
    create_gold(t)

# COMMAND ----------

# The CDC feed. Each U/I event carries the FULL after-image of the row; D carries only the key.
# seq = monotonically increasing sequence assigned by the source (think: LSN / commit order).
EVT_SCHEMA = "customer_id STRING, name STRING, email STRING, city STRING, tier STRING, op STRING, seq BIGINT, event_ts STRING"
T1 = "2026-10-01 00:00:00"   # snapshot time of the initial load
BATCHES = {
    1: [  # initial snapshot
        ("C1", "Asha Rao",    "asha@shop.co", "Dubai",     "plus",    "I", 1, T1),
        ("C2", "Ben Ali",     "ben@shop.co",  "Abu Dhabi", "basic",   "I", 2, T1),
        ("C3", "Chen Wu",     "chen@shop.co", "Dubai",     "premium", "I", 3, T1),
        ("C4", "Dana Okafor", "dana@shop.co", "Sharjah",   "plus",    "I", 4, T1),
        ("C5", "Eli Haddad",  "eli@shop.co",  "Ajman",     "basic",   "I", 5, T1),
    ],
    2: [  # file order as received
        ("C1", "Asha Rao",   "asha@shop.co",    "Abu Dhabi", "plus",    "U", 10, "2026-10-02 09:00:00"),
        ("C2", "Ben Ali",    "ben@shop.co",     "Abu Dhabi", "plus",    "U", 11, "2026-10-02 09:05:00"),
        ("C3", "Chen Wu",    "chen.wu@shop.co", "Dubai",     "plus",    "U", 14, "2026-10-02 09:20:00"),  # arrives BEFORE seq 13
        ("C3", "Chen Wu",    "chen.wu@shop.co", "Dubai",     "premium", "U", 13, "2026-10-02 09:10:00"),  # out of order
        ("C2", "Ben Ali",    "ben@shop.co",     "Abu Dhabi", "plus",    "U", 11, "2026-10-02 09:05:00"),  # exact duplicate
        ("C4", None,         None,              None,        None,      "D", 15, "2026-10-02 09:30:00"),  # delete
        ("C6", "Farah Khan", "farah@shop.co",   "Dubai",     "basic",   "I", 16, "2026-10-02 09:40:00"),  # new customer
    ],
    3: [
        ("C4", "Dana Okafor", "dana@shop.co",     "Dubai",     "plus",    "U", 12, "2026-10-02 09:08:00"),  # LATE: older than the delete (15)
        ("C1", "Asha Rao",    "asha@shop.co",     "Abu Dhabi", "premium", "U", 20, "2026-10-03 10:00:00"),  # 3 updates, same key
        ("C1", "Asha Rao",    "asha.rao@shop.co", "Abu Dhabi", "premium", "U", 21, "2026-10-03 10:05:00"),
        ("C1", "Asha Rao",    "asha.rao@shop.co", "Dubai",     "premium", "U", 22, "2026-10-03 10:10:00"),
        ("C5", "Eli Haddad",  "eli@shop.co",      "Ajman",     "basic",   "U", 23, "2026-10-03 10:20:00"),  # no-op: values unchanged
    ],
}

def batch_df(n):
    return spark.createDataFrame(BATCHES[n], EVT_SCHEMA).withColumn("event_ts", F.to_timestamp("event_ts"))

# COMMAND ----------

# ---------- BRONZE: append raw events + audit columns, idempotently ----------
def load_bronze(n):
    df = (batch_df(n)
          .withColumn("_batch_id", F.lit(n))
          .withColumn("_source_file", F.lit(f"customers_cdc_batch_{n}.json"))
          .withColumn("_ingested_at", F.current_timestamp()))
    (df.write.format("delta").mode("append")
       .option("txnAppId", "customer_cdc_bronze")   # stable writer id
       .option("txnVersion", n)                     # batch number: a version <= last committed is SKIPPED
       .saveAsTable(BRONZE))

# ---------- SILVER: SCD1 current state ----------
def latest_per_key(events):
    # One row per key: highest seq wins. _ingested_at is only a deterministic tie-breaker.
    w = Window.partitionBy("customer_id").orderBy(F.col("seq").desc(), F.col("_ingested_at").desc())
    return events.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

def merge_silver(events, target, hard_delete=False):
    src = latest_per_key(events)
    m = DeltaTable.forName(spark, target).alias("t").merge(src.alias("s"), "t.customer_id = s.customer_id")
    if hard_delete:
        m = m.whenMatchedDelete(condition="s.seq > t._seq AND s.op = 'D'")
    else:  # soft delete: keep the row as a tombstone, remember the delete's seq
        m = m.whenMatchedUpdate(condition="s.seq > t._seq AND s.op = 'D'", set={
            "is_deleted": "true", "_seq": "s.seq", "_event_ts": "s.event_ts", "_batch_id": "s._batch_id"})
    (m.whenMatchedUpdate(condition="s.seq > t._seq", set={          # newer insert/update -> overwrite (SCD1)
            "name": "s.name", "email": "s.email", "city": "s.city", "tier": "s.tier",
            "is_deleted": "false", "_seq": "s.seq", "_event_ts": "s.event_ts", "_batch_id": "s._batch_id"})
      .whenNotMatchedInsert(condition="s.op <> 'D'", values={       # new key -> insert (a delete for an unknown key is ignored)
            "customer_id": "s.customer_id", "name": "s.name", "email": "s.email", "city": "s.city", "tier": "s.tier",
            "is_deleted": "false", "_seq": "s.seq", "_event_ts": "s.event_ts", "_batch_id": "s._batch_id"})
      .execute())

# COMMAND ----------

# ---------- GOLD: SCD2 via ONE atomic MERGE ("staged union") ----------
def row_hash(cols):
    # COALESCE so that NULLs are hashed explicitly: concat_ws silently skips NULLs, which would make
    # ('a', NULL, 'b') and ('a', 'b', NULL) hash the same.
    return F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c), F.lit("<null>")) for c in cols]), 256)

def stage_gold(events, gold, stage):
    """Build the MERGE source. Handles any number of events per key in the batch (event-grain history)."""
    g = spark.table(gold)

    # 1. exact duplicates: same key + same seq = same event delivered twice
    ev = events.select("customer_id", *TRACKED, "op", "seq", "event_ts").dropDuplicates(["customer_id", "seq"])

    # 2. stale / already-applied events: seq must be newer than anything Gold has seen for the key
    last = g.groupBy("customer_id").agg(F.max(F.coalesce("end_seq", "start_seq")).alias("_last_seq"))
    ev = (ev.join(last, "customer_id", "left")
            .filter(F.col("_last_seq").isNull() | (F.col("seq") > F.col("_last_seq")))
            .drop("_last_seq"))

    # 3. change detection: compare each event with the previous state of the same key
    ev = ev.withColumn("row_hash", row_hash(TRACKED)).withColumn("_is_anchor", F.lit(False))
    anchors = (g.filter("is_current")
                .join(ev.select("customer_id").distinct(), "customer_id", "left_semi")
                .select("customer_id", *TRACKED, F.lit("A").alias("op"), F.col("start_seq").alias("seq"),
                        F.col("effective_from").alias("event_ts"), "row_hash", F.lit(True).alias("_is_anchor")))
    w = Window.partitionBy("customer_id").orderBy("seq")
    seq_rows = (ev.unionByName(anchors)
                  .withColumn("_prev_op", F.lag("op").over(w))
                  .withColumn("_prev_hash", F.lag("row_hash").over(w)))
    keep_delete = (F.col("op") == "D") & F.col("_prev_op").isNotNull() & (F.col("_prev_op") != "D")
    keep_upsert = (F.col("op") != "D") & (F.col("_prev_op").isNull() | (F.col("_prev_op") == "D")
                                          | (F.col("row_hash") != F.col("_prev_hash")))
    kept = seq_rows.filter(~F.col("_is_anchor") & (keep_delete | keep_upsert))

    # 4. each kept I/U event becomes a version, closed by the NEXT kept event of the same key (if any)
    kept = (kept.withColumn("_next_seq", F.lead("seq").over(w))
                .withColumn("_next_ts", F.lead("event_ts").over(w))
                .withColumn("_rank", F.row_number().over(w)))
    inserts = (kept.filter(F.col("op") != "D")
        .select(F.lit(None).cast("string").alias("merge_key"),        # NULL key -> can never match -> INSERT
                "customer_id", *TRACKED, "row_hash",
                F.col("event_ts").alias("effective_from"),
                F.col("_next_ts").alias("effective_to"),
                F.col("_next_seq").isNull().alias("is_current"),
                F.col("seq").alias("start_seq"),
                F.col("_next_seq").alias("end_seq")))

    # 5. the FIRST kept event of a key closes that key's current Gold version (if one exists)
    expires = (kept.filter("_rank = 1")
        .join(anchors.select("customer_id"), "customer_id", "left_semi")
        .select(F.col("customer_id").alias("merge_key"),              # real key -> matches current row -> UPDATE
                "customer_id", *[F.lit(None).cast("string").alias(c) for c in TRACKED],
                F.lit(None).cast("string").alias("row_hash"),
                F.col("event_ts").alias("effective_from"),             # = close timestamp
                F.lit(None).cast("timestamp").alias("effective_to"),
                F.lit(None).cast("boolean").alias("is_current"),
                F.col("seq").alias("start_seq"),                       # = close seq
                F.lit(None).cast("bigint").alias("end_seq")))

    staged = expires.unionByName(inserts)
    # Materialise the source: it reads the target, so pin it before the MERGE writes the target.
    staged.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(stage)
    return spark.table(stage).count()

def merge_gold(gold, stage):
    if spark.table(stage).isEmpty():
        print(f"{gold}: nothing to apply")
        return
    spark.sql(f"""
        MERGE INTO {gold} AS t
        USING {stage} AS s
        ON  t.customer_id = s.merge_key
        AND t.is_current = true
        WHEN MATCHED THEN UPDATE SET
            t.is_current   = false,
            t.effective_to = s.effective_from,
            t.end_seq      = s.start_seq
        WHEN NOT MATCHED AND s.merge_key IS NULL THEN INSERT
            (customer_id, name, email, city, tier, row_hash, effective_from, effective_to, is_current, start_seq, end_seq)
            VALUES (s.customer_id, s.name, s.email, s.city, s.tier, s.row_hash,
                    s.effective_from, s.effective_to, s.is_current, s.start_seq, s.end_seq)
    """)

# COMMAND ----------

def run_batch(n):
    load_bronze(n)
    events = spark.table(BRONZE).filter(F.col("_batch_id") == n)
    merge_silver(events, SILVER)
    merge_silver(events, SILVER_HARD, hard_delete=True)
    print(f"batch {n}: staged {stage_gold(events, GOLD, STAGE)} row(s) for Gold")
    merge_gold(GOLD, STAGE)

def show(n):
    print(f"Bronze rows: {spark.table(BRONZE).count()}")
    display(spark.table(SILVER).orderBy("customer_id"))
    display(spark.table(SILVER_HARD).orderBy("customer_id"))
    display(spark.table(GOLD).orderBy("customer_id", "start_seq"))

def fingerprint(t, cols=None):
    df = spark.table(t)
    cols = cols or df.columns
    return df.select(F.count("*").alias("rows"), F.sum(F.xxhash64(*cols).cast("decimal(38,0)")).alias("hash")).first()

# COMMAND ----------

# BATCH 1 — initial load. Same code path as incremental: the targets are empty, so every row is NOT MATCHED -> INSERT.
run_batch(1)
show(1)
# Expected: Silver 5 rows (C1..C5, is_deleted=false, _seq 1..5); Gold 5 open versions (is_current=true, effective_to NULL).

# COMMAND ----------

# Why dedupe before MERGE? A naive MERGE with the raw batch-2 rows fails:
# C2 appears twice and C3 twice -> several source rows match one target row.
spark.sql(f"CREATE OR REPLACE TABLE {SILVER_NAIVE} AS SELECT * FROM {SILVER}")
try:
    (DeltaTable.forName(spark, SILVER_NAIVE).alias("t")
        .merge(batch_df(2).alias("s"), "t.customer_id = s.customer_id")
        .whenMatchedUpdate(set={"city": "s.city", "tier": "s.tier", "_seq": "s.seq"})
        .execute())
    print("Unexpected: naive MERGE succeeded")
except Exception as e:
    print("Naive MERGE failed as expected:", str(e)[:250])   # DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE

# COMMAND ----------

# BATCH 2 — updates, duplicate, out-of-order pair, delete, insert
run_batch(2)
display(spark.table(STAGE).orderBy(F.col("merge_key").asc_nulls_last(), "customer_id", "start_seq"))   # 4 expire + 5 insert rows
show(2)

# COMMAND ----------

# BATCH 3 — late stale event for deleted C4, three updates to C1, no-op update for C5
run_batch(3)
display(spark.table(STAGE).orderBy(F.col("merge_key").asc_nulls_last(), "customer_id", "start_seq"))   # 1 expire + 3 insert rows
show(3)
# silver_customer:             C4 stays a tombstone (is_deleted=true, _seq 15) -> late seq 12 ignored
# silver_customer_hard_delete: C4 is BACK (Dubai, _seq 12) -> resurrection bug of hard deletes
# dim_customer:                13 rows, 5 current; C5 has no new version (no-op)

# COMMAND ----------

# IDEMPOTENCY — re-run batch 3 (e.g. an automatic retry). Nothing may change.
before = {t: fingerprint(t) for t in (BRONZE, SILVER, GOLD)}
run_batch(3)
after = {t: fingerprint(t) for t in (BRONZE, SILVER, GOLD)}
for t in before:
    print(t, "unchanged" if before[t] == after[t] else "CHANGED!", before[t])
# Bronze: write skipped (txnVersion 3 already committed). Silver: guard s.seq > t._seq is false for every row.
# Gold: every event is stale (seq <= last seq) or a no-op -> staged source is empty -> MERGE skipped.

# COMMAND ----------

# MAGIC %md ### History of each table

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {GOLD}").select("version", "operation", "operationMetrics"))
display(spark.sql(f"DESCRIBE HISTORY {SILVER}").select("version", "operation", "operationMetrics"))
display(spark.sql(f"DESCRIBE HISTORY {BRONZE}").select("version", "operation", "operationMetrics"))
# Gold:  v0 CREATE TABLE, v1 MERGE (inserted 5), v2 MERGE (updated 4, inserted 5), v3 MERGE (updated 1, inserted 3); no v4.
# Silver: v1 inserted 5, v2 updated 4 + inserted 1, v3 updated 2; the re-run may add a MERGE commit with 0 rows changed.
# Bronze: v1, v2, v3 WRITE; no commit for the skipped re-run.

# COMMAND ----------

# Time travel: Gold as it was after batch 2
display(spark.sql(f"SELECT * FROM {GOLD} VERSION AS OF 2 ORDER BY customer_id, start_seq"))
# To roll back a bad load (do NOT run casually):
# spark.sql(f"RESTORE TABLE {GOLD} TO VERSION AS OF 2")

# COMMAND ----------

# MAGIC %md ### Point-in-time lookup: which version was valid when the order happened?

# COMMAND ----------

spark.sql(f"""CREATE TABLE {ORDERS} AS SELECT * FROM VALUES
  ('O1', 'C1', TIMESTAMP'2026-10-01 12:00:00', 100.00),
  ('O2', 'C1', TIMESTAMP'2026-10-02 18:00:00',  60.00),
  ('O3', 'C1', TIMESTAMP'2026-10-03 10:07:00',  80.00),
  ('O4', 'C3', TIMESTAMP'2026-10-02 09:15:00',  45.00),
  ('O5', 'C4', TIMESTAMP'2026-10-02 10:00:00',  30.00)
  AS t(order_id, customer_id, order_ts, amount)""")

display(spark.sql(f"""
SELECT o.order_id, o.customer_id, o.order_ts,
       COALESCE(d.customer_sk, -1) AS customer_sk,     -- -1 = Unknown member
       d.start_seq, d.city, d.tier
FROM {ORDERS} o
LEFT JOIN {GOLD} d
  ON  o.customer_id = d.customer_id
  AND o.order_ts >= d.effective_from                              -- inclusive start
  AND (d.effective_to IS NULL OR o.order_ts < d.effective_to)     -- exclusive end (half-open interval)
ORDER BY o.order_id"""))
# Expected: O1 -> start_seq 1 (Dubai, plus) | O2 -> 10 (Abu Dhabi, plus) | O3 -> 21 (Abu Dhabi, premium)
#           O4 -> 13 (Dubai, premium)       | O5 -> no version (C4 deleted at 09:30) -> customer_sk -1

# COMMAND ----------

# MAGIC %md ### Reconciliation

# COMMAND ----------

bronze = spark.table(BRONZE)
print("Bronze rows:", bronze.count(), "| distinct (customer_id, seq):", bronze.select("customer_id", "seq").distinct().count())  # 17 | 16

active_silver = spark.table(SILVER).filter("NOT is_deleted").select("customer_id", *TRACKED)
current_gold  = spark.table(GOLD).filter("is_current").select("customer_id", *TRACKED)
print("Silver active:", active_silver.count(), "| Gold current:", current_gold.count())                         # 5 | 5
print("Attribute mismatches:", active_silver.exceptAll(current_gold).union(current_gold.exceptAll(active_silver)).count())  # 0

# SCD2 integrity: exactly one current row per active key; versions contiguous (each effective_to = next effective_from)
display(spark.sql(f"SELECT customer_id, count(*) AS current_rows FROM {GOLD} WHERE is_current GROUP BY customer_id HAVING count(*) > 1"))  # empty
display(spark.sql(f"""
SELECT * FROM (
  SELECT customer_id, start_seq, effective_to,
         lead(effective_from) OVER (PARTITION BY customer_id ORDER BY start_seq) AS next_from
  FROM {GOLD})
WHERE next_from IS NOT NULL AND effective_to <> next_from"""))                                                  # empty

# Key reconciliation for deletes: keys that exist in the source system right now (simulated key extract)
source_keys = spark.createDataFrame([("C1",), ("C2",), ("C3",), ("C5",), ("C6",)], "customer_id STRING")
silver_keys = spark.table(SILVER).filter("NOT is_deleted").select("customer_id")
print("In source, missing in Silver:", source_keys.exceptAll(silver_keys).count())   # 0
print("In Silver, missing in source:", silver_keys.exceptAll(source_keys).count())   # 0 (a non-zero value = a missed delete)

# COMMAND ----------

# MAGIC %md ### Replay: rebuild Silver and Gold from ALL Bronze events in one pass

# COMMAND ----------

all_events = spark.table(BRONZE)
merge_silver(all_events, SILVER_REPLAY)
stage_gold(all_events, GOLD_REPLAY, STAGE_REPLAY)
merge_gold(GOLD_REPLAY, STAGE_REPLAY)

cols = ["customer_id", *TRACKED]
inc = spark.table(SILVER).filter("NOT is_deleted").select(cols)
rep = spark.table(SILVER_REPLAY).filter("NOT is_deleted").select(cols)
print("Silver replay differences:", inc.exceptAll(rep).union(rep.exceptAll(inc)).count())        # 0

gcols = ["customer_id", *TRACKED, "is_current", "start_seq", "end_seq"]
ginc, grep = spark.table(GOLD).select(gcols), spark.table(GOLD_REPLAY).select(gcols)
print("Gold incremental rows:", ginc.count(), "| Gold replay rows:", grep.count())               # 13 | 14
display(ginc.exceptAll(grep).withColumn("side", F.lit("incremental only"))
        .unionByName(grep.exceptAll(ginc).withColumn("side", F.lit("replay only"))))
# Difference = the late C4 event (seq 12). Incremental Gold dropped it as stale; the replay saw it in order and
# built C4 Sharjah [seq 4 -> 12) + C4 Dubai [12 -> 15). This is the limitation of the hand-built incremental SCD2
# for late events; AUTO CDC (03_auto_cdc_pipeline.py) is designed to place out-of-order events into history.