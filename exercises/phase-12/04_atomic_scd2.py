# Databricks notebook source
# Phase 12.4 — SCD2 two ways on an invented passenger dimension:
#   A) two-step (MERGE to expire, then INSERT new versions)  -> two commits, a crash in between breaks the table
#   B) single atomic MERGE with a staged union                 -> one commit
# Business key: passenger_id. Tracked attributes: name, tier.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG, SCHEMA = "main", "de_prep"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
def t(n): return f"{CATALOG}.{SCHEMA}.{n}"

initial = spark.createDataFrame(
    [("P1", "Aisha", "Gold",   "2026-01-01 00:00:00"),
     ("P2", "Ben",   "Silver", "2026-01-01 00:00:00"),
     ("P3", "Chen",  "Bronze", "2026-01-01 00:00:00")],
    "passenger_id STRING, name STRING, tier STRING, change_ts STRING")
changes = spark.createDataFrame(
    [("P2", "Ben",   "Gold",   "2026-10-01 08:00:00"),   # tier upgrade -> new version
     ("P3", "Chen",  "Bronze", "2026-10-01 08:00:00"),   # no change    -> nothing
     ("P4", "Dana",  "Silver", "2026-10-01 08:00:00")],  # new key      -> insert
    "passenger_id STRING, name STRING, tier STRING, change_ts STRING")

def prep(df):
    return (df.withColumn("change_ts", F.to_timestamp("change_ts"))
              .withColumn("row_hash", F.sha2(F.concat_ws("||", "name", "tier"), 256)))
initial, changes = prep(initial), prep(changes)
changes.createOrReplaceTempView("changes")

def check(table):
    """SCD2 invariants: every key that exists has exactly one current row."""
    bad = spark.sql(f"""
        SELECT k.passenger_id, coalesce(c.n_current, 0) AS n_current
        FROM (SELECT DISTINCT passenger_id FROM {table}) k
        LEFT JOIN (SELECT passenger_id, count(*) AS n_current FROM {table} WHERE is_current GROUP BY passenger_id) c
        USING (passenger_id) WHERE coalesce(c.n_current, 0) <> 1""")
    n = bad.count()
    print(f"{table}: {'OK' if n == 0 else f'{n} key(s) without exactly one current row'}")
    if n: display(bad)

# COMMAND ----------

# ===================== A) TWO-STEP SCD2 (as commonly hand-written) =====================
A = t("scd2_two_step")
spark.sql(f"DROP TABLE IF EXISTS {A}")
spark.sql(f"""CREATE TABLE {A} (sk BIGINT, passenger_id STRING, name STRING, tier STRING, row_hash STRING,
              is_current BOOLEAN, start_ts TIMESTAMP, end_ts TIMESTAMP)""")
(initial.withColumn("sk", F.monotonically_increasing_id() + 1)
        .selectExpr("sk", "passenger_id", "name", "tier", "row_hash", "true AS is_current",
                    "change_ts AS start_ts", "CAST(NULL AS TIMESTAMP) AS end_ts")
        .write.mode("append").saveAsTable(A))

def step1_expire():                                   # commit #1
    spark.sql(f"""MERGE INTO {A} t USING changes s
                  ON t.passenger_id = s.passenger_id AND t.is_current
                  WHEN MATCHED AND t.row_hash <> s.row_hash
                    THEN UPDATE SET is_current = false, end_ts = s.change_ts""")

def step2_insert():                                   # commit #2
    # new versions = changes that have no identical current row; sk = max(sk) + n (control-table style)
    spark.sql(f"""INSERT INTO {A}
        SELECT (SELECT coalesce(max(sk), 0) FROM {A}) + row_number() OVER (ORDER BY s.passenger_id) AS sk,
               s.passenger_id, s.name, s.tier, s.row_hash, true, s.change_ts, NULL
        FROM changes s LEFT ANTI JOIN (SELECT * FROM {A} WHERE is_current) c
          ON s.passenger_id = c.passenger_id AND s.row_hash = c.row_hash""")

step1_expire()
# ---- simulated crash here (cluster lost, job killed) ----
print("State after a crash between the two commits:")
check(A)                                              # P2 has NO current row -> curated joins drop Ben
display(spark.table(A).orderBy("passenger_id", "start_ts"))

# Rerun (orchestrator retry): step 1 finds nothing new to expire, step 2 inserts P2 and P4.
# It recovers ONLY because step 2 derives new versions from the change set (anti-join), not from step 1's output.
step1_expire(); step2_insert()
check(A)

# COMMAND ----------

# ===================== B) SINGLE ATOMIC MERGE (staged union) =====================
B = t("scd2_atomic")
spark.sql(f"DROP TABLE IF EXISTS {B}")
spark.sql(f"""CREATE TABLE {B} (sk BIGINT GENERATED ALWAYS AS IDENTITY, passenger_id STRING, name STRING,
              tier STRING, row_hash STRING, is_current BOOLEAN, start_ts TIMESTAMP, end_ts TIMESTAMP)""")
(initial.selectExpr("passenger_id", "name", "tier", "row_hash", "true AS is_current",
                    "change_ts AS start_ts", "CAST(NULL AS TIMESTAMP) AS end_ts")
        .write.mode("append").saveAsTable(B))

ATOMIC_MERGE = f"""
MERGE INTO {B} t
USING (
  -- copy 1: every change keyed by its business key -> matches the current row (expire) or inserts a new key
  SELECT s.passenger_id AS merge_key, s.* FROM changes s
  UNION ALL
  -- copy 2: only CHANGED existing keys, with a NULL key -> never matches -> inserts the new version
  SELECT NULL AS merge_key, s.* FROM changes s
  JOIN {B} c ON s.passenger_id = c.passenger_id AND c.is_current AND s.row_hash <> c.row_hash
) s
ON t.passenger_id = s.merge_key AND t.is_current
WHEN MATCHED AND t.row_hash <> s.row_hash
  THEN UPDATE SET is_current = false, end_ts = s.change_ts
WHEN NOT MATCHED
  THEN INSERT (passenger_id, name, tier, row_hash, is_current, start_ts, end_ts)
       VALUES (s.passenger_id, s.name, s.tier, s.row_hash, true, s.change_ts, NULL)
"""
spark.sql(ATOMIC_MERGE)          # ONE commit: expiry of P2's old row and insert of P2/P4 succeed or fail together
check(B)
display(spark.table(B).orderBy("passenger_id", "start_ts"))
# Expected (sk values may differ — identity values are unique, not necessarily consecutive):
# P1 Aisha Gold   current
# P2 Ben   Silver expired 2026-10-01 08:00
# P2 Ben   Gold   current  from 2026-10-01 08:00
# P3 Chen  Bronze current  (unchanged, untouched)
# P4 Dana  Silver current  (new)

# COMMAND ----------

# Idempotency: run the same batch again -> no changes (hashes now equal, copy 2 is empty).
before = spark.table(B).count()
spark.sql(ATOMIC_MERGE)
print("rows before/after rerun:", before, spark.table(B).count())   # 5 / 5
check(B)
display(spark.sql(f"DESCRIBE HISTORY {B}").select("version", "operation", "operationMetrics"))

# COMMAND ----------

# Notes for the interview
# - Each MERGE is atomic; the two-step pattern is two transactions -> a visible broken state between them.
# - Dedupe the change set to ONE row per key first (latest by change_ts), otherwise MERGE fails with
#   "multiple source rows matched" — or process multiple versions per key in sequence order.
# - Add a guard so an older change can't create a newer version (compare change_ts with the current start_ts).
# - In declarative pipelines, AUTO CDC (create_auto_cdc_flow, stored_as_scd_type=2, sequence_by) does all of this.