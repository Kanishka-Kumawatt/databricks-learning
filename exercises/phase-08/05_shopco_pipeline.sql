-- Phase 8.5 / 8.6 — the SAME ShopCo pipeline in SQL (Lakeflow Spark Declarative Pipelines, current syntax).
-- Pipeline source file: deploy it as its OWN pipeline (see 07_shopco_pipeline.pipeline.yml, `shopco_orders_sql`).
-- Do not put it in the same pipeline as 05_shopco_pipeline.py: both define the same dataset names.
--
-- ${shopco.landing_path} is substituted from the pipeline `configuration` block.
-- Unqualified names (bronze_orders, ...) resolve to the pipeline's default catalog + schema. No LIVE. prefix.

-- ================================== BRONZE ==================================
-- STREAM read_files(...) = Auto Loader in SQL. Schema location and checkpoint are managed by the pipeline.
CREATE OR REFRESH STREAMING TABLE bronze_orders
COMMENT 'Raw ShopCo orders as received, plus audit columns. Append-only.'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT
  *,
  _metadata.file_path  AS _source_file,
  current_timestamp()  AS _ingested_at
FROM STREAM read_files(
  '${shopco.landing_path}',
  format      => 'json',
  schemaHints => 'order_id BIGINT, amount DECIMAL(10,2), order_ts TIMESTAMP'
);

-- ============================ SHARED PREPARATION ============================
-- Temporary view: part of the graph, never published to Unity Catalog.
CREATE TEMPORARY VIEW orders_prepared
COMMENT 'Typed and standardised orders shared by Silver and quarantine.'
AS SELECT
  CAST(order_id AS BIGINT)        AS order_id,
  trim(customer_id)               AS customer_id,
  upper(trim(region))             AS region,
  CAST(amount AS DECIMAL(10,2))   AS amount,
  upper(trim(status))             AS status,
  CAST(order_ts AS TIMESTAMP)     AS order_ts,
  _rescued_data, _source_file, _ingested_at
FROM STREAM(bronze_orders);

-- ================================== SILVER ==================================
-- No ON VIOLATION  -> warn (row kept, metric recorded)
-- DROP ROW         -> row removed before the write, metric recorded
-- FAIL UPDATE      -> the update of this flow fails and its transaction is rolled back
CREATE OR REFRESH STREAMING TABLE silver_orders (
  CONSTRAINT no_rescued_data EXPECT (_rescued_data IS NULL),
  CONSTRAINT amount_sane     EXPECT (amount IS NULL OR amount < 100000)               ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_order_id  EXPECT (order_id IS NOT NULL)                            ON VIOLATION DROP ROW,
  CONSTRAINT valid_customer  EXPECT (customer_id IS NOT NULL)                         ON VIOLATION DROP ROW,
  CONSTRAINT positive_amount EXPECT (amount IS NOT NULL AND amount > 0)               ON VIOLATION DROP ROW,
  CONSTRAINT valid_status    EXPECT (status IS NOT NULL AND status IN ('PLACED', 'SHIPPED', 'CANCELLED')) ON VIOLATION DROP ROW,
  CONSTRAINT valid_order_ts  EXPECT (order_ts IS NOT NULL)                            ON VIOLATION DROP ROW
)
COMMENT 'Validated ShopCo orders. Invalid rows are kept in silver_orders_quarantine.'
TBLPROPERTIES ('quality' = 'silver')
AS SELECT * FROM STREAM(orders_prepared);
-- (The Python version also de-duplicates with a watermark; keep the SQL version simple.)

-- ================================ QUARANTINE ================================
-- Inverse of the DROP rules, NULL-safe, with the list of failed rule names.
CREATE OR REFRESH STREAMING TABLE silver_orders_quarantine
COMMENT 'Rows that failed at least one Silver rule.'
TBLPROPERTIES ('quality' = 'quarantine')
AS SELECT
  *,
  filter(array(
    CASE WHEN NOT coalesce(order_id IS NOT NULL, FALSE)                    THEN 'valid_order_id'  END,
    CASE WHEN NOT coalesce(customer_id IS NOT NULL, FALSE)                 THEN 'valid_customer'  END,
    CASE WHEN NOT coalesce(amount IS NOT NULL AND amount > 0, FALSE)       THEN 'positive_amount' END,
    CASE WHEN NOT coalesce(status IN ('PLACED', 'SHIPPED', 'CANCELLED'), FALSE) THEN 'valid_status' END,
    CASE WHEN NOT coalesce(order_ts IS NOT NULL, FALSE)                    THEN 'valid_order_ts'  END
  ), x -> x IS NOT NULL)  AS _failed_rules,
  current_timestamp()     AS _quarantined_at
FROM STREAM(orders_prepared)
WHERE NOT coalesce(
        order_id IS NOT NULL
    AND customer_id IS NOT NULL
    AND amount IS NOT NULL AND amount > 0
    AND status IN ('PLACED', 'SHIPPED', 'CANCELLED')
    AND order_ts IS NOT NULL, FALSE);

-- =================================== GOLD ===================================
-- Batch read (no STREAM) of an upstream dataset -> materialized view.
CREATE OR REFRESH MATERIALIZED VIEW gold_daily_revenue
COMMENT 'Revenue and order count per order date and region.'
TBLPROPERTIES ('quality' = 'gold')
AS SELECT
  to_date(order_ts) AS order_date,
  region,
  count(*)          AS orders,
  sum(amount)       AS revenue
FROM silver_orders
GROUP BY ALL;

-- ============================ OPTIONAL PATTERNS =============================
-- Multiple flows into one streaming table (each flow = own checkpoint):
--   CREATE OR REFRESH STREAMING TABLE bronze_orders_all;
--   CREATE FLOW web_orders_flow         AS INSERT INTO bronze_orders_all BY NAME SELECT * FROM STREAM(bronze_orders);
--   CREATE FLOW marketplace_orders_flow AS INSERT INTO bronze_orders_all BY NAME
--     SELECT * FROM STREAM read_files('/Volumes/main/de_prep/landing/marketplace_orders', format => 'json');
--   CREATE FLOW history_backfill        AS INSERT ONCE INTO bronze_orders_all BY NAME SELECT * FROM main.legacy.orders_2019_2025;
--   Syntax: CREATE FLOW name AS INSERT [ONCE] INTO target BY NAME query
--
-- CDC (Phase 9):
--   CREATE OR REFRESH STREAMING TABLE silver_customers;
--   CREATE FLOW customers_cdc AS AUTO CDC INTO silver_customers
--     FROM STREAM(bronze_customer_changes) KEYS (customer_id) SEQUENCE BY change_ts STORED AS SCD TYPE 2;
--
-- LEGACY syntax you will still see in older code (do not write new code like this):
--   CREATE OR REFRESH LIVE TABLE gold AS SELECT ... FROM LIVE.silver_orders;      -- LIVE TABLE / LIVE. prefix
--   CREATE OR REFRESH STREAMING LIVE TABLE bronze AS SELECT * FROM cloud_files('/path', 'json');
--   APPLY CHANGES INTO LIVE.silver_customers FROM STREAM(LIVE.bronze_customer_changes) KEYS (customer_id) SEQUENCE BY change_ts;