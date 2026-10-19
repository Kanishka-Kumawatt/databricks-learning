-- Databricks notebook source
-- Phase 9 — SQL version of 03_auto_cdc_pipeline.py (Lakeflow Spark Declarative Pipelines, AUTO CDC).
-- Add to a pipeline (not an interactive SQL warehouse). Use EITHER this file OR the Python file in one
-- pipeline, or rename the targets - two pipelines cannot own the same table.

-- COMMAND ----------

-- Pipeline-scoped view that removes exact duplicate events (same key + same seq)
CREATE TEMPORARY VIEW customer_cdc_events_sql AS
SELECT DISTINCT customer_id, name, email, city, tier, op, seq, event_ts
FROM STREAM(main.de_prep.bronze_customer_cdc);

-- COMMAND ----------

-- SCD Type 1
CREATE OR REFRESH STREAMING TABLE silver_customer_auto_cdc_sql
COMMENT 'Customer current state (SCD1) via AUTO CDC';

CREATE FLOW silver_customer_scd1_flow AS AUTO CDC INTO silver_customer_auto_cdc_sql
FROM STREAM(customer_cdc_events_sql)
KEYS (customer_id)
APPLY AS DELETE WHEN op = 'D'
SEQUENCE BY seq
COLUMNS * EXCEPT (op, seq, event_ts)
STORED AS SCD TYPE 1;

-- COMMAND ----------

-- SCD Type 2
CREATE OR REFRESH STREAMING TABLE dim_customer_auto_cdc_sql
COMMENT 'Customer history (SCD2) via AUTO CDC';

CREATE FLOW dim_customer_scd2_flow AS AUTO CDC INTO dim_customer_auto_cdc_sql
FROM STREAM(customer_cdc_events_sql)
KEYS (customer_id)
APPLY AS DELETE WHEN op = 'D'
SEQUENCE BY seq
COLUMNS * EXCEPT (op, seq, event_ts)
STORED AS SCD TYPE 2;
-- Variant: TRACK HISTORY ON * EXCEPT (email)   -> email changes overwrite in place

-- COMMAND ----------

-- Legacy syntax you will still see in older projects (same semantics, still supported):
--   CREATE OR REFRESH STREAMING TABLE dim_customer;
--   APPLY CHANGES INTO dim_customer
--   FROM STREAM(customer_cdc_events)
--   KEYS (customer_id) APPLY AS DELETE WHEN op = 'D' SEQUENCE BY seq
--   COLUMNS * EXCEPT (op, seq, event_ts) STORED AS SCD TYPE 2;
-- Even older (DLT era): APPLY CHANGES INTO LIVE.dim_customer FROM STREAM(LIVE.customer_cdc_events) ...