-- Phase 8.7 — monitoring a pipeline through its EVENT LOG. Run in a SQL editor / notebook as the pipeline's
-- run-as identity (by default only the run-as user can query the event log).
-- Replace <pipeline-id> with the ID from the pipeline page (or `databricks bundle summary`).

-- 1) Make a reusable view. Either the event_log() table-valued function ...
CREATE OR REPLACE TEMPORARY VIEW event_log_raw AS
SELECT * FROM event_log('<pipeline-id>');
-- ... or, if the event log is PUBLISHED (event_log: {catalog, schema, name} in the pipeline settings):
-- CREATE OR REPLACE VIEW main.de_prep.shopco_event_log AS SELECT * FROM main.de_prep_sdp.shopco_event_log;

-- 2) Update history: one row per update with its final state.
SELECT origin.update_id,
       max_by(details:update_progress.state::string, timestamp) AS last_state,
       min(timestamp) AS first_event, max(timestamp) AS last_event
FROM event_log_raw
WHERE event_type = 'update_progress'
GROUP BY origin.update_id
ORDER BY first_event DESC;

-- 3) Expectation metrics of the latest update (passed / failed per rule and dataset).
WITH latest_update AS (
  SELECT origin.update_id AS id FROM event_log_raw
  WHERE event_type = 'create_update' ORDER BY timestamp DESC LIMIT 1
)
SELECT e.dataset, e.name AS expectation,
       SUM(e.passed_records) AS passed, SUM(e.failed_records) AS failed
FROM (
  SELECT explode(from_json(details:flow_progress:data_quality:expectations,
         'array<struct<name: string, dataset: string, passed_records: int, failed_records: int>>')) AS e
  FROM event_log_raw, latest_update
  WHERE event_type = 'flow_progress' AND origin.update_id = latest_update.id
)
GROUP BY ALL
ORDER BY dataset, expectation;
-- Expected after the day-1 file: silver_orders -> valid_order_id failed 1, positive_amount failed 1,
-- valid_status failed 1, the others failed 0 (exact numbers depend on rule evaluation order of dropped rows).

-- 4) Rows written / dropped per flow.
SELECT origin.flow_name, origin.update_id,
       MAX(TRY_CAST(details:flow_progress.metrics.num_output_rows AS BIGINT))       AS output_rows,
       MAX(TRY_CAST(details:flow_progress.data_quality.dropped_records AS BIGINT))  AS dropped_rows,
       MAX_BY(details:flow_progress.status::string, timestamp)                      AS final_status
FROM event_log_raw
WHERE event_type = 'flow_progress' AND origin.flow_name IS NOT NULL
GROUP BY ALL
ORDER BY origin.update_id DESC, origin.flow_name;

-- 5) Errors: what failed and why.
SELECT timestamp, origin.flow_name, message, error
FROM event_log_raw
WHERE level = 'ERROR'
ORDER BY timestamp DESC;

-- 6) Lineage as the pipeline planned it.
SELECT details:flow_definition.output_dataset AS output,
       details:flow_definition.input_datasets AS inputs,
       details:flow_definition.flow_type      AS flow_type
FROM event_log_raw
WHERE details:flow_definition IS NOT NULL;

-- 7) Who started / stopped / edited the pipeline.
SELECT timestamp, details:user_action:action, details:user_action:user_name
FROM event_log_raw WHERE event_type = 'user_action' ORDER BY timestamp DESC;