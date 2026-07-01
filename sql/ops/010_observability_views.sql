CREATE OR REPLACE VIEW crm_prod.ops_control.vw_recent_pipeline_runs AS
SELECT * FROM crm_prod.ops_control.pipeline_run_registry ORDER BY created_ts DESC;

CREATE OR REPLACE VIEW crm_prod.ops_control.vw_failed_pipeline_runs AS
SELECT * FROM crm_prod.ops_control.pipeline_run_registry WHERE status='failed' ORDER BY updated_ts DESC;

CREATE OR REPLACE VIEW crm_prod.ops_control.vw_layer_freshness AS
WITH r AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY entity_name, layer_name ORDER BY updated_ts DESC) rn
  FROM crm_prod.ops_control.pipeline_run_registry WHERE status IN ('success','partial_success')
)
SELECT *, ROUND((unix_timestamp(current_timestamp())-unix_timestamp(updated_ts))/60,2) AS minutes_since_success
FROM r WHERE rn=1;

CREATE OR REPLACE VIEW crm_prod.ops_control.vw_stale_watermarks AS
SELECT *, ROUND((unix_timestamp(current_timestamp())-unix_timestamp(updated_ts))/60,2) AS minutes_since_watermark_update,
CASE WHEN layer_name IN ('bronze','silver','gold') AND (unix_timestamp(current_timestamp())-unix_timestamp(updated_ts))/60 > 30 THEN CONCAT('STALE_', upper(layer_name))
     WHEN layer_name='gold_snapshot' AND (unix_timestamp(current_timestamp())-unix_timestamp(updated_ts))/3600 > 26 THEN 'STALE_SNAPSHOT'
     ELSE 'OK' END AS freshness_status
FROM crm_prod.ops_control.pipeline_watermark WHERE is_current=true;

CREATE OR REPLACE VIEW crm_prod.ops_quarantine.vw_quarantine_summary AS
SELECT entity_name, layer_name, failed_rule_name, status, DATE(quarantined_ts) AS quarantine_date, COUNT(*) AS quarantined_record_count, MAX(quarantined_ts) AS latest_quarantine_ts
FROM crm_prod.ops_quarantine.bad_records GROUP BY entity_name, layer_name, failed_rule_name, status, DATE(quarantined_ts);

CREATE OR REPLACE VIEW crm_prod.ops_control.vw_pipeline_health_summary AS
WITH latest AS (
 SELECT *, ROW_NUMBER() OVER (PARTITION BY entity_name, layer_name ORDER BY updated_ts DESC) rn
 FROM crm_prod.ops_control.pipeline_run_registry
)
SELECT entity_name, layer_name, status AS latest_status, updated_ts AS latest_run_ts, source_row_count, rows_quarantined,
CASE WHEN status='failed' THEN 'RED' WHEN status='partial_success' THEN 'AMBER' WHEN (unix_timestamp(current_timestamp())-unix_timestamp(updated_ts))/60 > 30 THEN 'AMBER' ELSE 'GREEN' END AS health_status
FROM latest WHERE rn=1;
