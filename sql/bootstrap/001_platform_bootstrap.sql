CREATE CATALOG IF NOT EXISTS crm_dev;
CREATE CATALOG IF NOT EXISTS crm_uat;
CREATE CATALOG IF NOT EXISTS crm_prod;

-- Run below per target catalog by replacing crm_prod if needed.
CREATE SCHEMA IF NOT EXISTS crm_prod.bronze;
CREATE SCHEMA IF NOT EXISTS crm_prod.silver;
CREATE SCHEMA IF NOT EXISTS crm_prod.gold;
CREATE SCHEMA IF NOT EXISTS crm_prod.ops_control;
CREATE SCHEMA IF NOT EXISTS crm_prod.ops_quarantine;
CREATE SCHEMA IF NOT EXISTS crm_prod.serving;

CREATE TABLE IF NOT EXISTS crm_prod.ops_control.pipeline_run_registry (
    run_id STRING NOT NULL,
    parent_run_id STRING,
    pipeline_name STRING NOT NULL,
    entity_name STRING NOT NULL,
    layer_name STRING NOT NULL,
    trigger_type STRING NOT NULL,
    attempt_no INT NOT NULL,
    run_start_ts TIMESTAMP NOT NULL,
    run_end_ts TIMESTAMP,
    status STRING NOT NULL,
    source_high_watermark STRING,
    source_row_count BIGINT,
    target_row_count BIGINT,
    rows_quarantined BIGINT DEFAULT 0,
    error_message STRING,
    created_ts TIMESTAMP NOT NULL,
    updated_ts TIMESTAMP NOT NULL
) USING DELTA TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

CREATE TABLE IF NOT EXISTS crm_prod.ops_control.pipeline_watermark (
    entity_name STRING NOT NULL,
    layer_name STRING NOT NULL,
    watermark_type STRING NOT NULL,
    watermark_value STRING NOT NULL,
    run_id STRING NOT NULL,
    is_current BOOLEAN NOT NULL,
    updated_ts TIMESTAMP NOT NULL
) USING DELTA TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

CREATE TABLE IF NOT EXISTS crm_prod.ops_quarantine.bad_records (
    quarantine_id STRING NOT NULL,
    run_id STRING NOT NULL,
    entity_name STRING NOT NULL,
    layer_name STRING NOT NULL,
    record_key STRING,
    failed_rule_name STRING,
    failure_reason STRING,
    raw_record STRING,
    source_file_path STRING,
    status STRING NOT NULL,
    quarantined_ts TIMESTAMP NOT NULL
) USING DELTA TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');
