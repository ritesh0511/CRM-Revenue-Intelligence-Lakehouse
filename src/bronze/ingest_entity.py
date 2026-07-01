from datetime import datetime, timezone
from pyspark.sql import functions as F
from src.common.config_loader import ConfigLoader
from src.common.run_registry_utils import RunRegistry
from src.common.watermark_utils import WatermarkManager

def _build_trigger_kwargs(trigger_mode, microbatch_interval=None):
    if trigger_mode == 'available_now': return {'availableNow': True}
    if trigger_mode == 'micro_batch': return {'processingTime': microbatch_interval or '5 minutes'}
    if trigger_mode == 'continuous': return {'continuous': microbatch_interval or '5 minutes'}
    raise ValueError(f'Unsupported trigger_mode: {trigger_mode}')

def _with_bronze_cdc_columns(df, cfg):
    op_col = cfg['source'].get('operation_column')
    if op_col and op_col in df.columns:
        op = F.upper(F.coalesce(F.col(op_col).cast('string'), F.lit('')))
        delete_vals = [v.upper() for v in cfg['source'].get('delete_values', [])]
        update_vals = [v.upper() for v in cfg['source'].get('update_values', [])]
        insert_vals = [v.upper() for v in cfg['source'].get('insert_values', [])]
        op_type = (F.when(op.isin(delete_vals), F.lit('DELETE'))
                     .when(op.isin(update_vals), F.lit('UPDATE'))
                     .when(op.isin(insert_vals), F.lit('INSERT'))
                     .otherwise(F.lit('UNKNOWN')))
        return df.withColumn('bronze_op_type', op_type).withColumn('bronze_is_delete', op_type == F.lit('DELETE'))
    return df.withColumn('bronze_op_type', F.lit('UNKNOWN')).withColumn('bronze_is_delete', F.lit(False))

def run(runtime_params: dict, project_root: str):
    resolved = ConfigLoader(project_root).build_resolved_config(runtime_params)
    cfg = resolved.to_dict()
    spark = globals()['spark']
    rr = RunRegistry(spark, cfg['targets']['pipeline_run_registry'])
    wm = WatermarkManager(spark, cfg['targets']['pipeline_watermark'])
    rr.log_start(cfg['runtime']['run_id'], cfg['runtime'].get('parent_run_id'), f"crm_bronze_{cfg['entity_name']}", cfg['entity_name'], 'bronze', cfg['runtime']['trigger_type'], cfg['runtime']['attempt_no'])
    try:
        reader = (spark.readStream.format('cloudFiles')
            .option('cloudFiles.format', cfg['source']['file_format'])
            .option('cloudFiles.schemaLocation', cfg['paths']['schema_path'])
            .option('cloudFiles.inferColumnTypes', 'true'))
        df = reader.load(cfg['source']['path'])
        df = _with_bronze_cdc_columns(df, cfg)
        df = (df.withColumn('bronze_ingest_ts', F.current_timestamp())
                .withColumn('bronze_source_file_path', F.input_file_name())
                .withColumn('bronze_pipeline_run_id', F.lit(cfg['runtime']['run_id']))
                .withColumn('bronze_entity_name', F.lit(cfg['entity_name']))
                .withColumn('bronze_source_type', F.lit(cfg['source']['source_type']))
                .withColumn('bronze_write_mode', F.lit(cfg['source']['write_mode']))
                .withColumn('bronze_delete_handling', F.lit(cfg['source']['delete_handling'])))
        q = (df.writeStream.format('delta')
            .option('checkpointLocation', cfg['paths']['checkpoint_path'])
            .option('mergeSchema', str(cfg['features']['schema_evolution_enabled']).lower())
            .outputMode('append')
            .trigger(**_build_trigger_kwargs(cfg['runtime']['trigger_mode'], cfg['runtime'].get('microbatch_interval')))
            .toTable(cfg['targets']['bronze_table']))
        q.awaitTermination()
        count = spark.table(cfg['targets']['bronze_table']).filter(F.col('bronze_pipeline_run_id') == cfg['runtime']['run_id']).count()
        high_wm = datetime.now(timezone.utc).isoformat()
        rr.log_success(cfg['runtime']['run_id'], high_wm, count, count, 0)
        wm.upsert_current(cfg['entity_name'], 'bronze', cfg['source'].get('watermark_type', 'source_high_watermark'), high_wm, cfg['runtime']['run_id'])
    except Exception as e:
        rr.log_failure(cfg['runtime']['run_id'], str(e))
        raise
