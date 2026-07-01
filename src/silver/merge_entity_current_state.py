from datetime import datetime, timezone
from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable
from src.common.config_loader import ConfigLoader
from src.common.run_registry_utils import RunRegistry
from src.common.watermark_utils import WatermarkManager
from src.common.dq_utils import apply_mandatory_column_checks, apply_non_negative_checks, split_valid_invalid, build_bad_records_df

def _table_exists(spark, table_name):
    return spark.catalog.tableExists(table_name)

def _get_current_watermark(spark, table, entity, layer):
    rows = (spark.table(table).filter((F.col('entity_name')==entity)&(F.col('layer_name')==layer)&(F.col('is_current')==True)).orderBy(F.col('updated_ts').desc()).limit(1).collect())
    return rows[0]['watermark_value'] if rows else None

def _standardize_event_time(df, event_col):
    return df.withColumn('bronze_business_event_ts', F.col(event_col).cast('timestamp') if event_col in df.columns else F.col('bronze_ingest_ts').cast('timestamp'))

def _dedup_latest(df, pk):
    w = Window.partitionBy(pk).orderBy(F.col('bronze_business_event_ts').desc_nulls_last(), F.col('bronze_ingest_ts').desc_nulls_last())
    return df.withColumn('_rn', F.row_number().over(w)).filter(F.col('_rn')==1).drop('_rn')

def _bootstrap_table(spark, table, df, active_col):
    if _table_exists(spark, table): return
    out = (df.limit(0).withColumn(active_col, F.lit(True)).withColumn('silver_deleted_ts', F.lit(None).cast('timestamp'))
             .withColumn('silver_last_event_ts', F.col('bronze_business_event_ts')).withColumn('silver_last_run_id', F.lit(None).cast('string')).withColumn('silver_updated_ts', F.current_timestamp()))
    out.write.format('delta').mode('overwrite').option('overwriteSchema','true').saveAsTable(table)

def run(runtime_params: dict, project_root: str):
    resolved = ConfigLoader(project_root).build_resolved_config(runtime_params)
    cfg = resolved.to_dict(); spark = globals()['spark']
    pk = resolved.silver['primary_key']; event_col = resolved.silver['event_time_column']; active_col = resolved.silver['active_flag_column']
    rr = RunRegistry(spark, cfg['targets']['pipeline_run_registry']); wm = WatermarkManager(spark, cfg['targets']['pipeline_watermark'])
    rr.log_start(cfg['runtime']['run_id'], cfg['runtime'].get('parent_run_id'), f"crm_silver_{cfg['entity_name']}", cfg['entity_name'], 'silver', cfg['runtime']['trigger_type'], cfg['runtime']['attempt_no'])
    try:
        df = spark.table(cfg['targets']['bronze_table'])
        current_wm = _get_current_watermark(spark, cfg['targets']['pipeline_watermark'], cfg['entity_name'], 'silver')
        if current_wm:
            df = df.filter(F.col('bronze_ingest_ts') > F.to_timestamp(F.lit(current_wm)))
        df = _standardize_event_time(df, event_col)
        if df.limit(1).count() == 0:
            spark.sql(f"UPDATE {cfg['targets']['pipeline_run_registry']} SET run_end_ts=current_timestamp(), status='skipped', updated_ts=current_timestamp() WHERE run_id='{cfg['runtime']['run_id']}'")
            return
        dq = apply_mandatory_column_checks(df, resolved.quality.get('mandatory_columns', []))
        dq = apply_non_negative_checks(dq, resolved.quality.get('non_negative_columns', []))
        valid, invalid = split_valid_invalid(dq)
        q_count = invalid.count()
        if q_count > 0 and resolved.features.get('quarantine_enabled', True):
            build_bad_records_df(invalid, cfg['runtime']['run_id'], cfg['entity_name'], 'silver', pk).write.format('delta').mode('append').saveAsTable(cfg['targets']['bad_records'])
        if resolved.silver.get('dedup_enabled', True): valid = _dedup_latest(valid, pk)
        valid = (valid.withColumn(active_col, F.when(F.col('bronze_is_delete') == True, F.lit(False)).otherwise(F.lit(True)))
                    .withColumn('silver_deleted_ts', F.when(F.col('bronze_is_delete') == True, F.coalesce(F.col('bronze_business_event_ts'), F.current_timestamp())).otherwise(F.lit(None).cast('timestamp')))
                    .withColumn('silver_last_event_ts', F.col('bronze_business_event_ts'))
                    .withColumn('silver_last_run_id', F.lit(cfg['runtime']['run_id']))
                    .withColumn('silver_updated_ts', F.current_timestamp()))
        _bootstrap_table(spark, cfg['targets']['silver_table'], valid, active_col)
        delta = DeltaTable.forName(spark, cfg['targets']['silver_table'])
        cols = [c for c in valid.columns if c != 'bronze_is_delete']
        update_non_delete = {c: f'src.{c}' for c in cols}; update_non_delete[active_col]='true'; update_non_delete['silver_deleted_ts']='NULL'; update_non_delete['silver_updated_ts']='current_timestamp()'
        update_delete = {active_col:'false','silver_deleted_ts':'coalesce(src.bronze_business_event_ts, current_timestamp())','silver_last_event_ts':'src.bronze_business_event_ts','silver_last_run_id':f"'{cfg['runtime']['run_id']}'",'silver_updated_ts':'current_timestamp()'}
        insert_values = {c:f'src.{c}' for c in cols}; insert_values[active_col]='true'; insert_values['silver_deleted_ts']='NULL'; insert_values['silver_updated_ts']='current_timestamp()'
        (delta.alias('tgt').merge(valid.alias('src'), f'tgt.{pk}=src.{pk}')
            .whenMatchedUpdate(condition='src.bronze_is_delete = true', set=update_delete)
            .whenMatchedUpdate(condition='src.bronze_is_delete = false', set=update_non_delete)
            .whenNotMatchedInsert(condition='src.bronze_is_delete = false', values=insert_values)
            .execute())
        high_wm_row = valid.agg(F.max('bronze_ingest_ts').alias('wm')).collect()[0]
        high_wm = high_wm_row['wm'].isoformat() if high_wm_row['wm'] else datetime.now(timezone.utc).isoformat()
        rr.log_success(cfg['runtime']['run_id'], high_wm, df.count(), valid.count(), q_count)
        wm.upsert_current(cfg['entity_name'], 'silver', 'bronze_ingest_ts', high_wm, cfg['runtime']['run_id'])
    except Exception as e:
        rr.log_failure(cfg['runtime']['run_id'], str(e)); raise
