from datetime import datetime, timezone
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from src.common.config_loader import ConfigLoader
from src.common.run_registry_utils import RunRegistry
from src.common.watermark_utils import WatermarkManager

def _table_exists(spark, table): return spark.catalog.tableExists(table)
def _col_or_null(df, source, alias): return F.col(source).alias(alias) if source and source in df.columns else F.lit(None).alias(alias)
def _get_current_watermark(spark, table, entity, layer):
    rows = spark.table(table).filter((F.col('entity_name')==entity)&(F.col('layer_name')==layer)&(F.col('is_current')==True)).orderBy(F.col('updated_ts').desc()).limit(1).collect()
    return rows[0]['watermark_value'] if rows else None

def build_current_fact(runtime_params, project_root, pipeline_name_suffix=None):
    resolved = ConfigLoader(project_root).build_resolved_config(runtime_params)
    cfg = resolved.to_dict(); spark = globals()['spark']
    rr = RunRegistry(spark, cfg['targets']['pipeline_run_registry']); wm = WatermarkManager(spark, cfg['targets']['pipeline_watermark'])
    layer_entity = pipeline_name_suffix or cfg['entity_name']
    rr.log_start(cfg['runtime']['run_id'], cfg['runtime'].get('parent_run_id'), f'crm_gold_{layer_entity}', cfg['entity_name'], 'gold', cfg['runtime']['trigger_type'], cfg['runtime']['attempt_no'])
    try:
        silver_df = spark.table(cfg['targets']['silver_table'])
        current_wm = _get_current_watermark(spark, cfg['targets']['pipeline_watermark'], cfg['entity_name'], 'gold')
        if current_wm and 'silver_updated_ts' in silver_df.columns:
            silver_df = silver_df.filter(F.col('silver_updated_ts') > F.to_timestamp(F.lit(current_wm)))
        if silver_df.limit(1).count() == 0:
            spark.sql(f"UPDATE {cfg['targets']['pipeline_run_registry']} SET run_end_ts=current_timestamp(), status='skipped', updated_ts=current_timestamp() WHERE run_id='{cfg['runtime']['run_id']}'")
            return
        mapping = resolved.gold.get('column_mapping', {})
        exprs = [_col_or_null(silver_df, src, tgt) for tgt, src in mapping.items()]
        active_col = resolved.silver.get('active_flag_column')
        fact_df = silver_df.select(*exprs, F.col(active_col).alias('is_active'), F.col('silver_last_event_ts').alias('source_event_ts'), F.col('silver_last_run_id').alias('source_run_id'), F.col('silver_updated_ts').alias('gold_source_updated_ts'), F.current_timestamp().alias('gold_updated_ts'))
        weighted = resolved.gold.get('derived_columns', {}).get('weighted_amount', {})
        if weighted.get('enabled'):
            fact_df = fact_df.withColumn('weighted_amount', F.col(weighted.get('amount_column','estimated_value')).cast('double') * (F.col(weighted.get('probability_column','probability')).cast('double') / F.lit(100.0)))
        if 'created_ts' in fact_df.columns: fact_df = fact_df.withColumn('created_date', F.to_date('created_ts'))
        if 'expected_close_date' in fact_df.columns: fact_df = fact_df.withColumn('expected_close_month', F.date_trunc('month', F.col('expected_close_date')))
        fact_df = fact_df.withColumn('gold_delete_flag', F.when(F.col('is_active')==False, F.lit(True)).otherwise(F.lit(False)))
        business_key = resolved.gold.get('business_key')
        if not business_key or business_key not in fact_df.columns: raise ValueError(f'Invalid gold.business_key: {business_key}')
        if not _table_exists(spark, cfg['targets']['gold_table']):
            fact_df.filter(F.col('gold_delete_flag')==False).drop('gold_delete_flag').limit(0).write.format('delta').mode('overwrite').option('overwriteSchema','true').saveAsTable(cfg['targets']['gold_table'])
        delta = DeltaTable.forName(spark, cfg['targets']['gold_table'])
        cols = [c for c in fact_df.columns if c != 'gold_delete_flag']
        vals = {c:f'src.{c}' for c in cols}
        (delta.alias('tgt').merge(fact_df.alias('src'), f'tgt.{business_key}=src.{business_key}')
            .whenMatchedDelete(condition='src.gold_delete_flag = true')
            .whenMatchedUpdate(condition='src.gold_delete_flag = false', set=vals)
            .whenNotMatchedInsert(condition='src.gold_delete_flag = false', values=vals)
            .execute())
        spark.sql(f"CREATE OR REPLACE VIEW {cfg['targets']['serving_view']} AS SELECT * FROM {cfg['targets']['gold_table']}")
        high = silver_df.agg(F.max('silver_updated_ts').alias('wm')).collect()[0]['wm']
        high_wm = high.isoformat() if high else datetime.now(timezone.utc).isoformat()
        rr.log_success(cfg['runtime']['run_id'], high_wm, silver_df.count(), fact_df.filter(F.col('gold_delete_flag')==False).count(), 0)
        wm.upsert_current(cfg['entity_name'], 'gold', 'silver_updated_ts', high_wm, cfg['runtime']['run_id'])
    except Exception as e:
        rr.log_failure(cfg['runtime']['run_id'], str(e)); raise
