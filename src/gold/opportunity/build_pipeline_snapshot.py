from datetime import datetime, timezone
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from src.common.config_loader import ConfigLoader
from src.common.run_registry_utils import RunRegistry
from src.common.watermark_utils import WatermarkManager

def _table_exists(spark, table): return spark.catalog.tableExists(table)
def _snapshot_ts(runtime_params): return runtime_params.get('snapshot_ts') or datetime.now(timezone.utc).isoformat()

def run(runtime_params: dict, project_root: str):
    resolved = ConfigLoader(project_root).build_resolved_config(runtime_params); cfg = resolved.to_dict(); spark = globals()['spark']
    snap_cfg = resolved.gold.get('snapshot', {})
    if not snap_cfg.get('enabled'): raise ValueError('Snapshot not enabled in config')
    rr = RunRegistry(spark, cfg['targets']['pipeline_run_registry']); wm = WatermarkManager(spark, cfg['targets']['pipeline_watermark'])
    rr.log_start(cfg['runtime']['run_id'], cfg['runtime'].get('parent_run_id'), 'crm_gold_pipeline_snapshot', cfg['entity_name'], 'gold_snapshot', cfg['runtime']['trigger_type'], cfg['runtime']['attempt_no'])
    try:
        ts = _snapshot_ts(runtime_params); business_key = resolved.gold.get('business_key','opportunity_id')
        df = spark.table(cfg['targets']['gold_table'])
        snap = (df.withColumn('snapshot_ts', F.to_timestamp(F.lit(ts))).withColumn('snapshot_date', F.to_date(F.to_timestamp(F.lit(ts))))
                  .withColumn('snapshot_hour', F.date_trunc('hour', F.to_timestamp(F.lit(ts))))
                  .withColumn('snapshot_grain', F.lit(snap_cfg.get('snapshot_grain','daily')))
                  .withColumn('snapshot_run_id', F.lit(cfg['runtime']['run_id'])).withColumn('snapshot_created_ts', F.current_timestamp()))
        if 'created_date' in snap.columns: snap = snap.withColumn('days_open', F.datediff(F.col('snapshot_date'), F.col('created_date')))
        if 'expected_close_date' in snap.columns: snap = snap.withColumn('days_to_expected_close', F.datediff(F.col('expected_close_date'), F.col('snapshot_date')))
        if 'days_open' in snap.columns:
            snap = snap.withColumn('pipeline_age_bucket', F.when(F.col('days_open')<=7,'0-7 days').when(F.col('days_open')<=30,'8-30 days').when(F.col('days_open')<=60,'31-60 days').when(F.col('days_open')<=90,'61-90 days').otherwise('90+ days'))
        if not _table_exists(spark, cfg['targets']['snapshot_table']):
            snap.limit(0).write.format('delta').mode('overwrite').option('overwriteSchema','true').saveAsTable(cfg['targets']['snapshot_table'])
        delta = DeltaTable.forName(spark, cfg['targets']['snapshot_table']); vals = {c:f'src.{c}' for c in snap.columns}
        (delta.alias('tgt').merge(snap.alias('src'), f'tgt.snapshot_ts=src.snapshot_ts AND tgt.{business_key}=src.{business_key}')
             .whenMatchedUpdate(set=vals).whenNotMatchedInsert(values=vals).execute())
        spark.sql(f"CREATE OR REPLACE VIEW {cfg['targets']['snapshot_serving_view']} AS SELECT * FROM {cfg['targets']['snapshot_table']}")
        rr.log_success(cfg['runtime']['run_id'], ts, df.count(), snap.count(), 0)
        wm.upsert_current(cfg['entity_name'], 'gold_snapshot', 'snapshot_ts', ts, cfg['runtime']['run_id'])
    except Exception as e:
        rr.log_failure(cfg['runtime']['run_id'], str(e)); raise
