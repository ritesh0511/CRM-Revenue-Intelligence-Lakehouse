from datetime import datetime, timezone
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from src.common.config_loader import ConfigLoader
from src.common.run_registry_utils import RunRegistry
from src.common.watermark_utils import WatermarkManager

def _table_exists(spark, table): return spark.catalog.tableExists(table)

def _select_activity(df, activity_type):
    cols = df.columns
    def c(name, alias=None):
        return F.col(name).alias(alias or name) if name in cols else F.lit(None).alias(alias or name)
    return df.select(
        c('activityid','activity_id'), F.lit(activity_type).alias('activity_type'), c('regardingobjectid','opportunity_id'),
        c('ownerid','owner_id'), c('subject'), c('scheduledstart','scheduled_start'), c('scheduledend','scheduled_end'),
        c('actualend','actual_end'), c('createdon','created_ts'), c('modifiedon','modified_ts'), c('statuscode','status_code'),
        c('statecode','state_code'), c('is_active'), c('silver_last_event_ts','source_event_ts'), c('silver_last_run_id','source_run_id'),
        c('silver_updated_ts','gold_source_updated_ts'), F.current_timestamp().alias('gold_updated_ts')
    )

def run(runtime_params: dict, project_root: str):
    # Uses env runtime but activity is a combined mart from phonecall + appointment configs.
    params = dict(runtime_params); params['entity_name'] = 'phonecall'
    phone = ConfigLoader(project_root).build_resolved_config(params).to_dict()
    params['entity_name'] = 'appointment'
    appt = ConfigLoader(project_root).build_resolved_config(params).to_dict()
    spark = globals()['spark']
    target = phone['targets']['gold_table']  # fact_activity as configured in phonecall
    run_table = phone['targets']['pipeline_run_registry']; wm_table = phone['targets']['pipeline_watermark']
    rr = RunRegistry(spark, run_table); wm = WatermarkManager(spark, wm_table)
    rr.log_start(runtime_params['run_id'], runtime_params.get('parent_run_id'), 'crm_gold_activity', 'activity', 'gold', runtime_params['trigger_type'], runtime_params['attempt_no'])
    try:
        df = _select_activity(spark.table(phone['targets']['silver_table']), 'phonecall').unionByName(_select_activity(spark.table(appt['targets']['silver_table']), 'appointment'), allowMissingColumns=True)
        df = df.withColumn('gold_delete_flag', F.when(F.col('is_active')==False, F.lit(True)).otherwise(F.lit(False)))
        if not _table_exists(spark, target):
            df.filter(F.col('gold_delete_flag')==False).drop('gold_delete_flag').limit(0).write.format('delta').mode('overwrite').option('overwriteSchema','true').saveAsTable(target)
        delta = DeltaTable.forName(spark, target); cols = [c for c in df.columns if c != 'gold_delete_flag']; vals = {c:f'src.{c}' for c in cols}
        (delta.alias('tgt').merge(df.alias('src'), 'tgt.activity_id=src.activity_id')
            .whenMatchedDelete(condition='src.gold_delete_flag = true')
            .whenMatchedUpdate(condition='src.gold_delete_flag = false', set=vals)
            .whenNotMatchedInsert(condition='src.gold_delete_flag = false', values=vals).execute())
        spark.sql(f"CREATE OR REPLACE VIEW {phone['targets']['serving_view']} AS SELECT * FROM {target}")
        high_wm = datetime.now(timezone.utc).isoformat()
        rr.log_success(runtime_params['run_id'], high_wm, df.count(), df.filter(F.col('gold_delete_flag')==False).count(), 0)
        wm.upsert_current('activity', 'gold', 'run_timestamp', high_wm, runtime_params['run_id'])
    except Exception as e:
        rr.log_failure(runtime_params['run_id'], str(e)); raise
