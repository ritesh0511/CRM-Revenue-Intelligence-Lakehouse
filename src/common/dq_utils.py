from typing import List
from pyspark.sql import functions as F

def apply_mandatory_column_checks(df, mandatory_columns: List[str]):
    reasons = []
    for c in mandatory_columns:
        if c in df.columns:
            reasons.append(F.when(F.col(c).isNull() | (F.trim(F.col(c).cast('string')) == ''), F.lit(f'MANDATORY_NULL:{c}')))
        else:
            reasons.append(F.lit(f'MISSING_COLUMN:{c}'))
    return df.withColumn('dq_failure_reasons', F.array_remove(F.array(*reasons), F.lit(None)) if reasons else F.array())

def apply_non_negative_checks(df, non_negative_columns: List[str]):
    reasons = []
    for c in non_negative_columns:
        if c in df.columns:
            reasons.append(F.when(F.col(c).isNotNull() & (F.col(c).cast('double') < F.lit(0)), F.lit(f'NEGATIVE_VALUE:{c}')))
    if not reasons:
        return df
    return df.withColumn('dq_failure_reasons', F.array_union(F.coalesce(F.col('dq_failure_reasons'), F.array()), F.array_remove(F.array(*reasons), F.lit(None))))

def split_valid_invalid(df):
    invalid_df = df.filter(F.size(F.col('dq_failure_reasons')) > 0)
    valid_df = df.filter(F.size(F.col('dq_failure_reasons')) == 0)
    return valid_df, invalid_df

def build_bad_records_df(invalid_df, run_id, entity_name, layer_name, record_key_col):
    record_key = F.col(record_key_col).cast('string') if record_key_col in invalid_df.columns else F.lit(None).cast('string')
    source_file = F.col('bronze_source_file_path') if 'bronze_source_file_path' in invalid_df.columns else F.lit(None).cast('string')
    return (invalid_df
        .withColumn('quarantine_id', F.expr('uuid()'))
        .withColumn('run_id', F.lit(run_id))
        .withColumn('entity_name', F.lit(entity_name))
        .withColumn('layer_name', F.lit(layer_name))
        .withColumn('record_key', record_key)
        .withColumn('failed_rule_name', F.concat_ws('|', F.col('dq_failure_reasons')))
        .withColumn('failure_reason', F.concat_ws('|', F.col('dq_failure_reasons')))
        .withColumn('raw_record', F.to_json(F.struct(*invalid_df.columns)))
        .withColumn('source_file_path', source_file)
        .withColumn('status', F.lit('quarantined'))
        .withColumn('quarantined_ts', F.current_timestamp())
        .select('quarantine_id','run_id','entity_name','layer_name','record_key','failed_rule_name','failure_reason','raw_record','source_file_path','status','quarantined_ts'))
