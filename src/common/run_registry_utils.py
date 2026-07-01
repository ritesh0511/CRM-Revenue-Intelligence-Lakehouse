class RunRegistry:
    def __init__(self, spark, table_name: str):
        self.spark = spark
        self.table_name = table_name
    @staticmethod
    def _sql_safe(value):
        if value is None:
            return 'NULL'
        return "'" + str(value).replace("'", "''") + "'"
    def log_start(self, run_id, parent_run_id, pipeline_name, entity_name, layer_name, trigger_type, attempt_no):
        self.spark.sql(f"""
        INSERT INTO {self.table_name}
        (run_id,parent_run_id,pipeline_name,entity_name,layer_name,trigger_type,attempt_no,run_start_ts,run_end_ts,status,source_high_watermark,source_row_count,target_row_count,rows_quarantined,error_message,created_ts,updated_ts)
        VALUES ({self._sql_safe(run_id)},{self._sql_safe(parent_run_id)},{self._sql_safe(pipeline_name)},{self._sql_safe(entity_name)},{self._sql_safe(layer_name)},{self._sql_safe(trigger_type)},{attempt_no},current_timestamp(),NULL,'running',NULL,NULL,NULL,0,NULL,current_timestamp(),current_timestamp())
        """)
    def log_success(self, run_id, source_high_watermark, source_row_count, target_row_count, rows_quarantined=0):
        status_expr = "'partial_success'" if rows_quarantined and rows_quarantined > 0 else "'success'"
        self.spark.sql(f"""
        UPDATE {self.table_name}
        SET run_end_ts=current_timestamp(), status={status_expr}, source_high_watermark={self._sql_safe(source_high_watermark)},
            source_row_count={source_row_count if source_row_count is not None else 'NULL'},
            target_row_count={target_row_count if target_row_count is not None else 'NULL'}, rows_quarantined={rows_quarantined}, updated_ts=current_timestamp()
        WHERE run_id={self._sql_safe(run_id)}
        """)
    def log_failure(self, run_id, error_message):
        self.spark.sql(f"""
        UPDATE {self.table_name}
        SET run_end_ts=current_timestamp(), status='failed', error_message={self._sql_safe(str(error_message)[:4000])}, updated_ts=current_timestamp()
        WHERE run_id={self._sql_safe(run_id)}
        """)
