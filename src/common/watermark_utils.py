class WatermarkManager:
    def __init__(self, spark, table_name: str):
        self.spark = spark
        self.table_name = table_name
    @staticmethod
    def _sql_safe(value):
        if value is None:
            return 'NULL'
        return "'" + str(value).replace("'", "''") + "'"
    def upsert_current(self, entity_name, layer_name, watermark_type, watermark_value, run_id):
        self.spark.sql(f"""
        UPDATE {self.table_name}
        SET is_current=false, updated_ts=current_timestamp()
        WHERE entity_name={self._sql_safe(entity_name)} AND layer_name={self._sql_safe(layer_name)} AND is_current=true
        """)
        self.spark.sql(f"""
        INSERT INTO {self.table_name}
        (entity_name,layer_name,watermark_type,watermark_value,run_id,is_current,updated_ts)
        VALUES ({self._sql_safe(entity_name)},{self._sql_safe(layer_name)},{self._sql_safe(watermark_type)},{self._sql_safe(watermark_value)},{self._sql_safe(run_id)},true,current_timestamp())
        """)
