import pytest
from src.common.config_validator import validate_entity_config, ConfigValidationError

def test_ignore_deletes_fails():
    cfg = {
        'entity_name':'x',
        'source': {'source_subpath':'x/','file_format':'parquet','source_type':'dataverse_synapse_link','write_mode':'append_only','delete_handling':'ignore_deletes'},
        'bronze': {'table_name':'x_raw','checkpoint_subpath':'bronze/x/','schema_subpath':'bronze/x/','watermark_type':'source_high_watermark'},
        'silver': {'table_name':'x_curated','primary_key':'id','event_time_column':'modifiedon','delete_strategy':'soft_delete','active_flag_column':'is_active'}
    }
    with pytest.raises(ConfigValidationError):
        validate_entity_config(cfg)
