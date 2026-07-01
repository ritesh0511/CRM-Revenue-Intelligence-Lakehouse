from typing import Dict, Any, List

class ConfigValidationError(Exception):
    pass

def _require(cfg: Dict[str, Any], keys: List[str], cfg_name: str) -> None:
    for key in keys:
        cur = cfg
        for part in key.split('.'):
            if not isinstance(cur, dict) or part not in cur:
                raise ConfigValidationError(f"Missing required key '{key}' in {cfg_name} config")
            cur = cur[part]

def validate_env_config(env_cfg: Dict[str, Any]) -> None:
    required = [
        'env','catalog.name','catalog.bronze_schema','catalog.silver_schema','catalog.gold_schema',
        'catalog.ops_control_schema','catalog.ops_quarantine_schema','catalog.serving_schema',
        'storage.storage_account','storage.raw_container','storage.checkpoint_container','storage.schema_container',
        'storage.quarantine_container','paths.dataverse_base_path','paths.checkpoint_base_path',
        'paths.schema_base_path','paths.quarantine_base_path','runtime_defaults.trigger_mode',
        'runtime_defaults.default_file_format'
    ]
    _require(env_cfg, required, 'environment')
    allowed = {'available_now','micro_batch','continuous'}
    if env_cfg['runtime_defaults']['trigger_mode'] not in allowed:
        raise ConfigValidationError(f"Invalid trigger_mode. Allowed: {sorted(allowed)}")

def validate_entity_config(entity_cfg: Dict[str, Any]) -> None:
    required = [
        'entity_name','source.source_subpath','source.file_format','source.source_type','source.write_mode',
        'source.delete_handling','bronze.table_name','bronze.checkpoint_subpath','bronze.schema_subpath',
        'bronze.watermark_type','silver.table_name','silver.primary_key','silver.event_time_column',
        'silver.delete_strategy'
    ]
    _require(entity_cfg, required, 'entity')
    if entity_cfg['source']['write_mode'] not in {'append_only','in_place_update'}:
        raise ConfigValidationError('Invalid source.write_mode')
    if entity_cfg['source']['delete_handling'] != 'process_delete_events':
        raise ConfigValidationError("Only process_delete_events is allowed for production-safe design")
    if entity_cfg['silver']['delete_strategy'] not in {'soft_delete','hard_delete'}:
        raise ConfigValidationError('Invalid silver.delete_strategy')
    if entity_cfg['silver']['delete_strategy'] == 'soft_delete' and not entity_cfg['silver'].get('active_flag_column'):
        raise ConfigValidationError("active_flag_column required for soft_delete")

def validate_runtime_params(runtime_params: Dict[str, Any]) -> None:
    for k in ['env','entity_name','run_id','trigger_type','attempt_no']:
        if k not in runtime_params or runtime_params[k] in (None, ''):
            raise ConfigValidationError(f"Missing runtime parameter {k}")
    if runtime_params['trigger_type'] not in {'scheduled','manual','retry','backfill'}:
        raise ConfigValidationError('Invalid trigger_type')
    if not isinstance(runtime_params['attempt_no'], int) or runtime_params['attempt_no'] < 1:
        raise ConfigValidationError('attempt_no must be integer >= 1')
    if runtime_params['trigger_type'] == 'backfill' and (not runtime_params.get('backfill_from') or not runtime_params.get('backfill_to')):
        raise ConfigValidationError('backfill_from and backfill_to required when trigger_type=backfill')

def validate_cross_config(env_cfg, entity_cfg, runtime_params) -> None:
    if env_cfg['env'] != runtime_params['env']:
        raise ConfigValidationError('Runtime env does not match env config')
    if entity_cfg['entity_name'] != runtime_params['entity_name']:
        raise ConfigValidationError('Runtime entity_name does not match entity config')
