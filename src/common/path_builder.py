from typing import Dict, Any

def _join_path(base: str, subpath: str) -> str:
    return f"{base.rstrip('/')}/{subpath.lstrip('/')}"

def fq_table(catalog: str, schema: str, table: str) -> str:
    return f"{catalog}.{schema}.{table}"

def build_source_path(env_cfg: Dict[str, Any], entity_cfg: Dict[str, Any]) -> str:
    return _join_path(env_cfg['paths']['dataverse_base_path'], entity_cfg['source']['source_subpath'])

def build_checkpoint_path(env_cfg: Dict[str, Any], entity_cfg: Dict[str, Any]) -> str:
    return _join_path(env_cfg['paths']['checkpoint_base_path'], entity_cfg['bronze']['checkpoint_subpath'])

def build_schema_path(env_cfg: Dict[str, Any], entity_cfg: Dict[str, Any]) -> str:
    return _join_path(env_cfg['paths']['schema_base_path'], entity_cfg['bronze']['schema_subpath'])

def build_quarantine_path(env_cfg: Dict[str, Any], entity_cfg: Dict[str, Any]) -> str:
    return _join_path(env_cfg['paths']['quarantine_base_path'], entity_cfg['entity_name'])

def build_target_tables(env_cfg: Dict[str, Any], entity_cfg: Dict[str, Any]) -> Dict[str, str]:
    catalog = env_cfg['catalog']['name']
    targets = {
        'bronze_table': fq_table(catalog, env_cfg['catalog']['bronze_schema'], entity_cfg['bronze']['table_name']),
        'silver_table': fq_table(catalog, env_cfg['catalog']['silver_schema'], entity_cfg['silver']['table_name']),
        'gold_table': fq_table(catalog, env_cfg['catalog']['gold_schema'], entity_cfg.get('gold', {}).get('fact_table_name', f"fact_{entity_cfg['entity_name']}")),
        'serving_view': fq_table(catalog, env_cfg['catalog']['serving_schema'], entity_cfg.get('gold', {}).get('serving_view_name', f"vw_{entity_cfg['entity_name']}")),
        'pipeline_run_registry': fq_table(catalog, env_cfg['catalog']['ops_control_schema'], 'pipeline_run_registry'),
        'pipeline_watermark': fq_table(catalog, env_cfg['catalog']['ops_control_schema'], 'pipeline_watermark'),
        'bad_records': fq_table(catalog, env_cfg['catalog']['ops_quarantine_schema'], 'bad_records'),
    }
    snapshot_cfg = entity_cfg.get('gold', {}).get('snapshot', {})
    targets['snapshot_table'] = fq_table(catalog, env_cfg['catalog']['gold_schema'], snapshot_cfg['snapshot_fact_table_name']) if snapshot_cfg.get('enabled') else None
    targets['snapshot_serving_view'] = fq_table(catalog, env_cfg['catalog']['serving_schema'], snapshot_cfg['snapshot_serving_view_name']) if snapshot_cfg.get('enabled') else None
    return targets
