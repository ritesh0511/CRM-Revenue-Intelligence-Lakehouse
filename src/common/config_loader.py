from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, Any
import yaml
from src.common.config_validator import validate_env_config, validate_entity_config, validate_runtime_params, validate_cross_config
from src.common.path_builder import build_source_path, build_checkpoint_path, build_schema_path, build_quarantine_path, build_target_tables

@dataclass
class ResolvedConfig:
    env: str
    catalog_name: str
    schemas: Dict[str, str]
    entity_name: str
    pipeline_name: str
    source: Dict[str, Any]
    targets: Dict[str, str]
    paths: Dict[str, str]
    runtime: Dict[str, Any]
    features: Dict[str, Any]
    delete_strategy: Dict[str, Any]
    silver: Dict[str, Any]
    quality: Dict[str, Any]
    history: Dict[str, Any]
    gold: Dict[str, Any]
    def to_dict(self):
        return asdict(self)

class ConfigLoader:
    def __init__(self, project_root: str):
        self.project_root = Path(project_root)
    def _read_yaml(self, path: Path) -> Dict[str, Any]:
        if not path.exists():
            raise FileNotFoundError(f'Config file not found: {path}')
        with path.open('r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    def load_env_config(self, env: str):
        cfg = self._read_yaml(self.project_root / 'conf' / f'{env}.yml')
        validate_env_config(cfg)
        return cfg
    def load_entity_config(self, entity_name: str):
        cfg = self._read_yaml(self.project_root / 'conf' / 'entities' / f'{entity_name}.yml')
        validate_entity_config(cfg)
        return cfg
    def build_resolved_config(self, runtime_params: Dict[str, Any]) -> ResolvedConfig:
        validate_runtime_params(runtime_params)
        env_cfg = self.load_env_config(runtime_params['env'])
        entity_cfg = self.load_entity_config(runtime_params['entity_name'])
        validate_cross_config(env_cfg, entity_cfg, runtime_params)
        targets = build_target_tables(env_cfg, entity_cfg)
        trigger_mode = runtime_params.get('trigger_mode', env_cfg['runtime_defaults']['trigger_mode'])
        microbatch_interval = runtime_params.get('microbatch_interval', env_cfg['runtime_defaults'].get('microbatch_interval'))
        source_cfg = {
            'path': build_source_path(env_cfg, entity_cfg),
            'file_format': entity_cfg['source'].get('file_format', env_cfg['runtime_defaults']['default_file_format']),
            'source_type': entity_cfg['source']['source_type'],
            'write_mode': entity_cfg['source']['write_mode'],
            'delete_handling': entity_cfg['source']['delete_handling'],
            'operation_column': entity_cfg['source'].get('cdc', {}).get('operation_column'),
            'delete_values': entity_cfg['source'].get('cdc', {}).get('delete_values', []),
            'update_values': entity_cfg['source'].get('cdc', {}).get('update_values', []),
            'insert_values': entity_cfg['source'].get('cdc', {}).get('insert_values', []),
            'watermark_type': entity_cfg['bronze']['watermark_type'],
        }
        schemas = {
            'bronze': env_cfg['catalog']['bronze_schema'],
            'silver': env_cfg['catalog']['silver_schema'],
            'gold': env_cfg['catalog']['gold_schema'],
            'ops_control': env_cfg['catalog']['ops_control_schema'],
            'ops_quarantine': env_cfg['catalog']['ops_quarantine_schema'],
            'serving': env_cfg['catalog']['serving_schema'],
        }
        silver = {
            'table_name': entity_cfg['silver']['table_name'],
            'dedup_enabled': entity_cfg['silver'].get('dedup_enabled', True),
            'quarantine_enabled': entity_cfg['silver'].get('quarantine_enabled', env_cfg['features'].get('quarantine_enabled', True)),
            'primary_key': entity_cfg['silver']['primary_key'],
            'event_time_column': entity_cfg['silver']['event_time_column'],
            'delete_strategy': entity_cfg['silver']['delete_strategy'],
            'active_flag_column': entity_cfg['silver'].get('active_flag_column'),
        }
        runtime = {
            'run_id': runtime_params['run_id'],
            'parent_run_id': runtime_params.get('parent_run_id'),
            'trigger_type': runtime_params['trigger_type'],
            'attempt_no': runtime_params['attempt_no'],
            'trigger_mode': trigger_mode,
            'microbatch_interval': microbatch_interval,
            'backfill_from': runtime_params.get('backfill_from'),
            'backfill_to': runtime_params.get('backfill_to'),
            'snapshot_ts': runtime_params.get('snapshot_ts'),
        }
        features = {
            'schema_evolution_enabled': env_cfg['features'].get('schema_evolution_enabled', True),
            'quarantine_enabled': silver['quarantine_enabled'],
            'cdf_enabled': entity_cfg.get('history', {}).get('cdf_enabled', env_cfg['features'].get('cdf_enabled_by_default', True)),
            'data_quality_enabled': env_cfg['features'].get('data_quality_enabled', True),
        }
        return ResolvedConfig(
            env=env_cfg['env'], catalog_name=env_cfg['catalog']['name'], schemas=schemas,
            entity_name=entity_cfg['entity_name'], pipeline_name=f"crm_{entity_cfg['entity_name']}",
            source=source_cfg, targets=targets,
            paths={
                'checkpoint_path': build_checkpoint_path(env_cfg, entity_cfg),
                'schema_path': build_schema_path(env_cfg, entity_cfg),
                'quarantine_path': build_quarantine_path(env_cfg, entity_cfg),
            },
            runtime=runtime,
            features=features,
            delete_strategy={
                'write_mode': entity_cfg['source']['write_mode'],
                'delete_handling': entity_cfg['source']['delete_handling'],
                'silver_mode': entity_cfg['silver']['delete_strategy'],
                'active_flag_column': entity_cfg['silver'].get('active_flag_column'),
            },
            silver=silver,
            quality=entity_cfg.get('quality', {}),
            history=entity_cfg.get('history', {}),
            gold=entity_cfg.get('gold', {}),
        )
