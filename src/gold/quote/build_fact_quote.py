from src.gold.common_fact_builder import build_current_fact

def run(runtime_params: dict, project_root: str):
    build_current_fact(runtime_params, project_root, 'quote')
