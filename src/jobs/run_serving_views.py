import argparse
from src.common.config_loader import ConfigLoader

def parse_args():
    p=argparse.ArgumentParser(); p.add_argument('--env', required=True); p.add_argument('--project_root', required=True); return p.parse_args()
if __name__=='__main__':
    args=parse_args(); spark=globals()['spark']
    # Main cross-fact serving views. Adjust join keys/columns for your CRM customization.
    catalog = ConfigLoader(args.project_root).load_env_config(args.env)['catalog']['name']
    spark.sql(f"""
    CREATE OR REPLACE VIEW {catalog}.serving.vw_rep_scorecard AS
    SELECT owner_id, COUNT(*) AS open_opportunity_count, SUM(estimated_value) AS open_pipeline_value, SUM(weighted_amount) AS weighted_pipeline_value
    FROM {catalog}.gold.fact_opportunity
    GROUP BY owner_id
    """)
    spark.sql(f"""
    CREATE OR REPLACE VIEW {catalog}.serving.vw_stale_deals AS
    SELECT * FROM {catalog}.gold.fact_opportunity
    WHERE modified_ts < current_timestamp() - INTERVAL 30 DAYS
    """)
