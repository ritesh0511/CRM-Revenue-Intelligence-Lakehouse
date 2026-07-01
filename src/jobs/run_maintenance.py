import argparse

def parse_args():
    p=argparse.ArgumentParser(); p.add_argument('--catalog', required=True); return p.parse_args()
if __name__=='__main__':
    args=parse_args(); spark=globals()['spark']; c=args.catalog
    for table in [f'{c}.silver.opportunity_curated', f'{c}.gold.fact_opportunity', f'{c}.gold.fact_quote', f'{c}.gold.fact_contract', f'{c}.gold.fact_activity', f'{c}.gold.fact_pipeline_snapshot']:
        try:
            spark.sql(f'OPTIMIZE {table}')
            spark.sql(f'VACUUM {table} RETAIN 168 HOURS')
        except Exception as e:
            print(f'Skipping maintenance for {table}: {e}')
