import argparse
from src.gold.opportunity.build_pipeline_snapshot import run

def parse_args():
    p=argparse.ArgumentParser()
    for a in ['env','entity_name','run_id','trigger_type','project_root']:
        p.add_argument(f'--{a}', required=True)
    p.add_argument('--parent_run_id', default=None)
    p.add_argument('--attempt_no', required=True, type=int)
    p.add_argument('--snapshot_ts', required=False)
    return p.parse_args()
if __name__=='__main__':
    a=parse_args(); run(vars(a), a.project_root)
