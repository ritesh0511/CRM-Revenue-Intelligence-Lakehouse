# Operational Runbook

## First checks

1. Check `ops_control.vw_pipeline_health_summary`
2. Check `ops_control.vw_failed_pipeline_runs`
3. Check `ops_control.vw_stale_watermarks`
4. Check `ops_quarantine.vw_quarantine_summary`

## Common failure types

- Source path missing: verify Synapse Link export path and config.
- Schema drift: inspect Auto Loader schema location and Bronze schema.
- DQ failures: inspect `ops_quarantine.bad_records`.
- Merge conflicts: ensure `max_concurrent_runs = 1` and no overlapping maintenance.

## Rerun guidance

- Bronze rerun: safe with checkpoint intact.
- Silver rerun: safe if watermark is correct; for backfill, reset/change watermark intentionally.
- Gold rerun: can be rerun from Silver safely.
- Snapshot rerun: pass same `snapshot_ts` to remain idempotent.
