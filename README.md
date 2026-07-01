# CRM Revenue Intelligence Lakehouse

Production-grade Azure Databricks lakehouse project for Dynamics 365 / Dataverse CRM pipeline analytics.

## Architecture

```text
Dynamics 365 / Dataverse
  -> Azure Synapse Link for Dataverse
  -> ADLS Gen2
  -> Databricks Auto Loader
  -> Delta Bronze
  -> Delta Silver Current-State
  -> Delta Gold Facts + Snapshots
  -> Databricks SQL Warehouse
  -> Power BI
```

## What is included

- Config-driven pipeline framework
- Environment and entity YAML configs
- Generic Bronze ingestion using Auto Loader
- Generic Silver current-state merge with DQ, quarantine, and soft deletes
- Gold facts for opportunity, quote, contract, activity
- Pipeline snapshot fact
- Serving views
- Pipeline run registry and watermark tables
- Basic observability SQL views
- Databricks bundle YAML for workflows
- Unit test scaffolding

## Important before deployment

Update these placeholders before running:

- `conf/dev.yml`, `conf/uat.yml`, `conf/prod.yml`
- ADLS storage account/container paths
- Synapse Link entity subpaths
- CRM column names in entity YAML files
- Databricks workspace repo path in `databricks.yml`
- cluster node type / runtime version

## Deployment

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

Run SQL bootstrap first:

```sql
-- sql/bootstrap/001_platform_bootstrap.sql
```

## Recommended execution order

1. Configure Synapse Link and confirm ADLS landing paths
2. Run platform bootstrap SQL
3. Deploy bundle to dev
4. Run `wf_crm_sales_current_pipeline_dev`
5. Validate Bronze/Silver/Gold tables
6. Validate serving views from Databricks SQL Warehouse
7. Promote to UAT/Prod using bundle targets
