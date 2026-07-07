# CRM Revenue Intelligence Lakehouse
# Low Level Design (LLD)

**Project:** CRM Revenue Intelligence Lakehouse

**Version:** 1.0

**Author:** Ritesh Diwane

**Platform:** Azure Databricks Lakehouse

---

# 1 Overview

## 1.1 Purpose

This document describes the detailed technical implementation of the CRM Revenue Intelligence Lakehouse.

The platform ingests Dynamics 365 CRM data through Synapse Link, processes data within Azure Databricks using Medallion Architecture, and serves curated datasets to Power BI.

---

# 2 Solution Components

| Layer | Technology |
|---------|------------|
| Source | Dynamics 365 CRM |
| Export | Azure Synapse Link |
| Storage | ADLS Gen2 |
| Processing | Databricks |
| Lakehouse | Delta Lake |
| Governance | Unity Catalog |
| Serving | SQL Warehouse |
| Reporting | Power BI |

---

# 3 Environment Design

## Development

```text
Workspace:
crm-dev-dbx

Storage:
crmdevadls

Catalog:
crm_dev
```

## UAT

```text
Workspace:
crm-uat-dbx

Storage:
crmuatadls

Catalog:
crm_uat
```

## Production

```text
Workspace:
crm-prod-dbx

Storage:
crmprodadls

Catalog:
crm_prod
```

---

# 4 ADLS Storage Design

## Container Structure

```text
raw
bronze
silver
gold
checkpoints
logs
archive
```

---

## Folder Structure

```text
raw/

├── opportunity/
├── quote/
├── contract/
├── pipeline_item/
├── phonecall/
└── appointment/

bronze/

├── opportunity/
├── quote/
├── contract/
├── activity/

silver/

├── opportunity/
├── quote/
├── contract/
├── activity/

gold/

├── mart_sales/
├── mart_activity/
└── mart_forecast/
```

---

# 5 Unity Catalog Design

## Catalog Structure

```text
crm_prod
```

---

## Schemas

```text
crm_prod.bronze

crm_prod.silver

crm_prod.gold
```

---

# 6 Databricks Workflow Design

## Workflow Name

```text
CRM_Revenue_Intelligence_Workflow
```

---

## Task Sequence

```text
Task 1
Opportunity Bronze Load

Task 2
Quote Bronze Load

Task 3
Contract Bronze Load

Task 4
PhoneCall Bronze Load

Task 5
Appointment Bronze Load

Task 6
Silver Transform

Task 7
Gold Mart Build

Task 8
Data Quality Checks

Task 9
Notification Task
```

---

# 7 Auto Loader Configuration

## Opportunity Ingestion

```python
spark.readStream \
.format("cloudFiles") \
.option("cloudFiles.format","parquet") \
.option("cloudFiles.schemaLocation",
"/checkpoints/opportunity/schema") \
.load(raw_path)
```

---

## Checkpoint Location

```text
abfss://checkpoints/opportunity/
```

---

## Trigger Strategy

```python
.trigger(processingTime="5 minutes")
```

---

# 8 Bronze Layer Design

## Purpose

Store source data exactly as received.

---

## Bronze Opportunity

### Table

```text
crm_prod.bronze.bronze_opportunity
```

### Partition

```text
ingestion_date
```

### Additional Columns

```text
source_file
ingestion_timestamp
batch_id
```

---

## Bronze Quote

```text
crm_prod.bronze.bronze_quote
```

---

## Bronze Contract

```text
crm_prod.bronze.bronze_contract
```

---

## Bronze Activity

```text
crm_prod.bronze.bronze_phonecall

crm_prod.bronze.bronze_appointment
```

---

# 9 Silver Layer Design

## Purpose

Produce validated and standardized datasets.

---

## Deduplication Strategy

Business Key:

```text
opportunityid
```

Latest Record:

```text
modifiedon
```

Logic:

```sql
ROW_NUMBER()
OVER(
PARTITION BY opportunityid
ORDER BY modifiedon DESC
)
```

---

## Standardization

### Data Types

```text
Date
Timestamp
Decimal
Integer
String
```

---

## Stage Mapping

```text
Open
In Progress
Quoted
Won
Lost
Closed
```

---

## Unified Activity Table

```text
silver_activity
```

Source:

```text
phonecall
appointment
```

Columns:

```text
activity_id
activity_type
activity_owner
activity_date
activity_status
related_opportunity
```

---

# 10 Delta Merge Strategy

## Merge Key

```text
opportunityid
```

---

## Merge Condition

```sql
MERGE INTO target t
USING source s

ON t.opportunityid=s.opportunityid

WHEN MATCHED THEN UPDATE

WHEN NOT MATCHED THEN INSERT
```

---

# 11 Gold Layer Design

## Star Schema

```text
                    dim_date
                        |
                        |
dim_sales_owner --- fact_opportunity --- dim_stage
                        |
                        |
                 dim_activity_type
```

---

# 12 Dimension Design

## dim_date

Columns

```text
date_key
date
month
quarter
year
weekday
```

---

## dim_sales_owner

Columns

```text
owner_key
owner_id
owner_name
region
department
```

---

## dim_stage

Columns

```text
stage_key
stage_name
stage_order
```

---

## dim_activity_type

Columns

```text
activity_type_key
activity_type
```

---

# 13 Fact Table Design

## fact_opportunity

Granularity

```text
One Row Per Opportunity
```

Columns

```text
opportunity_key
owner_key
stage_key
date_key

amount
probability
weighted_amount

created_date
expected_close_date
last_activity_date
```

---

## fact_quote

Granularity

```text
One Row Per Quote
```

Columns

```text
quote_id
opportunity_id
quote_amount
quote_status
quote_date
```

---

## fact_contract

Granularity

```text
One Row Per Contract
```

Columns

```text
contract_id
contract_value
contract_status
effective_date
```

---

## fact_activity

Granularity

```text
One Row Per Activity
```

Columns

```text
activity_id
activity_type
activity_date
activity_duration
owner_id
```

---

## fact_pipeline_snapshot

Granularity

```text
One Row Per Opportunity Per Day
```

Columns

```text
snapshot_date

opportunity_id

stage

amount

probability

weighted_amount
```

---

# 14 Data Quality Framework

## Rule 1

```text
OpportunityId cannot be null
```

---

## Rule 2

```text
Amount must be greater than zero
```

---

## Rule 3

```text
Expected Close Date >= Created Date
```

---

## Rule 4

```text
Owner ID cannot be null
```

---

## Rule 5

```text
No duplicate Opportunity IDs
```

---

# 15 Error Handling

## Quarantine Tables

```text
error_opportunity

error_quote

error_contract

error_activity
```

---

## Retry Strategy

```text
3 Retries

5 Minute Delay
```

---

# 16 Monitoring Design

## Metrics

```text
Rows Ingested

Rows Failed

Pipeline Runtime

Data Freshness

Cluster Utilization
```

---

## Monitoring Tools

```text
Databricks Workflows

Azure Monitor

Log Analytics
```

---

# 17 Security Design

## Authentication

```text
Microsoft Entra ID
```

---

## Authorization

```text
Unity Catalog RBAC
```

---

## Secrets

Stored in:

```text
Azure Key Vault
```

---

# 18 Backup & Recovery

## Recovery Point Objective

```text
15 Minutes
```

---

## Recovery Time Objective

```text
1 Hour
```

---

## Recovery Mechanisms

```text
Delta Time Travel

Storage Backup

Metadata Recovery
```

---

# 19 Power BI Design

## Dataset

```text
CRM Revenue Intelligence Dataset
```

---

## Dashboards

### Executive Dashboard

KPIs

```text
Pipeline Revenue

Won Revenue

Lost Revenue

Forecast Revenue
```

---

### Sales Funnel Dashboard

KPIs

```text
Opportunity Count

Quote Count

Contract Count

Conversion Rate
```

---

### Activity Dashboard

KPIs

```text
Calls Per Rep

Meetings Per Rep

Inactive Opportunities
```

---

# 20 Performance Optimization

## Delta Optimization

```sql
OPTIMIZE fact_opportunity
ZORDER BY(opportunity_id)
```

---

## Auto Optimize

```text
Enabled
```

---

## Auto Compaction

```text
Enabled
```

---

# 21 Scheduling Design

## Bronze Loads

```text
Every 5 Minutes
```

---

## Silver Loads

```text
Every 5 Minutes
```

---

## Gold Loads

```text
Every 10 Minutes
```

---

## Snapshot Generation

```text
Daily Midnight UTC
```

---

# 22 Deployment Strategy

```text
Developer

→ GitHub

→ Pull Request

→ GitHub Actions

→ Databricks Asset Bundle

→ DEV

→ UAT

→ PROD
```

---

# 23 Acceptance Criteria

## Ingestion

✓ All configured CRM entities loaded.

## Freshness

✓ Data available within 10 minutes.

## Reliability

✓ Exactly-once processing.

## Reporting

✓ Power BI dashboards refresh successfully.

## Security

✓ RBAC enforced.

---

# 24 Future Enhancements

Phase 2

- Customer 360
- AI Forecasting
- Copilot Integration
- Lead Scoring
- Sales Recommendations

---

# 25 Conclusion

This LLD provides the technical implementation details for building the CRM Revenue Intelligence Lakehouse using Dynamics 365, Azure Synapse Link, Databricks Auto Loader, Delta Lake, Unity Catalog, Databricks SQL Warehouse, and Power BI while adhering to enterprise-scale design principles.
