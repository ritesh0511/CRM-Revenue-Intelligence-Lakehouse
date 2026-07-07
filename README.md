# CRM Revenue Intelligence Lakehouse

End-to-end Azure Lakehouse project for CRM Revenue Analytics built using Azure Databricks, Delta Lake, ADLS Gen2, Auto Loader, Synapse Link patterns, and Power BI serving models.

> ⚠️ This project uses synthetic CRM data and is intended solely to demonstrate enterprise-scale data engineering design patterns. No proprietary or customer data is included.

---

## Business Problem

CRM systems such as Microsoft Dynamics 365 and Dataverse are optimized for operational workloads but are not ideal for analytical reporting.

Business teams require trusted and analytics-ready datasets for:

- Revenue Pipeline Tracking
- Opportunity Conversion Analysis
- Quote Performance Monitoring
- Contract Revenue Reporting
- Sales Activity Analytics
- Historical Pipeline Trend Analysis

This project demonstrates how raw CRM operational data can be transformed into enterprise-grade analytical data products using a modern Lakehouse architecture.

---

## Solution Architecture

<img width="1024" height="1536" alt="crm_lakehouse_architecture" src="https://github.com/ritesh0511/CRM-Revenue-Intelligence-Lakehouse/blob/master/docs/crm_lakehouse_architecture.png" />


### End-to-End Flow

<img width="1024" height="1536" alt="CRM_Workflow_DAG" src="https://github.com/ritesh0511/CRM-Revenue-Intelligence-Lakehouse/blob/master/docs/CRM_Workflow_DAG.png" />

## Technology Stack

| Category | Technology |
|-----------|------------|
| Cloud Platform | Azure |
| Storage | ADLS Gen2 |
| Compute | Azure Databricks |
| Processing | PySpark |
| Lakehouse | Delta Lake |
| Ingestion | Databricks Auto Loader |
| Source Integration | Azure Synapse Link Pattern |
| Governance | Unity Catalog |
| Orchestration | Databricks Workflows |
| Reporting | Power BI |
| CI/CD | Databricks Bundles |

---

## Key Engineering Features

✅ Incremental ingestion using Auto Loader

✅ Bronze → Silver → Gold Medallion Architecture

✅ Delta MERGE based upserts

✅ Soft-delete processing

✅ Metadata-driven entity onboarding

✅ Data Quality Framework

✅ Quarantine Zone for bad records

✅ Watermark tracking

✅ Run registry control framework

✅ Historical snapshot generation

✅ Power BI serving layer

✅ Workflow orchestration

✅ CI/CD-ready deployment model

---

## Business Entities

### CRM Entities

- Opportunity
- Quote
- Contract
- Phone Call
- Appointment
- Pipeline Item

---

## Medallion Architecture

### Bronze Layer

Raw source-aligned ingestion layer preserving:

- Source records
- File lineage
- Operation type
- Delete indicators

### Silver Layer

Curated current-state layer applying:

- Data Quality checks
- Deduplication
- Soft-delete logic
- Delta MERGE upserts

### Gold Layer

Business-ready datasets powering analytics and reporting.

### Serving Layer

Power BI optimized views for reporting consumers.

---

## Data Quality Framework

Implemented quality controls:

- Mandatory field validation
- Numeric validation
- Schema validation
- Delete event validation

Invalid records are redirected to a quarantine framework for operational review.

---

## Operational Monitoring

### Control Tables

- pipeline_run_registry
- pipeline_watermark
- bad_records

### Monitoring Views

- Recent Pipeline Runs
- Failed Pipeline Runs
- Layer Freshness Monitoring
- Stale Watermark Detection
- Quarantine Summary
- Pipeline Health Dashboard

---

## Gold Data Products

### Fact Tables

- fact_opportunity
- fact_quote
- fact_contract
- fact_activity
- fact_pipeline_snapshot

### Serving Views

- vw_opportunity_pipeline
- vw_quote_conversion
- vw_contract_bookings
- vw_sales_activity
- vw_pipeline_snapshot
- vw_rep_scorecard
- vw_stale_deals

---

## Engineering Concepts Demonstrated

- Lakehouse Architecture
- Medallion Architecture
- Incremental Processing
- Metadata-Driven Pipelines
- Delta MERGE Operations
- Data Quality Engineering
- Soft Delete Framework
- Watermark Management
- Historical Snapshot Design
- Operational Observability
- Workflow Orchestration
- CI/CD for Data Platforms

---

## Repository Structure

├── conf/

├── src/

├── sql/

├── workflows/

├── tests/

├── docs/

│ ├── architecture/

│ ├── screenshots/

│ └── diagrams/

├── data/

│ └── synthetic/

└── README.md

---

## Screenshots

### Architecture Diagram

[Add architecture image]

### Workflow Orchestration

[Add Databricks workflow screenshot]

### Data Quality Monitoring

[Add quarantine screenshot]

### Power BI Dashboard

[Add dashboard screenshot]

---

## Future Enhancements

- Real-Time Streaming CDC
- Alerting Framework
- Data Lineage Visualization
- Semantic Model Layer
- ML-based Revenue Forecasting
- Credit Risk Intelligence Lakehouse

---

## Author

Ritesh Diwane

Data Engineer

Focused on Azure Databricks, Lakehouse Architecture, Data Engineering, and Analytics Platform Design.
