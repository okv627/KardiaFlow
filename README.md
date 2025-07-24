# Kardiaflow: Azure-Based Healthcare Data Platform

## Scenario

Kardiaflow simulates a real-world healthcare data platform built on Azure Databricks and Delta Lake. It demonstrates a modular, streaming-capable ETL architecture that handles structured (CSV, Avro, TSV) and semi-structured (JSON) healthcare datasets using a medallion design pattern.

The pipeline ingests raw files into Bronze Delta tables using Auto Loader, applies data masking and CDC logic in the Silver layer with Delta Change Data Feed (CDF), and materializes analytics-ready Gold views for reporting and dashboards.



## Architecture Overview

The following diagram illustrates the end-to-end data flow, including ingestion, transformation, validation, and storage layers:

![Kardiaflow Architecture](https://raw.githubusercontent.com/okv627/Kardiaflow/master/docs/assets/kflow_lineage.png?v=2)



## Key Features

**Multi-Domain Simulation**  
&nbsp;&nbsp;&nbsp;&nbsp;• *Clinical*: Patients, Encounters  
&nbsp;&nbsp;&nbsp;&nbsp;• *Billing & Feedback*: Claims, Providers, Feedback

**Flexible Ingestion Framework**  
&nbsp;&nbsp;&nbsp;&nbsp;• Structured formats (CSV, Avro, Parquet, TSV) via Auto Loader  
&nbsp;&nbsp;&nbsp;&nbsp;• Semi-structured JSONL via COPY INTO  
&nbsp;&nbsp;&nbsp;&nbsp;• All Bronze tables include `_ingest_ts`, `_source_file`, and enable Delta Change Data Feed (CDF)  


**Robust Silver-Layer Transformations**  
&nbsp;&nbsp;&nbsp;&nbsp;• Deduplication, PHI masking**, SCD Type 1/2  
&nbsp;&nbsp;&nbsp;&nbsp;• Supports streaming and batch upserts  
&nbsp;&nbsp;&nbsp;&nbsp;• Stream-static joins yield enriched Silver views  


**Business-Ready Gold KPIs**  
&nbsp;&nbsp;&nbsp;&nbsp;• Lifecycle metrics, spend trends, claim anomalies, feedback sentiment  
&nbsp;&nbsp;&nbsp;&nbsp;• Outputs structured fact tables for analytics  


**Automated Data Validation**  
&nbsp;&nbsp;&nbsp;&nbsp;• `99_smoke_checks.py` tests row counts, nulls, duplicates, and schema contracts  
&nbsp;&nbsp;&nbsp;&nbsp;• Logs results to `kardia_validation.smoke_results` (Delta)  


**Modular Notebook Design**  
&nbsp;&nbsp;&nbsp;&nbsp;• One notebook per dataset and medallion layer  
&nbsp;&nbsp;&nbsp;&nbsp;• Clear separation: Raw → Bronze → Silver → Gold  


**Reproducible Infrastructure-as-Code**  
&nbsp;&nbsp;&nbsp;&nbsp;• Deploys all Azure services via Bicep + Azure CLI  
&nbsp;&nbsp;&nbsp;&nbsp;• Secrets managed via Databricks CLI v0  
&nbsp;&nbsp;&nbsp;&nbsp;• Safe teardown with `infra/teardown.sh`



## Setting Up the Infrastructure

To deploy the Kardiaflow environment in Azure, follow the step-by-step instructions in:

[`infra/README.md`](infra/README.md) — *Infrastructure Deployment Guide*

> ⚠️ All setup commands must be run from the **project root**.



## Technology Stack

| Layer        | Tool/Service                  |
|--------------|-------------------------------|
| Cloud        | Azure                         |
| Storage      | ADLS Gen2                     |
| Compute      | Azure Databricks              |
| ETL Engine   | Apache Spark (Structured)     |
| Metadata     | Delta Lake + Change Data Feed |
| Infra-as-Code| Bicep + Azure CLI             |
| Validation   | PySpark + Delta               |



## Security & Compliance
- All Delta tables and raw files are stored in Databricks File System (DBFS), which is encrypted at rest.
- All traffic between the cluster and storage is secured via TLS-encrypted HTTPS.
- No real PHI is used — all data is synthetic and generated for simulation purposes only.
