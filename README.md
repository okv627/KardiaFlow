# Kardiaflow: Azure-Based Healthcare Data Platform

## Scenario

Kardiaflow simulates a real-world healthcare data platform built on Azure Databricks and Delta Lake. It demonstrates a modular, streaming-capable ETL architecture that handles structured (CSV, Avro, TSV) and semi-structured (JSON) healthcare datasets using a medallion design pattern.

The pipeline ingests raw files into Bronze Delta tables using Auto Loader, applies data masking and CDC logic in the Silver layer with Delta Change Data Feed (CDF), and materializes analytics-ready Gold views for reporting and dashboards.

### Architecture Overview

The following diagram illustrates the end-to-end data flow across ingestion, transformation, validation, and secure storage:

![Kardiaflow Architecture](https://raw.githubusercontent.com/okv627/Kardiaflow/master/docs/assets/kflow_lineage.png?v=2)

### Data Sources

| Dataset            | Format  |
|--------------------|---------|
| Patients           | CSV     |
| Encounters         | Avro    |
| Claims             | Parquet |
| Providers          | TSV     |
| Feedback           | JSON    |

---

## Key Features

- **Streaming Ingestion** via Databricks Auto Loader
- **PHI Masking** in the Silver layer (e.g., SSN, names, address fields)
- **Delta CDF** for change tracking, deduplication, and SCD1/SCD2 joins
- **Gold Views** with monthly rollups, gender distributions, and claims KPIs
- **Modular Notebook Structure** aligned to Medallion architecture (Raw - Bronze - Silver - Gold)
- **Teardown Scripts** to delete all Azure resources and DBFS files safely
- **Row-Level Traceability** via `_ingest_ts`, `_source_file`, and `kardia_meta.bronze_qc`

---

## Security & Compliance
- All Delta tables and raw files are stored in Databricks File System (DBFS), which is encrypted at rest.
- All traffic between the cluster and storage is secured via TLS-encrypted HTTPS.
- No real PHI is used — all data is synthetic and generated for simulation purposes only.
