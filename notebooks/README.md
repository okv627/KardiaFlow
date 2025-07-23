# Kardiaflow: Unified Health Data Pipeline

This project ingests synthetic healthcare data into a Databricks Lakehouse using **Delta Lake**, **Auto Loader**, and a scalable **medallion architecture** (Bronze → Silver → Gold). It supports two core domains:

- **Encounters & Patients** — clinical events and demographics  
- **Claims, Providers & Feedback** — billing, metadata, and satisfaction

---

## Raw Bootstrap

Test files are manually uploaded to **DBFS** or **ADLS Gen2**, then staged into structured raw zones using the following notebooks:

| Purpose             | Notebook                             | Description                                      |
|---------------------|---------------------------------------|--------------------------------------------------|
| Bootstrap raw zones | `bootstrap_raw.ipynb`                | Creates folder structure and copies seed files   |
| Move new files      | `move_new_files.ipynb`               | Moves new raw files into proper raw locations    |
| Reset environment   | `reset_kardia_environment.ipynb`     | Tears down and reinitializes entire environment  |

### Raw File Paths

| Dataset     | Raw Path                                 | Format  |
|-------------|-------------------------------------------|---------|
| Patients    | `dbfs:/kardia/raw/patients/`              | CSV     |
| Encounters  | `dbfs:/kardia/raw/encounters/`            | Avro    |
| Claims      | `dbfs:/kardia/raw/claims/`                | Parquet |
| Providers   | `dbfs:/kardia/raw/providers/`             | TSV     |
| Feedback    | `dbfs:/kardia/raw/feedback/`              | JSONL   |

---

## Bronze Ingestion

Raw files are ingested into **Bronze Delta tables** using **Auto Loader** (or **COPY INTO** for JSONL formats). Each table includes:

Auto Loader is used for structured formats like CSV, Parquet, and TSV where incremental discovery and schema evolution are important. In contrast, COPY INTO is used for the semistructured JSONL Feedback data, where SQL-based projection, type coercion, and optional field handling are required during load.

- Audit columns: `_ingest_ts`, `_source_file`
- Change Data Feed (CDF) enabled
- Partitioning and schema enforcement

| Dataset     | Format   | Loader       | Bronze Table                      |
|-------------|----------|--------------|-----------------------------------|
| Patients    | CSV      | Auto Loader  | `kardia_bronze.bronze_patients`   |
| Encounters  | Avro     | Auto Loader  | `kardia_bronze.bronze_encounters` |
| Claims      | Parquet  | Auto Loader  | `kardia_bronze.bronze_claims`     |
| Providers   | TSV      | Auto Loader  | `kardia_bronze.bronze_providers`  |
| Feedback    | JSONL    | COPY INTO    | `kardia_bronze.bronze_feedback`   |

---

## Validation (Bronze Only)

Validation is performed **immediately after Bronze ingestion**. Each Bronze notebook ends with a structured validation step to ensure data quality and readiness for Silver transformation.

Validation includes:

- Null checks on required fields  
- Primary key uniqueness  
- Row count comparison to previous loads  
- Schema profiling (e.g. distinct counts, column types)  
- Presence of audit columns (`_ingest_ts`, `_source_file`)

### Output Tables

Results are stored in Delta tables:

| Dataset     | Validation Table                                  |
|-------------|---------------------------------------------------|
| Patients    | `kardia_validation.bronze_patients_summary`       |
| Encounters  | `kardia_validation.bronze_encounters_summary`     |
| Claims      | `kardia_validation.bronze_claims_summary`         |
| Providers   | `kardia_validation.bronze_providers_summary`      |
| Feedback    | `kardia_validation.bronze_feedback_summary`       |

---

## Silver Transformation

Silver notebooks apply:

- **Deduplication** and **SCD logic**  
- **PHI masking**  
- **Stream-static joins**

| Dataset     | Method               | Silver Table                        |
|-------------|----------------------|-------------------------------------|
| Patients    | Batch SCD Type 1     | `kardia_silver.silver_patients`     |
| Encounters  | Continuous Streaming | `kardia_silver.silver_encounters`   |
| Claims      | SCD Type 1           | `kardia_silver.silver_claims`       |
| Providers   | SCD Type 2           | `kardia_silver.silver_providers`    |
| Feedback    | Append-only          | `kardia_silver.silver_feedback`     |

### Enriched Silver Views

| View Name                    | Description                                      |
|-----------------------------|--------------------------------------------------|
| `silver_encounters_enriched`| Encounters joined with patient demographics      |
| `silver_claims_enriched`    | Claims joined with current provider attributes   |
| `silver_feedback_enriched`  | Feedback joined with current provider metadata   |

---

## Gold KPIs

Gold notebooks generate business-level aggregations for analytics and dashboards:

| Table Name                    | Description                                                  |
|------------------------------|--------------------------------------------------------------|
| `gold_patient_lifecycle`     | Visit intervals, patient lifespan, new/returning flags       |
| `gold_claim_anomalies`       | Approval rates, denials, high-cost procedures               |
| `gold_provider_rolling_spend`| Daily spend and 7-day rolling KPIs for provider payments     |
| `gold_feedback_metrics`      | Satisfaction tags, comment analysis, sentiment scoring       |