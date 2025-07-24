# Kardiaflow: Unified Health Data Pipeline

This project ingests synthetic healthcare data into a Databricks Lakehouse using
**Delta Lake**, **Auto Loader**, and a **medallion architecture**. It supports two core domains:

- **Encounters & Patients** — clinical events and demographics  
- **Claims, Providers & Feedback** — billing, metadata, and satisfaction

Test files are manually uploaded to **ADLS Gen2**, where they are automatically
discovered by Auto Loader pipelines.

### Raw File Paths

| Dataset     | Raw Path                                                    | Format  |
|-------------|-------------------------------------------------------------|---------|
| Patients    | `abfss://raw@kardiaadlsdemo.dfs.core.windows.net/patients/` | CSV     |
| Encounters  | `abfss://raw@kardiaadlsdemo.dfs.core.windows.net/encounters/`| Avro    |
| Claims      | `abfss://raw@kardiaadlsdemo.dfs.core.windows.net/claims/`   | Parquet |
| Providers   | `abfss://raw@kardiaadlsdemo.dfs.core.windows.net/providers/`| TSV     |
| Feedback    | `abfss://raw@kardiaadlsdemo.dfs.core.windows.net/feedback/` | JSONL   |

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

---

## Validation

All data quality checks are handled by a single, lightweight test script: `99_smoke_checks.py`.
It runs structured smoke tests across Bronze, Silver, and Gold layers with no external dependencies.

- **Bronze:** Verifies row count > 0, primary key is non-null and unique, and optionally checks `_ingest_ts` freshness  
- **Silver:** Asserts that required columns exist (contract tests)  
- **Gold:** Ensures key columns like `patient_id` and `avg_score` are not null  
- **Results:** All checks are logged with status, metric name, and value to a persistent Delta table (`kardia_validation.smoke_results`)