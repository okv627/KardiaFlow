# Bronze Ingestion: Claims, Providers & Feedback

This layer ingests raw files into Delta tables under the `kardia_bronze` schema using either Auto Loader or COPY INTO, depending on the dataset structure and ingestion needs. All tables include **Change Data Feed (CDF)** and **audit columns**. Each dataset has its own dedicated notebook, schema definition, and checkpoint path, driven by `kflow.config.bronze_paths()`.

---

## Ingested Datasets

| Dataset   | Source Location                              | Format    | Loader Type | Bronze Table                     |
|-----------|-----------------------------------------------|-----------|-------------|----------------------------------|
| Claims    | `dbfs:/kardia/raw/claims/`                   | Parquet   | Auto Loader | `kardia_bronze.bronze_claims`    |
| Providers | `abfss://raw@kardiaadlsdemo.../providers/`   | TSV       | Auto Loader | `kardia_bronze.bronze_providers` |
| Feedback  | `abfss://raw@kardiaadlsdemo.../feedback/`    | JSONL     | COPY INTO   | `kardia_bronze.bronze_feedback`  |

---

## Loader Strategy

- **Auto Loader** is used for structured tabular datasets (CSV, TSV, Parquet) with known schemas and expected evolution over time. It supports incremental ingestion, schema tracking, and CDF compatibility, making it ideal for operational datasets like Claims and Providers.
- **COPY INTO** is used for semistructured formats like JSONL, where ingestion requires SQL-based projection, type coercion, and conditional field handling. Feedback records include optional fields (e.g., `tags`, `metadata`, `source`), making COPY INTO better suited for schema-on-read with runtime control.

---

## Features

- CDF enabled on all Bronze tables  
- Audit columns: `_ingest_ts`, `_source_file`, `_batch_id`  
- Auto Loader in `availableNow` mode for batch-style ingestion  
- Schema evolution enabled where supported  
- Config-driven checkpointing, bad record paths, and schema storage  
- Explicit schema enforcement for TSV/JSONL (Parquet uses embedded schema)

---

## Notebooks

| Notebook                          | Target Table                      | Notes                            |
|----------------------------------|-----------------------------------|----------------------------------|
| `01_bronze_claims_autoloader`    | `bronze_claims`                   | Parquet from DBFS                |
| `01_bronze_providers_autoloader`| `bronze_providers`                | TSV from ADLS, tab-delimited     |
| `01_bronze_feedback_copy_into`   | `bronze_feedback`                 | JSONL from ADLS with field casting and projection |

---

## Post-Ingestion Validation

After each Bronze write, a matching `01_validate_bronze_<dataset>.ipynb` logs key quality metrics to the `kardia_validation.<table>_summary` tables.

Example:
```sql
SELECT * FROM kardia_validation.bronze_claims_summary ORDER BY validation_run_ts DESC;
```

---