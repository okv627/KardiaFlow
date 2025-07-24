# Bronze Ingestion: Encounters & Patients

This layer ingests raw patient and encounter records into Delta tables in the
`kardia_bronze` schema using Auto Loader with **Change Data Feed (CDF)** and audit
columns enabled. Configuration paths and settings are managed via `kflow.config.bronze_paths()`.

---

## Ingested Datasets

| Dataset    | Source Location                                                | Format | Loader Type     | Bronze Table                      |
|------------|----------------------------------------------------------------|--------|------------------|-----------------------------------|
| Patients   | `abfss://raw@kardiaadlsdemo.dfs.core.windows.net/patients/`   | CSV    | Auto Loader      | `kardia_bronze.bronze_patients`   |
| Encounters | `abfss://raw@kardiaadlsdemo.dfs.core.windows.net/encounters/` | Avro   | Auto Loader      | `kardia_bronze.bronze_encounters` |

---

## Features

- CDF enabled on all Bronze tables  
- Audit columns: `_ingest_ts`, `_source_file`, `_batch_id`  
- Config-driven schema, checkpoint, and quarantine paths  
- Patients ingested via **incremental batch** (`availableNow`)  
- Encounters ingested via **continuous stream** (`trigger = 30s`)  
- Explicit schema enforcement for both formats

---

## Notebooks

| Notebook                          | Target Table                      | Trigger Type     |
|----------------------------------|-----------------------------------|------------------|
| `01_bronze_patients_autoloader`  | `bronze_patients`                 | Incremental batch |
| `01_bronze_encounters_autoloader`| `bronze_encounters`               | Continuous stream |