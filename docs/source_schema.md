# Source Schema Overview

This file supplements the data dictionary by outlining how raw source datasets
relate to one another structurally and semantically across different formats.

---

## 1. Entity Relationships

- **patients.csv** is the central table in the EHR dataset. It connects to:
  - `encounters.avro` via `patients.ID → encounters.PATIENT`
  - `claims.parquet` via `patients.ID → claims.PatientID`

- **claims.parquet** connects to:
  - `patients.csv` via `claims.PatientID → patients.ID`
  - `providers.tsv` via `claims.ProviderID → providers.ProviderID`

- **feedback.jsonl** connects to:
  - `encounters.avro` via `feedback.visit_id → encounters.ID`
  - `providers.tsv` via `feedback.provider_id → providers.ProviderID`

---
