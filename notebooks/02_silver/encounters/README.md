# Silver Layer: Patients & Encounters

Transforms Bronze records into clean, queryable Silver Delta tables using CDC logic (SCD‑1), PHI masking, and enrichment joins. Output is continuously updated for streaming tables and batched for patient data.

---

## Silver Tables

| Table Name                          | Description                                                       |
|------------------------------------|-------------------------------------------------------------------|
| `silver_patients`                  | Latest non-PHI demographics with birth year and masked identifiers |
| `silver_encounters`                | Deduplicated, timestamped clinical events with CDF-based updates   |
| `silver_encounters_enriched`       | Streaming join of encounters with patient demographics             |

---

## Patients

- **Trigger**: One-shot incremental batch from Bronze CDF
- **Pattern**: Deduplicate by `_commit_version`, mask PHI fields, derive `birth_year`
- **Join Logic**: Static dimension; used for enriching encounter streams

---

## Encounters

- **Trigger**: Continuous stream (every 30 seconds) with Auto Loader + Delta CDF
- **Pattern**: Upsert via MERGE using streaming `foreachBatch`
- **Enrichment**: Left join with `silver_patients` to produce `silver_encounters_enriched`

---

## Privacy Handling

The `silver_patients` table masks sensitive fields:
- `first`, `last`, `ssn`, `drivers`, `passport`, `birthplace`

These columns are explicitly nulled at write time to prevent exposure.

---

## Output Format

All Silver tables are Delta Lake managed and optimized for downstream Gold KPIs, joining, and analytics. Streaming logic ensures updates are applied incrementally, with schema evolution support.