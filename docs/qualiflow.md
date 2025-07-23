# Qualiflow

## A Practical Pattern for Transparent Data Trust

*Qualiflow adds a lightweight, structured summary to each dataset—capturing when it was refreshed, how it was built, whether it passed validation, and what privacy protections were applied.*

### Purpose

Details like validation results, lineage, and privacy enforcement are often buried in logs or scattered across systems. Qualiflow surfaces these signals at the point of use by attaching a clear, machine-readable summary to each dataset.

---

# Core Design

Qualiflow has two main layers: a **machine-readable trust summary** and a **human-facing trust indicator**.

## Trust Summary (Machine Layer)

> A structured JSON object generated during each Gold-table refresh, capturing key metadata such as freshness, validation results, schema contract status, privacy actions, and reproducibility context.

Each summary is stored in a centralized Delta table (e.g., `kardia_meta.qualiflow_summaries`), keyed by `dataset_id` and timestamp. Datasets may reference the summary via a fingerprint column (e.g., `_qf_fingerprint`).

### Included Metadata

- **Lineage**: Input tables, source systems, job ID, Git SHA
- **Freshness**: Ingestion timestamp, load timestamp, pipeline latency, SLA status
- **Validation**: Test results (pass/fail), data quality score, optional metrics (e.g., null %, value drift)
- **Schema Contract**: Schema version or hash, contract compliance
- **Privacy**: Masked fields, policy ID, percent masked
- **Fingerprint**: Deterministic hash of Delta version and code version
- **Ownership**: Generation timestamp, pipeline ID, team contact

> This metadata powers dynamic trust indicators in dashboards, notebooks, and validation reports—without requiring direct access to pipeline internals.

**Examples**  
- `latency_seconds = 20` vs SLA `900` → **Freshness: Green**  
- `tests_failed = 0`, `dq_score = 98.7` → **Quality: A**  
- `contract.status = OK` → **Schema v3.1**  
- `privacy.masked_fields` present → **Privacy: Masked**

---

## Trust Indicators (Human Layer)

> Lightweight UI signals embedded in dashboards, notebooks, or queries—designed to surface freshness, quality, privacy, and reproducibility context clearly and concisely.

Trust indicators translate metadata into intuitive visuals using progressive disclosure. Business users see quick status indicators; technical users can drill into validation details, lineage, and masking logic.

### Common Elements

- **Status Badge**: Green/yellow/red based on SLOs or test results  
- **Freshness Label**: `"Fresh (15m)"`, `"Stale"`, or `"Updated 2h ago"`  
- **Quality Summary**: `"98% quality (12/12 tests passed)"`  
- **Privacy Notice**: `"PII masked: Yes (2 fields)"`, with policy ID  
- **Schema Tag**: `"Schema v3.1"` or `"Contract: Compliant"`  
- **Fingerprint**: Short hash for traceability  
- **Lineage Link**: Optional UI for job and table ancestry

---

## Implementation in KardiaFlow

Within KardiaFlow, Qualiflow is implemented in Gold-layer refresh pipelines. Each run generates a standardized JSON summary using existing pipeline artifacts—such as `_ingest_ts` timestamps, Delta Lake table versions, Git commit SHAs, and PHI masking logic.

The summary is written to the `kardia_meta.qualiflow_summaries` table and optionally linked to Gold tables using a `_qf_fingerprint` column. Trust indicators in notebooks and dashboards pull directly from this metadata to render context-aware freshness labels, quality scores, schema tags, and privacy indicators.

SLOs for freshness, validation coverage, and masking requirements are evaluated at runtime. A human-readable trust receipt is also generated for auditability and compliance.

All trust summary logic is implemented via a shared helper (`emit_qualiflow_summary`) and modular rendering utilities to ensure consistency and reduce duplication.

---

## Sample Trust Summary

A compact JSON object generated at refresh time, capturing core trust signals and stored alongside the dataset.

```json
{
  "dataset_id": "gold_patient_lifecycle",
  "lineage": {
    "parents": [
      "silver_encounters_enriched_v5",
      "silver_patients_v3"
    ],
    "job_run_id": "pipeline_run_2025-07-22T14:33:02Z",
    "code_version": "git:main@a1b2c3d"
  },
  "freshness": {
    "data_timestamp": "2025-07-22T14:32:50Z",
    "loaded_at": "2025-07-22T14:33:10Z",
    "latency_seconds": 20,
    "sla": "15m",
    "status": "OK"
  },
  "quality": {
    "tests_passed": 28,
    "tests_failed": 0,
    "dq_score": 98.7,
    "dimensions": {
      "completeness": 1.0,
      "accuracy": 0.987,
      "consistency": 0.99
    }
  },
  "contract": {
    "schema_version": "3.1",
    "schema_hash": "sha256:abcdef123456...",
    "contract_id": "encounters_contract_v2",
    "status": "OK"
  },
  "privacy": {
    "masked_fields": ["SSN", "LAST_NAME"],
    "masking_policy_id": "PHI_Mask_v1",
    "masked_pct": 12.4
  },
  "fingerprint": "qf:sha256:4f67e2a8c9...",
  "owner": "data-eng-team@mycompany.com",
  "generated_at": "2025-07-22T14:33:15Z"
}
```

---