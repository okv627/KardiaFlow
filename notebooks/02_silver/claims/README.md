# Silver Layer: Claims, Providers & Feedback

Transforms raw Bronze data into refined, analytics-ready Delta tables using consistent, scalable logic. Each dataset applies appropriate deduplication, slowly changing dimension (SCD), or append patterns based on update behavior.

---

## Transformation Logic

| Dataset   | Method      | Output Table                       |
|-----------|-------------|------------------------------------|
| Claims    | SCD Type 1  | `kardia_silver.silver_claims`      |
| Providers | SCD Type 2  | `kardia_silver.silver_providers`   |
| Feedback  | Append-only | `kardia_silver.silver_feedback`    |

- **Claims**: Latest state per `claim_id`, derived from Bronze CDF with upserts by version.
- **Providers**: Tracks full historical change with close-old / insert-new logic by `provider_id`.
- **Feedback**: Deduplicated append using `feedback_id`; no claim linkage required.

---

## Enriched Joins

Snapshot-style joins create enriched views for downstream aggregations:

| View Name                    | Description                                    |
|-----------------------------|------------------------------------------------|
| `silver_claims_enriched`    | Claims joined with current provider metadata   |
| `silver_feedback_enriched`  | Feedback joined with current provider metadata |

All enriched views:
- Use LEFT JOIN to preserve base rows
- Join only to current (`is_current = TRUE`) provider records

---

## Triggers & Merge Patterns

| Dataset   | Trigger Type     | Merge Logic                          |
|-----------|------------------|--------------------------------------|
| Claims    | `availableNow`   | SCD1 merge by `claim_id`             |
| Providers | Snapshot Compare | SCD2 merge by `provider_id`          |
| Feedback  | Batch append     | Insert-only dedupe by `feedback_id`  |

Audit metadata (`_ingest_ts`, `_source_file`, `_batch_id`) is preserved across all tables.

---

## Downstream Targets

The Silver layer powers Gold KPIs including:

- `gold_claim_anomalies`
- `gold_provider_rolling_spend`
- `gold_feedback_metrics`

All are derived from Silver base tables and enriched joins.