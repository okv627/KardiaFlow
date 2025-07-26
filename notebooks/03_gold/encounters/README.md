# Gold Layer: Patients & Encounters

This layer aggregates lifecycle metrics for individual patients based on encounter history. Useful for longitudinal analysis, resource planning, and utilization stratification.

---

## Output Table

| Table Name              | Description                                                |
|-------------------------|------------------------------------------------------------|
| `gold_patient_lifecycle`| Patient-level KPIs including visit patterns and age bands  |

---

## KPIs Computed

- `visit_count`: Total number of encounters per patient  
- `first_visit`, `last_visit`: Earliest and most recent encounter timestamps  
- `lifetime_days`: Days between first and last encounter  
- `avg_days_between_visits`: Lifetime span ÷ (visit_count – 1), where applicable  
- `classification`: `"new"` (1 visit) or `"returning"` (2+ visits)  
- `age_band`: Binned age group derived from `birth_year`

---

## Source

- Table: `silver_encounters_enriched`  
- Trigger: Full snapshot overwrite (daily), fast for small datasets  
  - In production, switch to `foreachBatch + MERGE` for incremental updates

---

## Use Cases

- Track patient engagement patterns  
- Segment populations by age and visit frequency  
- Feed downstream cost, risk, or care pathway models