# Data Dictionary: Kardiaflow Synthetic Healthcare Dataset

## Source Files

- `dbfs:/kardia/raw/claims/claims.parquet`
- `dbfs:/kardia/raw/claims/providers.tsv`
- `dbfs:/kardia/raw/ehr/patients.csv`
- `dbfs:/kardia/raw/ehr/encounters.avro`
- `dbfs:/kardia/raw/feedback/feedback.json`

---

## File: `claims.parquet`

| Field Name             | Type     | Description                                                             |
|------------------------|----------|-------------------------------------------------------------------------|
| `ClaimID`              | UUID     | Unique identifier for each insurance claim                              |
| `PatientID`            | UUID     | Foreign key to patient                                                  |
| `ProviderID`           | UUID     | Foreign key to provider                                                 |
| `ClaimAmount`          | Float    | Amount in USD                                                           |
| `ClaimDate`            | Date     | Date submitted (YYYY-MM-DD)                                             |
| `DiagnosisCode`        | String   | ICD or SNOMED-style code                                                |
| `ProcedureCode`        | String   | CPT or similar procedure code                                           |
| `ClaimStatus`          | Enum     | Approved, Denied, Pending                                               |
| `ClaimType`            | Enum     | Inpatient, Outpatient, Emergency, Routine                               |
| `ClaimSubmissionMethod`| Enum     | Online, Paper, Phone                                                    |

---

## File: `providers.tsv`

| Field Name             | Type     | Description                                                             |
|------------------------|----------|-------------------------------------------------------------------------|
| `ProviderID`           | UUID     | Unique identifier for provider                                          |
| `ProviderSpecialty`    | String   | Medical specialty (e.g., Oncology, Pediatrics)                          |
| `ProviderLocation`     | String   | City/State/Region                                                       |

---

## File: `patients.csv`

| Field Name   | Type   | Description                          |
|--------------|--------|--------------------------------------|
| `ID`         | UUID   | Unique patient ID                    |
| `BIRTHDATE`  | Date   | Date of birth                        |
| `DEATHDATE`  | Date   | If deceased, date of death           |
| `SSN`        | String | Synthetic SSN                        |
| `DRIVERS`    | String | Driver’s License                     |
| `PASSPORT`   | String | Passport ID                          |
| `PREFIX`     | String | Mr., Ms., Dr., etc.                  |
| `FIRST`      | String | First name                           |
| `LAST`       | String | Last name                            |
| `MARITAL`    | String | Marital status                       |
| `RACE`       | String | Race                                 |
| `ETHNICITY`  | String | Ethnicity                            |
| `GENDER`     | String | Gender (M/F)                         |
| `BIRTHPLACE` | String | City/State of birth                  |
| `ADDRESS`    | String | Street address                       |

---

## File: `encounters.avro`

| Field Name         | Type   | Description                              |
|--------------------|--------|------------------------------------------|
| `ID`               | UUID   | Unique identifier for the encounter      |
| `DATE`             | Date   | Encounter date                           |
| `PATIENT`          | UUID   | Foreign key referencing `ID` in `patients.csv` |
| `CODE`             | String | Encounter type code                      |
| `DESCRIPTION`      | String | Human-readable encounter description     |
| `REASONCODE`       | String | Reason code for the visit                |
| `REASONDESCRIPTION`| String | Description of the visit reason          |

---

## File: `feedback.jsonl`

| Field Name           | Type            | Description                                                                     |
|----------------------|-----------------|---------------------------------------------------------------------------------|
| `feedback_id`        | UUID            | Unique identifier for each feedback record                                      |
| `provider_id`        | UUID            | Foreign key referencing `ProviderID` in the `providers.tsv` dataset                                          |
| `timestamp`          | ISO 8601        | Date and time when the feedback was submitted                                   |
| `visit_id`           | UUID            | Identifier of the related visit or encounter                                    |
| `satisfaction_score` | Integer         | Rating of the experience, from 1 (lowest) to 5 (highest)              |
| `comments`           | String          | Free-form patient input about their experience                                  |
| `source`             | String          | Origin of the feedback, such as `"in_clinic"`, `"web_portal"`, or `"mobile_app"` |
| `tags`               | Array of String | List of category labels for the feedback (e.g., `"staff"`, `"parking"`)         |
| `metadata`           | Object          | Additional structured data, such as `{"response_time_ms": 1238}`                |

---

## Notes

- All data is synthetic and non-PHI
- Designed for Delta Lake and streaming pipeline testing
