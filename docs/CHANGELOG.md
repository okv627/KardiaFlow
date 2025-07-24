# Kardiaflow Project — Changelog

## 2025-07-24

Simplified the pipeline by removing all Qualiflow components and post-Bronze validation notebooks.  

Introduced a unified smoke test script that runs lightweight checks across the Bronze, Silver, and
Gold layers, with results written to the Delta table `kardia_validation.smoke_results`.  

Also migrated the `patients`, `encounters`, and `claims` datasets from DBFS to ADLS Gen2 to
standardize all raw data sources on cloud-backed storage.

## 2025-07-23

Created `docs/qualiflow.md`, a formal specification for Qualiflow—a lightweight trust
layer for datasets. It defines a structured trust summary (JSON) capturing lineage,
quality scores, schema checks, and privacy indicators, and describes how these signals
are surfaced through portable trust indicators like badges and freshness labels. The
spec includes goals, architecture, key metadata fields (e.g., `dq_score`, `fingerprint`,
`masked_fields`).

Refactored the notebook structure so that each Medallion layer now has its own folder,
with pipeline notebooks organized within each layer.

The bootstrap script now populates ADLS with seed files for the Providers and Feedback flows.
The SAS token generation script was updated to include write permissions to support this.

Switched the Bronze Feedback notebook to use COPY INTO instead of Auto Loader for
incremental batch ingestion.

## 2025-07-22

**Major Refactor and Modularization**

- **Refactored all pipeline layers** (`bronze` → `validation` → `silver` → `gold`)
- **Created `src/kflow/` module** containing reusable shared utilities:
  - `config.py` – centralizes constants and path generators
  - `adls.py` – ADLS helpers for bootstrapping and file movement
  - `display_utils.py` – standardized banners and formatted output
  - `etl_utils.py` – shared stream/batch write patterns
  - `validation_utils.py` – schema checks and audit validations
- **Moved file utilities to global scope**:
  - `bootstrap_raw`
  - `move_new_files`
- **Standardized notebook patterns**:
  - Unified checkpointing and path logic via config
  - Consistent use of audit columns and validation
  - Reduced noise using helper functions across all layers
- **Moved all hardcoded values** out of `infra/gen_sas.sh` and `teardown.sh` into `.env`

Verified all changes work as expected end-to-end; pipelines works fine, gold tables produce meaningful results.

## 2025-07-21

Created three modules:
- config.py centralizes all paths, table names, checkpoint locations,
schema paths.
- display_utils.py handles formatting and printing of results.
- validation_utils.py handles all the core logic used in validation notebooks.

Created .env file and removed hardcoded variables from infrastructure scripts.

Standardized notebook output with color-coded summaries to improve readability.

## 2025-07-20

Implemented full feedback pipeline from raw JSONL in ADLS to Bronze (Auto Loader),
Silver (batch append), and Gold (aggregated satisfaction metrics). Overhauled the Silver and Gold layers.

Introduced three enriched Silver join tables:
- silver_encounters_enriched: patients + encounters for lifecycle and cohort metrics
- silver_claims_enriched: claims + providers for financial KPIs and QA
- silver_feedback_enriched: feedback + providers for experience and sentiment analysis

Reorganized Gold into four targeted fact tables:

- gold_patient_lifecycle: Time between visits, patient lifetime span, new/returning classification, age-band utilization
- gold_claim_anomalies: Approval rates by specialty, denial breakdowns, high-cost procedures, rapid-fire claims
- gold_feedback_metrics: Satisfaction scores, tag/comment analysis, and encounter-feedback match rates
- gold_provider_rolling_spend: Daily provider spend and 7-day rolling KPIs using window functions

Existing QA views were removed from Gold.

Deleted the device dataset. Replaced the feedback dataset with a provider-centric
schema using provider_id as a clean foreign key to providers.ProviderID. Converted
claims.csv to Parquet.

## 2025-07-19

Added ingestion tracking to validation notebooks by writing row counts and timestamps
into a centralized kardia_meta.bronze_qc table. This metadata log supports simple
auditing of each Bronze load. Added _ingest_ts and _source_file audit columns to
all Bronze-layer tables to improve traceability.

Applied strict column constraints (e.g., NOT NULL, CHECK) in Silver-layer tables
to enforce data quality. Refactored and cleaned all utility notebooks. Rewrote all
file movement and bootstrap notebooks and handle existence checks. Added filters
to Gold Gender Breakdown.

Standardized naming conventions for both columns and notebooks across all datasets.

Fixed issue preventing Bronze ingestion of Patient records: Added back missing
columns 'Maiden' and 'Suffix'.

Learned a critical lesson about ingestion reliability: Never manually modify raw
input schemas, even if the data looks wrong. CSV files are structurally brittle
and sensitive to positional shifts. Always assume the schema is correct until
proven otherwise, and perform all validation and cleaning downstream in code.
Treat raw files as immutable assets.

## 2025-07-18

Updated Bronze notebooks to support Auto Loader ingestion for new file formats.
Replaced raw ingestion types:
- Encounters: CSV to Avro
- Providers: CSV to TSV

Split and converted raw data into standardized test sets.

Refactored all notebooks in Patient/Encounter and Claim/Provider flows. 

Refined Silver logic to cleanse valid gender and birth year.

## 2025-07-17

Added a gen_sas.sh script that dynamically resolves the Databricks workspace URL,
generates a 24-hour container-level SAS token for ADLS using the connection string,
and stores it securely in a Databricks secret scope kardia/adls_raw_sas. This enables
repeatable, secure access to raw data after daily teardowns without manual intervention.

Changed the Bicep file to support proper dynamic resolution and provisioning of the
Databricks workspace and raw ADLS container, ensuring compatibility with the new
SAS-based access pattern. Also changed the bronze_providers ingestion notebook to
use Auto Loader on CSV files from ADLS instead of JDBC from Postgres.

Moved data validation to a post-Bronze pattern and added validation notebooks for
claims and providers to verify row counts, uniqueness, and schema conformity after
ingestion. Confirmed the full claim and provider ingestion paths are completed and
working end-to-end using Auto Loader with ADLS inputs and Delta Lake Bronze outputs.

## 2025-07-16

Fixed the SCD-2 process in Silver Providers by first closing current rows when tracked
fields change, then inserting new or changed versions using Bronze _ingest_ts as the
start timestamp. This prevents duplicate history from being created on reruns by
ensuring only actual changes trigger new versions.

In the claims Silver notebook, we added a foreachBatch setup that reads incrementally
from Bronze using CDF with availableNow. We changed the deduplication to use row_number()
on ClaimID, and moved the merge logic into a clean upsert_to_silver function.

## 2025-07-15

Implemented and validated the full Silver and Gold layers of the claims and providers
pipeline in Kardiaflow. The Silver layer includes a Type-1 upsert for claims using CDF
and a Type-2 SCD merge for providers to preserve history.

Added three Gold tables using a SQL-based notebook. The hourly Claims table aggregates
claim count, total amount, and average amount; the provider spend table uses a CTE for
daily totals and applies SUM and AVG with a 7-day rolling window; and the QA table
calculates unmatched provider counts and match rates. Both are Type-1 overwrite snapshots.

Edited the Gold scripts for Patients and Encounters to match the new SQL-only approach.
Replaced mixed PySpark and SQL logic with direct SQL statements for monthly encounter
rollups and QA views. The final pipeline now runs cleanly end to end, with all Gold
layers rewritten in SQL.

## 2025-07-14

Removed all partitioning logic from Silver and Gold Encounters tables.

Despite switching to monthly partitioning on 07-13, further testing showed that
even encounter_month produced dozens of small partitions under 1MB each, well
below the recommended 1GB target. Overpartitioning lead to slower queries.

## 2025-07-13

Revised the partitioning strategy for the Silver Encounters table:

Switched from daily granularity using START_DATE, which created thousands of
empty folders, to monthly partitioning using a new ENCOUNTER_MONTH column. This
change was prompted by the first full-scale test of the patient and encounter
pipeline using the complete datasets (~0.25 GB each).

The START_DATE column was removed from the Silver Encounters schema and all
downstream logic, as it is no longer in use.

## 2025-07-12

Added and validated the full ingestion flow for provider and claim data in the
Bronze layer. A bootstrap notebook was created to initialize the raw directory
structure, verify test files, and stage input data for ingestion. As part of the
daily environment setup, the Databricks CLI is used to create a temporary secret
scope and store the Postgres password securely. The cluster is then configured
with an init script and a secret-backed environment variable, enabling a
single-node embedded Postgres instance to install and launch on restart with the
password securely resolved from the secrets utility.

Added JDBC ingestion for provider metadata by first seeding the embedded Postgres
database with providers_10.csv, then reading the resulting table into Delta as
`kardia_bronze.bronze_providers.` For claim data, added an Auto Loader stream that
reads Avro files from the raw directory, applies defined schema, and writes the
results to kardia_bronze.bronze_claims.

Verified that both Bronze tables ingested data and display expected row counts.

## 2025-07-11

Removed unused .withWatermark("EVENT_TS", "1 day") from the Silver Encounters
stream since the query is stateless. Added a 2-year filter on START_DATE in the
Gold aggregation to enable partition pruning.

## 2025-07-10

Replaced the static read of the silver_encounters table with a streaming read
using spark.readStream.table. Switched the write logic from batch .write.mode("overwrite") to a
streaming .writeStream.outputMode("append") with trigger(availableNow=True) and a
configured checkpoint path to ensure idempotent, incremental processing. The left
join and output schema were preserved exactly to maintain compatibility with
downstream Gold-layer logic.

Previously used CTAS and version bookmarks to manage Delta CDF manually.
Now replaced with a simpler incremental streaming job. Reads only inserts and updates
from Bronze, masks PHI, and derives `BIRTH_YEAR`. Uses `row_number()` on `_commit_version`
to retain the latest change per patient ID. Performs SCD Type 1 upserts via `foreachBatch`,
with a checkpoint for exactly-once processing. This reduces complexity and removes the
need for explicit version tracking logic.

In the Gold-layer metrics notebook, replaced the vw_gold_encounters_by_month
view with a materialized Delta table named gold_encounters_by_month using a
streaming write in complete mode. This overwrites the table in each
run. The change enables downstream consumers to read from a table instead of a view,
simplifying integration. The two QA tables (gold_encounters_missing_patient and
gold_patients_no_encounter) were left as batch jobs.

## 2025-07-09

Updated the lineage diagram to include a legend for ingestion and transformation types.
Finalized the providers flow: source data will be stored in Postgres, read via JDBC into
a Bronze Delta table, and loaded into Silver using a batch SCD Type 2 process.

For the feedback/device flow, Bronze device will stream into Silver using a stream-static
join, while Bronze feedback will be batch-appended. The lineage diagram was updated to
reflect these decisions.

Also simplified the gender breakdown KPI by removing the materialized table and using
a view-only approach. Documented all patients and encounters notebooks.

Verified entire end-to-end Patients + Encounters pipeline.

## 2025-07-08

Refactored the Gold-layer logic to simplify the demo.
The KPI for monthly encounter volume is now surfaced through a view-only
design (`vw_gold_encounters_by_month`), eliminating the need for a persisted Delta
table. This view directly aggregates the Silver `silver_encounters_demographics`
table, excluding encounters with missing patient metadata (GENDER or BIRTH_YEAR),
which are now tracked separately in the QA table `gold_encounters_missing_patient`.

A second QA table, `gold_patients_no_encounter`, was added to monitor patients who
never appear in the encounters fact table. To further streamline the code,
previously included partitioning and broadcast join logic were removed, along
with an unused checkpoint path in `03_silver_patients_encounters_join.ipynb`.

## 2025-07-07

Removed unused cost-related fields (TOTAL_CLAIM_COST, BASE_ENCOUNTER_COST) from
the Silver encounters stream and dropped the claim_cost metric from the Gold
monthly aggregation. These columns were previously hardcoded with placeholder
values (0.0) despite no corresponding data in the source. The Silver schema was
reduced to 8 columns, and the Gold layer now reports only monthly encounter
counts grouped by month, GENDER, and BIRTH_YEAR. This cleanup simplifies the
pipeline, reduces I/O and storage, and avoids misleading metrics.

In the Silver Encounters transform, renamed the raw DATE column to EVENT_DATE_STR
and parsed it explicitly using to_date and to_timestamp with the "yyyy-MM-dd"
format to ensure deterministic handling of date-only values. Introduced EVENT_TS
for use with withWatermark, replacing the previous implicit cast to START_TS,
and then aliased it back to START_TS in the final output for consistency.
START_DATE is now derived directly from the parsed DateType, improving clarity.
These changes make the stream more robust and schema-safe.

Added a true SCD Type 1 upsert mechanism to the Silver Encounters pipeline.
Instead of appending rows (which could lead to duplicate EncounterIDs across
batches), we now use foreachBatch to run a Delta Lake MERGE operation on each
micro-batch. This ensures that only the most recent record for each EncounterID
is retained in the Silver table, matching typical business expectations for
encounter-level facts. To support this safely, we removed the append-based
streaming write and now explicitly create the Silver Delta table ahead of time
using an empty static DataFrame that carries the correct schema. This avoids
runtime errors from trying to write a streaming DataFrame to a non-existent
table or from mismatched schemas during development.

Replaced the always-on left-join stream between Silver Encounters and Silver
Patients with a single batch overwrite job. The batch job re-reads both Silver
tables, performs a left join to attach masked demographics (GENDER, BIRTH_YEAR),
and overwrites kardia_silver.silver_patient_encounters. Rows with as-yet-unloaded
patients now surface as NULL, giving Gold KPIs an accurate “unknown” bucket
without the complexity of continuous streaming.

Restored option("cloudFiles.includeExistingFiles","true") in the Bronze Auto-Loader
scripts to ensure historical CSV files are ingested on first run.

Also updated the schema path in Bronze scripts to use bronze_encounters,
aligning it with the naming conventions used elsewhere in the project.

## 2025-07-06

We fixed a subtle but important correctness issue in the Silver merge logic by
replacing a naive .dropDuplicates(["ID"]) call—which would have retained an
arbitrary row per patient ID—with a deterministic approach that explicitly
keeps only the most recent post-image for each patient. Using a window function
(row_number() over a partition by ID ordered by _commit_version descending),
we now ensure that only the latest change per ID (based on Delta Lake commit
lineage) is processed during the merge. This aligns with SCD Type 1 semantics
and eliminates the risk of randomly overwriting newer updates with stale data.

Renamed `STATE_PATH` to `BOOKMARK_FILE` for clarity and updated all table references
to use standard SQL identifiers (e.g., `FROM kardia_bronze.bronze_patients`
instead of file paths).

Removed `.option("cloudFiles.includeExistingFiles", "true")` from Bronze
ingestion scripts. When using `trigger(availableNow=True)`, Auto Loader already
processes all existing files at startup, making the option redundant.

## 2025-07-05

Refactored the Silver encounters pipeline to preserve all unique `EncounterID` rows
by removing unnecessary deduplication logic. Added `.withColumnRenamed("DATE", "START_DATE")`
for clarity and used `.partitionBy("START_DATE")` in the writeStream. Also enabled
schema evolution with `.option("mergeSchema", "true")` to support nullable column
additions during testing.

## 2025-07-04

Refactored Bronze ingestion and validation scripts for clarity and consistency.
Converted `claims_10.csv` to Avro to give the claims pipeline an explicitly typed,
schema-embedded format. Rewrote `device_data_10.json`, originally a single JSON
array, into line-delimited JSON and then Parquet—Auto Loader and Structured
Streaming require one JSON object per line, and Parquet supports the hourly,
windowed aggregations planned for the device-telemetry flow.

Updated the Bronze encounters schema to make the `ID` and `PATIENT` columns
non-nullable and corrected the `DATE` column type from `TimestampType` to
`DateType`. Added a quarantine path (`badRecordsPath`) to the Bronze patients
ingestion script. Standardized reader options to use string literals `"true"`
and `"false"` for `header` and `inferSchema`. Replaced inline SQL with `F.expr()`
for improved readability in Spark queries. Improved variable and path naming
for clarity across the Bronze ingestion scripts.

## 2025-07-02

Refactored all scripts in the Patients and Encounters flow to improve
maintainability and prepare for DLT migration. Enabled continuous streaming
mode in the Silver Encounters transformation and verified end-to-end
functionality. Added a JSON job definition to orchestrate the workflow.
Finalized ETL logic across Bronze, Silver, and Gold layers, and resolved a bug
in Silver Patients where an incorrect variable was used in CDF version tracking.
Revalidated the full pipeline to confirm correctness.

## 2025-06-30 — Implemented Encounters Flow and Integrated with Patients

Added and seeded a new raw landing folder (/kardia/raw/encounters/) with a 10-row
CSV for validation. Created an Auto Loader stream for kardia_bronze.bronze_encounters
with CDF enabled, schema evolution tracking, and checkpointing. Built a
new Silver table, silver_encounters, as a continuous stream. Joined this to the
static silver_patients dimension using a broadcast join to form silver_patient_encounters.

Added a new Gold KPI view, vw_encounters_by_month, aggregating directly from
the joined table. Refactored vw_gender_breakdown to read directly from silver_patients,
and updated the lineage diagram to reflect this change.

Validated full pipeline behavior across both flows: raw-to-Gold completes within
60 seconds, with CDF driving precise Silver updates and downstream refreshes.

The patients + encounters pipeline now matches the intended design and is fully operational.

## 2025-06-29

Completed the patients branch of the Kardiaflow pipeline.  
* Created dedicated landing folders (`dbfs:/kardia/raw/patients/`, `…/encounters/`) plus shared roots for schema tracking (`/kardia/_schemas/`) and stream checkpoints (`/kardia/_checkpoints/`).  
* Added a 10-row smoke test file.
* Implemented an Auto Loader Bronze stream with a fixed `StructType`, schema-drift tracking, and `delta.
enableChangeDataFeed=true`; the stream runs in `availableNow` mode and checkpoints to `/kardia/_checkpoints/bronze_patients`.
* Verified CDF is active from the first data commit, registered `kardia_bronze.bronze_patients` in the metastore.

Built the Silver transform to read only incremental CDF rows, mask direct PHI columns, and deduplicate on `ID`, overwriting `dbfs:/kardia/silver/silver_patients` on each run.  
A Gold notebook now materialises `vw_gender_breakdown`, refreshed after every ingest.  
Manual file drops (e.g., `patients_more_10_v2.csv`) were successfully processed end-to-end, confirming that the checkpointed Auto Loader ingests only new data and the Silver/Gold layers update instantly.  
With schema roots, checkpoints, and reusable notebook patterns in place, the pipeline is fully aligned with the diagram and ready to clone for `encounters` tomorrow.

**Pipeline flow:**
1. Validate the incoming CSV locally.
2. Copy it into the raw landing folder in DBFS.
3. Trigger a one-time `availableNow` Auto Loader read into the Bronze Delta table (with CDF enabled).
4. Re-run the Silver notebook to transform only new CDF changes, deduplicate, and mask PHI.
5. Refresh Gold KPI views from the updated Silver table.

## 2025-06-24

Successfully implemented the full end-to-end ETL pipeline for the `patients`
flow in Databricks. This includes raw data validation, Bronze Delta ingestion
with Change Data Feed, PHI-safe transformation into Silver, and creation of a
KPI Gold view. Environment paths were cleaned up and standardized for
Databricks-only deployment, and a scheduled job with four ordered tasks was
configured and verified. Using temp view in Gold layer for cost-effective
dashboarding.

## 2025-06-23

Raw -> Gold View Pipeline Complete (in local Dev)

Today we finalized the Kardiaflow architecture, integrating batch and streaming
PHI-compliant ETL paths and clarifying ingestion options like Auto Loader and
COPY INTO. We validated a raw 100-row CSV, ingested it into a Bronze Delta
table with Change Data Feed enabled, then transformed it to a schema-enforced,
masked Silver layer. Unit tests confirmed data quality (masking, enum
correctness, uniqueness), and we capped the workflow by creating a Gold-layer
KPI view (`vw_gender_breakdown`) using Delta SQL over a temp view. The pipeline
runs seamlessly in both local and Databricks environments.

## 2025-06-22

Completed Phase 1 and began Phase 2 of Kardiaflow by deploying safe,
cost-controlled infrastructure and validating an initial data pipeline run.
Used `deploy.bicep` to provision Azure Databricks (public-only, no VNet),
Key Vault (soft-delete enabled, purge protection off), and Azure Data Factory.
Confirmed no NAT Gateways or other hidden costs. Created a $5/month
Azure Budget Alert to prevent overages.

Built and verified a full infra loop with `az group create`,
`az deployment group create`, and `automation/infra/teardown.sh`, ensuring
clean teardown and full idempotence. Launched a minimal 1-node Databricks
cluster (Standard_D4s_v3, 10-min auto-terminate), and uploaded `patients_1k.csv`
to DBFS via CLI.

Created and executed the `00_mask_transform_validate` notebook, reading the
file with minimal schema inference, previewing rows, and writing a small Delta
table (`kardia_patients_stage`) with a load timestamp. Verified Spark plan and
partition count to ensure cost-efficiency.

Linked the Databricks workspace to GitHub via Repos, committed the notebook,
and pulled changes locally in PyCharm. All steps support reproducibility,
fast iteration, and teardown-safe development.

## 2025-06-04

After uncovering substantial and silently accumulating costs tied to ADLS Gen2
transaction billing, NAT Gateway persistence, and unremovable infrastructure
triggered by Unity Catalog's Access Connector, the Kardiaflow environment
(`kardiaflow-rg`) was systematically dismantled. Despite having Owner-level
permissions, key resources remained locked behind deny assignments automatically
applied during Unity Catalog provisioning. This prevented deletion of the
NAT Gateway, public IPs, and associated networking components. The situation
was resolved only after escalating to Microsoft Support, who manually removed
the deny policies. With that, all residual services—including Databricks-managed
identities, virtual networks, and the storage account holding partitioned
Parquet output—were eliminated, halting all further billing.

With the environment now fully reset, the project enters a structured four-day
simulation phase grounded in hardened cloud hygiene. The new protocol emphasizes
transient infrastructure by creating and deleting a dedicated resource group daily,
avoiding external storage, and limiting all transformation outputs to `/dbfs/tmp/`.
Over the next four days, I will sequentially explore safe implementations of
streaming ingestion (via Spark’s rate source), star schema modeling and serverless
SQL, Great Expectations for data quality, and Unity Catalog through offline
simulation or short-lived, controlled sessions. This phase will prioritize
operational reversibility and explicit cost boundaries, instilling best practices
for cloud-native data engineering without risk of recurrence.

## 2025-06-03

Developed and tested Azure Data Factory (ADF) copy pipelines to move data from
multiple source systems (Oracle, PostgreSQL, MongoDB) into raw landing zones in
Azure Data Lake Storage (ADLS Gen2). Utilized the Copy Activity in ADF to
extract data as Parquet/CSV and load it into the cloud, organizing pipelines
by source. Verified data ingestion through ADF execution and monitoring, ensuring
successful data landing and row count matching. Set up logging and notifications
for success and failure events.

Added a data validation layer to ensure successful data loading by
cross-checking row counts and performing basic data validation. Used the ADF
Monitor to track pipeline progress and verify completeness of the ingested data.

Set up Databricks and PySpark for data transformation. Mounted Azure Data Lake
Storage (ADLS Gen2) to DBFS and successfully loaded Parquet files into Databricks
notebooks. Performed initial exploration of the data by displaying schemas and
previewing the first few rows of the patients, encounters, and procedures datasets.
Verified successful loading and examined the structure to prepare for subsequent
data transformations.

Set up an Azure Databricks workspace and cluster for data transformation using
PySpark. Loaded raw data from Azure Data Lake Storage (ADLS Gen2) into Databricks,
reading Parquet files into Spark DataFrames and performing initial schema
exploration. Transformed the data by renaming columns, joining patient records
with encounter and procedure data, handling missing values, and adding new
fields like encounter count and readmission flags. Repartitioned the DataFrame
for efficient parallel processing and wrote the cleaned data to ADLS Gen2 in
Parquet format, partitioned by `final_patient_ID` and `encounter_DATE`. Verified
data output by successfully writing 5000 rows, ensuring data quality and
preparing for future processing steps.

## Changelog – 2025-06-02

- Set up and tested all data connections needed for Azure Data Factory to move
  data between systems.
- Created and connected to:
  - A local Oracle XE database
  - A local PostgreSQL database (on port 5433)
  - A local MongoDB instance
  - A cloud-based Azure Data Lake Storage (ADLS Gen2) account named
    `kardiaflowstorage`
- Used a self-hosted integration runtime (SHIR) to securely connect local
  databases to Azure.
- Stored all passwords and access keys in Azure Key Vault (`kardiaflow-kv`)
  for security.
- Verified that all connections worked by running tests in the ADF user interface.

## 2025-06-01

Resolved Oracle XE ingestion failures on `encounters.csv` (~1.5M rows) due to
index space exhaustion in the default `SYSTEM` tablespace. Created a dedicated
`USERS_DATA` tablespace for user data and updated `load_encounters.py` to
support mid-batch commits, `executemany()`, and retry logging via
`logs/skipped_encounters.csv`. Final run completed with no skipped rows. Also
ingested `procedures.csv` (624,139 rows, 0 skips).

Stood up a new PostgreSQL container (`postgres:15` on port 5433), created the
`claims` database, and developed ingestion scripts for `claims.csv` and
`providers.csv`. Scripts include snake_case normalization, deduplication on
primary keys, and schema alignment. Successfully loaded 4,500 claims and
1,500 providers.

Deployed MongoDB (`mongo:7` on port 27017) and created the `healthcare`
database. Wrote an ingestion script for `feedback.json` that parses timestamps,
cleans text fields, and inserts into the `feedback` collection. All 50 documents
loaded successfully.

Finally, created a validation notebook (`source_validation_checks.ipynb`)
to confirm ingestion integrity across all systems. Ran cross-database row
counts, sampled data, and checked for anomalies in `patients`, `claims`,
and `feedback`. All counts and structures verified.

---

## 2025-05-30

Today focused on the ingestion and validation of the synthetic EHR patient
dataset into Oracle XE. I developed and finalized a robust Python script
(`load_patients.py`) to batch load `patients.csv` from `data/raw/ehr/` into
the `patients` table within the Oracle database. The script utilizes **pandas**
for high-throughput data wrangling and **cx_Oracle** for database interaction.

Critical data quality safeguards were implemented within the pipeline:

- **Primary key enforcement**: Rows with missing or null `ID` values are skipped.
- **Deduplication logic**: Previously inserted patients are excluded by checking against existing Oracle records.
- **Field length validation**: Fields such as `SUFFIX`, `GENDER`, and `SSN` are trimmed to Oracle-safe lengths to avoid `ORA-12899` errors.
- **Date coercion**: Invalid or malformed dates are nullified using `pandas.to_datetime`, preserving otherwise valid records.
- **Error resilience**: Failed inserts are caught individually and logged to `logs/skipped_patients.csv` for review.

Performance-wise, the script successfully ingested over **133,000** patient records while skipping a small subset (~72 rows) due to data violations—these were logged for future inspection.

---

## 2025-05-29

Today marked the foundational setup of the Kardiaflow project’s infrastructure and datasets. An Azure account was created and provisioned with both **Azure Data Factory** and **Azure Databricks**, using the East US region to avoid quota limitations. These services will form the backbone of our orchestration and transformation layers.

Simultaneously, the local development environment was established using Docker containers for **PostgreSQL**, **MongoDB**, **Oracle XE**, and **SQL Server**. Each of these databases was configured to simulate realistic hybrid healthcare systems, and connection scripts were written in Python to validate access to all services. These scripts were organized under `automation/db_checks/`, and results were logged to `docs/environment_check.md`.

On the Python side, a virtual environment was created using `venv`, and essential packages such as `pyspark`, `pandas`, `sqlalchemy`, and `pymongo` were installed. This environment will support local testing, data generation, and PySpark-based transformations.

The raw data layer was also initialized. We sourced a synthetic health insurance claims dataset from Kaggle and placed the files—`claims.csv` and `providers.csv`—under `data/raw/claims/`. Two additional JSON files, `feedback.json` and `device_data.json`, were custom generated to simulate semi-structured patient feedback and wearable device data. These were saved under `data/raw/feedback/`.

Separately, a large synthetic EHR dataset was generated using **Synthea**. After extracting twelve `.tar.gz` archives into a consolidated output directory, we curated and moved core CSVs (`patients.csv`, `encounters.csv`, and `procedures.csv`) into the `data/raw/ehr/` directory. The rest of the archive was excluded from version control via `.gitignore`.

Finally, the project was initialized as a Git repository and connected to GitHub. A clean `.gitignore` was configured to prevent large datasets, environments, and cache files from polluting the repository. All datasets and environments were documented in `data/data_dictionary.md`, covering both schema definitions and usage notes for claims, feedback, device data, and EHR records.
