# src/kflow/config.py
from typing import Final
from types import SimpleNamespace
from pyspark.sql import SparkSession

from kflow.adls import resolve_sas, set_sas

# ── Databases ────────────────────────────────────────────────────────────────
BRONZE_DB:     Final = "kardia_bronze"
SILVER_DB:     Final = "kardia_silver"
GOLD_DB:       Final = "kardia_gold"
VALIDATION_DB: Final = "kardia_validation"

# ── CDF / masking ────────────────────────────────────────────────────────────
CHANGE_TYPES:  Final = ("insert", "update_postimage")
PHI_COLS_MASK: Final = ["DEATHDATE", "SSN", "DRIVERS", "PASSPORT",
                        "FIRST", "LAST", "BIRTHPLACE"]

# ── ADLS account / secrets (raw zone) ────────────────────────────────────────
ADLS_ACCOUNT:     Final = "kardiaadlsdemo"
ADLS_SUFFIX:      Final = "core.windows.net"
RAW_CONTAINER:    Final = "raw"

ADLS_SAS_SCOPE:   Final = "kardia"
ADLS_SAS_KEYNAME: Final = "adls_raw_sas"

RAW_BASE: Final = f"abfss://{RAW_CONTAINER}@{ADLS_ACCOUNT}.dfs.{ADLS_SUFFIX}"

def ensure_adls_auth(sas: str | None = None) -> None:
    """
    Call once in any notebook/script that touches ADLS.
    Resolution order:
      1) explicit sas arg
      2) Databricks secrets (dbutils)
      3) env var KARDIA_ADLS_SAS
    """
    tok = resolve_sas(ADLS_SAS_SCOPE, ADLS_SAS_KEYNAME, sas)
    if not tok:
        raise RuntimeError(
            "ensure_adls_auth() No SAS token found. Provide it via "
            "`ensure_adls_auth(sas=...)`, Databricks secrets "
            f"(scope='{ADLS_SAS_SCOPE}', key='{ADLS_SAS_KEYNAME}'), "
            "or env var KARDIA_ADLS_SAS."
        )
    set_sas(ADLS_ACCOUNT, tok, suffix=ADLS_SUFFIX)

# ── Path builders ────────────────────────────────────────────────────────────
def raw_path(ds: str)             -> str: return f"{RAW_BASE}/{ds}/"
def bronze_table(ds: str)         -> str: return f"{BRONZE_DB}.bronze_{ds}"
def bronze_path(ds: str)          -> str: return f"dbfs:/kardia/bronze/bronze_{ds}"
def schema_path(ds: str)          -> str: return f"dbfs:/kardia/_schemas/{ds}"
def checkpoint_path(name: str)    -> str: return f"dbfs:/kardia/_checkpoints/{name}"
def quarantine_path(ds: str)      -> str: return f"dbfs:/kardia/_quarantine/raw/bad_{ds}"
def silver_table(ds: str)         -> str: return f"{SILVER_DB}.silver_{ds}"
def silver_path(ds: str)          -> str: return f"dbfs:/kardia/silver/silver_{ds}"
def gold_table(name: str)         -> str: return f"{GOLD_DB}.{name}"

def validation_summary_table(name: str) -> str:
    return f"{VALIDATION_DB}.{name}_summary"

# ── Convenience bundlers ─────────────────────────────────────────────────────
def bronze_paths(ds: str, checkpoint_suffix: str | None = None) -> SimpleNamespace:
    cp = checkpoint_suffix or f"bronze_{ds}"
    return SimpleNamespace(
        db         = BRONZE_DB,
        table      = bronze_table(ds),
        raw        = raw_path(ds),
        bronze     = bronze_path(ds),
        schema     = schema_path(ds),
        checkpoint = checkpoint_path(cp),
        bad        = quarantine_path(ds),
    )

def silver_paths(ds: str, checkpoint_suffix: str | None = None) -> SimpleNamespace:
    cp = checkpoint_suffix or f"silver_{ds}"
    return SimpleNamespace(
        db         = SILVER_DB,
        table      = silver_table(ds),
        path       = silver_path(ds),
        checkpoint = checkpoint_path(cp),
    )

# ── Misc ─────────────────────────────────────────────────────────────────────
def current_batch_id() -> str:
    spark = SparkSession.builder.getOrCreate()
    return spark.conf.get("spark.databricks.job.runId", "manual")
