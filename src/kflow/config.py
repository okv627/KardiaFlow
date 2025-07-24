# src/kflow/config.py
from typing import Final
from types import SimpleNamespace
from pyspark.sql import SparkSession

# Databases
BRONZE_DB:     Final = "kardia_bronze"
SILVER_DB:     Final = "kardia_silver"
GOLD_DB:       Final = "kardia_gold"
VALIDATION_DB: Final = "kardia_validation"

# CDF / masking
CHANGE_TYPES:  Final = ("insert", "update_postimage")
PHI_COLS_MASK: Final = ["DEATHDATE", "SSN", "DRIVERS", "PASSPORT",
                        "FIRST", "LAST", "BIRTHPLACE"]

# ADLS account / secrets (used for raw zone)
ADLS_ACCOUNT:     Final = "kardiaadlsdemo"
ADLS_SAS_SCOPE:   Final = "kardia"
ADLS_SAS_KEYNAME: Final = "adls_raw_sas"
ADLS_SUFFIX:      Final = "core.windows.net"
RAW_CONTAINER:    Final = "raw"

def _adls_uri(container: str, subpath: str = "") -> str:
    sub = (subpath or "").lstrip("/")
    if sub:
        sub = f"{sub}/" if not sub.endswith("/") else sub
    return f"abfss://{container}@{ADLS_ACCOUNT}.dfs.{ADLS_SUFFIX}/{sub}"


def ensure_adls_auth():
    """
    Call once in any notebook that touches ADLS:
        from kflow.config import ensure_adls_auth
        ensure_adls_auth()
    """
    from kflow.adls import set_sas
    import builtins  # ensures dbutils exists in notebooks
    sas = dbutils.secrets.get(ADLS_SAS_SCOPE, ADLS_SAS_KEYNAME)  # type: ignore # noqa
    set_sas(ADLS_ACCOUNT, sas)


# Path builders
#   - raw_path(): ADLS (RAW container)
#   - bronze/silver/gold/etc remain on DBFS
def raw_path(ds: str)             -> str: return _adls_uri(RAW_CONTAINER, ds)

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


# Convenience bundlers
def bronze_paths(ds: str, checkpoint_suffix: str | None = None) -> SimpleNamespace:
    cp = checkpoint_suffix or f"bronze_{ds}"
    return SimpleNamespace(
        db         = BRONZE_DB,
        table      = bronze_table(ds),
        raw        = raw_path(ds),         # <-- now ADLS
        bronze     = bronze_path(ds),
        schema     = schema_path(ds),
        checkpoint = checkpoint_path(cp),
        bad        = quarantine_path(ds)
    )


def silver_paths(ds: str, checkpoint_suffix: str | None = None) -> SimpleNamespace:
    cp = checkpoint_suffix or f"silver_{ds}"
    return SimpleNamespace(
        db         = SILVER_DB,
        table      = silver_table(ds),
        path       = silver_path(ds),
        checkpoint = checkpoint_path(cp)
    )


# Misc
def current_batch_id() -> str:
    spark = SparkSession.builder.getOrCreate()
    return spark.conf.get("spark.databricks.job.runId", "manual")
