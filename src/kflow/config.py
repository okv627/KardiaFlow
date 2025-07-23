# src/kflow/config.py
from typing import Final
from types import SimpleNamespace
from pyspark.sql import SparkSession

# ── Databases ────────────────────────────────────────────────
BRONZE_DB:     Final = "kardia_bronze"
SILVER_DB:     Final = "kardia_silver"
GOLD_DB:       Final = "kardia_gold"
VALIDATION_DB: Final = "kardia_validation"

# ── CDF / masking ────────────────────────────────────────────
CHANGE_TYPES:  Final = ("insert", "update_postimage")
PHI_COLS_MASK: Final = ["DEATHDATE","SSN","DRIVERS","PASSPORT","FIRST","LAST","BIRTHPLACE"]

# ── DBFS path builders ───────────────────────────────────────
def raw_path(ds: str)             -> str: return f"dbfs:/kardia/raw/{ds}/"
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

# ── ADLS helpers (for providers/feedback) ────────────────────
def adls_raw_path(subdir: str,
                  account: str = "kardiaadlsdemo",
                  suffix: str = "core.windows.net") -> str:
    return f"abfss://raw@{account}.dfs.{suffix}/{subdir}/"

# ── Convenience bundlers ─────────────────────────────────────
def bronze_paths(ds: str, checkpoint_suffix: str | None = None) -> SimpleNamespace:
    cp = checkpoint_suffix or f"bronze_{ds}"
    return SimpleNamespace(
        db         = BRONZE_DB,
        table      = bronze_table(ds),
        raw        = raw_path(ds),
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

# ── Misc ─────────────────────────────────────────────────────
def current_batch_id() -> str:
    spark = SparkSession.builder.getOrCreate()
    return spark.conf.get("spark.databricks.job.runId", "manual")