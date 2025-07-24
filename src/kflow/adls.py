# src/kflow/adls.py
from pyspark.sql import SparkSession
import os

def _get_dbutils():
    """Return a dbutils handle if running on Databricks, else None."""
    try:
        return dbutils  # type: ignore[name-defined]
    except NameError:
        pass
    try:
        from pyspark.dbutils import DBUtils  # type: ignore
        spark = SparkSession.builder.getOrCreate()
        return DBUtils(spark)
    except Exception:
        return None

def resolve_sas(scope: str, key: str, explicit: str | None = None,
                env_var: str = "KARDIA_ADLS_SAS") -> str | None:
    """Resolve SAS in order: explicit → dbutils.secrets → env var."""
    if explicit:
        return explicit.lstrip("?")
    dbu = _get_dbutils()
    if dbu is not None:
        try:
            tok = dbu.secrets.get(scope, key)  # type: ignore[attr-defined]
            if tok:
                return tok.lstrip("?")
        except Exception:
            pass
    env_tok = os.getenv(env_var)
    return env_tok.lstrip("?") if env_tok else None

def set_sas(account: str, token: str, suffix: str = "core.windows.net") -> None:
    """Register SAS token for ABFS access on this cluster/session."""
    spark = SparkSession.builder.getOrCreate()
    base = f"{account}.dfs.{suffix}"
    token = token.lstrip("?")
    spark.conf.set(f"fs.azure.account.auth.type.{base}", "SAS")
    spark.conf.set(f"fs.azure.sas.token.provider.type.{base}",
                   "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
    spark.conf.set(f"fs.azure.sas.fixed.token.{base}", token)
