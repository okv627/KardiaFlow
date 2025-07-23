# src/kflow/adls.py
from pyspark.sql import SparkSession

def set_sas(account: str, token: str, suffix: str = "core.windows.net") -> None:
    """
    Register SAS token for ABFS access on this cluster/session.
    """
    spark = SparkSession.builder.getOrCreate()
    base = f"{account}.dfs.{suffix}"
    token = token.lstrip("?")
    spark.conf.set(f"fs.azure.account.auth.type.{base}", "SAS")
    spark.conf.set(f"fs.azure.sas.token.provider.type.{base}",
                   "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
    spark.conf.set(f"fs.azure.sas.fixed.token.{base}", token)
