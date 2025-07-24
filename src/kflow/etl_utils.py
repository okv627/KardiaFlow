# src/kflow/etl_utils.py
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from .config import current_batch_id

def add_audit_cols(df: DataFrame) -> DataFrame:
    """Add standard audit columns to an input DF."""
    return (df.withColumn("_ingest_ts",  F.current_timestamp())
             .withColumn("_source_file", F.input_file_name())
             .withColumn("_batch_id",    F.lit(current_batch_id())))
