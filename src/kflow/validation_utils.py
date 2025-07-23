# src/kflow/validation_utils.py
from typing import Dict, Iterable, Tuple, Callable
from pyspark.sql import functions as F
from pyspark.sql import DataFrame, Row
from pyspark.sql import SparkSession

from .config import VALIDATION_DB

Assertion = Tuple[Callable[[Row], bool], str]  # (predicate(row), message)

def _base_metrics(df: DataFrame, pk_col: str) -> Dict[str, F.Column]:
    return {
        "row_count": F.count("*").alias("row_count"),
        "distinct_pk": F.countDistinct(pk_col).alias("distinct_pk"),
        "null_pk": F.sum(F.when(F.col(pk_col).isNull(), 1).otherwise(0)).alias("null_pk")
    }

def validate_and_log(
    df: DataFrame,
    table_name: str,
    pk_col: str,
    extra_metrics: Dict[str, F.Column] | None = None,
    assertions: Iterable[Assertion] | None = None
) -> Row:
    """
    - Computes base + extra metrics
    - Evaluates assertions
    - Writes one-row summary to kardia_validation.<table_name>_summary
    - Prints green/red banner
    - Returns the Row of metrics
    """
    metrics = _base_metrics(df, pk_col)
    if extra_metrics:
        metrics.update(extra_metrics)

    stats_row = df.agg(*metrics.values()).first()
    stats_dict = stats_row.asDict()

    errors = []
    if assertions:
        for pred, msg in assertions:
            if not pred(stats_row):
                errors.append(msg)

    passed = len(errors) == 0

    _write_summary(stats_dict, table_name, passed, errors)
    from .display_utils import banner
    banner(
        f"{table_name} validation passed" if passed
        else f"Validation failed: {'; '.join(errors)}",
        passed
    )
    return stats_row

def _write_summary(stats_dict: Dict, table_name: str, passed: bool, errors: Iterable[str]):
    spark = SparkSession.builder.getOrCreate()
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {VALIDATION_DB}")

    summary_df = (
        spark.createDataFrame([stats_dict])
             .withColumn("table_name", F.lit(table_name))
             .withColumn("passed", F.lit(passed))
             .withColumn("errors", F.lit(", ".join(errors)))
             .withColumn("_run_ts", F.current_timestamp())
    )
    (summary_df.write.mode("append").option("mergeSchema", "true")
        .saveAsTable(f"{VALIDATION_DB}.{table_name}_summary"))