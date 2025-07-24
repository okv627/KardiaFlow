# src/kflow/display_utils.py
from pyspark.sql import SparkSession

def get_history_df(target: str, limit: int = 5):
    """
    Returns a Spark DataFrame with the Delta transaction history
    for the given table name or path. UI-friendly when passed to display().
    """
    spark = SparkSession.builder.getOrCreate()

    # Use Delta path if target looks like a URI or DBFS path
    if "://" in target or target.startswith("/") or target.startswith("dbfs:"):
        query = f"DESCRIBE HISTORY delta.`{target}`"
    else:
        query = f"DESCRIBE HISTORY {target}"

    return (
        spark.sql(query)
             .select("version", "timestamp", "operation", "operationParameters")
             .orderBy("version", ascending=False)
             .limit(limit)
    )

def show_history(target: str, limit: int = 5):
    """
    Displays the Delta transaction history in Databricks using the rich UI.
    """
    display(get_history_df(target, limit))
