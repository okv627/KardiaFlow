# src/kflow/display_utils.py
"""
Lightweight display helpers that work both on Databricks and locally.

Databricks: import display/displayHTML/spark from databricks.sdk.runtime
Local/IPython: fall back to IPython.display + a SparkSession.
"""

from pyspark.sql import SparkSession

# --- Resolve notebook helpers & Spark ---
try:
    # DBR 13+ supported import path
    from databricks.sdk.runtime import displayHTML, display, spark  # type: ignore
except Exception:  # local dev / unit tests
    from IPython.display import HTML as _HTML, display as _ip_display  # type: ignore

    def displayHTML(html: str):
        _ip_display(_HTML(html))

    display = _ip_display
    spark = SparkSession.builder.getOrCreate()

# --- Public API -------------------------------------------------------------

def banner(msg: str, ok: bool = True) -> None:
    """
    Render a bold colored banner. Green for success, red for failure.
    """
    color = "green" if ok else "red"
    displayHTML(f"<div style='color:{color};font-weight:bold'>{msg}</div>")


def show_history(delta_path: str, limit: int = 5) -> None:
    """
    Show recent Delta Lake history rows for a given path.
    """
    hist = (
        spark.sql(f"DESCRIBE HISTORY delta.`{delta_path}`")
             .select("version", "timestamp", "operation", "operationParameters")
             .limit(limit)
    )
    displayHTML("<div style='margin-top:10px;font-weight:bold'>Recent Delta History:</div>")
    display(hist)


def show_head(df, n: int = 5) -> None:
    """
    Display the first n rows of a DataFrame.
    """
    display(df.limit(n))


def banner_stream(name: str, trigger: str, source: str) -> None:
    """
    Convenience banner for streaming queries.
    """
    banner(f"Stream started: {name} • Trigger: {trigger} • Source: {source}", ok=True)
