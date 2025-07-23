# src/kflow/display_utils.py
def banner(msg: str, ok: bool = True):
    color = "green" if ok else "red"
    displayHTML(f"<div style='color:{color};font-weight:bold'>{msg}</div>")

def show_history(delta_path: str, limit: int = 5):
    hist = spark.sql(f"DESCRIBE HISTORY delta.`{delta_path}`").select(
        "version","timestamp","operation","operationParameters"
    ).limit(limit)
    displayHTML("<div style='margin-top:10px;font-weight:bold'>Recent Delta History:</div>")
    display(hist)

def show_head(df, n: int = 5):
    display(df.limit(n))

def banner_stream(name: str, trigger: str, source: str):
    """
    Convenience wrapper for a green 'stream started' banner.
    """
    banner(f"Stream started: {name} • Trigger: {trigger} • Source: {source}", ok=True)