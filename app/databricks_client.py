"""
Databricks SQL connector for the Streamlit demo application.

Reads Gold tables via Databricks SQL Warehouse using environment variables:
  DATABRICKS_HOST         — workspace URL
  DATABRICKS_TOKEN        — personal access token
  DATABRICKS_WAREHOUSE_ID — SQL warehouse ID

Falls back to local Delta/Pandas reads if DATABRICKS_HOST is not set,
enabling offline development without a live workspace.
"""

import os

import pandas as pd


def _use_databricks() -> bool:
    return bool(os.getenv("DATABRICKS_HOST"))


def _get_connection():
    from databricks import sql

    host = os.environ["DATABRICKS_HOST"]
    token = os.environ["DATABRICKS_TOKEN"]
    warehouse_id = os.environ["DATABRICKS_WAREHOUSE_ID"]

    return sql.connect(
        server_hostname=host.replace("https://", ""),
        http_path=f"/sql/1.0/warehouses/{warehouse_id}",
        access_token=token,
    )


def _query_databricks(sql_query: str) -> pd.DataFrame:
    with _get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql_query)
            return cursor.fetchall_arrow().to_pandas()


def _local_gold_path(table: str) -> str:
    base = os.path.join(os.path.dirname(__file__), "..", "data", "gold_sample")
    return os.path.join(base, f"{table}.parquet")


def _query_local(table: str) -> pd.DataFrame:
    path = _local_gold_path(table)
    if os.path.exists(path):
        return pd.read_parquet(path)
    # Return empty placeholder so the app still renders
    return pd.DataFrame()


def fetch_daily_revenue() -> pd.DataFrame:
    if _use_databricks():
        return _query_databricks("""
            SELECT date, channel, order_count, units_sold, gross_revenue
            FROM gold.daily_revenue
            ORDER BY date DESC, channel
            LIMIT 500
        """)
    return _query_local("daily_revenue")


def fetch_inventory_risk() -> pd.DataFrame:
    if _use_databricks():
        return _query_databricks("""
            SELECT product_id, current_quantity, avg_daily_sales, days_of_stock, risk_level
            FROM gold.inventory_risk
            ORDER BY days_of_stock ASC NULLS LAST
        """)
    return _query_local("inventory_risk")


def fetch_price_anomalies() -> pd.DataFrame:
    if _use_databricks():
        return _query_databricks("""
            SELECT product_id, old_price, new_price, price_change_ratio, changed_at, source
            FROM gold.price_anomalies
            ORDER BY ABS(price_change_ratio) DESC
            LIMIT 200
        """)
    return _query_local("price_anomalies")


def fetch_kpi_summary() -> dict:
    revenue_df = fetch_daily_revenue()
    risk_df = fetch_inventory_risk()
    anomaly_df = fetch_price_anomalies()

    today_revenue = 0.0
    if not revenue_df.empty and "gross_revenue" in revenue_df.columns:
        if "date" in revenue_df.columns:
            latest_date = revenue_df["date"].max()
            today_revenue = float(
                revenue_df[revenue_df["date"] == latest_date]["gross_revenue"].sum()
            )
        else:
            today_revenue = float(revenue_df["gross_revenue"].sum())

    at_risk_skus = 0
    if not risk_df.empty and "risk_level" in risk_df.columns:
        at_risk_skus = int(risk_df[risk_df["risk_level"].isin(["CRITICAL", "HIGH"])].shape[0])

    anomaly_count = len(anomaly_df) if not anomaly_df.empty else 0

    return {
        "revenue_today": today_revenue,
        "at_risk_skus": at_risk_skus,
        "price_anomalies": anomaly_count,
    }
