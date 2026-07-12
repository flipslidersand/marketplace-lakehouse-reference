"""
Acme Marketplace — Operations Dashboard

Streamlit application that queries Gold Delta tables through Databricks SQL
to surface real-time operational metrics for the operations team.

Run locally:
    streamlit run app/main.py

Environment variables (see .env.example):
    DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_WAREHOUSE_ID

If DATABRICKS_HOST is not set, the app loads local sample data from
data/gold_sample/ (populated by running the pipeline locally first).
"""

import os
import pandas as pd
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.databricks_client import (
    fetch_kpi_summary,
    fetch_inventory_risk,
    fetch_price_anomalies,
    fetch_daily_revenue,
)


# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Acme Marketplace — Operations",
    page_icon="📦",
    layout="wide",
)

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------

st.title("📦 Acme Marketplace — Operations Dashboard")

mode = "Databricks SQL" if os.getenv("DATABRICKS_HOST") else "Local sample data"
st.caption(f"Data source: {mode}")

# ---------------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------------

with st.spinner("Loading Gold tables..."):
    try:
        kpi = fetch_kpi_summary()
        risk_df = fetch_inventory_risk()
        anomaly_df = fetch_price_anomalies()
        revenue_df = fetch_daily_revenue()
        error = None
    except Exception as e:
        kpi = {"revenue_today": 0, "at_risk_skus": 0, "price_anomalies": 0}
        risk_df = pd.DataFrame()
        anomaly_df = pd.DataFrame()
        revenue_df = pd.DataFrame()
        error = str(e)

if error:
    st.error(f"Failed to load data: {error}")

# ---------------------------------------------------------------------------
# KPI Cards
# ---------------------------------------------------------------------------

st.subheader("Key Metrics")
col1, col2, col3 = st.columns(3)

with col1:
    st.metric(
        label="Revenue Today",
        value=f"¥{kpi['revenue_today']:,.0f}",
    )

with col2:
    at_risk = kpi["at_risk_skus"]
    st.metric(
        label="At-Risk SKUs",
        value=at_risk,
        delta=f"{at_risk} need attention" if at_risk > 0 else "All clear",
        delta_color="inverse" if at_risk > 0 else "normal",
    )

with col3:
    anomalies = kpi["price_anomalies"]
    st.metric(
        label="Price Anomalies",
        value=anomalies,
        delta=f"{anomalies} flagged" if anomalies > 0 else "None",
        delta_color="inverse" if anomalies > 0 else "normal",
    )

st.divider()

# ---------------------------------------------------------------------------
# Inventory Risk Table
# ---------------------------------------------------------------------------

st.subheader("Inventory Risk")
st.caption("Products with fewer than 7 days of stock. Days of stock = current quantity ÷ avg daily sales.")

if not risk_df.empty:
    risk_display = risk_df.copy()

    def _risk_badge(level: str) -> str:
        return {"CRITICAL": "🔴 CRITICAL", "HIGH": "🟠 HIGH", "NORMAL": "🟢 NORMAL"}.get(level, level)

    if "risk_level" in risk_display.columns:
        risk_display["risk_level"] = risk_display["risk_level"].apply(_risk_badge)

    if "days_of_stock" in risk_display.columns:
        risk_display["days_of_stock"] = risk_display["days_of_stock"].apply(
            lambda x: f"{x:.1f}" if pd.notna(x) else "N/A (no sales)"
        )

    if "avg_daily_sales" in risk_display.columns:
        risk_display["avg_daily_sales"] = risk_display["avg_daily_sales"].apply(
            lambda x: f"{x:.1f}" if pd.notna(x) else "—"
        )

    st.dataframe(
        risk_display,
        use_container_width=True,
        hide_index=True,
        column_config={
            "product_id": "SKU",
            "current_quantity": "Stock",
            "avg_daily_sales": "Avg Daily Sales",
            "days_of_stock": "Days of Stock",
            "risk_level": "Risk",
        },
    )
else:
    st.info("No inventory data available.")

st.divider()

# ---------------------------------------------------------------------------
# Price Anomaly Table
# ---------------------------------------------------------------------------

st.subheader("Price Anomalies")
st.caption("Price change events where absolute change ≥ 30%.")

if not anomaly_df.empty:
    anomaly_display = anomaly_df.copy()

    if "price_change_ratio" in anomaly_display.columns:
        anomaly_display["price_change_pct"] = anomaly_display["price_change_ratio"].apply(
            lambda x: f"{x*100:+.1f}%" if pd.notna(x) else "—"
        )
        anomaly_display = anomaly_display.drop(columns=["price_change_ratio"])

    if "changed_at" in anomaly_display.columns:
        anomaly_display["changed_at"] = pd.to_datetime(anomaly_display["changed_at"]).dt.strftime("%Y-%m-%d %H:%M")

    st.dataframe(
        anomaly_display,
        use_container_width=True,
        hide_index=True,
        column_config={
            "product_id": "SKU",
            "old_price": "Old Price",
            "new_price": "New Price",
            "price_change_pct": "Change",
            "changed_at": "Timestamp",
            "source": "Source",
        },
    )
else:
    st.info("No price anomalies detected.")

st.divider()

# ---------------------------------------------------------------------------
# Revenue Chart
# ---------------------------------------------------------------------------

st.subheader("Daily Revenue by Channel")

if not revenue_df.empty and "date" in revenue_df.columns and "gross_revenue" in revenue_df.columns:
    pivot = revenue_df.pivot_table(
        index="date", columns="channel", values="gross_revenue", aggfunc="sum"
    ).fillna(0)
    st.bar_chart(pivot)
else:
    st.info("No revenue data available.")

# ---------------------------------------------------------------------------
# Footer
# ---------------------------------------------------------------------------

st.caption(
    "Architecture: Synthetic Events → Bronze (raw Delta) → Silver (validated) "
    "→ Gold (business metrics) → Databricks SQL → Streamlit"
)
