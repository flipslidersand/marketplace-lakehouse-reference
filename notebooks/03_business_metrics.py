# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Gold: Business Metrics
# MAGIC
# MAGIC Build Gold datasets from validated Silver tables.
# MAGIC
# MAGIC | Table | Business Question |
# MAGIC |---|---|
# MAGIC | `gold.daily_revenue` | How much revenue was generated today by channel? |
# MAGIC | `gold.inventory_risk` | Which SKUs are at risk of running out of stock? |
# MAGIC | `gold.price_anomalies` | Which products had abnormal price changes? |

# COMMAND ----------

import sys

sys.path.insert(0, "/Workspace/Repos/marketplace-lakehouse-reference/src")

from marketplace_lakehouse.gold import (
    build_daily_revenue,
    build_inventory_risk,
    build_price_anomalies,
)

# COMMAND ----------

BASE_PATH = "/FileStore/marketplace-lakehouse"

SILVER_ORDERS    = f"{BASE_PATH}/silver/orders"
SILVER_INVENTORY = f"{BASE_PATH}/silver/inventory"
SILVER_PRICE_EVT = f"{BASE_PATH}/silver/price_events"

GOLD_REVENUE    = f"{BASE_PATH}/gold/daily_revenue"
GOLD_INV_RISK   = f"{BASE_PATH}/gold/inventory_risk"
GOLD_ANOMALIES  = f"{BASE_PATH}/gold/price_anomalies"

# COMMAND ----------

# MAGIC %md ## Daily Revenue

# COMMAND ----------

revenue = build_daily_revenue(spark, SILVER_ORDERS, GOLD_REVENUE)
print(f"Revenue rows: {revenue.count()}")
display(revenue.orderBy("date", "channel"))

# COMMAND ----------

# MAGIC %md ## Inventory Risk

# COMMAND ----------

risk = build_inventory_risk(spark, SILVER_ORDERS, SILVER_INVENTORY, GOLD_INV_RISK)

from pyspark.sql import functions as F

print("Risk distribution:")
display(risk.groupBy("risk_level").count())

print("\nAt-risk SKUs:")
display(risk.filter(F.col("risk_level").isin("CRITICAL", "HIGH")).orderBy("days_of_stock"))

# COMMAND ----------

# MAGIC %md ## Price Anomalies

# COMMAND ----------

anomalies = build_price_anomalies(spark, SILVER_PRICE_EVT, GOLD_ANOMALIES)
print(f"Anomalies detected: {anomalies.count()}")
display(
    anomalies.select(
        "product_id",
        F.round("old_price", 0).alias("old_price"),
        F.round("new_price", 0).alias("new_price"),
        F.round(F.col("price_change_ratio") * 100, 1).alias("change_pct"),
        "changed_at",
        "source",
    ).orderBy(F.abs("price_change_ratio").desc())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC Gold tables are now ready for consumption by the external Streamlit application.
# MAGIC Next: [04_technical_evaluation](04_technical_evaluation)
