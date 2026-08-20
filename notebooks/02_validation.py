# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Silver Validation and Quarantine
# MAGIC
# MAGIC Validate, deduplicate, and normalize Bronze records into Silver tables.
# MAGIC Invalid records are written to Quarantine tables — never silently dropped.
# MAGIC
# MAGIC **Quarantine reasons:**
# MAGIC - `MISSING_ORDER_ID` / `MISSING_PRODUCT_ID`
# MAGIC - `INVALID_QUANTITY` / `INVALID_PRICE`
# MAGIC - `INVALID_TIMESTAMP`
# MAGIC - `DUPLICATE_ORDER`
# MAGIC - `MISSING_WAREHOUSE`
# MAGIC - `INVALID_OLD_PRICE` / `INVALID_NEW_PRICE`

# COMMAND ----------

import sys

sys.path.insert(0, "/Workspace/Repos/marketplace-lakehouse-reference/src")

from marketplace_lakehouse.quality import quarantine_summary
from marketplace_lakehouse.silver import process_inventory, process_orders, process_price_events

# COMMAND ----------

BASE_PATH = "/FileStore/marketplace-lakehouse"

BRONZE_ORDERS    = f"{BASE_PATH}/bronze/orders"
BRONZE_INVENTORY = f"{BASE_PATH}/bronze/inventory"
BRONZE_PRICE_EVT = f"{BASE_PATH}/bronze/price_events"

SILVER_ORDERS    = f"{BASE_PATH}/silver/orders"
SILVER_INVENTORY = f"{BASE_PATH}/silver/inventory"
SILVER_PRICE_EVT = f"{BASE_PATH}/silver/price_events"
QUAR_ORDERS      = f"{BASE_PATH}/silver/quarantined_orders"
QUAR_INVENTORY   = f"{BASE_PATH}/silver/quarantined_inventory"
QUAR_PRICE_EVT   = f"{BASE_PATH}/silver/quarantined_price_events"

# COMMAND ----------

# MAGIC %md ## Process Orders

# COMMAND ----------

clean_orders, quar_orders = process_orders(spark, BRONZE_ORDERS, SILVER_ORDERS, QUAR_ORDERS)
print(f"Silver orders (clean):      {clean_orders.count()}")
print(f"Silver orders (quarantine): {quar_orders.count()}")
print("Quarantine breakdown:", quarantine_summary(quar_orders))

# COMMAND ----------

display(quar_orders.select("order_id", "ordered_at", "_quarantine_reason").limit(10))

# COMMAND ----------

# MAGIC %md ## Process Inventory

# COMMAND ----------

clean_inv, quar_inv = process_inventory(spark, BRONZE_INVENTORY, SILVER_INVENTORY, QUAR_INVENTORY)
print(f"Silver inventory (clean):      {clean_inv.count()}")
print(f"Silver inventory (quarantine): {quar_inv.count()}")
print("Negative stock rows:", clean_inv.filter("negative_stock = true").count())

# COMMAND ----------

# MAGIC %md ## Process Price Events

# COMMAND ----------

clean_pe, quar_pe = process_price_events(spark, BRONZE_PRICE_EVT, SILVER_PRICE_EVT, QUAR_PRICE_EVT)
print(f"Silver price events (clean):      {clean_pe.count()}")
print(f"Silver price events (quarantine): {quar_pe.count()}")
print("Quarantine breakdown:", quarantine_summary(quar_pe))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC Invalid records remain available in quarantine tables for investigation.
# MAGIC The `_quarantine_reason` field identifies the specific rule that failed.
# MAGIC
# MAGIC Next: [03_business_metrics](03_business_metrics)
