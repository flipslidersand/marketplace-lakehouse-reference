# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze Ingestion
# MAGIC
# MAGIC Ingest raw Orders (JSONL), Inventory (CSV), and Price Events (JSONL)
# MAGIC into Bronze Delta tables.
# MAGIC
# MAGIC **Responsibilities:**
# MAGIC - Preserve all source fields, including malformed values
# MAGIC - Add `_ingested_at` and `_source_file` metadata
# MAGIC - No business validation — Bronze is for traceability

# COMMAND ----------

import sys

sys.path.insert(0, "/Workspace/Repos/marketplace-lakehouse-reference/src")

from marketplace_lakehouse.bronze import ingest_inventory, ingest_orders, ingest_price_events

# COMMAND ----------

# MAGIC %md ## Configuration

# COMMAND ----------

BASE_PATH = "/FileStore/marketplace-lakehouse"
DATA_PATH = f"{BASE_PATH}/data/samples"

BRONZE_ORDERS    = f"{BASE_PATH}/bronze/orders"
BRONZE_INVENTORY = f"{BASE_PATH}/bronze/inventory"
BRONZE_PRICE_EVT = f"{BASE_PATH}/bronze/price_events"

# COMMAND ----------

# MAGIC %md ## Generate synthetic data (first run only)

# COMMAND ----------

# dbutils.fs.mkdirs(DATA_PATH)
# Upload data/samples/*.jsonl and *.csv via UI, or run:
# %sh python /Workspace/Repos/marketplace-lakehouse-reference/src/generator/generate_events.py

# COMMAND ----------

# MAGIC %md ## Ingest Orders

# COMMAND ----------

orders_bronze = ingest_orders(spark, f"{DATA_PATH}/orders.jsonl", BRONZE_ORDERS)
print(f"Bronze orders: {orders_bronze.count()} rows")
display(orders_bronze.limit(5))

# COMMAND ----------

# MAGIC %md ## Ingest Inventory

# COMMAND ----------

inventory_bronze = ingest_inventory(spark, f"{DATA_PATH}/inventory.csv", BRONZE_INVENTORY)
print(f"Bronze inventory: {inventory_bronze.count()} rows")
display(inventory_bronze.limit(5))

# COMMAND ----------

# MAGIC %md ## Ingest Price Events

# COMMAND ----------

price_bronze = ingest_price_events(spark, f"{DATA_PATH}/price_events.jsonl", BRONZE_PRICE_EVT)
print(f"Bronze price events: {price_bronze.count()} rows")
display(price_bronze.limit(5))

# COMMAND ----------

# MAGIC %md ## Verify metadata fields

# COMMAND ----------

display(orders_bronze.select("order_id", "ordered_at", "_ingested_at", "_source_file").limit(3))
