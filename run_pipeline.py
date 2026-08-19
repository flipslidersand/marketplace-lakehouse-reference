"""
Local pipeline runner — Bronze → Silver → Gold.

Usage:
    python3 run_pipeline.py

Outputs:
    data/delta/   — Delta tables (Bronze / Silver / Gold)
    data/gold_sample/   — Parquet exports for Streamlit offline mode

After running, launch the dashboard with:
    streamlit run app/main.py
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from generator.generate_events import main as generate_data
from marketplace_lakehouse import bronze, gold, silver

BASE = os.path.dirname(__file__)
DATA_SAMPLES = os.path.join(BASE, "data", "samples")
DATA_DELTA = os.path.join(BASE, "data", "delta")
GOLD_SAMPLE = os.path.join(BASE, "data", "gold_sample")

BRONZE_ORDERS    = f"{DATA_DELTA}/bronze/orders"
BRONZE_INVENTORY = f"{DATA_DELTA}/bronze/inventory"
BRONZE_PRICE_EVT = f"{DATA_DELTA}/bronze/price_events"

SILVER_ORDERS    = f"{DATA_DELTA}/silver/orders"
SILVER_INVENTORY = f"{DATA_DELTA}/silver/inventory"
SILVER_PRICE_EVT = f"{DATA_DELTA}/silver/price_events"
QUAR_ORDERS      = f"{DATA_DELTA}/silver/quarantined_orders"
QUAR_INVENTORY   = f"{DATA_DELTA}/silver/quarantined_inventory"
QUAR_PRICE_EVT   = f"{DATA_DELTA}/silver/quarantined_price_events"

GOLD_REVENUE     = f"{DATA_DELTA}/gold/daily_revenue"
GOLD_INV_RISK    = f"{DATA_DELTA}/gold/inventory_risk"
GOLD_ANOMALIES   = f"{DATA_DELTA}/gold/price_anomalies"


def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("marketplace-lakehouse-local")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def section(title: str) -> None:
    print(f"\n{'─' * 50}")
    print(f"  {title}")
    print(f"{'─' * 50}")


def export_gold_parquet(spark: SparkSession) -> None:
    """Export Gold Delta tables to single-file parquet for Streamlit offline mode."""
    os.makedirs(GOLD_SAMPLE, exist_ok=True)

    tables = {
        "daily_revenue": GOLD_REVENUE,
        "inventory_risk": GOLD_INV_RISK,
        "price_anomalies": GOLD_ANOMALIES,
    }
    for name, path in tables.items():
        df = spark.read.format("delta").load(path)
        out = os.path.join(GOLD_SAMPLE, f"{name}.parquet")
        df.toPandas().to_parquet(out, index=False)
        print(f"  Exported {name} → data/gold_sample/{name}.parquet ({df.count()} rows)")


def main() -> None:
    # ── Step 1: Generate synthetic data ──────────────────────────────────────
    section("Step 1/6  Generate synthetic data")
    if not os.path.exists(os.path.join(DATA_SAMPLES, "orders.jsonl")):
        generate_data()
    else:
        print("  data/samples/ already exists — skipping generation")
        print("  Delete data/samples/ and re-run to regenerate")

    # ── Step 2: Spark session ─────────────────────────────────────────────────
    section("Step 2/6  Starting SparkSession (local mode)")
    spark = build_spark()
    print(f"  Spark {spark.version}  |  Delta {spark.conf.get('spark.databricks.delta.preview.enabled', 'n/a')}")

    # ── Step 3: Bronze ────────────────────────────────────────────────────────
    section("Step 3/6  Bronze ingestion")
    b_orders = bronze.ingest_orders(spark, f"{DATA_SAMPLES}/orders.jsonl", BRONZE_ORDERS)
    b_inv    = bronze.ingest_inventory(spark, f"{DATA_SAMPLES}/inventory.csv", BRONZE_INVENTORY)
    b_pe     = bronze.ingest_price_events(spark, f"{DATA_SAMPLES}/price_events.jsonl", BRONZE_PRICE_EVT)
    print(f"  bronze.orders        {b_orders.count():>5} rows")
    print(f"  bronze.inventory     {b_inv.count():>5} rows")
    print(f"  bronze.price_events  {b_pe.count():>5} rows")

    # ── Step 4: Silver ────────────────────────────────────────────────────────
    section("Step 4/6  Silver validation & quarantine")
    clean_orders, quar_orders = silver.process_orders(spark, BRONZE_ORDERS, SILVER_ORDERS, QUAR_ORDERS)
    clean_inv,    quar_inv    = silver.process_inventory(spark, BRONZE_INVENTORY, SILVER_INVENTORY, QUAR_INVENTORY)
    clean_pe,     quar_pe     = silver.process_price_events(spark, BRONZE_PRICE_EVT, SILVER_PRICE_EVT, QUAR_PRICE_EVT)

    print(f"  silver.orders        {clean_orders.count():>5} clean  |  {quar_orders.count():>3} quarantined")
    print(f"  silver.inventory     {clean_inv.count():>5} clean  |  {quar_inv.count():>3} quarantined")
    print(f"  silver.price_events  {clean_pe.count():>5} clean  |  {quar_pe.count():>3} quarantined")

    from marketplace_lakehouse.quality import quarantine_summary
    print("\n  Quarantine breakdown (orders):")
    for reason, count in quarantine_summary(quar_orders).items():
        print(f"    {reason:<30} {count}")

    neg_stock = clean_inv.filter("negative_stock = true").count()
    if neg_stock:
        print(f"\n  Negative stock SKUs: {neg_stock} (flagged, not quarantined)")

    # ── Step 5: Gold ─────────────────────────────────────────────────────────
    section("Step 5/6  Gold business metrics")
    revenue    = gold.build_daily_revenue(spark, SILVER_ORDERS, GOLD_REVENUE)
    risk       = gold.build_inventory_risk(spark, SILVER_ORDERS, SILVER_INVENTORY, GOLD_INV_RISK)
    anomalies  = gold.build_price_anomalies(spark, SILVER_PRICE_EVT, GOLD_ANOMALIES)

    from pyspark.sql import functions as F
    total_rev = revenue.agg(F.sum("gross_revenue")).first()[0] or 0
    critical  = risk.filter(F.col("risk_level") == "CRITICAL").count()
    high      = risk.filter(F.col("risk_level") == "HIGH").count()

    print(f"  gold.daily_revenue   {revenue.count():>5} rows  |  total revenue ¥{total_rev:,.0f}")
    print(f"  gold.inventory_risk  {risk.count():>5} SKUs  |  CRITICAL={critical}  HIGH={high}")
    print(f"  gold.price_anomalies {anomalies.count():>5} events")

    # ── Step 6: Export for Streamlit ─────────────────────────────────────────
    section("Step 6/6  Export Gold → data/gold_sample/ (Streamlit offline)")
    export_gold_parquet(spark)

    spark.stop()

    print(f"\n{'═' * 50}")
    print("  Pipeline complete.")
    print("  Launch dashboard:  streamlit run app/main.py")
    print(f"{'═' * 50}\n")


if __name__ == "__main__":
    main()
