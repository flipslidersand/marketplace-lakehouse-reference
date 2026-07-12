"""
Gold Layer — Business-oriented datasets.

Tables:
  gold.daily_revenue     — Revenue by date and channel (dedup-safe)
  gold.inventory_risk    — Days of stock and risk classification per SKU
  gold.price_anomalies   — Price change events >= 30% absolute change

All metrics are computed from Silver tables, which are already validated
and deduplicated. Gold never re-introduces raw or invalid records.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ---------------------------------------------------------------------------
# Daily Revenue
# ---------------------------------------------------------------------------

def build_daily_revenue(spark: SparkSession, silver_orders_path: str, output_path: str) -> DataFrame:
    """
    Aggregate clean (deduplicated) orders by date and channel.
    Revenue = quantity * unit_price. Duplicate orders cannot inflate revenue
    because they were removed in the Silver layer.
    """
    orders = spark.read.format("delta").load(silver_orders_path)

    revenue = (
        orders
        .withColumn("date", F.to_date("ordered_at"))
        .groupBy("date", "channel")
        .agg(
            F.count("order_id").alias("order_count"),
            F.sum("quantity").alias("units_sold"),
            F.sum(F.col("quantity") * F.col("unit_price")).alias("gross_revenue"),
        )
        .orderBy("date", "channel")
    )

    revenue.write.format("delta").mode("overwrite").save(output_path)
    return revenue


# ---------------------------------------------------------------------------
# Inventory Risk
# ---------------------------------------------------------------------------

def build_inventory_risk(
    spark: SparkSession,
    silver_orders_path: str,
    silver_inventory_path: str,
    output_path: str,
) -> DataFrame:
    """
    Calculate days-of-stock per product.

    avg_daily_sales = total units sold over the observation window / number of distinct dates
    days_of_stock   = current_quantity / avg_daily_sales

    Risk levels:
      CRITICAL — < 3 days
      HIGH     — < 7 days
      NORMAL   — >= 7 days

    Division by zero is handled explicitly (products with no sales history
    receive risk_level = NORMAL and days_of_stock = NULL).
    """
    orders = spark.read.format("delta").load(silver_orders_path)
    inventory = spark.read.format("delta").load(silver_inventory_path)

    # Sales velocity per product
    sales_velocity = (
        orders
        .withColumn("date", F.to_date("ordered_at"))
        .groupBy("product_id")
        .agg(
            F.sum("quantity").alias("total_units_sold"),
            F.countDistinct("date").alias("active_days"),
        )
        .withColumn(
            "avg_daily_sales",
            F.when(F.col("active_days") > 0,
                   F.col("total_units_sold") / F.col("active_days"))
             .otherwise(F.lit(0.0))
        )
    )

    # Current stock per product (sum across warehouses)
    current_stock = (
        inventory
        .groupBy("product_id")
        .agg(F.sum("quantity").alias("current_quantity"))
    )

    # Join and calculate days of stock
    risk = (
        current_stock
        .join(sales_velocity, on="product_id", how="left")
        .withColumn(
            "days_of_stock",
            F.when(
                F.col("avg_daily_sales").isNotNull() & (F.col("avg_daily_sales") > 0),
                F.col("current_quantity") / F.col("avg_daily_sales")
            ).otherwise(F.lit(None).cast("double"))
        )
        .withColumn(
            "risk_level",
            F.when(F.col("days_of_stock").isNull(), F.lit("NORMAL"))
             .when(F.col("days_of_stock") < 3, F.lit("CRITICAL"))
             .when(F.col("days_of_stock") < 7, F.lit("HIGH"))
             .otherwise(F.lit("NORMAL"))
        )
        .select(
            "product_id",
            "current_quantity",
            "avg_daily_sales",
            "days_of_stock",
            "risk_level",
        )
        .orderBy("days_of_stock")
    )

    risk.write.format("delta").mode("overwrite").save(output_path)
    return risk


# ---------------------------------------------------------------------------
# Price Anomalies
# ---------------------------------------------------------------------------

ANOMALY_THRESHOLD = 0.30


def build_price_anomalies(
    spark: SparkSession,
    silver_price_events_path: str,
    output_path: str,
) -> DataFrame:
    """
    Flag price change events where abs(price_change_ratio) >= 0.30.

    price_change_ratio = (new_price - old_price) / old_price

    Deterministic business rule — no ML. This choice is documented in
    docs/technical-evaluation.md under "Why Deterministic Anomaly Rules".
    """
    events = spark.read.format("delta").load(silver_price_events_path)

    anomalies = (
        events
        .withColumn(
            "price_change_ratio",
            (F.col("new_price") - F.col("old_price")) / F.col("old_price")
        )
        .filter(F.abs(F.col("price_change_ratio")) >= ANOMALY_THRESHOLD)
        .select(
            "product_id",
            "old_price",
            "new_price",
            "price_change_ratio",
            "changed_at",
            "source",
        )
        .orderBy(F.abs(F.col("price_change_ratio")).desc())
    )

    anomalies.write.format("delta").mode("overwrite").save(output_path)
    return anomalies
