"""
Silver Layer — Validation, deduplication, and normalization.

Rules:
  Orders:
    - order_id must not be null             → MISSING_ORDER_ID
    - product_id must not be null           → MISSING_PRODUCT_ID
    - quantity must be > 0 (cast from str)  → INVALID_QUANTITY
    - unit_price must be > 0               → INVALID_PRICE
    - ordered_at must be parseable         → INVALID_TIMESTAMP
    - duplicate order_id (keep latest)     → DUPLICATE_ORDER

  Inventory:
    - product_id must not be null          → MISSING_PRODUCT_ID
    - warehouse must not be null           → MISSING_WAREHOUSE
    - updated_at must be parseable         → INVALID_TIMESTAMP
    - negative quantity is flagged but not quarantined → negative_stock=true

  Price Events:
    - product_id must not be null          → MISSING_PRODUCT_ID
    - old_price must be > 0               → INVALID_OLD_PRICE
    - new_price must be > 0               → INVALID_NEW_PRICE
    - changed_at must be parseable        → INVALID_TIMESTAMP

Invalid records are written to quarantine tables, never silently dropped.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
)

# ---------------------------------------------------------------------------
# Orders
# ---------------------------------------------------------------------------

def validate_orders(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Returns (clean_df, quarantine_df).
    clean_df has proper types; quarantine_df includes _quarantine_reason.
    Deduplication: for duplicate order_ids, keep the row with the latest ordered_at.
    """
    # Cast to proper types; NULL on cast failure is the quarantine signal
    typed = (
        df
        .withColumn("quantity_int", F.col("quantity").cast(LongType()))
        .withColumn("unit_price_dbl", F.col("unit_price").cast(DoubleType()))
        .withColumn("ordered_at_ts", F.try_to_timestamp(F.col("ordered_at")))
    )

    # Validation flags
    typed = (
        typed
        .withColumn("_null_order_id", F.col("order_id").isNull())
        .withColumn("_null_product_id", F.col("product_id").isNull())
        .withColumn("_bad_quantity", F.col("quantity_int").isNull() | (F.col("quantity_int") <= 0))
        .withColumn("_bad_price", F.col("unit_price_dbl").isNull() | (F.col("unit_price_dbl") <= 0))
        .withColumn("_bad_timestamp", F.col("ordered_at_ts").isNull())
    )

    # Build quarantine reason string (first failure wins for labeling)
    typed = typed.withColumn(
        "_quarantine_reason",
        F.when(F.col("_null_order_id"), F.lit("MISSING_ORDER_ID"))
         .when(F.col("_null_product_id"), F.lit("MISSING_PRODUCT_ID"))
         .when(F.col("_bad_quantity"), F.lit("INVALID_QUANTITY"))
         .when(F.col("_bad_price"), F.lit("INVALID_PRICE"))
         .when(F.col("_bad_timestamp"), F.lit("INVALID_TIMESTAMP"))
         .otherwise(F.lit(None).cast(StringType()))
    )

    invalid = typed.filter(F.col("_quarantine_reason").isNotNull())
    valid = typed.filter(F.col("_quarantine_reason").isNull())

    # Deduplicate: window over order_id, keep latest ordered_at
    from pyspark.sql.window import Window
    w = Window.partitionBy("order_id").orderBy(F.col("ordered_at_ts").desc())
    valid = valid.withColumn("_row_num", F.row_number().over(w))

    duplicates = (
        valid.filter(F.col("_row_num") > 1)
             .withColumn("_quarantine_reason", F.lit("DUPLICATE_ORDER"))
    )
    valid = valid.filter(F.col("_row_num") == 1)

    quarantine = invalid.unionByName(duplicates, allowMissingColumns=True)

    # Select with explicit aliases to avoid AMBIGUOUS_REFERENCE from typed columns
    # (typed has both "quantity"/StringType and "quantity_int"/LongType)
    clean = valid.select(
        "order_id", "product_id", "channel",
        F.col("quantity_int").alias("quantity"),
        F.col("unit_price_dbl").alias("unit_price"),
        F.col("ordered_at_ts").alias("ordered_at"),
        "_ingested_at", "_source_file",
    )

    quarantine_out = quarantine.select(
        "order_id", "product_id", "channel",
        "quantity", "unit_price", "ordered_at",
        "_ingested_at", "_source_file", "_quarantine_reason",
    )

    return clean, quarantine_out


def process_orders(
    spark: SparkSession,
    bronze_path: str,
    silver_path: str,
    quarantine_path: str,
) -> tuple[DataFrame, DataFrame]:
    bronze = spark.read.format("delta").load(bronze_path)
    clean, quarantine = validate_orders(bronze)
    clean.write.format("delta").mode("overwrite").save(silver_path)
    quarantine.write.format("delta").mode("overwrite").save(quarantine_path)
    return clean, quarantine


# ---------------------------------------------------------------------------
# Inventory
# ---------------------------------------------------------------------------

def validate_inventory(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    typed = (
        df
        .withColumn("quantity_int", F.col("quantity").cast(LongType()))
        .withColumn("updated_at_ts", F.try_to_timestamp(F.col("updated_at")))
    )

    typed = (
        typed
        .withColumn("_null_product_id", F.col("product_id").isNull())
        .withColumn("_null_warehouse", F.col("warehouse").isNull())
        .withColumn("_bad_timestamp", F.col("updated_at_ts").isNull())
    )

    typed = typed.withColumn(
        "_quarantine_reason",
        F.when(F.col("_null_product_id"), F.lit("MISSING_PRODUCT_ID"))
         .when(F.col("_null_warehouse"), F.lit("MISSING_WAREHOUSE"))
         .when(F.col("_bad_timestamp"), F.lit("INVALID_TIMESTAMP"))
         .otherwise(F.lit(None).cast(StringType()))
    )

    invalid = typed.filter(F.col("_quarantine_reason").isNotNull())
    valid = typed.filter(F.col("_quarantine_reason").isNull())

    # Flag negative stock — valid for data pipeline, but marked
    clean = (
        valid
        .withColumn("negative_stock", F.col("quantity_int") < 0)
        .select(
            "product_id", "warehouse",
            F.col("quantity_int").alias("quantity"),
            F.col("updated_at_ts").alias("updated_at"),
            "negative_stock",
            "_ingested_at", "_source_file",
        )
    )

    quarantine_out = invalid.select(
        "product_id", "warehouse", "quantity", "updated_at",
        "_ingested_at", "_source_file", "_quarantine_reason",
    )

    return clean, quarantine_out


def process_inventory(
    spark: SparkSession,
    bronze_path: str,
    silver_path: str,
    quarantine_path: str,
) -> tuple[DataFrame, DataFrame]:
    bronze = spark.read.format("delta").load(bronze_path)
    clean, quarantine = validate_inventory(bronze)
    clean.write.format("delta").mode("overwrite").save(silver_path)
    quarantine.write.format("delta").mode("overwrite").save(quarantine_path)
    return clean, quarantine


# ---------------------------------------------------------------------------
# Price Events
# ---------------------------------------------------------------------------

def validate_price_events(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    typed = (
        df
        .withColumn("old_price_dbl", F.col("old_price").cast(DoubleType()))
        .withColumn("new_price_dbl", F.col("new_price").cast(DoubleType()))
        .withColumn("changed_at_ts", F.try_to_timestamp(F.col("changed_at")))
    )

    typed = (
        typed
        .withColumn("_null_product_id", F.col("product_id").isNull())
        .withColumn("_bad_old_price", F.col("old_price_dbl").isNull() | (F.col("old_price_dbl") <= 0))
        .withColumn("_bad_new_price", F.col("new_price_dbl").isNull() | (F.col("new_price_dbl") <= 0))
        .withColumn("_bad_timestamp", F.col("changed_at_ts").isNull())
    )

    typed = typed.withColumn(
        "_quarantine_reason",
        F.when(F.col("_null_product_id"), F.lit("MISSING_PRODUCT_ID"))
         .when(F.col("_bad_old_price"), F.lit("INVALID_OLD_PRICE"))
         .when(F.col("_bad_new_price"), F.lit("INVALID_NEW_PRICE"))
         .when(F.col("_bad_timestamp"), F.lit("INVALID_TIMESTAMP"))
         .otherwise(F.lit(None).cast(StringType()))
    )

    invalid = typed.filter(F.col("_quarantine_reason").isNotNull())
    valid = typed.filter(F.col("_quarantine_reason").isNull())

    clean = valid.select(
        "product_id",
        F.col("old_price_dbl").alias("old_price"),
        F.col("new_price_dbl").alias("new_price"),
        F.col("changed_at_ts").alias("changed_at"),
        "source",
        "_ingested_at", "_source_file",
    )

    quarantine_out = invalid.select(
        "product_id", "old_price", "new_price", "changed_at", "source",
        "_ingested_at", "_source_file", "_quarantine_reason",
    )

    return clean, quarantine_out


def process_price_events(
    spark: SparkSession,
    bronze_path: str,
    silver_path: str,
    quarantine_path: str,
) -> tuple[DataFrame, DataFrame]:
    bronze = spark.read.format("delta").load(bronze_path)
    clean, quarantine = validate_price_events(bronze)
    clean.write.format("delta").mode("overwrite").save(silver_path)
    quarantine.write.format("delta").mode("overwrite").save(quarantine_path)
    return clean, quarantine
