"""
Bronze Layer — Raw ingestion into Delta tables.

Responsibilities:
- Ingest raw JSON (JSONL) and CSV data with minimal transformation
- Preserve all original fields including malformed ones
- Add ingestion metadata: _ingested_at, _source_file
- No business validation — Bronze is for traceability and reprocessing
"""

from datetime import datetime, timezone
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, DoubleType,
)


def _ingestion_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


# Raw schemas — intentionally loose to absorb schema drift and bad values.
# Numeric fields use StringType at Bronze so invalid values are not dropped.

ORDER_SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("quantity", StringType(), True),
    StructField("unit_price", StringType(), True),
    StructField("ordered_at", StringType(), True),
    StructField("extra_field", StringType(), True),  # schema drift
])

INVENTORY_SCHEMA = StructType([
    StructField("product_id", StringType(), True),
    StructField("warehouse", StringType(), True),
    StructField("quantity", StringType(), True),
    StructField("updated_at", StringType(), True),
])

PRICE_EVENT_SCHEMA = StructType([
    StructField("product_id", StringType(), True),
    StructField("old_price", StringType(), True),
    StructField("new_price", StringType(), True),
    StructField("changed_at", StringType(), True),
    StructField("source", StringType(), True),
])


def _add_metadata(df: DataFrame, source_file: str) -> DataFrame:
    return df.withColumn("_ingested_at", F.lit(_ingestion_timestamp())) \
             .withColumn("_source_file", F.lit(source_file))


def ingest_orders(spark: SparkSession, source_path: str, output_path: str) -> DataFrame:
    df = (
        spark.read
        .option("mode", "PERMISSIVE")
        .schema(ORDER_SCHEMA)
        .json(source_path)
    )
    df = _add_metadata(df, source_path)
    df.write.format("delta").mode("overwrite").save(output_path)
    return df


def ingest_inventory(spark: SparkSession, source_path: str, output_path: str) -> DataFrame:
    df = (
        spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .schema(INVENTORY_SCHEMA)
        .csv(source_path)
    )
    df = _add_metadata(df, source_path)
    df.write.format("delta").mode("overwrite").save(output_path)
    return df


def ingest_price_events(spark: SparkSession, source_path: str, output_path: str) -> DataFrame:
    df = (
        spark.read
        .option("mode", "PERMISSIVE")
        .schema(PRICE_EVENT_SCHEMA)
        .json(source_path)
    )
    df = _add_metadata(df, source_path)
    df.write.format("delta").mode("overwrite").save(output_path)
    return df
