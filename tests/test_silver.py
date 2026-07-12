"""
Silver layer acceptance tests.

Focus: explicit business acceptance criteria, not line coverage.
Each test verifies one specific data quality rule.
"""

import pytest
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType, TimestampType,
)

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from marketplace_lakehouse.silver import validate_orders, validate_inventory, validate_price_events


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_order(spark: SparkSession, **overrides) -> object:
    defaults = {
        "order_id": "ORD-99999",
        "product_id": "SKU-001",
        "channel": "web",
        "quantity": "2",
        "unit_price": "1200",
        "ordered_at": "2026-07-12T01:23:00Z",
        "_ingested_at": "2026-07-12T09:00:00Z",
        "_source_file": "test",
        "extra_field": None,
    }
    defaults.update(overrides)
    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("quantity", StringType(), True),
        StructField("unit_price", StringType(), True),
        StructField("ordered_at", StringType(), True),
        StructField("_ingested_at", StringType(), True),
        StructField("_source_file", StringType(), True),
        StructField("extra_field", StringType(), True),
    ])
    return spark.createDataFrame([defaults], schema=schema)


# ---------------------------------------------------------------------------
# Orders — quarantine tests
# ---------------------------------------------------------------------------

class TestOrdersQuarantine:

    def test_null_order_id_is_quarantined(self, spark):
        df = make_order(spark, order_id=None)
        clean, quarantine = validate_orders(df)
        assert clean.count() == 0
        assert quarantine.count() == 1
        reason = quarantine.select("_quarantine_reason").first()[0]
        assert reason == "MISSING_ORDER_ID"

    def test_null_product_id_is_quarantined(self, spark):
        df = make_order(spark, product_id=None)
        clean, quarantine = validate_orders(df)
        assert clean.count() == 0
        assert quarantine.filter(
            F.col("_quarantine_reason") == "MISSING_PRODUCT_ID"
        ).count() == 1

    def test_zero_quantity_is_quarantined(self, spark):
        df = make_order(spark, quantity="0")
        clean, quarantine = validate_orders(df)
        assert clean.count() == 0
        assert quarantine.filter(
            F.col("_quarantine_reason") == "INVALID_QUANTITY"
        ).count() == 1

    def test_negative_quantity_is_quarantined(self, spark):
        df = make_order(spark, quantity="-3")
        clean, quarantine = validate_orders(df)
        assert clean.count() == 0
        assert quarantine.filter(
            F.col("_quarantine_reason") == "INVALID_QUANTITY"
        ).count() == 1

    def test_invalid_timestamp_is_quarantined(self, spark):
        df = make_order(spark, ordered_at="NOT-A-DATE")
        clean, quarantine = validate_orders(df)
        assert clean.count() == 0
        assert quarantine.filter(
            F.col("_quarantine_reason") == "INVALID_TIMESTAMP"
        ).count() == 1

    def test_valid_order_passes_through(self, spark):
        df = make_order(spark)
        clean, quarantine = validate_orders(df)
        assert clean.count() == 1
        assert quarantine.count() == 0

    def test_duplicate_order_ids_deduplicated(self, spark):
        schema = StructType([
            StructField("order_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("channel", StringType(), True),
            StructField("quantity", StringType(), True),
            StructField("unit_price", StringType(), True),
            StructField("ordered_at", StringType(), True),
            StructField("_ingested_at", StringType(), True),
            StructField("_source_file", StringType(), True),
            StructField("extra_field", StringType(), True),
        ])
        rows = [
            ("ORD-001", "SKU-001", "web", "1", "1000", "2026-07-12T01:00:00Z", "t", "f", None),
            ("ORD-001", "SKU-001", "web", "2", "1000", "2026-07-12T02:00:00Z", "t", "f", None),
            ("ORD-002", "SKU-002", "mobile", "3", "2000", "2026-07-12T01:00:00Z", "t", "f", None),
        ]
        df = spark.createDataFrame(rows, schema=schema)
        clean, quarantine = validate_orders(df)
        # Only one ORD-001 should survive (latest: 02:00)
        assert clean.count() == 2
        dup_order = clean.filter(F.col("order_id") == "ORD-001").first()
        # Later record has quantity=2; time string comparison is timezone-dependent
        assert dup_order["quantity"] == 2
        # The earlier duplicate is quarantined
        assert quarantine.filter(
            F.col("_quarantine_reason") == "DUPLICATE_ORDER"
        ).count() == 1


# ---------------------------------------------------------------------------
# Inventory
# ---------------------------------------------------------------------------

class TestInventoryValidation:

    def _make_inv(self, spark, **overrides):
        defaults = {
            "product_id": "SKU-001",
            "warehouse": "tokyo",
            "quantity": "100",
            "updated_at": "2026-07-12T01:00:00Z",
            "_ingested_at": "2026-07-12T09:00:00Z",
            "_source_file": "test",
        }
        defaults.update(overrides)
        schema = StructType([
            StructField(k, StringType(), True) for k in defaults
        ])
        return spark.createDataFrame([defaults], schema=schema)

    def test_null_product_id_quarantined(self, spark):
        df = self._make_inv(spark, product_id=None)
        clean, quarantine = validate_inventory(df)
        assert clean.count() == 0
        assert quarantine.filter(
            F.col("_quarantine_reason") == "MISSING_PRODUCT_ID"
        ).count() == 1

    def test_invalid_timestamp_quarantined(self, spark):
        df = self._make_inv(spark, updated_at="BAD")
        clean, quarantine = validate_inventory(df)
        assert clean.count() == 0
        assert quarantine.filter(
            F.col("_quarantine_reason") == "INVALID_TIMESTAMP"
        ).count() == 1

    def test_negative_stock_flagged_not_quarantined(self, spark):
        df = self._make_inv(spark, quantity="-5")
        clean, quarantine = validate_inventory(df)
        assert clean.count() == 1
        assert clean.filter(F.col("negative_stock") == True).count() == 1
        assert quarantine.count() == 0


# ---------------------------------------------------------------------------
# Price Events
# ---------------------------------------------------------------------------

class TestPriceEventsValidation:

    def _make_pe(self, spark, **overrides):
        defaults = {
            "product_id": "SKU-001",
            "old_price": "1000",
            "new_price": "1200",
            "changed_at": "2026-07-12T01:00:00Z",
            "source": "pricing-service",
            "_ingested_at": "2026-07-12T09:00:00Z",
            "_source_file": "test",
        }
        defaults.update(overrides)
        schema = StructType([StructField(k, StringType(), True) for k in defaults])
        return spark.createDataFrame([defaults], schema=schema)

    def test_null_product_id_quarantined(self, spark):
        df = self._make_pe(spark, product_id=None)
        clean, quarantine = validate_price_events(df)
        assert quarantine.filter(
            F.col("_quarantine_reason") == "MISSING_PRODUCT_ID"
        ).count() == 1

    def test_zero_old_price_quarantined(self, spark):
        df = self._make_pe(spark, old_price="0")
        clean, quarantine = validate_price_events(df)
        assert quarantine.filter(
            F.col("_quarantine_reason") == "INVALID_OLD_PRICE"
        ).count() == 1

    def test_invalid_timestamp_quarantined(self, spark):
        df = self._make_pe(spark, changed_at="INVALID")
        clean, quarantine = validate_price_events(df)
        assert quarantine.filter(
            F.col("_quarantine_reason") == "INVALID_TIMESTAMP"
        ).count() == 1

    def test_valid_event_passes(self, spark):
        df = self._make_pe(spark)
        clean, quarantine = validate_price_events(df)
        assert clean.count() == 1
        assert quarantine.count() == 0
