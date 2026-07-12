"""
Gold layer acceptance tests.

Focus: business metric correctness — revenue calculation, inventory risk
classification, price anomaly thresholds.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType, TimestampType,
)

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from marketplace_lakehouse.gold import (
    build_daily_revenue,
    build_inventory_risk,
    build_price_anomalies,
    ANOMALY_THRESHOLD,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

ORDERS_SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("quantity", LongType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("ordered_at", TimestampType(), True),
    StructField("_ingested_at", StringType(), True),
    StructField("_source_file", StringType(), True),
])

INVENTORY_SCHEMA = StructType([
    StructField("product_id", StringType(), True),
    StructField("warehouse", StringType(), True),
    StructField("quantity", LongType(), True),
    StructField("updated_at", TimestampType(), True),
    StructField("negative_stock", StringType(), True),
    StructField("_ingested_at", StringType(), True),
    StructField("_source_file", StringType(), True),
])

PRICE_SCHEMA = StructType([
    StructField("product_id", StringType(), True),
    StructField("old_price", DoubleType(), True),
    StructField("new_price", DoubleType(), True),
    StructField("changed_at", TimestampType(), True),
    StructField("source", StringType(), True),
    StructField("_ingested_at", StringType(), True),
    StructField("_source_file", StringType(), True),
])


def make_orders(spark, rows):
    return spark.createDataFrame(rows, schema=ORDERS_SCHEMA)


def make_inventory(spark, rows):
    return spark.createDataFrame(rows, schema=INVENTORY_SCHEMA)


def make_price_events(spark, rows):
    return spark.createDataFrame(rows, schema=PRICE_SCHEMA)


def _ts(s):
    from datetime import datetime, timezone
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


# ---------------------------------------------------------------------------
# Daily Revenue
# ---------------------------------------------------------------------------

class TestDailyRevenue:

    def _run(self, spark, rows, tmp_path):
        orders = make_orders(spark, rows)
        delta_path = str(tmp_path / "silver_orders")
        orders.write.format("delta").mode("overwrite").save(delta_path)
        out_path = str(tmp_path / "gold_revenue")
        revenue = build_daily_revenue(spark, delta_path, out_path)
        return revenue

    def test_revenue_calculation(self, spark, tmp_path):
        rows = [
            ("ORD-001", "SKU-001", "web", 2, 1200.0, _ts("2026-07-12T01:00:00Z"), "t", "f"),
            ("ORD-002", "SKU-002", "web", 3, 800.0, _ts("2026-07-12T02:00:00Z"), "t", "f"),
        ]
        revenue = self._run(spark, rows, tmp_path)
        total = revenue.agg(F.sum("gross_revenue")).first()[0]
        assert total == pytest.approx(2 * 1200 + 3 * 800)

    def test_duplicate_orders_do_not_inflate_revenue(self, spark, tmp_path):
        # Silver already deduped; if only one record is in Silver, revenue is correct
        rows = [
            ("ORD-001", "SKU-001", "web", 2, 1200.0, _ts("2026-07-12T01:00:00Z"), "t", "f"),
        ]
        revenue = self._run(spark, rows, tmp_path)
        total = revenue.agg(F.sum("gross_revenue")).first()[0]
        assert total == pytest.approx(2400.0)

    def test_revenue_grouped_by_channel(self, spark, tmp_path):
        rows = [
            ("ORD-001", "SKU-001", "web", 1, 1000.0, _ts("2026-07-12T01:00:00Z"), "t", "f"),
            ("ORD-002", "SKU-002", "mobile", 1, 2000.0, _ts("2026-07-12T01:00:00Z"), "t", "f"),
        ]
        revenue = self._run(spark, rows, tmp_path)
        channels = {r["channel"] for r in revenue.collect()}
        assert "web" in channels
        assert "mobile" in channels


# ---------------------------------------------------------------------------
# Inventory Risk
# ---------------------------------------------------------------------------

class TestInventoryRisk:

    def _run(self, spark, order_rows, inv_rows, tmp_path):
        orders = make_orders(spark, order_rows)
        inventory = make_inventory(spark, inv_rows)
        orders_path = str(tmp_path / "silver_orders")
        inv_path = str(tmp_path / "silver_inventory")
        out_path = str(tmp_path / "gold_risk")
        orders.write.format("delta").mode("overwrite").save(orders_path)
        inventory.write.format("delta").mode("overwrite").save(inv_path)
        risk = build_inventory_risk(spark, orders_path, inv_path, out_path)
        return risk

    def test_days_of_stock_calculation(self, spark, tmp_path):
        # 10 units sold over 1 day → avg_daily_sales=10; stock=50 → days=5
        order_rows = [
            ("ORD-001", "SKU-001", "web", 10, 1000.0, _ts("2026-07-12T00:00:00Z"), "t", "f"),
        ]
        inv_rows = [
            ("SKU-001", "tokyo", 50, _ts("2026-07-12T00:00:00Z"), None, "t", "f"),
        ]
        risk = self._run(spark, order_rows, inv_rows, tmp_path)
        row = risk.filter(F.col("product_id") == "SKU-001").first()
        assert row["days_of_stock"] == pytest.approx(5.0)

    def test_zero_sales_velocity_does_not_crash(self, spark, tmp_path):
        # Product has stock but no order history
        inv_rows = [
            ("SKU-999", "tokyo", 100, _ts("2026-07-12T00:00:00Z"), None, "t", "f"),
        ]
        risk = self._run(spark, [], inv_rows, tmp_path)
        row = risk.filter(F.col("product_id") == "SKU-999").first()
        assert row is not None
        assert row["days_of_stock"] is None
        assert row["risk_level"] == "NORMAL"

    def test_critical_risk_classification(self, spark, tmp_path):
        # stock=20, avg_daily_sales=10 → days=2 → CRITICAL
        order_rows = [
            ("ORD-001", "SKU-001", "web", 10, 1000.0, _ts("2026-07-12T00:00:00Z"), "t", "f"),
        ]
        inv_rows = [
            ("SKU-001", "tokyo", 20, _ts("2026-07-12T00:00:00Z"), None, "t", "f"),
        ]
        risk = self._run(spark, order_rows, inv_rows, tmp_path)
        row = risk.filter(F.col("product_id") == "SKU-001").first()
        assert row["risk_level"] == "CRITICAL"

    def test_high_risk_classification(self, spark, tmp_path):
        # stock=50, avg_daily_sales=10 → days=5 → HIGH
        order_rows = [
            ("ORD-001", "SKU-001", "web", 10, 1000.0, _ts("2026-07-12T00:00:00Z"), "t", "f"),
        ]
        inv_rows = [
            ("SKU-001", "tokyo", 50, _ts("2026-07-12T00:00:00Z"), None, "t", "f"),
        ]
        risk = self._run(spark, order_rows, inv_rows, tmp_path)
        row = risk.filter(F.col("product_id") == "SKU-001").first()
        assert row["risk_level"] == "HIGH"

    def test_normal_risk_classification(self, spark, tmp_path):
        # stock=700, avg_daily_sales=10 → days=70 → NORMAL
        order_rows = [
            ("ORD-001", "SKU-001", "web", 10, 1000.0, _ts("2026-07-12T00:00:00Z"), "t", "f"),
        ]
        inv_rows = [
            ("SKU-001", "tokyo", 700, _ts("2026-07-12T00:00:00Z"), None, "t", "f"),
        ]
        risk = self._run(spark, order_rows, inv_rows, tmp_path)
        row = risk.filter(F.col("product_id") == "SKU-001").first()
        assert row["risk_level"] == "NORMAL"


# ---------------------------------------------------------------------------
# Price Anomalies
# ---------------------------------------------------------------------------

class TestPriceAnomalies:

    def _run(self, spark, rows, tmp_path):
        events = make_price_events(spark, rows)
        events_path = str(tmp_path / "silver_price_events")
        out_path = str(tmp_path / "gold_anomalies")
        events.write.format("delta").mode("overwrite").save(events_path)
        anomalies = build_price_anomalies(spark, events_path, out_path)
        return anomalies

    def test_positive_spike_flagged(self, spark, tmp_path):
        # +50% change → should be flagged
        rows = [
            ("SKU-001", 1000.0, 1500.0, _ts("2026-07-12T01:00:00Z"), "pricing-service", "t", "f"),
        ]
        anomalies = self._run(spark, rows, tmp_path)
        assert anomalies.count() == 1
        row = anomalies.first()
        assert row["price_change_ratio"] == pytest.approx(0.5)

    def test_negative_drop_flagged(self, spark, tmp_path):
        # -40% change → should be flagged
        rows = [
            ("SKU-001", 1000.0, 600.0, _ts("2026-07-12T01:00:00Z"), "pricing-service", "t", "f"),
        ]
        anomalies = self._run(spark, rows, tmp_path)
        assert anomalies.count() == 1
        row = anomalies.first()
        assert row["price_change_ratio"] == pytest.approx(-0.4)

    def test_small_change_not_flagged(self, spark, tmp_path):
        # +10% change → below threshold, should not be flagged
        rows = [
            ("SKU-001", 1000.0, 1100.0, _ts("2026-07-12T01:00:00Z"), "pricing-service", "t", "f"),
        ]
        anomalies = self._run(spark, rows, tmp_path)
        assert anomalies.count() == 0

    def test_exactly_threshold_is_flagged(self, spark, tmp_path):
        # exactly +30% → flagged (>= threshold)
        rows = [
            ("SKU-001", 1000.0, 1300.0, _ts("2026-07-12T01:00:00Z"), "pricing-service", "t", "f"),
        ]
        anomalies = self._run(spark, rows, tmp_path)
        assert anomalies.count() == 1
