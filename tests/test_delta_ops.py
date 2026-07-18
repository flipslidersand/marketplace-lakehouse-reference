"""
Delta Lake operations tests.

Focus: the correctness properties an FDE must be able to demonstrate —
idempotent MERGE (safe replay), insert-vs-update semantics, and time travel.
"""

import os
import sys

from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from marketplace_lakehouse.delta_ops import (  # noqa: E402
    latest_version,
    merge_upsert,
    optimize_table,
    read_version,
    table_exists,
    table_history,
    vacuum_table,
)

ORDERS_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", LongType(), True),
        StructField("unit_price", DoubleType(), True),
    ]
)


def _orders(spark, rows):
    return spark.createDataFrame(rows, ORDERS_SCHEMA)


class TestMergeUpsert:
    def test_first_call_creates_table(self, spark, tmp_path):
        path = str(tmp_path / "orders")
        assert not table_exists(spark, path)
        merge_upsert(spark, _orders(spark, [("o1", "p1", 2, 10.0)]), path, ["order_id"])
        assert table_exists(spark, path)
        assert spark.read.format("delta").load(path).count() == 1

    def test_replaying_same_batch_is_idempotent(self, spark, tmp_path):
        path = str(tmp_path / "orders")
        batch = [("o1", "p1", 2, 10.0), ("o2", "p2", 1, 5.0)]
        merge_upsert(spark, _orders(spark, batch), path, ["order_id"])
        # Replay the identical batch twice more.
        merge_upsert(spark, _orders(spark, batch), path, ["order_id"])
        merge_upsert(spark, _orders(spark, batch), path, ["order_id"])
        out = spark.read.format("delta").load(path)
        assert out.count() == 2  # no duplicates from replay

    def test_merge_updates_existing_and_inserts_new(self, spark, tmp_path):
        path = str(tmp_path / "orders")
        merge_upsert(
            spark,
            _orders(spark, [("o1", "p1", 2, 10.0), ("o2", "p2", 1, 5.0)]),
            path,
            ["order_id"],
        )
        # o1 quantity corrected to 9; o3 is new.
        merge_upsert(
            spark,
            _orders(spark, [("o1", "p1", 9, 10.0), ("o3", "p3", 4, 7.0)]),
            path,
            ["order_id"],
        )
        rows = {r["order_id"]: r["quantity"] for r in spark.read.format("delta").load(path).collect()}
        assert rows == {"o1": 9, "o2": 1, "o3": 4}

    def test_missing_key_column_raises(self, spark, tmp_path):
        import pytest

        path = str(tmp_path / "orders")
        with pytest.raises(ValueError):
            merge_upsert(spark, _orders(spark, [("o1", "p1", 1, 1.0)]), path, ["nope"])

    def test_empty_key_cols_raises(self, spark, tmp_path):
        import pytest

        path = str(tmp_path / "orders")
        with pytest.raises(ValueError):
            merge_upsert(spark, _orders(spark, [("o1", "p1", 1, 1.0)]), path, [])


class TestTimeTravel:
    def test_read_version_returns_prior_state(self, spark, tmp_path):
        path = str(tmp_path / "orders")
        merge_upsert(spark, _orders(spark, [("o1", "p1", 2, 10.0)]), path, ["order_id"])
        v0 = latest_version(spark, path)
        merge_upsert(spark, _orders(spark, [("o1", "p1", 99, 10.0)]), path, ["order_id"])

        # Current reflects the update; the pinned version still sees the original.
        current = {r["order_id"]: r["quantity"] for r in read_version(spark, path, latest_version(spark, path)).collect()}
        original = {r["order_id"]: r["quantity"] for r in read_version(spark, path, v0).collect()}
        assert current["o1"] == 99
        assert original["o1"] == 2

    def test_version_advances_on_each_commit(self, spark, tmp_path):
        path = str(tmp_path / "orders")
        merge_upsert(spark, _orders(spark, [("o1", "p1", 1, 1.0)]), path, ["order_id"])
        v0 = latest_version(spark, path)
        merge_upsert(spark, _orders(spark, [("o2", "p2", 1, 1.0)]), path, ["order_id"])
        assert latest_version(spark, path) > v0


class TestHistory:
    def test_history_records_each_commit(self, spark, tmp_path):
        path = str(tmp_path / "orders")
        merge_upsert(spark, _orders(spark, [("o1", "p1", 1, 1.0)]), path, ["order_id"])
        merge_upsert(spark, _orders(spark, [("o2", "p2", 1, 1.0)]), path, ["order_id"])
        hist = table_history(spark, path)
        # At least the create + one merge commit are recorded.
        assert hist.count() >= 2
        ops = {r["operation"] for r in hist.collect()}
        assert any("MERGE" in op or "WRITE" in op for op in ops)


class TestMaintenance:
    """Smoke tests: OPTIMIZE / VACUUM execute and preserve row-level data."""

    def test_optimize_preserves_data(self, spark, tmp_path):
        path = str(tmp_path / "orders")
        # Several commits produce several small files worth compacting.
        for i in range(3):
            merge_upsert(spark, _orders(spark, [(f"o{i}", "p", 1, 1.0)]), path, ["order_id"])
        optimize_table(spark, path)
        assert spark.read.format("delta").load(path).count() == 3

    def test_optimize_zorder_preserves_data(self, spark, tmp_path):
        path = str(tmp_path / "orders")
        merge_upsert(
            spark,
            _orders(spark, [("o1", "p1", 1, 1.0), ("o2", "p2", 1, 1.0)]),
            path,
            ["order_id"],
        )
        optimize_table(spark, path, zorder_by=["product_id"])
        assert spark.read.format("delta").load(path).count() == 2

    def test_vacuum_runs_without_error(self, spark, tmp_path):
        path = str(tmp_path / "orders")
        merge_upsert(spark, _orders(spark, [("o1", "p1", 1, 1.0)]), path, ["order_id"])
        # Default 168h retention deletes nothing but must execute cleanly.
        vacuum_table(spark, path)
        assert spark.read.format("delta").load(path).count() == 1
