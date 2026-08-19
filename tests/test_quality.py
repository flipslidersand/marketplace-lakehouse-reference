"""
Data quality utility tests.
"""

import os
import sys

import pytest
from pyspark.sql.types import LongType, StringType, StructField, StructType

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from marketplace_lakehouse.quality import assert_all_positive, assert_no_nulls, quarantine_summary


class TestQualityUtils:

    def test_quarantine_summary(self, spark):
        schema = StructType([
            StructField("_quarantine_reason", StringType(), True),
        ])
        df = spark.createDataFrame(
            [("MISSING_ORDER_ID",), ("MISSING_ORDER_ID",), ("INVALID_TIMESTAMP",)],
            schema=schema,
        )
        summary = quarantine_summary(df)
        assert summary["MISSING_ORDER_ID"] == 2
        assert summary["INVALID_TIMESTAMP"] == 1

    def test_assert_no_nulls_passes(self, spark):
        schema = StructType([StructField("val", StringType(), True)])
        df = spark.createDataFrame([("a",), ("b",)], schema=schema)
        assert_no_nulls(df, "val")  # should not raise

    def test_assert_no_nulls_fails(self, spark):
        schema = StructType([StructField("val", StringType(), True)])
        df = spark.createDataFrame([("a",), (None,)], schema=schema)
        with pytest.raises(AssertionError):
            assert_no_nulls(df, "val")

    def test_assert_all_positive_passes(self, spark):
        schema = StructType([StructField("qty", LongType(), True)])
        df = spark.createDataFrame([(1,), (5,)], schema=schema)
        assert_all_positive(df, "qty")

    def test_assert_all_positive_fails_on_zero(self, spark):
        schema = StructType([StructField("qty", LongType(), True)])
        df = spark.createDataFrame([(0,), (5,)], schema=schema)
        with pytest.raises(AssertionError):
            assert_all_positive(df, "qty")
