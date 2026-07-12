"""
Data quality utilities shared across Silver and Gold layers.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def quarantine_summary(df: DataFrame) -> dict:
    """Return counts by quarantine reason for reporting."""
    rows = (
        df
        .groupBy("_quarantine_reason")
        .count()
        .collect()
    )
    return {r["_quarantine_reason"]: r["count"] for r in rows}


def assert_no_nulls(df: DataFrame, column: str) -> None:
    """Raise if any null exists in column — for test assertions."""
    null_count = df.filter(F.col(column).isNull()).count()
    if null_count > 0:
        raise AssertionError(f"Column '{column}' has {null_count} null values")


def assert_all_positive(df: DataFrame, column: str) -> None:
    """Raise if any value <= 0 in column."""
    bad_count = df.filter(F.col(column) <= 0).count()
    if bad_count > 0:
        raise AssertionError(f"Column '{column}' has {bad_count} non-positive values")
