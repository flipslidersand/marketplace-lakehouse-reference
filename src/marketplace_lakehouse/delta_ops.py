"""
Delta Lake Operations — the operational primitives a Field/Solutions Engineer
reaches for once a Medallion pipeline is live.

The layer modules (bronze/silver/gold) rebuild each table with an
``overwrite`` for reproducibility. Production ingestion is rarely a full
rebuild, so this module demonstrates the incremental and maintenance
operations that Delta Lake provides on top of plain Parquet:

  * ``merge_upsert``   — idempotent MERGE (re-running a batch never duplicates)
  * ``optimize_table`` — OPTIMIZE (+ optional ZORDER) file compaction
  * ``vacuum_table``   — reclaim storage from tombstoned files
  * ``table_history``  — the Delta transaction log as an audit trail
  * ``read_version`` / ``latest_version`` — time travel

Every function takes an explicit table *path* (this reference stays
path-based rather than catalog-based so it runs identically on a laptop and
on a Databricks workspace).
"""

from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession


def table_exists(spark: SparkSession, path: str) -> bool:
    """True if a Delta table already exists at ``path``."""
    return DeltaTable.isDeltaTable(spark, path)


def merge_upsert(
    spark: SparkSession,
    updates: DataFrame,
    target_path: str,
    key_cols: list[str],
) -> None:
    """Idempotently merge ``updates`` into the Delta table at ``target_path``.

    Rows matching on ``key_cols`` are updated; unmatched rows are inserted.
    The first call creates the table. Because the match is on business keys,
    replaying the same batch is a no-op — the core correctness property that
    lets an ingestion job retry safely after a partial failure.

    Example
    -------
    >>> merge_upsert(spark, clean_orders, silver_orders_path, ["order_id"])
    """
    if not key_cols:
        raise ValueError("key_cols must contain at least one column")

    missing = [c for c in key_cols if c not in updates.columns]
    if missing:
        raise ValueError(f"key_cols not present in updates DataFrame: {missing}")

    if not table_exists(spark, target_path):
        updates.write.format("delta").mode("overwrite").save(target_path)
        return

    target = DeltaTable.forPath(spark, target_path)
    condition = " AND ".join(f"t.{c} = s.{c}" for c in key_cols)
    (
        target.alias("t")
        .merge(updates.alias("s"), condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def optimize_table(
    spark: SparkSession,
    path: str,
    zorder_by: list[str] | None = None,
) -> None:
    """Compact small files with OPTIMIZE, optionally clustering with ZORDER.

    Bin-packing many small files into fewer large ones is the first lever an
    FDE pulls when read latency degrades. ZORDER co-locates rows by the given
    columns so that predicate push-down skips more files.
    """
    target = f"delta.`{path}`"
    if zorder_by:
        cols = ", ".join(zorder_by)
        spark.sql(f"OPTIMIZE {target} ZORDER BY ({cols})")
    else:
        spark.sql(f"OPTIMIZE {target}")


def vacuum_table(spark: SparkSession, path: str, retention_hours: float = 168.0) -> None:
    """Physically delete files tombstoned longer than ``retention_hours``.

    Defaults to 168h (7 days) — the same floor Delta enforces to avoid
    breaking in-flight readers and time-travel guarantees.
    """
    DeltaTable.forPath(spark, path).vacuum(retention_hours)


def table_history(spark: SparkSession, path: str, limit: int = 20) -> DataFrame:
    """Return the most recent commits from the Delta transaction log.

    Each row is one atomic commit (operation, timestamp, metrics) — the
    built-in audit trail an FDE points to when asked "who changed this table?".
    """
    return DeltaTable.forPath(spark, path).history(limit)


def latest_version(spark: SparkSession, path: str) -> int:
    """Latest committed version number of the Delta table."""
    return DeltaTable.forPath(spark, path).history(1).collect()[0]["version"]


def read_version(spark: SparkSession, path: str, version: int) -> DataFrame:
    """Time-travel read: the table exactly as it was at ``version``."""
    return spark.read.format("delta").option("versionAsOf", version).load(path)
