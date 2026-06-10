"""
data_quality.py
===============
Reusable, production-grade data quality checks for PySpark DataFrames.

All checks log results via Python's standard logging module and raise
``ValueError`` on failures, making them composable as pipeline gates.
"""

import logging
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────
# Core assertions
# ──────────────────────────────────────────────────────────────────

def assert_not_empty(df: DataFrame, table_name: str) -> None:
    """Raise ValueError if the DataFrame has zero rows."""
    count = df.count()
    if count == 0:
        raise ValueError(f"[DQ FAIL] {table_name} is empty (0 rows)")
    logger.info("[DQ OK] %s row count = %d", table_name, count)


def assert_no_nulls(df: DataFrame, columns: list[str], table_name: str) -> None:
    """Raise ValueError if any of the specified columns contain null values."""
    # Build a single aggregation pass over all columns — avoids N separate .count() calls
    agg_exprs = [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in columns]
    null_counts = df.agg(*agg_exprs).collect()[0].asDict()

    failures = {col: cnt for col, cnt in null_counts.items() if cnt > 0}
    if failures:
        details = ", ".join(f"{c}={n}" for c, n in failures.items())
        raise ValueError(
            f"[DQ FAIL] {table_name} has null values in required columns: {details}"
        )
    logger.info("[DQ OK] %s — no nulls in %s", table_name, columns)


def assert_positive_values(df: DataFrame, column: str, table_name: str) -> None:
    """Raise ValueError if a numeric column contains values ≤ 0."""
    invalid_count = df.filter(F.col(column) <= 0).count()
    if invalid_count > 0:
        raise ValueError(
            f"[DQ FAIL] {table_name}.{column} has {invalid_count} non-positive values"
        )
    logger.info("[DQ OK] %s.%s — all values positive", table_name, column)


def assert_no_duplicates(df: DataFrame, key_columns: list[str], table_name: str) -> None:
    """Raise ValueError if the key columns contain duplicate combinations."""
    total  = df.count()
    unique = df.select(*key_columns).distinct().count()
    dupes  = total - unique
    if dupes > 0:
        raise ValueError(
            f"[DQ FAIL] {table_name} has {dupes} duplicate rows on key {key_columns}"
        )
    logger.info("[DQ OK] %s — no duplicates on %s", table_name, key_columns)


def assert_value_in_set(
    df: DataFrame,
    column: str,
    allowed_values: set[str],
    table_name: str,
) -> None:
    """Raise ValueError if the column contains values outside the allowed set."""
    invalid = (
        df.filter(~F.col(column).isin(list(allowed_values)))
          .select(column)
          .distinct()
          .rdd.flatMap(lambda r: [r[0]])
          .collect()
    )
    if invalid:
        raise ValueError(
            f"[DQ FAIL] {table_name}.{column} contains unexpected values: {invalid}"
        )
    logger.info("[DQ OK] %s.%s — all values within allowed set", table_name, column)


def assert_schema_matches(
    df: DataFrame,
    expected_schema: StructType,
    table_name: str,
) -> None:
    """Raise ValueError if the DataFrame is missing any expected column names."""
    actual_cols   = {f.name.lower() for f in df.schema.fields}
    expected_cols = {f.name.lower() for f in expected_schema.fields}
    missing = expected_cols - actual_cols
    if missing:
        raise ValueError(
            f"[DQ FAIL] {table_name} is missing expected columns: {sorted(missing)}"
        )
    logger.info("[DQ OK] %s — schema OK", table_name)


# ──────────────────────────────────────────────────────────────────
# Cleaning helpers
# ──────────────────────────────────────────────────────────────────

def drop_invalid_positive_values(df: DataFrame, column: str) -> DataFrame:
    """Return DataFrame with only rows where ``column`` > 0."""
    before = df.count()
    result = df.filter(F.col(column) > 0)
    dropped = before - result.count()
    if dropped:
        logger.warning("Dropped %d rows with non-positive %s", dropped, column)
    return result


def drop_nulls_in_columns(df: DataFrame, columns: list[str]) -> DataFrame:
    """Return DataFrame with rows removed where any of ``columns`` is null."""
    before = df.count()
    result = df.dropna(subset=columns)
    dropped = before - result.count()
    if dropped:
        logger.warning("Dropped %d rows with nulls in %s", dropped, columns)
    return result


# ──────────────────────────────────────────────────────────────────
# Reporting
# ──────────────────────────────────────────────────────────────────

def profile_dataframe(df: DataFrame, table_name: str) -> dict:
    """
    Compute a lightweight profile of the DataFrame.

    Returns a dict with row_count, column_count, null_counts per column,
    and logs a summary. Useful for pipeline observability.
    """
    row_count = df.count()
    col_count = len(df.columns)

    null_exprs = [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns]
    null_counts = df.agg(*null_exprs).collect()[0].asDict()
    cols_with_nulls = {c: n for c, n in null_counts.items() if n > 0}

    profile = {
        "table":            table_name,
        "row_count":        row_count,
        "column_count":     col_count,
        "columns_with_nulls": cols_with_nulls,
    }

    logger.info(
        "[PROFILE] %s | rows=%d | cols=%d | null_cols=%s",
        table_name, row_count, col_count,
        cols_with_nulls if cols_with_nulls else "none",
    )
    return profile
