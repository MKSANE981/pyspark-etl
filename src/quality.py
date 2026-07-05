"""
Data quality checks — validation and profiling for Spark DataFrames.

Data quality issues are silent killers in pipelines: they don't raise
exceptions, they just produce wrong numbers downstream. This module makes
quality checks explicit and configurable. Each function either raises an
exception (fail-fast) or prints a warning (soft check), controlled by the
raise_on_fail parameter.

For production pipelines, wire these checks to a monitoring system
(e.g. write results to a quality_log table) rather than just printing.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import List, Dict


def null_rates(df: DataFrame) -> Dict[str, float]:
    """
    Compute the fraction of null values in each column.

    A quick way to get a quality snapshot of a new dataset. Values close
    to 1.0 mean the column is almost entirely empty — likely not useful.
    Values above 0.3 usually warrant investigation before using the column
    in aggregations or models.

    Args:
        df: Input DataFrame.

    Returns:
        Dict mapping column name → null fraction in [0.0, 1.0].
    """
    n = df.count()
    rates = {}
    for col in df.columns:
        null_count = df.filter(F.col(col).isNull()).count()
        rates[col] = round(null_count / n, 4)
    return rates


def assert_no_nulls(df: DataFrame, cols: List[str], raise_on_fail: bool = True) -> bool:
    """
    Assert that required columns contain no null values.

    Use this for columns that must be complete before writing output
    (e.g. primary keys, join keys, amount fields).

    Args:
        df: Input DataFrame.
        cols: List of column names that must be null-free.
        raise_on_fail: If True, raise ValueError on failure.
            If False, print a warning and return False.

    Returns:
        True if all columns pass, False otherwise (when raise_on_fail=False).
    """
    failed = [c for c in cols if df.filter(F.col(c).isNull()).count() > 0]
    if failed:
        msg = f"Null values found in required columns: {failed}"
        if raise_on_fail:
            raise ValueError(msg)
        print(f"[WARN] {msg}")
        return False
    return True


def assert_unique(df: DataFrame, cols: List[str], raise_on_fail: bool = True) -> bool:
    """
    Assert that the combination of given columns is unique across all rows.

    Essential for verifying primary key constraints before loading into a
    target table that enforces uniqueness. Also useful for catching
    duplicate rows introduced by a bad join (fanout explosion).

    Args:
        df: Input DataFrame.
        cols: Columns that together should form a unique key.
        raise_on_fail: Raise or warn on failure.

    Returns:
        True if unique, False otherwise (when raise_on_fail=False).
    """
    total = df.count()
    distinct = df.select(cols).distinct().count()
    if total != distinct:
        msg = f"Uniqueness violated on {cols}: {total} rows, {distinct} distinct"
        if raise_on_fail:
            raise ValueError(msg)
        print(f"[WARN] {msg}")
        return False
    return True


def assert_value_range(
    df: DataFrame,
    col: str,
    min_val: float,
    max_val: float,
    raise_on_fail: bool = True,
) -> bool:
    """
    Assert that all values in a numeric column fall within [min_val, max_val].

    Catches data entry errors (e.g. negative prices, ages above 150) or
    unit mismatches (e.g. euros vs cents). Set raise_on_fail=False to
    log violations without blocking the pipeline.

    Args:
        df: Input DataFrame.
        col: Name of the numeric column to check.
        min_val: Minimum acceptable value (inclusive).
        max_val: Maximum acceptable value (inclusive).
        raise_on_fail: Raise or warn on failure.

    Returns:
        True if all values are in range, False otherwise.
    """
    out = df.filter((F.col(col) < min_val) | (F.col(col) > max_val)).count()
    if out > 0:
        msg = f"{out} rows outside [{min_val}, {max_val}] in column '{col}'"
        if raise_on_fail:
            raise ValueError(msg)
        print(f"[WARN] {msg}")
        return False
    return True


def schema_report(df: DataFrame) -> None:
    """
    Print a readable summary of the DataFrame schema.

    Useful at the start of a pipeline run to confirm that the source
    data matches the expected schema before any transformations begin.
    """
    print(f"{'Column':<30} {'Type':<20} {'Nullable'}")
    print("-" * 60)
    for field in df.schema.fields:
        print(f"{field.name:<30} {str(field.dataType):<20} {field.nullable}")