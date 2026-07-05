"""
Transformation layer — cleaning, standardization and aggregation.

Each function in this module takes a DataFrame and returns a DataFrame.
This makes them composable: you can chain them together or skip any step
without touching the others. It also makes unit testing straightforward —
just build a small test DataFrame and assert on the output.

None of these functions call spark.collect() or trigger actions. They
only add stages to the Spark execution plan, so the actual computation
is lazy and happens when you write the output.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import List, Optional


def drop_duplicates(df: DataFrame, subset: Optional[List[str]] = None) -> DataFrame:
    """
    Remove duplicate rows, optionally considering only a subset of columns.

    Without a subset, two rows are duplicates only if ALL columns match.
    With a subset (e.g. a primary key), duplicates are rows that share
    the same key — useful for deduplicating event streams where a row
    may arrive twice due to at-least-once delivery.

    Args:
        df: Input DataFrame.
        subset: Column names to consider for deduplication. None = all columns.

    Returns:
        Deduplicated DataFrame.
    """
    before = df.count()
    df = df.dropDuplicates(subset)
    print(f"drop_duplicates: removed {before - df.count()} rows")
    return df


def cast_columns(df: DataFrame, casts: dict) -> DataFrame:
    """
    Cast one or more columns to specified Spark types.

    Useful after reading from CSV where everything comes in as StringType.

    Args:
        df: Input DataFrame.
        casts: Dict mapping column name → target Spark type string or
            DataType object (e.g. {"amount": "double", "date": DateType()}).

    Returns:
        DataFrame with cast columns.
    """
    for col, dtype in casts.items():
        df = df.withColumn(col, F.col(col).cast(dtype))
    return df


def fill_nulls(df: DataFrame, fills: dict) -> DataFrame:
    """
    Fill null values with provided defaults, column by column.

    Args:
        df: Input DataFrame.
        fills: Dict mapping column name → fill value
            (e.g. {"amount": 0.0, "category": "unknown"}).

    Returns:
        DataFrame with nulls replaced.
    """
    return df.fillna(fills)


def standardize_strings(df: DataFrame, cols: List[str]) -> DataFrame:
    """
    Lowercase and strip whitespace from string columns.

    This prevents groupBy aggregations from creating separate buckets for
    "France", "france" and " France " — a surprisingly common issue with
    data coming from web forms or manual entry.

    Args:
        df: Input DataFrame.
        cols: List of string column names to standardize.

    Returns:
        DataFrame with cleaned string columns.
    """
    for col in cols:
        df = df.withColumn(col, F.trim(F.lower(F.col(col))))
    return df


def add_date_parts(df: DataFrame, date_col: str) -> DataFrame:
    """
    Decompose a date/timestamp column into year, month and week columns.

    These derived columns are useful for time-based aggregations and
    partitioning output Parquet files by date for efficient downstream reads.

    Args:
        df: Input DataFrame.
        date_col: Name of the date or timestamp column.

    Returns:
        DataFrame with added year, month, week integer columns.
    """
    return (
        df
        .withColumn("year", F.year(date_col))
        .withColumn("month", F.month(date_col))
        .withColumn("week", F.weekofyear(date_col))
    )


def aggregate_metrics(
    df: DataFrame,
    group_by: List[str],
    agg_col: str,
) -> DataFrame:
    """
    Compute count, sum, mean and standard deviation of a numeric column.

    This is the standard aggregation pattern for business reporting — group
    by time or category dimensions, then summarize a metric column.

    Args:
        df: Input DataFrame.
        group_by: Columns to group by (e.g. ["year", "month", "region"]).
        agg_col: Numeric column to aggregate (e.g. "revenue", "amount").

    Returns:
        Aggregated DataFrame with one row per group.
    """
    return df.groupBy(group_by).agg(
        F.count("*").alias("count"),
        F.sum(agg_col).alias(f"{agg_col}_sum"),
        F.mean(agg_col).alias(f"{agg_col}_mean"),
        F.stddev(agg_col).alias(f"{agg_col}_std"),
    )


def join_reference(
    df: DataFrame,
    ref: DataFrame,
    on: str,
    how: str = "left",
    ref_cols: Optional[List[str]] = None,
) -> DataFrame:
    """
    Enrich a DataFrame with columns from a reference/dimension table.

    We use a left join by default — the fact table drives the output,
    and unmatched rows get nulls from the reference (which you can fill
    afterward with fill_nulls). Use how="inner" if you want to drop rows
    that have no match in the reference.

    Args:
        df: Main (fact) DataFrame.
        ref: Reference (dimension) DataFrame.
        on: Column name to join on (must exist in both DataFrames).
        how: Join type: "left", "inner", "right", "full".
        ref_cols: Columns to bring from the reference table. If None,
            all columns are included (may cause column name conflicts).

    Returns:
        Enriched DataFrame.
    """
    if ref_cols:
        ref = ref.select([on] + ref_cols)
    return df.join(ref, on=on, how=how)