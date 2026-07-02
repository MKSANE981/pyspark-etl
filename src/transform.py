"""Business transformation logic — cleaning, joins, aggregations."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from typing import List, Optional


def drop_duplicates(df: DataFrame, subset: Optional[List[str]] = None) -> DataFrame:
    before = df.count()
    df = df.dropDuplicates(subset)
    print(f"drop_duplicates: removed {before - df.count()} rows")
    return df


def cast_columns(df: DataFrame, casts: dict) -> DataFrame:
    for col, dtype in casts.items():
        df = df.withColumn(col, F.col(col).cast(dtype))
    return df


def fill_nulls(df: DataFrame, fills: dict) -> DataFrame:
    return df.fillna(fills)


def standardize_strings(df: DataFrame, cols: List[str]) -> DataFrame:
    for col in cols:
        df = df.withColumn(col, F.trim(F.lower(F.col(col))))
    return df


def add_date_parts(df: DataFrame, date_col: str) -> DataFrame:
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
    if ref_cols:
        ref = ref.select([on] + ref_cols)
    return df.join(ref, on=on, how=how)