"""Data quality checks — null rates, schema validation, statistical bounds."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import List, Dict, Any


def null_rates(df: DataFrame) -> Dict[str, float]:
    n = df.count()
    rates = {}
    for col in df.columns:
        null_count = df.filter(F.col(col).isNull()).count()
        rates[col] = round(null_count / n, 4)
    return rates


def assert_no_nulls(df: DataFrame, cols: List[str], raise_on_fail: bool = True) -> bool:
    failed = [c for c in cols if df.filter(F.col(c).isNull()).count() > 0]
    if failed:
        msg = f"Null values found in required columns: {failed}"
        if raise_on_fail:
            raise ValueError(msg)
        print(f"[WARN] {msg}")
        return False
    return True


def assert_unique(df: DataFrame, cols: List[str], raise_on_fail: bool = True) -> bool:
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
    out = df.filter((F.col(col) < min_val) | (F.col(col) > max_val)).count()
    if out > 0:
        msg = f"{out} rows outside [{min_val}, {max_val}] in column '{col}'"
        if raise_on_fail:
            raise ValueError(msg)
        print(f"[WARN] {msg}")
        return False
    return True


def schema_report(df: DataFrame) -> None:
    print(f"{'Column':<30} {'Type':<20} {'Nullable'}")
    print("-" * 60)
    for field in df.schema.fields:
        print(f"{field.name:<30} {str(field.dataType):<20} {field.nullable}")