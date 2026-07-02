"""End-to-end ETL orchestration."""

import argparse
from extract import get_spark, read_csv, read_parquet
from transform import (
    drop_duplicates, cast_columns, fill_nulls,
    standardize_strings, add_date_parts, aggregate_metrics,
)
from quality import null_rates, assert_no_nulls, assert_unique


def run_pipeline(
    source_path: str,
    output_path: str,
    source_format: str = "csv",
    group_by: list = None,
    agg_col: str = "amount",
):
    spark = get_spark("ETL-Pipeline")

    # Extract
    print(f"Reading {source_format} from {source_path} ...")
    df = read_csv(spark, source_path) if source_format == "csv" else read_parquet(spark, source_path)
    print(f"Loaded {df.count():,} rows | {len(df.columns)} columns")

    # Quality pre-check
    nrs = null_rates(df)
    high_null = {c: r for c, r in nrs.items() if r > 0.3}
    if high_null:
        print(f"[WARN] High null rate columns: {high_null}")

    # Transform
    df = drop_duplicates(df)
    df = standardize_strings(df, [c for c in df.columns if "name" in c or "category" in c])
    if "date" in df.columns:
        df = add_date_parts(df, "date")

    # Aggregate
    group_by = group_by or ["year", "month"]
    available_group = [c for c in group_by if c in df.columns]
    if available_group and agg_col in df.columns:
        agg = aggregate_metrics(df, available_group, agg_col)
    else:
        agg = df

    # Quality post-check
    assert_no_nulls(df, [agg_col] if agg_col in df.columns else [], raise_on_fail=False)

    # Load
    print(f"Writing output to {output_path} ...")
    agg.write.mode("overwrite").parquet(output_path)
    print("Done.")

    spark.stop()
    return agg


def main():
    parser = argparse.ArgumentParser(description="PySpark ETL Pipeline")
    parser.add_argument("--source_path", required=True)
    parser.add_argument("--output_path", required=True)
    parser.add_argument("--source_format", default="csv")
    parser.add_argument("--group_by", nargs="+", default=["year", "month"])
    parser.add_argument("--agg_col", default="amount")
    args = parser.parse_args()
    run_pipeline(
        args.source_path, args.output_path,
        source_format=args.source_format,
        group_by=args.group_by,
        agg_col=args.agg_col,
    )


if __name__ == "__main__":
    main()