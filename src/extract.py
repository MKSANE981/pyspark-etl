"""Data extraction layer — CSV, Parquet, JDBC sources."""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from typing import Optional


def get_spark(app_name: str = "ETL") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )


def read_csv(spark: SparkSession, path: str, **options) -> DataFrame:
    return (
        spark.read
        .options(header=True, inferSchema=True, **options)
        .csv(path)
    )


def read_parquet(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.parquet(path)


def read_jdbc(
    spark: SparkSession,
    url: str,
    table: str,
    user: str,
    password: str,
    driver: str = "com.postgresql.Driver",
    partition_col: Optional[str] = None,
    num_partitions: int = 8,
) -> DataFrame:
    opts = dict(url=url, dbtable=table, user=user, password=password, driver=driver)
    if partition_col:
        bounds = (
            spark.read.format("jdbc").options(**opts).load()
            .agg(F.min(partition_col), F.max(partition_col))
            .collect()[0]
        )
        opts.update(
            partitionColumn=partition_col,
            lowerBound=str(bounds[0]),
            upperBound=str(bounds[1]),
            numPartitions=str(num_partitions),
        )
    return spark.read.format("jdbc").options(**opts).load()