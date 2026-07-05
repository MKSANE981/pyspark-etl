"""
Extraction layer — read data from CSV, Parquet and JDBC sources into Spark DataFrames.

The extract layer's only job is to get data into Spark. It should not
transform anything — that's transform.py's responsibility. Keeping the
two separate makes each step independently testable and replaceable.

SparkSession configuration: we enable Adaptive Query Execution (AQE) by
default because it improves performance on skewed datasets without any
manual tuning. You can always add more .config() calls before .getOrCreate().
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from typing import Optional


def get_spark(app_name: str = "ETL") -> SparkSession:
    """
    Get or create a SparkSession with sensible defaults.

    In local mode (development), Spark uses all available cores automatically.
    In cluster mode (YARN, Kubernetes), resource allocation is controlled
    by your cluster config — this function just sets application-level options.

    Args:
        app_name: Name shown in the Spark UI. Use something descriptive
            so you can find your job when multiple are running.

    Returns:
        An active SparkSession.
    """
    return (
        SparkSession.builder
        .appName(app_name)
        # AQE dynamically coalesces shuffle partitions — critical for avoiding
        # thousands of tiny output files after a groupBy on skewed data
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )


def read_csv(spark: SparkSession, path: str, **options) -> DataFrame:
    """
    Read one or more CSV files into a DataFrame.

    inferSchema=True triggers a two-pass read (first pass to infer types,
    second to load data) — acceptable for moderate file sizes. For very
    large files, define the schema explicitly with StructType to save time.

    Args:
        spark: Active SparkSession.
        path: Path to a CSV file or a directory of CSVs. Supports glob
            patterns (e.g. "s3://bucket/data/2024-*.csv").
        **options: Additional Spark CSV reader options (e.g. sep=";",
            encoding="latin1", nullValue="N/A").

    Returns:
        Spark DataFrame.
    """
    return (
        spark.read
        .options(header=True, inferSchema=True, **options)
        .csv(path)
    )


def read_parquet(spark: SparkSession, path: str) -> DataFrame:
    """
    Read Parquet files into a DataFrame.

    Parquet is the preferred format for large datasets — it's columnar,
    compressed, and Spark reads only the columns it needs (predicate
    pushdown). If you have a choice between CSV and Parquet, use Parquet.

    Args:
        spark: Active SparkSession.
        path: Path to a Parquet file or directory.

    Returns:
        Spark DataFrame.
    """
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
    """
    Read a database table into a DataFrame via JDBC.

    By default, JDBC reads are single-threaded (one connection, one partition).
    For large tables this is a bottleneck. When partition_col is provided,
    we split the read across num_partitions parallel connections — Spark
    queries each range concurrently. Choose a numeric column with good
    cardinality (e.g. a surrogate key or a timestamp cast to epoch).

    Args:
        spark: Active SparkSession.
        url: JDBC connection string (e.g. "jdbc:postgresql://host:5432/db").
        table: Table name or a subquery in parentheses (e.g. "(SELECT ...) t").
        user: Database username.
        password: Database password. In production, load this from a secret
            manager (Vault, Azure Key Vault) rather than passing it directly.
        driver: JDBC driver class name. The corresponding JAR must be on
            the Spark classpath (--jars or spark.jars config).
        partition_col: Numeric column to use for parallel reads.
        num_partitions: Number of parallel JDBC connections.

    Returns:
        Spark DataFrame.
    """
    opts = dict(url=url, dbtable=table, user=user, password=password, driver=driver)

    if partition_col:
        # We need the actual min/max values to set partition bounds.
        # This requires a quick single-threaded read of just two aggregated values.
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