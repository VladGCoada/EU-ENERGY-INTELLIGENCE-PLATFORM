from __future__ import annotations

from typing import Any

try:
    from pyspark.sql import DataFrame, SparkSession
except ImportError:  # pragma: no cover
    DataFrame = Any
    SparkSession = None


def get_spark(app_name: str = "eu-energy-intelligence") -> SparkSession:
    """Create a local Spark session configured for UTC execution."""
    if SparkSession is None:
        raise ImportError("pyspark is not installed.")

    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def write_parquet(df: DataFrame, path: str, mode: str = "overwrite") -> None:
    """Write a DataFrame to Parquet."""
    df.write.mode(mode).parquet(path)
