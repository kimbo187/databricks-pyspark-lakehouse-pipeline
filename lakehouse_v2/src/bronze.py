"""
bronze.py
=========
Bronze layer: raw CSV ingestion with audit metadata.

Responsibilities:
  - Read source CSV files with explicit schema options
  - Attach lineage metadata (_source_file, _ingested_at, _ingestion_date)
  - Write to Parquet (or Delta on Databricks) as-is — no cleansing here
  - Log row counts per table for observability
"""

import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)

# Source tables expected in raw_path
_TABLES = ["customers", "products", "orders", "order_items"]


def read_raw_csv(spark: SparkSession, path: str) -> DataFrame:
    """
    Read a raw CSV file with header detection and schema inference.

    Using ``mergeSchema=True`` makes it tolerant of minor schema drift
    between incremental loads.
    """
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .option("mergeSchema", True)
        .option("nullValue", "")          # treat empty strings as NULL
        .option("emptyValue", "")
        .csv(path)
    )


def add_bronze_metadata(df: DataFrame, source_path: str) -> DataFrame:
    """
    Attach audit / lineage columns to a raw DataFrame.

    Columns added:
      _source_file   — exact file path for row-level lineage
      _ingested_at   — UTC timestamp of this ingestion run
      _ingestion_date — date partition for efficient time-range queries
    """
    return (
        df
        .withColumn("_source_file",     F.lit(source_path))
        .withColumn("_ingested_at",     F.current_timestamp())
        .withColumn("_ingestion_date",  F.to_date(F.current_timestamp()))
    )


def write_table(df: DataFrame, path: str, file_format: str = "parquet") -> None:
    """Persist a DataFrame to the lakehouse path, overwriting any previous load."""
    (
        df.write
        .mode("overwrite")
        .format(file_format)
        .save(path)
    )


def run_bronze(
    spark: SparkSession,
    raw_path: str,
    bronze_path: str,
    file_format: str = "parquet",
) -> dict[str, int]:
    """
    Ingest all source CSV files into the Bronze layer.

    Parameters
    ----------
    spark       : active SparkSession
    raw_path    : directory containing the source CSV files
    bronze_path : target directory for Bronze output
    file_format : ``"parquet"`` locally, ``"delta"`` on Databricks

    Returns
    -------
    row_counts : dict mapping table name → number of rows written
    """
    row_counts: dict[str, int] = {}

    for table in _TABLES:
        source = f"{raw_path}/{table}.csv"
        target = f"{bronze_path}/{table}"

        logger.info("Bronze ingesting: %s → %s", source, target)
        df      = read_raw_csv(spark, source)
        df      = add_bronze_metadata(df, source)
        n_rows  = df.count()

        write_table(df, target, file_format)

        row_counts[table] = n_rows
        logger.info("Bronze written: %-15s | %d rows", table, n_rows)
        print(f"  [Bronze] {table:<15} {n_rows:>6} rows  →  {target}")

    return row_counts
