"""
run_pipeline.py
===============
Entrypoint for the local PySpark Medallion pipeline.

Usage:
    python src/run_pipeline.py [--format parquet|delta]

Runs Bronze → Silver → Gold in sequence, logs row counts at each stage,
and prints a summary table on completion.
"""

import argparse
import logging
import sys
import time
from pathlib import Path

from pyspark.sql import SparkSession

# Allow `python src/run_pipeline.py` from project root
sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import RAW_DATA_PATH, BRONZE_PATH, SILVER_PATH, GOLD_PATH, DEFAULT_FORMAT
from bronze import run_bronze
from silver import run_silver
from gold import run_gold


# ──────────────────────────────────────────────────────────────────
# Logging setup
# ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("pipeline")


# ──────────────────────────────────────────────────────────────────
# Spark session
# ──────────────────────────────────────────────────────────────────

def create_spark_session(app_name: str = "LakehousePipeline") -> SparkSession:
    """Create a local Spark session optimised for the demo dataset size."""
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions", "8")    # right-sized for local demo
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


# ──────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────

def _fmt_duration(seconds: float) -> str:
    return f"{seconds:.1f}s" if seconds < 60 else f"{seconds/60:.1f}m"


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the Lakehouse Medallion pipeline.")
    parser.add_argument(
        "--format", choices=["parquet", "delta"], default=DEFAULT_FORMAT,
        help="Output file format (default: parquet; use delta on Databricks)",
    )
    args = parser.parse_args()
    file_format = args.format

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    pipeline_start = time.time()
    timings: dict[str, float] = {}
    counts: dict[str, dict[str, int]] = {}

    logger.info("=" * 60)
    logger.info("PIPELINE START  format=%s", file_format)
    logger.info("=" * 60)

    # ── Bronze ────────────────────────────────────────────────────
    print("\n── Bronze ──────────────────────────────────────────────")
    t0 = time.time()
    counts["bronze"] = run_bronze(
        spark=spark,
        raw_path=str(RAW_DATA_PATH),
        bronze_path=str(BRONZE_PATH),
        file_format=file_format,
    )
    timings["bronze"] = time.time() - t0
    logger.info("Bronze completed in %s", _fmt_duration(timings["bronze"]))

    # ── Silver ────────────────────────────────────────────────────
    print("\n── Silver ──────────────────────────────────────────────")
    t0 = time.time()
    counts["silver"] = run_silver(
        spark=spark,
        bronze_path=str(BRONZE_PATH),
        silver_path=str(SILVER_PATH),
        file_format=file_format,
    )
    timings["silver"] = time.time() - t0
    logger.info("Silver completed in %s", _fmt_duration(timings["silver"]))

    # ── Gold ──────────────────────────────────────────────────────
    print("\n── Gold ────────────────────────────────────────────────")
    t0 = time.time()
    counts["gold"] = run_gold(
        spark=spark,
        silver_path=str(SILVER_PATH),
        gold_path=str(GOLD_PATH),
        file_format=file_format,
    )
    timings["gold"] = time.time() - t0
    logger.info("Gold completed in %s", _fmt_duration(timings["gold"]))

    # ── Summary ───────────────────────────────────────────────────
    total = time.time() - pipeline_start
    print("\n" + "=" * 60)
    print("PIPELINE SUMMARY")
    print("=" * 60)
    for layer, layer_counts in counts.items():
        print(f"\n  {layer.upper()}")
        for table, n in layer_counts.items():
            print(f"    {table:<30} {n:>8,} rows")
        print(f"    {'duration':<30} {_fmt_duration(timings[layer]):>8}")
    print(f"\n  Total wall time: {_fmt_duration(total)}")
    print("=" * 60)

    spark.stop()
    logger.info("Pipeline finished successfully in %s", _fmt_duration(total))


if __name__ == "__main__":
    main()
