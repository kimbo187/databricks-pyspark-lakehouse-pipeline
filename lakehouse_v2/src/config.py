"""Configuration for the lakehouse demo pipeline."""

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

RAW_DATA_PATH = PROJECT_ROOT / "data" / "raw"
LAKEHOUSE_PATH = PROJECT_ROOT / "data" / "lakehouse"

BRONZE_PATH = LAKEHOUSE_PATH / "bronze"
SILVER_PATH = LAKEHOUSE_PATH / "silver"
GOLD_PATH = LAKEHOUSE_PATH / "gold"

DEFAULT_FORMAT = "parquet"  # Use "delta" in Databricks if Delta Lake is available.
