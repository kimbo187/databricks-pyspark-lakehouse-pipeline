"""
silver.py
=========
Silver layer: cleansing, type casting, validation, and deduplication.

Each ``clean_*`` function is a pure transformation — it receives a
Bronze DataFrame and returns a validated Silver DataFrame.
``run_silver`` orchestrates the full layer and enforces data quality gates.

Design principles:
  - Fail fast: DQ assertions run before writes
  - Drop don't corrupt: invalid rows are removed and logged, not coerced
  - Idempotent: re-running produces the same output (mode=overwrite)
"""

import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from data_quality import (
    assert_not_empty,
    assert_no_nulls,
    assert_no_duplicates,
    assert_value_in_set,
    drop_invalid_positive_values,
    drop_nulls_in_columns,
    profile_dataframe,
)

logger = logging.getLogger(__name__)

# Allowed domain values — centralised here so Gold filters stay consistent
ALLOWED_STATUSES = {"completed", "cancelled", "returned", "shipped"}
ALLOWED_CHANNELS = {"web", "mobile", "store", "partner"}


# ──────────────────────────────────────────────────────────────────
# I/O helpers
# ──────────────────────────────────────────────────────────────────

def _read(spark: SparkSession, path: str, fmt: str) -> DataFrame:
    return spark.read.format(fmt).load(path)


def _write(df: DataFrame, path: str, fmt: str) -> None:
    df.write.mode("overwrite").format(fmt).save(path)


# ──────────────────────────────────────────────────────────────────
# Table-level transformations
# ──────────────────────────────────────────────────────────────────

def clean_customers(df: DataFrame) -> DataFrame:
    """
    Silver customers:
      - Cast types, trim whitespace, lower-case email
      - Drop rows with null customer_id or blank email
      - Deduplicate on customer_id (keep first occurrence)
    """
    cleaned = (
        df.select(
            F.col("customer_id").cast("int"),
            F.trim(F.col("customer_name")).alias("customer_name"),
            F.lower(F.trim(F.col("email"))).alias("email"),
            F.trim(F.col("city")).alias("city"),
            F.trim(F.col("country")).alias("country"),
            F.to_date(F.col("signup_date")).alias("signup_date"),
        )
        .filter(F.col("customer_id").isNotNull())
        .filter(F.col("email").isNotNull() & (F.col("email") != ""))
        .dropDuplicates(["customer_id"])
    )
    return cleaned


def clean_products(df: DataFrame) -> DataFrame:
    """
    Silver products:
      - Cast types, trim strings
      - Drop rows with list_price ≤ 0 (data entry errors)
      - Deduplicate on product_id
    """
    cleaned = (
        df.select(
            F.col("product_id").cast("int"),
            F.trim(F.col("product_name")).alias("product_name"),
            F.trim(F.col("category")).alias("category"),
            F.col("list_price").cast("double").alias("list_price"),
        )
        .filter(F.col("product_id").isNotNull())
        .dropDuplicates(["product_id"])
    )
    return drop_invalid_positive_values(cleaned, "list_price")


def clean_orders(df: DataFrame) -> DataFrame:
    """
    Silver orders:
      - Cast types, parse dates, normalise status/channel to lowercase
      - Drop rows with null order_id, customer_id, or order_date
      - Deduplicate on order_id
      - Keep all statuses (completed, cancelled, returned, shipped)
        so that Gold can segment by status correctly
    """
    cleaned = (
        df.select(
            F.col("order_id").cast("int"),
            F.col("customer_id").cast("int"),
            F.to_date(F.col("order_date")).alias("order_date"),
            F.lower(F.trim(F.col("status"))).alias("status"),
            F.lower(F.trim(F.col("channel"))).alias("channel"),
        )
        .filter(F.col("order_id").isNotNull())
        .filter(F.col("customer_id").isNotNull())
        .filter(F.col("order_date").isNotNull())
        .dropDuplicates(["order_id"])
    )
    return cleaned


def clean_order_items(df: DataFrame) -> DataFrame:
    """
    Silver order_items:
      - Cast all numeric types
      - Drop rows with null foreign keys (order_id, product_id)
      - Drop rows with quantity ≤ 0
      - Deduplicate on order_item_id
    """
    cleaned = (
        df.select(
            F.col("order_item_id").cast("int"),
            F.col("order_id").cast("int"),
            F.col("product_id").cast("int"),
            F.col("quantity").cast("int"),
            F.col("unit_price").cast("double"),
            F.col("discount").cast("double"),
        )
        .filter(F.col("order_item_id").isNotNull())
        .filter(F.col("order_id").isNotNull())
        .filter(F.col("product_id").isNotNull())
        .dropDuplicates(["order_item_id"])
    )
    return drop_invalid_positive_values(cleaned, "quantity")


# ──────────────────────────────────────────────────────────────────
# Orchestration
# ──────────────────────────────────────────────────────────────────

def run_silver(
    spark: SparkSession,
    bronze_path: str,
    silver_path: str,
    file_format: str = "parquet",
) -> dict[str, int]:
    """
    Run the full Silver transformation layer.

    For each table:
      1. Read from Bronze
      2. Apply cleaning transformations
      3. Profile the result for observability
      4. Enforce data quality gates (fail fast on violations)
      5. Write to Silver

    Returns
    -------
    row_counts : dict mapping table name → number of rows written
    """
    row_counts: dict[str, int] = {}

    # ── Customers ────────────────────────────────────────────────
    customers = clean_customers(_read(spark, f"{bronze_path}/customers", file_format))
    profile_dataframe(customers, "silver_customers")
    assert_not_empty(customers, "silver_customers")
    assert_no_nulls(customers, ["customer_id", "email"], "silver_customers")
    assert_no_duplicates(customers, ["customer_id"], "silver_customers")
    _write(customers, f"{silver_path}/customers", file_format)
    row_counts["customers"] = customers.count()
    print(f"  [Silver] customers      {row_counts['customers']:>6} rows")

    # ── Products ─────────────────────────────────────────────────
    products = clean_products(_read(spark, f"{bronze_path}/products", file_format))
    profile_dataframe(products, "silver_products")
    assert_not_empty(products, "silver_products")
    assert_no_nulls(products, ["product_id", "product_name"], "silver_products")
    assert_no_duplicates(products, ["product_id"], "silver_products")
    assert_positive_values_check(products, "list_price", "silver_products")
    _write(products, f"{silver_path}/products", file_format)
    row_counts["products"] = products.count()
    print(f"  [Silver] products       {row_counts['products']:>6} rows")

    # ── Orders ───────────────────────────────────────────────────
    orders = clean_orders(_read(spark, f"{bronze_path}/orders", file_format))
    profile_dataframe(orders, "silver_orders")
    assert_not_empty(orders, "silver_orders")
    assert_no_nulls(orders, ["order_id", "customer_id", "order_date"], "silver_orders")
    assert_no_duplicates(orders, ["order_id"], "silver_orders")
    assert_value_in_set(orders, "status", ALLOWED_STATUSES, "silver_orders")
    _write(orders, f"{silver_path}/orders", file_format)
    row_counts["orders"] = orders.count()
    print(f"  [Silver] orders         {row_counts['orders']:>6} rows")

    # ── Order Items ───────────────────────────────────────────────
    order_items = clean_order_items(_read(spark, f"{bronze_path}/order_items", file_format))
    profile_dataframe(order_items, "silver_order_items")
    assert_not_empty(order_items, "silver_order_items")
    assert_no_nulls(order_items, ["order_item_id", "order_id", "product_id"], "silver_order_items")
    assert_no_duplicates(order_items, ["order_item_id"], "silver_order_items")
    _write(order_items, f"{silver_path}/order_items", file_format)
    row_counts["order_items"] = order_items.count()
    print(f"  [Silver] order_items    {row_counts['order_items']:>6} rows")

    return row_counts


# ── thin wrapper so silver.py doesn't import its own sibling ─────
def assert_positive_values_check(df: DataFrame, column: str, table_name: str) -> None:
    from data_quality import assert_positive_values
    assert_positive_values(df, column, table_name)
