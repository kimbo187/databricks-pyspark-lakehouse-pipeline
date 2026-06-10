"""
test_data_quality.py
====================
Unit tests for data_quality.py and all Silver transformation functions.

Run with:
    pytest tests/ -v

Requires a local PySpark install (see requirements.txt).
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType, DoubleType, DateType,
)

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from data_quality import (
    assert_not_empty,
    assert_no_nulls,
    assert_positive_values,
    assert_no_duplicates,
    assert_value_in_set,
    drop_invalid_positive_values,
    drop_nulls_in_columns,
    profile_dataframe,
)
from silver import clean_customers, clean_products, clean_orders, clean_order_items


# ──────────────────────────────────────────────────────────────────
# Session fixture (shared across all tests for speed)
# ──────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("test_lakehouse")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


# ──────────────────────────────────────────────────────────────────
# Shared fixtures
# ──────────────────────────────────────────────────────────────────

@pytest.fixture
def sample_df(spark):
    schema = StructType([
        StructField("id",     IntegerType(), True),
        StructField("name",   StringType(),  True),
        StructField("amount", DoubleType(),  True),
    ])
    return spark.createDataFrame([(1,"Alice",100.0),(2,"Bob",200.0),(3,"Charlie",300.0)], schema)


@pytest.fixture
def empty_df(spark):
    schema = StructType([StructField("id", IntegerType(), True)])
    return spark.createDataFrame([], schema)


@pytest.fixture
def df_with_nulls(spark):
    schema = StructType([
        StructField("id",     IntegerType(), True),
        StructField("name",   StringType(),  True),
        StructField("amount", DoubleType(),  True),
    ])
    return spark.createDataFrame([(1,"Alice",100.0),(2,None,200.0),(3,"Charlie",None)], schema)


@pytest.fixture
def df_with_negatives(spark):
    schema = StructType([
        StructField("id",     IntegerType(), True),
        StructField("amount", DoubleType(),  True),
    ])
    return spark.createDataFrame([(1,100.0),(2,-50.0),(3,0.0),(4,200.0)], schema)


@pytest.fixture
def df_with_duplicates(spark):
    schema = StructType([
        StructField("id",   IntegerType(), True),
        StructField("name", StringType(),  True),
    ])
    return spark.createDataFrame([(1,"A"),(1,"B"),(2,"C")], schema)


# ──────────────────────────────────────────────────────────────────
# assert_not_empty
# ──────────────────────────────────────────────────────────────────

class TestAssertNotEmpty:
    def test_non_empty_passes(self, sample_df):
        assert_not_empty(sample_df, "t")

    def test_empty_raises(self, empty_df):
        with pytest.raises(ValueError, match="empty"):
            assert_not_empty(empty_df, "t")


# ──────────────────────────────────────────────────────────────────
# assert_no_nulls
# ──────────────────────────────────────────────────────────────────

class TestAssertNoNulls:
    def test_clean_columns_pass(self, sample_df):
        assert_no_nulls(sample_df, ["id", "name"], "t")

    def test_null_in_name_raises(self, df_with_nulls):
        with pytest.raises(ValueError, match="null values"):
            assert_no_nulls(df_with_nulls, ["name"], "t")

    def test_null_in_amount_raises(self, df_with_nulls):
        with pytest.raises(ValueError, match="null values"):
            assert_no_nulls(df_with_nulls, ["amount"], "t")

    def test_reports_all_failing_columns(self, df_with_nulls):
        """Error message should list every failing column."""
        with pytest.raises(ValueError) as exc:
            assert_no_nulls(df_with_nulls, ["name", "amount"], "t")
        assert "name" in str(exc.value) or "amount" in str(exc.value)


# ──────────────────────────────────────────────────────────────────
# assert_positive_values
# ──────────────────────────────────────────────────────────────────

class TestAssertPositiveValues:
    def test_all_positive_passes(self, sample_df):
        assert_positive_values(sample_df, "amount", "t")

    def test_negative_raises(self, df_with_negatives):
        with pytest.raises(ValueError, match="non-positive"):
            assert_positive_values(df_with_negatives, "amount", "t")

    def test_zero_raises(self, df_with_negatives):
        with pytest.raises(ValueError, match="non-positive"):
            assert_positive_values(df_with_negatives, "amount", "t")


# ──────────────────────────────────────────────────────────────────
# assert_no_duplicates
# ──────────────────────────────────────────────────────────────────

class TestAssertNoDuplicates:
    def test_unique_keys_pass(self, sample_df):
        assert_no_duplicates(sample_df, ["id"], "t")

    def test_duplicate_key_raises(self, df_with_duplicates):
        with pytest.raises(ValueError, match="duplicate"):
            assert_no_duplicates(df_with_duplicates, ["id"], "t")


# ──────────────────────────────────────────────────────────────────
# assert_value_in_set
# ──────────────────────────────────────────────────────────────────

class TestAssertValueInSet:
    def test_all_allowed_passes(self, spark):
        df = spark.createDataFrame([("completed",),("cancelled",)], ["status"])
        assert_value_in_set(df, "status", {"completed","cancelled","returned","shipped"}, "t")

    def test_unexpected_value_raises(self, spark):
        df = spark.createDataFrame([("completed",),("unknown_status",)], ["status"])
        with pytest.raises(ValueError):
            assert_value_in_set(df, "status", {"completed","cancelled"}, "t")


# ──────────────────────────────────────────────────────────────────
# drop_invalid_positive_values
# ──────────────────────────────────────────────────────────────────

class TestDropInvalidPositiveValues:
    def test_removes_negatives_and_zeros(self, df_with_negatives):
        result = drop_invalid_positive_values(df_with_negatives, "amount")
        assert result.count() == 2

    def test_keeps_all_when_all_positive(self, sample_df):
        result = drop_invalid_positive_values(sample_df, "amount")
        assert result.count() == 3

    def test_returns_dataframe_type(self, sample_df):
        from pyspark.sql import DataFrame
        assert isinstance(drop_invalid_positive_values(sample_df, "amount"), DataFrame)


# ──────────────────────────────────────────────────────────────────
# drop_nulls_in_columns
# ──────────────────────────────────────────────────────────────────

class TestDropNullsInColumns:
    def test_removes_null_rows(self, df_with_nulls):
        result = drop_nulls_in_columns(df_with_nulls, ["name"])
        assert result.count() == 2

    def test_removes_rows_with_null_in_any_specified_column(self, df_with_nulls):
        result = drop_nulls_in_columns(df_with_nulls, ["name", "amount"])
        assert result.count() == 1   # only Alice has both non-null


# ──────────────────────────────────────────────────────────────────
# profile_dataframe
# ──────────────────────────────────────────────────────────────────

class TestProfileDataframe:
    def test_returns_dict_with_correct_keys(self, sample_df):
        p = profile_dataframe(sample_df, "t")
        assert set(p.keys()) == {"table","row_count","column_count","columns_with_nulls"}

    def test_correct_row_count(self, sample_df):
        assert profile_dataframe(sample_df, "t")["row_count"] == 3

    def test_detects_nulls(self, df_with_nulls):
        p = profile_dataframe(df_with_nulls, "t")
        assert "name" in p["columns_with_nulls"]


# ──────────────────────────────────────────────────────────────────
# Silver transformation functions
# ──────────────────────────────────────────────────────────────────

@pytest.fixture
def raw_customers_df(spark):
    schema = StructType([
        StructField("customer_id",   StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("email",         StringType(), True),
        StructField("city",          StringType(), True),
        StructField("country",       StringType(), True),
        StructField("signup_date",   StringType(), True),
        StructField("_source_file",  StringType(), True),
        StructField("_ingested_at",  StringType(), True),
        StructField("_ingestion_date", StringType(), True),
    ])
    data = [
        ("1", "  Alice  ", "ALICE@Example.COM", "Stockholm", "Sweden", "2024-01-01","f","t","d"),
        ("2", "Bob",        "bob@example.com",   "Oslo",      "Norway", "2024-02-01","f","t","d"),
        ("1", "Alice Dup",  "alice2@example.com","Stockholm", "Sweden", "2024-01-01","f","t","d"),  # dupe id
        (None,"No ID",      "noid@example.com",  "Berlin",   "Germany","2024-03-01","f","t","d"),  # null id
        ("3", "No Email",   None,                "Paris",    "France", "2024-04-01","f","t","d"),  # null email
    ]
    return spark.createDataFrame(data, schema)


class TestCleanCustomers:
    def test_deduplicates_on_customer_id(self, raw_customers_df):
        result = clean_customers(raw_customers_df)
        ids = [r["customer_id"] for r in result.select("customer_id").collect()]
        assert len(ids) == len(set(ids))

    def test_drops_null_customer_id(self, raw_customers_df):
        result = clean_customers(raw_customers_df)
        assert all(r["customer_id"] is not None for r in result.collect())

    def test_drops_null_email(self, raw_customers_df):
        result = clean_customers(raw_customers_df)
        assert all(r["email"] is not None for r in result.collect())

    def test_lowercases_email(self, raw_customers_df):
        result = clean_customers(raw_customers_df)
        emails = [r["email"] for r in result.select("email").collect()]
        assert all(e == e.lower() for e in emails)

    def test_trims_customer_name(self, raw_customers_df):
        result = clean_customers(raw_customers_df)
        names = [r["customer_name"] for r in result.select("customer_name").collect()]
        assert all(n == n.strip() for n in names)


@pytest.fixture
def raw_products_df(spark):
    schema = StructType([
        StructField("product_id",   StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("category",     StringType(), True),
        StructField("list_price",   StringType(), True),
    ])
    data = [
        ("101", "Laptop Stand",  "Accessories", "349.0"),
        ("102", " Wireless Mouse ", "Accessories", "249.0"),
        ("103", "Broken Product", "Test",        "-50.0"),   # negative price
        ("104", "Zero Price",    "Test",          "0.0"),    # zero price
        ("101", "Laptop Dupe",   "Accessories",  "399.0"),   # duplicate id
    ]
    return spark.createDataFrame(data, schema)


class TestCleanProducts:
    def test_removes_negative_price(self, raw_products_df):
        result = clean_products(raw_products_df)
        prices = [r["list_price"] for r in result.select("list_price").collect()]
        assert all(p > 0 for p in prices)

    def test_removes_zero_price(self, raw_products_df):
        result = clean_products(raw_products_df)
        prices = [r["list_price"] for r in result.select("list_price").collect()]
        assert 0.0 not in prices

    def test_deduplicates_product_id(self, raw_products_df):
        result = clean_products(raw_products_df)
        ids = [r["product_id"] for r in result.select("product_id").collect()]
        assert len(ids) == len(set(ids))

    def test_trims_product_name(self, raw_products_df):
        result = clean_products(raw_products_df)
        names = [r["product_name"] for r in result.select("product_name").collect()]
        assert all(n == n.strip() for n in names)


@pytest.fixture
def raw_orders_df(spark):
    schema = StructType([
        StructField("order_id",    StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("order_date",  StringType(), True),
        StructField("status",      StringType(), True),
        StructField("channel",     StringType(), True),
    ])
    data = [
        ("1001","1","2024-01-10","COMPLETED","WEB"),
        ("1002","2","2024-01-11","returned","mobile"),
        ("1001","1","2024-01-10","completed","web"),   # duplicate order_id
        (None,  "3","2024-01-12","cancelled","store"), # null order_id
        ("1003",None,"2024-01-13","shipped","web"),    # null customer_id
    ]
    return spark.createDataFrame(data, schema)


class TestCleanOrders:
    def test_lowercases_status(self, raw_orders_df):
        result = clean_orders(raw_orders_df)
        statuses = [r["status"] for r in result.select("status").collect()]
        assert all(s == s.lower() for s in statuses)

    def test_drops_null_order_id(self, raw_orders_df):
        result = clean_orders(raw_orders_df)
        assert all(r["order_id"] is not None for r in result.collect())

    def test_drops_null_customer_id(self, raw_orders_df):
        result = clean_orders(raw_orders_df)
        assert all(r["customer_id"] is not None for r in result.collect())

    def test_deduplicates_order_id(self, raw_orders_df):
        result = clean_orders(raw_orders_df)
        ids = [r["order_id"] for r in result.select("order_id").collect()]
        assert len(ids) == len(set(ids))

    def test_preserves_returned_status(self, raw_orders_df):
        """Returned orders must survive Silver (they are analysed in Gold)."""
        result = clean_orders(raw_orders_df)
        statuses = {r["status"] for r in result.select("status").collect()}
        assert "returned" in statuses
