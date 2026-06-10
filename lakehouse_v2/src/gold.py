"""
gold.py
=======
Gold layer: business-intelligence and analytics-ready tables.

All tables are derived from the Silver layer. The Gold layer is the
single source of truth for dashboards, SQL analytics, and BI tools.

Tables produced:
  sales_fact              — line-level fact table for all completed/returned/shipped orders
  daily_sales             — revenue and order volume aggregated by day
  monthly_revenue         — month-over-month revenue with growth rate
  product_performance     — units sold and revenue per product, ranked
  customer_lifetime_value — CLV per customer with value segment label
  returns_analysis        — return rate and lost revenue per category
  channel_performance     — order volume and revenue split by sales channel
"""

import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────
# I/O helpers
# ──────────────────────────────────────────────────────────────────

def _read(spark: SparkSession, path: str, fmt: str) -> DataFrame:
    return spark.read.format(fmt).load(path)


def _write(df: DataFrame, path: str, fmt: str) -> None:
    df.write.mode("overwrite").format(fmt).save(path)


# ──────────────────────────────────────────────────────────────────
# Fact table
# ──────────────────────────────────────────────────────────────────

def build_sales_fact(
    orders: DataFrame,
    order_items: DataFrame,
    products: DataFrame,
    customers: DataFrame,
) -> DataFrame:
    """
    Joined line-level fact table.

    Includes ALL order statuses so that Gold aggregations can segment
    by status (completed revenue, returned revenue, etc.) without
    re-joining Bronze or Silver.

    Derived columns:
      gross_amount    = quantity × unit_price
      discount_amount = gross_amount × discount
      net_amount      = gross_amount − discount_amount
      order_year      = year(order_date)      — for YoY analysis
      order_month     = month(order_date)     — for seasonality
      order_quarter   = quarter(order_date)   — for QoQ analysis
    """
    return (
        order_items
        .join(orders,    on="order_id",    how="inner")
        .join(products,  on="product_id",  how="left")
        .join(customers, on="customer_id", how="left")
        .withColumn("gross_amount",    F.col("quantity") * F.col("unit_price"))
        .withColumn("discount_amount", F.col("gross_amount") * F.col("discount"))
        .withColumn("net_amount",      F.col("gross_amount") - F.col("discount_amount"))
        .withColumn("order_year",      F.year(F.col("order_date")))
        .withColumn("order_month",     F.month(F.col("order_date")))
        .withColumn("order_quarter",   F.quarter(F.col("order_date")))
    )


# ──────────────────────────────────────────────────────────────────
# Aggregation tables
# ──────────────────────────────────────────────────────────────────

def build_daily_sales(sales_fact: DataFrame) -> DataFrame:
    """Daily revenue and order count for completed orders."""
    return (
        sales_fact
        .filter(F.col("status") == "completed")
        .groupBy("order_date")
        .agg(
            F.countDistinct("order_id").alias("orders"),
            F.round(F.sum("net_amount"), 2).alias("revenue"),
            F.round(F.avg("net_amount"), 2).alias("avg_order_value"),
        )
        .orderBy("order_date")
    )


def build_monthly_revenue(sales_fact: DataFrame) -> DataFrame:
    """
    Month-over-month revenue with growth rate.

    Adds ``revenue_prev_month`` and ``mom_growth_pct`` so that dashboards
    can show trend lines without post-processing SQL.
    """
    monthly = (
        sales_fact
        .filter(F.col("status") == "completed")
        .groupBy("order_year", "order_month")
        .agg(
            F.countDistinct("order_id").alias("orders"),
            F.round(F.sum("net_amount"), 2).alias("revenue"),
        )
        .withColumn(
            "year_month",
            F.concat(F.col("order_year"), F.lit("-"), F.lpad(F.col("order_month"), 2, "0")),
        )
        .orderBy("order_year", "order_month")
    )

    w = Window.orderBy("order_year", "order_month")
    return (
        monthly
        .withColumn("revenue_prev_month", F.lag("revenue").over(w))
        .withColumn(
            "mom_growth_pct",
            F.round(
                (F.col("revenue") - F.col("revenue_prev_month"))
                / F.col("revenue_prev_month") * 100,
                2,
            ),
        )
    )


def build_product_performance(sales_fact: DataFrame) -> DataFrame:
    """Product-level KPIs ranked by revenue, with revenue share."""
    base = (
        sales_fact
        .filter(F.col("status") == "completed")
        .groupBy("product_id", "product_name", "category")
        .agg(
            F.sum("quantity").alias("units_sold"),
            F.round(F.sum("net_amount"), 2).alias("revenue"),
            F.round(F.avg("discount"), 4).alias("avg_discount_rate"),
        )
    )
    total_revenue = base.agg(F.sum("revenue")).collect()[0][0] or 1.0
    return (
        base
        .withColumn("revenue_share_pct", F.round(F.col("revenue") / total_revenue * 100, 2))
        .withColumn("revenue_rank", F.rank().over(Window.orderBy(F.col("revenue").desc())))
        .orderBy("revenue_rank")
    )


def build_customer_lifetime_value(sales_fact: DataFrame) -> DataFrame:
    """
    CLV per customer with value segmentation.

    Segments:
      high_value   : CLV ≥ top-33rd percentile
      medium_value : CLV ≥ median
      standard     : CLV < median
    """
    clv = (
        sales_fact
        .filter(F.col("status") == "completed")
        .groupBy("customer_id", "customer_name", "city", "country")
        .agg(
            F.countDistinct("order_id").alias("completed_orders"),
            F.round(F.sum("net_amount"), 2).alias("customer_lifetime_value"),
            F.min("order_date").alias("first_order_date"),
            F.max("order_date").alias("last_order_date"),
        )
    )

    # Percentile thresholds computed in a single pass
    thresholds = clv.approxQuantile("customer_lifetime_value", [0.5, 0.67], 0.01)
    p50, p67   = (thresholds[0] if thresholds else 1000), (thresholds[1] if len(thresholds) > 1 else 3000)

    return (
        clv
        .withColumn(
            "value_segment",
            F.when(F.col("customer_lifetime_value") >= p67, "high_value")
             .when(F.col("customer_lifetime_value") >= p50, "medium_value")
             .otherwise("standard"),
        )
        .orderBy(F.col("customer_lifetime_value").desc())
    )


def build_returns_analysis(sales_fact: DataFrame) -> DataFrame:
    """
    Return rate and lost revenue aggregated by product category.

    Useful for identifying product quality issues and informing
    inventory and pricing decisions.
    """
    all_orders = (
        sales_fact
        .groupBy("category")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.round(F.sum("net_amount"), 2).alias("total_revenue"),
        )
    )
    returned = (
        sales_fact
        .filter(F.col("status") == "returned")
        .groupBy("category")
        .agg(
            F.countDistinct("order_id").alias("returned_orders"),
            F.round(F.sum("net_amount"), 2).alias("returned_revenue"),
        )
    )
    return (
        all_orders
        .join(returned, on="category", how="left")
        .fillna(0, subset=["returned_orders", "returned_revenue"])
        .withColumn(
            "return_rate_pct",
            F.round(F.col("returned_orders") / F.col("total_orders") * 100, 2),
        )
        .orderBy(F.col("return_rate_pct").desc())
    )


def build_channel_performance(sales_fact: DataFrame) -> DataFrame:
    """Revenue, orders, and AOV by sales channel for completed orders."""
    return (
        sales_fact
        .filter(F.col("status") == "completed")
        .groupBy("channel")
        .agg(
            F.countDistinct("order_id").alias("orders"),
            F.round(F.sum("net_amount"), 2).alias("revenue"),
            F.round(F.avg("net_amount"), 2).alias("avg_order_value"),
        )
        .orderBy(F.col("revenue").desc())
    )


# ──────────────────────────────────────────────────────────────────
# Orchestration
# ──────────────────────────────────────────────────────────────────

def run_gold(
    spark: SparkSession,
    silver_path: str,
    gold_path: str,
    file_format: str = "parquet",
) -> dict[str, int]:
    """
    Build and persist all Gold analytics tables.

    Returns
    -------
    row_counts : dict mapping table name → number of rows written
    """
    customers   = _read(spark, f"{silver_path}/customers",   file_format)
    products    = _read(spark, f"{silver_path}/products",    file_format)
    orders      = _read(spark, f"{silver_path}/orders",      file_format)
    order_items = _read(spark, f"{silver_path}/order_items", file_format)

    sales_fact = build_sales_fact(orders, order_items, products, customers)
    # Cache — reused by every downstream aggregation
    sales_fact.cache()
    logger.info("sales_fact cached: %d rows", sales_fact.count())

    tables = {
        "sales_fact":              sales_fact,
        "daily_sales":             build_daily_sales(sales_fact),
        "monthly_revenue":         build_monthly_revenue(sales_fact),
        "product_performance":     build_product_performance(sales_fact),
        "customer_lifetime_value": build_customer_lifetime_value(sales_fact),
        "returns_analysis":        build_returns_analysis(sales_fact),
        "channel_performance":     build_channel_performance(sales_fact),
    }

    row_counts: dict[str, int] = {}
    for name, df in tables.items():
        _write(df, f"{gold_path}/{name}", file_format)
        row_counts[name] = df.count()
        print(f"  [Gold]   {name:<30} {row_counts[name]:>6} rows")

    sales_fact.unpersist()
    return row_counts
