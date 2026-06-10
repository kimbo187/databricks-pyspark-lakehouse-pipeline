# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks PySpark Lakehouse Pipeline
# MAGIC **Medallion Architecture: Bronze → Silver → Gold**
# MAGIC
# MAGIC Portfolio project demonstrating production-ready data engineering patterns:
# MAGIC - Bronze: raw CSV ingestion + audit metadata
# MAGIC - Silver: type casting, null handling, deduplication, DQ gates
# MAGIC - Gold: 7 analytics-ready tables (daily sales, MoM revenue, CLV, returns, channel)
# MAGIC
# MAGIC *Uses synthetic e-commerce data only — no real customer PII.*

# COMMAND ----------

# MAGIC %md ## 0. Configuration
# MAGIC Update paths to your DBFS mount, Unity Catalog volume, or cloud storage.

# COMMAND ----------

# Adjust these to your Databricks environment
BASE_PATH    = "/FileStore/lakehouse_demo"     # DBFS path
RAW_PATH     = f"{BASE_PATH}/raw"
BRONZE_PATH  = f"{BASE_PATH}/bronze"
SILVER_PATH  = f"{BASE_PATH}/silver"
GOLD_PATH    = f"{BASE_PATH}/gold"
FILE_FORMAT  = "delta"    # Use "parquet" for local; "delta" on Databricks

print(f"Base path  : {BASE_PATH}")
print(f"Format     : {FILE_FORMAT}")

# COMMAND ----------

# MAGIC %md ## 1. Imports

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md ## 2. Bronze Layer
# MAGIC Ingest raw CSV files and attach audit metadata.

# COMMAND ----------

def read_raw_csv(path):
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .option("nullValue", "")
        .csv(path)
    )

def add_bronze_metadata(df, source_path):
    return (
        df
        .withColumn("_source_file",    F.lit(source_path))
        .withColumn("_ingested_at",    F.current_timestamp())
        .withColumn("_ingestion_date", F.to_date(F.current_timestamp()))
    )

def write_table(df, path, fmt=FILE_FORMAT):
    df.write.format(fmt).mode("overwrite").save(path)

tables = ["customers", "products", "orders", "order_items"]
for table in tables:
    src = f"{RAW_PATH}/{table}.csv"
    dst = f"{BRONZE_PATH}/{table}"
    df  = add_bronze_metadata(read_raw_csv(src), src)
    write_table(df, dst)
    print(f"[Bronze] {table:<15}  {df.count():,} rows")

# COMMAND ----------

# MAGIC %md ## 3. Silver Layer
# MAGIC Clean, validate, and deduplicate.

# COMMAND ----------

def clean_customers(df):
    return (
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

def clean_products(df):
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
    return cleaned.filter(F.col("list_price") > 0)

def clean_orders(df):
    return (
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

def clean_order_items(df):
    return (
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
        .filter(F.col("quantity") > 0)
    )

cleaners = {"customers": clean_customers, "products": clean_products,
            "orders": clean_orders, "order_items": clean_order_items}

for table, fn in cleaners.items():
    raw_df = spark.read.format(FILE_FORMAT).load(f"{BRONZE_PATH}/{table}")
    clean  = fn(raw_df)
    write_table(clean, f"{SILVER_PATH}/{table}")
    print(f"[Silver] {table:<15}  {clean.count():,} rows")

# COMMAND ----------

# MAGIC %md ## 4. Gold Layer
# MAGIC Build analytics-ready tables.

# COMMAND ----------

customers   = spark.read.format(FILE_FORMAT).load(f"{SILVER_PATH}/customers")
products    = spark.read.format(FILE_FORMAT).load(f"{SILVER_PATH}/products")
orders      = spark.read.format(FILE_FORMAT).load(f"{SILVER_PATH}/orders")
order_items = spark.read.format(FILE_FORMAT).load(f"{SILVER_PATH}/order_items")

sales_fact = (
    order_items
    .join(orders,    on="order_id",    how="inner")
    .join(products,  on="product_id",  how="left")
    .join(customers, on="customer_id", how="left")
    .withColumn("gross_amount",    F.col("quantity") * F.col("unit_price"))
    .withColumn("discount_amount", F.col("gross_amount") * F.col("discount"))
    .withColumn("net_amount",      F.col("gross_amount") - F.col("discount_amount"))
    .withColumn("order_year",      F.year("order_date"))
    .withColumn("order_month",     F.month("order_date"))
    .withColumn("order_quarter",   F.quarter("order_date"))
)
sales_fact.cache()

# Daily sales
daily_sales = (
    sales_fact.filter(F.col("status") == "completed")
    .groupBy("order_date")
    .agg(F.countDistinct("order_id").alias("orders"),
         F.round(F.sum("net_amount"), 2).alias("revenue"),
         F.round(F.avg("net_amount"), 2).alias("avg_order_value"))
    .orderBy("order_date")
)

# Monthly revenue with MoM growth
w = Window.orderBy("order_year", "order_month")
monthly = (
    sales_fact.filter(F.col("status") == "completed")
    .groupBy("order_year", "order_month")
    .agg(F.countDistinct("order_id").alias("orders"),
         F.round(F.sum("net_amount"), 2).alias("revenue"))
    .withColumn("year_month", F.concat(F.col("order_year"), F.lit("-"),
                                       F.lpad(F.col("order_month"), 2, "0")))
    .withColumn("revenue_prev_month", F.lag("revenue").over(w))
    .withColumn("mom_growth_pct",
                F.round((F.col("revenue") - F.col("revenue_prev_month"))
                        / F.col("revenue_prev_month") * 100, 2))
    .orderBy("order_year","order_month")
)

# Product performance
total_rev = sales_fact.filter(F.col("status")=="completed").agg(F.sum("net_amount")).collect()[0][0] or 1
product_perf = (
    sales_fact.filter(F.col("status") == "completed")
    .groupBy("product_id","product_name","category")
    .agg(F.sum("quantity").alias("units_sold"),
         F.round(F.sum("net_amount"), 2).alias("revenue"),
         F.round(F.avg("discount"), 4).alias("avg_discount_rate"))
    .withColumn("revenue_share_pct", F.round(F.col("revenue") / total_rev * 100, 2))
    .withColumn("revenue_rank", F.rank().over(Window.orderBy(F.col("revenue").desc())))
    .orderBy("revenue_rank")
)

# Customer lifetime value
thresholds = sales_fact.filter(F.col("status")=="completed")\
    .groupBy("customer_id","customer_name","city","country")\
    .agg(F.round(F.sum("net_amount"),2).alias("clv"))\
    .approxQuantile("clv",[0.5,0.67],0.01)
p50, p67 = (thresholds[0] if thresholds else 1000), (thresholds[1] if len(thresholds)>1 else 3000)
clv = (
    sales_fact.filter(F.col("status") == "completed")
    .groupBy("customer_id","customer_name","city","country")
    .agg(F.countDistinct("order_id").alias("completed_orders"),
         F.round(F.sum("net_amount"),2).alias("customer_lifetime_value"),
         F.min("order_date").alias("first_order_date"),
         F.max("order_date").alias("last_order_date"))
    .withColumn("value_segment",
        F.when(F.col("customer_lifetime_value")>=p67,"high_value")
         .when(F.col("customer_lifetime_value")>=p50,"medium_value")
         .otherwise("standard"))
    .orderBy(F.col("customer_lifetime_value").desc())
)

# Returns analysis
all_ord  = sales_fact.groupBy("category").agg(
    F.countDistinct("order_id").alias("total_orders"),
    F.round(F.sum("net_amount"),2).alias("total_revenue"))
returned = sales_fact.filter(F.col("status")=="returned").groupBy("category").agg(
    F.countDistinct("order_id").alias("returned_orders"),
    F.round(F.sum("net_amount"),2).alias("returned_revenue"))
returns_analysis = (
    all_ord.join(returned, on="category", how="left")
    .fillna(0, subset=["returned_orders","returned_revenue"])
    .withColumn("return_rate_pct",
                F.round(F.col("returned_orders")/F.col("total_orders")*100,2))
    .orderBy(F.col("return_rate_pct").desc())
)

# Channel performance
channel_perf = (
    sales_fact.filter(F.col("status") == "completed")
    .groupBy("channel")
    .agg(F.countDistinct("order_id").alias("orders"),
         F.round(F.sum("net_amount"),2).alias("revenue"),
         F.round(F.avg("net_amount"),2).alias("avg_order_value"))
    .orderBy(F.col("revenue").desc())
)

# Write all Gold tables
gold_tables = {
    "sales_fact": sales_fact, "daily_sales": daily_sales,
    "monthly_revenue": monthly, "product_performance": product_perf,
    "customer_lifetime_value": clv, "returns_analysis": returns_analysis,
    "channel_performance": channel_perf,
}
for name, df in gold_tables.items():
    write_table(df, f"{GOLD_PATH}/{name}")
    print(f"[Gold]   {name:<30}  {df.count():,} rows")

sales_fact.unpersist()

# COMMAND ----------

# MAGIC %md ## 5. Quick Sanity Checks

# COMMAND ----------

# Monthly revenue trend
display(spark.read.format(FILE_FORMAT).load(f"{GOLD_PATH}/monthly_revenue")
        .orderBy("order_year","order_month"))

# COMMAND ----------

# Top 10 products
display(spark.read.format(FILE_FORMAT).load(f"{GOLD_PATH}/product_performance").limit(10))

# COMMAND ----------

# Return rates by category
display(spark.read.format(FILE_FORMAT).load(f"{GOLD_PATH}/returns_analysis"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Register as Delta Tables (optional)
# MAGIC Uncomment to register tables in Unity Catalog or the Hive metastore.

# COMMAND ----------

# for name in gold_tables:
#     spark.sql(f"""
#         CREATE TABLE IF NOT EXISTS lakehouse_demo.gold_{name}
#         USING DELTA
#         LOCATION '{GOLD_PATH}/{name}'
#     """)
#     print(f"Registered: gold_{name}")
