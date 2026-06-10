[README.md](https://github.com/user-attachments/files/28787028/README.md)
# Databricks PySpark Lakehouse Pipeline

> **Medallion Architecture** (Bronze → Silver → Gold) built with PySpark.  
> Production patterns: data quality gates, schema validation, logging, MoM analytics, CLV segmentation, returns analysis.  
> Runs locally with Parquet or on Databricks with Delta Lake — no config change required.

---

## What This Demonstrates

| Skill | Where |
|---|---|
| PySpark DataFrame API — joins, window functions, aggregations | `src/gold.py` |
| Layered data architecture (Medallion) | `src/bronze.py` → `src/silver.py` → `src/gold.py` |
| Data quality framework — assertions, profiling, drop-vs-fail | `src/data_quality.py` |
| Schema validation, deduplication, null handling | `src/silver.py` |
| Business analytics — MoM growth, CLV segmentation, return rate | `src/gold.py` |
| Unit testing with PySpark — 30+ tests, fixture-based | `tests/test_data_quality.py` |
| Config-driven pipeline with CLI args and logging | `src/run_pipeline.py` |
| Databricks-ready notebook with Delta Lake support | `notebooks/` |
| Data catalog / lineage documentation | `docs/data_catalog.yaml` |

---

## Architecture

```
Raw CSV
  │
  ▼
┌─────────────────────────────────────────────────────────┐
│  BRONZE  — Raw ingestion + audit metadata               │
│  _source_file  _ingested_at  _ingestion_date            │
│  customers │ products │ orders │ order_items            │
└──────────────────────┬──────────────────────────────────┘
                       │  type cast · deduplicate · validate
                       ▼
┌─────────────────────────────────────────────────────────┐
│  SILVER  — Cleansed, validated, deduplicated            │
│  DQ gates: not_empty · no_nulls · no_dupes              │
│           value_in_set · positive_values                │
│  customers │ products │ orders │ order_items            │
└──────────────────────┬──────────────────────────────────┘
                       │  join · aggregate · window
                       ▼
┌─────────────────────────────────────────────────────────┐
│  GOLD  — Analytics-ready BI tables                      │
│  sales_fact              — line-level fact table        │
│  daily_sales             — revenue + AOV by day         │
│  monthly_revenue         — MoM growth rate              │
│  product_performance     — units sold + revenue rank    │
│  customer_lifetime_value — CLV + value segment          │
│  returns_analysis        — return rate by category      │
│  channel_performance     — web / mobile / store / partner│
└─────────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Component | Technology |
|---|---|
| Compute | PySpark 3.5+ (local or Databricks cluster) |
| Storage | Parquet (local) / Delta Lake (Databricks) |
| Data quality | Custom DQ framework (`src/data_quality.py`) |
| Testing | pytest + PySpark local session |
| Config | `pathlib`-based, environment-agnostic |

---

## Quick Start

### 1. Clone and install

```bash
git clone https://github.com/kimbo187/databricks-pyspark-lakehouse-pipeline.git
cd databricks-pyspark-lakehouse-pipeline
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Run the pipeline

```bash
python src/run_pipeline.py
# or explicitly set output format:
python src/run_pipeline.py --format parquet
```

Sample output:

```
── Bronze ──────────────────────────────────────────────
  [Bronze] customers         504 rows  →  data/lakehouse/bronze/customers
  [Bronze] products           52 rows  →  data/lakehouse/bronze/products
  [Bronze] orders           5020 rows  →  data/lakehouse/bronze/orders
  [Bronze] order_items      8909 rows  →  data/lakehouse/bronze/order_items

── Silver ──────────────────────────────────────────────
  [Silver] customers         487 rows
  [Silver] products           50 rows
  [Silver] orders           4961 rows
  [Silver] order_items      8870 rows

── Gold ────────────────────────────────────────────────
  [Gold]   sales_fact                  8765 rows
  [Gold]   daily_sales                  516 rows
  [Gold]   monthly_revenue              17 rows
  [Gold]   product_performance          50 rows
  [Gold]   customer_lifetime_value     487 rows
  [Gold]   returns_analysis              6 rows
  [Gold]   channel_performance           4 rows

══════════════════════════════════════════════════════════
PIPELINE SUMMARY
  Total wall time: 42.3s
══════════════════════════════════════════════════════════
```

### 3. Run tests

```bash
pytest tests/ -v
```

Runs **30+ tests** covering:
- `assert_not_empty`, `assert_no_nulls`, `assert_positive_values`
- `assert_no_duplicates`, `assert_value_in_set`
- `drop_invalid_positive_values`, `drop_nulls_in_columns`, `profile_dataframe`
- Silver transformations: `clean_customers`, `clean_products`, `clean_orders`, `clean_order_items`

---

## Project Structure

```
.
├── README.md
├── requirements.txt
├── requirements-dev.txt
├── .gitignore
│
├── data/
│   ├── raw/                       # Source CSV files (synthetic data)
│   │   ├── customers.csv          # 500 customers, 20 cities, 15 countries
│   │   ├── products.csv           # 50 products across 6 categories
│   │   ├── orders.csv             # 5 000 orders, Jan 2024 – May 2026
│   │   └── order_items.csv        # ~9 000 line items
│   └── lakehouse/                 # Created at runtime
│       ├── bronze/
│       ├── silver/
│       └── gold/
│
├── src/
│   ├── config.py                  # Path configuration
│   ├── bronze.py                  # Raw ingestion + audit metadata
│   ├── silver.py                  # Cleansing, validation, deduplication
│   ├── gold.py                    # 7 analytics tables
│   ├── data_quality.py            # Reusable DQ checks + profiling
│   └── run_pipeline.py            # CLI entrypoint with logging + summary
│
├── notebooks/
│   └── databricks_lakehouse_pipeline.py   # Databricks-ready notebook
│
├── sql/
│   └── gold_kpi_queries.sql       # 8 BI queries for Gold tables
│
├── docs/
│   ├── architecture.md
│   ├── data_catalog.yaml          # Column-level lineage + DQ rules
│   └── databricks_run_guide.md
│
└── tests/
    └── test_data_quality.py       # 30+ unit tests
```

---

## Gold Tables

| Table | Grain | Key Columns |
|---|---|---|
| `sales_fact` | order line item | net_amount, gross_amount, discount_amount |
| `daily_sales` | day | orders, revenue, avg_order_value |
| `monthly_revenue` | month | revenue, mom_growth_pct, cumulative revenue |
| `product_performance` | product | units_sold, revenue, revenue_rank, revenue_share_pct |
| `customer_lifetime_value` | customer | CLV, value_segment, first/last order |
| `returns_analysis` | category | return_rate_pct, returned_revenue, revenue_at_risk |
| `channel_performance` | channel | orders, revenue, avg_order_value |

---

## Data Quality Framework

`src/data_quality.py` provides composable, logging-enabled checks:

```python
from data_quality import assert_not_empty, assert_no_nulls, assert_no_duplicates

assert_not_empty(df, "silver_orders")
assert_no_nulls(df, ["order_id", "customer_id"], "silver_orders")
assert_no_duplicates(df, ["order_id"], "silver_orders")
assert_value_in_set(df, "status", {"completed","cancelled","returned","shipped"}, "silver_orders")
```

All checks log results via Python's `logging` module and raise `ValueError` on failure — composable as pipeline gates without depending on Spark's internal exception handling.

---

## Databricks

See [`docs/databricks_run_guide.md`](docs/databricks_run_guide.md).

Switch from Parquet to Delta Lake by running:

```bash
python src/run_pipeline.py --format delta
```

Or in the notebook, set `FILE_FORMAT = "delta"` and register as Delta tables:

```sql
CREATE TABLE IF NOT EXISTS lakehouse_demo.gold_daily_sales
USING DELTA
LOCATION '/path/to/gold/daily_sales';
```

---

## Data

All data is **synthetic**. No real customer information is used at any point. The dataset is generated to be realistic in scale and distribution:

- 500 customers across 20 cities in 15 countries
- 50 products in 6 categories
- 5 000 orders spanning Jan 2024 – May 2026
- ~9 000 order line items
- Intentional data quality issues: ~3% null emails, duplicate IDs, ~1% invalid quantities, negative product prices — for DQ demonstration purposes

---

## Contact

- **GitHub:** [@kimbo187](https://github.com/kimbo187)
- **Email:** kamal.tikabo82@gmail.com
