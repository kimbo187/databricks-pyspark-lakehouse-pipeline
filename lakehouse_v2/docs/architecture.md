# Architecture

This project follows a medallion architecture pattern commonly used in lakehouse data platforms.

## Bronze

The Bronze layer stores raw source data with minimal transformation. It adds ingestion metadata to make the data auditable.

## Silver

The Silver layer contains cleaned, typed, deduplicated, and validated data. This is the trusted layer used for further transformation.

## Gold

The Gold layer contains analytics-ready tables designed for reporting, dashboards, and business analysis.

## Data Flow

```text
data/raw/*.csv
    ↓
data/lakehouse/bronze/
    ↓
data/lakehouse/silver/
    ↓
data/lakehouse/gold/
```

## Local vs Databricks

The local version writes Parquet files so it can run without paid cloud resources. In Databricks, the same design can be adapted to Delta Lake by changing the write format to `delta`.
