# Databricks Run Guide

## Option 1: Databricks Repos

1. Create a GitHub repository from this project.
2. Open Databricks.
3. Go to Repos.
4. Clone the GitHub repository.
5. Open `notebooks/databricks_lakehouse_pipeline.py`.
6. Update the base paths to your DBFS, volume, or cloud storage location.

## Option 2: Upload Notebook

1. Upload `notebooks/databricks_lakehouse_pipeline.py`.
2. Upload the files from `data/raw/` to DBFS or a Unity Catalog volume.
3. Adjust `raw_path`, `bronze_path`, `silver_path`, and `gold_path`.
4. Run the notebook cell by cell.

## Delta Lake

In Databricks, replace local Parquet writes with Delta writes:

```python
df.write.format("delta").mode("overwrite").save(path)
```

You can then register tables using SQL:

```sql
CREATE TABLE gold_daily_sales
USING DELTA
LOCATION '/path/to/gold/daily_sales';
```
