"""
silver_transformation_example.py
Illustrative example: Bronze -> Silver transformation in a Databricks Lakehouse.

Purpose:
- Cleanse and validate ingested data (Bronze)
- Deduplicate and standardize schema (Silver)
- Write Silver Delta tables for governed analytics and downstream consumption

Notes:
- This is a reference implementation snippet (intentionally minimal).
- Replace paths/catalog/schema with your environment conventions (Unity Catalog recommended).
"""

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# -----------------------------
# Inputs (Bronze)
# -----------------------------
bronze_path = "/mnt/bronze/customer_events"  # example path
df_bronze = spark.read.format("delta").load(bronze_path)

# -----------------------------
# Standardization & Validation
# -----------------------------
df_clean = (
    df_bronze
    .withColumn("event_time", F.to_timestamp("event_time"))
    .withColumn("customer_id", F.col("customer_id").cast("string"))
    .withColumn("event_type", F.upper(F.trim(F.col("event_type"))))
    .withColumn("ingest_date", F.to_date(F.col("ingest_time")))
    .filter(F.col("customer_id").isNotNull())
    .filter(F.col("event_time").isNotNull())
)

# -----------------------------
# Deduplication (example rule)
# Keep latest record per (customer_id, event_time, event_type)
# -----------------------------
w = Window.partitionBy("customer_id", "event_time", "event_type").orderBy(F.col("ingest_time").desc())
df_silver = (
    df_clean
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

# -----------------------------
# Output (Silver)
# -----------------------------
silver_path = "/mnt/silver/customer_events"  # example path
(
    df_silver
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(silver_path)
)

# Optional: register as table (Unity Catalog style)
# spark.sql("CREATE SCHEMA IF NOT EXISTS main.silver")
# spark.sql(f"CREATE TABLE IF NOT EXISTS main.silver.customer_events USING DELTA LOCATION '{silver_path}'")
