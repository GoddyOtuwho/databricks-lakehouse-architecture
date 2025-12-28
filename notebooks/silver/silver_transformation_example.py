"""
silver_transformation_example.py
Illustrative example: Bronze -> Silver transformation

Purpose:
- Cleanse and validate Bronze data
- Standardize fields
- Deduplicate events
- Write governed Silver Delta tables

Notes:
- Intentionally minimal (reference design).
- Replace paths/catalog/schema to match your environment and Unity Catalog conventions.
"""

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# -----------------------------
# 1) Parameters (edit these)
# -----------------------------
# Prefer table-based reads for enterprise governance (Unity Catalog / Hive metastore).
# If you do not create tables, set USE_TABLES = False to use paths.
USE_TABLES = True

BRONZE_PATH  = "/mnt/bronze/customer_events"
SILVER_PATH  = "/mnt/silver/customer_events"

BRONZE_TABLE = "bronze.customer_events"          # recommended if you register Bronze as a table
SILVER_TABLE = "silver.customer_events_clean"    # recommended if you register Silver as a table

# -----------------------------
# 2) Read Bronze (table-first)
# -----------------------------
if USE_TABLES:
    df_bronze = spark.table(BRONZE_TABLE)
else:
    df_bronze = spark.read.format("delta").load(BRONZE_PATH)

# -----------------------------
# 3) Clean + Standardize
# -----------------------------
df_clean = (
    df_bronze
    .withColumn("customer_id", F.col("customer_id").cast("string"))
    .withColumn("event_type", F.upper(F.trim(F.col("event_type"))))
    # Bronze may store event_time as string; standardize to timestamp in Silver
    .withColumn("event_time", F.to_timestamp(F.col("event_time")))
    # Ensure numeric amount; default null -> 0.0 for illustration
    .withColumn("amount", F.coalesce(F.col("amount").cast("double"), F.lit(0.0)))
    # Basic quality filters (example)
    .filter(F.col("customer_id").isNotNull())
    .filter(F.col("event_time").isNotNull())
    .withColumn("ingest_date", F.to_date(F.col("ingest_time")))
)

# -----------------------------
# 4) Deduplicate
# -----------------------------
# Example rule: keep latest record per (customer_id, event_time, event_type)
w = Window.partitionBy("customer_id", "event_time", "event_type").orderBy(F.col("ingest_time").desc_nulls_last())

df_silver = (
    df_clean
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

# -----------------------------
# 5) Write Silver (path + optional table)
# -----------------------------
(
    df_silver
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(SILVER_PATH)
)

# Optional: register as a table for governed access
if USE_TABLES:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {SILVER_TABLE.split('.')[0]}")
    spark.sql(f"DROP TABLE IF EXISTS {SILVER_TABLE}")
    spark.sql(f"CREATE TABLE {SILVER_TABLE} USING DELTA LOCATION '{SILVER_PATH}'")

print("✅ Silver transformation complete.")
print(f"Silver path: {SILVER_PATH}")
if USE_TABLES:
    print(f"Silver table: {SILVER_TABLE}")
