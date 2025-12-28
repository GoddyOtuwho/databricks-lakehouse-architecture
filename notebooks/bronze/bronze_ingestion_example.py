"""
Bronze Data Ingestion Example (Databricks Lakehouse)
---------------------------------------------------
Purpose:
- Ingest raw enterprise data into the Bronze layer
- Apply schema enforcement and basic data quality checks
- Persist immutable raw data in Delta Lake (append-only pattern)

Notes:
- This is an illustrative reference implementation
- Shows batch ingestion (can be extended to Auto Loader / streaming)
- Replace paths/catalog/schema with your environment conventions
"""

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType

# -----------------------------
# 1) Parameters (environment)
# -----------------------------
RAW_INPUT_PATH = "/mnt/raw/customer_events/"            # e.g., ADLS Gen2 landing zone
BRONZE_TABLE_PATH = "/mnt/bronze/customer_events"       # e.g., ADLS Gen2 bronze zone
BRONZE_TABLE_NAME = "bronze.customer_events"            # optional: Unity Catalog table name

# -----------------------------
# 2) Schema Enforcement
# -----------------------------
event_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_time", TimestampType(), True),
    StructField("amount", DoubleType(), True),
    StructField("source_system", StringType(), True)
])

# -----------------------------
# 3) Read Raw Data (Batch)
# -----------------------------
# Example assumes JSON. Replace with csv/parquet as needed.
df_raw = (
    spark.read
    .schema(event_schema)
    .json(RAW_INPUT_PATH)
)

# -----------------------------
# 4) Basic Data Quality Checks
# -----------------------------
# Keep raw data mostly intact in Bronze, but filter out unusable records
df_bronze = (
    df_raw
    .withColumn("ingest_time", F.current_timestamp())
    .withColumn("ingest_date", F.to_date(F.col("ingest_time")))
    .filter(F.col("event_id").isNotNull())
    .filter(F.col("event_time").isNotNull())
)

# Optional: lightweight normalization (still "raw-ish")
df_bronze = df_bronze.withColumn("event_type", F.upper(F.trim(F.col("event_type"))))

# -----------------------------
# 5) Write Bronze Delta (Append)
# -----------------------------
# Bronze is typically append-only and immutable (no updates)
(
    df_bronze.write
    .format("delta")
    .mode("append")
    .partitionBy("ingest_date")
    .save(BRONZE_TABLE_PATH)
)

# Optional: Register table in catalog (if desired)
# spark.sql(f"CREATE TABLE IF NOT EXISTS {BRONZE_TABLE_NAME} USING DELTA LOCATION '{BRONZE_TABLE_PATH}'")

print("✅ Bronze ingestion complete.")
print(f"Bronze path: {BRONZE_TABLE_PATH}")
