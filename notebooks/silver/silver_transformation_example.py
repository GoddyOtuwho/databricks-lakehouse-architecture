"""
Silver Transformation Example (Bronze -> Silver)
------------------------------------------------
Purpose:
- Cleanse and validate ingested data (Bronze)
- Deduplicate and standardize schema
- Write curated Silver Delta tables for governed analytics and downstream consumption

Notes:
- This is a minimal reference implementation
- Replace catalog/schema/table names with your environment (Unity Catalog recommended)
"""

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# -----------------------------
# 1) Parameters
# -----------------------------
BRONZE_PATH = "/mnt/bronze/customer_events"
SILVER_PATH = "/mnt/silver/customer_events"
SILVER_TABLE_NAME = "silver.customer_events"  # optional UC table name

# -----------------------------
# 2) Read Bronze Delta
# -----------------------------
df_bronze = spark.read.format("delta").load(BRONZE_PATH)

# -----------------------------
# 3) Standardization & Validation
# -----------------------------
df_clean = (
    df_bronze
    .withColumn("customer_id", F.col("customer_id").cast("string"))
    .withColumn("event_id", F.col("event_id").cast("string"))
    .withColumn("event_time", F.to_timestamp(F.col("event_time")))
    .withColumn("event_type", F.upper(F.trim(F.col("event_type"))))
    .withColumn("source_system", F.upper(F.trim(F.col("source_system"))))
    .withColumn("amount", F.col("amount").cast("double"))
)

# Keep only valid records (example rules)
df_clean = (
    df_clean
    .filter(F.col("customer_id").isNotNull())
    .filter(F.col("event_time").isNotNull())
)

# -----------------------------
# 4) Deduplication
# -----------------------------
# Keep latest record per (event_id) based on ingest_time (or event_time)
w = Window.partitionBy("event_id").orderBy(F.col("ingest_time").desc_nulls_last())

df_silver = (
    df_clean
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

# -----------------------------
# 5) Write Silver Delta
# -----------------------------
(
    df_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(SILVER_PATH)
)

# Optional: Register table in catalog
# spark.sql(f"CREATE TABLE IF NOT EXISTS {SILVER_TABLE_NAME} USING DELTA LOCATION '{SILVER_PATH}'")

print("✅ Silver transformation complete.")
print(f"Silver path: {SILVER_PATH}")
