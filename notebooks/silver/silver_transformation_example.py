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
- Replace paths/catalog/schema to match your environment and UC conventions.
"""

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# -----------------------------
# 1) Parameters
# -----------------------------
BRONZE_PATH = "/mnt/bronze/customer_events"
SILVER_PATH = "/mnt/silver/customer_events"
SILVER_TABLE = "silver.customer_events"  # optional

# -----------------------------
# 2) Read Bronze Delta
# -----------------------------
df_bronze = spark.read.format("delta").load(BRONZE_PATH)

# -----------------------------
# 3) Clean + Standardize
# -----------------------------
df_clean = (
    df_bronze
    .withColumn("customer_id", F.col("customer_id").cast("string"))
    .withColumn("event_type", F.upper(F.trim(F.col("event_type"))))
    .withColumn("event_time", F.to_timestamp("event_time"))
    .withColumn("amount", F.coalesce(F.col("amount"), F.lit(0.0)))
    .filter(F.col("customer_id").isNotNull())
    .filter(F.col("event_time").isNotNull())
)

# -----------------------------
# 4) Deduplicate
# Example rule: keep latest record per (event_id) if present,
# else per (customer_id, event_time, event_type)
# -----------------------------
has_event_id = "event_id" in df_clean.columns

if has_event_id:
    w = Window.partitionBy("event_id").orderBy(F.col("ingest_time").desc_nulls_last())
    df_silver = (
        df_clean
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )
else:
    w = Window.partitionBy("customer_id", "event_time", "event_type").orderBy(F.col("ingest_time").desc_nulls_last())
    df_silver = (
        df_clean
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

df_silver = df_silver.withColumn("silver_as_of_time", F.current_timestamp())

# -----------------------------
# 5) Write Silver Delta (overwrite for demo; use MERGE for prod)
# -----------------------------
(
    df_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(SILVER_PATH)
)

# Optional: register table
# spark.sql(f"CREATE TABLE IF NOT EXISTS {SILVER_TABLE} USING DELTA LOCATION '{SILVER_PATH}'")

print("✅ Silver transformation complete.")
print(f"Silver path: {SILVER_PATH}")
