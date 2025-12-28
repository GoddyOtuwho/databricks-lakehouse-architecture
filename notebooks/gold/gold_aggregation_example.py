"""
gold_aggregation_example.py
Illustrative example: Silver -> Gold aggregation

Purpose:
- Create analytics-ready Gold datasets
- Apply business-level aggregations
- Optimize for BI / ML consumption

Notes:
- Intentionally minimal (reference design)
- Replace catalog/schema as needed
"""

from pyspark.sql import functions as F

# -----------------------------
# Parameters
# -----------------------------
USE_TABLES = True

SILVER_PATH = "/mnt/silver/customer_events"
GOLD_PATH = "/mnt/gold/customer_metrics"

SILVER_TABLE = "silver.customer_events_clean"
GOLD_TABLE = "gold.customer_metrics"

# -----------------------------
# Read Silver
# -----------------------------
if USE_TABLES:
    df_silver = spark.table(SILVER_TABLE)
else:
    df_silver = spark.read.format("delta").load(SILVER_PATH)

# -----------------------------
# Business Aggregations
# -----------------------------
df_gold = (
    df_silver
    .groupBy("customer_id")
    .agg(
        F.count("*").alias("total_events"),
        F.sum(
            F.when(F.col("event_type") == "PURCHASE", F.col("amount")).otherwise(0.0)
        ).alias("total_purchase_amount"),
        F.max("event_time").alias("last_event_time")
    )
)

# -----------------------------
# Write Gold
# -----------------------------
(
    df_gold
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(GOLD_PATH)
)

# Optional: register table
if USE_TABLES:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {GOLD_TABLE.split('.')[0]}")
    spark.sql(f"DROP TABLE IF EXISTS {GOLD_TABLE}")
    spark.sql(f"CREATE TABLE {GOLD_TABLE} USING DELTA LOCATION '{GOLD_PATH}'")

print("✅ Gold aggregation complete")
print(f"Gold path: {GOLD_PATH}")
if USE_TABLES:
    print(f"Gold table: {GOLD_TABLE}")
