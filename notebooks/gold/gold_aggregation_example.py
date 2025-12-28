"""
Gold Aggregation Example (Silver -> Gold)
-----------------------------------------
Purpose:
- Produce analytics-ready datasets (Gold)
- Apply business aggregations and metrics
- Serve BI tools and downstream analytics workloads

Notes:
- This is a reference implementation
- Replace paths/catalog/schema with your enterprise conventions
"""

from pyspark.sql import functions as F

# -----------------------------
# 1) Parameters
# -----------------------------
SILVER_PATH = "/mnt/silver/customer_events"
GOLD_PATH = "/mnt/gold/customer_event_metrics"
GOLD_TABLE_NAME = "gold.customer_event_metrics"  # optional UC table name

# -----------------------------
# 2) Read Silver Delta
# -----------------------------
df_silver = spark.read.format("delta").load(SILVER_PATH)

# -----------------------------
# 3) Example Business Aggregations
# -----------------------------
# Daily metrics per customer
df_gold = (
    df_silver
    .withColumn("event_date", F.to_date(F.col("event_time")))
    .groupBy("customer_id", "event_date")
    .agg(
        F.count("*").alias("event_count"),
        F.countDistinct("event_type").alias("distinct_event_types"),
        F.sum(F.coalesce(F.col("amount"), F.lit(0.0))).alias("total_amount"),
        F.max("event_time").alias("last_event_time")
    )
    .withColumn("as_of_time", F.current_timestamp())
)

# -----------------------------
# 4) Write Gold Delta (Overwrite)
# -----------------------------
# Gold tables are often rebuilt on schedule (e.g., daily)
(
    df_gold.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("event_date")
    .save(GOLD_PATH)
)

# Optional: Register table in catalog
# spark.sql(f"CREATE TABLE IF NOT EXISTS {GOLD_TABLE_NAME} USING DELTA LOCATION '{GOLD_PATH}'")

print("✅ Gold aggregation complete.")
print(f"Gold path: {GOLD_PATH}")
