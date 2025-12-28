"""
gold_aggregation_example.py
Illustrative example: Silver -> Gold aggregation

Purpose:
- Produce analytics-ready datasets (Gold)
- Apply business aggregations/metrics
- Serve BI tools and downstream analytics workloads

Notes:
- Intentionally minimal (reference design).
- Replace paths/catalog/schema to match your environment.
"""

from pyspark.sql import functions as F

# -----------------------------
# 1) Parameters
# -----------------------------
SILVER_PATH = "/mnt/silver/customer_events"
GOLD_PATH = "/mnt/gold/customer_event_metrics"
GOLD_TABLE = "gold.customer_event_metrics"  # optional

# -----------------------------
# 2) Read Silver Delta
# -----------------------------
df_silver = spark.read.format("delta").load(SILVER_PATH)

# -----------------------------
# 3) Example business aggregations
# Daily metrics per customer
# -----------------------------
df_gold = (
    df_silver
    .withColumn("event_date", F.to_date(F.col("event_time")))
    .groupBy("customer_id", "event_date")
    .agg(
        F.count("*").alias("event_count"),
        F.countDistinct("event_type").alias("distinct_event_types"),
        F.sum(F.col("amount")).alias("total_amount"),
        F.max("event_time").alias("last_event_time"),
    )
    .withColumn("as_of_time", F.current_timestamp())
)

# -----------------------------
# 4) Write Gold Delta
# -----------------------------
(
    df_gold.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("event_date")
    .save(GOLD_PATH)
)

# Optional: register table
# spark.sql(f"CREATE TABLE IF NOT EXISTS {GOLD_TABLE} USING DELTA LOCATION '{GOLD_PATH}'")

print("✅ Gold aggregation complete.")
print(f"Gold path: {GOLD_PATH}")
