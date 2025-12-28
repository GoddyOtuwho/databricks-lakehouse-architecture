"""
gold_aggregation_example.py
Illustrative example: Silver -> Gold aggregation in a Databricks Lakehouse.

Purpose:
- Produce curated, analytics-ready datasets (Gold layer)
- Model business-facing aggregates for BI and reporting use cases

Notes:
- Reference implementation (intentionally minimal)
- Replace paths/catalog/schema with your environment conventions
"""

from pyspark.sql import functions as F

# -----------------------------
# Inputs (Silver)
# -----------------------------
silver_path = "/mnt/silver/customer_events"  # example path
df_silver = spark.read.format("delta").load(silver_path)

# -----------------------------
# Business Aggregations
# Example: Daily metrics by event type
# -----------------------------
df_gold = (
    df_silver
    .withColumn("event_date", F.to_date("event_time"))
    .groupBy("event_date", "event_type")
    .agg(
        F.count("*").alias("event_count"),
        F.countDistinct("customer_id").alias("unique_customers")
    )
    .orderBy(F.col("event_date").desc())
)

# -----------------------------
# Output (Gold)
# -----------------------------
gold_path = "/mnt/gold/daily_event_metrics"  # example path
(
    df_gold
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(gold_path)
)

# Optional: Register table (Unity Catalog style)
# spark.sql("CREATE SCHEMA IF NOT EXISTS main.gold")
# spark.sql(
#     f"CREATE TABLE IF NOT EXISTS main.gold.daily_event_metrics "
#     f"USING DELTA LOCATION '{gold_path}'"
# )
