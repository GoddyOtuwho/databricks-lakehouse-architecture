"""
bronze_ingestion_example.py
Illustrative example: JSON ingestion -> Bronze Delta (raw, immutable-style)

Purpose:
- Read raw events (JSON/NDJSON)
- Basic schema + ingestion metadata
- Write to Bronze Delta path (and optionally register a table)

Notes:
- This is intentionally minimal (reference design).
- Replace paths/catalog/schema to match your environment (Unity Catalog, storage accounts, etc.).
"""

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)

# -----------------------------
# 1) Parameters (edit these)
# -----------------------------
# In Databricks, you will typically copy the repo file into DBFS/Volume first.
# Example DBFS path: /dbfs/FileStore/... or /Volumes/<catalog>/<schema>/...
RAW_INPUT_PATH = "/dbfs/FileStore/lakehouse_demo/customer_events.ndjson"

BRONZE_PATH = "/mnt/bronze/customer_events"
BRONZE_TABLE = "bronze.customer_events"  # optional UC/Hive table name

# -----------------------------
# 2) Schema (keep stable in Bronze)
# -----------------------------
schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_time", StringType(), True),  # parse later
    StructField("amount", DoubleType(), True),
    StructField("source_system", StringType(), True),
])

# -----------------------------
# 3) Read raw (NDJSON recommended)
# -----------------------------
df_raw = (
    spark.read
    .schema(schema)
    .json(RAW_INPUT_PATH)   # NDJSON: one JSON object per line
)

df_bronze = (
    df_raw
    .withColumn("event_time", F.to_timestamp("event_time"))
    .withColumn("ingest_time", F.current_timestamp())
    .withColumn("ingest_date", F.to_date("ingest_time"))
)

# -----------------------------
# 4) Write Bronze Delta (append)
# -----------------------------
(
    df_bronze.write
    .format("delta")
    .mode("append")
    .partitionBy("ingest_date")
    .save(BRONZE_PATH)
)

# Optional: register table (Unity Catalog or Hive Metastore)
# spark.sql(f"CREATE TABLE IF NOT EXISTS {BRONZE_TABLE} USING DELTA LOCATION '{BRONZE_PATH}'")

print("✅ Bronze ingestion complete.")
print(f"Bronze path: {BRONZE_PATH}")
