"""Session analytics pipeline — correct caching patterns.

Demonstrates: filter BEFORE cache, cache only DataFrames used multiple
times, always unpersist when done, never cache inside a loop.
"""

import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)

DATA_ROOT   = os.environ.get("DATA_ROOT",   "/data")
OUTPUT_ROOT = os.environ.get("OUTPUT_ROOT", "/output")

spark = (
    SparkSession.builder
    .appName("session_analytics")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.executor.memory", "8g")
    .config("spark.driver.memory",   "4g")
    .config("spark.executor.cores",  "4")
    .config("spark.sql.shuffle.partitions", "400")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.adaptive.skewJoin.enabled", "true")
    .config("spark.sql.cbo.enabled", "true")
    .config("spark.sql.cbo.joinReorder.enabled", "true")
    .config("spark.dynamicAllocation.enabled", "true")
    .config("spark.speculation", "true")
    .config("spark.extraListeners",
            "org.apache.spark.scheduler.StatsReportListener")
    .getOrCreate()
)

# ---------------------------------------------------------------------------
# Load raw events
# ---------------------------------------------------------------------------

raw_events = spark.read.parquet(f"{DATA_ROOT}/session_events")

# ---------------------------------------------------------------------------
# Filter BEFORE cache — only keep active-session rows in memory.
# Caching the full raw table would waste memory on rows we don't need.
# ---------------------------------------------------------------------------

active_events = (
    raw_events
    .filter(F.col("session_status") == "active")
    .filter(F.col("event_date") >= F.lit("2024-01-01"))
)

# Cache after filtering — the downstream pipeline uses active_events
# in three separate aggregations.
active_events.cache()

# ---------------------------------------------------------------------------
# Three aggregations over the same filtered DataFrame — cache pays off.
# ---------------------------------------------------------------------------

hourly_counts = (
    active_events
    .groupBy("user_id", "hour_bucket")
    .agg(F.count("*").alias("events_per_hour"))
)

feature_usage = (
    active_events
    .groupBy("user_id", "feature_name")
    .agg(
        F.count("*").alias("usage_count"),
        F.sum("duration_seconds").alias("total_duration"),
    )
)

session_summary = (
    active_events
    .groupBy("user_id")
    .agg(
        F.countDistinct("session_id").alias("session_count"),
        F.sum("revenue").alias("total_revenue"),
        F.avg("duration_seconds").alias("avg_duration"),
    )
)

# Done with active_events — release the cached data.
active_events.unpersist()

# ---------------------------------------------------------------------------
# A second pass: cache a joined result used in two writes.
# ---------------------------------------------------------------------------

users = spark.read.parquet(f"{DATA_ROOT}/users")

enriched_summary = session_summary.join(
    F.broadcast(users), on="user_id", how="left"
)
enriched_summary.cache()

high_value = enriched_summary.filter(F.col("total_revenue") > 1000)
low_value  = enriched_summary.filter(F.col("total_revenue") <= 1000)

enriched_summary.unpersist()

# ---------------------------------------------------------------------------
# No cache() inside a loop — each segment is written in one pass.
# ---------------------------------------------------------------------------

segments = ["enterprise", "smb", "consumer"]

for seg in segments:
    seg_data = (
        hourly_counts
        .join(F.broadcast(users.filter(F.col("segment") == seg)), on="user_id")
    )
    seg_data.write.mode("overwrite").parquet(f"{OUTPUT_ROOT}/hourly/{seg}")

logger.info("Writing caching outputs")
try:
    feature_usage.write.mode("overwrite").parquet(f"{OUTPUT_ROOT}/feature_usage")
    high_value.write.mode("overwrite").parquet(f"{OUTPUT_ROOT}/high_value_users")
    low_value.write.mode("overwrite").parquet(f"{OUTPUT_ROOT}/low_value_users")
except Exception as exc:
    logger.error("Write failed: %s", exc)
    raise
