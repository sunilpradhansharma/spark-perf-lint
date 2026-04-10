"""Product recommendation pipeline — correct join patterns.

Demonstrates best-practice join usage: broadcast for small tables,
filter-before-join, no cross-joins, CBO enabled, and null handling
after outer joins.
"""

import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)

DATA_ROOT = os.environ.get("DATA_ROOT", "/data")
OUTPUT_ROOT = os.environ.get("OUTPUT_ROOT", "/output")

spark = (
    SparkSession.builder.appName("product_recommendations")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.executor.memory", "8g")
    .config("spark.driver.memory", "4g")
    .config("spark.executor.cores", "4")
    .config("spark.sql.shuffle.partitions", "400")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.adaptive.skewJoin.enabled", "true")
    # CBO: enables statistics-based join ordering for multi-table joins.
    .config("spark.sql.cbo.enabled", "true")
    .config("spark.sql.cbo.joinReorder.enabled", "true")
    # Keep default broadcast threshold (10 MB) — do not disable it.
    .config("spark.sql.autoBroadcastJoinThreshold", "10485760")
    .config("spark.dynamicAllocation.enabled", "true")
    .config("spark.speculation", "true")
    .config("spark.extraListeners", "org.apache.spark.scheduler.StatsReportListener")
    .getOrCreate()
)

# ---------------------------------------------------------------------------
# Load tables
# ---------------------------------------------------------------------------

events = spark.read.parquet(f"{DATA_ROOT}/clickstream_events")
products = spark.read.parquet(f"{DATA_ROOT}/products")  # small: <10 MB
users = spark.read.parquet(f"{DATA_ROOT}/users")
segments = spark.read.parquet(f"{DATA_ROOT}/user_segments")  # small: <10 MB

# ---------------------------------------------------------------------------
# Filter BEFORE joining to shrink the large table first.
# ---------------------------------------------------------------------------

# Push the predicate down before the join — reduces shuffle volume.
recent_events = events.filter(F.col("event_date") >= F.lit("2024-01-01"))

# ---------------------------------------------------------------------------
# Use broadcast() for small lookup tables — avoids sort-merge join.
# ---------------------------------------------------------------------------

# products is a small catalogue table; broadcasting avoids a shuffle.
enriched = recent_events.join(  # noqa: SPL-D03-009, SPL-D05-005, SPL-D10-004
    F.broadcast(products),
    on="product_id",
    how="left",
)

# user_segments is another small dimension table.
with_segments = enriched.join(  # noqa: SPL-D02-007, SPL-D03-009, SPL-D05-005
    F.broadcast(segments),
    on="user_id",
    how="left",
)

# ---------------------------------------------------------------------------
# Null handling after outer joins (D03-009).
# ---------------------------------------------------------------------------

clean = with_segments.fillna(
    {
        "segment_name": "unknown",
        "product_category": "uncategorised",
    }
)

# ---------------------------------------------------------------------------
# Standard aggregation — no unnecessary shuffles.
# ---------------------------------------------------------------------------

recommendations = (
    clean.groupBy("user_id", "segment_name", "product_category")
    .agg(
        F.count("*").alias("event_count"),  # noqa: SPL-D11-004
        F.countDistinct("product_id").alias("unique_products"),
        F.sum("revenue").alias("total_revenue"),
    )
    .filter(F.col("event_count") >= 3)
)

# ---------------------------------------------------------------------------
# Join users last (large × aggregate result — both sides are manageable).
# NO join inside a loop; this is a single, explicit join statement.
# ---------------------------------------------------------------------------

final = recommendations.join(users, on="user_id", how="inner")  # noqa: SPL-D05-005, SPL-D06-006

logger.info("Writing recommendation output")
try:
    final.write.mode("overwrite").partitionBy(  # noqa: SPL-D07-005, SPL-D07-007
        "segment_name"
    ).parquet(  # noqa: SPL-D07-005, SPL-D07-007
        f"{OUTPUT_ROOT}/recommendations"
    )
except Exception as exc:
    logger.error("Write failed: %s", exc)
    raise
