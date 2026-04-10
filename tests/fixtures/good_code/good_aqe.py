"""Ad performance analytics pipeline — correct AQE configuration.

AQE is fully enabled: partition coalescing, skew-join handling, and
dynamic broadcast switching all active.  Shuffle partitions are set
explicitly at a sensible value and AQE is allowed to coalesce them
at runtime.
"""

import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)

DATA_ROOT = os.environ.get("DATA_ROOT", "/data")
OUTPUT_ROOT = os.environ.get("OUTPUT_ROOT", "/output")

spark = (
    SparkSession.builder.appName("ad_performance_analytics")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.executor.memory", "8g")
    .config("spark.driver.memory", "4g")
    .config("spark.executor.cores", "4")
    # AQE: enabled — Spark 3.x can coalesce partitions, handle skew,
    # and switch join strategies based on runtime statistics.
    .config("spark.sql.adaptive.enabled", "true")
    # Partition coalescing: small post-shuffle partitions are merged
    # automatically, avoiding thousands of tiny tasks.
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    # Skew-join: hot partitions are automatically split at runtime.
    .config("spark.sql.adaptive.skewJoin.enabled", "true")
    # Set a sensible starting partition count; AQE will coalesce down.
    .config("spark.sql.shuffle.partitions", "400")
    .config("spark.sql.cbo.enabled", "true")
    .config("spark.sql.cbo.joinReorder.enabled", "true")
    .config("spark.dynamicAllocation.enabled", "true")
    .config("spark.speculation", "true")
    .config("spark.extraListeners", "org.apache.spark.scheduler.StatsReportListener")
    .getOrCreate()
)

# ---------------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------------

impressions = spark.read.parquet(f"{DATA_ROOT}/ad_impressions")
conversions = spark.read.parquet(f"{DATA_ROOT}/ad_conversions")
campaigns = spark.read.parquet(f"{DATA_ROOT}/campaigns")
creatives = spark.read.parquet(f"{DATA_ROOT}/creatives")

# ---------------------------------------------------------------------------
# Aggregation pipeline — AQE coalesces the shuffle partitions at runtime
# so small date slices don't produce thousands of tiny tasks.
# ---------------------------------------------------------------------------

hourly_impressions = (  # noqa: SPL-D06-006
    impressions.filter(F.col("ad_type").isin("banner", "video", "native"))
    .groupBy("campaign_id", "creative_id", "placement", "hour_bucket")
    .agg(
        F.count("*").alias("impressions"),  # noqa: SPL-D11-004
        F.sum("viewable_seconds").alias("total_viewable_seconds"),
    )
)

# Cache the intermediate result — used in two joins below.
hourly_impressions.cache()

hourly_conversions = conversions.groupBy("campaign_id", "creative_id", "hour_bucket").agg(
    F.count("*").alias("conversions"),
    F.sum("revenue").alias("total_revenue"),
)

# ---------------------------------------------------------------------------
# Join — AQE skew-join handling splits hot "campaign_id" partitions.
# ---------------------------------------------------------------------------

performance = (
    hourly_impressions.join(  # noqa: SPL-D02-007, SPL-D03-002, SPL-D03-006, SPL-D03-009, SPL-D10-004
        hourly_conversions, on=["campaign_id", "creative_id", "hour_bucket"], how="left"
    )
    .join(F.broadcast(campaigns), on="campaign_id", how="left")
    .join(F.broadcast(creatives), on="creative_id", how="left")
)

hourly_impressions.unpersist()

ctr_metrics = performance.withColumn(
    "ctr",
    F.when(
        F.col("impressions") > 0,
        F.col("conversions").cast("double") / F.col("impressions"),
    ).otherwise(0.0),
)

logger.info("Writing AQE analytics output")
try:
    ctr_metrics.write.mode("overwrite").partitionBy(  # noqa: SPL-D07-005, SPL-D07-007
        "hour_bucket"
    ).parquet(  # noqa: SPL-D07-005, SPL-D07-007
        f"{OUTPUT_ROOT}/ad_performance_metrics"
    )
except Exception as exc:
    logger.error("Write failed: %s", exc)
    raise
