"""Ad-hoc analytics job — bad AQE configuration.

Performs multi-dimensional aggregations across ad impression and
conversion data.  AQE is explicitly disabled or misconfigured,
preventing Spark 3.x from optimising shuffle partitions at runtime.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("ad_performance_analytics")
    # SPL-D08-001: AQE disabled entirely.  Without AQE, Spark cannot
    # coalesce small shuffle partitions, handle join skew, or switch
    # broadcast/sort-merge strategy at runtime.                       [CRITICAL]
    .config("spark.sql.adaptive.enabled", "false")
    # SPL-D08-002: partition coalescing also disabled.  Post-shuffle
    # stages will produce as many output partitions as pre-set, even
    # if most of them are empty or contain only a few bytes.          [WARNING]
    .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
    # SPL-D08-007: manual shuffle.partitions = 2000 with AQE disabled
    # means every shuffle in this job produces exactly 2000 output
    # files regardless of actual data volume.                         [INFO]
    .config("spark.sql.shuffle.partitions", "2000")
    .getOrCreate()
)

# ---------------------------------------------------------------------------
# Load impression and conversion data
# ---------------------------------------------------------------------------

impressions = spark.read.parquet("/data/ad_impressions")
conversions = spark.read.parquet("/data/ad_conversions")
campaigns   = spark.read.parquet("/data/campaigns")
creatives   = spark.read.parquet("/data/creatives")

# ---------------------------------------------------------------------------
# Multi-stage aggregation pipeline
# ---------------------------------------------------------------------------

# With AQE=false, each of these aggregations materialises exactly 2000
# shuffle output partitions.  For a small date slice (e.g. one hour of
# impressions) this means 2000 tasks each reading ~10 KB — massive
# scheduling overhead and thousands of tiny output files.

hourly_impressions = (
    impressions
    .filter(F.col("ad_type").isin("banner", "video", "native"))
    .groupBy("campaign_id", "creative_id", "placement", "hour_bucket")
    .agg(
        F.count("*").alias("impressions"),
        F.sum("viewable_seconds").alias("total_viewable_seconds"),
    )
)

hourly_conversions = (
    conversions
    .groupBy("campaign_id", "creative_id", "hour_bucket")
    .agg(
        F.count("*").alias("conversions"),
        F.sum("revenue").alias("total_revenue"),
    )
)

# ---------------------------------------------------------------------------
# Join — skew-join handling also disabled
# ---------------------------------------------------------------------------

# SPL-D08-004: spark.sql.adaptive.skewJoin.enabled is implicitly false
# because the whole AQE framework is off.  The join below on
# "campaign_id" is likely skewed (a few campaigns drive most traffic).
# With AQE and skew-join enabled, Spark would automatically split hot
# partitions; without it, one executor processes the entire "top campaign"
# partition alone.                                                    [WARNING]
performance = (
    hourly_impressions
    .join(hourly_conversions, on=["campaign_id", "creative_id", "hour_bucket"], how="left")
    .join(campaigns,  on="campaign_id", how="left")
    .join(creatives,  on="creative_id", how="left")
)

ctr_metrics = performance.withColumn(
    "ctr",
    F.when(F.col("impressions") > 0,
           F.col("conversions").cast("double") / F.col("impressions"))
     .otherwise(0.0)
)

ctr_metrics.write.mode("overwrite").parquet("/output/ad_performance_metrics")
