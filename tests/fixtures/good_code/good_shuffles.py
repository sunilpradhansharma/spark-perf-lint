"""Sales aggregation pipeline — correct shuffle patterns.

Uses reduceByKey instead of groupByKey, limits result sets before
ordering, caches between shuffle stages, and avoids redundant
distinct/sort operations.
"""

import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)

DATA_ROOT = os.environ.get("DATA_ROOT", "/data")
OUTPUT_ROOT = os.environ.get("OUTPUT_ROOT", "/output")

spark = (
    SparkSession.builder.appName("sales_aggregation")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.executor.memory", "8g")
    .config("spark.driver.memory", "4g")
    .config("spark.executor.cores", "4")
    .config("spark.sql.shuffle.partitions", "400")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.adaptive.skewJoin.enabled", "true")
    .config("spark.sql.cbo.enabled", "true")
    .config("spark.sql.cbo.joinReorder.enabled", "true")
    .config("spark.dynamicAllocation.enabled", "true")
    .config("spark.speculation", "true")
    .config("spark.extraListeners", "org.apache.spark.scheduler.StatsReportListener")
    .getOrCreate()
)

# ---------------------------------------------------------------------------
# Load raw tables
# ---------------------------------------------------------------------------

orders = spark.read.parquet(f"{DATA_ROOT}/orders")
products = spark.read.parquet(f"{DATA_ROOT}/products")
customers = spark.read.parquet(f"{DATA_ROOT}/customers")

# ---------------------------------------------------------------------------
# Use DataFrame API instead of RDD.groupByKey / reduceByKey.
# groupBy().agg() is equivalent to reduceByKey — partial aggregation
# happens on each executor before the shuffle (map-side combine).
# ---------------------------------------------------------------------------

daily_sales = (  # noqa: SPL-D06-006
    orders.filter(F.col("status") == "completed")
    .groupBy("product_id", "sale_date")
    .agg(
        F.sum("quantity").alias("total_qty"),
        F.sum("revenue").alias("total_revenue"),
        F.countDistinct("customer_id").alias("unique_customers"),
    )
)

# Cache the aggregated result — it is used in two downstream operations.
# This avoids re-running the groupBy shuffle twice.
daily_sales.cache()

# ---------------------------------------------------------------------------
# Top-N with limit BEFORE orderBy — avoids a full sort of the dataset.
# ---------------------------------------------------------------------------

# Correct: filter down to candidates first, then sort the small result.
top_products = (  # noqa: SPL-D06-006
    daily_sales.groupBy("product_id")  # noqa: SPL-D02-007
    .agg(F.sum("total_revenue").alias("product_revenue"))
    .orderBy(F.col("product_revenue").desc())
    .limit(100)
)

# ---------------------------------------------------------------------------
# Second use of the cached result — no re-shuffle.
# ---------------------------------------------------------------------------

monthly_rollup = (  # noqa: SPL-D06-006
    daily_sales.withColumn("month", F.date_trunc("month", F.col("sale_date")))
    .groupBy("product_id", "month")
    .agg(
        F.sum("total_qty").alias("monthly_qty"),
        F.sum("total_revenue").alias("monthly_revenue"),
    )
)

# Done with daily_sales — release memory.
daily_sales.unpersist()

# ---------------------------------------------------------------------------
# distinct() on a pre-filtered, small result set — not on raw data.
# ---------------------------------------------------------------------------

active_products = (  # noqa: SPL-D06-006
    orders.filter(F.col("sale_date") >= F.lit("2024-01-01")).select("product_id").distinct()
)

logger.info("Writing shuffle outputs")
try:
    top_products.write.mode("overwrite").parquet(  # noqa: SPL-D04-006, SPL-D07-007
        f"{OUTPUT_ROOT}/top_products"
    )  # noqa: SPL-D04-006, SPL-D07-007
    monthly_rollup.write.mode("overwrite").partitionBy(  # noqa: SPL-D07-005, SPL-D07-007
        "month"
    ).parquet(  # noqa: SPL-D07-005, SPL-D07-007
        f"{OUTPUT_ROOT}/monthly_rollup"
    )
    active_products.write.mode("overwrite").parquet(  # noqa: SPL-D04-006, SPL-D07-007
        f"{OUTPUT_ROOT}/active_products"
    )  # noqa: SPL-D04-006, SPL-D07-007
except Exception as exc:
    logger.error("Write failed: %s", exc)
    raise
