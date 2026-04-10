"""User activity aggregation pipeline — correct skew-handling patterns.

Demonstrates: AQE skew-join enabled, high-cardinality join keys,
salting for known skewed joins, and window functions on high-cardinality
partition keys.
"""

import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)

DATA_ROOT = os.environ.get("DATA_ROOT", "/data")
OUTPUT_ROOT = os.environ.get("OUTPUT_ROOT", "/output")

spark = (
    SparkSession.builder.appName("user_engagement_metrics")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.executor.memory", "8g")
    .config("spark.driver.memory", "4g")
    .config("spark.executor.cores", "4")
    .config("spark.sql.shuffle.partitions", "400")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    # AQE skew-join: automatically splits hot partitions at runtime.
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

events = spark.read.parquet(f"{DATA_ROOT}/clickstream_events")
users = spark.read.parquet(f"{DATA_ROOT}/users")
sellers = spark.read.parquet(f"{DATA_ROOT}/sellers")
orders = spark.read.parquet(f"{DATA_ROOT}/orders")

# ---------------------------------------------------------------------------
# Join on a HIGH-CARDINALITY column to avoid skew.
# Instead of joining on "account_status" (5 values), join on "user_id"
# (millions of values) to distribute data evenly across partitions.
# ---------------------------------------------------------------------------

status_lookup = spark.createDataFrame(
    [("active", 1.0), ("trial", 0.5), ("churned", 0.1)],
    ["account_status", "status_weight"],
)

# Enrich users with status weights first (small broadcast join).
users_weighted = users.join(  # noqa: SPL-D03-002, SPL-D03-009, SPL-D10-004
    F.broadcast(status_lookup), on="account_status", how="left"
)  # noqa: SPL-D03-002, SPL-D03-009, SPL-D10-004

# Now join events on the high-cardinality user_id key.
weighted_events = events.join(  # noqa: SPL-D02-007, SPL-D03-002, SPL-D03-009
    users_weighted, on="user_id", how="left"
)  # noqa: SPL-D02-007, SPL-D03-002, SPL-D03-009

# ---------------------------------------------------------------------------
# GroupBy on a HIGH-CARDINALITY column to avoid reducer hotspots.
# Instead of groupBy("account_status"), group by user_id + status.
# ---------------------------------------------------------------------------

user_counts = events.groupBy("user_id", "account_status").agg(  # noqa: SPL-D06-006
    F.count("*").alias("event_count"),  # noqa: SPL-D11-004
    F.sum("revenue").alias("total_revenue"),
)

# If you need status-level rollup, do it as a second aggregation
# on the already-reduced user_counts — much smaller shuffle.
status_counts = user_counts.groupBy("account_status").agg(  # noqa: SPL-D06-006
    F.sum("event_count").alias("event_count"),
    F.countDistinct("user_id").alias("unique_users"),
    F.sum("total_revenue").alias("total_revenue"),
)

# ---------------------------------------------------------------------------
# Window function on a HIGH-CARDINALITY partition key (user_id, not country).
# country would put ~80% of rows on a single executor for "US".
# user_id distributes rows evenly — each user's events on one executor.
# ---------------------------------------------------------------------------

user_window = Window.partitionBy("user_id").orderBy(F.col("event_time").desc())  # noqa: SPL-D02-004
ranked_events = events.withColumn("user_rank", F.rank().over(user_window))

# ---------------------------------------------------------------------------
# Salting pattern for a known skewed join (top sellers have millions of orders).
# Salt distributes one hot partition into N buckets across N reducers.
# ---------------------------------------------------------------------------

N_SALT = 10

salted_orders = orders.withColumn(
    "salt",
    (F.rand() * N_SALT).cast("int"),
)

# Explode sellers so each seller gets N copies — one per salt bucket.
salt_values = spark.range(N_SALT).toDF("salt_val")
salted_sellers = (
    sellers.crossJoin(salt_values)  # noqa: SPL-D03-001
    .withColumn("salt", F.col("salt_val"))  # noqa: SPL-D03-001
    .drop("salt_val")
)

# Join on (seller_id, salt) — hot partitions are now spread across 10 tasks.
seller_orders = salted_orders.join(  # noqa: SPL-D03-002, SPL-D06-006
    salted_sellers,
    on=["seller_id", "salt"],
    how="inner",
).drop("salt")

logger.info("Writing skew-handled outputs")
try:
    seller_orders.write.mode("overwrite").parquet(  # noqa: SPL-D04-006, SPL-D07-007
        f"{OUTPUT_ROOT}/seller_order_metrics"
    )  # noqa: SPL-D04-006, SPL-D07-007
    status_counts.write.mode("overwrite").parquet(  # noqa: SPL-D04-006, SPL-D07-007
        f"{OUTPUT_ROOT}/status_event_counts"
    )  # noqa: SPL-D04-006, SPL-D07-007
    ranked_events.write.mode("overwrite").partitionBy(  # noqa: SPL-D07-005, SPL-D07-007
        "account_status"
    ).parquet(  # noqa: SPL-D07-005, SPL-D07-007
        f"{OUTPUT_ROOT}/ranked_events"
    )
except Exception as exc:
    logger.error("Write failed: %s", exc)
    raise
