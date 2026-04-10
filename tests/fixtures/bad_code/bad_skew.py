"""User activity aggregation pipeline — bad data-skew patterns.

Computes per-user engagement metrics from a clickstream table.  The
table has severe data skew: ~70 % of events have status = "active"
and ~60 % of transactions belong to a handful of top sellers.
Anti-patterns here amplify that skew instead of handling it.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = (
    SparkSession.builder
    .appName("user_engagement_metrics")
    # SPL-D05-003: explicitly disabling AQE skew-join handling forces
    # Spark to use the standard sort-merge join even for partitions
    # that are 100× larger than average.                              [WARNING]
    .config("spark.sql.adaptive.skewJoin.enabled", "false")
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate()
)

# ---------------------------------------------------------------------------
# Load raw tables
# ---------------------------------------------------------------------------

events    = spark.read.parquet("/data/clickstream_events")
users     = spark.read.parquet("/data/users")
sellers   = spark.read.parquet("/data/sellers")
orders    = spark.read.parquet("/data/orders")

# ---------------------------------------------------------------------------
# Join on a low-cardinality column — guaranteed skew
# ---------------------------------------------------------------------------

# SPL-D05-001: joining on "account_status" which has only ~5 distinct
# values ("active", "suspended", "trial", "churned", "pending").
# The sort-merge join will produce massive hot partitions for "active".
# Salting or a broadcast join on a filtered lookup would avoid this.  [WARNING]
status_lookup = spark.createDataFrame(
    [("active", 1.0), ("trial", 0.5), ("churned", 0.1)],
    ["account_status", "status_weight"],
)
weighted_events = events.join(status_lookup, on="account_status", how="left")

# ---------------------------------------------------------------------------
# GroupBy on a low-cardinality column
# ---------------------------------------------------------------------------

# SPL-D05-002: groupBy("account_status") with only one low-cardinality
# key.  All rows with status="active" land on the same reducer, creating
# a partition that may be 1000× larger than the others.              [WARNING]
status_counts = (
    events
    .groupBy("account_status")
    .agg(
        F.count("*").alias("event_count"),
        F.countDistinct("user_id").alias("unique_users"),
        F.sum("revenue").alias("total_revenue"),
    )
)

# ---------------------------------------------------------------------------
# Window function with skew-prone partition key
# ---------------------------------------------------------------------------

# SPL-D05-006: Window.partitionBy("country") on a dataset where "US"
# alone accounts for 80 % of rows.  The executor handling the US
# partition may OOM while others sit idle.                            [WARNING]
country_window = Window.partitionBy("country").orderBy(F.col("event_time").desc())

ranked_events = events.withColumn("country_rank", F.rank().over(country_window))

# ---------------------------------------------------------------------------
# Join on user_id without salting — skewed seller traffic
# ---------------------------------------------------------------------------

# SPL-D05-005: joining on "seller_id" where a few top sellers have
# millions of rows.  No rand()/explode() salting pattern in this file.
# Adding a salt column (e.g. seller_id + "_" + (rand*10).cast(int))
# distributes hot partitions across multiple reducers.                [INFO]
seller_orders = orders.join(sellers, on="seller_id", how="inner")

seller_orders.write.mode("overwrite").parquet("/output/seller_order_metrics")
status_counts.write.mode("overwrite").parquet("/output/status_event_counts")
ranked_events.write.mode("overwrite").parquet("/output/ranked_events")
