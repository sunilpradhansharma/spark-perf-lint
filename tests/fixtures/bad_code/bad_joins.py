"""Order-fulfilment analytics pipeline — bad join patterns.

Realistic ETL that builds a per-customer order summary.  Several join
anti-patterns have been introduced that will cause performance problems
on datasets larger than a few thousand rows.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder.appName("order_fulfilment_analytics")
    # SPL-D03-003: broadcast auto-threshold disabled — forces sort-merge joins
    # even for tiny lookup tables.                                    [CRITICAL]
    .config("spark.sql.autoBroadcastJoinThreshold", "-1").getOrCreate()
)

# ---------------------------------------------------------------------------
# Load raw tables
# ---------------------------------------------------------------------------

customers = spark.read.parquet("/data/customers")
orders = spark.read.parquet("/data/orders")
products = spark.read.parquet("/data/products")
regions = spark.read.parquet("/data/regions")

# ---------------------------------------------------------------------------
# Build a region-product matrix
# ---------------------------------------------------------------------------

# SPL-D03-001: cartesian cross join — multiplies all regions × products.
# On 200 regions and 50 000 products this produces 10 000 000 rows.  [CRITICAL]
region_product_matrix = regions.crossJoin(products)

# ---------------------------------------------------------------------------
# Enrich orders — joining without pre-filtering either side
# ---------------------------------------------------------------------------

# SPL-D03-004: both sides are unfiltered full table scans.  Applying
# `.filter(F.col("year") == 2024)` on `orders` before this join would
# reduce shuffle volume by an order of magnitude.                    [WARNING]
enriched = orders.join(customers, on="customer_id", how="left")

# A second join without CBO hints — Catalyst cannot choose the optimal
# join order because statistics have not been collected and CBO is off.
# SPL-D03-010: file contains 2+ joins but spark.sql.cbo.enabled is not set.
#                                                                     [WARNING]
enriched = enriched.join(products, on="product_id", how="left")

# ---------------------------------------------------------------------------
# Per-region aggregation driven by a loop — join inside loop
# ---------------------------------------------------------------------------

region_ids = ["APAC", "EMEA", "AMER", "LATAM"]
regional_summaries = []

for region in region_ids:
    region_df = regions.filter(F.col("region_code") == region)

    # SPL-D03-008: join() called inside a for-loop.  Each iteration
    # triggers a full shuffle of `enriched`.  Use a single grouped
    # aggregation or broadcast the regions table instead.            [CRITICAL]
    summary = enriched.join(region_df, on="region_id", how="inner")

    summary = summary.groupBy("region_code", "product_category").agg(
        F.sum("revenue").alias("total_revenue"),
        F.countDistinct("customer_id").alias("unique_customers"),
    )
    regional_summaries.append(summary)

# Reduce the per-region DataFrames into a single result
from functools import reduce  # noqa: E402

final = reduce(lambda a, b: a.union(b), regional_summaries)

final.write.mode("overwrite").parquet("/output/regional_order_summary")
