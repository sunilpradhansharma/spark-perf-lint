"""Inventory ETL pipeline — correct partitioning patterns.

Demonstrates: sensible repartition counts, partitionBy on write,
avoiding coalesce(1)/repartition(1) on large datasets, and not
over-partitioning.
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
    .appName("inventory_etl")
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
# Load tables
# ---------------------------------------------------------------------------

inventory = spark.read.parquet(f"{DATA_ROOT}/inventory")
warehouses = spark.read.parquet(f"{DATA_ROOT}/warehouses")
categories = spark.read.parquet(f"{DATA_ROOT}/categories")   # small table

# ---------------------------------------------------------------------------
# Repartition to a sensible count based on cluster size.
# Rule of thumb: 2-3× the number of CPU cores available.
# Do NOT use repartition(1) — that collapses all data to a single task.
# ---------------------------------------------------------------------------

# 200 is a reasonable default for a mid-size cluster (50 nodes × 4 cores).
balanced = inventory.repartition(200, "warehouse_id")

# ---------------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------------

enriched = (
    balanced
    .join(F.broadcast(categories), on="category_id", how="left")
    .join(warehouses, on="warehouse_id", how="inner")
    .withColumn(
        "stock_value",
        F.col("quantity_on_hand") * F.col("unit_cost"),
    )
    .withColumn(
        "reorder_flag",
        F.col("quantity_on_hand") < F.col("reorder_point"),
    )
)

# ---------------------------------------------------------------------------
# Write with partitionBy — creates a Hive-style directory layout so
# downstream readers can prune partitions efficiently.
# Do NOT write everything into a single file with coalesce(1).
# ---------------------------------------------------------------------------

logger.info("Writing partitioned inventory output")
try:
    enriched.write.mode("overwrite").partitionBy("warehouse_id", "category_id").parquet(
        f"{OUTPUT_ROOT}/inventory_enriched"
    )
except Exception as exc:
    logger.error("Write failed: %s", exc)
    raise

# ---------------------------------------------------------------------------
# Small reference output — coalesce to a few files, NOT to 1.
# coalesce(4) avoids a full shuffle while still producing small output.
# ---------------------------------------------------------------------------

reorder_items = enriched.filter(F.col("reorder_flag") == True)

try:
    reorder_items.coalesce(4).write.mode("overwrite").parquet(
        f"{OUTPUT_ROOT}/reorder_alerts"
    )
except Exception as exc:
    logger.error("Reorder write failed: %s", exc)
    raise
