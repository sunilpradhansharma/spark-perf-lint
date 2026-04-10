"""Data lake ingestion job — bad partitioning patterns.

Ingests raw sensor readings from IoT devices and writes them to the
data lake in Parquet format.  Several partitioning mistakes will cause
small-file problems, single-partition bottlenecks, or skipped partition
pruning at read time.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

spark = (
    SparkSession.builder.appName("iot_sensor_ingestion")
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate()
)

schema = StructType(
    [
        StructField("device_id", StringType(), False),
        StructField("sensor_type", StringType(), False),
        StructField("value", DoubleType(), True),
        StructField("event_time", TimestampType(), False),
        StructField("region", StringType(), False),
    ]
)

# ---------------------------------------------------------------------------
# Ingest raw data
# ---------------------------------------------------------------------------

raw = spark.read.schema(schema).json("/landing/iot_events/")

cleaned = (
    raw.filter(F.col("value").isNotNull())
    .withColumn("event_date", F.to_date("event_time"))
    .withColumn("event_hour", F.hour("event_time"))
)

# ---------------------------------------------------------------------------
# Aggregate to hourly rollups per device
# ---------------------------------------------------------------------------

hourly = cleaned.groupBy("device_id", "sensor_type", "event_date", "event_hour", "region").agg(
    F.avg("value").alias("avg_value"),
    F.min("value").alias("min_value"),
    F.max("value").alias("max_value"),
    F.count("*").alias("reading_count"),
)

# ---------------------------------------------------------------------------
# Write path — several anti-patterns
# ---------------------------------------------------------------------------

# SPL-D04-001: repartition(1) collapses all output into a single file.
# One task writes the entire dataset sequentially; no parallelism in the
# write stage, and downstream readers get no partition pruning.       [CRITICAL]
hourly.repartition(1).write.mode("overwrite").parquet("/output/iot_hourly")

# ---------------------------------------------------------------------------
# Daily summary — coalesce bottleneck
# ---------------------------------------------------------------------------

daily = hourly.groupBy("device_id", "sensor_type", "event_date", "region").agg(
    F.avg("avg_value").alias("daily_avg"),
    F.sum("reading_count").alias("total_readings"),
)

# SPL-D04-002: coalesce(1) has the same single-task write bottleneck as
# repartition(1), plus it can skew upstream stages because coalesce does
# not reshuffle — it just merges partitions on the same executor.    [WARNING]
daily.coalesce(1).write.mode("overwrite").parquet("/output/iot_daily")

# ---------------------------------------------------------------------------
# High-cardinality repartition for a small staging table
# ---------------------------------------------------------------------------

device_registry = spark.read.parquet("/data/device_registry")

# SPL-D04-003: repartition(20000) creates far more partitions than the
# dataset has rows, resulting in tens of thousands of tiny (or empty)
# output files.  A good rule of thumb: target 128–256 MB per partition.
#                                                                     [WARNING]
device_registry.repartition(20000).write.mode("overwrite").parquet("/staging/devices")

# ---------------------------------------------------------------------------
# Write without partitionBy — full table scan required for every query
# ---------------------------------------------------------------------------

# SPL-D04-006: writing sensor data without .partitionBy("event_date",
# "region") forces every downstream query to scan the entire dataset.
# Adding a partition column allows Spark (and engines like Athena or
# Presto) to prune irrelevant files at query time.                     [INFO]
cleaned.write.mode("overwrite").parquet("/output/iot_cleaned")
