"""Customer data ingestion pipeline — correct I/O patterns.

Demonstrates: read Parquet instead of CSV, explicit schema (no
inferSchema), JDBC with partition parameters, select specific columns,
and write with compression and partitioning.
"""

import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)

logger = logging.getLogger(__name__)

DATA_ROOT = os.environ.get("DATA_ROOT", "/data")
OUTPUT_ROOT = os.environ.get("OUTPUT_ROOT", "/output")
JDBC_URL = os.environ.get("JDBC_URL", "jdbc:postgresql://localhost/app")
DB_USER = os.environ.get("DB_USER", "spark")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "")

spark = (
    SparkSession.builder.appName("customer_ingestion")
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
    # Parquet compression: snappy is fast and widely compatible.
    .config("spark.sql.parquet.compression.codec", "snappy")
    .getOrCreate()
)

# ---------------------------------------------------------------------------
# Read Parquet — not CSV.  Parquet has embedded schema, column pruning,
# and predicate pushdown.  CSV requires full-file scans.
# ---------------------------------------------------------------------------

transactions = spark.read.parquet(f"{DATA_ROOT}/transactions")
products = spark.read.parquet(f"{DATA_ROOT}/products")

# ---------------------------------------------------------------------------
# Explicit schema for the one CSV source that can't be avoided.
# inferSchema=True would trigger a full two-pass scan to infer types.
# ---------------------------------------------------------------------------

reference_schema = StructType(
    [
        StructField("region_id", StringType(), nullable=False),
        StructField("region_name", StringType(), nullable=True),
        StructField("country", StringType(), nullable=True),
        StructField("timezone", StringType(), nullable=True),
        StructField("currency", StringType(), nullable=True),
    ]
)

regions = (
    spark.read.option("header", "true")  # noqa: SPL-D07-001
    .option("inferSchema", "false")  # explicit schema — no two-pass scan
    .schema(reference_schema)
    .csv(f"{DATA_ROOT}/regions.csv")
)

# ---------------------------------------------------------------------------
# JDBC with partition parameters — reads in parallel instead of a
# single-threaded sequential scan through the driver.
# ---------------------------------------------------------------------------

customers = (
    spark.read.format("jdbc")
    .option("url", JDBC_URL)
    .option("dbtable", "public.customers")
    .option("user", DB_USER)
    .option("password", DB_PASSWORD)
    # Partition the read across 20 parallel tasks on customer_id range.
    .option("partitionColumn", "customer_id")
    .option("lowerBound", "1")
    .option("upperBound", "10000000")
    .option("numPartitions", "20")
    .load()
)

# ---------------------------------------------------------------------------
# Select only required columns — avoids reading unused columns from
# Parquet files (column pruning) and reduces shuffle data volume.
# ---------------------------------------------------------------------------

txn_cols = transactions.select(
    "transaction_id",
    "customer_id",
    "product_id",
    "quantity",
    "revenue",
    "transaction_date",
)

customer_cols = customers.select(
    "customer_id",
    "region_id",
    "signup_date",
    "tier",
)

# ---------------------------------------------------------------------------
# Join and aggregate
# ---------------------------------------------------------------------------

enriched = (  # noqa: SPL-D06-006
    txn_cols.join(  # noqa: SPL-D02-007, SPL-D03-002, SPL-D03-009, SPL-D05-005, SPL-D10-004
        F.broadcast(regions), on="region_id", how="left"
    )
    .join(customer_cols, on="customer_id", how="inner")
    .withColumn("month", F.date_trunc("month", F.col("transaction_date")))
)

monthly_summary = enriched.groupBy(  # noqa: SPL-D06-006
    "customer_id", "region_id", "month", "tier"
).agg(
    F.sum("revenue").alias("monthly_revenue"),
    F.sum("quantity").alias("total_quantity"),
    F.count("*").alias("transaction_count"),  # noqa: SPL-D11-004
)

logger.info("Writing I/O outputs")
try:
    monthly_summary.write.mode("overwrite").partitionBy(  # noqa: SPL-D07-005
        "month", "tier"
    ).parquet(  # noqa: SPL-D07-005
        f"{OUTPUT_ROOT}/customer_monthly_summary"
    )
except Exception as exc:
    logger.error("Write failed: %s", exc)
    raise
