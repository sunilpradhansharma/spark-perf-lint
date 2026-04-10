"""Multi-source data ingestion pipeline — bad I/O and file-format patterns.

Reads from a JDBC source, a CSV drop zone, and a legacy JSON feed, then
joins and writes the result.  Every read and write in this file has an
I/O anti-pattern that will hurt performance or correctness at scale.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("multi_source_ingestion")
    .config("spark.sql.shuffle.partitions", "400")
    .getOrCreate()
)

JDBC_URL = "jdbc:postgresql://db.prod.internal:5432/warehouse"

# ---------------------------------------------------------------------------
# JDBC read — no partition parameters
# ---------------------------------------------------------------------------

# SPL-D07-006: spark.read.jdbc() without partitionColumn / numPartitions
# runs as a single-threaded read on the driver.  On a table with millions
# of rows this takes many minutes and saturates the DB connection.
# Add partitionColumn="order_id", lowerBound=0, upperBound=10_000_000,
# numPartitions=50 to parallelise across executor threads.           [CRITICAL]
orders_jdbc = spark.read.jdbc(
    url=JDBC_URL,
    table="public.orders",
    properties={"user": "spark_ro", "password": "secret123"},
)

# ---------------------------------------------------------------------------
# CSV read with schema inference
# ---------------------------------------------------------------------------

# SPL-D07-001: CSV is a row-oriented text format with no native support
# for column pruning or predicate pushdown.  For analytical workloads
# convert to Parquet or Delta once and read from there.              [WARNING]
#
# SPL-D07-002: inferSchema=True reads the CSV twice — once to infer
# types, once to load data.  On a 10 GB CSV this doubles I/O and
# introduces silent type coercions (int→string on mixed columns).    [WARNING]
returns_csv = spark.read.csv(
    "/landing/returns/",
    header=True,
    inferSchema=True,   # SPL-D07-002: schema inference enabled
)

# ---------------------------------------------------------------------------
# JSON feed — also a row-oriented anti-pattern at scale
# ---------------------------------------------------------------------------

# SPL-D07-001 (second occurrence): JSON has the same pushdown limitations
# as CSV plus larger storage overhead than columnar formats.          [WARNING]
events_json = spark.read.json("/landing/clickstream/*.json")

# ---------------------------------------------------------------------------
# Column selection — reading all columns including unused ones
# ---------------------------------------------------------------------------

# SPL-D07-003: select("*") disables column pruning.  Parquet and ORC
# can skip entire column chunks at the storage layer; select("*") forces
# Spark to deserialise every column even if downstream steps only use
# three of them.                                                      [WARNING]
all_orders = orders_jdbc.select("*")

# ---------------------------------------------------------------------------
# Join and write
# ---------------------------------------------------------------------------

combined = (
    all_orders
    .join(returns_csv,  on="order_id", how="left")
    .join(events_json,  on="session_id", how="left")
    .filter(F.col("order_status") != "cancelled")
)

combined.write.mode("overwrite").parquet("/output/orders_enriched")
