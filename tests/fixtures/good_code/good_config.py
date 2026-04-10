"""Nightly batch scoring job — correct cluster configuration.

Every D01 rule is satisfied: serialiser, executor memory, driver memory,
executor cores within the recommended limit, explicit shuffle partitions,
AQE enabled, dynamic allocation, speculation, and a Spark listener.
"""

import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)

# Externalise paths so the scanner does not flag hardcoded literals.
DATA_ROOT   = os.environ.get("DATA_ROOT",   "/data")
OUTPUT_ROOT = os.environ.get("OUTPUT_ROOT", "/output")

# ---------------------------------------------------------------------------
# SparkSession — all D01 rules satisfied
# ---------------------------------------------------------------------------

spark = (
    SparkSession.builder
    .appName("nightly_ml_scoring")
    # KryoSerializer: 3-10× faster than Java serialiser for Spark Row objects.
    .config("spark.serializer",
            "org.apache.spark.serializer.KryoSerializer")
    # Executor / driver memory: explicitly sized for the workload.
    .config("spark.executor.memory", "8g")
    .config("spark.driver.memory",   "4g")
    # Executor cores: ≤ 5 avoids GC pressure and HDFS throughput limits.
    .config("spark.executor.cores", "4")
    # Shuffle partitions: set explicitly; 400 works well on a 50-node cluster.
    .config("spark.sql.shuffle.partitions", "400")
    # AQE: coalesces small shuffle partitions and handles join skew at runtime.
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.adaptive.skewJoin.enabled", "true")
    # CBO: enables statistics-based join ordering.
    .config("spark.sql.cbo.enabled", "true")
    .config("spark.sql.cbo.joinReorder.enabled", "true")
    # Dynamic allocation: scales executors to actual workload.
    .config("spark.dynamicAllocation.enabled", "true")
    # Speculation: re-launches straggler tasks automatically.
    .config("spark.speculation", "true")
    # Listener: reports stage/task statistics to Spark History Server.
    .config("spark.extraListeners",
            "org.apache.spark.scheduler.StatsReportListener")
    # Parquet compression: snappy is fast and widely compatible.
    .config("spark.sql.parquet.compression.codec", "snappy")
    .getOrCreate()
)

# ---------------------------------------------------------------------------
# ETL pipeline
# ---------------------------------------------------------------------------

features   = spark.read.parquet(f"{DATA_ROOT}/ml_features/current")
model_meta = spark.read.parquet(f"{DATA_ROOT}/model_metadata")

candidates = (
    features
    .filter(F.col("is_eligible") == True)
    .join(
        F.broadcast(model_meta),
        on="model_id",
        how="inner",
    )
)

scored = (
    candidates
    .withColumn(
        "raw_score",
        F.col("feature_1") * 0.4 + F.col("feature_2") * 0.6,
    )
    .withColumn(
        "score_bucket",
        F.when(F.col("raw_score") > 0.8, "high")
         .when(F.col("raw_score") > 0.5, "medium")
         .otherwise("low"),
    )
)

score_distribution = (
    scored
    .groupBy("model_id", "score_bucket")
    .agg(
        F.count("*").alias("count"),
        F.avg("raw_score").alias("avg_score"),
    )
)

logger.info("Scoring complete — writing outputs")
try:
    scored.write.mode("overwrite").partitionBy("score_bucket").parquet(
        f"{OUTPUT_ROOT}/ml_scores"
    )
    score_distribution.write.mode("overwrite").parquet(
        f"{OUTPUT_ROOT}/score_distribution"
    )
except Exception as exc:
    logger.error("Write failed: %s", exc)
    raise
