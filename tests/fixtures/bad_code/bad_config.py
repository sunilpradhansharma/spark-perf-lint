"""Nightly batch scoring job — bad cluster configuration.

Runs a large-scale ML scoring pipeline.  The SparkSession is created
with only a minimal set of configs; critical performance settings are
left at their defaults or set to suboptimal values.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# SparkSession with missing and bad config values
# ---------------------------------------------------------------------------

# SPL-D01-001: spark.serializer is not set to KryoSerializer.
#   Default Java serialiser is 10× slower and produces larger objects.
#   Fix: .config("spark.serializer",
#               "org.apache.spark.serializer.KryoSerializer")        [WARNING]
#
# SPL-D01-002: spark.executor.memory not configured.
#   Executors run with the JVM default (512 MB), OOM-ing on any
#   moderately-sized join or aggregation.                             [WARNING]
#
# SPL-D01-003: spark.driver.memory not configured.
#   Driver collects broadcast tables and metadata; at default 1 GB it
#   will fail on a schema-heavy dataset.                              [WARNING]
#
# SPL-D01-010: spark.sql.shuffle.partitions left at default (200).
#   On a 200-node cluster this produces 1 partition per executor —
#   far too few for large shuffles.                                   [WARNING]
spark = (
    SparkSession.builder
    .appName("nightly_ml_scoring")
    # SPL-D01-005: executor.cores = 8 exceeds the recommended maximum
    # of 5.  More cores per executor increases GC pressure and reduces
    # HDFS throughput (each core opens its own HDFS stream).          [WARNING]
    .config("spark.executor.cores", "8")
    .getOrCreate()
)

# ---------------------------------------------------------------------------
# Load feature table and model inputs
# ---------------------------------------------------------------------------

features   = spark.read.parquet("/data/ml_features/current")
model_meta = spark.read.parquet("/data/model_metadata")

# ---------------------------------------------------------------------------
# Score — multi-join pipeline that suffers from all the missing configs
# ---------------------------------------------------------------------------

candidates = (
    features
    .filter(F.col("is_eligible") == True)
    .join(model_meta, on="model_id", how="inner")
)

# Without KryoSerializer each row in this wide table is serialised with
# Java ObjectOutputStream — roughly 3× larger than Kryo for typical
# Spark Row objects.
scored = (
    candidates
    .withColumn("raw_score",    F.col("feature_1") * 0.4 + F.col("feature_2") * 0.6)
    .withColumn("score_bucket", F.when(F.col("raw_score") > 0.8, "high")
                                 .when(F.col("raw_score") > 0.5, "medium")
                                 .otherwise("low"))
)

# With shuffle.partitions=200 and a large dataset this groupBy creates
# 200 tasks regardless of data size — too few for a cluster with
# hundreds of cores.
score_distribution = (
    scored
    .groupBy("model_id", "score_bucket")
    .agg(F.count("*").alias("count"), F.avg("raw_score").alias("avg_score"))
)

scored.write.mode("overwrite").parquet("/output/ml_scores")
score_distribution.write.mode("overwrite").parquet("/output/score_distribution")
